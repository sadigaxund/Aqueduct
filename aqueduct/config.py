"""Project-level engine configuration — aqueduct.yml loader.

aqueduct.yml is separate from Blueprint YAML files.  It configures the engine
itself: deployment target, store backends, agent settings, probe limits, and
secrets provider.  It is NOT the pipeline definition.

Default behaviour (no file present):
  load_config() returns AqueductConfig with all defaults — equivalent to a
  local-mode dev setup with no external stores or secrets provider.

Precedence when both file and environment are present:
  File values override built-in defaults; CLI flags passed to commands override
  both (handled in cli.py, not here).

Schema reference: docs/specs.md §10.1
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError


# ── Schema error ──────────────────────────────────────────────────────────────

class ConfigError(Exception):
    """Raised when aqueduct.yml cannot be loaded or fails validation."""


# ── Sub-models ────────────────────────────────────────────────────────────────

class DeploymentConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    engine: str = Field(
        default="spark",
        description="Execution engine.  Currently supported: spark.  Planned: flink.",
    )
    target: str = Field(
        default="local",
        description="Deployment target: local | standalone | yarn | kubernetes | databricks | emr | dataproc",
    )
    master_url: str = Field(
        default="local[*]",
        description="Engine-specific cluster URL (Spark: SparkSession.builder.master()).",
    )


class StoreBackendConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    backend: str = Field(default="duckdb", description="Storage backend: duckdb | s3 | gcs | adls")
    path: str = Field(..., description="Local path or remote URI for the store")


class StoresConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    observability: StoreBackendConfig = Field(
        default_factory=lambda: StoreBackendConfig(path=".aqueduct/signals"),
        description="Observability store (run records, failure contexts)",
    )
    lineage: StoreBackendConfig = Field(
        default_factory=lambda: StoreBackendConfig(path=".aqueduct/lineage"),
        description="Column-level lineage store",
    )
    depot: StoreBackendConfig = Field(
        default_factory=lambda: StoreBackendConfig(path=".aqueduct/depot.duckdb"),
        description="Depot KV store (pipeline state, @aq.depot.*)",
    )


class AgentEngineConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    default_model: str = "claude-sonnet-4-20250514"
    api_endpoint: str = "https://api.anthropic.com/v1/messages"
    max_tokens: int = 4096
    max_patches_per_run: int = 5
    default_approval_mode: str = Field(
        default="auto",
        description="Patch approval mode: auto | human",
    )


class ProbesConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    max_sample_rows: int = 100
    default_sample_fraction: float = 0.01
    block_full_actions_in_prod: bool = True


class SecretsConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: str = Field(
        default="env",
        description="Secrets provider: env | aws | gcp | azure | vault",
    )


class WebhooksConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    on_failure: str | None = Field(
        default=None,
        description="HTTP(S) URL to POST FailureContext JSON on pipeline failure",
    )


# ── Top-level config ──────────────────────────────────────────────────────────

class AqueductConfig(BaseModel):
    """Fully validated engine configuration.

    All fields have sensible defaults so that running without an aqueduct.yml
    (development / CI) works out of the box.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    aqueduct_config: str = Field(default="1.0", description="Config schema version")
    deployment: DeploymentConfig = Field(default_factory=DeploymentConfig)
    stores: StoresConfig = Field(default_factory=StoresConfig)
    agent: AgentEngineConfig = Field(default_factory=AgentEngineConfig)
    probes: ProbesConfig = Field(default_factory=ProbesConfig)
    secrets: SecretsConfig = Field(default_factory=SecretsConfig)
    webhooks: WebhooksConfig = Field(default_factory=WebhooksConfig)
    spark_config: dict[str, Any] = Field(
        default_factory=dict,
        description="Engine-level Spark conf merged with Blueprint spark_config (Blueprint wins)",
    )


# ── Loader ────────────────────────────────────────────────────────────────────

_DEFAULT_FILENAME = "aqueduct.yml"


def load_config(path: Path | None = None) -> AqueductConfig:
    """Load and validate engine configuration.

    Args:
        path: Path to the YAML config file.  If None, looks for ``aqueduct.yml``
              in the current working directory.  If the default file is absent,
              returns AqueductConfig with all defaults — no error.  If an
              explicit path is given and the file is missing, raises ConfigError.

    Returns:
        Validated, frozen AqueductConfig.

    Raises:
        ConfigError: Explicit path not found, invalid YAML, or schema violation.
    """
    explicit = path is not None
    resolved = path if explicit else Path(_DEFAULT_FILENAME)

    if not resolved.exists():
        if explicit:
            raise ConfigError(f"Config file not found: {resolved}")
        # Implicit lookup — no file, use all defaults
        return AqueductConfig()

    try:
        raw_text = resolved.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"Cannot read config file {resolved}: {exc}") from exc

    try:
        data = yaml.safe_load(raw_text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {resolved}: {exc}") from exc

    if data is None:
        # Empty file — treat as all defaults
        return AqueductConfig()

    if not isinstance(data, dict):
        raise ConfigError(f"Config file {resolved} must be a YAML mapping, not {type(data).__name__}")

    try:
        return AqueductConfig.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(
            f"Config validation failed for {resolved}:\n{exc}"
        ) from exc
