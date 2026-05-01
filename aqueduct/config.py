"""Project-level engine configuration — aqueduct.yml loader.

aqueduct.yml is separate from Blueprint YAML files.  It configures the engine
itself: deployment target, store backends, probe limits, secrets provider, and
webhook endpoints.  It is NOT the pipeline definition.

LLM agent connection config (provider, base_url, model, ollama_options) lives
here as engine-level defaults.  Per-pipeline policy (approval_mode,
on_pending_patches, max_patches_per_run) lives in the Blueprint agent: block.
Blueprint connection values override engine defaults on conflict.

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
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


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


class AgentConnectionConfig(BaseModel):
    """Engine-level LLM connection defaults.

    Sets provider, endpoint, and model used by all pipelines unless overridden
    in the Blueprint agent: block.  Policy fields (approval_mode,
    on_pending_patches, max_patches_per_run) belong in the Blueprint, not here.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: Literal["anthropic", "openai_compat"] = "anthropic"
    base_url: str | None = None
    model: str = "claude-sonnet-4-6"
    ollama_options: dict[str, Any] | None = None


class WebhookEndpointConfig(BaseModel):
    """Configuration for a single webhook endpoint.

    Supports template variables in payload values and header values.
    Variables use ${VAR} syntax.

    Built-in variables (always available in payload templates):
      ${run_id}          UUID of this pipeline run
      ${pipeline_id}     Pipeline identifier
      ${pipeline_name}   Pipeline display name
      ${failed_module}   Module ID where failure occurred
      ${error_message}   Error description string
      ${error_type}      Exception class name
      ${started_at}      ISO-8601 run start timestamp
      ${attempt}         Attempt number (1-based)

    In header values, ${VAR} resolves to os.environ[VAR] as a fallback.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    url: str = Field(..., description="HTTP(S) endpoint URL")
    method: Literal["POST", "PUT", "PATCH"] = "POST"
    headers: dict[str, str] = Field(
        default_factory=dict,
        description="Additional HTTP headers. Values may contain ${ENV_VAR} tokens.",
    )
    payload: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Custom payload template. Values may contain ${VAR} tokens. "
            "If null, the full FailureContext JSON is sent as-is."
        ),
    )
    timeout: int = Field(default=10, description="HTTP socket timeout in seconds")


class WebhooksConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    on_failure: WebhookEndpointConfig | None = Field(
        default=None,
        description="Endpoint to POST when a pipeline run fails. Accepts a URL string or full config object.",
    )

    @field_validator("on_failure", mode="before")
    @classmethod
    def coerce_string_url(cls, v: Any) -> Any:
        """Allow on_failure: 'https://...' as shorthand for {url: 'https://...'}."""
        if isinstance(v, str):
            return {"url": v}
        return v


# ── Top-level config ──────────────────────────────────────────────────────────

class AqueductConfig(BaseModel):
    """Fully validated engine configuration.

    All fields have sensible defaults so that running without an aqueduct.yml
    (development / CI) works out of the box.

    LLM connection defaults (provider, base_url, model) live here.
    Per-pipeline policy (approval_mode) lives in the Blueprint agent: block.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    aqueduct_config: str = Field(default="1.0", description="Config schema version")
    deployment: DeploymentConfig = Field(default_factory=DeploymentConfig)
    stores: StoresConfig = Field(default_factory=StoresConfig)
    probes: ProbesConfig = Field(default_factory=ProbesConfig)
    secrets: SecretsConfig = Field(default_factory=SecretsConfig)
    webhooks: WebhooksConfig = Field(default_factory=WebhooksConfig)
    agent: AgentConnectionConfig = Field(default_factory=AgentConnectionConfig)
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
        return AqueductConfig()

    if not isinstance(data, dict):
        raise ConfigError(f"Config file {resolved} must be a YAML mapping, not {type(data).__name__}")

    try:
        return AqueductConfig.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(
            f"Config validation failed for {resolved}:\n{exc}"
        ) from exc
