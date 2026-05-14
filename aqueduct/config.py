"""Project-level engine configuration — aqueduct.yml loader.

aqueduct.yml is separate from Blueprint YAML files.  It configures the engine
itself: deployment target, store backends, probe limits, secrets provider, and
webhook endpoints.  It is NOT the blueprint definition.

LLM agent connection config (provider, base_url, model, provider_options) lives
here as engine-level defaults.  Per-blueprint policy (approval_mode,
on_pending_patches, aggressive_max_patches) lives in the Blueprint agent: block.
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

import os
import re
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

    engine: Literal["spark", "flink"] = Field(
        default="spark",
        description="Execution engine.  Currently supported: spark.  Planned: flink.",
    )
    target: Literal[
        "local", "standalone", "yarn", "kubernetes", "databricks", "emr", "dataproc"
    ] = Field(
        default="local",
        description="Deployment target: local | standalone | yarn | kubernetes | databricks | emr | dataproc",
    )
    master_url: str = Field(
        default="local[*]",
        description="Engine-specific cluster URL (Spark: SparkSession.builder.master()).",
    )
    env: Literal["local", "cluster", "cloud"] = Field(
        default="local",
        description="Deployment environment tier. Doctor warns on local paths in cluster/cloud mode.",
    )


RelationalBackend = Literal["duckdb", "postgres"]
KVBackend = Literal["duckdb", "postgres", "redis"]


class RelationalStoreConfig(BaseModel):
    """Backend config for stores that need SQL semantics (obs, lineage).

    `redis` is rejected at parse time via the `RelationalBackend` Literal —
    redis is KV-only and cannot satisfy joins/aggregates that the obs /
    lineage queries rely on.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    backend: RelationalBackend = Field(
        default="duckdb",
        description=(
            "Relational backend for this store. `duckdb` (default) is the "
            "embedded single-writer file format. `postgres` is a networked "
            "DSN — supports concurrent writers across multiple driver "
            "processes. `s3 | gcs | adls` are deferred (TODOs.md Phase 28+)."
        ),
    )
    path: str = Field(
        ...,
        description=(
            "DuckDB: local file path. Postgres: libpq DSN such as "
            "`postgresql://user:pass@host/aqueduct_db`."
        ),
    )


class KVStoreConfig(BaseModel):
    """Backend config for the depot — accepts relational or pure KV backends."""
    model_config = ConfigDict(frozen=True, extra="forbid")

    backend: KVBackend = Field(
        default="duckdb",
        description=(
            "Depot backend. `duckdb` (default) or `postgres` for relational; "
            "`redis` for high-QPS KV-only depot reads (watermarks, atomic "
            "counters). `redis` is depot-only; choosing it for obs or "
            "lineage is rejected at config load."
        ),
    )
    path: str = Field(
        ...,
        description=(
            "DuckDB: local file path. Postgres: libpq DSN. "
            "Redis: `redis://host:port/db` URL."
        ),
    )


# Backwards-compatible alias for code that still imports the old name.
# The two new classes have identical shapes for the relational path; the
# alias maps to `RelationalStoreConfig` so static typing keeps working.
StoreBackendConfig = RelationalStoreConfig


class StoresConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    obs: RelationalStoreConfig = Field(
        default_factory=lambda: RelationalStoreConfig(path=".aqueduct/obs.db"),
        description=(
            "Observability store. Contains run records, failure contexts, "
            "healing outcomes, signal overrides, probe signals, module + "
            "maintenance metrics. With postgres backend, tables live in the "
            "`obs` schema of the target DB."
        ),
    )
    lineage: RelationalStoreConfig = Field(
        default_factory=lambda: RelationalStoreConfig(path=".aqueduct/lineage.db"),
        description=(
            "Column-level lineage store. With postgres backend, tables live "
            "in the `lineage` schema of the target DB."
        ),
    )
    depot: KVStoreConfig = Field(
        default_factory=lambda: KVStoreConfig(path=".aqueduct/depot.db"),
        description=(
            "Depot KV store (`@aq.depot.*`). With postgres backend, the "
            "`depot_kv` table lives in the `depot` schema. With redis "
            "backend, keys live directly in the configured Redis database."
        ),
    )


class MetricsConfig(BaseModel):
    """Per-module observability tuning.

    See docs/SPARK_GUIDE.md ("DataFrame.observe() and Whole-Stage Codegen") for
    the tradeoff between accurate per-module attribution and codegen overhead.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    use_observe: bool = Field(
        default=True,
        description=(
            "When true (default), wrap each Ingress read / Egress write with "
            "DataFrame.observe() for accurate per-module records_read / "
            "records_written counts. When false, rely on SparkListener stage "
            "metrics only — avoids a ~5–15% throughput regression on "
            "high-throughput pipelines where observe() can break whole-stage "
            "codegen, at the cost of less accurate per-module attribution "
            "(stage fusion groups consecutive modules into one stage)."
        ),
    )


class ProbesConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    max_sample_rows: int = 100
    default_sample_fraction: float = 0.01
    # Note: the legacy `block_full_actions_in_prod` flag was removed in v1.0.0a3.
    # The active gate is `danger.allow_full_probe_actions` (inverted polarity).


class DangerConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    allow_full_probe_actions: bool = Field(
        default=False,
        description="Allow full Spark actions in Probes (row_count, freshness). Default false = safe.",
    )
    allow_aggressive_patching: bool = Field(
        default=False,
        description="Allow approval_mode: aggressive to auto-apply LLM patches without human review.",
    )


class SecretsConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: str = Field(
        default="env",
        description="Secrets provider: env | aws | gcp | azure | custom",
    )
    region: str | None = Field(
        default=None,
        description="Cloud region for aws/gcp/azure providers (e.g. 'us-east-1').",
    )
    resolver: str | None = Field(
        default=None,
        description="Dotted import path for custom provider: 'mypackage.secrets.fetch'. "
                    "Callable signature: (key: str) -> str | None. "
                    "Return None to fall back to os.environ.",
    )


class AgentConnectionConfig(BaseModel):
    """Engine-level LLM connection defaults.

    Sets provider, endpoint, and model used by all blueprints unless overridden
    in the Blueprint agent: block.  Policy fields (approval_mode,
    on_pending_patches, aggressive_max_patches) belong in the Blueprint, not here.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: Literal["anthropic", "openai_compat"] = "anthropic"
    base_url: str | None = None
    model: str = "claude-sonnet-4-6"
    provider_options: dict[str, Any] | None = None
    llm_timeout: float = Field(
        default=120.0,
        description="HTTP socket timeout in seconds for LLM API calls. Increase for slow local models (e.g. 600.0).",
    )
    llm_max_reprompts: int = Field(
        default=3,
        description="Max reprompt attempts when LLM returns invalid PatchSpec JSON.",
    )
    prompt_context: str | None = Field(
        default=None,
        description="Extra context appended to the LLM system prompt for all blueprints. Use for cluster constraints, naming conventions, schema hints.",
    )
    ci_webhook_url: str | None = Field(
        default=None,
        description="Webhook URL for approval_mode: ci. POST target for external CI to create PR.",
    )
    max_heal_attempts_per_hour: int | None = Field(
        default=None,
        description=(
            "Engine-wide spend-cap on LLM self-healing. When set, every blueprint's "
            "Surveyor counts rows in `healing_outcomes` within the last 60 minutes "
            "and skips further LLM calls when the count is reached. Defaults to "
            "None (unlimited). Per-blueprint override: agent.max_heal_attempts_per_hour."
        ),
    )


class WebhookEndpointConfig(BaseModel):
    """Configuration for a single webhook endpoint.

    Supports template variables in payload values and header values.
    Variables use ${VAR} syntax.

    Built-in variables (always available in payload templates):
      ${run_id}           UUID of this blueprint run
      ${blueprint_id}     Blueprint identifier
      ${blueprint_name}   Blueprint display name
      ${failed_module}    Module ID where failure occurred
      ${error_message}   Error description string
      ${error_type}      Exception class name
      ${started_at}      ISO-8601 run start timestamp
      ${attempt}         Number of failed modules in this run (on_failure scope).
                         The naming is historical — it is not a retry counter.

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
        description="Endpoint to POST when a blueprint run fails. Accepts a URL string or full config object.",
    )
    on_success: WebhookEndpointConfig | None = Field(
        default=None,
        description="Endpoint to POST when a blueprint run succeeds. Accepts a URL string or full config object.",
    )
    on_patch_pending: WebhookEndpointConfig | None = Field(
        default=None,
        description="Endpoint to POST when a patch is staged to patches/pending/.",
    )
    on_ci_patch: WebhookEndpointConfig | None = Field(
        default=None,
        description="Endpoint for approval_mode: ci — receives patch JSON for external PR creation.",
    )

    @field_validator("on_failure", "on_success", "on_patch_pending", "on_ci_patch", mode="before")
    @classmethod
    def coerce_string_url(cls, v: Any) -> Any:
        """Allow on_failure/on_success: 'https://...' as shorthand for {url: 'https://...'}."""
        if isinstance(v, str):
            return {"url": v}
        return v


# ── Top-level config ──────────────────────────────────────────────────────────

class AqueductConfig(BaseModel):
    """Fully validated engine configuration.

    All fields have sensible defaults so that running without an aqueduct.yml
    (development / CI) works out of the box.

    LLM connection defaults (provider, base_url, model) live here.
    Per-blueprint policy (approval_mode) lives in the Blueprint agent: block.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    aqueduct_config: str = Field(default="1.0", description="Config schema version")
    deployment: DeploymentConfig = Field(default_factory=DeploymentConfig)
    stores: StoresConfig = Field(default_factory=StoresConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    probes: ProbesConfig = Field(default_factory=ProbesConfig)
    danger: DangerConfig = Field(default_factory=DangerConfig)
    secrets: SecretsConfig = Field(default_factory=SecretsConfig)
    webhooks: WebhooksConfig = Field(default_factory=WebhooksConfig)
    agent: AgentConnectionConfig = Field(default_factory=AgentConnectionConfig)
    spark_config: dict[str, Any] = Field(
        default_factory=dict,
        description="Engine-level Spark conf merged with Blueprint spark_config (Blueprint wins)",
    )


# ── Loader ────────────────────────────────────────────────────────────────────

_DEFAULT_FILENAME = "aqueduct.yml"


# Provider → (probe module, install hint). Keep in sync with secrets.py and
# pyproject.toml [project.optional-dependencies].
_SECRETS_BACKEND_REQUIREMENTS: dict[str, tuple[str, str]] = {
    "aws":   ("boto3",                           "pip install aqueduct-core[aws]"),
    "gcp":   ("google.cloud.secretmanager",      "pip install aqueduct-core[gcp]"),
    "azure": ("azure.keyvault.secrets",          "pip install aqueduct-core[azure]"),
}


# Store backend → (probe module, install hint). Mirrors the secrets pattern.
# `duckdb` is always available (it is a hard dependency of aqueduct-core).
_STORE_BACKEND_REQUIREMENTS: dict[str, tuple[str, str]] = {
    "postgres": ("psycopg2", "pip install aqueduct-core[postgres]"),
    "redis":    ("redis",    "pip install aqueduct-core[redis]"),
}


def _validate_store_backends(stores_cfg: "StoresConfig") -> None:
    """Fail fast at config-load if a store's backend SDK is missing.

    Mirrors `_validate_secrets_backend()`. The `RelationalBackend` /
    `KVBackend` Literal split already prevents `redis` from being chosen
    for obs/lineage, so this function only needs to check driver
    availability — it does not need to re-enforce the legal-combination
    matrix.
    """
    import importlib.util as _import_util

    seen: set[str] = set()
    for store_label, backend in (
        ("obs",     stores_cfg.obs.backend),
        ("lineage", stores_cfg.lineage.backend),
        ("depot",   stores_cfg.depot.backend),
    ):
        if backend == "duckdb":
            continue
        if backend in seen:
            continue
        seen.add(backend)
        if backend not in _STORE_BACKEND_REQUIREMENTS:
            continue  # Unknown backend — Literal validation will have caught it.
        module_name, install_hint = _STORE_BACKEND_REQUIREMENTS[backend]
        root_pkg = module_name.split(".")[0]
        if _import_util.find_spec(root_pkg) is None:
            raise ConfigError(
                f"stores.*.backend={backend!r} (used by stores.{store_label}) "
                f"requires the {root_pkg!r} package, which is not installed.\n"
                f"Install it with:  {install_hint}"
            )


def _validate_secrets_backend(secrets_cfg: "SecretsConfig") -> None:
    """Fail fast at config-load if the configured secrets provider's SDK is missing.

    Mirrors the check `aqueduct doctor:check_secrets()` performs, but earlier in
    the lifecycle so `aqueduct run` doesn't error mid-compile with a generic
    ImportError. `provider: env` and `provider: custom` require no SDK.
    """
    provider = secrets_cfg.provider
    if provider in ("env", "custom"):
        return
    if provider not in _SECRETS_BACKEND_REQUIREMENTS:
        return  # Unknown provider — let the resolver raise its own clear error.
    import importlib.util as _import_util

    module_name, install_hint = _SECRETS_BACKEND_REQUIREMENTS[provider]
    # find_spec walks the top-level package; nested attribute paths are tolerated
    # by checking only the first segment we can actually resolve.
    root_pkg = module_name.split(".")[0]
    if _import_util.find_spec(root_pkg) is None:
        raise ConfigError(
            f"secrets.provider={provider!r} requires the {root_pkg!r} package, "
            f"which is not installed.\n"
            f"Install it with:  {install_hint}\n"
            f"Or install all secret backends:  pip install aqueduct-core[secrets]"
        )

_ENV_VAR_RE = re.compile(r'\$\{([^}:]+)(?::[-]?(.*?))?\}')
# Matches: "@aq.secret('KEY')" or "@aq.secret(\"KEY\")" with optional surrounding YAML quotes
_AQ_SECRET_RE = re.compile(r"""["']?@aq\.secret\(['"]([^'"]+)['"]\)["']?""")


def _expand_env_vars(text: str) -> tuple[str, list[str]]:
    """Expand ${VAR}, ${VAR:-default}, and @aq.secret('KEY') in aqueduct.yml.

    Returns (expanded_text, missing_vars). Caller raises ConfigError if missing_vars
    is non-empty. @aq.secret() resolves via os.environ only (provider not live yet).
    """
    missing: list[str] = []

    def _replace_env(m: re.Match) -> str:
        var, default = m.group(1), m.group(2)
        val = os.environ.get(var)
        if val is not None:
            return val
        if default is not None:
            return default
        missing.append(f"${{{var}}}")
        return ""

    def _replace_secret(m: re.Match) -> str:
        key = m.group(1)
        val = os.environ.get(key)
        if val is not None:
            return val
        missing.append(f"@aq.secret('{key}')")
        return ""

    text = _ENV_VAR_RE.sub(_replace_env, text)
    text = _AQ_SECRET_RE.sub(_replace_secret, text)
    return text, missing


def _format_config_error(resolved: Path, exc: ValidationError) -> str:
    errors = exc.errors(include_url=False)
    n = len(errors)
    header = f"{n} validation error{'s' if n != 1 else ''} in {resolved}:"
    lines = [header]
    for e in errors:
        loc_parts = []
        for part in e["loc"]:
            if isinstance(part, int):
                loc_parts.append(f"[{part}]")
            elif loc_parts:
                loc_parts.append(f".{part}")
            else:
                loc_parts.append(str(part))
        loc = "".join(loc_parts)
        lines.append(f"  • {loc} — {e['msg']}")
    return "\n".join(lines)


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

    raw_text, missing = _expand_env_vars(raw_text)
    if missing:
        raise ConfigError(
            f"Missing environment variables in {resolved}: {', '.join(missing)}"
        )

    try:
        data = yaml.safe_load(raw_text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {resolved}: {exc}") from exc

    if data is None:
        return AqueductConfig()

    if not isinstance(data, dict):
        raise ConfigError(f"Config file {resolved} must be a YAML mapping, not {type(data).__name__}")

    try:
        cfg = AqueductConfig.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(_format_config_error(resolved, exc)) from exc

    # Fail fast on missing secrets-backend SDKs so the failure surface is
    # `aqueduct.yml` validation, not a generic ImportError mid-compile.
    _validate_secrets_backend(cfg.secrets)
    # Same pattern for store backends — fail at config load instead of
    # mid-run when the first DuckDB / Postgres / Redis connect() fires.
    _validate_store_backends(cfg.stores)

    return cfg
