"""Project-level engine configuration — aqueduct.yml loader.

aqueduct.yml is separate from Blueprint YAML files.  It configures the engine
itself: deployment target, store backends, probe limits, secrets provider, and
webhook endpoints.  It is NOT the blueprint definition.

LLM agent connection config (provider, base_url, model, provider_options) lives
here as engine-level defaults.  Per-blueprint policy (approval_mode,
on_pending_patches, max_patches) lives in the Blueprint agent: block.
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
from typing import Annotated, Any, Literal

import yaml
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, ValidationError, field_validator

from aqueduct.parser.fs_path import FsPath, field_is_fs_path


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
    """Backend config for stores that need SQL semantics (observability, lineage).

    `redis` is rejected at parse time via the `RelationalBackend` Literal —
    redis is KV-only and cannot satisfy joins/aggregates that the observability /
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
    path: Annotated[str, FsPath()] = Field(
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
            "counters). `redis` is depot-only; choosing it for observability or "
            "lineage is rejected at config load."
        ),
    )
    path: Annotated[str, FsPath()] = Field(
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


def _anchor_fs_path_fields_under_stores(data: dict, base_dir: Path) -> None:
    """Anchor any ``Annotated[str, FsPath()]`` field under ``stores.*`` in-place.

    Walks ``StoresConfig.model_fields`` to find each store-name → sub-model
    pair, then walks each sub-model's ``model_fields`` to find FsPath-marked
    string fields. For each, anchors the raw-input value against ``base_dir``
    using the standard rule: leave URI-style values and absolute paths
    untouched; anchor everything else relative to ``base_dir``.

    Schema-driven — adding a new path field anywhere under ``stores.*`` is one
    edit at the schema site (``Annotated[str, FsPath()]``) and the walker
    picks it up automatically.
    """
    stores_raw = data.get("stores") if isinstance(data, dict) else None
    if not isinstance(stores_raw, dict):
        return

    # Map store-name (observability / lineage / depot) → sub-model class
    # via StoresConfig's own field annotations. Avoids a hardcoded mapping
    # that would drift if a new store is added.
    for store_name, field_info in StoresConfig.model_fields.items():
        sub_model_cls = field_info.annotation
        if not (isinstance(sub_model_cls, type) and issubclass(sub_model_cls, BaseModel)):
            continue
        store_val = stores_raw.get(store_name)
        if not isinstance(store_val, dict):
            continue
        for sub_field_name, sub_field_info in sub_model_cls.model_fields.items():
            marker = field_is_fs_path(tuple(sub_field_info.metadata))
            if marker is None:
                continue
            raw_val = store_val.get(sub_field_name)
            if not isinstance(raw_val, str) or not raw_val:
                continue
            if marker.allow_uri and "://" in raw_val:
                continue
            pp = Path(raw_val)
            if pp.is_absolute():
                continue
            store_val[sub_field_name] = str((base_dir / pp).resolve())


class StoresConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    observability: RelationalStoreConfig = Field(
        default_factory=lambda: RelationalStoreConfig(path=".aqueduct/observability.db"),
        description=(
            "Observability store. Contains run records, failure contexts, "
            "healing outcomes, signal overrides, probe signals, module + "
            "maintenance metrics. With postgres backend, tables live in the "
            "`observability` schema of the target DB."
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
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    allow_full_probe_actions: bool = Field(
        default=False,
        description="Allow full Spark actions in Probes (row_count, freshness). Default false = safe.",
    )
    # 1.1.0 — `allow_multi_patch` is the canonical name. `allow_aggressive_patching`
    # is accepted as a deprecated alias and emits a warning when the multi-patch
    # loop is opted into (max_patches > 1).
    allow_multi_patch: bool = Field(
        default=False,
        validation_alias=AliasChoices("allow_multi_patch", "allow_aggressive_patching"),
        description=(
            "Allow `max_patches > 1` — the multi-patch reprompt loop. Auto-applies "
            "successive LLM patches without human review until success or budget exhaustion."
        ),
    )
    allow_full_preflight: bool = Field(
        default=False,
        description=(
            "Allow agent.sandbox_mode: preflight — full-dataset sandbox replay "
            "(no Egress writes). Slow but conclusive. Default false = safe."
        ),
    )
    allow_skip_sandbox: bool = Field(
        default=False,
        description=(
            "Allow agent.sandbox_mode: off — skip sandbox replay entirely; "
            "patches go straight to production data via the next execute(). "
            "Most dangerous; combine with approval_mode: auto + max_patches > 1 "
            "only when you fully trust the model and blueprint scope is tiny."
        ),
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


class AgentBudgetConfig(BaseModel):
    """Phase 34 multi-axis heal-loop budget. All fields optional with safe defaults.

    Same instance is used by production heal AND benchmark — divergence would
    silently invalidate the leaderboard. Only ``max_reprompts`` + ``max_seconds``
    are recommended for routine tuning; the rest are advanced axes documented in
    ``aqueduct.agent.budget``.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    max_reprompts: int = 5
    max_seconds: float = 120.0
    max_tokens_total: int | None = 50_000
    same_error_consecutive: int = 2
    same_signature_overall: int = 3
    progress_stalled_window: int = 3


class AgentConnectionConfig(BaseModel):
    """Engine-level LLM connection defaults.

    Sets provider, endpoint, and model used by all blueprints unless overridden
    in the Blueprint agent: block.  Policy fields (approval_mode,
    on_pending_patches, max_patches) belong in the Blueprint, not here.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: Literal["anthropic", "openai_compat"] = "anthropic"
    base_url: str | None = None
    model: str = "claude-sonnet-4-6"
    provider_options: dict[str, Any] | None = None
    timeout: float = Field(
        default=300.0,
        description=(
            "HTTP socket timeout (seconds) for agent API calls. Default 300 — "
            "tolerates local-model cold-start (model load into VRAM) plus "
            "inference on small/medium models. Hosted APIs (Anthropic) "
            "typically respond in <30s so the larger ceiling does not affect "
            "them. Increase to 600+ for very large local models, or set "
            "explicitly to 120 to restore the pre-1.0.3 behaviour."
        ),
    )
    max_reprompts: int = Field(
        default=3,
        description="Max reprompt attempts when the agent returns invalid PatchSpec JSON.",
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
    patch_validation: Literal["full_run", "sandbox"] = Field(
        default="full_run",
        description=(
            "Phase 29a — engine-wide validation level for LLM-generated patches in "
            "`auto` mode. `full_run` (default, safest): sandbox replay "
            "first, then a real Spark run against the patched Blueprint, write "
            "only if both pass. `sandbox`: sandbox replay only; write on sandbox "
            "pass. Per-blueprint override: agent.patch_validation."
        ),
    )
    budget: AgentBudgetConfig | None = Field(
        default=None,
        description=(
            "Phase 34 multi-axis heal-loop budget. None ⇒ synthesized from "
            "`max_reprompts` for backward compatibility. Production heal and "
            "`aqueduct benchmark` share the SAME budget — divergence would "
            "make the leaderboard lie about model behaviour under the user's "
            "real config."
        ),
    )
    block_on_explain_regression: bool = Field(
        default=False,
        description=(
            "Phase 29b — when True, the explain gate (post-patch `explain()` regression "
            "check) is treated as blocking in `auto` multi-patch mode: a patch that "
            "adds shuffles / Python UDF nodes or drops broadcast hints is "
            "rejected instead of merely warned. Default False — the explain gate is "
            "warn-only across all healing modes, preserving current behaviour."
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

class WarningsConfig(BaseModel):
    """Phase 30a — controls Aqueduct's diagnostic warnings (compile-time + session-startup).

    Warnings are emitted with the format ``AQ-WARN [rule_id] message``; copy
    the bracketed rule_id into ``suppress`` to silence one rule. One
    mechanism: blacklist rule_ids, or the single sentinel ``"*"`` to silence
    every diagnostic (CI smoke jobs, one-shot scripts), or empty for none.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    suppress: list[str] = Field(
        default_factory=list,
        description=(
            "List of `rule_id` strings to silence (copy IDs from the "
            "`AQ-WARN [...]` prefix). The single entry `\"*\"` silences ALL "
            "Aqueduct warnings."
        ),
    )


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
    warnings: "WarningsConfig" = Field(default_factory=lambda: WarningsConfig())
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
    for observability/lineage, so this function only needs to check driver
    availability — it does not need to re-enforce the legal-combination
    matrix.
    """
    import importlib.util as _import_util

    seen: set[str] = set()
    for store_label, backend in (
        ("observability", stores_cfg.observability.backend),
        ("lineage",       stores_cfg.lineage.backend),
        ("depot",         stores_cfg.depot.backend),
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


def _expand_env(text: str) -> tuple[str, list[str]]:
    """First pass — expand ``${VAR}`` and ``${VAR:-default}`` only.

    ``@aq.secret()`` is deferred to the second pass because resolving it
    requires a validated ``SecretsConfig`` (provider, region, resolver),
    which is not available until the first pass output has been parsed.

    Returns ``(expanded_text, missing_env_vars)``. Caller raises
    ``ConfigError`` if ``missing_env_vars`` is non-empty.
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

    return _ENV_VAR_RE.sub(_replace_env, text), missing


def _expand_secrets(text: str, secrets_cfg: "SecretsConfig") -> tuple[str, list[str]]:
    """Second pass — resolve ``@aq.secret('KEY')`` via the configured provider.

    Registers every resolved value with ``aqueduct.redaction`` so it can be
    scrubbed from downstream outputs (logs, observability rows, patch sidecar
    files, webhook bodies, LLM request payloads).

    Returns ``(expanded_text, missing_secrets)``. ``missing_secrets`` collects
    keys the provider did not find — caller raises ``ConfigError``.
    """
    from aqueduct.secrets import SecretsError, resolve_secret
    from aqueduct import redaction

    missing: list[str] = []

    def _replace_secret(m: re.Match) -> str:
        key = m.group(1)
        try:
            val = resolve_secret(
                key,
                provider=secrets_cfg.provider,
                region=secrets_cfg.region,
                resolver=secrets_cfg.resolver,
            )
        except SecretsError:
            missing.append(f"@aq.secret('{key}')")
            return ""
        redaction.register(val, key_hint=key)
        return val

    return _AQ_SECRET_RE.sub(_replace_secret, text), missing


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

    # ── Pass 1: expand ${VAR} only, parse + validate to discover secrets cfg ──
    pass1_text, missing_env = _expand_env(raw_text)
    if missing_env:
        unique_missing = list(dict.fromkeys(missing_env))
        raise ConfigError(
            f"Missing environment variables in {resolved}: {', '.join(unique_missing)}"
        )

    try:
        data = yaml.safe_load(pass1_text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {resolved}: {exc}") from exc

    if data is None:
        return AqueductConfig()

    if not isinstance(data, dict):
        raise ConfigError(f"Config file {resolved} must be a YAML mapping, not {type(data).__name__}")

    # Schema-driven anchoring. The hand-rolled
    # ``data["stores"][name]["path"]`` walk has been replaced with a
    # generic walker that reads ``FsPath()`` markers from each store
    # model's ``model_fields`` metadata. Adding a new filesystem-path
    # field anywhere under ``stores.*`` is now one edit at the schema
    # site (`Annotated[str, FsPath()]`) — no parser/config touch.
    # Idempotent: absolute paths and URI-style values
    # (``s3://``, ``postgresql://``, ``redis://``) pass through.
    _cfg_dir = resolved.parent.resolve()
    _anchor_fs_path_fields_under_stores(data, _cfg_dir)

    try:
        cfg_pass1 = AqueductConfig.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(_format_config_error(resolved, exc)) from exc

    # Fail fast on missing secrets-backend SDKs so the failure surface is
    # `aqueduct.yml` validation, not a generic ImportError mid-compile.
    _validate_secrets_backend(cfg_pass1.secrets)

    # ── Pass 2: resolve @aq.secret() via the configured provider ──────────────
    # Short-circuit when the file has no @aq.secret() tokens — keeps the common
    # case (no secrets) at one YAML parse and one validation.
    if not _AQ_SECRET_RE.search(pass1_text):
        _validate_store_backends(cfg_pass1.stores)
        return cfg_pass1

    pass2_text, missing_secrets = _expand_secrets(pass1_text, cfg_pass1.secrets)
    if missing_secrets:
        unique_missing = list(dict.fromkeys(missing_secrets))
        raise ConfigError(
            f"Unresolved secrets in {resolved} via provider "
            f"{cfg_pass1.secrets.provider!r}: {', '.join(unique_missing)}"
        )

    try:
        data2 = yaml.safe_load(pass2_text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {resolved} after secret expansion: {exc}") from exc

    if data2 is None:
        cfg = AqueductConfig()
    else:
        if not isinstance(data2, dict):
            raise ConfigError(
                f"Config file {resolved} must be a YAML mapping, not {type(data2).__name__}"
            )
        try:
            cfg = AqueductConfig.model_validate(data2)
        except ValidationError as exc:
            raise ConfigError(_format_config_error(resolved, exc)) from exc

    _validate_store_backends(cfg.stores)
    return cfg
