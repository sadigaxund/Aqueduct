"""Project-level engine configuration — aqueduct.yml loader.

aqueduct.yml is separate from Blueprint YAML files.  It configures the engine
itself: deployment target, store backends, probe limits, secrets provider, and
webhook endpoints.  It is NOT the blueprint definition.

LLM agent connection config (provider, base_url, model, provider_options) lives
here as engine-level defaults.  Per-blueprint policy (approval,
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
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from aqueduct.agent.constants import DEFAULT_LLM_MODEL
from aqueduct.errors import ConfigError
from aqueduct.parser.fs_path import FsPath, field_is_fs_path
from aqueduct.parser.schema import CascadeTierSchema

DEFAULT_OBS_DB_FILENAME: str = "observability.db"

# Matches any RFC3986-shaped URI scheme prefix (s3://, s3a://, gs://, hdfs://,
# abfss://, postgresql://, ...). Used to reject remote URIs on LOCAL-PATH-ONLY
# config fields (e.g. `checkpoint_root`) with an actionable error instead of
# silently mangling the value through `pathlib.Path`.
_URI_SCHEME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.\-]*://")

# ``ConfigError`` lives in ``aqueduct.errors`` (imported above) to break an
# import cycle: config → agent.constants → agent.budget → config. Imported at
# module scope so ``from aqueduct.config import ConfigError`` keeps working with
# a single class identity.


# ── Sub-models ────────────────────────────────────────────────────────────────

class DatabricksDeployConfig(BaseModel):
    """Per-target settings for ``deployment.target: databricks``.

    Required when ``target`` is ``databricks``.  Credentials flow through
    ``DATABRICKS_TOKEN`` env var or ``@aq.secret(...)`` — never plaintext
    in this block.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    workspace_url: str = Field(
        ...,
        description="Databricks workspace URL, e.g. https://dbc-xxxx.cloud.databricks.com",
    )
    cluster_id: str | None = Field(
        default=None,
        description="Existing all-purpose cluster ID. Mutually exclusive with new_cluster.",
    )
    new_cluster: dict | None = Field(
        default=None,
        description="Raw cluster-creation spec per the Databricks Jobs API new_cluster object. "
                    "Mutually exclusive with cluster_id.",
    )
    libraries: list[dict] | None = Field(
        default=None,
        description="Libraries to install on the cluster, e.g. [{'pypi': {'package': 'aqueduct-core[spark]'}}].",
    )
    max_concurrent_runs: int | None = Field(
        default=1,
        description="Maximum concurrent runs for the generated one-shot job.",
    )

    @model_validator(mode="after")
    def _validate_cluster(self) -> DatabricksDeployConfig:
        if not self.cluster_id and not self.new_cluster:
            raise ValueError(
                "deployment.databricks: one of cluster_id or new_cluster is required"
            )
        if self.cluster_id and self.new_cluster:
            raise ValueError(
                "deployment.databricks: cluster_id and new_cluster are mutually exclusive"
            )
        return self


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

    @model_validator(mode="after")
    def _validate_target_master_url(self) -> DeploymentConfig:
        """Enforce target ↔ master_url consistency for in-cluster Spark targets.

        Remote-submit targets (databricks / emr / dataproc) are rejected with
        a forward pointer to Phase 64.
        """
        if self.engine != "spark":
            if self.engine == "flink":
                raise ConfigError(
                    "engine: flink is not yet supported (see docs/roadmap.md). "
                    "Use engine: spark."
                )
            return self

        target = self.target
        master = self.master_url

        # ── Remote-submit targets (not yet implemented) ─────────────────────
        if target == "databricks":
            if self.databricks is None:
                raise ConfigError(
                    "deployment.target=databricks requires the "
                    "deployment.databricks block to be set"
                )
            return self

        if target in ("emr", "dataproc"):
            raise ConfigError(
                f"deployment.target={target!r} is a remote-submit target not "
                f"yet supported. "
                f"Use local | standalone | yarn | kubernetes | databricks."
            )

        # ── In-cluster targets ────────────────────────────────────────────────
        _EXPECTED: dict[str, str] = {
            "local":       "local",
            "standalone":  "spark://",
            "yarn":        "yarn",
            "kubernetes":  "k8s://",
        }
        expected = _EXPECTED.get(target)
        if expected is None:
            return self

        if expected == "yarn":
            if master != "yarn":
                raise ConfigError(
                    f"deployment.target={target!r} requires "
                    f"master_url={expected!r}, "
                    f"got master_url={master!r}"
                )
        elif not master.startswith(expected):
            raise ConfigError(
                f"deployment.target={target!r} requires "
                f"master_url starting with {expected!r}, "
                f"got master_url={master!r}"
            )

        return self
    databricks: DatabricksDeployConfig | None = Field(
        default=None,
        description="Databricks Jobs API settings. Required when target=databricks.",
    )


RelationalBackend = Literal["duckdb", "postgres"]
KVBackend = Literal["duckdb", "postgres", "redis"]
ObjectBackend = Literal["local", "s3", "gcs", "adls"]


class RelationalStoreConfig(BaseModel):
    """Backend config for stores that need SQL semantics (observability).

    `redis` is rejected at parse time via the `RelationalBackend` Literal —
    redis is KV-only and cannot satisfy joins/aggregates that the observability
    queries rely on.
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
    path: Annotated[str, FsPath()] | None = Field(
        default=None,
        description=(
            "DuckDB: a DIRECTORY (routing base) — per-blueprint files are "
            "created underneath as `<path>/<blueprint_id>/observability.db`; "
            "None (default) routes under `.aqueduct/observability/`. A file "
            "path (`.db` suffix) is a config error (2.0: the single-shared-"
            "file mode was removed — DuckDB is single-writer, so it was never "
            "parallel-safe; use `backend: postgres` for one shared concurrent "
            "store). Postgres: libpq DSN such as "
            "`postgresql://user:pass@host/aqueduct_db`."
        ),
    )

    @model_validator(mode="after")
    def _duckdb_path_is_directory(self) -> RelationalStoreConfig:
        # 2.0 BREAKING — duckdb observability path is a routing DIRECTORY only.
        # The old explicit-.db-file mode wrote to `<parent>/observability.db`
        # while reads honoured the configured basename (silent read/write
        # split-brain for any name but the default), and a single shared file
        # is not parallel-safe anyway. Fail loud at load with the migration.
        if self.backend == "duckdb" and self.path and Path(self.path).suffix:
            raise ValueError(
                f"stores.observability.path {self.path!r} looks like a file. "
                "Point it at a DIRECTORY — per-blueprint files are created "
                "underneath (<path>/<blueprint_id>/observability.db). For one "
                "shared concurrent store use backend: postgres. (The DuckDB "
                "single-file mode was removed in 2.x.)"
            )
        return self


class KVStoreConfig(BaseModel):
    """Backend config for the depot — accepts relational or pure KV backends."""
    model_config = ConfigDict(frozen=True, extra="forbid")

    backend: KVBackend = Field(
        default="duckdb",
        description=(
            "Depot backend. `duckdb` (default) or `postgres` for relational; "
            "`redis` for high-QPS KV-only depot reads (watermarks, atomic "
            "counters). `redis` is depot-only; choosing it for observability "
            "is rejected at config load."
        ),
    )
    path: Annotated[str, FsPath()] = Field(
        ...,
        description=(
            "DuckDB: local file path. Postgres: libpq DSN. "
            "Redis: `redis://host:port/db` URL."
        ),
    )


class DepotMountConfig(BaseModel):
    """One depot mount, keyed by name in the ``stores.depots:`` map.

    The depot is the cross-run KV store (`@aq.depot.*`). There is always an
    implicit **default** mount (per-blueprint isolated, DuckDB) even when
    ``depots:`` is omitted; add a ``default:`` entry only to re-back it
    (different backend/path). Any other key is an additional mount accessed via
    ``@aq.depot.<name>.get(...)``.

    Isolation: every mount is **per-blueprint isolated by default** — keys are
    transparently prefixed with the ``blueprint_id`` so two blueprints sharing a
    physical depot never collide. Set ``shared: true`` to opt a mount into
    **cross-blueprint** sharing (raw, unprefixed keys) — the *only* place key
    collisions are possible, and an explicit choice.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    backend: KVBackend = Field(
        default="duckdb",
        description="`duckdb` (default) / `postgres` (relational) or `redis` (KV-only).",
    )
    path: Annotated[str, FsPath()] = Field(
        ...,
        description="DuckDB: local file path. Postgres: libpq DSN. Redis: `redis://host:port/db`.",
    )
    shared: bool = Field(
        default=False,
        description="False (default) = per-blueprint isolated (keys prefixed by blueprint_id). "
                    "True = cross-blueprint shared (raw keys) — collisions become your "
                    "deliberate semantics; for parallel writers use postgres/redis.",
    )


class ObjectStoreConfig(BaseModel):
    """Backend config for opaque driver artefacts — observability blobs and the
    patch lifecycle (Phase 53).

    These are not relational rows but compressed/JSON files the driver writes.
    Routing them through a pluggable object store lets an ephemeral cluster pod
    produce no local-FS artefacts under its cwd. `local` (default) is
    byte-identical to the historical layout, so the git review workflow is
    unchanged. `s3` / `gcs` / `adls` are served by one `fsspec` handle — install
    the `[object-store]` extra.  The `path` is FsPath‑anchored with
    ``allow_uri=True`` (the default) so relative paths resolve against the
    config‑file directory while ``s3://`` / ``gs://`` / ``abfs://`` URIs pass
    through verbatim.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    backend: ObjectBackend = Field(
        default="local",
        description=(
            "Object-store backend for blobs + patch lifecycle. `local` (default) "
            "writes to the driver filesystem. `s3` / `gcs` / `adls` route through "
            "fsspec (requires the `[object-store]` extra)."
        ),
    )
    path: Annotated[str, FsPath()] = Field(
        default="",
        description=(
            "Object-store location. `local`: base directory (empty → per-store "
            "default: blobs under the observability dir, patches under cwd). "
            "`s3` / `gcs` / `adls`: base URI such as `s3://bucket/aqueduct`."
        ),
    )


class BenchmarkStoreConfig(BaseModel):
    """Backend config for the benchmark history store (`aqueduct benchmark`).

    Separate from the observability/lineage/depot stores: benchmark rows are
    not tied to a real ``run_id`` and live in their own DB (DuckDB file) or
    Postgres schema. ``path`` is optional — left unset, the DuckDB store is
    anchored to the scenarios directory (``<scenarios_dir>/.aqueduct/
    benchmark.duckdb``); set it to a file path or, for ``backend: postgres``, a
    libpq DSN.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    backend: RelationalBackend = Field(
        default="duckdb",
        description="`duckdb` (default, embedded file) or `postgres` (networked DSN).",
    )
    path: str | None = Field(
        default=None,
        description=(
            "DuckDB: file path (default: scenario-anchored "
            "`.aqueduct/benchmark.duckdb`). Postgres: libpq DSN. Not FsPath-"
            "anchored — the CLI resolves the scenario-relative default."
        ),
    )
    persist: bool = Field(
        default=True,
        description="Write each (scenario, model) row to the store. False = the old `--no-persist`.",
    )
    gate_on_regression: bool = Field(
        default=False,
        description=(
            "After persisting, diff each (scenario, model) vs its baseline and "
            "exit non-zero on any regression. The old `--gate-on-regression`."
        ),
    )




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

    def _anchor_entry(entry: dict, model_cls: type[BaseModel]) -> None:
        for sub_field_name, sub_field_info in model_cls.model_fields.items():
            marker = field_is_fs_path(tuple(sub_field_info.metadata), sub_field_info.annotation)
            if marker is None:
                continue
            raw_val = entry.get(sub_field_name)
            if not isinstance(raw_val, str) or not raw_val:
                continue
            if marker.allow_uri and "://" in raw_val:
                continue
            pp = Path(raw_val)
            if pp.is_absolute():
                continue
            entry[sub_field_name] = str((base_dir / pp).resolve())

    # Map store-name → sub-model class via StoresConfig's own field annotations.
    for store_name, field_info in StoresConfig.model_fields.items():
        sub_model_cls = field_info.annotation
        if not (isinstance(sub_model_cls, type) and issubclass(sub_model_cls, BaseModel)):
            continue
        store_val = stores_raw.get(store_name)
        if isinstance(store_val, dict):
            _anchor_entry(store_val, sub_model_cls)

    # `depots` is a MAP of sub-models, not a single one — anchor each value.
    depots_raw = stores_raw.get("depots")
    if isinstance(depots_raw, dict):
        for entry in depots_raw.values():
            if isinstance(entry, dict):
                _anchor_entry(entry, DepotMountConfig)


class StoresConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    observability: RelationalStoreConfig = Field(
        default_factory=lambda: RelationalStoreConfig(path=None),
        description=(
            "Observability store. Contains run records, failure contexts, "
            "healing outcomes, signal overrides, probe signals, module + "
            "maintenance metrics. With postgres backend, tables live in the "
            "`observability` schema of the target DB."
        ),
    )
    depots: dict[str, DepotMountConfig] = Field(
        default_factory=lambda: {
            "default": DepotMountConfig(backend="duckdb", path=".aqueduct/depot.db")},
        description=(
            "Depot KV mounts (`@aq.depot.*`), keyed by name. The implicit "
            "`default` mount (per-blueprint isolated, DuckDB) always exists even "
            "when omitted; add a `default:` entry to re-back it, and other keys as "
            "extra mounts (`@aq.depot.<name>.get()`). Set `shared: true` on a mount "
            "for cross-blueprint (raw-key) sharing. Override a mount with "
            "`--set stores.depots.<name>.backend=…`."
        ),
    )

    def default_depot(self) -> DepotMountConfig:
        """The effective **default** depot mount.

        Returns the ``default`` entry, or an implicit DuckDB default when
        ``depots:`` has none (e.g. ``AqueductConfig()`` built in-memory). Call
        this explicitly, or index ``cfg.stores.depots['default']``.
        """
        d = self.depots.get("default")
        if d is not None:
            return d
        return DepotMountConfig(backend="duckdb", path=".aqueduct/depot.db")

    def effective_depots(self) -> dict[str, DepotMountConfig]:
        """All mounts (name → config) including the implicit default if absent."""
        if "default" in self.depots:
            return dict(self.depots)
        return {"default": DepotMountConfig(backend="duckdb", path=".aqueduct/depot.db"),
                **self.depots}
    blob: ObjectStoreConfig = Field(
        default_factory=ObjectStoreConfig,
        description=(
            "Object store for opaque driver artefacts — observability blobs "
            "(fat manifest/stack/provenance columns) and the patch lifecycle. "
            "`local` (default) keeps the historical on-disk layout; `s3`/`gcs`/"
            "`adls` route through fsspec so cluster pods leave no local files."
        ),
    )
    benchmark: BenchmarkStoreConfig = Field(
        default_factory=BenchmarkStoreConfig,
        description=(
            "Benchmark history store (`aqueduct benchmark`). DuckDB file or "
            "Postgres schema (`benchmark`); disjoint from observability rows."
        ),
    )

    @model_validator(mode="after")
    def _warn_local_blob_under_remote_obs(self) -> StoresConfig:
        """Storage-integrity guardrail: a remote observability backend with an
        IMPLICITLY-local blob store silently writes large payloads (manifests,
        stack traces, provenance) to the driver's local disk — a surprise that can
        fill the driver. Warn ONLY when blob was left at its default (not an
        explicit `local` choice) so users who deliberately keep blobs local are
        never nagged, and unrelated features never trigger it.
        """
        import warnings as _warnings

        from aqueduct import AqueductWarning

        obs_remote = self.observability.backend != "duckdb"
        blob_defaulted_local = (
            self.blob.backend == "local" and "backend" not in self.blob.model_fields_set
        )
        if obs_remote and blob_defaulted_local:
            _warnings.warn(
                f"stores.observability.backend is {self.observability.backend!r} but "
                "stores.blob.backend is unset and defaults to 'local' — externalised "
                "blobs (manifests/stack traces/provenance) will be written to the "
                "DRIVER's local disk, not your remote backend. Set stores.blob.backend "
                "(e.g. s3/gcs/adls) to keep them remote, or set it explicitly to 'local' "
                "to acknowledge and silence this.",
                AqueductWarning,
                stacklevel=2,
            )
        return self


class MetricsConfig(BaseModel):
    """Per-module observability tuning.

    See docs/spark_guide.md ("DataFrame.observe() and Whole-Stage Codegen") for
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

    max_sample_rows: int = Field(default=100, ge=1, description="Maximum rows to sample in probes (>= 1)")
    default_sample_fraction: float = Field(default=0.1, gt=0, le=1, description="Default sampling fraction (0 < x <= 1)")
    # Full probe actions are gated by `danger.allow_full_probe_actions` (inverted polarity).


class DangerConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    allow_full_probe_actions: bool = Field(
        default=False,
        description="Allow full Spark actions in Probes (row_count, freshness). Default false = safe.",
    )
    # `allow_multi_patch` gates the multi-patch loop (max_patches > 1).
    allow_multi_patch: bool = Field(
        default=False,
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
            "Most dangerous; combine with approval: auto + max_patches > 1 "
            "only when you fully trust the model and blueprint scope is tiny."
        ),
    )
    allow_command_hooks: bool = Field(
        default=False,
        description=(
            "Allow `command:` entries in a Blueprint's `hooks:` block (arbitrary "
            "subprocess after on_success/on_failure). The gate lives HERE — in "
            "the operator-owned engine config — so a Blueprint cannot "
            "self-authorize shell execution. `blueprint:` and `webhook:` hook "
            "entries are declarative and never gated. Default false = command "
            "hooks are skipped with a warning."
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


class AgentRetryConfig(BaseModel):
    """Phase 46 — transient-error retry for agent LLM calls.

    Applies to rate-limit/overload responses (HTTP 429, 503, 529) from both
    providers. Sleeps are exponential with jitter, honor a server-sent
    ``Retry-After`` header, and are always capped by the heal budget's
    remaining per-call deadline — a retry can delay an attempt but never
    overrun ``agent.budget.max_seconds``. Shared by production heal and
    ``aqueduct benchmark`` (same provider path).
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    max_retries: int = Field(
        default=2, ge=0,
        description="Extra attempts after the first call on 429/503/529. 0 disables retry.",
    )
    backoff_seconds: float = Field(
        default=2.0, gt=0,
        description="Base backoff; attempt N sleeps ~backoff_seconds * 2^N (+ jitter), capped by the remaining budget deadline.",
    )


class AgentMemoryConfig(BaseModel):
    """Phase 45 signature memory — zero-token heal paths.

    ``replay`` governs both reuse paths consulted before the LLM is called:
    pending-patch reuse (a patch for the same failure signature already
    awaits review → surface it, ``stop_reason: cached``) and exact replay
    (an archived patch already fixed this signature → re-run it through the
    gate pyramid, ``stop_reason: replayed``). ``coaching`` governs
    signature-matched few-shot examples in the heal prompt. Both default on;
    disable ``replay`` when re-running gates (sandbox Spark time) is more
    expensive than fresh LLM tokens.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    replay: bool = Field(
        default=True,
        description="Zero-token reuse of pending/archived patches matching the failure signature.",
    )
    coaching: bool = Field(
        default=True,
        description="Signature-matched (failure → validated fix) few-shot examples in the heal prompt.",
    )


class AgentConnectionConfig(BaseModel):
    """Engine-level LLM connection defaults.

    Sets provider, endpoint, and model used by all blueprints unless overridden
    in the Blueprint agent: block.  Policy fields (approval,
    on_pending_patches, max_patches) belong in the Blueprint, not here.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: Literal["anthropic", "openai_compat"] = "anthropic"
    base_url: str | None = None
    model: str = DEFAULT_LLM_MODEL
    api_key: str | None = Field(
        default=None,
        description=(
            "LLM API key. Optional — falls back to ANTHROPIC_API_KEY / "
            "OPENAI_API_KEY in the environment when unset. Prefer "
            "@aq.secret('NAME') or ${ENV_VAR} over a plaintext literal; "
            "a literal triggers an insecure-config warning and is redacted "
            "from logs and LLM payloads."
        ),
    )
    cascade: list[CascadeTierSchema] | None = Field(
        default=None,
        description=(
            "Engine-wide default multi-model healing cascade. Each tier is tried "
            "in order; cheaper models first, escalate on stuck/exhausted/deferred. "
            "A blueprint's own agent.cascade (or model: [list] shorthand) fully "
            "overrides this default. The engine cascade does NOT support the "
            "model: [list] shorthand — use an explicit list of tiers."
        ),
    )
    provider_options: dict[str, Any] | None = None
    timeout: float = Field(
        default=300.0,
        gt=0,
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
        ge=1,
        description="Max reprompt attempts when the agent returns invalid PatchSpec JSON.",
    )
    prompt_context: str | None = Field(
        default=None,
        description="Extra context appended to the LLM system prompt for all blueprints. Use for cluster constraints, naming conventions, schema hints.",
    )
    ci_webhook_url: str | None = Field(
        default=None,
        description="Webhook URL for approval: ci. POST target for external CI to create PR.",
    )
    max_heal_attempts_per_hour: int | None = Field(
        default=None,
        ge=1,
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
    sandbox_master_url: str | None = Field(
        default=None,
        description=(
            "Spark master URL for sandbox replay (patch preview). When None "
            "(default), sandbox uses the default 'local[*]' — 1000 sampled rows "
            "fit in any driver's memory. Set to 'spark://host:7077' or 'yarn' "
            "to run sandbox on a cluster when your blueprint is too large for "
            "a single driver node."
        ),
    )
    memory: AgentMemoryConfig = Field(
        default_factory=AgentMemoryConfig,
        description=(
            "Phase 45 signature memory: zero-token patch reuse/replay and "
            "signature-matched coaching. Both sub-flags default on."
        ),
    )
    retry: AgentRetryConfig = Field(
        default_factory=AgentRetryConfig,
        description=(
            "Phase 46 — retry on transient provider errors (429/503/529) with "
            "exponential backoff, capped by the heal budget deadline."
        ),
    )
    regression_artifact: bool = Field(
        default=False,
        description=(
            "When True, a SUCCESSFUL heal (patch applied AND the re-run "
            "succeeded) optionally emits an `.aqtest.yml` regression test for "
            "the patched module under `aqtests/` next to the Blueprint, so a "
            "later hand-edit/SQL refactor/revert that reintroduces the root "
            "cause is caught by `aqueduct test` before the next production "
            "run instead of by another heal. Complementary to signature-"
            "memory heal-cache replay, not a replacement for it: the cache "
            "resolves a RECURRING failure for zero tokens; this artifact is "
            "a CI guard against the FIX being undone. Generation is "
            "conservative — it silently no-ops (info log only) whenever the "
            "healed failure/module shape can't be expressed as a valid "
            "aqtest (e.g. non-testable module type, no upstream schema_hint "
            "to synthesize fixture rows from, or a multi-module patch). "
            "Default False. Per-blueprint override: agent.regression_artifact."
        ),
    )
    mode: Literal["oneshot", "agentic"] = Field(
        default="oneshot",
        description=(
            "Phase 75 — 'oneshot' (default) is today's single-turn prompt→"
            "PatchSpec loop, unchanged. 'agentic' lets the model call "
            "read-only diagnostic tools (list_runs, run_detail, lineage, "
            "patch_list/show, probe_signals, blueprint_history, "
            "read_blueprint, get_source_schema, sample_rows) before "
            "answering with a PatchSpec — see docs/specs.md §8.11. "
            "Per-blueprint override: agent.mode."
        ),
    )
    max_tool_calls: int = Field(
        default=8,
        ge=1,
        description=(
            "Hard cap on tool calls per heal attempt in agentic mode. "
            "Exceeding it forces the model to answer with a PatchSpec on a "
            "final no-tools turn. Per-blueprint override: agent.max_tool_calls."
        ),
    )
    supports_tools: Literal["auto", True, False] = Field(
        default="auto",
        description=(
            "Tool-use capability for the LLM endpoint. 'auto' (default) "
            "resolves True without probing for provider: anthropic (known "
            "tool-capable); for provider: openai_compat it is determined on "
            "the first agentic-mode call (a rejected/ignored tools request "
            "degrades to the oneshot path for the rest of the heal session, "
            "emitting one [agent_tools_unsupported] warning). Set true/false "
            "to skip the probe. Per-blueprint / per-cascade-tier override: "
            "agent.supports_tools / cascade tier supports_tools."
        ),
    )
    progressive: bool = Field(
        default=False,
        description=(
            "Progressive (chained) multi-patch healing. Opt-in, separate "
            "from `max_patches` (whose semantics stay UNCHANGED — N "
            "independent retries of the SAME failure). When True, a "
            "candidate patch that passes its own validation but still "
            "leaves the pipeline failing is NOT discarded if the new "
            "failure surfaces at a DIFFERENT module than the one just "
            "patched — it is folded into an accumulating multi-op patch "
            "and the next link diagnoses the new failure. Same module "
            "again = stuck, chain ends. Nothing is written to the "
            "Blueprint until the full accumulated patch passes the "
            "pipeline end-to-end — see docs/specs.md §8.13. Requires "
            "`agent.sandbox_mode` other than 'off' (refused at heal-start "
            "otherwise — each link's advancement test IS the sandbox "
            "gate). Default False. Per-blueprint override: agent.progressive."
        ),
    )
    max_chain: int = Field(
        default=3,
        ge=1,
        description=(
            "Hard cap on the number of links in a progressive healing "
            "chain — independent of each link's own reprompt/budget "
            "ceiling (`max_reprompts`/`budget`). Only meaningful when "
            "`agent.progressive: true`. Default 3. Per-blueprint override: "
            "agent.max_chain."
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
    timeout: int = Field(default=10, ge=1, description="HTTP socket timeout in seconds (>= 1)")
    secret: str | None = Field(
        default=None,
        description=(
            "HMAC-SHA256 signing secret. When set, an "
            "'X-Aqueduct-Signature: sha256=<digest>' header is added so the "
            "receiver can verify payload authenticity and integrity. The digest "
            "is computed over the exact request body. May contain ${ENV_VAR} tokens."
        ),
    )
    max_retries: int = Field(
        default=1,
        ge=0,
        description=(
            "Retries beyond the first attempt on transient failures "
            "(429/500/502/503/504 or network errors). 0 disables retry."
        ),
    )
    backoff_seconds: float = Field(
        default=2.0,
        gt=0,
        description="Base backoff between retries; exponential (base * 2**(n-1)) when max_retries > 1.",
    )
    health_probe: Literal["connect", "options", "full"] = Field(
        default="options",
        description=(
            "How `aqueduct doctor` probes this endpoint. 'connect' = TCP/TLS "
            "reachability only, no HTTP request sent. 'options' (default) = "
            "an HTTP OPTIONS request; any response (including 405) counts as "
            "reachable. 'full' = the old behaviour — a real POST/PUT/PATCH "
            "with a synthetic 'doctor_probe' payload, which can trigger "
            "consumer-side side effects on endpoints that react to any "
            "request body."
        ),
    )


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
        description="Endpoint for approval: ci — receives patch JSON for external PR creation.",
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


class LineageConfig(BaseModel):
    """Phase 55 — OpenLineage emission.

    Top-level `lineage:` block. **Naming note:** this is the OpenLineage
    *emission* config — unrelated to the former `stores.lineage` store, which was
    removed (column lineage merged into the observability store). This block
    configures *emission* of OpenLineage run events.

    When `openlineage_url` is unset (default), no events are emitted — zero cost.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    openlineage_url: str | None = Field(
        default=None,
        description="OpenLineage receiver endpoint (Marquez / DataHub / Atlan). "
                    "POST target for run events. Unset → emission disabled.",
    )
    openlineage_namespace: str = Field(
        default="aqueduct",
        description="OpenLineage namespace for jobs and datasets emitted by this engine.",
    )


class AqueductConfig(BaseModel):
    """Fully validated engine configuration.

    All fields have sensible defaults so that running without an aqueduct.yml
    (development / CI) works out of the box.

    LLM connection defaults (provider, base_url, model) live here.
    Per-blueprint policy (approval) lives in the Blueprint agent: block.
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
    lineage: LineageConfig = Field(default_factory=LineageConfig)
    agent: AgentConnectionConfig = Field(default_factory=AgentConnectionConfig)
    warnings: WarningsConfig = Field(default_factory=lambda: WarningsConfig())
    checkpoint_root: str | None = Field(
        default=None,
        description=(
            "Local filesystem path overriding the derived "
            "<store_dir>/checkpoints/ location for module checkpoint/resume "
            "state. LOCAL PATHS ONLY — remote URI schemes (s3://, s3a://, "
            "gs://, hdfs://, abfss://, ...) are rejected at config-load; see "
            "docs/roadmap.md 'Remote-Filesystem Checkpoint Root'."
        ),
    )
    spark_config: dict[str, Any] = Field(
        default_factory=dict,
        description="Engine-level Spark conf merged with Blueprint spark_config (Blueprint wins)",
    )

    @field_validator("checkpoint_root")
    @classmethod
    def _validate_checkpoint_root(cls, v: str | None) -> str | None:
        if v is None or not v:
            return v
        if _URI_SCHEME_RE.match(v):
            scheme = v.split("://", 1)[0]
            raise ValueError(
                f"checkpoint_root={v!r} uses a remote URI scheme ({scheme!r}://) "
                "— checkpoint_root only supports local filesystem paths today. "
                "Remote checkpoint roots (S3/GCS/HDFS/ABFSS) are tracked as a "
                "roadmap item ('Remote-Filesystem Checkpoint Root' in "
                "docs/roadmap.md). Use a local path, or omit checkpoint_root "
                "to fall back to the derived <store_dir>/checkpoints/ default."
            )
        return v


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

_OBJECT_BACKEND_REQUIREMENTS: dict[str, tuple[str, str]] = {
    "s3":   ("s3fs",  "pip install aqueduct-core[object-store]"),
    "gcs":  ("gcsfs", "pip install aqueduct-core[object-store]"),
    "adls": ("adlfs", "pip install aqueduct-core[object-store]"),
}


def _validate_store_backends(stores_cfg: StoresConfig) -> None:
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
        ("depot",         stores_cfg.default_depot().backend),
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

    # Object store backends (s3 / gcs / adls) are validated separately because
    # they need different SDKs (s3fs / gcsfs / adlfs) than the relational/KV stores.
    blob_backend = stores_cfg.blob.backend
    if blob_backend != "local" and blob_backend not in seen:
        seen.add(blob_backend)
        if blob_backend in _OBJECT_BACKEND_REQUIREMENTS:
            module_name, install_hint = _OBJECT_BACKEND_REQUIREMENTS[blob_backend]
            root_pkg = module_name.split(".")[0]
            if _import_util.find_spec(root_pkg) is None:
                raise ConfigError(
                    f"stores.blob.backend={blob_backend!r} "
                    f"requires the {root_pkg!r} package, which is not installed.\n"
                    f"Install it with:  {install_hint}"
                )


def _validate_secrets_backend(secrets_cfg: SecretsConfig) -> None:
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


def _expand_secrets(
    text: str, secrets_cfg: SecretsConfig, base_dir: str | None = None
) -> tuple[str, list[str]]:
    """Second pass — resolve ``@aq.secret('KEY')`` via the configured provider.

    Registers every resolved value with ``aqueduct.redaction`` so it can be
    scrubbed from downstream outputs (logs, observability rows, patch sidecar
    files, webhook bodies, LLM request payloads).

    Returns ``(expanded_text, missing_secrets)``. ``missing_secrets`` collects
    keys the provider did not find — caller raises ``ConfigError``.
    """
    from aqueduct import redaction
    from aqueduct.secrets import SecretsError, resolve_secret

    missing: list[str] = []

    def _replace_secret(m: re.Match) -> str:
        key = m.group(1)
        try:
            val = resolve_secret(
                key,
                provider=secrets_cfg.provider,
                region=secrets_cfg.region,
                resolver=secrets_cfg.resolver,
                base_dir=base_dir,
            )
        except SecretsError:
            missing.append(f"@aq.secret('{key}')")
            return ""
        redaction.register(val, key_hint=key)
        return val

    return _AQ_SECRET_RE.sub(_replace_secret, text), missing


def _format_config_error(resolved: Path, exc: ValidationError) -> str:
    from aqueduct.utils import format_error_loc

    errors = exc.errors(include_url=False)
    n = len(errors)
    header = f"{n} validation error{'s' if n != 1 else ''} in {resolved}:"
    lines = [header]
    for e in errors:
        loc_str = format_error_loc(e["loc"])
        lines.append(f"  • {loc_str} — {e['msg']}")
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


    # ── Agent API key insecure-literal check ──────────────────────────────────
    # Inspect the raw pre-expansion text so we can distinguish
    # @aq.secret('X') / ${X} from plaintext. By the time the value reaches
    # AgentConnectionConfig it is already resolved; only the raw text tells us
    # whether the user baked a literal key into the file.
    try:
        _raw_data = yaml.safe_load(raw_text)
        if isinstance(_raw_data, dict):
            _raw_agent = _raw_data.get("agent") or {}
            _raw_key = _raw_agent.get("api_key") if isinstance(_raw_agent, dict) else None
            if isinstance(_raw_key, str) and _raw_key.strip() and not _raw_key.startswith("@aq.secret(") and "${" not in _raw_key:
                import warnings as _warnings

                from aqueduct.warnings import AqueductWarning
                _warnings.warn(
                    f"[aqueduct:insecure_api_key] agent.api_key is a plaintext literal in "
                    f"{resolved} — prefer @aq.secret('NAME') or ${{ENV_VAR}}. The value is "
                    "redacted from logs and LLM payloads, but a literal in a committed config "
                    "is a credential leak risk.",
                    AqueductWarning,
                    stacklevel=2,
                )
    except Exception:
        pass  # best-effort; real parse errors surface later

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

    # Guard: only @aq.secret() resolves in aqueduct.yml. Any other @aq.* token
    # (run / blueprint / deployment / date / depot / version) needs a Blueprint
    # + run context that does not exist at config-load time (override-downstream,
    # not propagate-uphill — see specs §5.3.1). Reject with a clear pointer
    # instead of letting an unresolved literal leak into a config value.
    _bad_aq = sorted({
        f"@aq.{m.group(1)}"
        for m in re.finditer(r"@aq\.(?!secret\b)([a-zA-Z_][\w.]*)", pass1_text)
    })
    if _bad_aq:
        raise ConfigError(
            f"{resolved}: {', '.join(_bad_aq)} cannot be used in aqueduct.yml — only "
            "@aq.secret(...) and ${ENV} resolve in the engine config. The "
            "run / blueprint / deployment scopes don't exist until a Blueprint "
            "compiles; move these into the Blueprint instead."
        )

    # ── Pass 2: resolve @aq.secret() via the configured provider ──────────────
    # Short-circuit when the file has no @aq.secret() tokens — keeps the common
    # case (no secrets) at one YAML parse and one validation.
    if not _AQ_SECRET_RE.search(pass1_text):
        _validate_store_backends(cfg_pass1.stores)
        return cfg_pass1

    pass2_text, missing_secrets = _expand_secrets(pass1_text, cfg_pass1.secrets, base_dir=str(_cfg_dir))
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
