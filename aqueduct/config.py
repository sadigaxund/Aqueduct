"""Project-level engine configuration — aqueduct.yml loader.

aqueduct.yml is separate from Blueprint YAML files.  It configures the engine
itself: deployment target, store backends, probe limits, secrets provider, and
webhook endpoints.  It is NOT the blueprint definition.

LLM agent CONNECTION config (provider, base_url, api_key, model,
provider_options, timeout, cascade) lives here ONLY — a Blueprint's agent:
block cannot set or override any of these (AgentSchema in
aqueduct/parser/schema.py has no such fields; extra="forbid" rejects one
named). Per-blueprint POLICY (approval, on_pending_patches, max_patches,
guardrails, ...) lives in the Blueprint agent: block; a shared subset of
policy fields (max_reprompts, mode, max_tool_calls, supports_tools,
progressive, max_chain, prompt_context, max_heal_attempts_per_hour,
patch_validation, block_on_explain_regression, regression_artifact) is
ALSO settable here as the engine-wide default, and the Blueprint's own
value, when set, wins — see aqueduct.cli.resolve_agent_connection. See
AgentConnectionConfig's docstring below for the full rationale.

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
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    ValidationError,
    field_validator,
    model_validator,
)

from aqueduct.agent.constants import DEFAULT_LLM_MODEL
from aqueduct.errors import ConfigError
from aqueduct.parser.fs_path import FsPath, field_is_fs_path
from aqueduct.parser.schema import AgentPolicySchema, CascadeTierSchema

DEFAULT_OBS_DB_FILENAME: str = "observability.db"

# The default observability ROUTING directory (not a `.db` file — config load
# rejects `.db`-suffixed `stores.observability.path` values; per-blueprint
# files always live at `<this>/<blueprint_id>/observability.db`). Single
# source of truth for the three call sites that previously each hardcoded
# this literal independently: `aqueduct/cli/run.py` (`_obs_routing_base`),
# `aqueduct/stores/queries.py` (`_DEFAULT_OBS_ROOT`), and
# `aqueduct/stores/read.py` (`_OBS_ROUTING_ROOT`). Pure DRY — no behavior
# change; the value is unchanged.
DEFAULT_OBS_ROUTING_ROOT: str = ".aqueduct/observability"

# Default root for cross-engine handoff spill files (Phase 81 step 2's
# `handoff:` block, `handoff.root`). Unlike `checkpoint_root` (LOCAL
# filesystem only — a same-engine Spark resume concept), a handoff spill
# must be reachable by BOTH engines on either side of an island boundary,
# so this default is just a relative directory convention, not a
# local-only guarantee — `handoff.root` accepts a remote URI scheme
# (s3://, gs://, abfss://, ...) with no rejection.
DEFAULT_HANDOFF_ROOT: str = ".aqueduct/handoff"

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


class DeploymentConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    engine: str = Field(
        default="spark",
        description=(
            "Execution engine. Validated against the engines registered "
            "through the aqueduct.engines entry-point group (see "
            "docs/specs.md §10.9) — today just spark."
        ),
        json_schema_extra={"engine_scoped": False},
    )
    target: Literal["local", "standalone", "yarn", "kubernetes", "emr", "dataproc"] = Field(
        default="local",
        description="Deployment target: local | standalone | yarn | kubernetes | emr | dataproc",
        json_schema_extra={"engine_scoped": True},
    )
    env: Literal["local", "cluster", "cloud"] = Field(
        default="local",
        description="Deployment environment tier. Doctor warns on local paths in cluster/cloud mode.",
        json_schema_extra={"engine_scoped": False},
    )

    @model_validator(mode="after")
    def _validate_engine(self) -> DeploymentConfig:
        """Validate ``engine`` against the registered ``aqueduct.engines`` set.

        ``engine`` is checked against the engines actually registered through
        the ``aqueduct.engines`` entry-point group (Phase 78 Step 1,
        ``aqueduct/executor/capabilities.py::load_engines``) rather than a
        fixed ``Literal`` — a new engine (e.g. DuckDB) becomes valid the
        moment it ships its entry point, with no edit here. An unregistered
        name (a typo, or an engine whose extra isn't installed) is a
        ``ConfigError`` naming the registered engines, not a bare pydantic
        ``ValidationError``.

        The EMPTY-registry state gets its own message. Engine validation is
        fail-closed, so a stale install (one whose dist-info predates the
        entry-point declaration) would otherwise hard-fail every
        ``aqueduct.yml`` load with an alarming, misleading "Registered
        engines: []". That is a packaging problem, not a config problem, and
        says so: reinstall.

        Target/``engine.spark.master_url`` consistency (2.0 — master_url
        moved off this block, see ``SparkEngineConfig``) is checked at
        ``AqueductConfig`` level (``_validate_target_master_url`` below),
        since it needs both this block AND the ``engine:`` block; this
        validator only owns engine-name registration, which needs neither.
        """
        from aqueduct.executor.capabilities import (
            CAPABILITY_REGISTRY,
            NO_ENGINES_HINT,
            load_engines,
        )

        # EnginePluginError (a broken third-party engine plugin) and
        # CapabilityDeclarationError (an engine's capabilities.yml is
        # incomplete/invalid) are both AqueductErrors and deliberately
        # propagate as-is — each names its own culprit (the failing entry
        # point / the undeclared leaves) and carries its own fix, which is
        # more actionable than anything this layer could add. They are
        # distinguished by TYPE, never by message text.
        load_engines()
        if self.engine not in CAPABILITY_REGISTRY:
            registered = sorted(CAPABILITY_REGISTRY)
            if not registered:
                raise ConfigError(
                    f"cannot validate deployment.engine={self.engine!r}: {NO_ENGINES_HINT}"
                )
            raise ConfigError(
                f"deployment.engine={self.engine!r} is not a registered engine. "
                f"Registered engines: {registered}. Install the "
                "matching extra (e.g. aqueduct-core[spark]) or fix the typo."
            )

        if self.engine != "spark":
            return self

        if self.target in ("emr", "dataproc"):
            raise ConfigError(
                f"deployment.target={self.target!r} is a remote-submit target "
                f"not yet supported. "
                f"Use local | standalone | yarn | kubernetes."
            )

        return self


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
        json_schema_extra={"engine_scoped": False},
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
        json_schema_extra={"engine_scoped": False},
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
            "DuckDB: local file path. Postgres: libpq DSN. Redis: `redis://host:port/db` URL."
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
        json_schema_extra={"engine_scoped": False},
    )
    path: Annotated[str, FsPath()] = Field(
        default="",
        description=(
            "Object-store location. `local`: base directory (empty → per-store "
            "default: blobs under the observability dir, patches under cwd). "
            "`s3` / `gcs` / `adls`: base URI such as `s3://bucket/aqueduct`."
        ),
        json_schema_extra={"engine_scoped": False},
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
        json_schema_extra={"engine_scoped": False},
    )
    path: str | None = Field(
        default=None,
        description=(
            "DuckDB: file path (default: scenario-anchored "
            "`.aqueduct/benchmark.duckdb`). Postgres: libpq DSN. Not FsPath-"
            "anchored — the CLI resolves the scenario-relative default."
        ),
        json_schema_extra={"engine_scoped": False},
    )
    persist: bool = Field(
        default=True,
        description="Write each (scenario, model) row to the store. False = the old `--no-persist`.",
        json_schema_extra={"engine_scoped": False},
    )
    gate_on_regression: bool = Field(
        default=False,
        description=(
            "After persisting, diff each (scenario, model) vs its baseline and "
            "exit non-zero on any regression. The old `--gate-on-regression`."
        ),
        json_schema_extra={"engine_scoped": False},
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
            "default": DepotMountConfig(backend="duckdb", path=".aqueduct/depot.db")
        },
        description=(
            "Depot KV mounts (`@aq.depot.*`), keyed by name. The implicit "
            "`default` mount (per-blueprint isolated, DuckDB) always exists even "
            "when omitted; add a `default:` entry to re-back it, and other keys as "
            "extra mounts (`@aq.depot.<name>.get()`). Set `shared: true` on a mount "
            "for cross-blueprint (raw-key) sharing. Override a mount with "
            "`--set stores.depots.<name>.backend=…`."
        ),
        json_schema_extra={"engine_scoped": False},
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
        return {
            "default": DepotMountConfig(backend="duckdb", path=".aqueduct/depot.db"),
            **self.depots,
        }

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
        obs_remote = self.observability.backend != "duckdb"
        blob_defaulted_local = (
            self.blob.backend == "local" and "backend" not in self.blob.model_fields_set
        )
        if obs_remote and blob_defaulted_local:
            # Routed through aqueduct.warnings.emit (not a raw warnings.warn)
            # so the message carries a real rule_id and is suppressible via
            # `--suppress-warning stores_blob_local_under_remote_obs` (only
            # `emit()`'s AQ-WARN machinery checks the suppress set — a raw
            # warnings.warn call was invisible to it). This validator runs
            # on StoresConfig alone, before the enclosing AqueductConfig's
            # sibling `warnings:` block exists to read a per-file suppress
            # list from, so the CLI-flag / process-global suppress path is
            # what this rule_id actually gets — setting stores.blob.backend
            # explicitly to "local" remains the direct way to silence this
            # validator's own precondition, independent of rule_id
            # suppression.
            from aqueduct.warnings import emit as _emit

            _emit(
                "stores_blob_local_under_remote_obs",
                f"stores.observability.backend is {self.observability.backend!r} but "
                "stores.blob.backend is unset and defaults to 'local' — externalised "
                "blobs (manifests/stack traces/provenance) will be written to the "
                "DRIVER's local disk, not your remote backend. Set stores.blob.backend "
                "(e.g. s3/gcs/adls) to keep them remote, or set it explicitly to 'local' "
                "to acknowledge this and stop the warning.",
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
        json_schema_extra={"engine_scoped": True},
    )


class ProbesConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    max_sample_rows: int = Field(
        default=100,
        ge=1,
        description="Maximum rows to sample in probes (>= 1)",
        json_schema_extra={"engine_scoped": True},
    )
    default_sample_fraction: float = Field(
        default=0.1,
        gt=0,
        le=1,
        description="Default sampling fraction (0 < x <= 1)",
        json_schema_extra={"engine_scoped": True},
    )
    # Full probe actions are gated by `danger.allow_full_probe_actions` (inverted polarity).


class ObservabilityRetentionConfig(BaseModel):
    """Phase 85 B1 — per-table retention windows for the observability store.

    Core (never engine-scoped): pruning is a store-level housekeeping
    concern, identical regardless of which engine produced the rows —
    exactly the same reasoning `webhooks`/`secrets`/`stores` already use
    (see ``aqueduct/executor/config_leaves.py``'s Q4 scoping note). Every
    field below is tagged ``engine_scoped: False`` accordingly.

    Applied by ``aqueduct.surveyor.retention.prune_store()``, called
    throttled (at most once per day per store, via a ``store_maintenance``
    marker row) at the end of every run, and by the explicit
    ``aqueduct report-prune`` CLI verb for an on-demand deep clean.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    run_records_days: int = Field(
        default=90,
        ge=1,
        description="Delete run_records rows older than this many days.",
        json_schema_extra={"engine_scoped": False},
    )
    failure_contexts_days: int = Field(
        default=90,
        ge=1,
        description="Delete failure_contexts rows older than this many days.",
        json_schema_extra={"engine_scoped": False},
    )
    healing_outcomes_days: int = Field(
        default=180,
        ge=1,
        description="Delete healing_outcomes rows older than this many days.",
        json_schema_extra={"engine_scoped": False},
    )
    heal_attempts_days: int = Field(
        default=180,
        ge=1,
        description="Delete heal_attempts rows older than this many days.",
        json_schema_extra={"engine_scoped": False},
    )
    patch_simulation_days: int = Field(
        default=90,
        ge=1,
        description="Delete patch_simulation rows older than this many days.",
        json_schema_extra={"engine_scoped": False},
    )
    column_lineage_days: int = Field(
        default=90,
        ge=1,
        description="Delete column_lineage rows older than this many days.",
        json_schema_extra={"engine_scoped": False},
    )
    probe_signals_days: int = Field(
        default=90,
        ge=1,
        description=(
            "Delete probe_signals rows older than this many days (all "
            "signal types — an age-based backstop). sample_rows also gets "
            "its own tighter per-probe cap below, applied at write time."
        ),
        json_schema_extra={"engine_scoped": False},
    )
    sample_rows_keep_last_n: int = Field(
        default=20,
        ge=1,
        description=(
            "Per-probe retention cap for the sample_rows Probe signal — "
            "keeps only the most recent N sample_rows rows per probe_id, "
            "enforced at write time regardless of probe_signals_days above. "
            "sample_rows is the one signal type that persists real data "
            "rows (redacted, but still row content), so it gets a tighter, "
            "count-based cap on top of the age-based one."
        ),
        json_schema_extra={"engine_scoped": False},
    )


class ObservabilityConfig(BaseModel):
    """Phase 85 B1 — observability-store housekeeping (retention/pruning)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    retention: ObservabilityRetentionConfig = Field(default_factory=ObservabilityRetentionConfig)


class DangerConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    allow_full_probe_actions: bool = Field(
        default=False,
        description="Allow full Spark actions in Probes (row_count, freshness). Default false = safe.",
        json_schema_extra={"engine_scoped": True},
    )
    # `allow_multi_patch` gates the multi-patch loop (max_patches > 1).
    allow_multi_patch: bool = Field(
        default=False,
        description=(
            "Allow `max_patches > 1` — the multi-patch reprompt loop. Auto-applies "
            "successive LLM patches without human review until success or budget exhaustion."
        ),
        json_schema_extra={"engine_scoped": False},
    )
    allow_full_preflight: bool = Field(
        default=False,
        description=(
            "Allow agent.sandbox_mode: preflight — full-dataset sandbox replay "
            "(no Egress writes). Slow but conclusive. Default false = safe."
        ),
        json_schema_extra={"engine_scoped": False},
    )
    allow_skip_sandbox: bool = Field(
        default=False,
        description=(
            "Allow agent.sandbox_mode: off — skip sandbox replay entirely; "
            "patches go straight to production data via the next execute(). "
            "Most dangerous; combine with approval: auto + max_patches > 1 "
            "only when you fully trust the model and blueprint scope is tiny."
        ),
        json_schema_extra={"engine_scoped": False},
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
        json_schema_extra={"engine_scoped": False},
    )


class SecretsConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: str = Field(
        default="env",
        description="Secrets provider: env | aws | gcp | azure | custom",
        json_schema_extra={"engine_scoped": False},
    )
    region: str | None = Field(
        default=None,
        description="Cloud region for aws/gcp/azure providers (e.g. 'us-east-1').",
        json_schema_extra={"engine_scoped": False},
    )
    resolver: str | None = Field(
        default=None,
        description="Dotted import path for custom provider: 'mypackage.secrets.fetch'. "
        "Callable signature: (key: str) -> str | None. "
        "Return None to fall back to os.environ.",
        json_schema_extra={"engine_scoped": False},
    )


class AgentBudgetConfig(BaseModel):
    """Phase 34 multi-axis heal-loop budget. All fields optional with safe defaults.

    Same instance is used by production heal AND benchmark — divergence would
    silently invalidate the leaderboard. Only ``max_reprompts`` + ``max_seconds``
    are recommended for routine tuning; the rest are advanced axes documented in
    ``aqueduct.agent.budget``.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    max_reprompts: int = Field(default=5, json_schema_extra={"engine_scoped": False})
    max_seconds: float = Field(default=120.0, json_schema_extra={"engine_scoped": False})
    max_tokens_total: int | None = Field(default=50_000, json_schema_extra={"engine_scoped": False})
    same_error_consecutive: int = Field(default=2, json_schema_extra={"engine_scoped": False})
    same_signature_overall: int = Field(default=3, json_schema_extra={"engine_scoped": False})
    progress_stalled_window: int = Field(default=3, json_schema_extra={"engine_scoped": False})


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
        default=2,
        ge=0,
        description="Extra attempts after the first call on 429/503/529. 0 disables retry.",
        json_schema_extra={"engine_scoped": False},
    )
    backoff_seconds: float = Field(
        default=2.0,
        gt=0,
        description="Base backoff; attempt N sleeps ~backoff_seconds * 2^N (+ jitter), capped by the remaining budget deadline.",
        json_schema_extra={"engine_scoped": False},
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
        json_schema_extra={"engine_scoped": False},
    )
    coaching: bool = Field(
        default=True,
        description="Signature-matched (failure → validated fix) few-shot examples in the heal prompt.",
        json_schema_extra={"engine_scoped": False},
    )


class AgentConnectionConfig(AgentPolicySchema):
    """Engine-level LLM agent config — the ONLY place CONNECTION fields
    (``provider``/``base_url``/``api_key``/``model``/``provider_options``/
    ``timeout``/``cascade``) are legal. A Blueprint's ``agent:`` block
    (``AgentSchema``, ``aqueduct/parser/schema.py``) cannot set or override
    any of them — see that class's docstring for why (a Blueprint that could
    choose its own LLM endpoint could redirect the healing loop's
    ``FailureContext``, which carries pruned manifest/provenance/error text
    and, in agentic mode, sampled data rows, to an arbitrary host on any
    failure).

    Extends ``AgentPolicySchema`` for the POLICY fields it shares by name
    with the Blueprint (``max_reprompts``, ``mode``, ``max_tool_calls``,
    ``supports_tools``, ``progressive``, ``max_chain``, ``prompt_context``,
    ``max_heal_attempts_per_hour``, ``patch_validation``,
    ``block_on_explain_regression``, ``regression_artifact``) — each
    overridden below with this level's concrete engine-wide default (the
    base class's default is ``None``, meaning "inherit", which has no
    meaning at the level that IS the thing being inherited from). A
    Blueprint's own value for one of these, when set, still wins — see
    ``aqueduct.cli.resolve_agent_connection``.

    Policy fields with NO engine-level equivalent (``approval``,
    ``on_pending_patches``, ``max_patches``, ``guardrails``,
    ``confidence_threshold``, ``on_heal_failure``, ``allow_defer``,
    ``deep_loop``, ``sandbox_mode``) are Blueprint-only by design — they are
    risk decisions about ONE pipeline, and there is no coherent "engine-wide
    default approval mode" for a fleet of otherwise-unrelated Blueprints; a
    field here with nothing to resolve it would be a silent no-op (AGENTS.md).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: Literal["anthropic", "openai_compat"] = Field(
        default="anthropic",
        json_schema_extra={"engine_scoped": False},
    )
    base_url: str | None = Field(default=None, json_schema_extra={"engine_scoped": False})
    model: str = Field(default=DEFAULT_LLM_MODEL, json_schema_extra={"engine_scoped": False})
    api_key: str | None = Field(
        default=None,
        description=(
            "LLM API key. Optional — falls back to ANTHROPIC_API_KEY / "
            "OPENAI_API_KEY in the environment when unset. Prefer "
            "@aq.secret('NAME') or ${ENV_VAR} over a plaintext literal; "
            "a literal triggers an insecure-config warning and is redacted "
            "from logs and LLM payloads."
        ),
        json_schema_extra={"engine_scoped": False},
    )
    cascade: list[CascadeTierSchema] | None = Field(
        default=None,
        description=(
            "Multi-model healing cascade. Each tier is tried in order; "
            "cheaper models first, escalate on stuck/exhausted/deferred. "
            "CONNECTION field — engine-level ONLY (2.59): a Blueprint cannot "
            "declare its own cascade or override this one (AgentSchema has "
            "no cascade field). The former `model: [list]` shorthand was "
            "Blueprint-only and was removed along with the Blueprint's "
            "`model`/`cascade` fields — always write an explicit list of "
            "tiers here."
        ),
        json_schema_extra={"engine_scoped": False},
    )
    provider_options: dict[str, Any] | None = Field(
        default=None,
        json_schema_extra={"engine_scoped": False},
    )
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
        json_schema_extra={"engine_scoped": False},
    )
    max_reprompts: int = Field(
        default=3,
        ge=1,
        description="Max reprompt attempts when the agent returns invalid PatchSpec JSON.",
        json_schema_extra={"engine_scoped": False},
    )
    prompt_context: str | None = Field(
        default=None,
        description="Extra context appended to the LLM system prompt for all blueprints. Use for cluster constraints, naming conventions, schema hints.",
        json_schema_extra={"engine_scoped": False},
    )
    ci_webhook_url: str | None = Field(
        default=None,
        description="Webhook URL for approval: ci. POST target for external CI to create PR.",
        json_schema_extra={"engine_scoped": False},
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
        json_schema_extra={"engine_scoped": False},
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
        json_schema_extra={"engine_scoped": False},
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
        json_schema_extra={"engine_scoped": True},
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
        json_schema_extra={"engine_scoped": True},
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
        json_schema_extra={"engine_scoped": False},
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
        json_schema_extra={"engine_scoped": False},
    )
    max_tool_calls: int = Field(
        default=8,
        ge=1,
        description=(
            "Hard cap on tool calls per heal attempt in agentic mode. "
            "Exceeding it forces the model to answer with a PatchSpec on a "
            "final no-tools turn. Per-blueprint override: agent.max_tool_calls."
        ),
        json_schema_extra={"engine_scoped": False},
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
        json_schema_extra={"engine_scoped": False},
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
        json_schema_extra={"engine_scoped": False},
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
        json_schema_extra={"engine_scoped": False},
    )


class GitConfig(BaseModel):
    """Phase 87 — mechanics shared by every git-writing flow (``patch import``,
    ``patch commit``, ``patch rollback``, and the new ``patch pr``).

    Connection-level only: which repository/remote a git-writing command is
    allowed to act on. No Blueprint influence — see ``PrConfig`` for why.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    expected_root: str | None = Field(
        default=None,
        description=(
            "Pin the git work-tree root a patch command is allowed to write "
            "into. When unset, the resolved `git rev-parse --show-toplevel` "
            "must equal the project root (the aqueduct.yml directory); a "
            "mismatch (project nested in a larger repo, or a stray .git "
            "between the project root and the blueprint) hard-refuses. Set "
            "this to the intended repo root to make a monorepo layout "
            "explicit instead of refusing it."
        ),
        json_schema_extra={"engine_scoped": False},
    )
    remote: str = Field(
        default="origin",
        description="git remote `patch pr` pushes the heal branch to.",
        json_schema_extra={"engine_scoped": False},
    )


class PrConfig(BaseModel):
    """Phase 87 — ``aqueduct patch pr`` presentation.

    Connection-level PR mechanics only: base branch, title, draft state. No
    labels or reviewers field, and no Blueprint influence, ever: reviewer
    routing belongs to repo-level CODEOWNERS / branch protection. A healed
    pipeline, potentially steered by an injected upstream dataset, must
    never influence who reviews its own fix or how the PR is classified.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    base_branch: str = Field(
        default="main",
        description="Base branch `gh pr create --base` targets.",
        json_schema_extra={"engine_scoped": False},
    )
    title_template: str = Field(
        default="aqueduct heal: {blueprint_id} ({module}) [{patch_id}]",
        description=(
            "PR title template. Available tokens: {patch_id}, "
            "{blueprint_id}, {module} (the healed run's failed_module, or "
            "'unknown' when the patch carries none)."
        ),
        json_schema_extra={"engine_scoped": False},
    )
    draft: bool = Field(
        default=False,
        description="Open the PR as a draft (`gh pr create --draft`).",
        json_schema_extra={"engine_scoped": False},
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

    url: str = Field(
        ...,
        description="HTTP(S) endpoint URL",
        json_schema_extra={"engine_scoped": False},
    )
    method: Literal["POST", "PUT", "PATCH"] = Field(
        default="POST",
        json_schema_extra={"engine_scoped": False},
    )
    headers: dict[str, str] = Field(
        default_factory=dict,
        description="Additional HTTP headers. Values may contain ${ENV_VAR} tokens.",
        json_schema_extra={"engine_scoped": False},
    )
    payload: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Custom payload template. Values may contain ${VAR} tokens. "
            "If null, the full FailureContext JSON is sent as-is."
        ),
        json_schema_extra={"engine_scoped": False},
    )
    timeout: int = Field(
        default=10,
        ge=1,
        description="HTTP socket timeout in seconds (>= 1)",
        json_schema_extra={"engine_scoped": False},
    )
    secret: str | None = Field(
        default=None,
        description=(
            "HMAC-SHA256 signing secret. When set, an "
            "'X-Aqueduct-Signature: sha256=<digest>' header is added so the "
            "receiver can verify payload authenticity and integrity. The digest "
            "is computed over the exact request body. May contain ${ENV_VAR} tokens."
        ),
        json_schema_extra={"engine_scoped": False},
    )
    max_retries: int = Field(
        default=1,
        ge=0,
        description=(
            "Retries beyond the first attempt on transient failures "
            "(429/500/502/503/504 or network errors). 0 disables retry."
        ),
        json_schema_extra={"engine_scoped": False},
    )
    backoff_seconds: float = Field(
        default=2.0,
        gt=0,
        description="Base backoff between retries; exponential (base * 2**(n-1)) when max_retries > 1.",
        json_schema_extra={"engine_scoped": False},
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
        json_schema_extra={"engine_scoped": False},
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
    on_defer: WebhookEndpointConfig | None = Field(
        default=None,
        description=(
            "Endpoint to POST when a patch defers to a human (defer_to_human op). "
            "Carries defer_reason, diagnosis/suggestions, and confidence_reason. "
            "When unset, defer notifications fall back to on_patch_pending so "
            "nobody loses notifications on upgrade."
        ),
    )

    @field_validator(
        "on_failure", "on_success", "on_patch_pending", "on_ci_patch", "on_defer", mode="before"
    )
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
            '`AQ-WARN [...]` prefix). The single entry `"*"` silences ALL '
            "Aqueduct warnings."
        ),
        json_schema_extra={"engine_scoped": False},
    )
    # Phase 79 — the escalation counterpart to `suppress`: rule_ids listed
    # here are promoted from warning to a hard CompileError instead of being
    # printed. Default empty = no behaviour change for anyone. Introduced for
    # `cross_engine_heal` (a DuckDB-shaped healed patch compiling for Spark,
    # or vice versa) but is a general rule_id set — any future compile
    # warning can opt a deployment into hard-failing on it.
    strict: list[str] = Field(
        default_factory=list,
        description=(
            "List of `rule_id` strings to promote from warning to a hard "
            "CompileError. Default empty (warn-only, current behaviour). "
            "Symmetric with `suppress` — same rule_id vocabulary."
        ),
        json_schema_extra={"engine_scoped": False},
    )


class LineageConfig(BaseModel):
    """Phase 55 — OpenLineage emission.

    Top-level `lineage:` block. **Naming note:** this is the OpenLineage
    *emission* config — unrelated to the former `stores.lineage` store, which was
    removed (column lineage merged into the observability store). This block
    configures *emission* of OpenLineage run events.

    When `openlineage_url` is unset (default), no events are emitted — zero cost.

    CORE, not engine-scoped (Q4 step 2, corrected 2026-07-31): the emitter is
    built in `Surveyor.__init__` (`aqueduct/surveyor/surveyor.py`) gated only
    on `openlineage_url` being set — no engine condition anywhere. Emission
    (START/COMPLETE/FAIL) and the column-level `columnLineage` facet are both
    derived from compile-time lineage (sqlglot) and delivered via
    `infra/http.py`; no engine module reads either key. A DuckDB run emits
    OpenLineage events exactly like a Spark run.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    openlineage_url: str | None = Field(
        default=None,
        description="OpenLineage receiver endpoint (Marquez / DataHub / Atlan). "
        "POST target for run events. Unset → emission disabled.",
        json_schema_extra={"engine_scoped": False},
    )
    openlineage_namespace: str = Field(
        default="aqueduct",
        description="OpenLineage namespace for jobs and datasets emitted by this run "
        "(engine-independent — see class docstring).",
        json_schema_extra={"engine_scoped": False},
    )


class SparkEngineConfig(BaseModel):
    """Engine-level Spark session configuration (`engine.spark:` block, 2.0).

    Replaces two pre-2.0 fields: ``deployment.master_url`` (Spark-specific —
    ``SparkSession.builder.master()`` — not a cross-engine deployment
    concern) and the top-level ``spark_config`` dict (named after one engine
    when it was the only one). ``master_url``/``conf`` here are both
    Spark-only; every other engine ignores them (config-leaf governance,
    docs/specs.md §10.9) without ceremony.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    master_url: str = Field(
        default="local[*]",
        description=(
            "Spark cluster URL (SparkSession.builder.master()). Moved from "
            "deployment.master_url in 2.0."
        ),
        json_schema_extra={"engine_scoped": True},
    )
    conf: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Per-run Spark session configuration, merged with the "
            "Blueprint's own engine.spark.conf (Blueprint wins), and with "
            "this invocation's -s/--set on top of both. Replaces "
            "the pre-2.0 top-level spark_config dict."
        ),
        json_schema_extra={"engine_scoped": True},
    )


class DuckDBEngineConfig(BaseModel):
    """Engine-level DuckDB configuration (`engine.duckdb:` block, 2.0).

    Every field here is read by ``aqueduct/executor/duckdb_/engine.py::
    _make_session`` — no field exists without a consumer (AGENTS.md's "no
    silent no-ops" rule). Two independent motivations for ``database_path``:
    (1) the RAM ceiling — a bare ``:memory:`` connection hard-caps a
    receiving cross-engine handoff island at available RAM; (2) disk spill —
    a file-backed connection lets DuckDB spill large intermediates to disk
    instead of failing, which is what lets a large RECEIVING island survive
    at all.

    S3/GCS credentials (``s3_key_id_secret``/``s3_secret_access_key_secret``)
    are SECRET KEY NAMES, resolved through the existing ``secrets:`` block
    resolver (``aqueduct.secrets.resolve_secret`` — the same function
    ``@aq.secret()`` calls) at session-creation time, never a parallel
    credential path. The resolved values are fed into DuckDB's own
    ``CREATE SECRET (TYPE S3, ...)`` (core DuckDB, works with the ``httpfs``
    extension unloaded) via parameter binding — never string-interpolated,
    so a credential value can never end up embedded in generated SQL text.
    ``httpfs`` itself is a DuckDB EXTENSION, not a Python package — no new
    dependency, no new extra.

    ``s3_endpoint``/``s3_url_style``/``s3_use_ssl`` are the non-AWS-S3-
    compatible escape hatch — verified end-to-end against a real MinIO
    instance (Phase 82 remediation). Unlike the credential fields above,
    these are NOT secrets — a literal endpoint/style/SSL flag is fine here.
    AWS's default virtual-hosted addressing (``bucket.s3.amazonaws.com``)
    and TLS-everywhere assumption don't hold for MinIO or any other
    S3-compatible store: without ``s3_endpoint``, DuckDB resolves the
    bucket against AWS's real S3 regardless of which credentials are
    configured (measured: the SAME credentials against the SAME bucket
    name fail when only ``s3_endpoint`` is omitted).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    memory_limit: str | None = Field(
        default=None,
        description=(
            "DuckDB `SET memory_limit='<value>'` (e.g. '4GB'), applied at "
            "session creation. Unset leaves DuckDB's own default (roughly "
            "80% of detected system RAM)."
        ),
        json_schema_extra={"engine_scoped": True},
    )
    threads: int | None = Field(
        default=None,
        gt=0,
        description=(
            "DuckDB `SET threads=<value>`, applied at session creation. "
            "Unset leaves DuckDB's own default (all available cores)."
        ),
        json_schema_extra={"engine_scoped": True},
    )
    database_path: str | None = Field(
        default=None,
        description=(
            "Local filesystem path for a persistent DuckDB database file, "
            "replacing the default `:memory:` connection. LOCAL PATHS "
            "ONLY — remote URI schemes (s3://, gs://, abfss://, ...) are "
            "rejected at config-load; DuckDB's own database file is always "
            "local even when the tables/relations it reads or writes point "
            "at remote storage. Two independent reasons to set this: "
            "raising the RAM ceiling a receiving cross-engine handoff "
            "island is bound by, and letting large intermediates spill to "
            "disk instead of aborting. Unset (default) keeps the "
            "always-fresh `:memory:` connection."
        ),
        json_schema_extra={"engine_scoped": True},
    )
    extension_repository: str | None = Field(
        default=None,
        description=(
            "DuckDB `SET custom_extension_repository='<url>'`, applied "
            "before any extension install. Points DuckDB's extension "
            "installer at an internal mirror instead of the public "
            "repository — the escape hatch for an airgapped/hermetic "
            "environment that cannot reach the network the first time a "
            "Blueprint touches a remote (s3://, gs://, ...) path and DuckDB "
            "tries to autoinstall `httpfs`. The other escape hatch is a "
            "pre-populated `~/.duckdb/extensions` directory, which needs no "
            "config at all."
        ),
        json_schema_extra={"engine_scoped": True},
    )
    s3_key_id_secret: str | None = Field(
        default=None,
        description=(
            "Secret KEY NAME (resolved through the configured `secrets:` "
            "provider — env | aws | gcp | azure | custom) holding the S3/GCS "
            "access-key-id credential. NOT the credential value itself — "
            "never put a literal key in aqueduct.yml. Fed into `CREATE "
            "SECRET (TYPE S3, KEY_ID ..., ...)` at session creation via "
            "parameter binding. Must be set together with "
            "`s3_secret_access_key_secret`."
        ),
        json_schema_extra={"engine_scoped": True},
    )
    s3_secret_access_key_secret: str | None = Field(
        default=None,
        description=(
            "Secret KEY NAME for the matching S3/GCS secret-access-key "
            "credential — see `s3_key_id_secret`. Must be set together "
            "with it."
        ),
        json_schema_extra={"engine_scoped": True},
    )
    s3_region: str | None = Field(
        default=None,
        description=(
            "AWS region for the `CREATE SECRET (TYPE S3, ...)` statement "
            "(e.g. 'us-east-1'). Not sensitive — given literally, unlike "
            "the key_id/secret_access_key fields above. Optional: DuckDB's "
            "S3 secret does not require a region."
        ),
        json_schema_extra={"engine_scoped": True},
    )
    s3_endpoint: str | None = Field(
        default=None,
        description=(
            "S3-compatible endpoint override for `CREATE SECRET (TYPE S3, "
            "..., ENDPOINT ...)` — `host:port`, no scheme (e.g. "
            "'localhost:9000' or 'minio.internal:9000'). Required for any "
            "non-AWS S3-compatible store (MinIO, on-prem object storage); "
            "unset (default) leaves DuckDB's own AWS endpoint resolution "
            "untouched."
        ),
        json_schema_extra={"engine_scoped": True},
    )
    s3_url_style: Literal["vhost", "path"] | None = Field(
        default=None,
        description=(
            "URL addressing style for `CREATE SECRET (TYPE S3, ..., "
            "URL_STYLE ...)`. MinIO and most non-AWS S3-compatible stores "
            "need `path` (bucket in the URL path, e.g. "
            "`http://host:9000/bucket/key`) rather than AWS's default "
            "virtual-hosted `vhost` style (`bucket.s3.amazonaws.com/key`). "
            "Unset (default) leaves DuckDB's own default untouched."
        ),
        json_schema_extra={"engine_scoped": True},
    )
    s3_use_ssl: bool | None = Field(
        default=None,
        description=(
            "Whether to use HTTPS for `CREATE SECRET (TYPE S3, ..., "
            "USE_SSL ...)`. A local/dev MinIO instance typically serves "
            "plain HTTP — set `false` for those. Unset (default, `None`) "
            "leaves DuckDB's own default (`true`) untouched."
        ),
        json_schema_extra={"engine_scoped": True},
    )

    @field_validator("database_path")
    @classmethod
    def _validate_database_path(cls, v: str | None) -> str | None:
        if v is None or not v:
            return v
        if _URI_SCHEME_RE.match(v):
            scheme = v.split("://", 1)[0]
            raise ValueError(
                f"engine.duckdb.database_path={v!r} uses a remote URI scheme "
                f"({scheme!r}://) — database_path only supports local "
                "filesystem paths. DuckDB's own database FILE is always "
                "local even when the tables/relations it reads or writes "
                "point at remote storage (s3://, gs://, ...) — use "
                "`s3_key_id_secret`/`s3_secret_access_key_secret`/`s3_region` "
                "for those. Use a local path, or omit database_path to keep "
                "the default `:memory:` connection."
            )
        return v

    @model_validator(mode="after")
    def _validate_s3_credential_pair(self) -> DuckDBEngineConfig:
        has_key = bool(self.s3_key_id_secret)
        has_secret = bool(self.s3_secret_access_key_secret)
        if has_key != has_secret:
            raise ValueError(
                "engine.duckdb.s3_key_id_secret and s3_secret_access_key_secret "
                "must be set TOGETHER (both name secret keys for one S3 "
                "credential pair) — got only "
                f"{'s3_key_id_secret' if has_key else 's3_secret_access_key_secret'}."
            )
        return self


class EngineConfig(BaseModel):
    """Per-engine configuration, namespaced by engine name (`engine:` block, 2.0).

    Replaces the pre-2.0 top-level `spark_config` dict — named after one
    engine — now that DuckDB is a second registered engine (docs/specs.md
    §10.9). A key under `engine.<name>.` belongs to that engine; every other
    engine ignores it (warn, never error — see config-leaf governance).
    Adding a new engine's settings is a new sub-block here, not a new
    top-level `<engine>_config` dict.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    spark: SparkEngineConfig = Field(default_factory=SparkEngineConfig)
    duckdb: DuckDBEngineConfig = Field(default_factory=DuckDBEngineConfig)


class HandoffConfig(BaseModel):
    """Cross-engine handoff transport config (top-level `handoff:` block,
    Phase 81 step 2).

    Governs WHERE the compiler-synthesized Handoff module's storage-spill
    parquet lands, and whether that spill directory survives a failed run
    so a rerun (e.g. after a self-heal) can skip recomputing an
    already-materialized upstream island. NOT nested under
    `checkpoint_root` — checkpoint is a same-engine Spark resume concept
    (local filesystem only, see `AqueductConfig.checkpoint_root`); handoff
    is cross-engine and borrows checkpoint's LIFECYCLE semantics (kept on
    failure, cleaned up on success), not its location or its local-only
    constraint. Parquet is a fixed internal transport detail, not a config
    key — there is deliberately no format knob here.

    Directory layout the executor lays out per run (step 3 implements the
    actual write/read; this block only configures WHERE):
    `<root>/<manifest_hash>/<run_id>/<edge_id>/`. Deleted on a successful
    run; kept when `keep_on_failure` is true (the default) so a rerun
    reads the existing spill instead of recomputing it. Keeping is bounded
    by two deterministic RELEASE events, not by a clock: a successful
    `--resume` deletes the spill it consumed, and the orphan sweep
    reclaims a kept failure once a later run of the same blueprint has
    succeeded (see `aqueduct/executor/spill.py` and `docs/specs.md`
    §10.4.3). There is no retention window and no age threshold.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    root: str = Field(
        default=DEFAULT_HANDOFF_ROOT,
        description=(
            "Root directory/URI for cross-engine handoff spill files. Any "
            "location BOTH engines on either side of a boundary can read "
            "and write — a local path, or a remote URI (s3://, gs://, "
            "abfss://, ...). Unlike `checkpoint_root`, remote URI schemes "
            "are NOT rejected here."
        ),
        json_schema_extra={"engine_scoped": True},
    )
    keep_on_failure: bool = Field(
        default=True,
        description=(
            "Keep a boundary's spilled parquet after a failed run so a "
            "rerun (e.g. after a self-heal) can read the upstream "
            "island's already-materialized output instead of recomputing "
            "it. A kept spill is released once it has been consumed by a "
            "successful resume, or once a later run of the same blueprint "
            "has succeeded (no time-based expiry exists). When false, "
            "spill directories are removed regardless of outcome."
        ),
        json_schema_extra={"engine_scoped": False},
    )
    prune_eagerly: bool = Field(
        default=True,
        description=(
            "When true (the default), a polyglot run deletes a boundary's "
            "spilled parquet as soon as EVERY island that reads it has "
            "finished successfully IN THIS RUN, instead of waiting for the "
            "whole run to end — bounds peak spill storage on a long "
            "same-engine-to-same-engine chain (Phase 89 item 3). A spill "
            "feeding an island that has not yet run, or that this run "
            "resumed from a PRIOR run (`--resume <run_id>`), is never "
            "touched by this: only this run's OWN already-consumed "
            "boundaries are eligible. The end-of-run deletion described "
            "above still runs either way — an eagerly pruned boundary is "
            "simply already gone by then. Set to false to defer every "
            "deletion to the run's own end, exactly as before this flag "
            "existed."
        ),
        json_schema_extra={"engine_scoped": False},
    )


class ExecutionConfig(BaseModel):
    """Top-level ``execution:`` block (Phase 89 item 1) — polyglot session
    keep-alive across same-engine islands.

    A polyglot run (``manifest.islands`` has more than one entry) executes
    each island against its own engine session
    (``aqueduct.executor.orchestrator.run_polyglot``). Before this block
    existed, every island's session was closed the moment that island
    finished, even when the SAME engine recurred later in the run (e.g.
    spark -> duckdb -> spark) — see ``orchestrator.py``'s module docstring
    for the history. These two flags govern that behavior; neither has any
    effect on a single-engine run (``aqueduct/cli/run.py``'s single-engine
    ``_execute_target`` fingerprint funnel, and the heal-retry rebuild it
    guards, are untouched by this block).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    session_keep_alive: bool = Field(
        default=True,
        description=(
            "When true (the default), a polyglot run hands a LIVE session "
            "to the next island in execution order when that island uses "
            "the SAME engine, instead of closing it and building a fresh "
            "one — skipping the cost of a session rebuild (most significant "
            "for Spark, whose session build re-runs SparkContext "
            "initialization). Session-scoped state the finishing island "
            "created — catalog tables/views registered via "
            "`register_as_table` — is dropped at the boundary unless "
            "`share_island_state` is also true, so a reused session stays "
            "observationally identical to a fresh one. Every session is "
            "still closed by the time the run returns, on success, "
            "failure, or exception. Set to false to restore the original "
            "close-every-island behavior exactly."
        ),
        json_schema_extra={"engine_scoped": False},
    )
    share_island_state: bool = Field(
        default=False,
        description=(
            "When true, SKIP the inter-island cleanup that normally runs "
            "when `session_keep_alive` reuses a session across a boundary "
            "— the next same-engine island sees whatever catalog "
            "tables/views the previous one registered via "
            "`register_as_table`, e.g. to avoid reloading state when a run "
            "returns to an engine it already visited. Sharing is opt-in: "
            "only meaningful when `session_keep_alive` is true, and a "
            "no-op otherwise."
        ),
        json_schema_extra={"engine_scoped": False},
    )


def _freeze_for_hash(value: Any) -> Any:
    """Recursively turn *value* into something hashable, for
    ``AqueductConfig.__hash__`` — several nested config models carry plain
    ``dict``/``list`` fields (e.g. ``EngineConfig.spark.conf``,
    ``WarningsConfig``'s list fields), which are not hashable as-is.
    Dict items are sorted by key so equal dicts (order-independent by
    definition) always freeze to the same tuple.
    """
    if isinstance(value, BaseModel):
        return (type(value), _freeze_for_hash(value.__dict__))
    if isinstance(value, dict):
        return tuple(sorted((k, _freeze_for_hash(v)) for k, v in value.items()))
    if isinstance(value, list | tuple):
        return tuple(_freeze_for_hash(v) for v in value)
    if isinstance(value, set | frozenset):
        return frozenset(_freeze_for_hash(v) for v in value)
    return value


class AqueductConfig(BaseModel):
    """Fully validated engine configuration.

    All fields have sensible defaults so that running without an aqueduct.yml
    (development / CI) works out of the box.

    LLM connection defaults (provider, base_url, model) live here.
    Per-blueprint policy (approval) lives in the Blueprint agent: block.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    aqueduct_config: str = Field(
        default="2.0",
        description="Config schema version",
        json_schema_extra={"engine_scoped": False},
    )
    deployment: DeploymentConfig = Field(default_factory=DeploymentConfig)
    engine: EngineConfig = Field(default_factory=EngineConfig)
    stores: StoresConfig = Field(default_factory=StoresConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    probes: ProbesConfig = Field(default_factory=ProbesConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)
    danger: DangerConfig = Field(default_factory=DangerConfig)
    secrets: SecretsConfig = Field(default_factory=SecretsConfig)
    git: GitConfig = Field(default_factory=GitConfig)
    pr: PrConfig = Field(default_factory=PrConfig)
    webhooks: WebhooksConfig = Field(default_factory=WebhooksConfig)
    lineage: LineageConfig = Field(default_factory=LineageConfig)
    agent: AgentConnectionConfig = Field(default_factory=AgentConnectionConfig)
    warnings: WarningsConfig = Field(default_factory=lambda: WarningsConfig())
    handoff: HandoffConfig = Field(default_factory=HandoffConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    checkpoint_root: str | None = Field(
        default=None,
        description=(
            "Local filesystem path overriding the derived "
            "<store_dir>/checkpoints/ location for module checkpoint/resume "
            "state. LOCAL PATHS ONLY — remote URI schemes (s3://, s3a://, "
            "gs://, hdfs://, abfss://, ...) are rejected at config-load; "
            "remote checkpoint roots are not supported."
        ),
        json_schema_extra={"engine_scoped": True},
    )
    timezone: str | None = Field(
        default=None,
        description=(
            "IANA/Olson time zone name (e.g. 'UTC', 'America/New_York'), "
            "applied to EVERY registered engine's session at creation — "
            "Spark's spark.sql.session.timeZone, DuckDB's SET TimeZone. "
            "Engine-native tz settings (engine.spark.conf.spark.sql.session."
            "timeZone) already work standalone; this key only earns its "
            "keep when a Blueprint spans more than one engine (Phase 81/82 "
            "cross-engine handoff), where a divergent per-engine session "
            "time zone silently changes to_timestamp()/cast results between "
            "islands. An explicit engine-native override always wins for "
            "that engine, and Aqueduct warns (rule_id "
            "'engine_timezone_conflict') when the two disagree."
        ),
        json_schema_extra={"engine_scoped": True},
    )

    # ── Per-invocation `-s/--set` engine-config layer ────────────────────
    #
    # NOT a config field, deliberately. It is a `PrivateAttr`, so it never
    # appears in `model_dump()`, never becomes an `aqueduct.yml` key a user
    # could write, and never becomes a `config.*` capability leaf every
    # engine has to declare a verdict for. It carries what the user typed on
    # THIS invocation's command line and nothing else, and is never
    # persisted anywhere.
    #
    # It lives on `AqueductConfig` because every single site that resolves a
    # session's engine config already threads `cfg` — the CLI run path
    # (single-engine and polyglot), the drift command, the sandbox gate, the
    # effective-config delta gate, `patch revert`, and the doctor check. An
    # extra parameter on the resolver would have to be remembered at each of
    # them, and a site that forgot it would silently drop the user's flag,
    # which is the exact silent-no-op class AGENTS.md forbids. Carrying it
    # on the object they all already hold makes forgetting impossible.
    #
    # Shape: ``{engine_name: {key: value}}`` — the same shape
    # ``Manifest.engine_config`` uses, so it layers through
    # ``aqueduct.executor.session_config.resolve_session_engine_config``
    # with no reshaping. Written only by
    # ``aqueduct.overrides.apply_to_model``; read through the
    # ``cli_engine_overrides`` property below.
    _cli_engine_overrides: dict[str, dict[str, Any]] = PrivateAttr(default_factory=dict)

    @property
    def cli_engine_overrides(self) -> dict[str, dict[str, Any]]:
        """Per-invocation ``-s/--set`` engine-config layer, ``{engine: {key: value}}``.

        The TOP layer of the session-config merge — above `aqueduct.yml` and
        above the Blueprint's own ``engine.<name>:`` block. A CLI flag is the
        most explicit statement a user can make about one run, and it is
        never written back to any file, so it cannot silently undo a healed
        Blueprint value beyond the invocation that typed it. The merge order
        itself is stated once, in
        ``aqueduct.executor.session_config.resolve_session_engine_config``;
        this is only the carrier.
        """
        return self._cli_engine_overrides

    def with_cli_engine_overrides(self, layers: dict[str, dict[str, Any]]) -> AqueductConfig:
        """Copy of this config carrying *layers* as its ``--set`` engine layer."""
        out = self.model_copy()
        out._cli_engine_overrides = {
            engine: dict(keys) for engine, keys in (layers or {}).items() if keys
        }
        return out

    def __eq__(self, other: object) -> bool:
        """Field equality only — deliberately excludes ``_cli_engine_overrides``.

        Pydantic v2's generated ``BaseModel.__eq__`` compares
        ``__pydantic_private__`` (which holds every ``PrivateAttr``,
        including ``_cli_engine_overrides`` above) BEFORE it compares
        fields. Two configs that are identical except for this
        invocation's ``--set`` layer would therefore compare unequal —
        a per-invocation carrier leaking into object identity. Comparing
        ``self.__dict__`` (pydantic's field storage; private attrs live
        separately in ``__pydantic_private__`` and are not part of it)
        restores the intended semantics: same config, different
        ``--set`` layer, still equal.
        """
        if type(other) is not type(self):
            return NotImplemented
        return self.__dict__ == other.__dict__

    def __hash__(self) -> int:
        """Restore hashability — defining ``__eq__`` sets ``__hash__`` to
        ``None`` on any class, frozen or not. Hashes exactly the fields
        ``__eq__`` compares (``self.__dict__``, private attrs excluded),
        via a recursive freeze since several nested config models carry
        plain ``dict``/``list`` fields that are not directly hashable.
        """
        return hash((type(self), _freeze_for_hash(self.__dict__)))

    @field_validator("checkpoint_root")
    @classmethod
    def _validate_checkpoint_root(cls, v: str | None) -> str | None:
        if v is None or not v:
            return v
        if _URI_SCHEME_RE.match(v):
            scheme = v.split("://", 1)[0]
            raise ValueError(
                f"checkpoint_root={v!r} uses a remote URI scheme ({scheme!r}://) "
                "— checkpoint_root only supports local filesystem paths today; "
                "remote checkpoint roots (S3/GCS/HDFS/ABFSS) are not supported. "
                "Use a local path, or omit checkpoint_root to fall back to the "
                "derived <store_dir>/checkpoints/ default."
            )
        return v

    @model_validator(mode="after")
    def _validate_target_master_url(self) -> AqueductConfig:
        """Enforce ``deployment.target`` ↔ ``engine.spark.master_url`` consistency.

        Moved here (2.0) from ``DeploymentConfig`` because it now needs
        fields from two blocks: ``deployment.target`` and
        ``engine.spark.master_url`` (the pre-2.0 ``deployment.master_url``).
        Only meaningful for ``deployment.engine == "spark"`` — a non-Spark
        engine has no cluster master to validate against a target shape.
        Remote-submit targets (emr/dataproc) are handled in
        ``DeploymentConfig._validate_engine``, which runs first (field-level
        validators run before this model-level one) and already
        rejects/accepts those independent of master_url.
        """
        if self.deployment.engine != "spark":
            return self

        target = self.deployment.target
        master = self.engine.spark.master_url

        if target in ("emr", "dataproc"):
            return self  # handled by DeploymentConfig._validate_engine

        _EXPECTED: dict[str, str] = {
            "local": "local",
            "standalone": "spark://",
            "yarn": "yarn",
            "kubernetes": "k8s://",
        }
        expected = _EXPECTED.get(target)
        if expected is None:
            return self

        if expected == "yarn":
            if master != "yarn":
                raise ConfigError(
                    f"deployment.target={target!r} requires "
                    f"engine.spark.master_url={expected!r}, "
                    f"got engine.spark.master_url={master!r}"
                )
        elif not master.startswith(expected):
            raise ConfigError(
                f"deployment.target={target!r} requires "
                f"engine.spark.master_url starting with {expected!r}, "
                f"got engine.spark.master_url={master!r}"
            )

        return self


# ── Loader ────────────────────────────────────────────────────────────────────

_DEFAULT_FILENAME = "aqueduct.yml"


# Provider → (probe module, install hint). Keep in sync with secrets.py and
# pyproject.toml [project.optional-dependencies].
_SECRETS_BACKEND_REQUIREMENTS: dict[str, tuple[str, str]] = {
    "aws": ("boto3", "pip install aqueduct-core[aws]"),
    "gcp": ("google.cloud.secretmanager", "pip install aqueduct-core[gcp]"),
    "azure": ("azure.keyvault.secrets", "pip install aqueduct-core[azure]"),
}


# Store backend → (probe module, install hint). Mirrors the secrets pattern.
# `duckdb` is always available (it is a hard dependency of aqueduct-core).
_STORE_BACKEND_REQUIREMENTS: dict[str, tuple[str, str]] = {
    "postgres": ("psycopg2", "pip install aqueduct-core[postgres]"),
    "redis": ("redis", "pip install aqueduct-core[redis]"),
}

_OBJECT_BACKEND_REQUIREMENTS: dict[str, tuple[str, str]] = {
    "s3": ("s3fs", "pip install aqueduct-core[object-store]"),
    "gcs": ("gcsfs", "pip install aqueduct-core[object-store]"),
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
    # Every named depot mount (not just the "default" one) and the
    # benchmark store's own backend need the same fail-fast check as
    # observability — audit-fixed 2026-08: `depots.<name>: {backend:
    # postgres}` without [postgres], or `stores.benchmark.backend:
    # postgres` without it, previously loaded cleanly and only died with a
    # bare ImportError at first real use (`@aq.depot.<name>.get()` /
    # `aqueduct benchmark`), mid-run, instead of the ConfigError every
    # other store backend gets at load time.
    _checks: list[tuple[str, str]] = [
        ("observability", stores_cfg.observability.backend),
        ("benchmark", stores_cfg.benchmark.backend),
    ]
    _checks.extend(
        (f"depots.{name}", mount.backend) for name, mount in stores_cfg.effective_depots().items()
    )
    for store_label, backend in _checks:
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


def _warn_ignored_config_keys(cfg: AqueductConfig) -> None:
    """Config-leaf governance (Phase 78 Step 3) — warn on keys the target
    engine ignores.

    Runs at CONFIG RESOLUTION, once ``cfg.deployment.engine`` is known and
    already validated as a registered engine (``DeploymentConfig.
    _validate_target_master_url``, above). Walks only the leaves the user
    EXPLICITLY set (``config_leaves.explicitly_set_config_leaves`` —
    untouched defaults never warn) and emits the SAME suppressible
    ``engine_key_ignored`` rule id the compile-time blueprint gate uses
    (``aqueduct/compiler/capability_check.py``), through the same
    ``aqueduct.warnings.emit`` machinery.

    Deliberately WARN, never ERROR — unlike the blueprint gate, where
    UNSUPPORTED is a hard CompileError. A config key that means nothing on
    the current engine must not break loading ``aqueduct.yml``: the same
    file has to stay valid across engines (a user's test overrides, or one
    shared config used by multiple deployment profiles, must not hard-fail
    just because one engine ignores one key). Both non-SUPPORTED verdicts
    (``IGNORED_WITH_WARNING`` and ``UNSUPPORTED``) are treated the same way
    here — a config leaf has no compile-time "this can never work" state,
    only "this engine does nothing with it".

    "Warn, never error" governs the VERDICT, not the machinery: an ignored key
    warns instead of raising, but a genuine bug in this code path (an
    AttributeError from a walker change, say) must still surface. Only
    ``UnknownEngineError`` is swallowed, and only because it is already
    impossible here — ``DeploymentConfig`` validated ``deployment.engine``
    against the same registry before this runs, so the sole way to reach it is
    a registry mutated between validation and here (a test double). A blanket
    ``except Exception`` would hide real defects in ``get_capabilities()`` and
    the leaf walkers behind a silently-absent warning, which is the same class
    of silent no-op this whole gate exists to eliminate.

    Lazy imports (not module-level) to avoid a circular import: ``aqueduct.
    executor.config_leaves`` imports ``aqueduct.config`` at module scope to
    introspect ``AqueductConfig``, so this module cannot import it back at
    module scope. Same precedent as ``DeploymentConfig.
    _validate_target_master_url``'s lazy ``aqueduct.executor.capabilities``
    import.
    """
    from aqueduct.compiler.capability_check import RULE_ID_IGNORED
    from aqueduct.errors import UnknownEngineError
    from aqueduct.executor.capabilities import Support, get_capabilities
    from aqueduct.executor.config_leaves import explicitly_set_config_leaves
    from aqueduct.warnings import emit as _emit

    try:
        caps = get_capabilities(cfg.deployment.engine)
    except UnknownEngineError:
        return  # unreachable via load_config() — see docstring

    suppress = set(cfg.warnings.suppress)
    for leaf_id in sorted(explicitly_set_config_leaves(cfg)):
        cap = caps.verdict(leaf_id)
        if cap.support == Support.SUPPORTED:
            continue
        hint = f" {cap.hint}" if cap.hint else ""
        _emit(
            RULE_ID_IGNORED,
            f"aqueduct.yml sets {leaf_id!r}, which engine "
            f"{cfg.deployment.engine!r} ignores (accepted but has no "
            f"effect).{hint}",
            suppress=suppress,
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


_ENV_VAR_RE = re.compile(r"\$\{([^}:]+)(?::[-]?(.*?))?\}")
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


def _register_agent_api_key_for_redaction(cfg: AqueductConfig) -> None:
    """Register `agent.api_key`'s final resolved value with `aqueduct.redaction`.

    Audit-fixed 2026-08: only `@aq.secret()`-resolved values were ever
    registered (`_expand_secrets` above) — a `${ENV_VAR}` value or a
    plaintext literal never entered the redaction registry at all, despite
    the field's own docstring AND the `insecure_api_key` warning message
    both stating the value "is redacted from logs and LLM payloads" as a
    fact. By config-load time all three forms (literal / ${ENV} / @aq.
    secret) have already collapsed to the same plain string on
    `cfg.agent.api_key`, so one unconditional registration here — a
    superset of what `_expand_secrets` already covers for this specific
    field — makes that claim true instead of narrowing the docstring.
    """
    try:
        api_key = getattr(getattr(cfg, "agent", None), "api_key", None)
        if api_key:
            from aqueduct import redaction

            redaction.register(api_key, key_hint="agent.api_key")
    except Exception:
        pass  # redaction registration must never block config load


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
        raise ConfigError(
            f"Config file {resolved} must be a YAML mapping, not {type(data).__name__}"
        )

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
            if (
                isinstance(_raw_key, str)
                and _raw_key.strip()
                and not _raw_key.startswith("@aq.secret(")
                and "${" not in _raw_key
            ):
                # Routed through emit() (not a raw warnings.warn) so
                # `warnings.suppress: [insecure_api_key]` /
                # `--suppress-warning insecure_api_key` actually does
                # something — the old code hand-embedded the
                # "[aqueduct:insecure_api_key]" text but called
                # warnings.warn() directly, which emit()'s suppress-set
                # check never saw (a raw warnings.warn is invisible to it).
                from aqueduct.warnings import emit as _emit

                _emit(
                    "insecure_api_key",
                    f"agent.api_key is a plaintext literal in {resolved} — prefer "
                    "@aq.secret('NAME') or ${ENV_VAR}. The value is registered for "
                    "redaction from logs and LLM payloads once config-load finishes, "
                    "but a literal in a committed config is still a credential leak "
                    "risk.",
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
    _bad_aq = sorted(
        {
            f"@aq.{m.group(1)}"
            for m in re.finditer(r"@aq\.(?!secret\b)([a-zA-Z_][\w.]*)", pass1_text)
        }
    )
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
        _warn_ignored_config_keys(cfg_pass1)
        _register_agent_api_key_for_redaction(cfg_pass1)
        return cfg_pass1

    pass2_text, missing_secrets = _expand_secrets(
        pass1_text, cfg_pass1.secrets, base_dir=str(_cfg_dir)
    )
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
    _warn_ignored_config_keys(cfg)
    _register_agent_api_key_for_redaction(cfg)
    return cfg
