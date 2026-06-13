"""Surveyor — monitors blueprint execution and persists observability signals.

Lifecycle (called from CLI):

    surveyor = Surveyor(manifest, store_dir=Path(".aqueduct"), webhook_url=...)
    surveyor.start(run_id)          # create DB tables, record run start
    result = execute(manifest, spark, run_id=run_id)
    failure_ctx = surveyor.record(result)   # persist result, fire webhook if failed
    surveyor.stop()                 # close DB connection

DuckDB schema
─────────────
  run_records        — one row per run (start + final status)
  failure_contexts   — one row per failed run (FailureContext document)

Phase 5 scope: logging + webhook only.  LLM patch loop wired in Phase 7.
"""

from __future__ import annotations

import json
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import logging

from aqueduct.compiler.models import Manifest
from aqueduct.executor.models import ExecutionResult
from aqueduct.redaction import redact as _redact
from aqueduct.surveyor.models import FailureContext, RunRecord
from aqueduct.surveyor.webhook import fire_webhook

if TYPE_CHECKING:
    from aqueduct.stores import ObservabilityStore, StoreBundle

logger = logging.getLogger(__name__)

# ── DDL ───────────────────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS run_records (
    run_id         VARCHAR PRIMARY KEY,
    blueprint_id   VARCHAR NOT NULL,
    status         VARCHAR NOT NULL,
    started_at     TIMESTAMPTZ NOT NULL,
    finished_at    TIMESTAMPTZ,
    module_results JSON,
    parent_run_id  VARCHAR
);

CREATE TABLE IF NOT EXISTS failure_contexts (
    run_id            VARCHAR PRIMARY KEY,
    blueprint_id      VARCHAR NOT NULL,
    failed_module     VARCHAR NOT NULL,
    error_message     VARCHAR NOT NULL,
    stack_trace       VARCHAR,
    manifest_json     VARCHAR,     -- Phase 39: blob path or inline JSON
    provenance_json   VARCHAR,     -- Phase 39: blob path or inline JSON
    started_at        TIMESTAMPTZ NOT NULL,
    finished_at       TIMESTAMPTZ NOT NULL,
    -- Structured Spark-error extraction. Populated when PySparkException or
    -- Py4JJavaError surfaces enough metadata to identify the failure class,
    -- offending object, and suggested column names — much cheaper for the
    -- agent to consume than a raw multi-kilobyte JVM stack trace.
    error_class       VARCHAR,
    root_exception    JSON,
    sql_state         VARCHAR,
    object_name       VARCHAR,
    suggested_columns JSON
);

CREATE TABLE IF NOT EXISTS healing_outcomes (
    id           VARCHAR PRIMARY KEY,
    run_id       VARCHAR NOT NULL,
    parent_run_id VARCHAR,
    failed_module VARCHAR,
    failure_category VARCHAR,
    model        VARCHAR,
    patch_id     VARCHAR,
    confidence   DOUBLE PRECISION,
    patch_applied BOOLEAN,
    run_success_after_patch BOOLEAN,
    applied_at   VARCHAR,
    prompt_version VARCHAR,
    -- Phase 45 signature memory: exact failure-signature hash + how the heal
    -- was resolved ('llm' fresh agent patch, 'cached' pending-patch reuse,
    -- 'replayed' zero-token replay of an archived successful patch).
    failure_signature VARCHAR,
    resolution   VARCHAR,
    -- Phase 46: 0-based cascade tier index of the model that produced the
    -- patch; NULL outside multi-model cascade (or when no LLM was involved).
    model_cascade_position INTEGER
);

CREATE TABLE IF NOT EXISTS patch_simulation (
    id           VARCHAR PRIMARY KEY,
    run_id       VARCHAR,
    blueprint_id VARCHAR,
    patch_id     VARCHAR NOT NULL,
    gate         VARCHAR NOT NULL,
    status       VARCHAR NOT NULL,
    detail       VARCHAR,
    sample_rows  BIGINT,
    duration_ms  BIGINT,
    recorded_at  VARCHAR NOT NULL
);

-- Column-level lineage extracted at compile time (driver-side, zero Spark actions).
-- Merged from the former lineage.db in Phase 38.
CREATE TABLE IF NOT EXISTS column_lineage (
    blueprint_id   VARCHAR NOT NULL,
    run_id         VARCHAR NOT NULL,
    channel_id     VARCHAR NOT NULL,
    output_column  VARCHAR NOT NULL,
    source_table   VARCHAR NOT NULL,
    source_column  VARCHAR NOT NULL,
    captured_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_lineage_channel
    ON column_lineage (blueprint_id, channel_id);
"""

_SIGNAL_OVERRIDES_DDL = """
CREATE TABLE IF NOT EXISTS signal_overrides (
    signal_id     VARCHAR PRIMARY KEY,
    passed        BOOLEAN NOT NULL,
    error_message VARCHAR,
    set_at        TIMESTAMPTZ NOT NULL
);
"""

_EXPLAIN_SNAPSHOT_DDL = """
CREATE TABLE IF NOT EXISTS explain_snapshot (
    blueprint_id     VARCHAR NOT NULL,
    run_id           VARCHAR NOT NULL,
    module_id        VARCHAR NOT NULL,
    captured_at      VARCHAR NOT NULL,
    exchange_count   INTEGER NOT NULL,
    python_udf_count INTEGER NOT NULL,
    broadcast_count  INTEGER NOT NULL,
    plan_text        VARCHAR NOT NULL,
    PRIMARY KEY (blueprint_id, run_id, module_id)
);
"""

# Per-attempt log for the unified reprompt loop.
# One row per LLM turn (success or failure) so post-mortem can answer
# "what did attempt 2 actually say" — which `healing_outcomes` alone could
# not (it only carries the final patch outcome).
_HEAL_ATTEMPTS_DDL = """
CREATE TABLE IF NOT EXISTS heal_attempts (
    id                    VARCHAR PRIMARY KEY,
    run_id                VARCHAR NOT NULL,
    attempt_num           INTEGER NOT NULL,
    error_class           VARCHAR,
    where_field           VARCHAR,
    normalized_message    VARCHAR,
    signature_hash        VARCHAR,
    tokens_in             INTEGER NOT NULL DEFAULT 0,
    tokens_out            INTEGER NOT NULL DEFAULT 0,
    latency_ms            INTEGER NOT NULL DEFAULT 0,
    gate_that_rejected    VARCHAR,
    escalated             BOOLEAN NOT NULL DEFAULT FALSE,
    stop_reason           VARCHAR,
    prompt_version        VARCHAR,
    recorded_at           VARCHAR NOT NULL
);
"""

# Phase 45/46 columns for observability DBs created before the schema change.
# Both DuckDB and Postgres support ADD COLUMN IF NOT EXISTS.
_PHASE45_MIGRATION_DDL = """
ALTER TABLE healing_outcomes ADD COLUMN IF NOT EXISTS failure_signature VARCHAR;
ALTER TABLE healing_outcomes ADD COLUMN IF NOT EXISTS resolution VARCHAR;
ALTER TABLE healing_outcomes ADD COLUMN IF NOT EXISTS model_cascade_position INTEGER;
"""

# ── Helpers ───────────────────────────────────────────────────────────────────

def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _first_failed_module(result: ExecutionResult) -> str:
    """Return the module_id of the first failing module, or '_executor'."""
    for mr in result.module_results:
        if mr.status == "error":
            return mr.module_id
    return "_executor"


def _first_error_message(result: ExecutionResult, exc: Exception | None) -> str:
    for mr in result.module_results:
        if mr.status == "error" and mr.error:
            return mr.error
    if exc is not None:
        return str(exc)
    return "unknown error"


_PY4J_CAUSE_HOP_LIMIT = 10
# Spark 4.0 error-class names we recognise as carrying column-suggestion data.
_COLUMN_SUGGEST_CLASSES = frozenset({
    "UNRESOLVED_COLUMN.WITH_SUGGESTION",
    "UNRESOLVED_FIELD.WITH_SUGGESTION",
    "UNRESOLVED_MAP_KEY.WITH_SUGGESTION",
})


def _parse_suggested_columns(blob: str) -> tuple[str, ...]:
    """Parse Spark 'Did you mean one of the following? [`a`, `b`]' segment.

    Spark's UNRESOLVED_COLUMN.WITH_SUGGESTION message embeds the suggestion
    list as backtick-quoted identifiers separated by commas. Extracting them
    explicitly lets the prompt show "actual columns: …" without asking the
    LLM to parse the trace.
    """
    import re as _re
    if not blob:
        return ()
    out: list[str] = []
    for m in _re.finditer(r"`([^`]+)`", blob):
        name = m.group(1).strip()
        if name and name not in out:
            out.append(name)
    return tuple(out)


def _extract_structured_error(exc: BaseException | None) -> dict[str, Any] | None:
    """Return a dict of structured error fields, or None if extraction fails.

    Best-effort: lazy-imports pyspark/py4j and swallows any failure so that a
    bug in extraction can never block self-heal. Used by Surveyor.record()
    before the exception is stringified into FailureContext.error_message.

    Resolution order:
      1. PySparkException (Spark 4.0): getCondition / getErrorClass +
         getMessageParameters + getSqlState. Highest-fidelity path.
      2. Py4JJavaError: walk .java_exception.getCause() up to
         _PY4J_CAUSE_HOP_LIMIT to find innermost Java throwable.
      3. Python cause chain: traceback.TracebackException root.

    Returns mapping with keys: error_class, root_exception, sql_state,
    suggested_columns, object_name. Any field may be None / empty tuple.
    """
    if exc is None:
        return None
    out: dict[str, Any] = {
        "error_class": None,
        "root_exception": None,
        "sql_state": None,
        "suggested_columns": (),
        "object_name": None,
    }
    try:
        # --- 1. Spark 4.0 PySparkException ---------------------------------
        try:
            from pyspark.errors import PySparkException  # type: ignore
        except Exception:
            PySparkException = None  # type: ignore[assignment]

        spark_exc = None
        if PySparkException is not None:
            cur = exc
            for _ in range(_PY4J_CAUSE_HOP_LIMIT):
                if isinstance(cur, PySparkException):
                    spark_exc = cur
                    break
                cur = getattr(cur, "__cause__", None) or getattr(cur, "__context__", None)
                if cur is None:
                    break

        if spark_exc is not None:
            try:
                if hasattr(spark_exc, "getCondition"):
                    out["error_class"] = spark_exc.getCondition()
                elif hasattr(spark_exc, "getErrorClass"):
                    out["error_class"] = spark_exc.getErrorClass()
            except Exception:
                pass
            try:
                if hasattr(spark_exc, "getSqlState"):
                    out["sql_state"] = spark_exc.getSqlState()
            except Exception:
                pass
            params: dict[str, Any] = {}
            try:
                if hasattr(spark_exc, "getMessageParameters"):
                    raw = spark_exc.getMessageParameters() or {}
                    params = {str(k): str(v) for k, v in dict(raw).items()}
            except Exception:
                params = {}
            if params:
                for k in ("objectName", "fieldName", "tableName", "relationName", "columnName"):
                    if k in params:
                        out["object_name"] = params[k]
                        break
                if out["error_class"] in _COLUMN_SUGGEST_CLASSES:
                    for k in ("proposal", "suggestion", "suggestions"):
                        if k in params:
                            out["suggested_columns"] = _parse_suggested_columns(params[k])
                            break

        # --- 2. Py4JJavaError cause chain ----------------------------------
        if out["error_class"] is None:
            try:
                from py4j.protocol import Py4JJavaError  # type: ignore
            except Exception:
                Py4JJavaError = None  # type: ignore[assignment]

            if Py4JJavaError is not None:
                cur = exc
                for _ in range(_PY4J_CAUSE_HOP_LIMIT):
                    if isinstance(cur, Py4JJavaError):
                        java_exc = getattr(cur, "java_exception", None)
                        if java_exc is not None:
                            try:
                                innermost = java_exc
                                for _ in range(_PY4J_CAUSE_HOP_LIMIT):
                                    nxt = innermost.getCause()
                                    if nxt is None or nxt is innermost:
                                        break
                                    innermost = nxt
                                jclass = innermost.getClass().getName()
                                jmsg = innermost.getMessage() or ""
                                out["error_class"] = out["error_class"] or jclass
                                out["root_exception"] = {"type": jclass, "message": jmsg}
                            except Exception:
                                pass
                        break
                    cur = getattr(cur, "__cause__", None) or getattr(cur, "__context__", None)
                    if cur is None:
                        break

        # --- 3. Python cause chain (root fallback) -------------------------
        if out["root_exception"] is None:
            root = exc
            for _ in range(_PY4J_CAUSE_HOP_LIMIT):
                nxt = getattr(root, "__cause__", None) or getattr(root, "__context__", None)
                if nxt is None or nxt is root:
                    break
                root = nxt
            out["root_exception"] = {
                "type": type(root).__name__,
                "message": str(root),
            }
            if out["error_class"] is None:
                out["error_class"] = type(root).__name__

        if not any((out["error_class"], out["root_exception"], out["sql_state"],
                    out["suggested_columns"], out["object_name"])):
            return None
        return out
    except Exception:
        logger.debug("_extract_structured_error failed", exc_info=True)
        return None


def _first_error_type(result: ExecutionResult) -> str | None:
    """Return the error_type of the first failing module, or None for infra errors."""
    for mr in result.module_results:
        if mr.status == "error":
            return getattr(mr, "error_type", None)
    return None


# ── Public API ────────────────────────────────────────────────────────────────

class Surveyor:
    """Observability recorder for a single blueprint instance.

    One Surveyor per ``aqueduct run`` invocation.  Not thread-safe itself —
    the webhook is fire-and-forget in a daemon thread; everything else is
    single-threaded.
    """

    def __init__(
        self,
        manifest: Manifest,
        store_dir: Path,
        webhook_url: str | None = None,
        webhook_config: "WebhookEndpointConfig | None" = None,  # type: ignore[name-defined]  # noqa: F821
        blueprint_path: Path | None = None,
        patches_dir: Path | None = None,
        stores: "StoreBundle | None" = None,
    ) -> None:
        """Initialise the Surveyor.

        Args:
            stores: Optional pre-built `StoreBundle`. When None (the typical
                CLI path), the bundle is constructed lazily on `start()` from
                a default DuckDB layout under `store_dir`. The `stores=`
                parameter exists for the Phase 28 case where the CLI hands
                in a Postgres-backed bundle.
        """
        from aqueduct.config import WebhookEndpointConfig
        self._manifest = manifest
        self._store_dir = store_dir
        if webhook_config is not None:
            self._webhook_config: WebhookEndpointConfig | None = webhook_config
        elif webhook_url is not None:
            self._webhook_config = WebhookEndpointConfig(url=webhook_url)
        else:
            self._webhook_config = None
        self._blueprint_path = blueprint_path
        self._patches_dir = patches_dir or Path("patches")
        self._run_id: str | None = None
        self._started_at: datetime | None = None
        self._stores: "StoreBundle | None" = stores
        self._observability: "ObservabilityStore | None" = stores.observability if stores is not None else None
        self._started: bool = False  # DDL/migrations applied once per Surveyor.start()
        self._iteration_parents: dict[str, str] = {}  # run_id → parent_run_id (multi-patch)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self, run_id: str) -> None:
        """Open the configured observability store and record run start.

        Creates parent directories for DuckDB-backed stores; for Postgres
        the `observability` schema is auto-created on the first `connect()`.

        Args:
            run_id: The UUID generated by the CLI / Executor for this run.
        """
        self._run_id = run_id
        self._started_at = _utcnow()
        self._store_dir.mkdir(parents=True, exist_ok=True)

        # Construct a default DuckDB bundle when the caller did not hand one in.
        # The CLI path always builds and passes one via Phase 28's get_stores();
        # this fallback keeps direct programmatic callers (tests, scripts) working.
        if self._observability is None:
            from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
            self._observability = DuckDBObservabilityStore(self._store_dir / "observability.db")

        with self._observability.connect() as cur:
            cur.execute(_DDL)
            cur.execute(_SIGNAL_OVERRIDES_DDL)
            cur.execute(_EXPLAIN_SNAPSHOT_DDL)
            cur.execute(_HEAL_ATTEMPTS_DDL)
            cur.execute(_PHASE45_MIGRATION_DDL)

            cur.execute(
                """
                INSERT INTO run_records
                    (run_id, blueprint_id, status, started_at, finished_at, module_results, parent_run_id)
                VALUES (?, ?, 'running', ?, NULL, '[]', NULL)
                ON CONFLICT (run_id) DO UPDATE SET
                    blueprint_id   = EXCLUDED.blueprint_id,
                    status         = EXCLUDED.status,
                    started_at     = EXCLUDED.started_at,
                    finished_at    = EXCLUDED.finished_at,
                    module_results = EXCLUDED.module_results
                """,
                [run_id, self._manifest.blueprint_id, _iso(self._started_at)],
            )

        self._started = True

    def register_iteration(self, *, run_id: str, parent_run_id: str) -> None:
        """Register a multi-patch iteration's per-execute run_id.

        Multi-patch heal mints a fresh ``run_id`` per ``execute()`` call from
        iteration 1 onwards (iteration 0 reuses the outer/user-visible
        ``run_id``). The CLI calls this before each non-first ``execute()`` so
        the subsequent ``record()`` knows which outer ``run_id`` to stamp into
        ``run_records.parent_run_id``. Without it, the row would write
        ``parent_run_id=NULL`` and joins like
        ``WHERE COALESCE(parent_run_id, run_id) = '<outer>'`` would miss
        iteration 1+.
        """
        self._iteration_parents[run_id] = parent_run_id

    def record(
        self,
        result: ExecutionResult,
        exc: Exception | None = None,
        patched: bool = False,
    ) -> FailureContext | None:
        """Persist the final run outcome.

        Updates the ``run_records`` row to its terminal status.  On failure,
        also inserts a ``failure_contexts`` row and (if configured) fires a
        webhook in a daemon thread.

        Args:
            result: ``ExecutionResult`` from the Executor.
            exc:    Optional unhandled ``ExecuteError`` that escaped execute().
                    Used to populate ``stack_trace`` when the exception was not
                    caught inside the Executor loop.

        Returns:
            ``FailureContext`` if the run failed, else ``None``.
        """
        if not self._started or self._observability is None or self._run_id is None:
            raise RuntimeError("Surveyor.start() must be called before record()")

        finished_at = _utcnow()
        module_results_json = _redact(json.dumps(
            [{"module_id": r.module_id, "status": r.status, "error": r.error}
             for r in result.module_results]
        ))

        effective_status = "patched" if (patched and result.status == "success") else result.status
        # 1.1.0 fix — multi-patch heal mints a new run_id per iteration. The
        # outer run_id is INSERTed by start(); iteration 1+ never had a row
        # to UPDATE. Use INSERT-or-UPDATE so each iteration owns its row,
        # carrying parent_run_id back to the outer (registered via
        # register_iteration() before execute()).
        parent_for_iter = self._iteration_parents.get(result.run_id)
        with self._observability.connect() as cur:
            cur.execute(
                """
                INSERT INTO run_records
                    (run_id, blueprint_id, status, started_at, finished_at,
                     module_results, parent_run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (run_id) DO UPDATE SET
                    status         = EXCLUDED.status,
                    finished_at    = EXCLUDED.finished_at,
                    module_results = EXCLUDED.module_results,
                    parent_run_id  = COALESCE(run_records.parent_run_id, EXCLUDED.parent_run_id)
                """,
                [
                    result.run_id,
                    self._manifest.blueprint_id,
                    effective_status,
                    _iso(self._started_at),
                    _iso(finished_at),
                    module_results_json,
                    parent_for_iter,
                ],
            )

        if result.status == "success":
            return None

        # ── Build FailureContext ───────────────────────────────────────────────
        # When execute() catches the failure internally and reports
        # via ModuleResult (the common case), `exc` here is None — fall back
        # to `ModuleResult.exception` for the first failed module so the
        # structured-error extractor still has the live exception (with its
        # __cause__ chain intact) to work with. Without this fallback, the
        # extractor only ran for failures that escaped execute() entirely.
        live_exc: BaseException | None = exc
        if live_exc is None:
            for _mr in result.module_results:
                if _mr.status == "error" and getattr(_mr, "exception", None) is not None:
                    live_exc = _mr.exception
                    break
        stack_trace: str | None = None
        structured = _extract_structured_error(live_exc)
        if live_exc is not None:
            stack_trace = "".join(traceback.format_exception(type(live_exc), live_exc, live_exc.__traceback__))

        # Build provenance slice: failed module + full context block
        provenance_json: str | None = None
        pmap = getattr(self._manifest, "provenance_map", None)
        if pmap is not None:
            failed_mid = _first_failed_module(result)
            failed_mod_prov = pmap.for_module(failed_mid)
            prov_slice = {
                "blueprint_id": pmap.blueprint_id,
                "blueprint_path": pmap.blueprint_path,
                "failed_module": failed_mod_prov.to_dict() if failed_mod_prov else None,
                "context": {k: v.to_dict() for k, v in pmap.context.items()},
            }
            provenance_json = json.dumps(prov_slice, indent=2)

        blueprint_source_yaml: str | None = None
        if self._blueprint_path is not None:
            try:
                blueprint_source_yaml = self._blueprint_path.read_text(encoding="utf-8")
            except OSError:
                pass

        # Phase 39 — externalise fat columns to compressed blobs so the DB
        # row stores only a relative path (DuckDB row width drops ~10×).
        # Postgres is unaffected — TOAST handles large JSON natively.
        _blob_root = self._store_dir if self._store_dir is not None else None
        _manifest_json = _redact(json.dumps(self._manifest.to_dict()))
        _stack_trace_str = _redact(stack_trace) or ""
        _prov_json = _redact(provenance_json) or ""
        if _blob_root is not None:
            from aqueduct.surveyor.blob_store import externalise as _blob_ext
            _manifest_json = _blob_ext(_manifest_json, _blob_root, result.run_id, "manifest")
            _stack_trace_str = _blob_ext(_stack_trace_str, _blob_root, result.run_id, "stack")
            _prov_json = _blob_ext(_prov_json, _blob_root, result.run_id, "prov")

        ctx = FailureContext(
            run_id=result.run_id,
            blueprint_id=self._manifest.blueprint_id,
            failed_module=_first_failed_module(result),
            error_message=_redact(_first_error_message(result, exc)),
            stack_trace=_stack_trace_str,
            manifest_json=_manifest_json,
            started_at=_iso(self._started_at),  # type: ignore[arg-type]
            finished_at=_iso(finished_at),
            provenance_json=_prov_json,
            blueprint_source_yaml=_redact(blueprint_source_yaml),
            error_type=_first_error_type(result),
            error_class=(structured or {}).get("error_class"),
            root_exception=(structured or {}).get("root_exception"),
            sql_state=(structured or {}).get("sql_state"),
            suggested_columns=tuple((structured or {}).get("suggested_columns") or ()),
            object_name=(structured or {}).get("object_name"),
        )

        with self._observability.connect() as cur:
            cur.execute(
                """
                INSERT INTO failure_contexts
                    (run_id, blueprint_id, failed_module, error_message,
                     stack_trace, manifest_json, provenance_json, started_at, finished_at,
                     error_class, root_exception, sql_state, suggested_columns, object_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (run_id) DO UPDATE SET
                    blueprint_id      = EXCLUDED.blueprint_id,
                    failed_module     = EXCLUDED.failed_module,
                    error_message     = EXCLUDED.error_message,
                    stack_trace       = EXCLUDED.stack_trace,
                    manifest_json     = EXCLUDED.manifest_json,
                    provenance_json   = EXCLUDED.provenance_json,
                    started_at        = EXCLUDED.started_at,
                    finished_at       = EXCLUDED.finished_at,
                    error_class       = EXCLUDED.error_class,
                    root_exception    = EXCLUDED.root_exception,
                    sql_state         = EXCLUDED.sql_state,
                    suggested_columns = EXCLUDED.suggested_columns,
                    object_name       = EXCLUDED.object_name
                """,
                [
                    ctx.run_id,
                    ctx.blueprint_id,
                    ctx.failed_module,
                    ctx.error_message,
                    ctx.stack_trace,
                    ctx.manifest_json,
                    ctx.provenance_json,
                    ctx.started_at,
                    ctx.finished_at,
                    ctx.error_class,
                    json.dumps(ctx.root_exception) if ctx.root_exception else None,
                    ctx.sql_state,
                    json.dumps(list(ctx.suggested_columns)) if ctx.suggested_columns else None,
                    ctx.object_name,
                ],
            )

        if self._webhook_config:
            attempt = sum(1 for mr in result.module_results if mr.status == "error")
            template_vars = {
                "run_id": ctx.run_id,
                "blueprint_id": ctx.blueprint_id,
                "blueprint_name": self._manifest.name,
                "failed_module": ctx.failed_module,
                "error_message": ctx.error_message,
                "error_type": (ctx.stack_trace or "").splitlines()[0] if ctx.stack_trace else "ExecuteError",
                "started_at": ctx.started_at,
                "attempt": str(attempt),
            }
            fire_webhook(self._webhook_config, ctx.to_dict(), template_vars, event="on_failure")

        return ctx

    def record_healing_outcome(
        self,
        *,
        run_id: str,
        failed_module: str | None,
        failure_category: str | None,
        model: str | None,
        patch_id: str | None,
        confidence: float | None,
        patch_applied: bool,
        run_success_after_patch: bool,
        prompt_version: str | None = None,
        parent_run_id: str | None = None,
        failure_signature: str | None = None,
        resolution: str = "llm",
        model_cascade_position: int | None = None,
    ) -> None:
        """Persist one LLM healing attempt to healing_outcomes table.

        Phase 33 Part A: `prompt_version` defaults to the current engine
        constant (`agent.PROMPT_VERSION`) when not provided. Stored on every
        row so the version↔outcome correlation the docs claim is finally
        answerable in SQL.

        ``parent_run_id`` (optional): the user-visible outer run_id when the
        multi-patch loop minted a per-iteration ``run_id`` for ``execute()``.
        Lets cross-iteration queries (``WHERE parent_run_id = '<outer>'``)
        retrieve every outcome from the same heal call. NULL when caller is
        not in the multi-patch loop.

        Phase 45: ``failure_signature`` is the exact signature hash of the
        pipeline failure this heal addressed; ``resolution`` says how it was
        resolved — ``"llm"`` fresh agent patch, ``"cached"`` pending-patch
        reuse, ``"replayed"`` zero-token replay of an archived patch.
        """
        if self._observability is None:
            return
        import datetime as _dt
        import uuid as _uuid
        if prompt_version is None:
            from aqueduct.agent import PROMPT_VERSION as _PROMPT_VERSION
            prompt_version = _PROMPT_VERSION
        with self._observability.connect() as cur:
            cur.execute(
                """
                INSERT INTO healing_outcomes
                (id, run_id, parent_run_id, failed_module, failure_category, model, patch_id,
                 confidence, patch_applied, run_success_after_patch, applied_at, prompt_version,
                 failure_signature, resolution, model_cascade_position)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    str(_uuid.uuid4()),
                    run_id, parent_run_id, failed_module, failure_category, model, patch_id,
                    confidence, patch_applied, run_success_after_patch,
                    _dt.datetime.now(_dt.timezone.utc).isoformat(),
                    prompt_version,
                    failure_signature, resolution, model_cascade_position,
                ],
            )

    def successful_patch_ids(self) -> set[str]:
        """patch_ids with at least one ``run_success_after_patch = true`` row.

        Feeds the Phase 45 replay cache: only patches that demonstrably fixed
        a run are eligible for zero-token replay. Best-effort — store errors
        return an empty set (replay silently disabled, heal proceeds via LLM).
        """
        if self._observability is None:
            return set()
        try:
            with self._observability.connect() as cur:
                rows = cur.execute(
                    """
                    SELECT DISTINCT patch_id FROM healing_outcomes
                     WHERE run_success_after_patch AND patch_id IS NOT NULL
                    """
                ).fetchall()
            return {r[0] for r in rows if r and r[0]}
        except Exception:
            logger.debug("successful_patch_ids query failed", exc_info=True)
            return set()

    def record_heal_attempt(
        self,
        *,
        run_id: str,
        attempt_record: Any,                # agent.budget.AttemptRecord
        stop_reason: str | None = None,
        prompt_version: str | None = None,
    ) -> None:
        """Persist one row to ``heal_attempts`` (Task 88).

        Called from the unified reprompt loop's ``on_attempt`` hook after
        every LLM turn (success or failure). Best-effort — any DB-side
        failure is swallowed so a persistence problem cannot abort an
        otherwise-successful heal.

        Note on ``stop_reason``: when set, the value describes WHY the LLM
        loop terminated (e.g. ``solved`` = the model returned a parseable
        PatchSpec). It does NOT mean the heal actually fixed the pipeline —
        the patch can still be rejected downstream by sandbox/lineage/apply
        gates. To check whether the heal actually worked, join against
        ``healing_outcomes.run_success_after_patch``. In normal use the
        ``on_attempt`` hook inserts each row with ``stop_reason=NULL`` and
        the terminal row's stop_reason is attached afterwards via
        ``update_heal_attempt_stop_reason`` (a second
        ``record_heal_attempt`` call would write a duplicate row).
        """
        if self._observability is None:
            return
        import datetime as _dt
        import uuid as _uuid
        if prompt_version is None:
            from aqueduct.agent import PROMPT_VERSION as _PROMPT_VERSION
            prompt_version = _PROMPT_VERSION
        sig = getattr(attempt_record, "signature", None)
        try:
            with self._observability.connect() as cur:
                cur.execute(
                    """
                    INSERT INTO heal_attempts
                    (id, run_id, attempt_num, error_class, where_field,
                     normalized_message, signature_hash, tokens_in, tokens_out,
                     latency_ms, gate_that_rejected, escalated, stop_reason,
                     prompt_version, recorded_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        str(_uuid.uuid4()),
                        run_id,
                        int(getattr(attempt_record, "attempt_num", 0) or 0),
                        sig.error_class if sig else None,
                        sig.where if sig else None,
                        sig.normalized_message if sig else None,
                        sig.hash if sig else None,
                        int(getattr(attempt_record, "tokens_in", 0) or 0),
                        int(getattr(attempt_record, "tokens_out", 0) or 0),
                        int(getattr(attempt_record, "latency_ms", 0) or 0),
                        getattr(attempt_record, "gate_that_rejected", None),
                        bool(getattr(attempt_record, "escalated", False)),
                        stop_reason,
                        prompt_version,
                        _dt.datetime.now(_dt.timezone.utc).isoformat(),
                    ],
                )
        except Exception:
            logger.debug("record_heal_attempt failed", exc_info=True)

    def update_heal_attempt_stop_reason(
        self,
        *,
        run_id: str,
        attempt_num: int,
        stop_reason: str,
    ) -> None:
        """Update the terminal `heal_attempts` row's ``stop_reason`` in place.

        The unified reprompt loop's ``on_attempt`` hook INSERTs a row per
        attempt with ``stop_reason=NULL``. After the loop returns we want to
        attach the final stop reason to the LAST row for this ``(run_id,
        attempt_num)`` pair — NOT insert a duplicate row (which is what
        ``record_heal_attempt`` would do, since it always allocates a fresh
        UUID ``id``).

        ``stop_reason`` describes WHY the LLM loop terminated (e.g. it
        returned a parseable PatchSpec → ``solved``). It does NOT mean the
        heal actually fixed the pipeline — to answer that, join against
        ``healing_outcomes.run_success_after_patch``.

        Best-effort: swallows DB errors at DEBUG, never blocks the caller.
        """
        if self._observability is None:
            return
        try:
            with self._observability.connect() as cur:
                # DuckDB lacks correlated UPDATE-with-LIMIT; constrain by the
                # most-recent recorded_at to pick the terminal row only.
                cur.execute(
                    """
                    UPDATE heal_attempts
                       SET stop_reason = ?
                     WHERE id = (
                         SELECT id FROM heal_attempts
                          WHERE run_id = ? AND attempt_num = ?
                          ORDER BY recorded_at DESC
                          LIMIT 1
                     )
                    """,
                    [stop_reason, run_id, int(attempt_num)],
                )
        except Exception:
            logger.debug("update_heal_attempt_stop_reason failed", exc_info=True)

    def record_patch_simulation(
        self,
        *,
        patch_id: str,
        gate: str,
        status: str,
        detail: str | None = None,
        sample_rows: int | None = None,
        duration_ms: int | None = None,
        run_id: str | None = None,
        blueprint_id: str | None = None,
    ) -> None:
        """Append one row to `observability.patch_simulation`.

        Called by Phase 29a `aqueduct patch preview` and by the `auto` mode
        self-healing loop after each Gate 2 (lineage diff) and Gate 3 (sandbox
        replay) evaluation. The table is the audit trail for "why we accepted
        or rejected this patch without running the full pipeline."

        Args:
            patch_id:     PatchSpec identifier — matches `patches/applied/{id}.json`.
            gate:         `"lineage"` (lineage) or `"sandbox"` (sandbox replay).
            status:       `"pass"` | `"fail"` | `"warn"` | `"skip"`.
            detail:       Free-text reason — failing rule, missing column, etc.
            sample_rows:  Rows processed in the sandbox replay (NULL for gate2).
            duration_ms:  Wall-clock for the gate evaluation.
            run_id:       Originating failed run if invoked from the healing loop.
            blueprint_id: Blueprint identifier; defaults to the Surveyor's manifest id.
        """
        if self._observability is None:
            return
        import datetime as _dt
        import uuid as _uuid
        with self._observability.connect() as cur:
            cur.execute(
                """
                INSERT INTO patch_simulation
                  (id, run_id, blueprint_id, patch_id, gate, status, detail,
                   sample_rows, duration_ms, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    str(_uuid.uuid4()),
                    run_id,
                    blueprint_id or getattr(self._manifest, "blueprint_id", None),
                    patch_id,
                    gate,
                    status,
                    detail,
                    sample_rows,
                    duration_ms,
                    _dt.datetime.now(_dt.timezone.utc).isoformat(),
                ],
            )

    def record_explain_snapshot(
        self,
        *,
        run_id: str,
        module_id: str,
        exchange_count: int,
        python_udf_count: int,
        broadcast_count: int,
        plan_text: str,
        blueprint_id: str | None = None,
        keep_last_n: int = 5,
    ) -> None:
        """Append one row to `observability.explain_snapshot` for Gate 4 baseline.

        Phase 29b — physical-plan regression check. Captured per-module after
        each successful module execution. Rolling baseline: keeps the most
        recent `keep_last_n` runs per (blueprint_id, module_id), older rows
        are pruned to bound storage.

        Args:
            run_id:          Originating run.
            module_id:       Module the plan belongs to.
            exchange_count:  Count of `Exchange` nodes (shuffle proxy).
            python_udf_count:Count of `BatchEvalPython` nodes.
            broadcast_count: Count of `BroadcastExchange` nodes.
            plan_text:       Full `explain(mode="formatted")` text for debugging.
            blueprint_id:    Override Surveyor's manifest id.
            keep_last_n:     Pruning bound per (blueprint_id, module_id).
        """
        if self._observability is None:
            return
        import datetime as _dt
        bp_id = blueprint_id or getattr(self._manifest, "blueprint_id", None)
        if not bp_id:
            return
        with self._observability.connect() as cur:
            cur.execute(
                """
                INSERT INTO explain_snapshot
                  (blueprint_id, run_id, module_id, captured_at,
                   exchange_count, python_udf_count, broadcast_count, plan_text)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (blueprint_id, run_id, module_id) DO UPDATE SET
                    captured_at      = EXCLUDED.captured_at,
                    exchange_count   = EXCLUDED.exchange_count,
                    python_udf_count = EXCLUDED.python_udf_count,
                    broadcast_count  = EXCLUDED.broadcast_count,
                    plan_text        = EXCLUDED.plan_text
                """,
                [
                    bp_id, run_id, module_id,
                    _dt.datetime.now(_dt.timezone.utc).isoformat(),
                    exchange_count, python_udf_count, broadcast_count, plan_text,
                ],
            )
            # Rolling prune — keep last N run_ids per (blueprint_id, module_id)
            try:
                rows = cur.execute(
                    """
                    SELECT run_id FROM explain_snapshot
                    WHERE blueprint_id = ? AND module_id = ?
                    ORDER BY captured_at DESC
                    """,
                    [bp_id, module_id],
                ).fetchall()
                if len(rows) > keep_last_n:
                    stale = [r[0] for r in rows[keep_last_n:]]
                    for rid in stale:
                        cur.execute(
                            "DELETE FROM explain_snapshot WHERE blueprint_id=? AND module_id=? AND run_id=?",
                            [bp_id, module_id, rid],
                        )
            except Exception:
                pass

    def latest_explain_snapshots(
        self,
        *,
        blueprint_id: str | None = None,
    ) -> dict[str, dict]:
        """Return most-recent per-module snapshot for the blueprint.

        Returns mapping `module_id` → `{exchange_count, python_udf_count,
        broadcast_count, plan_text, run_id, captured_at}`. Used by the explain gate
        as the pre-patch baseline.
        """
        if self._observability is None:
            return {}
        bp_id = blueprint_id or getattr(self._manifest, "blueprint_id", None)
        if not bp_id:
            return {}
        out: dict[str, dict] = {}
        with self._observability.connect() as cur:
            rows = cur.execute(
                """
                SELECT module_id, run_id, captured_at, exchange_count,
                       python_udf_count, broadcast_count, plan_text
                FROM explain_snapshot
                WHERE blueprint_id = ?
                ORDER BY module_id, captured_at DESC
                """,
                [bp_id],
            ).fetchall()
        seen: set[str] = set()
        for r in rows:
            mid = r[0]
            if mid in seen:
                continue
            seen.add(mid)
            out[mid] = {
                "run_id": r[1],
                "captured_at": r[2],
                "exchange_count": r[3],
                "python_udf_count": r[4],
                "broadcast_count": r[5],
                "plan_text": r[6],
            }
        return out

    def count_recent_heal_attempts(self, within_minutes: int = 60) -> int:
        """Return the number of LLM healing attempts recorded in this blueprint's
        `healing_outcomes` table within the last ``within_minutes`` minutes.

        Used by the CLI self-healing loop to enforce a configurable spend-cap
        (``agent.max_heal_attempts_per_hour``). Each blueprint has its own
        `observability.db`, so the count is implicitly scoped to this blueprint without
        a JOIN against `run_records`.

        Returns 0 when the connection is not open (e.g. before `start()`).
        applied_at is ISO-8601 UTC which sorts lexicographically — direct
        string comparison is safe.
        """
        if self._observability is None:
            return 0
        import datetime as _dt
        threshold = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(minutes=within_minutes)).isoformat()
        try:
            with self._observability.connect() as cur:
                row = cur.execute(
                    "SELECT COUNT(*) FROM healing_outcomes WHERE applied_at >= ?",
                    [threshold],
                ).fetchone()
            return int(row[0]) if row else 0
        except Exception:
            return 0

    def evaluate_regulator(self, regulator_id: str) -> bool:
        """Evaluate whether a Regulator's gate is open (True) or closed (False).

        Finds the Probe wired to the Regulator via a signal-port edge, queries
        the latest ``passed`` field from its signals in the current run, and
        applies boolean coercion per spec:

          - ``passed=True`` or key absent or no signals → open (True)
          - ``passed=False``                            → closed (False)
          - any exception                               → open (True)

        Args:
            regulator_id: Module ID of the active Regulator to evaluate.

        Returns:
            True if the gate is open (downstream should execute).
            False if the gate is closed (on_block applies).
        """
        if self._run_id is None or self._observability is None:
            return True  # start() not called

        # Find the probe wired to this regulator's signal port
        probe_ids = [
            e.from_id
            for e in self._manifest.edges
            if e.to_id == regulator_id and e.port == "signal"
        ]
        if not probe_ids:
            return True  # no signal source → open

        probe_id = probe_ids[0]

        try:
            with self._observability.connect() as cur:
                cur.execute(_SIGNAL_OVERRIDES_DDL)
                # Persistent override takes priority over run-scoped probe data
                override_row = cur.execute(
                    "SELECT passed FROM signal_overrides WHERE signal_id = ?",
                    [probe_id],
                ).fetchone()
                if override_row is not None:
                    return bool(override_row[0])

                # Fall back to run-scoped probe signals
                try:
                    rows = cur.execute(
                        """
                        SELECT payload
                        FROM probe_signals
                        WHERE probe_id = ? AND run_id = ?
                        ORDER BY captured_at DESC
                        """,
                        [probe_id, self._run_id],
                    ).fetchall()
                except Exception:
                    return True  # probe_signals table not yet created → open

            # Walk rows newest-first, look for first payload containing 'passed'
            for (payload_raw,) in rows:
                payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw
                if isinstance(payload, dict) and "passed" in payload:
                    passed = payload["passed"]
                    if passed is None:
                        return True
                    return bool(passed)

            return True  # no signal with 'passed' key → open

        except Exception:
            return True  # error → open per spec

    def get_probe_signal(
        self,
        probe_id: str,
        signal_type: str | None = None,
    ) -> list[dict]:
        """Return probe signal payloads for a given probe.

        Opens a fresh read connection to ``store_dir/observability.db``.  Returns
        an empty list if the observability DB does not exist yet.

        Args:
            probe_id:    The Probe module ID to query.
            signal_type: Optional filter — if given, only rows of that type
                         are returned.

        Returns:
            List of dicts: ``{"run_id", "probe_id", "signal_type", "payload",
            "captured_at"}``.  ``payload`` is already deserialised (dict).
        """
        import json as _json

        if self._observability is None:
            from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
            observability_path = self._store_dir / "observability.db"
            if not observability_path.exists():
                return []
            self._observability = DuckDBObservabilityStore(observability_path)

        try:
            with self._observability.connect() as cur:
                if signal_type is not None:
                    rows = cur.execute(
                        """
                        SELECT run_id, probe_id, signal_type, payload,
                               CAST(captured_at AS VARCHAR) AS captured_at
                        FROM probe_signals
                        WHERE probe_id = ? AND signal_type = ?
                        ORDER BY captured_at DESC
                        """,
                        [probe_id, signal_type],
                    ).fetchall()
                else:
                    rows = cur.execute(
                        """
                        SELECT run_id, probe_id, signal_type, payload,
                               CAST(captured_at AS VARCHAR) AS captured_at
                        FROM probe_signals
                        WHERE probe_id = ?
                        ORDER BY captured_at DESC
                        """,
                        [probe_id],
                    ).fetchall()
        except Exception:
            return []

        return [
            {
                "run_id": r[0],
                "probe_id": r[1],
                "signal_type": r[2],
                "payload": _json.loads(r[3]) if isinstance(r[3], str) else r[3],
                "captured_at": str(r[4]),
            }
            for r in rows
        ]

    def stop(self) -> None:
        """No-op for the store-abstraction era.

        Pre-Phase 28 the Surveyor held an open DuckDB connection for the
        run lifetime; the new abstraction acquires connections per
        operation (pooled for Postgres, opened/closed for DuckDB) so there
        is nothing to close at the Surveyor level. Method retained for
        backwards-compatible call sites.
        """
        self._started = False