"""Execution result models — frozen dataclasses returned by the Executor."""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from aqueduct.models import ModuleType

logger = logging.getLogger(__name__)

# ── module_metrics DDL + writer (Phase 81 step 3) ───────────────────────────
#
# Historically only Spark's executor (`spark/executor.py`) wrote this table —
# DuckDB's Stage A executor writes NO per-module metrics at all (a documented
# gap: "per-module runtime metrics persistence to the observability store...
# left to the caller", see that module's docstring). The Handoff module
# (`aqueduct.compiler.handoff`) needs a `bytes_written`/`bytes_read` row on
# EITHER engine — the boundary might be written by DuckDB and read by Spark,
# or vice versa — so this is DuckDB's first module_metrics write, scoped
# ONLY to the Handoff module (not a retrofit of metrics for every DuckDB
# module type, which stays out of this step's scope). Spark's own
# `_write_stage_metrics`/`_MODULE_METRICS_DDL` are UNCHANGED and unrelated to
# this — the DDL text is intentionally duplicated (not imported) rather than
# risk pulling `spark/executor.py` (which imports `pyspark` at module scope)
# into a pyspark-free file.
MODULE_METRICS_DDL = """
CREATE TABLE IF NOT EXISTS module_metrics (
    run_id        VARCHAR     NOT NULL,
    module_id     VARCHAR     NOT NULL,
    records_read  BIGINT,
    bytes_read    BIGINT,
    records_written BIGINT,
    bytes_written BIGINT,
    duration_ms   BIGINT,
    captured_at   TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_module_metrics_module
    ON module_metrics (module_id);
-- Phase 85 E2 — the actual read pattern (`report --profile`,
-- aqueduct/stores/queries.py:270,280) filters by run_id, not module_id;
-- module_id-only left every profile lookup doing a full table scan. Both
-- indexes stay — module_id still serves the cross-run per-module trend
-- query (`report --profile --blueprint <id> --last N`).
CREATE INDEX IF NOT EXISTS idx_module_metrics_run
    ON module_metrics (run_id);
"""


def resolve_observability_store(store_dir: Any, observability_store: Any) -> Any:
    """Return the supplied observability-store backend, or build a default
    local DuckDB one from ``store_dir``, or ``None`` if neither is available.

    Engine-agnostic — hoisted out of ``spark/executor.py`` (where an
    identical private copy lives, now aliased to this one) so DuckDB's
    executor and the Phase 81 step 3 orchestrator can resolve a store
    without importing pyspark.
    """
    if observability_store is not None:
        return observability_store
    if store_dir is None:
        return None
    from pathlib import Path

    from aqueduct.stores.duckdb_ import DuckDBObservabilityStore

    store_dir = Path(store_dir)
    store_dir.mkdir(parents=True, exist_ok=True)
    return DuckDBObservabilityStore(store_dir / "observability.db")


def write_module_metrics(
    store_dir: Any,
    observability_store: Any,
    run_id: str,
    module_id: str,
    metrics: dict[str, Any],
) -> None:
    """Persist one ``module_metrics`` row (non-fatal — metrics writing must
    never abort a run). Engine-agnostic; see ``MODULE_METRICS_DDL`` above."""
    store = resolve_observability_store(store_dir, observability_store)
    if store is None:
        return
    try:
        from datetime import UTC, datetime

        with store.connect() as cur:
            cur.execute(MODULE_METRICS_DDL)
            cur.execute(
                """
                INSERT INTO module_metrics
                    (run_id, module_id, records_read, bytes_read,
                     records_written, bytes_written, duration_ms, captured_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    module_id,
                    metrics.get("records_read"),
                    metrics.get("bytes_read"),
                    metrics.get("records_written"),
                    metrics.get("bytes_written"),
                    metrics.get("duration_ms"),
                    datetime.now(tz=UTC).isoformat(),
                ],
            )
    except Exception as exc:
        logger.debug("write_module_metrics failed for %r: %s", module_id, exc)


def manifest_hash(manifest: Any) -> str:
    """Deterministic content hash of a compiled Manifest.

    Engine-agnostic (only calls ``manifest.to_dict()``) — the single source
    of truth both engines' executors use for checkpoint/resume hash
    comparison (``spark/executor.py``, ``duckdb_/executor.py`` each used to
    define an identical private copy of this function; hoisted here so the
    two can never silently diverge) and that the Phase 81 step 3 handoff
    spill-path orchestrator (``aqueduct/executor/orchestrator.py``) uses to
    lay out ``<root>/<manifest_hash>/<run_id>/<edge_id>/`` — the SAME hash
    for the SAME compiled Manifest regardless of which engine(s) a run
    touches.
    """
    import hashlib
    import json as _json

    return hashlib.sha256(_json.dumps(manifest.to_dict(), sort_keys=True).encode()).hexdigest()[:12]


class ExecutionStatus(StrEnum):
    """Module/blueprint execution outcome. Subclasses ``str`` so stored/serialized
    values (manifests, DDL rows, JSON output) are unchanged — ``ExecutionStatus.SUCCESS
    == "success"`` and ``str(ExecutionStatus.SUCCESS) == "success"``. Comparison sites
    should use the enum members; ``ModuleResult.status`` / ``ExecutionResult.status``
    stay typed ``str`` since callers may still hand in plain strings."""

    SUCCESS = "success"
    ERROR = "error"
    SKIPPED = "skipped"
    PATCHED = "patched"  # run-level effective status (succeeded after an applied patch); never set on a ModuleResult


# The terminal `run_records.status` values that mean "this run of the
# Blueprint completed its work". `patched` belongs here and is NOT an
# optional nicety: `Surveyor.record()` stamps `patched` (never `success`)
# on the iteration that succeeded after a heal applied a patch, so any
# reader that accepts only `success` silently misses the single most common
# way an Aqueduct pipeline recovers. Read by the handoff-spill retention
# rule (`executor/spill.py`) and by perf attribution
# (`patch/perf_attribution.py`); those two must not drift apart, which is
# why the tuple lives here beside the enum rather than being restated.
SUCCEEDED_STATUSES: tuple[str, ...] = (
    ExecutionStatus.SUCCESS.value,
    ExecutionStatus.PATCHED.value,
)


def concise_error(text: str | None, *, limit: int = 300) -> str:
    """First line of an error string, capped — for **display** only.

    Spark `AnalysisException` messages put the root cause on the first line and
    then dump the whole Catalyst plan tree on the following lines. Logs and the
    CLI status line want only the root cause; the full text is still stored
    verbatim in `ModuleResult.error` (the obs store + the heal agent need it).
    """
    s = (text or "").strip().split("\n", 1)[0].strip()
    return s if len(s) <= limit else s[: limit - 1] + "…"


@dataclass(frozen=True)
class ModuleResult:
    """Outcome of executing a single module."""

    module_id: str
    status: str  # "success" | "error" | "skipped"
    error: str | None = None
    error_type: str | None = None  # user-defined label from Assert rule's error_type field
    exception: BaseException | None = None
    """Live exception object when status == "error" — preserves the original
    PySparkException / Py4JJavaError / `__cause__` chain so
    ``Surveyor.record()`` can hand it to ``_extract_structured_error`` for
    Phase 35 structured fields (error_class, object_name, suggested_columns,
    sql_state, root_exception). Stringified ``error`` alone loses the chain.
    Not serialized to JSON — see ``ExecutionResult.to_dict``."""
    warnings: tuple[tuple[str, str], ...] = ()
    """Per-module runtime warnings collected during execution.
    Each entry is (rule_id, message).  Engine-agnostic — no pyspark."""
    notes: tuple[str, ...] = ()
    """Informational output lines (NOT warnings — never counted in the
    runtime warning roll-up). Today: Probe `report: stdout` signal results,
    printed dim under the module's summary row (capped unless -v)."""


@dataclass(frozen=True)
class ExecutionResult:
    """Aggregate outcome of a full blueprint run."""

    blueprint_id: str
    run_id: str
    status: str  # "success" | "error"
    module_results: tuple[ModuleResult, ...]
    trigger_agent: bool = False  # LLM loop should fire even if approval=disabled
    failed_engine: str | None = None
    """Set only by ``aqueduct.executor.orchestrator.run_polyglot()`` when
    ``status == "error"`` — the engine name of the ISLAND that actually
    failed, not the run's nominal ``deployment.engine`` default. A
    single-engine ``execute()`` call never sets this (stays ``None``), so a
    caller can pass it straight through as ``Surveyor.record(engine=...)``'s
    override unconditionally: ``None`` there already means "use my own
    construction-time engine", which is exactly today's single-engine
    behavior. Not part of ``to_dict()`` — this is an in-process routing
    signal for the CLI healing loop, not a persisted/serialized field."""

    def to_dict(self) -> dict:
        return {
            "blueprint_id": self.blueprint_id,
            "run_id": self.run_id,
            "status": self.status,
            "trigger_agent": self.trigger_agent,
            "module_results": [
                {
                    "module_id": r.module_id,
                    "status": r.status,
                    "error": r.error,
                    "error_type": r.error_type,
                    "warnings": [list(w) for w in r.warnings],
                    "notes": list(r.notes),
                }
                for r in self.module_results
            ],
        }


# ── Thread-safe per-module warning collector ──────────────────────────────
# probe / assert code runs inside a module execution context and appends
# (rule_id, message) pairs here.  The executor clears this before each module
# and gathers the accumulated warnings when constructing ModuleResult.
# Using threading.local() keeps parallel-component threads isolated.

_collector = threading.local()


def _add_module_warning(rule_id: str, message: str) -> None:
    """Append a runtime warning to the current module's warning list.

    Checks `aqueduct.warnings.is_suppressed()` first (Phase 85 F-15) — the
    SAME suppress-set predicate `aqueduct.warnings.emit()` uses — so a
    suppressed `rule_id` never enters the list at all. This is deliberately
    NOT a call into `warnings.warn()`/`emit()` itself: this collector is
    read directly by `run.py` (inline `↳ [rule_id]` lines nested under each
    module, plus the end-of-run roll-up) rather than gathered via
    `warnings.catch_warnings()`, and `run.py` installs a process-global
    `warnings.filterwarnings("ignore", category=AqueductWarning)` partway
    through a run (to stop already-shown compile warnings re-emitting on
    every heal recompile) that would silently swallow anything routed
    through the real warnings module during execution. Checking suppression
    HERE, at the point of collection, is also what keeps
    `ModuleResult.warnings` itself free of suppressed entries — a render-time
    filter would still expose them to any other consumer of that tuple.
    """
    from aqueduct.warnings import is_suppressed

    if is_suppressed(rule_id):
        return
    if not hasattr(_collector, "warnings"):
        _collector.warnings = []
    _collector.warnings.append((rule_id, message))


def _collect_module_warnings() -> tuple[tuple[str, str], ...]:
    """Gather and reset the per-module warning list."""
    ws: list[tuple[str, str]] = getattr(_collector, "warnings", []) or []
    _collector.warnings = []
    return tuple(ws)


# ── Module types testable in isolation (`aqueduct test`) ───────────────────
# Which module TYPES support standalone testing is a property of the type
# itself (a Channel/Junction/Funnel/Assert has a well-defined input->output
# contract that can run against fixture data; an Ingress/Egress/Probe/
# Regulator/Arcade does not), not of which engine runs it — so this lives
# here, engine-agnostic, rather than only inside spark/test_runner.py.
# Audit-fixed 2026-08: aqueduct/agent/regression_artifact.py previously
# imported test_runner.py's PRIVATE `_TESTABLE_TYPES` directly
# (`from aqueduct.executor.spark.test_runner import _TESTABLE_TYPES`) — a
# private-symbol cross-module import that also violated AGENTS.md's "do
# NOT re-add an engine import to aqueduct/agent/" rule (agent/ resolves
# engine specifics through the registry, never a direct import by name).
# spark/test_runner.py re-exports this under its historical private name
# so its own internal call sites are unchanged.
TESTABLE_MODULE_TYPES: frozenset[ModuleType] = frozenset(
    {ModuleType.Channel, ModuleType.Junction, ModuleType.Funnel, ModuleType.Assert}
)
