"""Executor orchestrator — runs a Manifest against a live SparkSession.

Supported module types: Ingress, Egress, Channel, Junction, Funnel, Probe, Regulator.
Raises ExecuteError immediately on any other unsupported module type.

Execution model:
  1. Register all UDFs from manifest.udf_registry before any module executes.
  2. Topological sort of non-Probe modules by data-flow edges.
     Probes are inserted immediately after their attach_to module so that
     Regulator evaluation sees fresh signals.
  3. Walk sorted order:
     - Ingress    → read_ingress()                    → frame_store[module.id]
     - Channel    → execute_channel(…)            → frame_store[module.id]
                    If spillway_condition set and a spillway edge exists:
                      frame_store[module.id]              = good rows
                      frame_store["module.id.spillway"]   = error rows (+_aq_error_*)
     - Junction   → execute_junction(…)               → frame_store["id.branch"]
     - Funnel     → execute_funnel(…)                 → frame_store[module.id]
     - Regulator  → evaluate gate; open → pass-through; closed → on_block action
     - Egress     → write_egress(frame_store[key], …)
     - Probe      → execute_probe(…) persists signals (+ returns `report: stdout`
                    note lines); never halts blueprint
     - Other      → ExecuteError (unsupported)
  4. Return ExecutionResult (frozen).

Frame store key convention:
  "main" port edge     → key = from_id
  branch port edge     → key = f"{from_id}.{port}"
  spillway port edge   → key = f"{from_id}.spillway"

Signal ports ("signal") carry control-only signals and are excluded from
the topo-sort and frame-store lookups. Spillway IS a data edge.

Regulator skip propagation:
  When a Regulator's gate is closed with on_block=skip, _GATE_CLOSED is
  placed in the frame_store.  Every downstream module that receives this
  sentinel skips itself (status="skipped") and propagates the sentinel,
  so the entire skipped sub-graph is recorded without aborting the blueprint.

Parallel execution (--parallel flag):
  When parallel=True, the executor identifies independent connected components
  in the data-flow DAG and submits each component to a ThreadPoolExecutor.
  Each component runs its modules serially in its own thread. frame_store
  writes are safe without locking because independent components write to
  disjoint keys. module_results and _ingress_obs are merged under a lock
  after each component completes (or fails). The first failure sets a
  cancel_event; remaining components skip their pending modules.

No Spark actions beyond the Egress .save() and Probe sampling calls.
"""

from __future__ import annotations

import concurrent.futures
import logging
import random
import threading
import time
import uuid
from collections import defaultdict, deque
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from aqueduct.executor.spark.probe import ProbeSampling

from datetime import UTC

from aqueduct.errors import AqueductError
from aqueduct.executor.models import (
    ExecutionResult,
    ExecutionStatus,
    ModuleResult,
    _collect_module_warnings,
    concise_error,
)
from aqueduct.executor.spark.assert_ import AssertError, execute_assert
from aqueduct.executor.spark.channel import ChannelError, execute_channel
from aqueduct.executor.spark.egress import EgressError, run_maintenance, write_egress
from aqueduct.executor.spark.error_columns import (
    AQ_ERROR_MODULE,
    AQ_ERROR_MSG,
    AQ_ERROR_TS,
    AQ_ERROR_TYPE,
)
from aqueduct.executor.spark.funnel import FunnelError, execute_funnel
from aqueduct.executor.spark.ingress import IngressError, read_ingress
from aqueduct.executor.spark.junction import JunctionError, execute_junction
from aqueduct.executor.spark.metrics import dir_bytes, get_observation, null_metrics, observe_df
from aqueduct.models import Edge, Manifest, Module, ModuleType, RetryPolicy

logger = logging.getLogger(__name__)

def _mr(**kwargs) -> ModuleResult:
    """Construct a ModuleResult, auto-collecting per-module runtime warnings."""
    return ModuleResult(**kwargs, warnings=_collect_module_warnings())

_T = TypeVar("_T")


# ── Retry helper ──────────────────────────────────────────────────────────────

def _is_retriable(exc: Exception, policy: RetryPolicy) -> bool:
    """Return True if this exception should be retried per policy."""
    msg = str(exc)
    # Non-transient patterns always block retry
    for pattern in policy.non_transient_errors:
        if pattern in msg:
            return False
    # If transient list is specified, only those patterns are retriable
    if policy.transient_errors:
        return any(p in msg for p in policy.transient_errors)
    # Default: all errors are potentially transient
    return True


def _backoff_seconds(attempt: int, policy: RetryPolicy) -> float:
    """Compute sleep duration for the given attempt number (0-indexed)."""
    strategy = policy.backoff_strategy
    base = float(policy.backoff_base_seconds)
    cap = float(policy.backoff_max_seconds)

    if strategy == "fixed":
        delay = base
    elif strategy == "linear":
        delay = base * (attempt + 1)
    else:  # exponential (default)
        delay = base * (2 ** attempt)

    delay = min(delay, cap)
    if policy.jitter:
        delay *= 0.5 + random.random() * 0.5  # uniform [0.5, 1.0] × delay
    return delay


def _with_retry(
    fn: Callable[[], _T],
    policy: RetryPolicy,
    module_id: str,
) -> _T:
    """Execute fn(), retrying per policy on retriable exceptions.

    Args:
        fn:        Zero-arg callable that executes one module attempt.
        policy:    RetryPolicy from the Manifest (blueprint-level).
        module_id: Used in log messages only.

    Returns:
        Result of fn() on success.

    Raises:
        The last exception if all attempts exhausted or deadline exceeded.
        ExecuteError if on_exhaustion=abort (wraps original exc).
    """
    first_failure_at: float | None = None
    last_exc: Exception = RuntimeError("no attempts made")

    for attempt in range(max(1, policy.max_attempts)):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            now = time.monotonic()

            if first_failure_at is None:
                first_failure_at = now

            # Deadline check
            if policy.deadline_seconds is not None:
                elapsed = now - first_failure_at
                if elapsed >= policy.deadline_seconds:
                    logger.warning(
                        "[runtime_retry_deadline] Module %r: deadline exceeded "
                        "(%.0fs elapsed, limit=%ds); not retrying.",
                        module_id, elapsed, policy.deadline_seconds,
                    )
                    break

            is_last = attempt == policy.max_attempts - 1
            if is_last or not _is_retriable(exc, policy):
                # Only narrate retry exhaustion when retries were actually
                # configured (max_attempts > 1). With the default of 1 there was
                # no retry to "give up" on — the module's ✗ summary line already
                # reports the failure, so a separate warning here just duplicates
                # the same error immediately above it.
                if policy.max_attempts > 1:
                    if is_last:
                        logger.warning(
                            "[runtime_retry_exhausted] Module %r: attempt %d/%d "
                            "failed (%s); giving up",
                            module_id, attempt + 1, policy.max_attempts, concise_error(str(exc)),
                        )
                    else:
                        logger.warning(
                            "[runtime_retry_non_retriable] Module %r: non-retriable "
                            "error on attempt %d/%d: %s",
                            module_id, attempt + 1, policy.max_attempts, concise_error(str(exc)),
                        )
                break

            sleep = _backoff_seconds(attempt, policy)
            logger.warning(
                "[runtime_retry_waiting] Module %r: attempt %d/%d failed (%s); "
                "retrying in %.1fs",
                module_id, attempt + 1, policy.max_attempts, concise_error(str(exc)), sleep,
            )
            time.sleep(sleep)

    raise last_exc

def _write_checkpoint(
    module: Module,
    checkpoint_dir: Path | None,
    manifest: Manifest,
    data: dict[str, Any] | None = None,
) -> None:
    """Write checkpoint Parquet + done marker when checkpoint is enabled."""
    if checkpoint_dir is None or not (manifest.checkpoint or module.checkpoint):
        return
    module_ckpt = checkpoint_dir / module.id
    module_ckpt.mkdir(parents=True, exist_ok=True)
    if data:
        for name, df in data.items():
            try:
                df.write.mode("overwrite").parquet(str(module_ckpt / name))
            except Exception as exc:
                logger.warning("[runtime_checkpoint_write_failed] Checkpoint write failed for %r/%s: %s", module.id, name, exc)
                return
    (module_ckpt / "_aq_done").write_text("", encoding="utf-8")
    logger.debug("Checkpoint written: %s", module_ckpt)


def _module_retry_policy(module: Module, manifest_policy: RetryPolicy) -> RetryPolicy:
    """Return per-module RetryPolicy if on_failure is set, else manifest-level policy."""
    if not module.on_failure:
        return manifest_policy
    try:
        return RetryPolicy(**module.on_failure)
    except TypeError as exc:
        raise ExecuteError(
            f"Module {module.id!r} on_failure has invalid keys: {exc}"
        ) from exc


def _module_checkpoint_enabled(module: Module, manifest: Manifest) -> bool:
    return manifest.checkpoint or module.checkpoint


def _manifest_hash(manifest: Manifest) -> str:
    import hashlib
    import json as _json
    return hashlib.sha256(
        _json.dumps(manifest.to_dict(), sort_keys=True).encode()
    ).hexdigest()[:12]


_MODULE_METRICS_DDL = """
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
"""

_MAINTENANCE_METRICS_DDL = """
CREATE TABLE IF NOT EXISTS maintenance_metrics (
    run_id        VARCHAR     NOT NULL,
    module_id     VARCHAR     NOT NULL,
    optimize_ms   BIGINT,
    vacuum_ms     BIGINT,
    captured_at   TIMESTAMPTZ NOT NULL
);
"""


def _resolve_observability_store(store_dir: Path | None, observability_store: Any) -> Any:
    """Return the supplied observability-store backend or build a default DuckDB one.

    Internal helper so probe/metric writers can stay backend-agnostic without
    every call site duplicating the construction logic.
    """
    if observability_store is not None:
        return observability_store
    if store_dir is None:
        return None
    from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
    store_dir.mkdir(parents=True, exist_ok=True)
    return DuckDBObservabilityStore(store_dir / "observability.db")


def _write_maintenance_metrics(
    module_id: str,
    run_id: str,
    timing: dict[str, Any],
    store_dir: Path | None,
    observability_store: Any = None,
) -> None:
    store = _resolve_observability_store(store_dir, observability_store)
    if store is None:
        return
    try:
        from datetime import datetime
        with store.connect() as cur:
            cur.execute(_MAINTENANCE_METRICS_DDL)
            cur.execute(
                "INSERT INTO maintenance_metrics "
                "(run_id, module_id, optimize_ms, vacuum_ms, captured_at) "
                "VALUES (?, ?, ?, ?, ?)",
                [
                    run_id,
                    module_id,
                    timing.get("optimize_ms"),
                    timing.get("vacuum_ms"),
                    datetime.now(tz=UTC).isoformat(),
                ],
            )
    except Exception as exc:
        logger.debug("Maintenance metrics write failed for %r: %s", module_id, exc)


def _write_stage_metrics(
    module_id: str,
    run_id: str,
    metrics: dict[str, Any],
    store_dir: Path | None,
    observability_store: Any = None,
) -> None:
    """Persist SparkListener stage metrics to the configured observability store (non-fatal)."""
    store = _resolve_observability_store(store_dir, observability_store)
    if store is None:
        return
    try:
        from datetime import datetime
        with store.connect() as cur:
            cur.execute(_MODULE_METRICS_DDL)
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
        logger.debug("Stage metrics write failed for %r: %s", module_id, exc)


def _update_metric(
    store_dir: Path,
    run_id: str,
    module_id: str,
    column: str,
    value: int | None,
    observability_store: Any = None,
) -> None:
    """UPDATE a single column in an existing module_metrics row (non-fatal)."""
    store = _resolve_observability_store(store_dir, observability_store)
    if store is None:
        return
    try:
        with store.connect() as cur:
            cur.execute(
                f"UPDATE module_metrics SET {column} = ? WHERE run_id = ? AND module_id = ?",
                [value, run_id, module_id],
            )
    except Exception as exc:
        logger.debug("Metric update failed for %r.%s: %s", module_id, column, exc)


_SUPPORTED_TYPES: frozenset[str] = frozenset(
    {m for m in ModuleType if m != ModuleType.Arcade}
)

# Ports that carry control signals only, not DataFrames.
# Spillway IS a data edge (routes error rows); only "signal" is control-only.
_SIGNAL_PORTS: frozenset[str] = frozenset({"signal"})

# Sentinel placed in frame_store when a Regulator closes its gate with on_block=skip.
# Downstream modules that encounter it skip themselves and propagate the sentinel.
_GATE_CLOSED: object = object()


class ExecuteError(AqueductError):
    """Raised for unrecoverable execution failures (config, unsupported type, etc.)."""


# ── DAG helpers ───────────────────────────────────────────────────────────────

def _is_data_edge(edge: Edge) -> bool:
    return edge.port not in _SIGNAL_PORTS


def _frame_key(from_id: str, port: str) -> str:
    return from_id if port == "main" else f"{from_id}.{port}"


def _apply_spillway_filter(val: Any, edge: Edge) -> Any:
    """Typed catch: on a spillway edge with ``error_types``, keep only rows
    whose ``_aq_error_type`` label matches. Lazy transformation — zero Spark
    actions. An edge without ``error_types`` is a catch-all (unfiltered);
    rows matching no spillway edge are simply never delivered anywhere.
    """
    if edge.port != "spillway" or not edge.error_types:
        return val
    if val is None or _is_gate_closed(val):
        return val
    from pyspark.sql import functions as F
    return val.filter(F.col(AQ_ERROR_TYPE).isin(list(edge.error_types)))


def _cache_if_multi_spillway(df: Any, module_id: str, edges: tuple[Edge, ...]) -> Any:
    """Cache the quarantine frame when >1 spillway consumer exists.

    Each spillway consumer (typed `error_types` filter or catch-all) is a
    separate Spark action over the same quarantine frame — without caching,
    the parent DAG re-executes per consumer, which is exactly what our own
    ``perf_multi_consumer_no_cache`` warn rule tells users not to do.
    Quarantine frames are small error-row subsets, so in-memory ``cache()``
    is appropriate (see spark_guide caching-strategy). No explicit
    unpersist — frames are run-scoped and released when the session stops.
    """
    n_consumers = sum(
        1 for e in edges if e.from_id == module_id and e.port == "spillway"
    )
    if n_consumers > 1:
        logger.debug(
            "[%s] %d spillway consumers — caching quarantine frame", module_id, n_consumers,
        )
        return df.cache()
    return df


def _is_gate_closed(value: Any) -> bool:
    return value is _GATE_CLOSED


def _topo_sort(modules: tuple[Module, ...], edges: tuple[Edge, ...]) -> list[Module]:
    """Kahn's topo-sort on data-flow edges.  Probe modules must be excluded first."""
    module_map: dict[str, Module] = {m.id: m for m in modules}
    in_degree: dict[str, int] = {m.id: 0 for m in modules}
    successors: dict[str, list[str]] = {m.id: [] for m in modules}

    for edge in edges:
        if _is_data_edge(edge) and edge.to_id in module_map:
            in_degree[edge.to_id] += 1
            successors[edge.from_id].append(edge.to_id)

    queue: deque[str] = deque(mid for mid, deg in in_degree.items() if deg == 0)
    order: list[Module] = []

    while queue:
        mid = queue.popleft()
        order.append(module_map[mid])
        for succ in successors[mid]:
            in_degree[succ] -= 1
            if in_degree[succ] == 0:
                queue.append(succ)

    if len(order) != len(modules):
        processed = {m.id for m in order}
        remaining = [mid for mid in module_map if mid not in processed]
        raise ExecuteError(
            f"Cycle detected in Manifest execution graph. "
            f"Modules in cycle: {remaining}"
        )

    return order


def _build_execution_order(manifest: Manifest) -> list[Module]:
    """Topo-sort non-Probe modules, then insert each Probe after its attach_to.

    This guarantees that a Probe's signals are written to the store before the
    Regulator downstream of that Probe evaluates them.
    """
    probe_modules = [m for m in manifest.modules if m.type == ModuleType.Probe]
    non_probe_modules = tuple(m for m in manifest.modules if m.type != ModuleType.Probe)

    order = _topo_sort(non_probe_modules, manifest.edges)

    # Group probes by their attach_to target
    probes_by_attach: dict[str | None, list[Module]] = defaultdict(list)
    for probe in probe_modules:
        probes_by_attach[probe.attach_to].append(probe)

    # Rebuild order inserting probes immediately after their attach_to module
    final_order: list[Module] = []
    for module in order:
        final_order.append(module)
        for probe in probes_by_attach.pop(module.id, []):
            final_order.append(probe)

    # Probes with unresolved attach_to (shouldn't happen after compiler validation)
    for remaining_probes in probes_by_attach.values():
        final_order.extend(remaining_probes)

    return final_order


def _find_connected_components(
    module_ids: set[str],
    edges: tuple[Edge, ...],
    modules: tuple[Module, ...] = (),
) -> list[set[str]]:
    """Union-Find: identify independent connected trees in the data-flow graph.

    Two modules are in the same component if there is any data-flow path between
    them (directly or transitively). Components with no shared edges can be
    executed concurrently.

    Probe modules have NO data edges — they bind to their target via the
    `attach_to` field, not the graph. Without explicit unioning a Probe would
    form its own singleton component and be dispatched to a separate parallel
    thread, racing the thread that produces its `attach_to` frame:
    `frame_store.get(attach_to)` returns None → Probe silently skipped. So
    union every Probe into its `attach_to` target's component — they then run
    in the same thread, after `attach_to` (the order already sequences them).

    Returns a list of sets, each set containing the module IDs of one component.
    """
    parent: dict[str, str] = {mid: mid for mid in module_ids}
    rank: dict[str, int] = {mid: 0 for mid in module_ids}

    def _find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]  # path halving
            x = parent[x]
        return x

    def _union(x: str, y: str) -> None:
        px, py = _find(x), _find(y)
        if px == py:
            return
        if rank[px] < rank[py]:
            px, py = py, px
        parent[py] = px
        if rank[px] == rank[py]:
            rank[px] += 1

    for e in edges:
        if _is_data_edge(e) and e.from_id in module_ids and e.to_id in module_ids:
            _union(e.from_id, e.to_id)

    # ISSUE-042: bind Probes to their attach_to target's component (no edge exists).
    for m in modules:
        if (
            m.type == ModuleType.Probe
            and m.attach_to
            and m.id in module_ids
            and m.attach_to in module_ids
        ):
            _union(m.id, m.attach_to)

    components: dict[str, set[str]] = {}
    for mid in module_ids:
        root = _find(mid)
        components.setdefault(root, set()).add(mid)

    return list(components.values())


def _incoming_data(module_id: str, edges: tuple[Edge, ...]) -> list[Edge]:
    return [e for e in edges if e.to_id == module_id and _is_data_edge(e)]


def _incoming_main(module_id: str, edges: tuple[Edge, ...]) -> list[Edge]:
    return [e for e in edges if e.to_id == module_id and e.port == "main"]


def _reachable_forward(start_id: str, edges: tuple[Edge, ...]) -> set[str]:
    """BFS: all module IDs reachable forward (downstream) from start_id on data edges."""
    graph: dict[str, list[str]] = defaultdict(list)
    for e in edges:
        if _is_data_edge(e):
            graph[e.from_id].append(e.to_id)
    visited: set[str] = set()
    queue: deque[str] = deque([start_id])
    while queue:
        mid = queue.popleft()
        if mid in visited:
            continue
        visited.add(mid)
        for succ in graph[mid]:
            queue.append(succ)
    return visited


def _reachable_backward(start_id: str, edges: tuple[Edge, ...]) -> set[str]:
    """BFS: all module IDs reachable backward (ancestors) from start_id on data edges."""
    rev_graph: dict[str, list[str]] = defaultdict(list)
    for e in edges:
        if _is_data_edge(e):
            rev_graph[e.to_id].append(e.from_id)
    visited: set[str] = set()
    queue: deque[str] = deque([start_id])
    while queue:
        mid = queue.popleft()
        if mid in visited:
            continue
        visited.add(mid)
        for pred in rev_graph[mid]:
            queue.append(pred)
    return visited


# ── Watermark persistence (Depot-only, Phase 53) ──────────────────────────────
#
# The incremental-Channel watermark is persisted to the Depot exclusively — a
# driver pod on an ephemeral filesystem leaves no artefacts. (The 1.x local-FS
# sidecar and its one-time migration shim were removed in 2.0.)


def _compute_watermark_from_output(
    spark: SparkSession,
    path: str,
    fmt: str,
    watermark_col: str,
) -> str | None:
    """Compute MAX(watermark_col) from already-written Egress output.

    For delta: uses ``SELECT MAX() FROM delta.`path` `` which Spark can satisfy
    using Delta's transaction log statistics — often a metadata-only operation.
    For other formats: reads the materialized output files (not the upstream DAG).
    Returns None on any failure.
    """
    try:
        from pyspark.sql import functions as _F
        if fmt == "delta":
            result = spark.sql(
                f"SELECT MAX(`{watermark_col}`) AS wm FROM delta.`{path}`"
            ).collect()[0][0]
        else:
            result = (
                spark.read.format(fmt).load(path)
                .agg(_F.max(watermark_col))
                .collect()[0][0]
            )
        return str(result) if result is not None else None
    except Exception:
        return None


def _find_downstream_egress_ids(
    channel_id: str,
    manifest: Manifest,
) -> list[str]:
    """Return IDs of all Egress modules reachable (any depth) downstream of channel_id."""
    reachable = _reachable_forward(channel_id, manifest.edges)
    return [
        m.id for m in manifest.modules
        if m.id in reachable and m.id != channel_id and m.type == ModuleType.Egress
    ]


def _selector_included(
    modules: tuple[Module, ...],
    edges: tuple[Edge, ...],
    from_module: str | None,
    to_module: str | None,
) -> set[str] | None:
    """Return the set of module IDs to execute given --from/--to selectors.

    Returns None when no selector is active (run everything).
    Probes whose attach_to is in the included set are automatically included.
    """
    if not from_module and not to_module:
        return None

    all_ids = {m.id for m in modules}

    if from_module:
        if from_module not in all_ids:
            raise ExecuteError(f"--from module {from_module!r} not found in Manifest")
        forward = _reachable_forward(from_module, edges)
    else:
        forward = all_ids

    if to_module:
        if to_module not in all_ids:
            raise ExecuteError(f"--to module {to_module!r} not found in Manifest")
        backward = _reachable_backward(to_module, edges)
    else:
        backward = all_ids

    included = forward & backward

    # Probes: include when their tap target is included
    for m in modules:
        if m.type == ModuleType.Probe and m.attach_to in included:
            included.add(m.id)

    return included


# ── Public API ────────────────────────────────────────────────────────────────

def execute(
    manifest: Manifest,
    spark: SparkSession,
    run_id: str | None = None,
    store_dir: Path | None = None,
    surveyor: Any | None = None,
    depot: Any | None = None,
    resume_run_id: str | None = None,
    from_module: str | None = None,
    to_module: str | None = None,
    block_full_actions: bool = False,
    parallel: bool = False,
    use_observe: bool = True,
    observability_store: Any = None,
    explain_capture: dict[str, dict] | None = None,
    warnings_suppress: set[str] | None = None,
    warnings_silence_all: bool = False,
    sampling: ProbeSampling | None = None,
) -> ExecutionResult:
    """Execute a compiled Manifest.

    Args:
        manifest:  Fully compiled, frozen Manifest.
        spark:     Active SparkSession (caller owns lifecycle).
        run_id:    Optional run identifier; auto-generated UUID if omitted.
        store_dir: Optional path for observability signals (Probe I/O).
        surveyor:  Optional Surveyor instance used to evaluate Regulator gates.
                   When None, all active Regulators default to open (pass-through).
        depot:          Optional DepotStore for ``format: depot`` Egress writes and
                        ``@aq.depot.get()`` resolution.
        resume_run_id:  If set, reload module outputs from checkpoints written by
                        that prior run. Modules with a valid checkpoint are skipped.
        from_module:    If set, skip all modules that are NOT reachable forward from
                        this module ID. Modules before from_module in the DAG are
                        skipped (status="skipped") — upstream data must already exist.
        to_module:      If set, skip all modules that are NOT ancestors of this
                        module ID. Stops execution after to_module completes.
        parallel:       If True, identify independent connected components in the
                        DAG and execute each component in a separate thread.
                        Only beneficial when the Blueprint has multiple independent
                        source trees (e.g. two separate Ingress→Egress chains).
                        Junction fan-out is already parallel via Spark's lazy
                        evaluation — serial mode is sufficient for those.

    Returns:
        Frozen ExecutionResult.

    Raises:
        ExecuteError: Unsupported module type, cycle detected, UDF registration
                      failure.  Most module errors are caught and recorded as
                      status="error" (fail-fast).  Probe errors are always swallowed.
    """
    from aqueduct.executor.spark.probe import ProbeSampling, execute_probe
    from aqueduct.executor.spark.udf import UDFError, register_udfs

    run_id = run_id or str(uuid.uuid4())

    if sampling is None:
        sampling = ProbeSampling()

    # Checkpoint / resume paths (None when feature disabled)
    checkpoint_dir: Path | None = None
    any_checkpoint = manifest.checkpoint or any(m.checkpoint for m in manifest.modules)
    if store_dir and any_checkpoint:
        checkpoint_dir = store_dir / "checkpoints" / run_id
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        (checkpoint_dir / "_manifest_hash").write_text(
            _manifest_hash(manifest), encoding="utf-8"
        )

    resume_dir: Path | None = None
    if store_dir and resume_run_id:
        resume_dir = store_dir / "checkpoints" / resume_run_id
        if not resume_dir.exists():
            raise ExecuteError(
                f"Resume run_id={resume_run_id!r} has no checkpoints at {resume_dir}"
            )
        stored_hash_path = resume_dir / "_manifest_hash"
        if stored_hash_path.exists():
            stored_hash = stored_hash_path.read_text(encoding="utf-8").strip()
            current_hash = _manifest_hash(manifest)
            if stored_hash != current_hash:
                logger.warning(
                    "[runtime_resume_hash_changed] Resuming run %r: Manifest has "
                    "changed since original run (hash %s → %s). Checkpoint data "
                    "may be stale.",
                    resume_run_id, stored_hash, current_hash,
                )

    # ── Phase 30a tier 2 — session-startup warnings (JAR availability, ...) ──
    if not warnings_silence_all:
        try:
            from aqueduct.executor.spark.warnings import run_all as _run_session_warnings
            from aqueduct.warnings import emit as _aq_emit
            for _rid, _msg in _run_session_warnings(manifest, spark, suppress=warnings_suppress):
                _aq_emit(_rid, _msg, suppress=warnings_suppress)
        except Exception:
            pass  # session-startup warnings must never crash the executor

    # Register UDFs before any module executes so Channel SQL can reference them.
    try:
        register_udfs(manifest.udf_registry, spark)
    except UDFError as exc:
        raise ExecuteError(str(exc)) from exc

    order = _build_execution_order(manifest)

    # Selector: compute included set when --from/--to flags are active
    included_ids = _selector_included(manifest.modules, manifest.edges, from_module, to_module)
    if included_ids is not None:
        excluded = [m.id for m in order if m.id not in included_ids]
        if excluded:
            logger.info(
                "Selector active (from=%s, to=%s): skipping %d module(s): %s",
                from_module, to_module, len(excluded), excluded,
            )

    # Shared execution state — frame_store keys are disjoint across components so
    # no locking needed for frame_store itself. module_results and _ingress_obs are
    # merged under _merge_lock when each component finishes.
    frame_store: dict[str, Any] = {}
    module_results: list[ModuleResult] = []
    _ingress_obs: dict[str, Any] = {}

    # Cancellation state — set by the first failing component; sibling threads
    # skip their remaining modules when they see it.
    _cancel_event = threading.Event()
    _merge_lock = threading.Lock()
    # trigger_agent flag from the first failing component
    _trigger_agent_flag: list[bool] = []

    # ── Component runner ──────────────────────────────────────────────────────
    def _run_component(comp_order: list[Module]) -> None:
        """Execute one connected component serially.

        On failure: sets _cancel_event, records trigger_agent flag, merges
        local results into the shared lists, then returns. The main thread
        checks _cancel_event after all components complete.
        """
        local_results: list[ModuleResult] = []
        local_ingress_obs: dict[str, Any] = {}
        # Maps egress_id → (channel_id, watermark_col, depot_key) for sidecar write after Egress success
        _pending_watermarks: dict[str, tuple[str, str, str]] = {}

        def _merge() -> None:
            with _merge_lock:
                module_results.extend(local_results)
                _ingress_obs.update(local_ingress_obs)

        def _signal_fail(*, trigger_agent: bool = False) -> None:
            _cancel_event.set()
            if not _trigger_agent_flag:
                _trigger_agent_flag.append(trigger_agent)
            _merge()

        for module in comp_order:
            _collect_module_warnings()  # clear per-module warning collector

            # ── Cancellation check ─────────────────────────────────────────────
            if _cancel_event.is_set():
                local_results.append(_mr(module_id=module.id, status=ExecutionStatus.SKIPPED))
                continue

            # ── Conditional execution — `enabled: false` (compiler already
            # cascade-disabled every downstream consumer, so no live module
            # reads a frame this skip never produces) ──────────────────────────
            if not getattr(module, "enabled", True):
                local_results.append(_mr(module_id=module.id, status=ExecutionStatus.SKIPPED))
                continue

            if module.type not in _SUPPORTED_TYPES:
                _signal_fail()
                raise ExecuteError(
                    f"Module type {module.type!r} (id={module.id!r}) is not supported. "
                    f"Supported: {sorted(m.value for m in _SUPPORTED_TYPES)}."
                )

            # ── Selector skip ──────────────────────────────────────────────────
            if included_ids is not None and module.id not in included_ids:
                local_results.append(_mr(module_id=module.id, status=ExecutionStatus.SKIPPED))
                continue

            # ── Resume: reload from checkpoint if available ────────────────────
            if resume_dir:
                module_ckpt = resume_dir / module.id
                done_marker = module_ckpt / "_aq_done"
                if done_marker.exists():
                    if module.type in (ModuleType.Ingress, ModuleType.Channel, ModuleType.Funnel):
                        data_ckpt = module_ckpt / "data"
                        if data_ckpt.exists():
                            frame_store[module.id] = spark.read.parquet(str(data_ckpt))
                    elif module.type == ModuleType.Junction:
                        for branch in module.config.get("branches", []):
                            bid = branch.get("id", "")
                            branch_ckpt = module_ckpt / bid
                            if branch_ckpt.exists():
                                frame_store[f"{module.id}.{bid}"] = spark.read.parquet(
                                    str(branch_ckpt)
                                )
                    local_results.append(
                        _mr(module_id=module.id, status=ExecutionStatus.SUCCESS)
                    )
                    logger.info("Module %r: resumed from checkpoint, skipping execution", module.id)
                    continue

            # ── Ingress ───────────────────────────────────────────────────────
            if module.type == ModuleType.Ingress:
                mod_policy = _module_retry_policy(module, manifest.retry_policy)
                _t0 = time.monotonic()
                try:
                    df = _with_retry(
                        lambda: read_ingress(module, spark),
                        mod_policy,
                        module.id,
                    )
                except IngressError as exc:
                    gate_closed, fail_result = _on_retry_exhausted(
                        exc, mod_policy, module, manifest.blueprint_id, run_id, local_results
                    )
                    if gate_closed:
                        frame_store[module.id] = _GATE_CLOSED
                        continue
                    _signal_fail(trigger_agent=getattr(fail_result, "trigger_agent", False))
                    return
                _obs_df, _obs = observe_df(df, f"{module.id}_read", "records_read", enabled=use_observe)
                local_ingress_obs[module.id] = _obs
                _write_stage_metrics(
                    module.id, run_id,
                    {**null_metrics(),
                     "bytes_read": dir_bytes(module.config.get("path", "")),
                     "duration_ms": int((time.monotonic() - _t0) * 1000)},
                    store_dir,
                    observability_store=observability_store,
                )
                frame_store[module.id] = _obs_df
                _write_checkpoint(module, checkpoint_dir, manifest, data={"data": df})
                local_results.append(_mr(module_id=module.id, status=ExecutionStatus.SUCCESS))

            # ── Channel ───────────────────────────────────────────────────────
            elif module.type == ModuleType.Channel:
                main_edges = _incoming_main(module.id, manifest.edges)
                if not main_edges:
                    err = f"[{module.id}] Channel has no main-port incoming edges"
                    local_results.append(
                        _mr(module_id=module.id, status=ExecutionStatus.ERROR, error=err)
                    )
                    _signal_fail()
                    return

                upstream_dfs: dict[str, Any] = {}
                for edge in main_edges:
                    val = frame_store.get(edge.from_id)
                    if _is_gate_closed(val):
                        frame_store[module.id] = _GATE_CLOSED
                        local_results.append(
                            _mr(module_id=module.id, status=ExecutionStatus.SKIPPED)
                        )
                        break
                    if val is None:
                        err = (
                            f"[{module.id}] upstream {edge.from_id!r} produced no DataFrame."
                        )
                        local_results.append(
                            _mr(module_id=module.id, status=ExecutionStatus.ERROR, error=err)
                        )
                        _signal_fail()
                        return
                    upstream_dfs[edge.from_id] = val
                else:
                    mod_policy = _module_retry_policy(module, manifest.retry_policy)
                    _t0 = time.monotonic()

                    # ── Incremental watermark injection ────────────────────────
                    _incremental = module.config.get("materialize") == "incremental"
                    _channel_module = module
                    if _incremental:
                        _watermark_col = module.config.get("watermark_column", "")
                        _depot_key = f"{manifest.blueprint_id}:{module.id}:_watermark"
                        # Phase 53 — Depot is the sole watermark store.
                        _watermark_val = (
                            (depot.get(_depot_key, "") if depot else "")
                            or "1900-01-01 00:00:00"
                        )
                        _query = module.config.get("query", "")
                        _patched_query = _query.replace("${ctx._watermark}", f"'{_watermark_val}'")
                        if _patched_query != _query:
                            import dataclasses as _dc
                            _channel_module = _dc.replace(
                                module,
                                config={**module.config, "query": _patched_query},
                            )
                        # Warn if downstream Egress uses mode=overwrite
                        _downstream_ids = {
                            e.to_id for e in manifest.edges if e.from_id == module.id
                        }
                        for _ds_m in manifest.modules:
                            if (
                                _ds_m.id in _downstream_ids
                                and _ds_m.type == ModuleType.Egress
                                and _ds_m.config.get("mode") == "overwrite"
                            ):
                                logger.warning(
                                    "[runtime_incremental_overwrite] [%s] "
                                    "materialize=incremental → downstream Egress "
                                    "%r uses mode=overwrite; incremental rows will "
                                    "replace prior data.",
                                    module.id, _ds_m.id,
                                )

                    try:
                        df = _with_retry(
                            lambda: execute_channel(_channel_module, upstream_dfs, spark),
                            mod_policy,
                            module.id,
                        )
                    except ChannelError as exc:
                        gate_closed, fail_result = _on_retry_exhausted(
                            exc, mod_policy, module, manifest.blueprint_id, run_id, local_results
                        )
                        if gate_closed:
                            frame_store[module.id] = _GATE_CLOSED
                            continue
                        _signal_fail(trigger_agent=getattr(fail_result, "trigger_agent", False))
                        return

                    # ── Spillway split ─────────────────────────────────────────
                    spillway_condition: str | None = module.config.get("spillway_condition")
                    has_spillway_edge = any(
                        e.from_id == module.id and e.port == "spillway"
                        for e in manifest.edges
                    )
                    if spillway_condition and has_spillway_edge:
                        from pyspark.sql import functions as F
                        good_df = df.filter(f"NOT ({spillway_condition})")
                        error_df = (
                            df.filter(spillway_condition)
                              .withColumn(AQ_ERROR_MODULE, F.lit(module.id))
                              .withColumn(AQ_ERROR_MSG, F.lit("spillway_condition matched"))
                              .withColumn(AQ_ERROR_TYPE, F.lit("SpillwayCondition"))
                              .withColumn(AQ_ERROR_TS, F.current_timestamp())
                        )
                        frame_store[module.id] = good_df
                        frame_store[f"{module.id}.spillway"] = _cache_if_multi_spillway(
                            error_df, module.id, manifest.edges,
                        )
                    elif spillway_condition and not has_spillway_edge:
                        # Static mismatch — warned at compile time
                        # (compiler/warnings/spillway_port_mismatch). All rows to main.
                        frame_store[module.id] = df
                    elif has_spillway_edge and not spillway_condition:
                        # Static mismatch — warned at compile time
                        # (compiler/warnings/spillway_port_mismatch). Spillway is empty.
                        frame_store[module.id] = df
                        frame_store[f"{module.id}.spillway"] = df.filter("1=0")
                    else:
                        frame_store[module.id] = df

                    # ── Incremental watermark — defer update to post-Egress ────
                    # Register pending watermark for each downstream Egress. The
                    # actual MAX computation happens after the Egress write, reading
                    # from the materialized output instead of re-executing the DAG.
                    if _incremental and _watermark_col:
                        for _eg_id in _find_downstream_egress_ids(module.id, manifest):
                            _pending_watermarks[_eg_id] = (module.id, _watermark_col, _depot_key)

                    _write_stage_metrics(
                        module.id, run_id,
                        {**null_metrics(), "duration_ms": int((time.monotonic() - _t0) * 1000)},
                        store_dir,
                        observability_store=observability_store,
                    )
                    _write_checkpoint(module, checkpoint_dir, manifest, data={"data": frame_store[module.id]})
                    local_results.append(_mr(module_id=module.id, status=ExecutionStatus.SUCCESS))

            # ── Junction ──────────────────────────────────────────────────────
            elif module.type == ModuleType.Junction:
                main_edges = _incoming_main(module.id, manifest.edges)
                if not main_edges:
                    err = f"[{module.id}] Junction has no main-port incoming edges"
                    local_results.append(
                        _mr(module_id=module.id, status=ExecutionStatus.ERROR, error=err)
                    )
                    _signal_fail()
                    return

                upstream_id = main_edges[0].from_id
                val = frame_store.get(upstream_id)
                if _is_gate_closed(val):
                    branches = module.config.get("branches", [])
                    for branch in branches:
                        frame_store[f"{module.id}.{branch.get('id', '')}"] = _GATE_CLOSED
                    local_results.append(_mr(module_id=module.id, status=ExecutionStatus.SKIPPED))
                    continue
                if val is None:
                    err = (
                        f"[{module.id}] upstream {upstream_id!r} produced no DataFrame."
                    )
                    local_results.append(
                        _mr(module_id=module.id, status=ExecutionStatus.ERROR, error=err)
                    )
                    _signal_fail()
                    return

                mod_policy = _module_retry_policy(module, manifest.retry_policy)
                _t0 = time.monotonic()
                try:
                    branch_dfs = _with_retry(
                        lambda: execute_junction(module, val),
                        mod_policy,
                        module.id,
                    )
                except JunctionError as exc:
                    gate_closed, fail_result = _on_retry_exhausted(
                        exc, mod_policy, module, manifest.blueprint_id, run_id, local_results
                    )
                    if gate_closed:
                        branches = module.config.get("branches", [])
                        for branch in branches:
                            frame_store[f"{module.id}.{branch.get('id', '')}"] = _GATE_CLOSED
                        continue
                    _signal_fail(trigger_agent=getattr(fail_result, "trigger_agent", False))
                    return

                _write_stage_metrics(
                    module.id, run_id,
                    {**null_metrics(), "duration_ms": int((time.monotonic() - _t0) * 1000)},
                    store_dir,
                    observability_store=observability_store,
                )
                for branch_id, branch_df in branch_dfs.items():
                    frame_store[f"{module.id}.{branch_id}"] = branch_df
                _write_checkpoint(module, checkpoint_dir, manifest, data=branch_dfs)
                local_results.append(_mr(module_id=module.id, status=ExecutionStatus.SUCCESS))

            # ── Funnel ────────────────────────────────────────────────────────
            elif module.type == ModuleType.Funnel:
                data_edges = _incoming_data(module.id, manifest.edges)
                if not data_edges:
                    err = f"[{module.id}] Funnel has no incoming data edges"
                    local_results.append(
                        _mr(module_id=module.id, status=ExecutionStatus.ERROR, error=err)
                    )
                    _signal_fail()
                    return

                funnel_upstream: dict[str, Any] = {}
                skipped = False
                for edge in data_edges:
                    store_key = _frame_key(edge.from_id, edge.port)
                    val = frame_store.get(store_key)
                    if _is_gate_closed(val):
                        skipped = True
                        break
                    if val is None:
                        err = (
                            f"[{module.id}] upstream {store_key!r} produced no DataFrame."
                        )
                        local_results.append(
                            _mr(module_id=module.id, status=ExecutionStatus.ERROR, error=err)
                        )
                        _signal_fail()
                        return
                    funnel_upstream[store_key] = _apply_spillway_filter(val, edge)

                if skipped:
                    frame_store[module.id] = _GATE_CLOSED
                    local_results.append(_mr(module_id=module.id, status=ExecutionStatus.SKIPPED))
                    continue

                mod_policy = _module_retry_policy(module, manifest.retry_policy)
                _t0 = time.monotonic()
                try:
                    df = _with_retry(
                        lambda: execute_funnel(module, funnel_upstream),
                        mod_policy,
                        module.id,
                    )
                except FunnelError as exc:
                    gate_closed, fail_result = _on_retry_exhausted(
                        exc, mod_policy, module, manifest.blueprint_id, run_id, local_results
                    )
                    if gate_closed:
                        frame_store[module.id] = _GATE_CLOSED
                        continue
                    _signal_fail(trigger_agent=getattr(fail_result, "trigger_agent", False))
                    return
                _write_stage_metrics(
                    module.id, run_id,
                    {**null_metrics(), "duration_ms": int((time.monotonic() - _t0) * 1000)},
                    store_dir,
                    observability_store=observability_store,
                )
                frame_store[module.id] = df
                _write_checkpoint(module, checkpoint_dir, manifest, data={"data": df})
                local_results.append(_mr(module_id=module.id, status=ExecutionStatus.SUCCESS))

            # ── Assert ────────────────────────────────────────────────────────
            elif module.type == ModuleType.Assert:
                main_edges = _incoming_main(module.id, manifest.edges)
                if not main_edges:
                    err = f"[{module.id}] Assert has no main-port incoming edges"
                    local_results.append(
                        _mr(module_id=module.id, status=ExecutionStatus.ERROR, error=err)
                    )
                    _signal_fail()
                    return

                upstream_id = main_edges[0].from_id
                val = frame_store.get(upstream_id)
                if _is_gate_closed(val):
                    frame_store[module.id] = _GATE_CLOSED
                    local_results.append(_mr(module_id=module.id, status=ExecutionStatus.SKIPPED))
                    continue
                if val is None:
                    err = f"[{module.id}] upstream {upstream_id!r} produced no DataFrame."
                    local_results.append(
                        _mr(module_id=module.id, status=ExecutionStatus.ERROR, error=err)
                    )
                    _signal_fail()
                    return

                has_spillway_edge = any(
                    e.from_id == module.id and e.port == "spillway" for e in manifest.edges
                )

                try:
                    passing_df, quarantine_df = execute_assert(
                        module, val, spark, run_id, manifest.blueprint_id,
                    )
                except AssertError as exc:
                    local_results.append(
                        _mr(module_id=module.id, status=ExecutionStatus.ERROR, error=str(exc), error_type=exc.error_type, exception=exc)
                    )
                    _signal_fail(trigger_agent=exc.trigger_agent)
                    return
                except Exception as exc:  # noqa: BLE001 — assert dispatch must fail cleanly, not leak a raw traceback
                    local_results.append(
                        _mr(module_id=module.id, status=ExecutionStatus.ERROR, error=str(exc), exception=exc)
                    )
                    _signal_fail()
                    return

                frame_store[module.id] = passing_df
                if quarantine_df is not None and has_spillway_edge:
                    frame_store[f"{module.id}.spillway"] = _cache_if_multi_spillway(
                        quarantine_df, module.id, manifest.edges,
                    )
                elif quarantine_df is not None:
                    logger.warning(
                        "[runtime_assert_quarantine_no_spillway] [%s] Assert "
                        "quarantine rows produced but no spillway edge; discarded.",
                        module.id,
                    )
                local_results.append(_mr(module_id=module.id, status=ExecutionStatus.SUCCESS))

            # ── Regulator ─────────────────────────────────────────────────────
            elif module.type == ModuleType.Regulator:
                main_edges = _incoming_main(module.id, manifest.edges)
                if not main_edges:
                    err = f"[{module.id}] Regulator has no main-port incoming edges"
                    local_results.append(
                        _mr(module_id=module.id, status=ExecutionStatus.ERROR, error=err)
                    )
                    _signal_fail()
                    return

                upstream_id = main_edges[0].from_id
                val = frame_store.get(upstream_id)
                if _is_gate_closed(val):
                    frame_store[module.id] = _GATE_CLOSED
                    local_results.append(_mr(module_id=module.id, status=ExecutionStatus.SKIPPED))
                    continue
                if val is None:
                    err = (
                        f"[{module.id}] upstream {upstream_id!r} produced no DataFrame."
                    )
                    local_results.append(
                        _mr(module_id=module.id, status=ExecutionStatus.ERROR, error=err)
                    )
                    _signal_fail()
                    return

                # ── Evaluate Regulator with optional timeout ───────────────
                timeout_sec = float(module.config.get("timeout_seconds", 0))
                # 1.1.0 — poll_seconds exposed as Regulator config (default 30s).
                # 2s default was wasteful CPU for batch-tier signals; 30s is sane
                # for most pipelines. Real-time use cases can override down to
                # 1s; long-running waits can set 300s.
                poll_interval = max(0.5, float(module.config.get("poll_seconds", 30.0)))
                elapsed = 0.0
                
                gate_open = surveyor.evaluate_regulator(module.id) if surveyor else True
                
                # If gate is closed and we have a timeout, poll until open or timeout
                if not gate_open and timeout_sec > 0 and surveyor:
                    logger.info(
                        "[%s] Regulator gate closed; polling for signal (timeout=%ss)...",
                        module.id, timeout_sec,
                    )
                    while not gate_open and elapsed < timeout_sec:
                        time.sleep(poll_interval)
                        elapsed += poll_interval
                        gate_open = surveyor.evaluate_regulator(module.id)
                        if gate_open:
                            logger.info("[%s] Regulator signal received; gate OPEN.", module.id)

                    if not gate_open:
                        logger.info(
                            "[%s] Regulator timeout_seconds=%s reached; gate still closed — applying on_block.",
                            module.id, timeout_sec,
                        )

                if gate_open:
                    frame_store[module.id] = val
                    local_results.append(_mr(module_id=module.id, status=ExecutionStatus.SUCCESS))
                else:
                    on_block = module.config.get("on_block", "skip")
                    if on_block == "abort":
                        err = f"[{module.id}] Regulator gate closed; on_block=abort"
                        local_results.append(
                            _mr(module_id=module.id, status=ExecutionStatus.ERROR, error=err)
                        )
                        _signal_fail()
                        return
                    elif on_block == "trigger_agent":
                        logger.info(
                            "Regulator %r: gate closed, on_block=trigger_agent; "
                            "downstream skipped — LLM patch loop will fire.",
                            module.id,
                        )
                        local_results.append(
                            _mr(
                                module_id=module.id,
                                status=ExecutionStatus.ERROR,
                                error=f"[{module.id}] Regulator gate closed; on_block=trigger_agent",
                            )
                        )
                        _signal_fail(trigger_agent=True)
                        return
                    # skip (default): propagate sentinel
                    frame_store[module.id] = _GATE_CLOSED
                    local_results.append(_mr(module_id=module.id, status=ExecutionStatus.SKIPPED))

            # ── Egress ────────────────────────────────────────────────────────
            elif module.type == ModuleType.Egress:
                data_edges = _incoming_data(module.id, manifest.edges)
                if not data_edges:
                    err = f"[{module.id}] no main-port edge arriving at this Egress module"
                    local_results.append(
                        _mr(module_id=module.id, status=ExecutionStatus.ERROR, error=err)
                    )
                    _signal_fail()
                    return

                edge = data_edges[0]
                key = _frame_key(edge.from_id, edge.port)
                val = frame_store.get(key)
                if _is_gate_closed(val):
                    local_results.append(_mr(module_id=module.id, status=ExecutionStatus.SKIPPED))
                    continue
                if val is None:
                    err = (
                        f"[{module.id}] upstream {edge.from_id!r} produced no DataFrame."
                    )
                    local_results.append(
                        _mr(module_id=module.id, status=ExecutionStatus.ERROR, error=err)
                    )
                    _signal_fail()
                    return
                val = _apply_spillway_filter(val, edge)

                mod_policy = _module_retry_policy(module, manifest.retry_policy)
                _obs_df, _obs = observe_df(val, f"{module.id}_egress", "records_written", enabled=use_observe)
                _t0 = time.monotonic()
                try:
                    _with_retry(
                        lambda: write_egress(_obs_df, module, depot=depot),
                        mod_policy,
                        module.id,
                    )
                except EgressError as exc:
                    gate_closed, fail_result = _on_retry_exhausted(
                        exc, mod_policy, module, manifest.blueprint_id, run_id, local_results
                    )
                    if gate_closed:
                        continue
                    _signal_fail(trigger_agent=getattr(fail_result, "trigger_agent", False))
                    return
                _write_stage_metrics(
                    module.id, run_id,
                    {**null_metrics(),
                     "records_written": get_observation(_obs, "records_written"),
                     "bytes_written": dir_bytes(module.config.get("path", "")),
                     "duration_ms": int((time.monotonic() - _t0) * 1000)},
                    store_dir,
                    observability_store=observability_store,
                )
                _write_checkpoint(module, checkpoint_dir, manifest)

                # ── Post-write maintenance (delta / iceberg / hudi) ───────────
                _maintenance_cfg = module.config.get("maintenance")
                if _maintenance_cfg and isinstance(_maintenance_cfg, dict):
                    _maint_path = module.config.get("path", "")
                    _maint_table = module.config.get("table")
                    _maint_fmt = module.config.get("format", "delta")
                    # iceberg drives procedures off `table`; delta/hudi off `path`.
                    if _maint_path or _maint_table:
                        _maint_timing = run_maintenance(
                            spark, module.id, _maint_path, _maintenance_cfg,
                            fmt=_maint_fmt, table=_maint_table,
                        )
                        _write_maintenance_metrics(
                            module.id, run_id, _maint_timing, store_dir,
                            observability_store=observability_store,
                        )

                # ── Watermark update (Depot-only, Phase 53) ────────────────────
                if module.id in _pending_watermarks:
                    _ch_id, _wm_col, _wm_depot_key = _pending_watermarks.pop(module.id)
                    _eg_path = module.config.get("path", "")
                    _eg_fmt = module.config.get("format", "parquet")
                    if _eg_path and _eg_fmt not in ("depot",):
                        _new_wm = _compute_watermark_from_output(spark, _eg_path, _eg_fmt, _wm_col)
                        if _new_wm is None:
                            logger.warning(
                                "[runtime_watermark_compute_failed] [%s] Could "
                                "not compute watermark from output path %r; "
                                "watermark not advanced this run.",
                                module.id, _eg_path,
                            )
                        elif depot is None:
                            logger.warning(
                                "[runtime_watermark_no_depot] [%s] Incremental "
                                "Channel %r advanced watermark to %s=%s but no "
                                "depot is configured — it cannot be persisted, "
                                "so the next run re-scans all source data. "
                                "Configure stores.depots.",
                                module.id, _ch_id, _wm_col, _new_wm,
                            )
                        else:
                            depot.put(_wm_depot_key, _new_wm)
                            logger.debug(
                                "[%s] Watermark persisted to depot: %s=%s",
                                module.id, _wm_col, _new_wm,
                            )

                local_results.append(_mr(module_id=module.id, status=ExecutionStatus.SUCCESS))

            # ── Probe ─────────────────────────────────────────────────────────
            elif module.type == ModuleType.Probe:
                source_id = module.attach_to
                source_val = frame_store.get(source_id) if source_id else None
                _probe_notes: tuple[str, ...] = ()

                if source_val is None or _is_gate_closed(source_val):
                    logger.debug(
                        "Probe %r: attach_to=%r not available; skipping.",
                        module.id, source_id,
                    )
                elif store_dir is not None:
                    try:
                        _probe_notes = execute_probe(module, source_val, spark, run_id, store_dir, block_full_actions=block_full_actions, observability_store=observability_store, sampling=sampling) or ()
                    except Exception as exc:
                        logger.warning("[runtime_probe_error] Probe %r failed: %s", module.id, exc)

                local_results.append(_mr(module_id=module.id, status=ExecutionStatus.SUCCESS, notes=_probe_notes))

        # Component completed — merge local results into shared collections
        _merge()

    # ── Dispatch ──────────────────────────────────────────────────────────────
    if parallel:
        all_module_ids = {m.id for m in order}
        components = _find_connected_components(
            all_module_ids, manifest.edges, manifest.modules
        )

        if len(components) > 1:
            component_orders = [
                [m for m in order if m.id in comp_ids]
                for comp_ids in components
            ]
            logger.info(
                "Parallel execution: %d independent components detected",
                len(component_orders),
            )
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(component_orders)
            ) as pool:
                futures = [pool.submit(_run_component, co) for co in component_orders]
                for f in concurrent.futures.as_completed(futures):
                    exc = f.exception()
                    if exc is not None:
                        logger.error(
                            "Component thread raised unexpected exception: %s", exc
                        )
                        _cancel_event.set()
        else:
            # Single component — no thread overhead
            _run_component(order)
    else:
        _run_component(order)

    if _cancel_event.is_set():
        return _fail(
            manifest.blueprint_id, run_id, module_results,
            trigger_agent=bool(_trigger_agent_flag and _trigger_agent_flag[0]),
        )

    # ── Collect deferred Ingress observations (fire after all Egress writes) ───
    if store_dir is not None and _ingress_obs:
        _succeeded = {r.module_id for r in module_results if r.status == ExecutionStatus.SUCCESS}
        for _mod_id, _obs in _ingress_obs.items():
            if _mod_id not in _succeeded or _obs is None:
                continue
            try:
                _rr = get_observation(_obs, "records_read")
                if _rr is not None:
                    _update_metric(store_dir, run_id, _mod_id, "records_read", _rr, observability_store=observability_store)
            except Exception as exc:
                logger.debug("Observation collection failed for %r: %s", _mod_id, exc)

    # ── Lineage — write after successful execution ─────────────────────────────
    if store_dir is not None:
        _obs = _resolve_observability_store(store_dir, observability_store)
        try:
            from aqueduct.compiler.lineage import write_lineage
            write_lineage(
                manifest.blueprint_id, run_id, manifest.modules, manifest.edges,
                observability_store=_obs,
            )
        except Exception as exc:
            logger.debug("Lineage write skipped: %s", exc)
        # Phase 56 — Channel SQL fingerprints (changelog of semantic SQL changes).
        try:
            from aqueduct.compiler.fingerprint import write_fingerprints
            write_fingerprints(
                manifest.blueprint_id, run_id, manifest.modules,
                observability_store=_obs,
            )
        except Exception as exc:
            logger.debug("Fingerprint write skipped: %s", exc)

    # ── Phase 29b: capture explain() snapshots for Gate 4 ─────────────────────
    # Two sinks: `surveyor.record_explain_snapshot()` (real runs → persists into
    # `observability.explain_snapshot`) and the in-memory `explain_capture` dict (sandbox
    # runs in the sandbox gate → no persistence, used by the explain gate in-process).
    if surveyor is not None or explain_capture is not None:
        try:
            from aqueduct.patch.explain_gate import capture_plan_snapshot
            _succeeded = {r.module_id for r in module_results if r.status == ExecutionStatus.SUCCESS}
            for _mod in manifest.modules:
                if _mod.id not in _succeeded or _mod.type == ModuleType.Egress:
                    continue
                _df = frame_store.get(_mod.id)
                if _df is None:
                    continue
                try:
                    _snap = capture_plan_snapshot(_df)
                    if explain_capture is not None:
                        explain_capture[_mod.id] = _snap
                    if surveyor is not None:
                        surveyor.record_explain_snapshot(
                            run_id=run_id,
                            module_id=_mod.id,
                            exchange_count=_snap["exchange_count"],
                            python_udf_count=_snap["python_udf_count"],
                            broadcast_count=_snap["broadcast_count"],
                            plan_text=_snap["plan_text"],
                        )
                except Exception as _exc:
                    logger.debug("explain snapshot failed for %r: %s", _mod.id, _exc)
        # Broad by design: plan-snapshot capture (including the lazy `patch.explain_gate`
        # import above — an accepted executor→patch exception, see AGENTS.md "Executor
        # Architecture") is a best-effort Gate 4 diagnostic. It must never abort or fail
        # a run — a missing/broken import, an unsupported Catalyst plan shape, or any
        # other capture failure just means Gate 4 has no baseline for this run.
        except Exception as exc:
            logger.debug("Phase 29b explain capture skipped: %s", exc)

    return ExecutionResult(
        blueprint_id=manifest.blueprint_id,
        run_id=run_id,
        status=ExecutionStatus.SUCCESS,
        module_results=tuple(module_results),
    )


def _fail(
    blueprint_id: str,
    run_id: str,
    module_results: list[ModuleResult],
    *,
    trigger_agent: bool = False,
) -> ExecutionResult:
    return ExecutionResult(
        blueprint_id=blueprint_id,
        run_id=run_id,
        status=ExecutionStatus.ERROR,
        module_results=tuple(module_results),
        trigger_agent=trigger_agent,
    )


def _on_retry_exhausted(
    exc: Exception,
    policy: RetryPolicy,
    module: Module,
    blueprint_id: str,
    run_id: str,
    module_results: list[ModuleResult],
) -> tuple[bool, ExecutionResult | None]:
    """Handle retry exhaustion per on_exhaustion policy.

    Returns:
        (gate_closed, fail_result) — if gate_closed is True the caller should
        set _GATE_CLOSED in frame_store and `continue`; fail_result is None.
        If gate_closed is False, return fail_result immediately.
    """
    # Preserve the live exception so Surveyor.record() can hand it to
    # `_extract_structured_error` for error_class / object_name /
    # suggested_columns / sql_state / root_exception extraction. The
    # IngressError / ChannelError / Py4JJavaError chains its underlying
    # PySparkException via `raise ... from exc`, so the extractor's __cause__
    # walk finds it from here.
    module_results.append(_mr(
        module_id=module.id, status=ExecutionStatus.ERROR, error=str(exc), exception=exc,
    ))

    # Per-module failure webhook — fires for terminal on_exhaustion actions
    # (trigger_agent / quarantine), not for alert_only (blueprint continues).
    # Payload construction runs before the on_exhaustion check so that a
    # Block-IO webhook implementation can't stall the executor thread before
    # the blueprint continues under alert_only.
    on_exhaustion = policy.on_exhaustion
    fire_webhook_for_module: bool = on_exhaustion != "alert_only"
    if fire_webhook_for_module and module.on_failure_webhook is not None:
        import os as _os

        from aqueduct.infra.http import _deliver_webhook_payload
        raw = module.on_failure_webhook
        full_payload = {
            "run_id": run_id,
            "blueprint_id": blueprint_id,
            "module_id": module.id,
            "error_message": str(exc),
            "error_type": type(exc).__name__,
        }
        if isinstance(raw, str):
            url = raw
            method = "POST"
            headers = {}
            timeout = 10
            attempts = 2
            backoff = 2.0
            secret = None
        else:
            url = raw["url"]
            method = raw.get("method", "POST")
            headers = dict(raw.get("headers") or {})
            timeout = raw.get("timeout", 10)
            attempts = raw.get("max_retries", 1) + 1
            backoff = raw.get("backoff_seconds", 2.0)
            secret = raw.get("secret")
            # Render ${VAR} tokens in headers and secret (mirrors fire_webhook).
            import re as _re
            _token_re = _re.compile(r"\$\{([^}]+)\}")
            def _render_val(val):
                if not isinstance(val, str):
                    return val
                return _token_re.sub(
                    lambda m: str(full_payload.get(m.group(1), _os.environ.get(m.group(1), m.group(0)))),
                    val,
                )
            headers = {k: _render_val(v) for k, v in headers.items()}
            if secret:
                secret = _render_val(secret)
        _deliver_webhook_payload(
            url, full_payload,
            method=method, headers=headers, timeout=timeout,
            attempts=attempts, backoff_seconds=backoff, secret=secret,
        )

    if on_exhaustion == "alert_only":
        logger.warning("[runtime_retry_exhausted_alert] [%s] Retry exhausted (alert_only): %s — blueprint continues.", module.id, exc)
        return True, None
    elif on_exhaustion == "trigger_agent":
        return False, _fail(blueprint_id, run_id, module_results, trigger_agent=True)
    else:  # "abort" (default)
        return False, _fail(blueprint_id, run_id, module_results)
