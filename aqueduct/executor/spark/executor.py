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
     - Channel    → execute_sql_channel(…)            → frame_store[module.id]
                    If spillway_condition set and a spillway edge exists:
                      frame_store[module.id]              = good rows
                      frame_store["module.id.spillway"]   = error rows (+_aq_error_*)
     - Junction   → execute_junction(…)               → frame_store["id.branch"]
     - Funnel     → execute_funnel(…)                 → frame_store[module.id]
     - Regulator  → evaluate gate; open → pass-through; closed → on_block action
     - Egress     → write_egress(frame_store[key], …)
     - Probe      → execute_probe(…) side-effect only; never halts pipeline
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
  so the entire skipped sub-graph is recorded without aborting the pipeline.

No Spark actions beyond the Egress .save() and Probe sampling calls.
"""

from __future__ import annotations

import logging
import math
import random
import time
import uuid
from collections import defaultdict, deque
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, TypeVar

from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from aqueduct.compiler.models import Manifest
from aqueduct.config import WebhookEndpointConfig
from aqueduct.executor.models import ExecutionResult, ModuleResult
from aqueduct.executor.spark.assert_ import AssertError, execute_assert
from aqueduct.executor.spark.channel import ChannelError, execute_sql_channel
from aqueduct.executor.spark.egress import EgressError, write_egress
from aqueduct.executor.spark.funnel import FunnelError, execute_funnel
from aqueduct.executor.spark.ingress import IngressError, read_ingress
from aqueduct.executor.spark.junction import JunctionError, execute_junction
from aqueduct.parser.models import Edge, Module, RetryPolicy

logger = logging.getLogger(__name__)

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
        policy:    RetryPolicy from the Manifest (pipeline-level).
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
                        "Module %r: deadline exceeded (%.0fs elapsed, limit=%ds); "
                        "not retrying.",
                        module_id, elapsed, policy.deadline_seconds,
                    )
                    break

            is_last = attempt == policy.max_attempts - 1
            if is_last or not _is_retriable(exc, policy):
                if is_last:
                    logger.warning(
                        "Module %r: attempt %d/%d failed (%s); giving up",
                        module_id, attempt + 1, policy.max_attempts, exc,
                    )
                else:
                    logger.warning(
                        "Module %r: non-retriable error on attempt %d/%d: %s",
                        module_id, attempt + 1, policy.max_attempts, exc,
                    )
                break

            sleep = _backoff_seconds(attempt, policy)
            logger.warning(
                "Module %r: attempt %d/%d failed (%s); retrying in %.1fs",
                module_id, attempt + 1, policy.max_attempts, exc, sleep,
            )
            time.sleep(sleep)

    raise last_exc

def _write_checkpoint(
    module: Module,
    checkpoint_dir: "Path | None",
    manifest: Manifest,
    data: "dict[str, Any] | None" = None,
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
                logger.warning("Checkpoint write failed for %r/%s: %s", module.id, name, exc)
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


def _write_stage_metrics(
    module_id: str,
    run_id: str,
    metrics: "dict[str, Any]",
    store_dir: "Path | None",
) -> None:
    """Persist SparkListener stage metrics to signals.db (non-fatal)."""
    if store_dir is None:
        return
    try:
        import duckdb
        from datetime import datetime, timezone

        db_path = store_dir / "signals.db"
        store_dir.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(db_path))
        try:
            conn.execute(_MODULE_METRICS_DDL)
            conn.execute(
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
                    datetime.now(tz=timezone.utc).isoformat(),
                ],
            )
        finally:
            conn.close()
    except Exception as exc:
        logger.debug("Stage metrics write failed for %r: %s", module_id, exc)


_SUPPORTED_TYPES: frozenset[str] = frozenset(
    {"Ingress", "Egress", "Channel", "Junction", "Funnel", "Probe", "Regulator", "Assert"}
)

# Ports that carry control signals only, not DataFrames.
# Spillway IS a data edge (routes error rows); only "signal" is control-only.
_SIGNAL_PORTS: frozenset[str] = frozenset({"signal"})

# Sentinel placed in frame_store when a Regulator closes its gate with on_block=skip.
# Downstream modules that encounter it skip themselves and propagate the sentinel.
_GATE_CLOSED: object = object()


class ExecuteError(Exception):
    """Raised for unrecoverable execution failures (config, unsupported type, etc.)."""


# ── DAG helpers ───────────────────────────────────────────────────────────────

def _is_data_edge(edge: Edge) -> bool:
    return edge.port not in _SIGNAL_PORTS


def _frame_key(from_id: str, port: str) -> str:
    return from_id if port == "main" else f"{from_id}.{port}"


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
    probe_modules = [m for m in manifest.modules if m.type == "Probe"]
    non_probe_modules = tuple(m for m in manifest.modules if m.type != "Probe")

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
        if m.type == "Probe" and m.attach_to in included:
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

    Returns:
        Frozen ExecutionResult.

    Raises:
        ExecuteError: Unsupported module type, cycle detected, UDF registration
                      failure.  Most module errors are caught and recorded as
                      status="error" (fail-fast).  Probe errors are always swallowed.
    """
    from aqueduct.executor.spark.probe import execute_probe
    from aqueduct.executor.spark.udf import UDFError, register_udfs

    run_id = run_id or str(uuid.uuid4())

    # Grab listener attached by make_spark_session (None if session created externally)
    _listener = getattr(spark, "_aq_metrics_listener", None)

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
                    "Resuming run %r: Manifest has changed since original run "
                    "(hash %s → %s). Checkpoint data may be stale.",
                    resume_run_id, stored_hash, current_hash,
                )

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

    frame_store: dict[str, Any] = {}
    module_results: list[ModuleResult] = []

    for module in order:
        # ── Selector skip ─────────────────────────────────────────────────────
        if included_ids is not None and module.id not in included_ids:
            module_results.append(ModuleResult(module_id=module.id, status="skipped"))
            continue

        if module.type not in _SUPPORTED_TYPES:
            raise ExecuteError(
                f"Module type {module.type!r} (id={module.id!r}) is not supported. "
                f"Supported: {sorted(_SUPPORTED_TYPES)}."
            )

        # ── Resume: reload from checkpoint if available ───────────────────────
        if resume_dir:
            module_ckpt = resume_dir / module.id
            done_marker = module_ckpt / "_aq_done"
            if done_marker.exists():
                # Reload DataFrame checkpoints back into frame_store
                if module.type in ("Ingress", "Channel", "Funnel"):
                    data_ckpt = module_ckpt / "data"
                    if data_ckpt.exists():
                        frame_store[module.id] = spark.read.parquet(str(data_ckpt))
                elif module.type == "Junction":
                    for branch in module.config.get("branches", []):
                        bid = branch.get("id", "")
                        branch_ckpt = module_ckpt / bid
                        if branch_ckpt.exists():
                            frame_store[f"{module.id}.{bid}"] = spark.read.parquet(
                                str(branch_ckpt)
                            )
                module_results.append(
                    ModuleResult(module_id=module.id, status="success")
                )
                logger.info("Module %r: resumed from checkpoint, skipping execution", module.id)
                continue

        # ── Ingress ───────────────────────────────────────────────────────────
        if module.type == "Ingress":
            if _listener:
                _listener.set_active_module(module.id)
            mod_policy = _module_retry_policy(module, manifest.retry_policy)
            try:
                df = _with_retry(
                    lambda: read_ingress(module, spark),
                    mod_policy,
                    module.id,
                )
            except IngressError as exc:
                if _listener:
                    _listener.collect_metrics()  # reset state
                gate_closed, fail_result = _on_retry_exhausted(
                    exc, mod_policy, module, manifest.pipeline_id, run_id, module_results
                )
                if gate_closed:
                    frame_store[module.id] = _GATE_CLOSED
                    continue
                return fail_result
            if _listener:
                _write_stage_metrics(module.id, run_id, _listener.collect_metrics(), store_dir)
            frame_store[module.id] = df
            _write_checkpoint(module, checkpoint_dir, manifest, data={"data": df})
            module_results.append(ModuleResult(module_id=module.id, status="success"))

        # ── Channel ───────────────────────────────────────────────────────────
        elif module.type == "Channel":
            main_edges = _incoming_main(module.id, manifest.edges)
            if not main_edges:
                err = f"[{module.id}] Channel has no main-port incoming edges"
                module_results.append(
                    ModuleResult(module_id=module.id, status="error", error=err)
                )
                return _fail(manifest.pipeline_id, run_id, module_results)

            upstream_dfs: dict[str, Any] = {}
            for edge in main_edges:
                val = frame_store.get(edge.from_id)
                if _is_gate_closed(val):
                    frame_store[module.id] = _GATE_CLOSED
                    module_results.append(
                        ModuleResult(module_id=module.id, status="skipped")
                    )
                    break
                if val is None:
                    err = (
                        f"[{module.id}] upstream {edge.from_id!r} produced no DataFrame."
                    )
                    module_results.append(
                        ModuleResult(module_id=module.id, status="error", error=err)
                    )
                    return _fail(manifest.pipeline_id, run_id, module_results)
                upstream_dfs[edge.from_id] = val
            else:
                if _listener:
                    _listener.set_active_module(module.id)
                mod_policy = _module_retry_policy(module, manifest.retry_policy)
                try:
                    df = _with_retry(
                        lambda: execute_sql_channel(module, upstream_dfs, spark),
                        mod_policy,
                        module.id,
                    )
                except ChannelError as exc:
                    if _listener:
                        _listener.collect_metrics()
                    gate_closed, fail_result = _on_retry_exhausted(
                        exc, mod_policy, module, manifest.pipeline_id, run_id, module_results
                    )
                    if gate_closed:
                        frame_store[module.id] = _GATE_CLOSED
                        continue
                    return fail_result

                # ── Spillway split ─────────────────────────────────────────────
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
                          .withColumn("_aq_error_module", F.lit(module.id))
                          .withColumn("_aq_error_msg", F.lit("spillway_condition matched"))
                          .withColumn("_aq_error_ts", F.current_timestamp())
                    )
                    frame_store[module.id] = good_df
                    frame_store[f"{module.id}.spillway"] = error_df
                elif spillway_condition and not has_spillway_edge:
                    logger.warning(
                        "Channel %r has spillway_condition but no spillway edge; "
                        "all rows routed to main stream.", module.id
                    )
                    frame_store[module.id] = df
                elif has_spillway_edge and not spillway_condition:
                    logger.warning(
                        "Channel %r has a spillway edge but no spillway_condition; "
                        "spillway DataFrame will be empty.", module.id
                    )
                    frame_store[module.id] = df
                    frame_store[f"{module.id}.spillway"] = df.filter("1=0")
                else:
                    frame_store[module.id] = df

                if _listener:
                    _write_stage_metrics(module.id, run_id, _listener.collect_metrics(), store_dir)
                _write_checkpoint(module, checkpoint_dir, manifest, data={"data": frame_store[module.id]})
                module_results.append(ModuleResult(module_id=module.id, status="success"))

        # ── Junction ──────────────────────────────────────────────────────────
        elif module.type == "Junction":
            main_edges = _incoming_main(module.id, manifest.edges)
            if not main_edges:
                err = f"[{module.id}] Junction has no main-port incoming edges"
                module_results.append(
                    ModuleResult(module_id=module.id, status="error", error=err)
                )
                return _fail(manifest.pipeline_id, run_id, module_results)

            upstream_id = main_edges[0].from_id
            val = frame_store.get(upstream_id)
            if _is_gate_closed(val):
                branches = module.config.get("branches", [])
                for branch in branches:
                    frame_store[f"{module.id}.{branch.get('id', '')}"] = _GATE_CLOSED
                module_results.append(ModuleResult(module_id=module.id, status="skipped"))
                continue
            if val is None:
                err = (
                    f"[{module.id}] upstream {upstream_id!r} produced no DataFrame."
                )
                module_results.append(
                    ModuleResult(module_id=module.id, status="error", error=err)
                )
                return _fail(manifest.pipeline_id, run_id, module_results)

            if _listener:
                _listener.set_active_module(module.id)
            mod_policy = _module_retry_policy(module, manifest.retry_policy)
            try:
                branch_dfs = _with_retry(
                    lambda: execute_junction(module, val),
                    mod_policy,
                    module.id,
                )
            except JunctionError as exc:
                if _listener:
                    _listener.collect_metrics()
                gate_closed, fail_result = _on_retry_exhausted(
                    exc, mod_policy, module, manifest.pipeline_id, run_id, module_results
                )
                if gate_closed:
                    branches = module.config.get("branches", [])
                    for branch in branches:
                        frame_store[f"{module.id}.{branch.get('id', '')}"] = _GATE_CLOSED
                    continue
                return fail_result

            if _listener:
                _write_stage_metrics(module.id, run_id, _listener.collect_metrics(), store_dir)
            for branch_id, branch_df in branch_dfs.items():
                frame_store[f"{module.id}.{branch_id}"] = branch_df
            _write_checkpoint(module, checkpoint_dir, manifest, data=branch_dfs)
            module_results.append(ModuleResult(module_id=module.id, status="success"))

        # ── Funnel ────────────────────────────────────────────────────────────
        elif module.type == "Funnel":
            data_edges = _incoming_data(module.id, manifest.edges)
            if not data_edges:
                err = f"[{module.id}] Funnel has no incoming data edges"
                module_results.append(
                    ModuleResult(module_id=module.id, status="error", error=err)
                )
                return _fail(manifest.pipeline_id, run_id, module_results)

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
                    module_results.append(
                        ModuleResult(module_id=module.id, status="error", error=err)
                    )
                    return _fail(manifest.pipeline_id, run_id, module_results)
                funnel_upstream[store_key] = val

            if skipped:
                frame_store[module.id] = _GATE_CLOSED
                module_results.append(ModuleResult(module_id=module.id, status="skipped"))
                continue

            if _listener:
                _listener.set_active_module(module.id)
            mod_policy = _module_retry_policy(module, manifest.retry_policy)
            try:
                df = _with_retry(
                    lambda: execute_funnel(module, funnel_upstream),
                    mod_policy,
                    module.id,
                )
            except FunnelError as exc:
                if _listener:
                    _listener.collect_metrics()
                gate_closed, fail_result = _on_retry_exhausted(
                    exc, mod_policy, module, manifest.pipeline_id, run_id, module_results
                )
                if gate_closed:
                    frame_store[module.id] = _GATE_CLOSED
                    continue
                return fail_result
            if _listener:
                _write_stage_metrics(module.id, run_id, _listener.collect_metrics(), store_dir)
            frame_store[module.id] = df
            _write_checkpoint(module, checkpoint_dir, manifest, data={"data": df})
            module_results.append(ModuleResult(module_id=module.id, status="success"))

        # ── Assert ────────────────────────────────────────────────────────────
        elif module.type == "Assert":
            main_edges = _incoming_main(module.id, manifest.edges)
            if not main_edges:
                err = f"[{module.id}] Assert has no main-port incoming edges"
                module_results.append(
                    ModuleResult(module_id=module.id, status="error", error=err)
                )
                return _fail(manifest.pipeline_id, run_id, module_results)

            upstream_id = main_edges[0].from_id
            val = frame_store.get(upstream_id)
            if _is_gate_closed(val):
                frame_store[module.id] = _GATE_CLOSED
                module_results.append(ModuleResult(module_id=module.id, status="skipped"))
                continue
            if val is None:
                err = f"[{module.id}] upstream {upstream_id!r} produced no DataFrame."
                module_results.append(
                    ModuleResult(module_id=module.id, status="error", error=err)
                )
                return _fail(manifest.pipeline_id, run_id, module_results)

            has_spillway_edge = any(
                e.from_id == module.id and e.port == "spillway" for e in manifest.edges
            )

            try:
                passing_df, quarantine_df = execute_assert(
                    module, val, spark, run_id, manifest.pipeline_id,
                )
            except AssertError as exc:
                module_results.append(
                    ModuleResult(module_id=module.id, status="error", error=str(exc))
                )
                return _fail(
                    manifest.pipeline_id, run_id, module_results,
                    trigger_agent=exc.trigger_agent,
                )

            frame_store[module.id] = passing_df
            if quarantine_df is not None and has_spillway_edge:
                frame_store[f"{module.id}.spillway"] = quarantine_df
            elif quarantine_df is not None:
                logger.warning(
                    "[%s] Assert quarantine rows produced but no spillway edge; discarded.",
                    module.id,
                )
            module_results.append(ModuleResult(module_id=module.id, status="success"))

        # ── Regulator ─────────────────────────────────────────────────────────
        elif module.type == "Regulator":
            main_edges = _incoming_main(module.id, manifest.edges)
            if not main_edges:
                err = f"[{module.id}] Regulator has no main-port incoming edges"
                module_results.append(
                    ModuleResult(module_id=module.id, status="error", error=err)
                )
                return _fail(manifest.pipeline_id, run_id, module_results)

            upstream_id = main_edges[0].from_id
            val = frame_store.get(upstream_id)
            if _is_gate_closed(val):
                frame_store[module.id] = _GATE_CLOSED
                module_results.append(ModuleResult(module_id=module.id, status="skipped"))
                continue
            if val is None:
                err = (
                    f"[{module.id}] upstream {upstream_id!r} produced no DataFrame."
                )
                module_results.append(
                    ModuleResult(module_id=module.id, status="error", error=err)
                )
                return _fail(manifest.pipeline_id, run_id, module_results)

            # Evaluate gate — defaults to open when no surveyor
            gate_open = surveyor.evaluate_regulator(module.id) if surveyor else True

            if gate_open:
                frame_store[module.id] = val  # transparent pass-through
                module_results.append(ModuleResult(module_id=module.id, status="success"))
            else:
                on_block = module.config.get("on_block", "skip")
                if on_block == "abort":
                    err = f"[{module.id}] Regulator gate closed; on_block=abort"
                    module_results.append(
                        ModuleResult(module_id=module.id, status="error", error=err)
                    )
                    return _fail(manifest.pipeline_id, run_id, module_results)
                elif on_block == "trigger_agent":
                    logger.info(
                        "Regulator %r: gate closed, on_block=trigger_agent; "
                        "downstream skipped — LLM patch loop will fire.",
                        module.id,
                    )
                    module_results.append(
                        ModuleResult(
                            module_id=module.id,
                            status="error",
                            error=f"[{module.id}] Regulator gate closed; on_block=trigger_agent",
                        )
                    )
                    return _fail(manifest.pipeline_id, run_id, module_results, trigger_agent=True)
                # skip (default): propagate sentinel
                frame_store[module.id] = _GATE_CLOSED
                module_results.append(ModuleResult(module_id=module.id, status="skipped"))

        # ── Egress ────────────────────────────────────────────────────────────
        elif module.type == "Egress":
            data_edges = _incoming_data(module.id, manifest.edges)
            if not data_edges:
                err = f"[{module.id}] no main-port edge arriving at this Egress module"
                module_results.append(
                    ModuleResult(module_id=module.id, status="error", error=err)
                )
                return _fail(manifest.pipeline_id, run_id, module_results)

            edge = data_edges[0]
            key = _frame_key(edge.from_id, edge.port)
            val = frame_store.get(key)
            if _is_gate_closed(val):
                module_results.append(ModuleResult(module_id=module.id, status="skipped"))
                continue
            if val is None:
                err = (
                    f"[{module.id}] upstream {edge.from_id!r} produced no DataFrame."
                )
                module_results.append(
                    ModuleResult(module_id=module.id, status="error", error=err)
                )
                return _fail(manifest.pipeline_id, run_id, module_results)

            if _listener:
                _listener.set_active_module(module.id)
            mod_policy = _module_retry_policy(module, manifest.retry_policy)
            try:
                _with_retry(
                    lambda: write_egress(val, module, depot=depot),
                    mod_policy,
                    module.id,
                )
            except EgressError as exc:
                if _listener:
                    _listener.collect_metrics()
                gate_closed, fail_result = _on_retry_exhausted(
                    exc, mod_policy, module, manifest.pipeline_id, run_id, module_results
                )
                if gate_closed:
                    continue  # Egress is terminal; no frame_store update needed
                return fail_result
            if _listener:
                _write_stage_metrics(module.id, run_id, _listener.collect_metrics(), store_dir)
            _write_checkpoint(module, checkpoint_dir, manifest)  # done-sentinel only
            module_results.append(ModuleResult(module_id=module.id, status="success"))

        # ── Probe ─────────────────────────────────────────────────────────────
        elif module.type == "Probe":
            source_id = module.attach_to
            source_val = frame_store.get(source_id) if source_id else None

            if source_val is None or _is_gate_closed(source_val):
                logger.debug(
                    "Probe %r: attach_to=%r not available; skipping.",
                    module.id, source_id,
                )
            elif store_dir is not None:
                try:
                    execute_probe(module, source_val, spark, run_id, store_dir, block_full_actions=block_full_actions)
                except Exception as exc:
                    logger.warning("Probe %r failed: %s", module.id, exc)

            module_results.append(ModuleResult(module_id=module.id, status="success"))

    # ── Lineage — write after successful execution ─────────────────────────────
    if store_dir is not None:
        try:
            from aqueduct.compiler.lineage import write_lineage
            write_lineage(manifest.pipeline_id, run_id, manifest.modules, manifest.edges, store_dir)
        except Exception as exc:
            logger.debug("Lineage write skipped: %s", exc)

    return ExecutionResult(
        pipeline_id=manifest.pipeline_id,
        run_id=run_id,
        status="success",
        module_results=tuple(module_results),
    )


def _fail(
    pipeline_id: str,
    run_id: str,
    module_results: list[ModuleResult],
    *,
    trigger_agent: bool = False,
) -> ExecutionResult:
    return ExecutionResult(
        pipeline_id=pipeline_id,
        run_id=run_id,
        status="error",
        module_results=tuple(module_results),
        trigger_agent=trigger_agent,
    )


def _on_retry_exhausted(
    exc: Exception,
    policy: "RetryPolicy",
    module: "Module",
    pipeline_id: str,
    run_id: str,
    module_results: "list[ModuleResult]",
) -> "tuple[bool, ExecutionResult | None]":
    """Handle retry exhaustion per on_exhaustion policy.

    Returns:
        (gate_closed, fail_result) — if gate_closed is True the caller should
        set _GATE_CLOSED in frame_store and `continue`; fail_result is None.
        If gate_closed is False, return fail_result immediately.
    """
    module_results.append(ModuleResult(module_id=module.id, status="error", error=str(exc)))

    # Per-module failure webhook — fires regardless of on_exhaustion action.
    if module.on_failure_webhook is not None:
        from aqueduct.surveyor.webhook import fire_webhook
        raw = module.on_failure_webhook
        wh_cfg = WebhookEndpointConfig(**({"url": raw} if isinstance(raw, str) else raw))
        full_payload = {
            "run_id": run_id,
            "pipeline_id": pipeline_id,
            "module_id": module.id,
            "error_message": str(exc),
            "error_type": type(exc).__name__,
        }
        fire_webhook(
            wh_cfg,
            full_payload=full_payload,
            template_vars={k: str(v) for k, v in full_payload.items()},
        )

    on_exhaustion = policy.on_exhaustion
    if on_exhaustion == "alert_only":
        logger.warning("[%s] Retry exhausted (alert_only): %s — pipeline continues.", module.id, exc)
        return True, None
    elif on_exhaustion == "trigger_agent":
        return False, _fail(pipeline_id, run_id, module_results, trigger_agent=True)
    else:  # "abort" (default)
        return False, _fail(pipeline_id, run_id, module_results)
