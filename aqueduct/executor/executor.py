"""Executor orchestrator — runs a Manifest against a live SparkSession.

Supported module types: Ingress, Egress, Channel, Junction, Funnel, Probe, Regulator.
Raises ExecuteError immediately on any other unsupported module type.

Execution model:
  1. Topological sort of non-Probe modules by data-flow edges.
     Probes are inserted immediately after their attach_to module so that
     Regulator evaluation sees fresh signals.
  2. Walk sorted order:
     - Ingress    → read_ingress()                    → frame_store[module.id]
     - Channel    → execute_sql_channel(…)            → frame_store[module.id]
     - Junction   → execute_junction(…)               → frame_store["id.branch"]
     - Funnel     → execute_funnel(…)                 → frame_store[module.id]
     - Regulator  → evaluate gate; open → pass-through; closed → on_block action
     - Egress     → write_egress(frame_store[key], …)
     - Probe      → execute_probe(…) side-effect only; never halts pipeline
     - Other      → ExecuteError (unsupported)
  3. Return ExecutionResult (frozen).

Frame store key convention:
  "main" port edge  → key = from_id
  branch port edge  → key = f"{from_id}.{port}"

Signal ports ("signal", "spillway") carry no DataFrames and are excluded
from the topo-sort and frame-store lookups.

Regulator skip propagation:
  When a Regulator's gate is closed with on_block=skip, _GATE_CLOSED is
  placed in the frame_store.  Every downstream module that receives this
  sentinel skips itself (status="skipped") and propagates the sentinel,
  so the entire skipped sub-graph is recorded without aborting the pipeline.

No Spark actions beyond the Egress .save() and Probe sampling calls.
"""

from __future__ import annotations

import logging
import uuid
from collections import defaultdict, deque
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from aqueduct.compiler.models import Manifest
from aqueduct.executor.channel import ChannelError, execute_sql_channel
from aqueduct.executor.egress import EgressError, write_egress
from aqueduct.executor.funnel import FunnelError, execute_funnel
from aqueduct.executor.ingress import IngressError, read_ingress
from aqueduct.executor.junction import JunctionError, execute_junction
from aqueduct.executor.models import ExecutionResult, ModuleResult
from aqueduct.parser.models import Edge, Module

logger = logging.getLogger(__name__)

_SUPPORTED_TYPES: frozenset[str] = frozenset(
    {"Ingress", "Egress", "Channel", "Junction", "Funnel", "Probe", "Regulator"}
)

# Ports that carry control signals, not DataFrames
_SIGNAL_PORTS: frozenset[str] = frozenset({"signal", "spillway"})

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


# ── Public API ────────────────────────────────────────────────────────────────

def execute(
    manifest: Manifest,
    spark: SparkSession,
    run_id: str | None = None,
    store_dir: Path | None = None,
    surveyor: Any | None = None,
) -> ExecutionResult:
    """Execute a compiled Manifest.

    Args:
        manifest:  Fully compiled, frozen Manifest.
        spark:     Active SparkSession (caller owns lifecycle).
        run_id:    Optional run identifier; auto-generated UUID if omitted.
        store_dir: Optional path for observability signals (Probe I/O).
        surveyor:  Optional Surveyor instance used to evaluate Regulator gates.
                   When None, all active Regulators default to open (pass-through).

    Returns:
        Frozen ExecutionResult.

    Raises:
        ExecuteError: Unsupported module type, cycle detected.  Most module
                      errors are caught and recorded as status="error" (fail-fast).
                      Probe errors are always swallowed.
    """
    from aqueduct.executor.probe import execute_probe

    run_id = run_id or str(uuid.uuid4())
    order = _build_execution_order(manifest)

    frame_store: dict[str, Any] = {}
    module_results: list[ModuleResult] = []

    for module in order:
        if module.type not in _SUPPORTED_TYPES:
            raise ExecuteError(
                f"Module type {module.type!r} (id={module.id!r}) is not supported. "
                f"Supported: {sorted(_SUPPORTED_TYPES)}."
            )

        # ── Ingress ───────────────────────────────────────────────────────────
        if module.type == "Ingress":
            try:
                df = read_ingress(module, spark)
            except IngressError as exc:
                module_results.append(
                    ModuleResult(module_id=module.id, status="error", error=str(exc))
                )
                return _fail(manifest.pipeline_id, run_id, module_results)
            frame_store[module.id] = df
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
                try:
                    df = execute_sql_channel(module, upstream_dfs, spark)
                except ChannelError as exc:
                    module_results.append(
                        ModuleResult(module_id=module.id, status="error", error=str(exc))
                    )
                    return _fail(manifest.pipeline_id, run_id, module_results)
                frame_store[module.id] = df
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

            try:
                branch_dfs = execute_junction(module, val)
            except JunctionError as exc:
                module_results.append(
                    ModuleResult(module_id=module.id, status="error", error=str(exc))
                )
                return _fail(manifest.pipeline_id, run_id, module_results)

            for branch_id, branch_df in branch_dfs.items():
                frame_store[f"{module.id}.{branch_id}"] = branch_df
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

            try:
                df = execute_funnel(module, funnel_upstream)
            except FunnelError as exc:
                module_results.append(
                    ModuleResult(module_id=module.id, status="error", error=str(exc))
                )
                return _fail(manifest.pipeline_id, run_id, module_results)
            frame_store[module.id] = df
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
                    # Stub for Phase 7 — fall through to skip for now
                    logger.info(
                        "Regulator %r: gate closed, on_block=trigger_agent (stub — skipping downstream)",
                        module.id,
                    )
                # skip (default) and trigger_agent stub: propagate sentinel
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

            try:
                write_egress(val, module)
            except EgressError as exc:
                module_results.append(
                    ModuleResult(module_id=module.id, status="error", error=str(exc))
                )
                return _fail(manifest.pipeline_id, run_id, module_results)
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
                    execute_probe(module, source_val, spark, run_id, store_dir)
                except Exception as exc:
                    logger.warning("Probe %r failed: %s", module.id, exc)

            module_results.append(ModuleResult(module_id=module.id, status="success"))

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
) -> ExecutionResult:
    return ExecutionResult(
        pipeline_id=pipeline_id,
        run_id=run_id,
        status="error",
        module_results=tuple(module_results),
    )
