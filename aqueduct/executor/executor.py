"""Executor orchestrator — runs a Manifest against a live SparkSession.

Phase 4 scope: Ingress, Egress, and Channel (op: sql).
Raises ExecuteError immediately on any unsupported module type.

Execution model:
  1. Topological sort of modules by main-port edges (Kahn's algorithm).
  2. Walk sorted order:
     - Ingress  → read_ingress()                        → frame_store[module.id]
     - Channel  → execute_sql_channel(upstream_dfs, …)  → frame_store[module.id]
     - Egress   → write_egress(frame_store[upstream_id], module)
     - Anything else → ExecuteError (unsupported)
  3. Return ExecutionResult (frozen).

No Spark actions beyond the Egress .save() call.
"""

from __future__ import annotations

import uuid
from collections import deque
from typing import TYPE_CHECKING, Any

from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from aqueduct.compiler.models import Manifest
from aqueduct.executor.channel import ChannelError, execute_sql_channel
from aqueduct.executor.egress import EgressError, write_egress
from aqueduct.executor.ingress import IngressError, read_ingress
from aqueduct.executor.models import ExecutionResult, ModuleResult
from aqueduct.parser.models import Edge, Module

# Module types handled in Phase 4
_SUPPORTED_TYPES: frozenset[str] = frozenset({"Ingress", "Egress", "Channel"})


class ExecuteError(Exception):
    """Raised for unrecoverable execution failures (config, unsupported type, etc.)."""


# ── DAG helpers ───────────────────────────────────────────────────────────────

def _topo_sort(modules: tuple[Module, ...], edges: tuple[Edge, ...]) -> list[Module]:
    """Kahn's topological sort using main-port data-flow edges only.

    Signal-port edges (Regulator, Probe) are ignored for execution ordering;
    they do not carry DataFrames.

    Raises:
        ExecuteError: If the graph contains a cycle (should not happen after
                      the Parser's cycle check, but guards against bad Manifests).
    """
    module_map: dict[str, Module] = {m.id: m for m in modules}
    in_degree: dict[str, int] = {m.id: 0 for m in modules}
    successors: dict[str, list[str]] = {m.id: [] for m in modules}

    for edge in edges:
        if edge.port == "main":
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


def _incoming_main(module_id: str, edges: tuple[Edge, ...]) -> list[Edge]:
    """Return edges arriving at module_id on the 'main' port."""
    return [e for e in edges if e.to_id == module_id and e.port == "main"]


# ── Public API ────────────────────────────────────────────────────────────────

def execute(
    manifest: Manifest,
    spark: SparkSession,
    run_id: str | None = None,
) -> ExecutionResult:
    """Execute a compiled Manifest.

    Args:
        manifest: Fully compiled, frozen Manifest from the Compiler.
        spark:    Active SparkSession (caller owns lifecycle).
        run_id:   Optional run identifier. Auto-generated UUID if not supplied.

    Returns:
        Frozen ExecutionResult.

    Raises:
        ExecuteError: Unsupported module type, cycle detected, or missing
                      upstream DataFrame.  I/O errors from Ingress/Egress/Channel
                      are caught and recorded in the result with status="error",
                      then execution halts (fail-fast).
    """
    run_id = run_id or str(uuid.uuid4())

    order = _topo_sort(manifest.modules, manifest.edges)

    frame_store: dict[str, DataFrame] = {}
    module_results: list[ModuleResult] = []

    for module in order:
        if module.type not in _SUPPORTED_TYPES:
            raise ExecuteError(
                f"Module type {module.type!r} (id={module.id!r}) is not supported. "
                f"Supported: {sorted(_SUPPORTED_TYPES)}."
            )

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

        elif module.type == "Channel":
            main_edges = _incoming_main(module.id, manifest.edges)
            if not main_edges:
                err = f"[{module.id}] Channel has no main-port incoming edges"
                module_results.append(
                    ModuleResult(module_id=module.id, status="error", error=err)
                )
                return _fail(manifest.pipeline_id, run_id, module_results)

            # Collect upstream DataFrames in edge order
            upstream_dfs: dict[str, DataFrame] = {}
            for edge in main_edges:
                df = frame_store.get(edge.from_id)
                if df is None:
                    err = (
                        f"[{module.id}] upstream module {edge.from_id!r} produced "
                        f"no DataFrame. Check that it completed successfully."
                    )
                    module_results.append(
                        ModuleResult(module_id=module.id, status="error", error=err)
                    )
                    return _fail(manifest.pipeline_id, run_id, module_results)
                upstream_dfs[edge.from_id] = df

            try:
                df = execute_sql_channel(module, upstream_dfs, spark)
            except ChannelError as exc:
                module_results.append(
                    ModuleResult(module_id=module.id, status="error", error=str(exc))
                )
                return _fail(manifest.pipeline_id, run_id, module_results)

            frame_store[module.id] = df
            module_results.append(ModuleResult(module_id=module.id, status="success"))

        elif module.type == "Egress":
            main_edges = _incoming_main(module.id, manifest.edges)
            if not main_edges:
                err = f"[{module.id}] no main-port edge arriving at this Egress module"
                module_results.append(
                    ModuleResult(module_id=module.id, status="error", error=err)
                )
                return _fail(manifest.pipeline_id, run_id, module_results)

            upstream_id = main_edges[0].from_id
            df = frame_store.get(upstream_id)
            if df is None:
                err = (
                    f"[{module.id}] upstream module {upstream_id!r} produced no DataFrame. "
                    f"Check that it completed successfully."
                )
                module_results.append(
                    ModuleResult(module_id=module.id, status="error", error=err)
                )
                return _fail(manifest.pipeline_id, run_id, module_results)

            try:
                write_egress(df, module)
            except EgressError as exc:
                module_results.append(
                    ModuleResult(module_id=module.id, status="error", error=str(exc))
                )
                return _fail(manifest.pipeline_id, run_id, module_results)

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
