"""Module graph validation — cycle detection and topological ordering.

Uses Kahn's algorithm (BFS-based topological sort). A non-empty remainder
after processing means at least one cycle exists.

Edge sources:
  - Explicit edges list in the Blueprint
  - depends_on fields on individual Modules
"""

from __future__ import annotations

from collections import defaultdict, deque

from aqueduct.errors import ParseError

# Same accepted cross-layer exception as `parser/parser.py` importing
# `executor/path_keys.py`: `channel_ops.py` is the pyspark-free hoist of the
# canonical op names, already imported by the (equally pyspark-free)
# capability-leaf walker. No Spark import is involved.
from aqueduct.executor.channel_ops import SQL_OPS
from aqueduct.parser.models import Edge, Module, ModuleType

# Ports every module understands without a Junction being involved. Anything
# else on an edge is a Junction branch id, whose frame key is dotted
# (`<junction_id>.<branch_id>`) and therefore not writable in SQL.
_STRUCTURAL_PORTS: frozenset[str] = frozenset({"main", "signal", "spillway"})


def _build_adjacency(
    modules: list[Module], edges: list[Edge]
) -> tuple[dict[str, list[str]], dict[str, int]]:
    """Build adjacency list and in-degree map from modules + edges.

    Raises ValueError for edges referencing unknown module IDs.
    """
    module_ids = {m.id for m in modules}
    adj: dict[str, list[str]] = defaultdict(list)
    in_degree: dict[str, int] = {m.id: 0 for m in modules}

    def _add_edge(from_id: str, to_id: str, context: str) -> None:
        if from_id not in module_ids:
            raise ParseError(f"{context}: references unknown module {from_id!r}")
        if to_id not in module_ids:
            raise ParseError(f"{context}: references unknown module {to_id!r}")
        adj[from_id].append(to_id)
        in_degree[to_id] += 1

    for edge in edges:
        _add_edge(edge.from_id, edge.to_id, f"Edge ({edge.from_id} → {edge.to_id})")

    for module in modules:
        for dep_id in module.depends_on:
            _add_edge(dep_id, module.id, f"Module {module.id!r} depends_on {dep_id!r}")

    return dict(adj), in_degree


def detect_cycles(modules: list[Module], edges: list[Edge]) -> list[str]:
    """Return module IDs involved in a cycle, or [] if the graph is a valid DAG."""
    adj, in_degree = _build_adjacency(modules, edges)

    queue = deque(node for node, deg in in_degree.items() if deg == 0)
    visited = 0

    while queue:
        node = queue.popleft()
        visited += 1
        for neighbour in adj.get(node, []):
            in_degree[neighbour] -= 1
            if in_degree[neighbour] == 0:
                queue.append(neighbour)

    if visited < len(modules):
        return [node for node, deg in in_degree.items() if deg > 0]
    return []


def topological_order(modules: list[Module], edges: list[Edge]) -> list[str]:
    """Return module IDs in topological execution order.

    Raises ValueError if the graph contains a cycle.
    """
    cycle_nodes = detect_cycles(modules, edges)
    if cycle_nodes:
        raise ParseError(f"Cycle detected in module graph. Involved modules: {cycle_nodes}")

    adj, in_degree = _build_adjacency(modules, edges)
    queue = deque(node for node, deg in in_degree.items() if deg == 0)
    order: list[str] = []

    while queue:
        node = queue.popleft()
        order.append(node)
        for neighbour in adj.get(node, []):
            in_degree[neighbour] -= 1
            if in_degree[neighbour] == 0:
                queue.append(neighbour)

    return order


def validate_spillway_targets(modules: list[Module]) -> None:
    """Verify that spillway targets reference existing module IDs."""
    module_ids = {m.id for m in modules}
    for module in modules:
        if module.spillway and module.spillway not in module_ids:
            raise ParseError(
                f"Module {module.id!r} spillway target {module.spillway!r} does not exist"
            )


def validate_edge_error_types(edges: list[Edge]) -> None:
    """``error_types`` is a spillway-only filter (typed catch) — reject it on
    any other port, where it would be silently ignored."""
    for edge in edges:
        if edge.error_types and edge.port != "spillway":
            raise ParseError(
                f"Edge {edge.from_id!r} -> {edge.to_id!r}: error_types is only "
                f"valid on port='spillway' (got port={edge.port!r}). It filters "
                "quarantined rows by their _aq_error_type label."
            )


def validate_edge_aliases(modules: list[Module], edges: list[Edge]) -> None:
    """Enforce the four rules of the edge `as:` alias.

    `as` names an edge's frame inside the module it points at, which only a
    Channel ever reads (its SQL text, or `left:`/`right:` on `op: join`).
    The rules:

    a. an edge on a Junction branch port into a multi-input SQL Channel MUST
       carry `as` — there is no other name its SQL could use, because the
       frame key is dotted;
    b. an `as` may not collide with a module id, nor with another `as` on an
       edge into the same module;
    c. `as` on a single-input Channel is fine, and simply names the frame
       alongside the `__input__` alias every single-input Channel gets;
    d. `as` on anything but a Channel is an error, because nothing else
       resolves an upstream by name.
    """
    modules_by_id = {m.id: m for m in modules}
    module_ids = set(modules_by_id)

    aliases_per_target: dict[str, dict[str, Edge]] = {}
    for edge in edges:
        if edge.alias is None:
            continue
        target = modules_by_id.get(edge.to_id)
        if target is None or target.type != ModuleType.Channel:
            kind = "unknown module" if target is None else str(target.type)
            raise ParseError(
                f"Edge {edge.from_id!r} -> {edge.to_id!r}: as={edge.alias!r} is only "
                f"valid on an edge into a Channel (target is a {kind}). Only a "
                "Channel refers to an upstream by name."
            )
        if edge.alias in module_ids:
            raise ParseError(
                f"Edge {edge.from_id!r} -> {edge.to_id!r}: as={edge.alias!r} collides "
                "with a module id of the same name. Pick a name no module uses."
            )
        claimed = aliases_per_target.setdefault(edge.to_id, {})
        prior = claimed.get(edge.alias)
        if prior is not None:
            raise ParseError(
                f"Edge {edge.from_id!r} -> {edge.to_id!r}: as={edge.alias!r} is already "
                f"used by edge {prior.from_id!r} -> {prior.to_id!r}. Two inputs of "
                f"{edge.to_id!r} cannot share one name."
            )
        claimed[edge.alias] = edge

    inbound: dict[str, list[Edge]] = {}
    for edge in edges:
        if edge.port == "signal":
            continue
        inbound.setdefault(edge.to_id, []).append(edge)

    for to_id, in_edges in inbound.items():
        target = modules_by_id.get(to_id)
        if target is None or target.type != ModuleType.Channel:
            continue
        if len(in_edges) < 2:
            continue
        if str(target.config.get("op", "")).lower() not in SQL_OPS:
            continue
        for edge in in_edges:
            if edge.port in _STRUCTURAL_PORTS or edge.alias is not None:
                continue
            raise ParseError(
                f"Edge {edge.from_id!r} -> {edge.to_id!r} (port {edge.port!r}) needs an "
                f"`as:` name. Channel {to_id!r} has {len(in_edges)} inputs, and a "
                f"Junction branch arrives as {edge.from_id}.{edge.port}, which is not a "
                "name SQL can reference. Add `as: <name>` to the edge and use that "
                "name in the query."
            )


def validate_watermark_keys(modules: list[Module], edges: list[Edge]) -> None:
    """Every Egress `watermark_key: K` must be matched by another Egress in
    the same blueprint that writes K via `format: depot`, and that depot
    Egress must be topologically AFTER the append Egress in the blueprint's
    edge graph (i.e. reachable from it) — otherwise the depot write can run
    (or even complete) before the append it is supposed to gate, and the
    crash-consistency intent row it is meant to clear is never guaranteed to
    be cleared after the write it protects.

    `EgressConfigSchema._validate_watermark_key` already enforces the
    per-module shape (append mode, a real row-writing format); this is the
    blueprint-level half — an intent row set by `watermark_key` that nothing
    ever clears (no `format: depot` Egress writes that key, or one does but
    is not downstream of the append) is a Blueprint that can never pass the
    run-start refusal check once it takes its first write, so it is rejected
    here instead of failing confusingly at run time.
    """
    depot_module_ids_by_key: dict[str, list[str]] = defaultdict(list)
    for module in modules:
        if module.type != ModuleType.Egress:
            continue
        if module.config.get("format") == "depot":
            key = module.config.get("key")
            if key:
                depot_module_ids_by_key[str(key)].append(module.id)

    append_modules = [
        module
        for module in modules
        if module.type == ModuleType.Egress and module.config.get("watermark_key")
    ]
    if not append_modules:
        return

    adj, _ = _build_adjacency(modules, edges)

    def _reachable_from(start: str) -> set[str]:
        seen = {start}
        queue = deque([start])
        while queue:
            node = queue.popleft()
            for neighbour in adj.get(node, []):
                if neighbour not in seen:
                    seen.add(neighbour)
                    queue.append(neighbour)
        return seen

    for module in append_modules:
        watermark_key = str(module.config.get("watermark_key"))
        depot_ids = depot_module_ids_by_key.get(watermark_key, [])
        if not depot_ids:
            raise ParseError(
                f"Egress {module.id!r}: watermark_key={watermark_key!r} names a depot "
                "key that no other Egress in this blueprint writes via "
                f"'format: depot' + key: {watermark_key!r}. Add a downstream "
                "`format: depot` Egress that writes this key (it clears the crash-"
                "consistency intent row `watermark_key` sets), or remove `watermark_key`."
            )

        reachable = _reachable_from(module.id)
        if not any(depot_id in reachable for depot_id in depot_ids):
            raise ParseError(
                f"Egress {module.id!r}: watermark_key={watermark_key!r} is written by "
                f"depot Egress {depot_ids!r}, but none of them is reachable from "
                f"{module.id!r} in the blueprint's edge graph — either the edges "
                f"connect them in the wrong order (the depot Egress runs before or "
                f"in parallel with {module.id!r} instead of after it) or they are not "
                f"connected at all. Add edges so {module.id!r} is topologically "
                f"BEFORE a `format: depot` Egress writing {watermark_key!r} (a path "
                f"{module.id!r} -> ... -> one of {depot_ids!r}), or remove "
                "`watermark_key`."
            )
