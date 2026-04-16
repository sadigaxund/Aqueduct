"""Module graph validation — cycle detection and topological ordering.

Uses Kahn's algorithm (BFS-based topological sort). A non-empty remainder
after processing means at least one cycle exists.

Edge sources:
  - Explicit edges list in the Blueprint
  - depends_on fields on individual Modules
"""

from __future__ import annotations

from collections import defaultdict, deque

from aqueduct.parser.models import Edge, Module


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
            raise ValueError(f"{context}: references unknown module {from_id!r}")
        if to_id not in module_ids:
            raise ValueError(f"{context}: references unknown module {to_id!r}")
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
        raise ValueError(
            f"Cycle detected in module graph. Involved modules: {cycle_nodes}"
        )

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
            raise ValueError(
                f"Module {module.id!r} spillway target {module.spillway!r} does not exist"
            )
