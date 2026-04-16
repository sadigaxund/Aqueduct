"""Arcade expansion — replaces Arcade modules with flat namespaced sub-Blueprints.

Expansion contract (per spec §4.4 Arcade):
  - Arcade modules are expanded at Manifest compile time.
  - The Manifest always contains a flat Module list (no nesting).
  - Module IDs within Arcades are namespaced: {arcade_id}.{child_id}.
  - Spark sees a single flat execution plan with no nesting overhead.

Edge rewiring:
  - Entry modules: sub-Blueprint modules with no incoming internal edges.
    Parent edges pointing TO the Arcade are redirected to all entry modules.
  - Exit modules: sub-Blueprint modules with no outgoing internal edges.
    Parent edges pointing FROM the Arcade are redirected from all exit modules.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

from aqueduct.parser.models import Blueprint, Edge, Module


class ExpandError(Exception):
    """Raised when an Arcade cannot be expanded."""


def _entry_modules(sub_bp: Blueprint) -> list[str]:
    """Module IDs in sub-Blueprint with no incoming edges (graph entry points)."""
    targets = {e.to_id for e in sub_bp.edges}
    return [m.id for m in sub_bp.modules if m.id not in targets]


def _exit_modules(sub_bp: Blueprint) -> list[str]:
    """Module IDs in sub-Blueprint with no outgoing edges (graph exit points)."""
    sources = {e.from_id for e in sub_bp.edges}
    return [m.id for m in sub_bp.modules if m.id not in sources]


def _expand_single(
    arcade: Module,
    sub_bp: Blueprint,
    parent_edges: list[Edge],
) -> tuple[list[Module], list[Edge]]:
    """Expand one Arcade module into namespaced sub-modules + rewired edges."""
    ns = arcade.id
    id_map = {m.id: f"{ns}.{m.id}" for m in sub_bp.modules}

    # Namespace sub-Blueprint modules
    expanded_modules: list[Module] = []
    for m in sub_bp.modules:
        expanded_modules.append(
            dataclasses.replace(
                m,
                id=id_map[m.id],
                # Namespace any internal references
                attach_to=id_map.get(m.attach_to) if m.attach_to else None,
                spillway=id_map.get(m.spillway) if m.spillway else None,
                depends_on=tuple(id_map.get(d, d) for d in m.depends_on),
            )
        )

    # Namespace internal edges
    internal_edges: list[Edge] = [
        Edge(
            from_id=id_map[e.from_id],
            to_id=id_map[e.to_id],
            port=e.port,
            error_types=e.error_types,
        )
        for e in sub_bp.edges
    ]

    entry_ids = _entry_modules(sub_bp)
    exit_ids = _exit_modules(sub_bp)

    if not entry_ids:
        raise ExpandError(
            f"Arcade {arcade.id!r}: sub-Blueprint has no entry modules (cycle?)"
        )
    if not exit_ids:
        raise ExpandError(
            f"Arcade {arcade.id!r}: sub-Blueprint has no exit modules (cycle?)"
        )

    # Rewire parent edges
    rewired: list[Edge] = []
    for e in parent_edges:
        if e.to_id == arcade.id:
            # Fan-out: connect parent upstream → each entry module
            for entry in entry_ids:
                rewired.append(
                    dataclasses.replace(e, to_id=id_map[entry])
                )
        elif e.from_id == arcade.id:
            # Fan-in: connect each exit module → parent downstream
            for exit_id in exit_ids:
                rewired.append(
                    dataclasses.replace(e, from_id=id_map[exit_id])
                )

    return expanded_modules, internal_edges + rewired


def expand_arcades(
    modules: list[Module],
    edges: list[Edge],
    base_dir: Path,
) -> tuple[list[Module], list[Edge]]:
    """Expand all Arcade modules in-place.

    Arcades are resolved depth-first: an Arcade inside an Arcade sub-Blueprint
    will also be expanded. Maximum depth is 10.

    Args:
        modules:  Module list from the parsed Blueprint.
        edges:    Edge list from the parsed Blueprint.
        base_dir: Directory of the parent Blueprint file (ref paths are relative).
    """
    return _expand_recursive(modules, edges, base_dir, depth=0)


def _expand_recursive(
    modules: list[Module],
    edges: list[Edge],
    base_dir: Path,
    depth: int,
) -> tuple[list[Module], list[Edge]]:
    if depth > 10:
        raise ExpandError("Arcade nesting depth exceeds 10. Check for recursive Arcade refs.")

    has_arcade = any(m.type == "Arcade" for m in modules)
    if not has_arcade:
        return modules, edges

    result_modules: list[Module] = []
    arcade_edges_consumed: set[int] = set()

    for m in modules:
        if m.type != "Arcade":
            result_modules.append(m)
            continue

        # Load and parse sub-Blueprint
        if not m.ref:
            raise ExpandError(f"Arcade module {m.id!r} is missing a 'ref' field")

        sub_path = base_dir / m.ref
        if not sub_path.exists():
            raise ExpandError(
                f"Arcade {m.id!r}: sub-Blueprint not found at {sub_path}"
            )

        # Inline import to avoid circular deps (parser → compiler → parser)
        from aqueduct.parser.parser import parse, ParseError  # noqa: PLC0415

        try:
            sub_bp = parse(
                sub_path,
                cli_overrides=m.context_override or {},
            )
        except ParseError as exc:
            raise ExpandError(
                f"Arcade {m.id!r}: failed to parse sub-Blueprint {sub_path}: {exc}"
            ) from exc

        # Validate required_context (stored in Blueprint but we need raw YAML for this)
        # Phase 2: skip required_context validation — context_override already passed to parse()

        expanded_mods, new_edges = _expand_single(m, sub_bp, edges)
        result_modules.extend(expanded_mods)

        # Replace edges touching this Arcade with rewired ones
        edges = [
            e for e in edges
            if e.from_id != m.id and e.to_id != m.id
        ] + new_edges

    # Recurse in case sub-Blueprints themselves contain Arcades
    return _expand_recursive(result_modules, edges, base_dir, depth + 1)
