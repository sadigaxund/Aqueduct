"""Arcade expansion — replaces Arcade modules with flat namespaced sub-Blueprints.

Expansion contract (per spec §4.4 Arcade):
  - Arcade modules are expanded at Manifest compile time.
  - The Manifest always contains a flat Module list (no nesting).
  - Module IDs within Arcades are namespaced: {arcade_id}__{child_id}.
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
from typing import Any

from aqueduct.parser.models import Blueprint, Edge, Module


class ExpandError(Exception):
    """Raised when an Arcade cannot be expanded."""


def _entry_modules(sub_bp: Blueprint) -> list[str]:
    """Module IDs in sub-Blueprint with no incoming edges (graph entry points)."""
    targets = {e.to_id for e in sub_bp.edges}
    return [m.id for m in sub_bp.modules if m.id not in targets]


def _exit_modules(sub_bp: Blueprint) -> list[str]:
    """Module IDs in sub-Blueprint with no outgoing edges (graph exit points).
    Note: Egress and Probe modules are excluded because they don't produce a DataFrame
    for downstream consumption.
    """
    sources = {e.from_id for e in sub_bp.edges}
    return [
        m.id for m in sub_bp.modules
        if m.id not in sources and m.type not in ("Egress", "Probe")
    ]


def _load_raw_module_configs(sub_path: Path) -> dict[str, dict]:
    """Load sub-blueprint YAML without context resolution.

    Returns {module_id: raw_config_dict} so we can capture original expressions
    (e.g. '${ctx.input_path}') before they were resolved via context_override.
    Uses plain yaml.safe_load — no Pydantic, no resolution, no side effects.
    """
    import yaml
    try:
        raw = yaml.safe_load(sub_path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {}
        return {
            m["id"]: (m.get("config") or {})
            for m in (raw.get("modules") or [])
            if isinstance(m, dict) and "id" in m
        }
    except Exception:
        return {}


def _expand_single(
    arcade: Module,
    sub_bp: Blueprint,
    parent_edges: list[Edge],
    sub_blueprint_path: str,
    raw_module_configs: dict[str, dict],
) -> tuple[list[Module], list[Edge], dict]:
    """Expand one Arcade module into namespaced sub-modules + rewired edges + provenance.

    Returns:
        (expanded_modules, all_edges, arcade_provenance)
        arcade_provenance: dict[namespaced_module_id → ModuleProvenance]
    """
    from aqueduct.compiler.provenance import ModuleProvenance, build_config_provenance

    ns = arcade.id
    id_map = {m.id: f"{ns}__{m.id}" for m in sub_bp.modules}

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

    # Build provenance for each expanded module
    # raw_module_configs has the pre-resolution config (original ${ctx.*} expressions)
    # sub_bp.modules have the resolved config (context_override applied)
    arcade_provenance: dict[str, ModuleProvenance] = {}
    sub_bp_modules_by_orig_id = {m.id: m for m in sub_bp.modules}
    for orig_m, expanded_m in zip(sub_bp.modules, expanded_modules):
        raw_cfg = raw_module_configs.get(orig_m.id, {})
        resolved_cfg = orig_m.config or {}
        config_prov = build_config_provenance(
            raw_cfg,
            resolved_cfg,
            arcade_module_id=arcade.id,
            arcade_sub_module_id=orig_m.id,
            sub_blueprint_path=sub_blueprint_path,
        )
        arcade_provenance[expanded_m.id] = ModuleProvenance(
            module_id=expanded_m.id,
            module_type=expanded_m.type,
            arcade_module_id=arcade.id,
            sub_blueprint_path=sub_blueprint_path,
            original_module_id=orig_m.id,
            config=config_prov,
        )

    return expanded_modules, internal_edges + rewired, arcade_provenance


def expand_arcades(
    modules: list[Module],
    edges: list[Edge],
    base_dir: Path,
) -> tuple[list[Module], list[Edge], dict]:
    """Expand all Arcade modules in-place.

    Arcades are resolved depth-first: an Arcade inside an Arcade sub-Blueprint
    will also be expanded. Maximum depth is 10.

    Args:
        modules:  Module list from the parsed Blueprint.
        edges:    Edge list from the parsed Blueprint.
        base_dir: Directory of the parent Blueprint file (ref paths are relative).

    Returns:
        (expanded_modules, rewired_edges, arcade_provenance_map)
        arcade_provenance_map: dict[manifest_module_id → ModuleProvenance]
    """
    return _expand_recursive(modules, edges, base_dir, depth=0)


def _expand_recursive(
    modules: list[Module],
    edges: list[Edge],
    base_dir: Path,
    depth: int,
) -> tuple[list[Module], list[Edge], dict]:
    if depth > 10:
        raise ExpandError("Arcade nesting depth exceeds 10. Check for recursive Arcade refs.")

    has_arcade = any(m.type == "Arcade" for m in modules)
    if not has_arcade:
        return modules, edges, {}

    result_modules: list[Module] = []
    merged_provenance: dict = {}

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

        # Load raw YAML first (for provenance original expressions)
        raw_module_configs = _load_raw_module_configs(sub_path)

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

        # Validate required_context — ensure parent provides all keys the sub-Blueprint needs
        missing = [
            key for key in (sub_bp.required_context or [])
            if key not in (m.context_override or {})
        ]
        if missing:
            raise ExpandError(
                f"Arcade {m.id!r}: sub-Blueprint requires context keys {missing} "
                f"that are not provided in context_override. "
                f"Add them under the Arcade module's context_override field."
            )

        sub_blueprint_path = str(Path(m.ref))  # relative path as declared in Blueprint
        expanded_mods, new_edges, arcade_prov = _expand_single(
            m, sub_bp, edges, sub_blueprint_path, raw_module_configs,
        )
        result_modules.extend(expanded_mods)
        merged_provenance.update(arcade_prov)

        exit_ids_ns = [f"{m.id}__{eid}" for eid in _exit_modules(sub_bp)]

        # Helper to recursively replace IDs in nested structures
        def _replace_ids(val: Any) -> Any:
            if isinstance(val, str):
                return exit_ids_ns if val == m.id else val
            if isinstance(val, list):
                new_list = []
                for item in val:
                    res = _replace_ids(item)
                    if isinstance(res, list):
                        new_list.extend(res)
                    else:
                        new_list.append(res)
                return new_list
            if isinstance(val, dict):
                return {k: _replace_ids(v) for k, v in val.items()}
            return val

        # Update references in all modules (processed and pending)
        for other_m in (result_modules + modules):
            if other_m.config and "inputs" in other_m.config and m.id in other_m.config["inputs"]:
                other_m.config["inputs"] = _replace_ids(other_m.config["inputs"])

        # Replace edges touching this Arcade with rewired ones
        edges = [
            e for e in edges
            if e.from_id != m.id and e.to_id != m.id
        ] + new_edges

    # Detect ID collisions produced by expansion before recursing
    seen: dict[str, str] = {}
    for mod in result_modules:
        if mod.id in seen:
            raise ExpandError(
                f"Module ID collision after Arcade expansion: '{mod.id}' appears more than once. "
                f"An Arcade expansion produced this ID and it conflicts with an existing module. "
                f"Rename the conflicting module or the Arcade/child module."
            )
        seen[mod.id] = mod.id

    # Recurse in case sub-Blueprints themselves contain Arcades
    rec_modules, rec_edges, rec_prov = _expand_recursive(result_modules, edges, base_dir, depth + 1)
    merged_provenance.update(rec_prov)
    return rec_modules, rec_edges, merged_provenance
