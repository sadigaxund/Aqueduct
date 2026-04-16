"""Probe/Spillway validation and Regulator compile-away.

Probe wiring:
  Probes use attach_to (a module-level field) rather than edges.
  The wirer validates that every Probe's attach_to references an existing module.
  No structural changes are made — Probes stay in the module list.

Spillway wiring:
  Validated during parsing (graph.validate_spillway_targets).
  The wirer confirms that any edge with port='spillway' points to a real module.

Regulator compile-away (P6 — Passive-by-default gates):
  A Regulator with no wired signal-port edge is passive and is compiled away.
  Its edges are bypassed: upstream modules connect directly to downstream modules.
"""

from __future__ import annotations

import dataclasses

from aqueduct.parser.models import Edge, Module


class WireError(Exception):
    """Raised when a wiring validation fails."""


def validate_probes(modules: list[Module]) -> None:
    """Verify every Probe module has a valid attach_to target."""
    module_ids = {m.id for m in modules}
    for m in modules:
        if m.type != "Probe":
            continue
        if not m.attach_to:
            raise WireError(
                f"Probe {m.id!r} is missing attach_to. "
                "Every Probe must specify which module output it taps."
            )
        if m.attach_to not in module_ids:
            raise WireError(
                f"Probe {m.id!r} attach_to={m.attach_to!r} references a module that does not exist."
            )


def validate_spillway_edges(modules: list[Module], edges: list[Edge]) -> None:
    """Verify that spillway-port edges point to existing modules."""
    module_ids = {m.id for m in modules}
    for e in edges:
        if e.port == "spillway":
            if e.to_id not in module_ids:
                raise WireError(
                    f"Spillway edge {e.from_id!r} → {e.to_id!r}: target module does not exist."
                )


def compile_away_regulators(
    modules: list[Module], edges: list[Edge]
) -> tuple[list[Module], list[Edge]]:
    """Remove passive Regulators and bypass their edges.

    A Regulator is passive when no edge with port='signal' points to it.
    Passive Regulators are compiled away entirely (zero runtime overhead).

    For active Regulators (signal edge exists), they remain in the plan.
    The Surveyor evaluates active Regulators at runtime.

    Bypass logic:
      For each passive Regulator R with upstream U and downstream D:
        - Remove all edges touching R.
        - Add a direct edge U → D for each (upstream, downstream) pair.
    """
    # Regulators that have at least one signal-port edge pointing to them
    active_regulator_ids = {e.to_id for e in edges if e.port == "signal"}

    passive_regulator_ids = {
        m.id
        for m in modules
        if m.type == "Regulator" and m.id not in active_regulator_ids
    }

    if not passive_regulator_ids:
        return modules, edges

    # Build upstream/downstream maps for passive regulators
    # (only main-port edges matter for bypass)
    upstream_of: dict[str, list[str]] = {}
    downstream_of: dict[str, list[str]] = {}
    for e in edges:
        if e.to_id in passive_regulator_ids and e.port == "main":
            upstream_of.setdefault(e.to_id, []).append(e.from_id)
        if e.from_id in passive_regulator_ids and e.port == "main":
            downstream_of.setdefault(e.from_id, []).append(e.to_id)

    # Build bypass edges
    bypass_edges: list[Edge] = []
    for reg_id in passive_regulator_ids:
        for upstream in upstream_of.get(reg_id, []):
            for downstream in downstream_of.get(reg_id, []):
                bypass_edges.append(
                    Edge(from_id=upstream, to_id=downstream, port="main")
                )

    # Remove passive regulators and their edges
    filtered_modules = [m for m in modules if m.id not in passive_regulator_ids]
    filtered_edges = [
        e for e in edges
        if e.from_id not in passive_regulator_ids
        and e.to_id not in passive_regulator_ids
    ] + bypass_edges

    return filtered_modules, filtered_edges
