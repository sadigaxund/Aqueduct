"""Probe/Spillway validation and Regulator compile-away.

Probe wiring:
  Probes use attach_to (a module-level field) rather than edges.
  The wirer validates that every Probe's attach_to references an existing module.
  No structural changes are made — Probes stay in the module list.

Spillway wiring:
  The module-level `Module.spillway: <target>` field (docs/specs.md) is
  authoring SUGAR, not a second runtime mechanism — `desugar_module_spillway`
  below expands it into a real `port="spillway"` Edge, the ONE mechanism
  every engine's executor actually reads (see its own docstring for why this
  must run AFTER Arcade expansion). Target existence is validated during
  parsing (graph.validate_spillway_targets, on the pre-expansion Blueprint);
  the wirer's own `validate_spillway_edges` below re-confirms it post-
  expansion/desugaring, the same as it does for every explicitly-authored
  spillway edge.

Regulator compile-away (P6 — Passive-by-default gates):
  A Regulator with no wired signal-port edge is passive and is compiled away.
  Its edges are bypassed: upstream modules connect directly to downstream modules.
"""

from __future__ import annotations

import dataclasses

from aqueduct.errors import AqueductError
from aqueduct.parser.models import Edge, Module, ModuleType


class WireError(AqueductError):
    """Raised when a wiring validation fails."""


def validate_probes(modules: list[Module]) -> None:
    """Verify every Probe module has a valid attach_to target.

    Skips a directly-disabled Probe (``enabled: false``) — specs.md §4
    documents "a disabled module still compiles but is skipped at run
    time"; a Probe an author disabled while still wiring it up must not
    block compilation on the very field they haven't finished writing yet.
    This only covers a Probe's OWN `enabled` flag, not the full cascade
    (a Probe attached to a module disabled elsewhere) — that closure isn't
    computed until the later cascade-disable step (6.7), which runs after
    this validation; disabling the Probe itself is the actionable escape
    hatch for the "still fails to compile" complaint this fixes.
    """
    module_ids = {m.id for m in modules}
    for m in modules:
        if m.type != ModuleType.Probe:
            continue
        if not m.enabled:
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
        _validate_custom_signals(m)


def _validate_custom_signals(probe: Module) -> None:
    """Fail fast on malformed ``type: custom`` signals (shape only, no import).

    Resolution (importing the module / loading the entry point) happens at
    execution time; here we only confirm exactly one source is configured so a
    typo surfaces at compile, not mid-run.
    """
    # Imported here (engine-agnostic, pyspark-free) to keep the compiler clean.
    from aqueduct.executor.probe_plugins import custom_signal_source

    for sig in probe.config.get("signals", []) or []:
        if not isinstance(sig, dict) or sig.get("type") != "custom":
            continue
        try:
            custom_signal_source(sig)
        except ValueError as exc:
            raise WireError(f"Probe {probe.id!r}: {exc}") from exc


def desugar_module_spillway(
    modules: list[Module], edges: list[Edge]
) -> tuple[list[Module], list[Edge]]:
    """Expand `Module.spillway: <target>` authoring sugar into a real
    `Edge(from_id=module.id, to_id=target, port="spillway")` — the ONLY
    runtime mechanism spillway routing uses on either engine.

    Before this function existed, `Module.spillway` was parsed, validated
    (`parser.graph.validate_spillway_targets` — target must exist), remapped
    through Arcade expansion (`compiler.expander`'s `id_map.get(m.spillway)`,
    the same treatment `attach_to` gets), and serialized into the compiled
    Manifest — but read by NO executor on either engine, ever. A correct
    value did nothing; only a WRONG value was ever rejected. This closes
    that gap by making the field desugar into the one mechanism (the edge)
    that already works and is already tested — deliberately NOT a second
    runtime path.

    Must run AFTER Arcade expansion (`expand_arcades`), not before: a
    `spillway:` field set on a module living inside an Arcade's own
    sub-Blueprint only makes sense once that module has been namespaced
    (`{arcade_id}__{child_id}`) and `m.spillway` has already been remapped
    to match (`expander.py::_expand_single`) — operating on the raw,
    pre-expansion module list would miss every Arcade-nested spillway sugar
    field entirely (those modules do not exist in the flat list yet) and
    would need a second, duplicated desugaring pass inside `expander.py`
    itself. Running here, once, on the final flat module list handles a
    top-level module and an Arcade-expanded one identically, with no
    Arcade-awareness of its own.

    Conflict handling — a module may ALSO carry an explicit `port="spillway"`
    edge (the pre-existing, already-working authoring form):
      - explicit edge to the SAME target as `spillway:` → idempotent no-op,
        no duplicate edge is added (the explicit edge already covers it).
      - explicit edge to a DIFFERENT target → `WireError` (never silently
        pick one — a module cannot spill to two different places).

    `Module.spillway` is CLEARED (`None`) on every module this touches,
    whether a new edge was added or an identical one already existed — after
    this step, the field carries no information the edge doesn't already
    encode, so nothing downstream (Manifest serialization, a future
    executor) can be tempted to read it as a second source of truth.
    """
    existing_spillway_targets: dict[str, set[str]] = {}
    for e in edges:
        if e.port == "spillway":
            existing_spillway_targets.setdefault(e.from_id, set()).add(e.to_id)

    new_modules: list[Module] = []
    new_edges: list[Edge] = list(edges)
    for m in modules:
        if not m.spillway:
            new_modules.append(m)
            continue
        targets = existing_spillway_targets.get(m.id, set())
        if targets and m.spillway not in targets:
            raise WireError(
                f"Module {m.id!r} sets spillway={m.spillway!r} but already has "
                f"an explicit spillway edge to a DIFFERENT target "
                f"({sorted(targets)!r}) — a module cannot spill to two places. "
                "Remove the `spillway:` field, point it at the same target as "
                "the edge, or delete the conflicting edge."
            )
        if m.spillway not in targets:
            new_edges.append(
                Edge(from_id=m.id, to_id=m.spillway, port="spillway", injected=True)
            )
        # else: an identical explicit edge already exists — idempotent no-op.
        new_modules.append(dataclasses.replace(m, spillway=None))
    return new_modules, new_edges


def validate_spillway_edges(modules: list[Module], edges: list[Edge]) -> None:
    """Verify that spillway-port edges point to existing modules."""
    module_ids = {m.id for m in modules}
    for e in edges:
        if e.port == "spillway":
            if e.to_id not in module_ids:
                raise WireError(
                    f"Spillway edge {e.from_id!r} → {e.to_id!r}: target module does not exist."
                )


def validate_probe_source_edges(modules: list[Module], edges: list[Edge]) -> None:
    """Reject a Probe used as a data-flow source (executor excludes Probes from
    the topo-sort node set, so a non-signal edge with from_id=Probe crashes it
    with a bare KeyError — catch the mistake here with a pointer instead)."""
    probe_ids = {m.id for m in modules if m.type == ModuleType.Probe}
    for e in edges:
        if e.from_id in probe_ids and e.port != "signal":
            raise WireError(
                f"Probe {e.from_id!r} cannot be a data source via edge to {e.to_id!r} "
                f"(port={e.port!r}) — Probes attach to a module via attach_to and only "
                "emit signals over port: signal. Remove this edge."
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
        if m.type == ModuleType.Regulator and m.id not in active_regulator_ids
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
