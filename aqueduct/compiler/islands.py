"""Engine islands — Phase 81 Step 1 (cross-engine handoff / polyglot blueprints).

An **island** is a connected subgraph of modules that share one resolved
execution engine. Islands are DERIVED, never declared — there is no
user-facing island syntax. A **boundary edge** is a data edge whose two
endpoints resolve to different islands; a later phase inserts a synthetic
handoff module there. This module only DERIVES and VALIDATES islands and
boundary edges — it does not synthesize anything.

Pure, engine-agnostic (no ``pyspark``, no engine imports by name — engines are
only ever referred to by their string name). Lives alongside the existing
topological/component machinery per the 4-layer boundary: island derivation
is Compiler work, not a new "Planner" layer.

Two related but distinct notions of "upstream" are used here:

- **Inheritance parents** (``_parents``) — who a module's ``engine:`` default
  comes from when the module itself doesn't pin one. Includes main/spillway
  data-edge sources, ``depends_on`` entries, and — for a Probe specifically —
  its ``attach_to`` target (a Probe has no incoming data edge; it taps a
  module's output via ``attach_to``, so that IS its one parent for
  inheritance purposes, which is what makes an unpinned Probe naturally land
  on its target's engine).
- **Island connectivity** (union-find in ``derive_islands``) — which modules
  end up grouped into the same island. Built from the SAME edge basis the
  executor's parallel-component detection uses (main/spillway data edges),
  restricted to edges whose two endpoints share a resolved engine (an edge
  connecting differing engines is a boundary, not a union), plus an
  unconditional Probe -> attach_to union (safe only because colocation is
  validated separately and must already hold). ``depends_on``-only links are
  deliberately NOT unioned — same precedent as
  ``aqueduct/executor/spark/executor.py::_find_connected_components``, which
  does not union on ``depends_on`` either. Two same-engine subgraphs with no
  edge between them are legitimately two separate islands; nothing requires
  islands to be maximal per engine name.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from aqueduct.errors import CompileError
from aqueduct.parser.models import Edge, Module, ModuleType

# Ports that carry an actual DataFrame between modules — the basis for both
# inheritance-parent lookup and island connectivity. Excludes "signal"
# (Probe -> Regulator control signal, not a data dependency; Regulators are
# themselves engine-agnostic gates, not islands-worth of behaviour).
#
# This must be an EXCLUDE-list, not an include-list: a Junction branch edge's
# `port` is the branch id (an arbitrary string — e.g. `port: active`, see
# `aqueduct/executor/spark/junction.py`'s config docstring), not `"main"`. An
# earlier version of this module used an include-list of `{"main",
# "spillway"}`, which silently dropped every Junction branch edge from both
# engine inheritance (`_parents`) and island connectivity (`derive_islands`) —
# a Junction and its branch targets landed in SEPARATE islands even on the
# SAME engine (verified against `gallery/aqtests/03_junction_branch`: 3
# islands instead of 1), which then misroutes an ordinary single-engine
# Junction blueprint through `run_polyglot()` (`len(manifest.islands) > 1`)
# and drops the branch edges entirely in `_sub_manifest`'s both-endpoints
# filter. Mirrors the real executor's own `_is_data_edge`
# (`aqueduct/executor/spark/executor.py`, `edge.port not in _SIGNAL_PORTS`),
# which already gets this right.
_SIGNAL_PORTS: frozenset[str] = frozenset({"signal"})


def _is_data_edge(edge: Edge) -> bool:
    return edge.port not in _SIGNAL_PORTS


def _parents(module_id: str, modules_by_id: dict[str, Module], edges: list[Edge]) -> list[str]:
    """Upstream modules this one inherits its default `engine:` from.

    Order is stable (edges first, then `depends_on`, then a Probe's
    `attach_to`) and de-duplicated — callers only care about the SET of
    distinct resolved engines among the parents (rule 2 vs rule 3), not the
    order, but stability keeps error messages/tests deterministic.
    """
    m = modules_by_id[module_id]
    seen: list[str] = []
    for e in edges:
        if (
            e.to_id == module_id
            and _is_data_edge(e)
            and e.from_id in modules_by_id
            and e.from_id not in seen
        ):
            seen.append(e.from_id)
    for dep in m.depends_on:
        if dep in modules_by_id and dep not in seen:
            seen.append(dep)
    if (
        m.type == ModuleType.Probe
        and m.attach_to
        and m.attach_to in modules_by_id
        and m.attach_to not in seen
    ):
        seen.append(m.attach_to)
    return seen


def _engine_resolution_order(modules: list[Module], edges: list[Edge]) -> list[str]:
    """Topological order over exactly the parent-relation `_parents` uses.

    Deliberately NOT `aqueduct.parser.graph.topological_order` — that
    function's adjacency doesn't know about a Probe's `attach_to` (Probes
    have no incoming edges), so it can emit a Probe before the module it
    taps, before that module's engine has been resolved. Building a
    dedicated order here guarantees every parent `_parents` will ask about
    is resolved before its child.
    """
    modules_by_id = {m.id: m for m in modules}
    ids = list(modules_by_id)
    parents_of = {mid: _parents(mid, modules_by_id, edges) for mid in ids}
    children_of: dict[str, list[str]] = {mid: [] for mid in ids}
    in_degree: dict[str, int] = {mid: 0 for mid in ids}
    for mid, parents in parents_of.items():
        for p in parents:
            children_of[p].append(mid)
            in_degree[mid] += 1

    queue: deque[str] = deque(mid for mid in ids if in_degree[mid] == 0)
    order: list[str] = []
    while queue:
        mid = queue.popleft()
        order.append(mid)
        for child in children_of[mid]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    if len(order) < len(ids):
        # A genuine cycle here would already have been caught by the
        # parser's own `detect_cycles` at parse time (edges + depends_on are
        # a superset of the parent-relation used here) — this is defensive
        # fallback so engine resolution never raises a confusing
        # KeyError/infinite-loop on a graph shape this module didn't
        # anticipate, not an expected path. Stable id order keeps the
        # (degraded) result deterministic.
        order.extend(sorted(mid for mid in ids if mid not in order))
    return order


def resolve_module_engines(
    modules: list[Module],
    edges: list[Edge],
    default_engine: str,
) -> dict[str, str]:
    """Resolve every module's execution engine. Exactly four rules, in order:

    1. An explicit `engine:` on the module wins.
    2. Unset -> inherit the SINGLE upstream parent's (already-resolved) engine.
    3. Unset + multiple parents with DIFFERING resolved engines ->
       CompileError demanding an explicit `engine:`.
    4. Unset + no parents (an Ingress, typically) -> `default_engine`
       (`deployment.engine` / `--set deployment.engine`).

    `default_engine` only ever supplies rule 4's fallback — an explicit
    per-module pin (rule 1) always wins over it, and an inherited engine
    (rule 2) never falls back to it once a parent has resolved. This is the
    "config expresses environment, the blueprint expresses semantics"
    precedence rule: the Blueprint's own pins are never overridden by the
    configured default.

    Modules are processed in `_engine_resolution_order` so a parent's
    resolved engine is always known before a child asks for it.
    """
    modules_by_id = {m.id: m for m in modules}
    order = _engine_resolution_order(modules, edges)
    resolved: dict[str, str] = {}

    for mid in order:
        m = modules_by_id[mid]
        if m.engine:
            resolved[mid] = m.engine
            continue
        parent_engines: list[str] = []
        for p in _parents(mid, modules_by_id, edges):
            eng = resolved.get(p)
            if eng is not None and eng not in parent_engines:
                parent_engines.append(eng)
        if len(parent_engines) == 1:
            resolved[mid] = parent_engines[0]
        elif len(parent_engines) > 1:
            raise CompileError(
                f"Module {mid!r} has upstream parents resolved to differing "
                f"engines ({sorted(parent_engines)}) and does not pin its own "
                "`engine:`. Add an explicit `engine:` to this module naming "
                "which engine it should run on."
            )
        else:
            resolved[mid] = default_engine
    return resolved


@dataclass(frozen=True)
class Island:
    """One connected, single-engine subgraph of the compiled module graph."""

    engine: str
    module_ids: frozenset[str]


@dataclass(frozen=True)
class BoundaryEdge:
    """One data edge whose endpoints resolve to different islands — the site
    a synthetic handoff module will be inserted at in a later phase. This
    step only derives and validates these; nothing is synthesized here.
    """

    edge: Edge
    from_engine: str
    to_engine: str


def derive_islands(
    modules: list[Module],
    edges: list[Edge],
    resolved_engine: dict[str, str],
) -> list[Island]:
    """Partition modules into islands via same-engine union-find.

    Two modules are unioned when a data edge connects them AND they resolve
    to the same engine (a differing-engine edge is a boundary, so it is
    deliberately NOT unioned — that's what makes an island single-engine by
    construction). A Probe is unconditionally unioned with its `attach_to`
    target — safe only because callers MUST run `validate_colocation` first
    (it guarantees the two already share a resolved engine); this mirrors
    `_find_connected_components`'s ISSUE-042 Probe binding one level up, at
    the engine-island granularity instead of the parallel-execution one.
    """
    ids = [m.id for m in modules]
    id_set = set(ids)
    parent: dict[str, str] = {mid: mid for mid in ids}

    def _find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def _union(x: str, y: str) -> None:
        px, py = _find(x), _find(y)
        if px != py:
            parent[px] = py

    for e in edges:
        if not _is_data_edge(e) or e.from_id not in id_set or e.to_id not in id_set:
            continue
        if resolved_engine[e.from_id] == resolved_engine[e.to_id]:
            _union(e.from_id, e.to_id)

    for m in modules:
        if m.type == ModuleType.Probe and m.attach_to and m.attach_to in id_set:
            _union(m.id, m.attach_to)

    groups: dict[str, set[str]] = {}
    for mid in ids:
        groups.setdefault(_find(mid), set()).add(mid)

    islands: list[Island] = []
    for group in groups.values():
        engines = {resolved_engine[mid] for mid in group}
        if len(engines) != 1:
            # Cannot happen given the union rules above (we only ever union
            # same-engine edges, plus a colocation-validated Probe bond) —
            # a defensive invariant check, not a real user-facing error
            # path. If this ever fires it means a future edit to the union
            # rules broke the single-engine-per-island invariant.
            raise CompileError(
                f"Internal error: island {sorted(group)!r} resolved to "
                f"multiple engines {sorted(engines)!r} — this should be "
                "unreachable; please report this as a bug."
            )
        islands.append(Island(engine=next(iter(engines)), module_ids=frozenset(group)))
    return islands


def find_boundary_edges(
    modules: list[Module],
    edges: list[Edge],
    resolved_engine: dict[str, str],
) -> list[BoundaryEdge]:
    """Every data edge whose endpoints resolve to different engines.

    Derivation only — Step 1 does not insert a handoff module here; a later
    phase consumes this list to do that.
    """
    id_set = {m.id for m in modules}
    out: list[BoundaryEdge] = []
    for e in edges:
        if not _is_data_edge(e) or e.from_id not in id_set or e.to_id not in id_set:
            continue
        from_engine = resolved_engine[e.from_id]
        to_engine = resolved_engine[e.to_id]
        if from_engine != to_engine:
            out.append(BoundaryEdge(edge=e, from_engine=from_engine, to_engine=to_engine))
    return out


def validate_colocation(
    modules: list[Module],
    edges: list[Edge],
    resolved_engine: dict[str, str],
) -> None:
    """Probe and Assert modules MUST colocate with their target's island.

    A Probe's target is its `attach_to` module; an Assert's target is its
    upstream data parent(s) (it is a single-input/single-output linear-chain
    type). Neither module type is allowed to introduce an engine boundary —
    ordinary modules can (that's the whole polyglot feature), but a
    Probe/Assert diverging from its target is always a CompileError, most
    commonly caused by an explicit `engine:` pin on the Probe/Assert itself
    that disagrees with its target.
    """
    modules_by_id = {m.id: m for m in modules}
    for m in modules:
        if m.type == ModuleType.Probe:
            target = m.attach_to
            if not target or target not in modules_by_id:
                continue  # unresolvable attach_to is caught elsewhere (wirer.validate_probes)
            if resolved_engine[m.id] != resolved_engine[target]:
                raise CompileError(
                    f"Probe {m.id!r} resolves to engine {resolved_engine[m.id]!r} "
                    f"but its attach_to target {target!r} resolves to engine "
                    f"{resolved_engine[target]!r}. A Probe must colocate with "
                    "its target's island — remove the mismatched `engine:` pin "
                    "or set it to match the target."
                )
        elif m.type == ModuleType.Assert:
            parents = _parents(m.id, modules_by_id, edges)
            mismatched = [p for p in parents if resolved_engine[p] != resolved_engine[m.id]]
            if mismatched:
                raise CompileError(
                    f"Assert {m.id!r} resolves to engine {resolved_engine[m.id]!r} "
                    f"but its upstream module(s) {sorted(mismatched)!r} resolve to "
                    "a different engine. An Assert must colocate with its "
                    "target's island — remove the mismatched `engine:` pin or "
                    "set it to match its upstream."
                )


def validate_spillway_islands(edges: list[Edge], resolved_engine: dict[str, str]) -> None:
    """A spillway edge crossing islands is a CompileError in v1.

    Cross-engine spillway routing (quarantining rows produced by one
    engine's Channel/Assert into a module running on a different engine) is
    not supported yet — the same "storage-spill-only, v1" transport
    boundary that gates ordinary handoffs hasn't been wired for the error
    port. Route the spillway to a module on the source's own engine.
    """
    for e in edges:
        if e.port != "spillway":
            continue
        from_engine = resolved_engine.get(e.from_id)
        to_engine = resolved_engine.get(e.to_id)
        if from_engine is not None and to_engine is not None and from_engine != to_engine:
            raise CompileError(
                f"Spillway edge {e.from_id!r} -> {e.to_id!r} crosses engines "
                f"({from_engine!r} -> {to_engine!r}) — cross-island spillway "
                "routing is not supported in v1. Route this spillway to a "
                f"module on engine {from_engine!r} (the source's own engine)."
            )


def validate_islands(
    modules: list[Module],
    edges: list[Edge],
    resolved_engine: dict[str, str],
) -> None:
    """Run every Step 1 island validation. Order matters for error clarity:
    colocation first (a mismatched Probe/Assert pin is the most specific,
    actionable diagnosis), then the spillway-crossing check. Whether an
    island's engine is actually REGISTERED is validated by the per-island
    capability gate call in `compiler.compile()` (it already calls
    `get_capabilities(engine)`, which fails closed for an unregistered
    engine) — not duplicated here.
    """
    validate_colocation(modules, edges, resolved_engine)
    validate_spillway_islands(edges, resolved_engine)


__all__ = [
    "BoundaryEdge",
    "Island",
    "derive_islands",
    "find_boundary_edges",
    "resolve_module_engines",
    "validate_colocation",
    "validate_islands",
    "validate_spillway_islands",
]
