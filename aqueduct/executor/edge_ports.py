"""Engine-neutral edge/port classification shared by both executors.

Both `executor/spark/executor.py` and `executor/duckdb_/executor.py` walk the
same Manifest edge list to answer the same two questions — "is this edge
carrying data?" and "which edges are this module's data input?" — so the
answers live here once instead of drifting apart in two files.

**Classify by what you EXCLUDE.** ``incoming_main`` deliberately does NOT
enumerate the ports that count as main input, because that set is open: a
Junction mints one port per branch id, named by the Blueprint author. An
include-list (the historical ``e.port == "main"``) silently dropped every
branch-port edge, so a Channel/Assert/Regulator/Arcade wired downstream of a
Junction branch failed with "has no main-port incoming edges" even though
`docs/specs.md`'s port table has always said a ``<branch_id>`` port is
consumed by "Any downstream module". The two ports that are genuinely NOT
main input are closed sets and are named here:

- ``signal`` — a control signal, not a DataFrame (Probe -> Regulator).
- ``spillway`` — the row-level error DataFrame. It IS a data edge (it carries
  rows, so `is_data_edge` returns True for it and the topological sort walks
  it), but it is a quarantine output consumed on its own port by an
  Egress/Funnel sink; it is never a module's main input.

Callers that want *any* data edge regardless of role (Handoff's WRITE-side
detection, `_topo_sort`, reachability walks) want ``incoming_data`` /
``is_data_edge`` instead.

Note that resolving the right EDGE is only half of consuming a branch port:
the frame it produced is stored under ``f"{from_id}.{port}"``, not
``from_id``, so every ``incoming_main`` call site must look the value up
through its engine's ``_effective_frame_key``/``_frame_key`` helper rather
than by ``edge.from_id`` alone.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aqueduct.models import Edge

# Ports carrying a control signal only, never a DataFrame.
SIGNAL_PORTS: frozenset[str] = frozenset({"signal"})

# The quarantine port. A data edge, but never a module's main input.
SPILLWAY_PORT: str = "spillway"

# Ports that are data-bearing but are not main input.
NON_MAIN_DATA_PORTS: frozenset[str] = frozenset({SPILLWAY_PORT})


def is_data_edge(edge: Edge) -> bool:
    """True when the edge carries rows (anything but a control signal)."""
    return edge.port not in SIGNAL_PORTS


def is_main_input_edge(edge: Edge) -> bool:
    """True when the edge is a module's main data input.

    Exclude-list: every data edge except a spillway edge. ``main`` and every
    Junction ``<branch_id>`` port qualify; ``signal`` and ``spillway`` do not.
    """
    return is_data_edge(edge) and edge.port not in NON_MAIN_DATA_PORTS


def incoming_data(module_id: str, edges: tuple[Edge, ...]) -> list[Edge]:
    """Every incoming data edge (any non-signal port, spillway included)."""
    return [e for e in edges if e.to_id == module_id and is_data_edge(e)]


def incoming_main(module_id: str, edges: tuple[Edge, ...]) -> list[Edge]:
    """Every incoming main-input edge — see ``is_main_input_edge``."""
    return [e for e in edges if e.to_id == module_id and is_main_input_edge(e)]
