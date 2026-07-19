"""Channel ``spillway_condition`` ↔ spillway-edge wiring mismatch.

A Channel can split rows: those matching ``spillway_condition`` go to the
``spillway`` port, the rest to ``main``. Both halves must be present to be
useful:

- condition but no spillway edge → the split is computed then discarded; every
  row still goes to ``main`` and the spillway rows vanish silently.
- spillway edge but no condition → the spillway DataFrame is always empty; the
  downstream consumer of the spillway port receives nothing.

Both are statically detectable from the Manifest (module config + edges), so
they are caught at **compile time** with a suppressible ``rule_id`` rather than
emitted mid-run from the executor.
"""

from __future__ import annotations

from typing import Any

from aqueduct.parser.models import ModuleType

RULE_ID = "spillway_port_mismatch"


def check(manifest: Any, engine: str = "spark") -> list[str]:
    # Pure config/edge-wiring check — no engine-physical claim, no gate.
    del engine
    out: list[str] = []
    for m in manifest.modules:
        if m.type != ModuleType.Channel:
            continue
        cfg = m.config or {}
        has_condition = bool(cfg.get("spillway_condition"))
        has_edge = any(
            e.from_id == m.id and e.port == "spillway" for e in manifest.edges
        )
        if has_condition and not has_edge:
            out.append(
                f"Channel '{m.id}' has spillway_condition but no spillway edge; all "
                "rows are routed to the main stream and the spillway rows are "
                "discarded. Add an edge with `port: spillway`, or remove "
                "spillway_condition."
            )
        elif has_edge and not has_condition:
            out.append(
                f"Channel '{m.id}' has a spillway edge but no spillway_condition; the "
                "spillway DataFrame is always empty. Add spillway_condition, or remove "
                "the spillway edge."
            )
    return out
