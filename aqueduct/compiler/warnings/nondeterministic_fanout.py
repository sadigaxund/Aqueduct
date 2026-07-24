"""Multi-consumer Channel using `rand()` / `uuid()` / `current_timestamp()`.

When a Channel has 2+ downstream consumers AND its SQL contains a
non-deterministic function, each consumer branch re-evaluates the function
independently — producing divergent values per branch. Almost never the
user's intent. Recommend checkpointing THIS Channel — freezing the value
requires materializing the Channel's OWN output (the exemption below checks
the Channel's own `checkpoint` flag accordingly; an upstream checkpoint would
NOT stop per-branch re-evaluation). 8e is the pure-cache variant; this rule
is the data-correctness variant.
"""

from __future__ import annotations

import re
from typing import Any

from aqueduct.parser.models import ModuleType

RULE_ID = "nondeterministic_fanout"

# Case-insensitive — sqlglot would be overkill for substring detection here.
_NONDETERMINISTIC_FNS = re.compile(
    r"\b(rand|uuid|current_timestamp|current_date|now|random)\s*\(",
    re.IGNORECASE,
)


def check(manifest: Any, engine: str = "spark") -> list[str]:
    # Per-branch re-evaluation of a nondeterministic function is a property
    # of fan-out over ANY lazy query engine — no gate.
    del engine
    out: list[str] = []
    consumer_counts: dict[str, int] = {}
    for e in manifest.edges:
        consumer_counts[e.from_id] = consumer_counts.get(e.from_id, 0) + 1

    checkpointed = {m.id for m in manifest.modules if getattr(m, "checkpoint", False)}

    for m in manifest.modules:
        if m.type != ModuleType.Channel:
            continue
        if (m.config or {}).get("op") != "sql":
            continue
        if consumer_counts.get(m.id, 0) < 2:
            continue
        if m.id in checkpointed:
            continue
        sql = (m.config or {}).get("query", "")
        match = _NONDETERMINISTIC_FNS.search(sql)
        if not match:
            continue
        out.append(
            f"Channel '{m.id}' has {consumer_counts[m.id]} consumers and its "
            f"SQL uses {match.group(1).lower()}() — a non-deterministic "
            "function. Each consumer branch will re-evaluate the function "
            "independently, yielding different values per branch. "
            "Set `checkpoint: true` on this Channel so the value is "
            "materialized once and shared by every branch."
        )
    return out
