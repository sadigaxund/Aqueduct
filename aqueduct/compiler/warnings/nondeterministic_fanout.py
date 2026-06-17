"""Multi-consumer Channel using `rand()` / `uuid()` / `current_timestamp()`.

When a Channel has 2+ downstream consumers AND its SQL contains a
non-deterministic function, each consumer branch re-evaluates the function
independently — producing divergent values per branch. Almost never the
user's intent. Recommend a Checkpoint upstream (already covered by 8e for
pure cache reasons; this rule is the data-correctness variant).
"""

from __future__ import annotations

import re
from aqueduct.parser.models import ModuleType
from typing import Any

RULE_ID = "nondeterministic_fanout"

# Case-insensitive — sqlglot would be overkill for substring detection here.
_NONDETERMINISTIC_FNS = re.compile(
    r"\b(rand|uuid|current_timestamp|current_date|now|random)\s*\(",
    re.IGNORECASE,
)


def check(manifest: Any) -> list[str]:
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
            "Add a Checkpoint upstream of this Channel so the value is "
            "computed once and shared."
        )
    return out
