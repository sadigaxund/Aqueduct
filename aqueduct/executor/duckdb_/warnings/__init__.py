"""DuckDB session-startup warnings — mirrors ``executor/spark/warnings/``.

Run once at session creation, after the live DuckDB connection exists.
Probes the connection to validate runtime preconditions the static compiler
cannot check. Cost: a handful of cheap SQL queries per rule per session —
not per Ingress, not per module, not per row. Safe to enable always.

Add a rule by exporting `def check(manifest, con) -> list[str]:` from a new
module in this package and appending to `RULES`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from . import httpfs_availability

CheckFn = Callable[[Any, Any], list[str]]

RULES: list[tuple[str, CheckFn]] = [
    (httpfs_availability.RULE_ID, httpfs_availability.check),
]


def run_all(manifest: Any, con: Any, suppress: set[str] | None = None) -> list[tuple[str, str]]:
    """Run every session-startup rule, return `[(rule_id, message), ...]`.

    Suppressed rules are skipped entirely. Per-rule exceptions swallowed —
    diagnostics never abort startup.
    """
    suppress = suppress or set()
    out: list[tuple[str, str]] = []
    for rule_id, rule in RULES:
        if rule_id in suppress:
            continue
        try:
            for msg in rule(manifest, con) or []:
                out.append((rule_id, msg))
        except Exception:
            continue
    return out
