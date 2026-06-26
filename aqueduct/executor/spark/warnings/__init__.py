"""Phase 30a tier 2 — Spark session-startup warnings.

Run once at SparkSession creation, after `getOrCreate()` returns. Probes the
live JVM (via py4j) to validate runtime preconditions that the static
compiler cannot check — namely: are the JARs the Blueprint depends on
actually loaded?

Cost: a single py4j round-trip per rule per session. Not per Ingress, not
per stage, not per row. Safe to enable always.

Add a rule by exporting `def check(manifest, spark) -> list[str]:` from a
new module in this package and appending to `RULES`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from . import jar_availability

CheckFn = Callable[[Any, Any], list[str]]

RULES: list[tuple[str, CheckFn]] = [
    (jar_availability.RULE_ID, jar_availability.check),
]


def run_all(manifest: Any, spark: Any, suppress: set[str] | None = None) -> list[tuple[str, str]]:
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
            for msg in rule(manifest, spark) or []:
                out.append((rule_id, msg))
        except Exception:
            continue
    return out
