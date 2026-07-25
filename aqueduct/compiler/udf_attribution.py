"""UDF → island attribution (Phase 81 Step 1 follow-up).

The per-island capability gate (`aqueduct/compiler/compiler.py` step 9)
checks each island against its OWN engine. Manifest-scoped UDF leaves
(`feature.python_udf`/`feature.java_udf`, and the `type.*` leaves a UDF's
`return_type` uses) are not owned by one module — a UDF is registered once
in `udf_registry` and referenced from SQL text across any number of
modules — so naively checking them against every island would reject the
phase's flagship shape: a Python UDF used only on a Spark island fails
compilation the moment ANY DuckDB island exists anywhere in the same
Blueprint, even though DuckDB never touches it.

This module attributes each UDF to the island(s) whose SQL can actually
reference it, using `aqueduct/compiler/lineage.py`'s sqlglot-backed helpers
(the same parser column lineage already uses — never a second SQL parser).
`compiler.py` filters each island's `udf_registry` copy through the result
before calling `check_capabilities`, so a UDF used only in island A never
gates island B.

**Fail-closed design.** A UDF is attributed ONLY to islands where we found
a positive, confidently-parsed reference, PLUS any island holding a
SQL-bearing construct we could not parse (dynamic content sqlglot rejects)
— that island might be hiding a reference to any UDF, so it is added to
every UDF's attribution unconditionally. If, after that, a UDF's
attribution is still empty (nothing found, and no unparsed construct
anywhere), it is either truly unused or referenced through a construct this
module does not scan (see below) — in that case attribution falls back to
EVERY island, the same conservative behavior as before this module existed.

**Scanned surfaces** (sqlglot-parsed, contribute positive hits or mark the
containing island "uncertain" on a parse failure):
  - Channel `op: sql`'s `query` (full SELECT statement)
  - Channel `op: join`'s `condition`, `op: filter`'s `condition`/`expr`
    (wrapped as a `WHERE` fragment)
  - Any Channel's `spillway_condition` (wrapped as a `WHERE` fragment)
  - Junction `mode: conditional` branch `condition` (wrapped; the `_else_`
    sentinel is not SQL and is skipped, not treated as unparseable)
  - Assert `sql`/`sql_row` rule `expr` (wrapped as a `WHERE` fragment)

**NOT scanned, and not tracked as "uncertain" either** — a UDF reachable
ONLY through one of these degrades to the "empty attribution -> fall back
to every island" case above, same as pre-attribution behavior: Channel
`deduplicate`'s `order_by`, `sort`'s order expressions, `select`'s column
list, Probe `type: custom` driver code, and Assert `type: custom`'s `fn:`
(a Python dotted path, not SQL — irrelevant to this attribution anyway).
These are column/expression LISTS or non-SQL references, not free-form
boolean SQL text, and are rare sites for a UDF call; narrowing them is
future work if it turns out to matter in practice.
"""

from __future__ import annotations

from typing import Any

from aqueduct.compiler.islands import Island
from aqueduct.compiler.lineage import (
    referenced_function_names,
    referenced_function_names_in_expr,
)
from aqueduct.parser.models import Module, ModuleType


def attribute_udfs_to_islands(
    modules: list[Module],
    udf_registry: tuple[dict[str, Any], ...],
    islands: list[Island],
) -> dict[str, set[Island]]:
    """Return ``{udf_id: {islands that may reference it}}`` for every UDF.

    Every UDF id present in *udf_registry* gets an entry. An empty set is
    never returned for a registered UDF with a non-empty id — see the
    module docstring's fail-closed fallback (empty findings AND no
    unparseable construct anywhere degrades to "every island").
    """
    udf_ids = {u.get("id") for u in udf_registry if isinstance(u, dict) and u.get("id")}
    if not udf_ids:
        return {}

    module_island: dict[str, Island] = {}
    for isl in islands:
        for mid in isl.module_ids:
            module_island[mid] = isl

    found: dict[str, set[Island]] = {uid: set() for uid in udf_ids}
    uncertain_islands: set[Island] = set()

    def _record(island: Island, names: set[str] | None) -> None:
        if names is None:
            uncertain_islands.add(island)
            return
        for uid in udf_ids:
            if uid.lower() in names:
                found[uid].add(island)

    for m in modules:
        island = module_island.get(m.id)
        if island is None:
            continue
        cfg = m.config if isinstance(m.config, dict) else {}

        if m.type == ModuleType.Channel:
            op = cfg.get("op")
            if op == "sql":
                query = cfg.get("query") or ""
                if query:
                    _record(island, referenced_function_names(query))
            elif op in ("join", "filter"):
                expr = cfg.get("condition") or cfg.get("expr") or ""
                if expr:
                    _record(island, referenced_function_names_in_expr(expr))
            spillway_condition = cfg.get("spillway_condition") or ""
            if spillway_condition:
                _record(island, referenced_function_names_in_expr(spillway_condition))

        elif m.type == ModuleType.Junction and cfg.get("mode") == "conditional":
            for branch in cfg.get("branches", []) or []:
                if not isinstance(branch, dict):
                    continue
                condition = branch.get("condition") or ""
                if condition and condition != "_else_":
                    _record(island, referenced_function_names_in_expr(condition))

        elif m.type == ModuleType.Assert:
            for rule in cfg.get("rules", []) or []:
                if not isinstance(rule, dict):
                    continue
                if rule.get("type") in ("sql", "sql_row"):
                    expr = rule.get("expr") or ""
                    if expr:
                        _record(island, referenced_function_names_in_expr(expr))

    return {
        uid: (found[uid] | uncertain_islands) if (found[uid] or uncertain_islands) else set(islands)
        for uid in udf_ids
    }


__all__ = ["attribute_udfs_to_islands"]
