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
  - Channel `op: deduplicate`'s `order_by` (a single string on both engines
    — `aqueduct/executor/spark/channel.py::_execute_deduplicate` feeds it
    straight to `F.expr(order_by)`, `duckdb_/channel.py`'s equivalent embeds
    it verbatim in a raw `ORDER BY {order_by}` clause — a real SQL
    expression on both, so a UDF call there is legal on either engine)
  - Channel `op: sort`'s `order_by`/`columns` (either spelling; a string OR
    a list of strings — `_execute_sort` on both engines reads
    `cfg.get("order_by") or cfg.get("columns")` then normalises
    `[order_by] if isinstance(order_by, str) else list(order_by)`, and each
    element is a real ORDER BY expression on both engines — DuckDB's
    `.order()` passes it straight to SQL; Spark's own `_to_col` only
    special-cases a trailing `ASC`/`DESC` token via a plain `F.col()` on the
    *first* whitespace-split token, so a UDF call embedded there fails at
    Spark RUNTIME rather than being invoked — but the same Blueprint field
    genuinely can invoke a UDF on a DuckDB-resolved island, so it must still
    be scanned)
  - Any Channel's `spillway_condition` (wrapped as a `WHERE` fragment)
  - Junction `mode: conditional` branch `condition` (wrapped; the `_else_`
    sentinel is not SQL and is skipped, not treated as unparseable)
  - Assert `sql`/`sql_row` rule `expr` (wrapped as a `WHERE` fragment)

**NOT scanned, and not tracked as "uncertain" either** — a UDF reachable
ONLY through one of these degrades to the "empty attribution -> fall back
to every island" case above, same as pre-attribution behavior:
  - Channel `op: select`'s `columns`/`cols` — `df.select(*cols)` /
    `rel.project(*columns)` treat each string as a column NAME, not an
    expression, so a UDF call written there fails at runtime on EITHER
    engine regardless of any capability gate. Not a real call site.
  - Probe `type: custom`'s driver-side callable and Assert `type: custom`'s
    `fn:` — a Python dotted path, not SQL text; irrelevant to a SQL-UDF
    attribution pass by construction.
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
            elif op == "deduplicate":
                # Both engines' `_execute_deduplicate` read `order_by` as a
                # single string (never a list) and use it as a real SQL
                # expression — see module docstring.
                order_by = cfg.get("order_by")
                if isinstance(order_by, str) and order_by:
                    _record(island, referenced_function_names_in_expr(order_by))
            elif op == "sort":
                # Both engines' `_execute_sort` accept EITHER key spelling
                # and EITHER a bare string or a list of strings — mirror
                # that exact normalisation (see module docstring).
                order_by = cfg.get("order_by") or cfg.get("columns")
                if order_by:
                    exprs = [order_by] if isinstance(order_by, str) else list(order_by)
                    for one_expr in exprs:
                        if isinstance(one_expr, str) and one_expr:
                            _record(island, referenced_function_names_in_expr(one_expr))
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
