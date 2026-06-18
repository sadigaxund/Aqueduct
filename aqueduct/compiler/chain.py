"""Type-tracked column chains (Phase 56, Lineage v2 — Wave 2).

Traces a single output column backwards through the Channel graph, annotating
each hop with an sqlglot-inferred SQL ``output_type`` and a ``transform_op``
classification (``passthrough`` | ``rename`` | ``CAST`` | ``CONCAT`` | a
function name | ``literal`` | ``expression``).

Computed **on demand** from the already-compiled manifest — only when
``aqueduct lineage --chain <col> --types`` is invoked. Nothing is persisted and
no Spark action runs: this is a human debugging tool, not part of the healing
loop (the LLM already reads full SQL from ``manifest_json``).

Coverage: sqlglot models ~90% of SparkSQL expressions. Where it cannot resolve
a type the hop falls back to ``output_type=UNKNOWN``; where it cannot classify
the expression, ``transform_op=expression``. Non-SQL Channels (``op != sql``)
pass a column through unchanged → ``transform_op=passthrough``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from aqueduct.parser.models import ModuleType

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChainHop:
    """One step in a type-annotated column trace (output side of a module)."""

    channel_id: str
    output_column: str
    source_table: str
    source_column: str
    output_type: str
    transform_op: str


def _literal_type(node: Any) -> str:
    import sqlglot.expressions as exp

    if isinstance(node, exp.Literal):
        return "STRING" if node.is_string else "DECIMAL" if "." in node.name else "INT"
    if isinstance(node, exp.Boolean):
        return "BOOLEAN"
    if isinstance(node, exp.Null):
        return "NULL"
    return "UNKNOWN"


def _classify(inner: Any) -> tuple[str, str]:
    """Return ``(transform_op, output_type)`` for a select-expression node."""
    import sqlglot.expressions as exp

    if isinstance(inner, exp.Column):
        return "passthrough", "UNKNOWN"
    if isinstance(inner, exp.Cast):
        # CAST(x AS TYPE) — the only case where the type is explicit in the SQL.
        to = inner.args.get("to")
        return "CAST", (to.sql(dialect="spark").upper() if to is not None else "UNKNOWN")
    if isinstance(inner, (exp.Concat, exp.DPipe)):
        return "CONCAT", "STRING"
    if isinstance(inner, (exp.Literal, exp.Boolean, exp.Null)):
        return "literal", _literal_type(inner)
    if isinstance(inner, exp.Func):
        name = (inner.sql_name() or type(inner).__name__).upper()
        return name, "UNKNOWN"
    return "expression", "UNKNOWN"


def _channel_column_map(channel_id: str, sql: str, upstream_ids: list[str]) -> dict[str, ChainHop]:
    """Map each output column of one SQL Channel to a single (primary) hop."""
    try:
        import sqlglot
        import sqlglot.expressions as exp
    except Exception:  # pragma: no cover - sqlglot is a base dep
        return {}

    try:
        stmt = sqlglot.parse_one(sql, dialect="spark")
    except Exception as exc:
        logger.debug("Chain: parse failed for %r: %s", channel_id, exc)
        return {}

    if not isinstance(stmt, exp.Select):
        return {}

    out: dict[str, ChainHop] = {}
    default_src = upstream_ids[0] if len(upstream_ids) == 1 else ""
    for sel in stmt.expressions:
        alias = sel.alias if isinstance(sel, exp.Alias) else None
        inner = sel.this if isinstance(sel, exp.Alias) else sel

        if isinstance(inner, exp.Star):
            continue

        if isinstance(inner, exp.Column):
            out_col = alias or inner.name
            transform_op = "rename" if (alias and alias != inner.name) else "passthrough"
            output_type = "UNKNOWN"
            src_table = inner.table or default_src
            src_col = inner.name
        else:
            out_col = alias or str(inner)[:64]
            transform_op, output_type = _classify(inner)
            cols = list(inner.find_all(exp.Column))
            first = cols[0] if cols else None
            src_table = (first.table if first and first.table else default_src) if first else ""
            src_col = first.name if first else str(inner)[:64]

        out[out_col] = ChainHop(
            channel_id=channel_id,
            output_column=out_col,
            source_table=src_table,
            source_column=src_col,
            output_type=output_type,
            transform_op=transform_op,
        )
    return out


def compute_type_chain(
    modules: tuple[Any, ...],
    edges: tuple[Any, ...],
    column: str,
    channel_id: str | None = None,
) -> list[ChainHop]:
    """Trace ``column`` backwards from its producing Channel to the source.

    ``channel_id`` pins the starting Channel; when omitted, the last Channel
    that emits ``column`` is used. Returns hops ordered source → … → output
    (i.e. reversed from the backward walk). Empty list when the column cannot be
    located in any SQL Channel.
    """
    by_id = {m.id: m for m in modules}
    upstream: dict[str, list[str]] = {}
    for m in modules:
        upstream[m.id] = [e.from_id for e in edges if e.to_id == m.id and getattr(e, "port", "main") == "main"]

    # Per-Channel output→hop maps for every SQL Channel.
    col_maps: dict[str, dict[str, ChainHop]] = {}
    for m in modules:
        if m.type == ModuleType.Channel and m.config.get("op") == "sql":
            col_maps[m.id] = _channel_column_map(m.id, m.config.get("query", ""), upstream.get(m.id, []))

    # Resolve the starting Channel.
    start = channel_id
    if start is None:
        for m in modules:
            if m.id in col_maps and column in col_maps[m.id]:
                start = m.id  # keep the last producer in module order
    if start is None or column not in col_maps.get(start, {}):
        return []

    hops: list[ChainHop] = []
    cur_channel: str | None = start
    cur_col = column
    seen: set[tuple[str, str]] = set()
    while cur_channel is not None and (cur_channel, cur_col) not in seen:
        seen.add((cur_channel, cur_col))
        cmap = col_maps.get(cur_channel, {})
        hop = cmap.get(cur_col)
        if hop is None:
            break
        hops.append(hop)
        src_mod = by_id.get(hop.source_table)
        if src_mod is None:
            break
        if src_mod.id in col_maps:
            cur_channel, cur_col = src_mod.id, hop.source_column
        elif src_mod.type == ModuleType.Channel:
            # Non-SQL Channel — column passes through unchanged, then continue.
            hops.append(
                ChainHop(
                    channel_id=src_mod.id,
                    output_column=hop.source_column,
                    source_table=(upstream.get(src_mod.id) or [""])[0],
                    source_column=hop.source_column,
                    output_type="UNKNOWN",
                    transform_op="passthrough",
                )
            )
            ups = upstream.get(src_mod.id) or []
            cur_channel = ups[0] if ups and ups[0] in col_maps else None
            cur_col = hop.source_column
        else:
            # Reached an Ingress / structural source — terminate.
            break

    return list(reversed(hops))
