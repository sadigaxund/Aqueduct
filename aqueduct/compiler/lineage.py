"""Column-level lineage extraction and persistence.

Runs at compile time on the driver (zero Spark actions).  For each Channel
module, sqlglot parses the SQL and walks the expression tree to map each
output column back to the source table(s) (upstream module IDs) that
contribute to it.

DuckDB schema (store_dir/lineage.db):
  column_lineage — one row per (blueprint_id, channel_id, output_column, source_table, source_column)

Limitations:
  - Only covers SQL Channels (op: sql).  Junctions, Funnels are structural.
  - sqlglot parses SparkSQL dialect but may not resolve all complex expressions
    (window functions, lateral views).  Those rows get source_column="*".
  - Wildcard SELECT (*) expands to all known input columns if schema is provided.
    Without schema info at compile time, stored as source_column="*".
"""

from __future__ import annotations

import logging
from aqueduct.parser.models import ModuleType
from typing import Any

logger = logging.getLogger(__name__)


def _extract_sql_lineage(
    channel_id: str,
    sql: str,
    upstream_ids: list[str],
) -> list[dict[str, str]]:
    """Parse SQL and extract output_column → source_table.source_column mappings.

    Returns a list of dicts with keys:
      channel_id, output_column, source_table, source_column
    """
    try:
        import sqlglot
        import sqlglot.expressions as exp
    except ImportError:
        logger.debug("sqlglot not available; skipping lineage extraction")
        return []

    rows: list[dict[str, str]] = []

    try:
        stmt = sqlglot.parse_one(sql, dialect="spark")
    except Exception as exc:
        logger.debug("Channel %r: sqlglot parse failed: %s", channel_id, exc)
        return []

    if not isinstance(stmt, exp.Select):
        # Subquery or CTE — treat as opaque
        return _opaque_row(channel_id, upstream_ids)

    for sel in stmt.expressions:
        alias = sel.alias if isinstance(sel, exp.Alias) else None
        inner = sel.this if isinstance(sel, exp.Alias) else sel

        # Determine output column name
        if alias:
            out_col = alias
        elif isinstance(inner, exp.Column):
            out_col = inner.name
        elif isinstance(inner, exp.Star):
            # SELECT * → wildcard entry per upstream table
            for src in upstream_ids:
                rows.append({
                    "channel_id": channel_id,
                    "output_column": "*",
                    "source_table": src,
                    "source_column": "*",
                })
            continue
        else:
            out_col = str(inner)[:64]

        # Columns inside a window's OVER spec (PARTITION BY / ORDER BY) only
        # control framing, not the computed value — exclude them. So
        # `row_number() OVER (ORDER BY id)` derives from nothing (→ "*"), and
        # `lag(price) OVER (ORDER BY dt)` derives from `price` only, not `dt`.
        has_window = next(inner.find_all(exp.Window), None) is not None
        spec_col_ids: set[int] = set()
        for win in inner.find_all(exp.Window):
            for part in (win.args.get("partition_by") or []):
                spec_col_ids.update(id(c) for c in part.find_all(exp.Column))
            order = win.args.get("order")
            if order is not None:
                spec_col_ids.update(id(c) for c in order.find_all(exp.Column))

        # Walk expression to find all value Column references (minus window spec)
        col_refs = [c for c in inner.find_all(exp.Column) if id(c) not in spec_col_ids]
        if not col_refs:
            # Window function with no value columns, a literal, or a set function:
            # no single source column. Window/expression → "*"; literal → its text.
            rows.append({
                "channel_id": channel_id,
                "output_column": out_col,
                "source_table": "",
                "source_column": "*" if has_window else str(inner)[:64],
            })
            continue

        for col_ref in col_refs:
            src_table = col_ref.table or (upstream_ids[0] if len(upstream_ids) == 1 else "")
            rows.append({
                "channel_id": channel_id,
                "output_column": out_col,
                "source_table": src_table,
                "source_column": col_ref.name,
            })

    return rows


def _opaque_row(channel_id: str, upstream_ids: list[str]) -> list[dict[str, str]]:
    return [
        {
            "channel_id": channel_id,
            "output_column": "*",
            "source_table": src,
            "source_column": "*",
        }
        for src in upstream_ids
    ]


def compute_lineage_rows(
    modules: tuple[Any, ...],
    edges: tuple[Any, ...],
) -> list[dict[str, str]]:
    """Extract column-level lineage rows from the compiled module/edge lists.

    Pure (no I/O): the single source of truth for both ``write_lineage`` (which
    persists these rows to ``column_lineage``) and the OpenLineage emitter
    (Phase 55, which serializes them into a ``columnLineage`` facet). Each row is
    ``{channel_id, output_column, source_table, source_column}``; unresolved
    columns fall back to ``UNKNOWN`` inside ``_extract_sql_lineage``.
    """
    channel_modules = [m for m in modules if m.type == ModuleType.Channel and m.config.get("op") == "sql"]
    if not channel_modules:
        return []

    upstream_map: dict[str, list[str]] = {}
    for m in channel_modules:
        upstream_map[m.id] = [
            e.from_id for e in edges
            if e.to_id == m.id and e.port == "main"
        ]

    all_rows: list[dict[str, str]] = []
    for m in channel_modules:
        query = m.config.get("query", "")
        if not query:
            continue
        all_rows.extend(_extract_sql_lineage(m.id, query, upstream_map.get(m.id, [])))
    return all_rows


def write_lineage(
    blueprint_id: str,
    run_id: str,
    modules: tuple[Any, ...],
    edges: tuple[Any, ...],
    observability_store: Any = None,
) -> None:
    """Extract and persist column-level lineage for all Channel modules.

    Writes to the observability store's ``column_lineage`` table (merged from
    the former ``lineage.db`` in Phase 38).  No exception propagates — lineage
    failure must never block compilation or execution.

    Args:
        blueprint_id:        Blueprint ID (from Manifest).
        run_id:              Run UUID.
        modules:             Compiled module list (flat, post-expansion).
        edges:               Compiled edge list.
        observability_store: ObservabilityStore backend. When None, lineage
                             extraction is skipped (no fallback).
    """
    try:
        from datetime import datetime, timezone

        all_rows = compute_lineage_rows(modules, edges)
        if not all_rows:
            return

        if observability_store is None:
            return  # no store backend configured, skip lineage

        now = datetime.now(tz=timezone.utc).isoformat()

        with observability_store.connect() as cur:
            cur.executemany(
                """
                INSERT INTO column_lineage
                    (blueprint_id, run_id, channel_id, output_column, source_table, source_column, captured_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        blueprint_id,
                        run_id,
                        r["channel_id"],
                        r["output_column"],
                        r["source_table"],
                        r["source_column"],
                        now,
                    )
                    for r in all_rows
                ],
            )

        logger.debug(
            "Lineage: wrote %d rows for blueprint %r run %r",
            len(all_rows), blueprint_id, run_id,
        )

    except Exception as exc:
        logger.debug("Lineage write failed (non-fatal): %s", exc)
