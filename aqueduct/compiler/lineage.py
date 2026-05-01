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
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS column_lineage (
    blueprint_id   VARCHAR NOT NULL,
    run_id         VARCHAR NOT NULL,
    channel_id     VARCHAR NOT NULL,
    output_column  VARCHAR NOT NULL,
    source_table   VARCHAR NOT NULL,
    source_column  VARCHAR NOT NULL,
    captured_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_lineage_channel
    ON column_lineage (blueprint_id, channel_id);
"""


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

        # Walk expression to find all Column references
        col_refs = list(inner.find_all(exp.Column))
        if not col_refs:
            # Literal or function with no column refs
            rows.append({
                "channel_id": channel_id,
                "output_column": out_col,
                "source_table": "",
                "source_column": str(inner)[:64],
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


def write_lineage(
    blueprint_id: str,
    run_id: str,
    modules: tuple[Any, ...],
    edges: tuple[Any, ...],
    store_dir: Path,
) -> None:
    """Extract and persist column-level lineage for all Channel modules.

    Args:
        blueprint_id: Blueprint ID (from Manifest).
        run_id:      Run UUID.
        modules:     Compiled module list (flat, post-expansion).
        edges:       Compiled edge list.
        store_dir:   Root observability store directory.

    No exception propagates — lineage failure must never block compilation.
    """
    try:
        import duckdb
        from datetime import datetime, timezone

        channel_modules = [m for m in modules if m.type == "Channel" and m.config.get("op") == "sql"]
        if not channel_modules:
            return

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
            rows = _extract_sql_lineage(m.id, query, upstream_map.get(m.id, []))
            all_rows.extend(rows)

        if not all_rows:
            return

        store_dir.mkdir(parents=True, exist_ok=True)
        db_path = store_dir / "lineage.db"
        now = datetime.now(tz=timezone.utc).isoformat()

        conn = duckdb.connect(str(db_path))
        try:
            conn.execute(_DDL)
            conn.executemany(
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
        finally:
            conn.close()

        logger.debug(
            "Lineage: wrote %d rows for blueprint %r run %r",
            len(all_rows), blueprint_id, run_id,
        )

    except Exception as exc:
        logger.debug("Lineage write failed (non-fatal): %s", exc)
