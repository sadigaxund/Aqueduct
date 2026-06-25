"""Ensure ``depot_kv`` DDL column-name sets match across backends.

The ``depot_kv`` table DDL is defined in two places (duckdb_, postgres).
This is a cheap guard: if someone adds/renames a column in one backend
without updating the other, this test fails with a clear diff.
Only column *names* are compared — types legitimately differ (DuckDB
``BLOB`` vs Postgres ``BYTEA``, etc.).
"""

from __future__ import annotations

import re

import pytest

pytestmark = pytest.mark.unit


def _parse_columns(ddl: str) -> set[str]:
    """Extract column-name set from a ``CREATE TABLE depot_kv (...)`` DDL string.

    Strips table-level constraints (PRIMARY KEY, UNIQUE, CHECK, etc.)
    and the closing ``)`` so only column lines contribute.
    """
    body_match = re.search(r"CREATE\s+TABLE.*depot_kv\s*\((.*?)\)", ddl, re.DOTALL | re.IGNORECASE)
    assert body_match is not None, f"Could not find CREATE TABLE depot_kv in:\n{ddl}"
    body = body_match.group(1)
    cols: set[str] = set()
    # split on commas, then take the first whitespace-delimited token as the column name
    for part in body.split(","):
        part = part.strip()
        if not part:
            continue
        # skip table-level constraints
        if re.match(r"^(PRIMARY\s+KEY|UNIQUE|CHECK|CONSTRAINT|FOREIGN\s+KEY)", part, re.IGNORECASE):
            continue
        # first token = column name (lowercase for comparison)
        col_name = part.split()[0].strip('"\'').lower()
        if col_name:
            cols.add(col_name)
    return cols


class TestDepotKvDdlSync:
    def test_column_sets_match(self):
        from aqueduct.stores.duckdb_ import DuckDBDepotStore
        from aqueduct.stores.postgres import PostgresDepotStore

        duck_cols = _parse_columns(DuckDBDepotStore._DDL)
        pg_cols = _parse_columns(PostgresDepotStore._DDL)

        only_duck = duck_cols - pg_cols
        only_pg = pg_cols - duck_cols
        diff = sorted(only_duck | only_pg)

        assert duck_cols == pg_cols, (
            f"depot_kv column sets diverge.\n"
            f"  DuckDB only: {sorted(only_duck) or '—'}\n"
            f"  Postgres only: {sorted(only_pg) or '—'}\n"
            f"  Mismatched columns: {diff}"
        )
