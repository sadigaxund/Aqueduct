"""DuckDB-backed store implementations.

Wraps the file-based `duckdb.connect()` pattern Aqueduct has used since
day one. SQL strings stay DuckDB-flavoured (`JSON` not `JSONB`, `?` not
`%s`); the cursor wrapper passes them through untouched.

Single-writer constraint is unchanged — that is the whole reason Phase 28
introduced the abstraction layer. Use Postgres for concurrent writers.
"""

from __future__ import annotations

import contextlib
import logging
from pathlib import Path
from typing import Iterator

import duckdb

from aqueduct.stores.base import (
    DepotStore,
    LineageStore,
    ObservabilityStore,
    RelationalCursor,
    _RelationalDepotMixin,
)

logger = logging.getLogger(__name__)


class _DuckDBRelational:
    """Mixin providing the duckdb-flavoured `connect()` context manager."""

    def __init__(self, path: Path) -> None:
        self._path = Path(path)

    @property
    def backend(self) -> str:
        return "duckdb"

    @property
    def location_label(self) -> str:
        return str(self._path)

    @contextlib.contextmanager
    def connect(self) -> Iterator[RelationalCursor]:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(self._path))
        try:
            cur = conn.cursor()
            yield RelationalCursor(cur, paramstyle="qmark")
        finally:
            with contextlib.suppress(Exception):
                conn.close()


class DuckDBObservabilityStore(_DuckDBRelational, ObservabilityStore):
    """Single-file DuckDB observability.db."""


class DuckDBLineageStore(_DuckDBRelational, LineageStore):
    """Single-file DuckDB lineage.db."""


class DuckDBDepotStore(_DuckDBRelational, _RelationalDepotMixin, DepotStore):
    """Depot KV backed by DuckDB. Same single-writer constraint as observability/lineage."""

    _DDL = """
        CREATE TABLE IF NOT EXISTS depot_kv (
            key        VARCHAR PRIMARY KEY,
            value      VARCHAR NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        );
    """


