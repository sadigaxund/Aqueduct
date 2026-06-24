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
    ObservabilityStore,
    RelationalCursor,
    _RelationalDepotMixin,
)

logger = logging.getLogger(__name__)


def _connect_with_retry(path: Path):
    """`duckdb.connect` that waits out a conflicting file lock instead of failing.

    DuckDB is single-writer per file: when parallel blueprints share a depot
    file (default depot) or a forced-shared obs file, a concurrent write holds an
    exclusive lock and a second `connect()` raises. Retry with capped backoff
    (~40s total) so writers serialise ("wait your turn") rather than crash; if it
    never frees, fail with a clear pointer to postgres/redis. Uncontended
    per-blueprint files succeed on the first try (zero added cost).
    """
    import random
    import time
    delay, last = 0.05, None
    for attempt in range(50):
        try:
            return duckdb.connect(str(path))
        except Exception as exc:  # noqa: BLE001 — only retry lock conflicts
            if "lock" not in str(exc).lower():
                raise
            last = exc
            time.sleep(min(delay, 1.0) + random.uniform(0, 0.05))
            delay *= 1.5
    raise RuntimeError(
        f"DuckDB store {path} stayed locked by another process after retrying. "
        "Concurrent writers to one DuckDB file serialise — for parallel runs use a "
        f"postgres/redis depot or per-blueprint stores. (last error: {last})"
    )


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
        conn = _connect_with_retry(self._path)
        try:
            cur = conn.cursor()
            yield RelationalCursor(cur, paramstyle="qmark")
        finally:
            with contextlib.suppress(Exception):
                conn.close()


class DuckDBObservabilityStore(_DuckDBRelational, ObservabilityStore):
    """Single-file DuckDB observability.db (includes column lineage since Phase 38)."""


class DuckDBDepotStore(_DuckDBRelational, _RelationalDepotMixin, DepotStore):
    """Depot KV backed by DuckDB. Same single-writer constraint as observability/lineage."""

    _DDL = """
        CREATE TABLE IF NOT EXISTS depot_kv (
            key        VARCHAR PRIMARY KEY,
            value      VARCHAR NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        );
    """


