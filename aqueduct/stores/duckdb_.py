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
from collections.abc import Iterator
from pathlib import Path

import duckdb

from aqueduct.stores.base import (
    DepotStore,
    ObservabilityStore,
    RelationalCursor,
    StoreConnectionError,
    _RelationalDepotMixin,
)
from aqueduct.stores.ddl import DEPOT_KV_DDL

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
                raise StoreConnectionError(
                    f"DuckDB store {path} could not be opened: {exc}"
                ) from exc
            last = exc
            time.sleep(min(delay, 1.0) + random.uniform(0, 0.05))
            delay *= 1.5
    raise StoreConnectionError(
        f"DuckDB store {path} stayed locked by another process after retrying. "
        "Concurrent writers to one DuckDB file serialise — for parallel runs use a "
        f"postgres/redis depot or per-blueprint stores. (last error: {last})"
    )


class _DuckDBRelational:
    """Mixin providing the duckdb-flavoured `connect()` context manager."""

    def __init__(self, path: Path, *, read_only: bool = False) -> None:
        self._path = Path(path)
        self._read_only = read_only

    @property
    def backend(self) -> str:
        return "duckdb"

    @property
    def location_label(self) -> str:
        return str(self._path)

    @contextlib.contextmanager
    def connect(self) -> Iterator[RelationalCursor]:
        if self._read_only:
            # Preview/read-only callers must never create the file or its
            # parent directory, and must never take the writer lock —
            # `duckdb.connect(..., read_only=True)` requires the file to
            # already exist. Callers that reach here on a missing file get
            # DuckDB's own error; `_RelationalDepotMixin.kv_get`/`kv_delete`
            # guard on `_path.exists()` before ever calling `connect()`, so
            # in practice this path is only hit for a file that exists.
            conn = duckdb.connect(str(self._path), read_only=True)
        else:
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

    _DDL = DEPOT_KV_DDL
