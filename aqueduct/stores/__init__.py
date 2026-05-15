"""Pluggable store backends for observability, lineage, and depot.

The `aqueduct/stores/` package abstracts the connection layer for the three
DuckDB files Aqueduct has historically owned. Phase 28 introduces:

- `duckdb` (default) — embedded; single-writer file. Suits local/dev/NFS.
- `postgres`        — networked; MVCC multi-writer. Solves concurrent
                       `aqueduct run` invocations against shared state.
- `redis`           — depot KV only; high-QPS watermark reads. Rejected
                       at config-load for observability/lineage (relational queries
                       required).

Call sites that historically did `duckdb.connect(path)` now go through
`get_stores(cfg)` → `StoreBundle.{observability,lineage,depot}.connect()`. SQL
strings stay portable (standard ANSI); the only translation done is
parameter style (`?` for DuckDB, `%s` for Postgres) handled by the
DuckDB-style cursor returned by every relational adapter.
"""

from __future__ import annotations

from aqueduct.stores.base import (
    BackendUnsupportedError,
    DepotStore,
    LineageStore,
    ObservabilityStore,
    RelationalCursor,
    StoreBundle,
    get_stores,
)

__all__ = [
    "BackendUnsupportedError",
    "DepotStore",
    "LineageStore",
    "ObservabilityStore",
    "RelationalCursor",
    "StoreBundle",
    "get_stores",
]
