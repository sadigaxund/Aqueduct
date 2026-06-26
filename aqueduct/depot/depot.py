"""Depot KV store — cross-run blueprint state backed by DuckDB.

Provides a simple key-value store for blueprint state (watermarks, counters,
shared config) that persists across blueprint runs.  On first run every key
returns its default; subsequent runs see the last written value.

Usage in Blueprint YAML:
    # Read at compile time (in any config value):
    path: "s3://data/@aq.depot.get('last_date', '2020-01-01')/out"

    # Write at runtime (Egress module with format: depot):
    - id: save_watermark
      type: Egress
      config:
        format: depot
        key: last_date
        value: "@aq.date.today()"          # static — zero Spark cost
        # OR:
        value_expr: "MAX(order_date)"      # opt-in — one Spark agg action
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from aqueduct.stores import DepotStore as _DepotStoreBackend


class DepotStore:
    """Façade over the Phase 28 store-abstraction depot backend.

    Historically `DepotStore` opened a DuckDB connection per call. Phase 28
    moved that logic behind `aqueduct.stores.DepotStore` (the ABC) so the
    depot can be backed by DuckDB, Postgres, or Redis without touching
    call sites. This class keeps the legacy `.get()` / `.put()` /
    `.close()` API and delegates to whichever backend is configured.

    Construct either with a backend object (`DepotStore(backend=...)`,
    the path used by the CLI Phase 28 wiring) or with a legacy file path
    (`DepotStore(db_path=...)`, which preserves the pre-Phase-28 DuckDB
    behaviour for direct programmatic callers).
    """

    def __init__(
        self,
        db_path: Path | None = None,
        *,
        backend: _DepotStoreBackend | None = None,
    ) -> None:
        if backend is not None:
            self._backend = backend
        elif db_path is not None:
            from aqueduct.stores.duckdb_ import DuckDBDepotStore
            self._backend = DuckDBDepotStore(db_path)
        else:
            raise TypeError("DepotStore requires either db_path or backend")

    def get(self, key: str, default: str = "") -> str:
        """Return stored value for *key*, or *default* if absent."""
        return self._backend.kv_get(key, default)

    def put(self, key: str, value: str) -> None:
        """Upsert *key* → *value* with current UTC timestamp (when supported)."""
        self._backend.kv_put(key, value)

    def close(self) -> None:
        """No-op — connections are managed by the underlying store backend."""
