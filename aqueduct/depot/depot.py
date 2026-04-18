"""Depot KV store — cross-run pipeline state backed by DuckDB.

Provides a simple key-value store for pipeline state (watermarks, counters,
shared config) that persists across pipeline runs.  On first run every key
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
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS depot_kv (
    key        VARCHAR PRIMARY KEY,
    value      VARCHAR NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
"""


class DepotStore:
    """Thread-safe-ish DuckDB-backed KV store for pipeline state.

    Designed for driver-side use only (workers cannot write here).
    """

    def __init__(self, db_path: Path) -> None:
        self._path = db_path

    def get(self, key: str, default: str = "") -> str:
        """Return stored value for *key*, or *default* if absent/DB missing."""
        import duckdb

        if not self._path.exists():
            return default
        try:
            conn = duckdb.connect(str(self._path), read_only=True)
            try:
                conn.execute(_DDL)
                row = conn.execute(
                    "SELECT value FROM depot_kv WHERE key = ?", [key]
                ).fetchone()
                return row[0] if row else default
            finally:
                conn.close()
        except Exception as exc:
            logger.warning("DepotStore.get(%r): %s — returning default", key, exc)
            return default

    def put(self, key: str, value: str) -> None:
        """Upsert *key* → *value* with current UTC timestamp."""
        import duckdb

        self._path.parent.mkdir(parents=True, exist_ok=True)
        conn = duckdb.connect(str(self._path))
        try:
            conn.execute(_DDL)
            conn.execute(
                """
                INSERT INTO depot_kv (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT (key) DO UPDATE
                    SET value = excluded.value,
                        updated_at = excluded.updated_at
                """,
                [key, value, datetime.now(tz=timezone.utc).isoformat()],
            )
        finally:
            conn.close()

    def close(self) -> None:
        """No-op — connections are opened/closed per-call."""
