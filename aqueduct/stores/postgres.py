"""Postgres-backed store implementations.

Single database, three schemas:

    aqueduct_db/
      observability/      run_records, failure_contexts, healing_outcomes,
                signal_overrides, probe_signals, module_metrics,
                maintenance_metrics
      lineage/  column_lineage
      depot/    depot_kv

Schemas are created idempotently on first connect (`CREATE SCHEMA IF NOT
EXISTS`). `search_path` is set per connection so call-site SQL stays
unqualified (`SELECT … FROM run_records` works the same as on DuckDB).

DSN handling
------------
`StoreBackendConfig.path` carries a libpq DSN
(`postgresql://user:pass@host:5432/aqueduct_db`). Identical DSNs across
`observability` / `lineage` / `depot` are connection-pool-deduplicated so a typical
3-line config opens one logical pool, not three.

The `psycopg2-binary>=2.9` extra (`pip install aqueduct-core[postgres]`)
provides the driver. Missing-import is caught at config-load time
(`config.py:_validate_store_backends`) so this module is only imported
when the driver is present.
"""

from __future__ import annotations

import contextlib
import logging
import threading
from datetime import datetime, timezone
from typing import Any, Iterator

from aqueduct.stores.base import DepotStore, ObservabilityStore, RelationalCursor, _RelationalDepotMixin

logger = logging.getLogger(__name__)


# ── Connection-pool deduplication ─────────────────────────────────────────────

_POOLS: dict[str, Any] = {}
_POOLS_LOCK = threading.Lock()


def _get_pool(dsn: str) -> Any:
    """Return a thread-safe psycopg2 SimpleConnectionPool keyed by DSN.

    Identical DSNs in the config (e.g. observability + lineage + depot all targeting
    the same `aqueduct_db`) share one pool. Pool size is intentionally
    modest — Aqueduct is driver-side; concurrent writers come from
    `--parallel` threads and parallel `aqueduct run` invocations.
    """
    with _POOLS_LOCK:
        existing = _POOLS.get(dsn)
        if existing is not None:
            return existing
        try:
            from psycopg2.pool import ThreadedConnectionPool  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ImportError(
                "Postgres stores backend requires psycopg2 — "
                "install with `pip install aqueduct-core[postgres]`"
            ) from exc
        pool = ThreadedConnectionPool(minconn=1, maxconn=8, dsn=dsn)
        _POOLS[dsn] = pool
        return pool


@contextlib.contextmanager
def _checkout(dsn: str) -> Iterator[Any]:
    """Borrow a connection from the dedup'd pool, return it on context exit."""
    pool = _get_pool(dsn)
    conn = pool.getconn()
    try:
        yield conn
    finally:
        with contextlib.suppress(Exception):
            pool.putconn(conn)


_BOOTSTRAPPED: set[tuple[str, str]] = set()  # (dsn, schema)


def _ensure_schema(dsn: str, schema: str) -> None:
    """Create the named schema on the target DB if it does not already exist.

    Idempotent and per-process cached so repeated `connect()` calls do not
    re-run the DDL every time.
    """
    key = (dsn, schema)
    if key in _BOOTSTRAPPED:
        return
    with _checkout(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            conn.commit()
    _BOOTSTRAPPED.add(key)


# ── Connection context manager ────────────────────────────────────────────────

@contextlib.contextmanager
def _pg_relational(dsn: str, schema: str) -> Iterator[RelationalCursor]:
    """Yield a `RelationalCursor` bound to *schema* via `search_path`.

    SQL written for DuckDB (`?` placeholders, `JSON`, `TIMESTAMPTZ`) works
    against Postgres unchanged after the `?` → `%s` translation in
    `RelationalCursor`.
    """
    _ensure_schema(dsn, schema)
    with _checkout(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f'SET search_path TO "{schema}"')
            yield RelationalCursor(cur, paramstyle="format")
        conn.commit()


# ── Concrete store classes ────────────────────────────────────────────────────

class _PostgresRelational:
    """Mixin parameterising the schema name a store writes into."""

    _SCHEMA: str = ""  # subclasses override

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    @property
    def backend(self) -> str:
        return "postgres"

    @property
    def location_label(self) -> str:
        # Redact password components for logs / doctor output
        from urllib.parse import urlsplit, urlunsplit
        try:
            parts = urlsplit(self._dsn)
            netloc = parts.hostname or ""
            if parts.port:
                netloc += f":{parts.port}"
            if parts.username:
                netloc = f"{parts.username}@{netloc}"
            return urlunsplit((parts.scheme, netloc, parts.path, "", ""))
        except Exception:
            return self._dsn

    @contextlib.contextmanager
    def connect(self) -> Iterator[RelationalCursor]:
        with _pg_relational(self._dsn, self._SCHEMA) as cur:
            yield cur


class PostgresObservabilityStore(_PostgresRelational, ObservabilityStore):
    _SCHEMA = "observability"


class PostgresDepotStore(_PostgresRelational, _RelationalDepotMixin, DepotStore):
    _SCHEMA = "depot"


    _DDL = """
        CREATE TABLE IF NOT EXISTS depot_kv (
            key        VARCHAR PRIMARY KEY,
            value      VARCHAR NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        )
    """

    def kv_get(self, key: str, default: str = "") -> str:
        try:
            with self.connect() as cur:
                cur.execute(self._DDL)
                row = cur.execute(
                    "SELECT value FROM depot_kv WHERE key = ?", [key]
                ).fetchone()
                return row[0] if row else default
        except Exception as exc:
            logger.warning("PostgresDepotStore.kv_get(%r): %s — returning default", key, exc)
            return default

    def kv_put(self, key: str, value: str) -> None:
        with self.connect() as cur:
            cur.execute(self._DDL)
            cur.execute(
                """
                INSERT INTO depot_kv (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT (key) DO UPDATE
                    SET value = excluded.value,
                        updated_at = excluded.updated_at
                """,
                [key, value, datetime.now(tz=timezone.utc).isoformat()],
            )

    def kv_delete(self, key: str) -> None:
        with self.connect() as cur:
            cur.execute(self._DDL)
            cur.execute("DELETE FROM depot_kv WHERE key = ?", [key])
