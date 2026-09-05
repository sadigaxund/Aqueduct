"""Redis-backed Depot KV store (depot only).

High-QPS watermark / counter / coordination workloads. Asking for
`relational_connect()` on this store raises `BackendUnsupportedError` —
the config-layer validators in `aqueduct.config` reject `redis` for
observability/lineage backends so this state is unreachable from a validated
config, but the guard is kept as a defense-in-depth measure for direct
programmatic callers.

The `redis>=5.0` extra (`pip install aqueduct-core[redis]`) provides the
driver. Missing-import is caught at config-load time via the same SDK
check pattern Phase 26b introduced for the secrets backends.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

from aqueduct.stores.base import DepotStore, StoreConnectionError

logger = logging.getLogger(__name__)


_CLIENTS: dict[str, Any] = {}
_CLIENTS_LOCK = threading.Lock()


def _get_client(url: str) -> Any:
    """Return a thread-safe `redis.Redis` client keyed by URL.

    Identical Redis URLs across multiple `RedisDepotStore` constructions
    in the same process share one client (and therefore one connection
    pool) just like the Postgres adapter dedupes by DSN.
    """
    with _CLIENTS_LOCK:
        existing = _CLIENTS.get(url)
        if existing is not None:
            return existing
        try:
            import redis  # type: ignore[import-not-found]
        except ImportError as exc:
            raise StoreConnectionError(
                "Redis depot backend requires the redis-py package — "
                "install with `pip install aqueduct-core[redis]`"
            ) from exc
        client = redis.Redis.from_url(url, decode_responses=True)
        _CLIENTS[url] = client
        return client


class RedisDepotStore(DepotStore):
    """Depot KV operations against a Redis server.

    Keys live in the database number selected by the URL (default 0).
    Values are stored as bare strings — there is no separate
    `updated_at` column because Redis does not expose per-key
    last-modified timestamps without per-write overhead. Use Postgres or
    DuckDB if you need to query when a depot value last changed.
    """

    # kv_put_and_clear is intentionally NOT overridden here — it keeps the
    # `DepotStore` ABC default (a plain `kv_put` then `kv_delete`, two
    # separate Redis commands). Redis has no cross-key transaction this
    # class uses (MULTI/EXEC would need to hold a connection across both
    # calls, which the shared-client dedup in `_get_client` does not
    # support), so a crash between the two commands is a real, un-closed
    # window: the watermark lands but the intent row survives. Redis is the
    # one depot backend where that window is not eliminated — only DuckDB
    # and Postgres (via `_RelationalDepotMixin`) close it.

    def __init__(self, url: str) -> None:
        self._url = url

    @property
    def backend(self) -> str:
        return "redis"

    @property
    def location_label(self) -> str:
        from urllib.parse import urlsplit, urlunsplit

        try:
            parts = urlsplit(self._url)
            netloc = parts.hostname or ""
            if parts.port:
                netloc += f":{parts.port}"
            return urlunsplit((parts.scheme, netloc, parts.path or "/", "", ""))
        except Exception:
            return self._url

    def kv_get(self, key: str, default: str = "") -> str:
        try:
            client = _get_client(self._url)
            val = client.get(key)
            return val if val is not None else default
        except Exception as exc:
            logger.warning("RedisDepotStore.kv_get(%r): %s — returning default", key, exc)
            return default

    def kv_put(self, key: str, value: str) -> None:
        # Unlike kv_get/kv_delete, a write failure must not be swallowed —
        # returning silently would tell the caller its watermark/counter
        # write succeeded when it did not. Surface it as one catchable type
        # instead of a driver-specific redis.exceptions.* leaking raw.
        try:
            client = _get_client(self._url)
            client.set(key, value)
        except StoreConnectionError:
            raise
        except Exception as exc:
            raise StoreConnectionError(f"RedisDepotStore.kv_put({key!r}) failed: {exc}") from exc

    def kv_delete(self, key: str) -> None:
        try:
            client = _get_client(self._url)
            client.delete(key)
        except Exception as exc:
            logger.warning("RedisDepotStore.kv_delete(%r): %s — ignoring", key, exc)
