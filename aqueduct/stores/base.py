"""Base ABCs + factory for the pluggable store backends.

Three relational-style stores (`observability`, `lineage`, `depot`) plus a depot-only
KV fast-path. Every backend exposes a uniform `connect()` returning a
context-manager that yields a `RelationalCursor`. The cursor accepts
DuckDB-style `?` placeholders and translates them when the underlying
driver requires `%s` (Postgres) — call sites stay readable and identical
regardless of backend.

Redis is an exception: it exposes a KV-only path (`kv_get` / `kv_put` /
`kv_delete`). Asking for `relational_connect()` on a Redis-backed observability
or lineage store is a configuration error rejected at `load_config()`
time via the `KVBackend` / `RelationalBackend` Literal split in
`aqueduct.config`.
"""

from __future__ import annotations

import contextlib
import logging
from aqueduct.errors import AqueductError
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator, Sequence

if TYPE_CHECKING:
    from aqueduct.config import AqueductConfig, StoreBackendConfig

logger = logging.getLogger(__name__)


class BackendUnsupportedError(AqueductError):
    """Raised when a call site asks a backend for an operation it does not support.

    Concrete trigger: a Redis-backed depot is asked for `relational_connect()`,
    or any observability/lineage write tries to run against Redis. In practice the
    `KVBackend`/`RelationalBackend` Literal split in `aqueduct.config`
    prevents that combination from validating, so this exception is a
    last-line-of-defense for direct programmatic callers.
    """


# ── Cursor abstraction ────────────────────────────────────────────────────────

class RelationalCursor:
    """Thin wrapper around a DB-API cursor with `?` → driver-native placeholder rewriting.

    All Aqueduct SQL uses DuckDB-style `?` parameters. For Postgres
    (`psycopg2`/`psycopg3`) and other `paramstyle="format"` drivers, we
    rewrite to `%s` on the way in. DuckDB and `paramstyle="qmark"` drivers
    pass through untouched. The wrapper otherwise forwards everything to
    the underlying cursor.
    """

    def __init__(self, raw_cursor: Any, paramstyle: str = "qmark") -> None:
        self._cursor = raw_cursor
        self._paramstyle = paramstyle

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> "RelationalCursor":
        sql_out = sql if self._paramstyle == "qmark" else sql.replace("?", "%s")
        if params is None:
            self._cursor.execute(sql_out)
        else:
            self._cursor.execute(sql_out, tuple(params))
        return self

    def executemany(self, sql: str, seq_of_params: Sequence[Sequence[Any]]) -> "RelationalCursor":
        sql_out = sql if self._paramstyle == "qmark" else sql.replace("?", "%s")
        self._cursor.executemany(sql_out, [tuple(p) for p in seq_of_params])
        return self

    def fetchone(self) -> Any:
        return self._cursor.fetchone()

    def fetchall(self) -> list[Any]:
        return self._cursor.fetchall()

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self._cursor.close()

    # Allow the wrapper to be used identically to a raw DB-API cursor
    def __getattr__(self, item: str) -> Any:
        return getattr(self._cursor, item)


# ── Store ABCs ────────────────────────────────────────────────────────────────

class _RelationalStore(ABC):
    """Common interface for relational-flavoured stores (observability + lineage + depot-PG)."""

    @abstractmethod
    @contextlib.contextmanager
    def connect(self) -> Iterator[RelationalCursor]:
        """Open a connection, yield a `RelationalCursor`, commit on success."""

    @property
    @abstractmethod
    def backend(self) -> str:
        """The configured backend name (`duckdb` | `postgres` | `redis`)."""

    @property
    @abstractmethod
    def location_label(self) -> str:
        """Human-readable string for log lines and `aqueduct doctor` output."""


class ObservabilityStore(_RelationalStore):
    """Run records, failures, healing outcomes, signal overrides, probe signals, metrics, column lineage."""


class DepotStore(ABC):
    """Cross-run KV state — `@aq.depot.get()` reads, `format: depot` Egress writes.

    Supported by both relational backends (duckdb, postgres) and the depot-only
    KV backend (redis). Relational backends expose `connect()` for direct SQL;
    redis exposes only the KV helpers below. Call sites should prefer the KV
    helpers because they work uniformly across all three backends.
    """

    @abstractmethod
    def kv_get(self, key: str, default: str = "") -> str: ...

    @abstractmethod
    def kv_put(self, key: str, value: str) -> None: ...

    @abstractmethod
    def kv_delete(self, key: str) -> None: ...

    @contextlib.contextmanager
    def connect(self) -> Iterator[RelationalCursor]:
        """Open a relational connection. Raises `BackendUnsupportedError` for Redis."""
        raise BackendUnsupportedError(
            f"depot backend {self.backend!r} does not expose a relational cursor; "
            "use kv_get / kv_put / kv_delete instead."
        )
        yield  # pragma: no cover  (unreachable; satisfies the iterator protocol)

    @property
    @abstractmethod
    def backend(self) -> str: ...

    @property
    @abstractmethod
    def location_label(self) -> str: ...


class _RelationalDepotMixin:
    """Default ``kv_get`` / ``kv_put`` / ``kv_delete`` for relational backends.

    Subclasses must provide:
      * ``connect()`` → ``RelationalCursor`` context manager
      * ``_DDL`` → the ``CREATE TABLE IF NOT EXISTS depot_kv`` string

    File‑existence optimisation (DuckDB) is handled by checking for a
    ``_path`` attribute — Postgres backends that lack it skip the guard
    and delegate to ``connect()`` directly.
    """
    _DDL: str = ""

    def kv_get(self, key: str, default: str = "") -> str:
        path = getattr(self, "_path", None)
        if path is not None and isinstance(path, Path) and not path.exists():
            return default
        try:
            with self.connect() as cur:
                cur.execute(self._DDL)
                row = cur.execute(
                    "SELECT value FROM depot_kv WHERE key = ?", [key]
                ).fetchone()
                return row[0] if row else default
        except Exception as exc:
            logger.warning("%s.kv_get(%r): %s — returning default", type(self).__name__, key, exc)
            return default

    def kv_put(self, key: str, value: str) -> None:
        from datetime import datetime, timezone

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
        path = getattr(self, "_path", None)
        if path is not None and isinstance(path, Path) and not path.exists():
            return
        with self.connect() as cur:
            cur.execute(self._DDL)
            cur.execute("DELETE FROM depot_kv WHERE key = ?", [key])


# ── Factory ───────────────────────────────────────────────────────────────────

class _NamespacedDepot:
    """Transparently prefixes every depot key with ``<blueprint_id>:`` so two
    blueprints sharing a physical depot are isolated (no key collisions). Applied
    to the default mount + any mount without ``shared: true``; shared mounts use
    raw keys. Wraps the **raw** KV store at the ``kv_*`` level, so all access —
    the ``DepotStore`` wrapper (``@aq.depot.*`` reads + ``format: depot`` Egress
    writes + ``_last_run_id``) and any direct ``kv_*`` call — stays consistent.
    Everything else (``backend``, ``location_label``, ``relational_connect`` …)
    passes through.
    """

    def __init__(self, inner: "DepotStore", prefix: str) -> None:
        self._inner = inner
        self._prefix = prefix

    def kv_get(self, key: str, default: Any = "") -> Any:
        return self._inner.kv_get(f"{self._prefix}{key}", default)

    def kv_put(self, key: str, value: Any) -> None:
        self._inner.kv_put(f"{self._prefix}{key}", value)

    def kv_delete(self, key: str) -> None:
        self._inner.kv_delete(f"{self._prefix}{key}")

    def __getattr__(self, name: str) -> Any:  # passthrough (backend, location_label, …)
        return getattr(self._inner, name)


@dataclass(frozen=True)
class StoreBundle:
    """Resolved per-run stores. Each store may share an underlying connection
    pool with another (e.g. postgres observability + lineage pointing at the same DSN).
    """

    observability: ObservabilityStore
    depot: DepotStore                          # the default mount (key-isolated per blueprint)
    depots: dict[str, DepotStore] = field(default_factory=dict)   # name → mount (incl. "default")


def get_stores(
    cfg: "AqueductConfig",
    store_dir_override: "Path | str | None" = None,
    blueprint_id: str | None = None,
) -> StoreBundle:
    """Construct the per-run store bundle from validated config.

    Backend dispatch:

    - `duckdb`   → `aqueduct.stores.duckdb_.DuckDBObservabilityStore` + `DuckDBLineageStore` +
                   `DuckDBDepotStore`.
    - `postgres` → `aqueduct.stores.postgres.PostgresObservabilityStore` etc., with the
                   three logical schemas (`observability` / `lineage` / `depot`) auto-created
                   on first connect.
    - `redis`    → `aqueduct.stores.redis_.RedisDepotStore` (depot only).

    Backends are validated at config-load time
    (`StoreBackendConfig` + `StoresConfig` validators in `aqueduct.config`),
    so an observability/lineage backend of `redis` cannot reach this function.

    Args:
        cfg: Validated `AqueductConfig`.
        store_dir_override: When set, used as the root directory for any
            backend that resolves paths relative to a directory (DuckDB).
            Passing a relative path keeps current behaviour; an absolute
            path lets `aqueduct run --store-dir` redirect persistence.
    """
    # Lazy imports keep optional backends optional.
    from aqueduct.stores.duckdb_ import (
        DuckDBDepotStore,
        DuckDBObservabilityStore,
    )

    def _resolve_duckdb_path(store_cfg: "StoreBackendConfig") -> Path:
        if store_cfg.path is None:
            from aqueduct.stores.read import _OBS_ROUTING_ROOT
            from aqueduct.config import DEFAULT_OBS_DB_FILENAME
            _base = Path(store_dir_override) if store_dir_override else Path(_OBS_ROUTING_ROOT)
            _bid = blueprint_id or "default"
            return _base / _bid / DEFAULT_OBS_DB_FILENAME
        p = Path(store_cfg.path)
        if store_dir_override is not None and not p.is_absolute():
            return Path(store_dir_override) / p.name
        return p

    obs: ObservabilityStore
    depot: DepotStore

    # ── observability (includes column lineage — merged in Phase 38) ──────────
    if cfg.stores.observability.backend == "duckdb":
        obs = DuckDBObservabilityStore(_resolve_duckdb_path(cfg.stores.observability))
    elif cfg.stores.observability.backend == "postgres":
        from aqueduct.stores.postgres import PostgresObservabilityStore
        obs = PostgresObservabilityStore(cfg.stores.observability.path or "")
    else:  # pragma: no cover — guarded at config layer
        raise BackendUnsupportedError(
            f"obs.backend={cfg.stores.observability.backend!r} is not a supported relational backend"
        )

    # ── depot mounts ───────────────────────────────────────────────────────────
    # Build every mount in stores.depots (incl. the implicit `default`). Each is
    # per-blueprint key-isolated (prefixed) unless shared: true. With no
    # blueprint_id (some non-run contexts) keys are raw — same as pre-isolation.
    def _build_depot(mount: Any) -> DepotStore:
        if mount.backend == "duckdb":
            return DuckDBDepotStore(_resolve_duckdb_path(mount))
        if mount.backend == "postgres":
            from aqueduct.stores.postgres import PostgresDepotStore
            return PostgresDepotStore(mount.path)
        if mount.backend == "redis":
            from aqueduct.stores.redis_ import RedisDepotStore
            return RedisDepotStore(mount.path)
        raise BackendUnsupportedError(  # pragma: no cover — guarded at config layer
            f"depot.backend={mount.backend!r} is not a supported KV backend")

    depots: dict[str, DepotStore] = {}
    for name, mount in cfg.stores.effective_depots().items():
        raw = _build_depot(mount)
        if blueprint_id and not mount.shared:
            depots[name] = _NamespacedDepot(raw, f"{blueprint_id}:")  # type: ignore[assignment]
        else:
            depots[name] = raw
    depot = depots["default"]

    return StoreBundle(observability=obs, depot=depot, depots=depots)


# Field placeholder so static analysis tools don't complain about the
# generator-yielding ABC method above.
_ = field
