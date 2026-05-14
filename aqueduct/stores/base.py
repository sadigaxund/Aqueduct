"""Base ABCs + factory for the pluggable store backends.

Three relational-style stores (`obs`, `lineage`, `depot`) plus a depot-only
KV fast-path. Every backend exposes a uniform `connect()` returning a
context-manager that yields a `RelationalCursor`. The cursor accepts
DuckDB-style `?` placeholders and translates them when the underlying
driver requires `%s` (Postgres) — call sites stay readable and identical
regardless of backend.

Redis is an exception: it exposes a KV-only path (`kv_get` / `kv_put` /
`kv_delete`). Asking for `relational_connect()` on a Redis-backed obs
or lineage store is a configuration error rejected at `load_config()`
time via the `KVBackend` / `RelationalBackend` Literal split in
`aqueduct.config`.
"""

from __future__ import annotations

import contextlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator, Sequence

if TYPE_CHECKING:
    from aqueduct.config import AqueductConfig, StoreBackendConfig

logger = logging.getLogger(__name__)


class BackendUnsupportedError(Exception):
    """Raised when a call site asks a backend for an operation it does not support.

    Concrete trigger: a Redis-backed depot is asked for `relational_connect()`,
    or any obs/lineage write tries to run against Redis. In practice the
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
    """Common interface for relational-flavoured stores (obs + lineage + depot-PG)."""

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


class ObsStore(_RelationalStore):
    """Run records, failures, healing outcomes, signal overrides, probe signals, metrics."""


class LineageStore(_RelationalStore):
    """Column-level lineage."""


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


# ── Factory ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class StoreBundle:
    """Resolved per-run stores. Each store may share an underlying connection
    pool with another (e.g. postgres obs + lineage pointing at the same DSN).
    """

    obs: ObsStore
    lineage: LineageStore
    depot: DepotStore


def get_stores(cfg: "AqueductConfig", store_dir_override: "Path | str | None" = None) -> StoreBundle:
    """Construct the per-run store bundle from validated config.

    Backend dispatch:

    - `duckdb`   → `aqueduct.stores.duckdb_.DuckDBObsStore` + `DuckDBLineageStore` +
                   `DuckDBDepotStore`.
    - `postgres` → `aqueduct.stores.postgres.PostgresObsStore` etc., with the
                   three logical schemas (`obs` / `lineage` / `depot`) auto-created
                   on first connect.
    - `redis`    → `aqueduct.stores.redis_.RedisDepotStore` (depot only).

    Backends are validated at config-load time
    (`StoreBackendConfig` + `StoresConfig` validators in `aqueduct.config`),
    so an obs/lineage backend of `redis` cannot reach this function.

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
        DuckDBLineageStore,
        DuckDBObsStore,
    )

    def _resolve_duckdb_path(store_cfg: "StoreBackendConfig") -> Path:
        p = Path(store_cfg.path)
        if store_dir_override is not None and not p.is_absolute():
            return Path(store_dir_override) / p.name
        return p

    obs: ObsStore
    lineage: LineageStore
    depot: DepotStore

    # ── obs ──────────────────────────────────────────────────────────────────
    if cfg.stores.obs.backend == "duckdb":
        obs = DuckDBObsStore(_resolve_duckdb_path(cfg.stores.obs))
    elif cfg.stores.obs.backend == "postgres":
        from aqueduct.stores.postgres import PostgresObsStore
        obs = PostgresObsStore(cfg.stores.obs.path)
    else:  # pragma: no cover — guarded at config layer
        raise BackendUnsupportedError(
            f"obs.backend={cfg.stores.obs.backend!r} is not a supported relational backend"
        )

    # ── lineage ──────────────────────────────────────────────────────────────
    if cfg.stores.lineage.backend == "duckdb":
        lineage = DuckDBLineageStore(_resolve_duckdb_path(cfg.stores.lineage))
    elif cfg.stores.lineage.backend == "postgres":
        from aqueduct.stores.postgres import PostgresLineageStore
        lineage = PostgresLineageStore(cfg.stores.lineage.path)
    else:  # pragma: no cover
        raise BackendUnsupportedError(
            f"lineage.backend={cfg.stores.lineage.backend!r} is not a supported relational backend"
        )

    # ── depot ────────────────────────────────────────────────────────────────
    if cfg.stores.depot.backend == "duckdb":
        depot = DuckDBDepotStore(_resolve_duckdb_path(cfg.stores.depot))
    elif cfg.stores.depot.backend == "postgres":
        from aqueduct.stores.postgres import PostgresDepotStore
        depot = PostgresDepotStore(cfg.stores.depot.path)
    elif cfg.stores.depot.backend == "redis":
        from aqueduct.stores.redis_ import RedisDepotStore
        depot = RedisDepotStore(cfg.stores.depot.path)
    else:  # pragma: no cover
        raise BackendUnsupportedError(
            f"depot.backend={cfg.stores.depot.backend!r} is not a supported KV backend"
        )

    return StoreBundle(obs=obs, lineage=lineage, depot=depot)


# Field placeholder so static analysis tools don't complain about the
# generator-yielding ABC method above.
_ = field
