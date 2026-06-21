"""Canonical backend-aware READ access to the observability store (Phase 69).

Read commands (`report`, `runs`, `lineage`, `heal`, `patch`, `studio`) MUST
resolve the observability store through here instead of hand-building a
`.aqueduct/...` path and calling `duckdb.connect()` directly. That older pattern
ignored both `stores.observability.backend` (so a Postgres store was unreadable)
and a non-default `stores.observability.path`.

Two layers:

- ``resolve_duckdb_obs_path(cfg, store_dir, run_id)`` — the DuckDB **file** a read
  should open: ``--store-dir`` override → a non-default configured path → the
  per-pipeline routed file matching ``run_id`` → the flat default. DuckDB-only
  (Postgres keeps every run in one schema, so there is no file to pick).
- ``open_obs_read(cfg, store_dir, run_id)`` — **backend-aware**: returns an
  ``ObservabilityStore`` whose ``.connect()`` yields a ``RelationalCursor``
  (`?` placeholders work on both backends). Returns ``None`` only for DuckDB when
  no store file exists yet.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from aqueduct.config import DEFAULT_OBS_DB_FILENAME

if TYPE_CHECKING:
    from aqueduct.config import AqueductConfig
    from aqueduct.stores.base import ObservabilityStore

_DEFAULT_OBS_PATH = f".aqueduct/{DEFAULT_OBS_DB_FILENAME}"
_OBS_ROUTING_ROOT = ".aqueduct/observability"


def resolve_duckdb_obs_path(
    cfg: "AqueductConfig",
    store_dir: str | None = None,
    run_id: str | None = None,
    blueprint_id: str | None = None,
) -> Path | None:
    """Resolve which DuckDB observability file a read should open (or None).

    Resolution order (the single source of truth — `cli._resolve_obs_db`
    delegates here):
      1. ``--store-dir`` → ``<store_dir>/observability.db``.
      2. A non-default ``stores.observability.path`` **ending in a file suffix**
         (e.g. ``.../obs.db``) → that single file (one store for all blueprints).
      3. Otherwise the path is a **base directory** (default ``.aqueduct/
         observability`` OR a suffix-less custom path = location-only routing):
         route ``<base>/<blueprint_id>/observability.db``, else the routed file
         whose ``run_records`` contains ``run_id``, else the flat file directly
         under the base.
    """
    if store_dir:
        candidate = Path(store_dir) / DEFAULT_OBS_DB_FILENAME
        return candidate if candidate.exists() else None

    obs_path = cfg.stores.observability.path
    routing_root = _OBS_ROUTING_ROOT
    flat_default = Path(_DEFAULT_OBS_PATH)
    if obs_path != _DEFAULT_OBS_PATH:
        explicit = Path(obs_path)
        if explicit.suffix and not explicit.is_dir():
            # Explicit single file — one store for every blueprint (no parallel).
            return explicit if explicit.exists() else None
        # Location-only base directory → route per-blueprint files under it.
        routing_root = str(explicit)
        flat_default = explicit / DEFAULT_OBS_DB_FILENAME

    if blueprint_id:
        routed = Path(routing_root) / blueprint_id / DEFAULT_OBS_DB_FILENAME
        if routed.exists():
            return routed

    if run_id:
        import duckdb as _duckdb

        for candidate in sorted(Path(routing_root).glob(f"*/{DEFAULT_OBS_DB_FILENAME}")):
            try:
                conn = _duckdb.connect(str(candidate), read_only=True)
                try:
                    hit = conn.execute(
                        "SELECT 1 FROM run_records WHERE run_id = ? LIMIT 1",
                        [run_id],
                    ).fetchone()
                finally:
                    conn.close()
                if hit:
                    return candidate
            except Exception:
                continue

    return flat_default if flat_default.exists() else None


def resolve_obs_store_dir(
    cfg: "AqueductConfig", blueprint_id: str, store_dir: str | None = None
) -> Path:
    """The directory holding a blueprint's ``observability.db`` on WRITE.

    The single source of truth for per-blueprint write routing (mirrors the inline
    logic in ``cli/run.py``): ``--store-dir`` wins; else the configured DuckDB path
    decides — the default sentinel OR a suffix-less directory → per-blueprint
    ``<base>/<blueprint_id>``; an explicit ``.db`` file → its parent (one shared
    file). DuckDB-only (Postgres self-manages its DSN).
    """
    if store_dir:
        return Path(store_dir)
    path = cfg.stores.observability.path
    if path == _DEFAULT_OBS_PATH:
        return Path(_OBS_ROUTING_ROOT) / blueprint_id
    p = Path(path)
    if not p.suffix:  # location-only base directory
        return p / blueprint_id
    return p.parent  # explicit single .db file


def open_obs_write(
    cfg: "AqueductConfig", blueprint_id: str, store_dir: str | None = None
) -> "ObservabilityStore":
    """Writable observability store at the per-blueprint path (mirrors ``run``).

    Postgres → the configured DSN store. DuckDB → a ``DuckDBObservabilityStore``
    at ``<resolve_obs_store_dir>/observability.db`` (the directory is created).
    Use this for commands that WRITE outside the run loop (e.g. ``drift``) so they
    land in the same per-blueprint file ``run`` uses, instead of opening the
    routing directory as a file.
    """
    if cfg.stores.observability.backend != "duckdb":
        from aqueduct.stores.base import get_stores

        return get_stores(cfg).observability
    d = resolve_obs_store_dir(cfg, blueprint_id, store_dir)
    d.mkdir(parents=True, exist_ok=True)
    from aqueduct.stores.duckdb_ import DuckDBObservabilityStore

    return DuckDBObservabilityStore(d / DEFAULT_OBS_DB_FILENAME)


def open_obs_read(
    cfg: "AqueductConfig",
    store_dir: str | None = None,
    run_id: str | None = None,
    blueprint_id: str | None = None,
) -> "ObservabilityStore | None":
    """Backend-aware observability store for reads.

    Postgres → the configured store (one schema, all runs). DuckDB → the resolved
    file wrapped in a ``DuckDBObservabilityStore``; ``None`` when no file exists.
    Use ``with store.connect() as cur: cur.execute("... ?", [param])``.
    """
    if cfg.stores.observability.backend == "postgres":
        from aqueduct.stores.base import get_stores

        return get_stores(cfg, store_dir_override=store_dir).observability

    path = resolve_duckdb_obs_path(cfg, store_dir, run_id, blueprint_id)
    if path is None:
        return None
    from aqueduct.stores.duckdb_ import DuckDBObservabilityStore

    return DuckDBObservabilityStore(path)
