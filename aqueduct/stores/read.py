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
      2. A non-default ``stores.observability.path`` → that file (or the file
         inside it if a directory).
      3. Default sentinel + ``blueprint_id`` → the per-pipeline routed file
         ``.aqueduct/observability/<blueprint_id>/observability.db``.
      4. Default sentinel + ``run_id`` → the routed file that contains the run.
      5. The flat default ``.aqueduct/observability.db``.
    """
    if store_dir:
        candidate = Path(store_dir) / DEFAULT_OBS_DB_FILENAME
        return candidate if candidate.exists() else None

    obs_path = cfg.stores.observability.path
    if obs_path != _DEFAULT_OBS_PATH:
        explicit = Path(obs_path)
        if explicit.is_dir():
            explicit = explicit / DEFAULT_OBS_DB_FILENAME
        return explicit if explicit.exists() else None

    if blueprint_id:
        routed = Path(_OBS_ROUTING_ROOT) / blueprint_id / DEFAULT_OBS_DB_FILENAME
        if routed.exists():
            return routed

    if run_id:
        import duckdb as _duckdb

        for candidate in sorted(Path(_OBS_ROUTING_ROOT).glob(f"*/{DEFAULT_OBS_DB_FILENAME}")):
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

    legacy = Path(_DEFAULT_OBS_PATH)
    return legacy if legacy.exists() else None


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
