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

from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aqueduct.stores import DepotStore as _DepotStoreBackend


class DepotStore:
    """Façade over the Phase 28 store-abstraction depot backend.

    Historically `DepotStore` opened a DuckDB connection per call. Phase 28
    moved that logic behind `aqueduct.stores.DepotStore` (the ABC) so the
    depot can be backed by DuckDB, Postgres, or Redis without touching
    call sites. This façade IS the supported `.get()` / `.put()` / `.close()`
    API (not a deprecation shim) and delegates to whichever backend is
    configured.

    Construct with a backend object (`DepotStore(backend=...)` — the CLI's
    Phase 28 wiring) or, as a convenience for direct programmatic callers
    and tests, with a DuckDB file path (`DepotStore(db_path=...)`).
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


def preview_depots(cfg: Any, blueprint_id: str | None) -> tuple[DepotStore, dict[str, DepotStore]]:
    """Build the real, blueprint-namespaced depots for a PREVIEW compile.

    Used by the three preview compile paths that don't already thread a depot
    through (``aqueduct compile``, ``aqueduct drift``, patch-preview Gate 3 —
    ``aqueduct.patch.preview.run_sandbox_gate``) so ``@aq.depot.get(...)`` /
    ``@aq.run.prev_id`` resolve the SAME namespaced value a real ``aqueduct
    run`` would see, instead of hard-failing on the loud
    ``_depot_get_or_raise`` `CompileError` (or, pre-Phase-fix, silently
    returning the default).

    Delegates the mount-building to ``aqueduct.stores.base.build_depot_mounts``
    — the single implementation of the ``_NamespacedDepot`` wrapping rules
    shared with ``aqueduct.stores.get_stores`` (the run path) — with
    ``read_only=True``: a preview must never contend with a live run's DuckDB
    single-writer lock. Returns ``(default_depot, depots_by_name)``, both
    wrapped in this façade (the compiler calls ``.get()``, not the raw
    backend's ``kv_get()``).

    Raises whatever ``build_depot_mounts``/store construction raises (bad
    config, unreachable backend, ...). Callers are expected to catch broadly
    and fall back to ``depot=None, depots=None`` — a depot build failure must
    never crash a preview that would otherwise succeed; the loud
    `_depot_get_or_raise` `CompileError` is the intended backstop for a
    Blueprint that genuinely needs a depot with none configured.
    """
    from aqueduct.stores.base import build_depot_mounts

    mounts = build_depot_mounts(cfg, blueprint_id=blueprint_id, read_only=True)
    wrapped = {name: DepotStore(backend=store) for name, store in mounts.items()}
    return wrapped["default"], wrapped
