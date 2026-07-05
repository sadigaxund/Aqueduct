"""Shared store-layer DDL constants.

Centralises schema strings that more than one store backend must keep in sync.
Today that is only ``depot_kv`` (the DuckDB and Postgres relational depot share
an identical schema), so it lives here as a single source of truth — the
``DuckDBDepotStore`` and ``PostgresDepotStore`` both bind ``_DDL`` to it.

DDL owned by a single layer stays with its owner (e.g. ``patch_index`` in
``patch/index.py``, the observability tables in ``surveyor/ddl.py``,
``benchmark_results`` in ``surveyor/benchmark_store.py``). Only genuinely
cross-backend store DDL belongs here.

Pure SQL strings — no imports.
"""

from __future__ import annotations

# Relational depot key/value table. Backend-agnostic: ``VARCHAR`` / ``TIMESTAMPTZ``
# are accepted by both DuckDB and Postgres; no trailing ``;`` so a single
# ``cur.execute()`` is valid on both.
DEPOT_KV_DDL = """
        CREATE TABLE IF NOT EXISTS depot_kv (
            key        VARCHAR PRIMARY KEY,
            value      VARCHAR NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        )
    """
