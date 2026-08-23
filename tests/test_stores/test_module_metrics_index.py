"""Phase 85 E2 — module_metrics gains a run_id index.

`report --profile <run_id>` (aqueduct/stores/queries.py:270,280) filters by
run_id, not module_id — the table was indexed on module_id only.
"""

from __future__ import annotations

import duckdb
import pytest

from aqueduct.executor.models import MODULE_METRICS_DDL

pytestmark = pytest.mark.unit


def _index_names(con) -> set[str]:
    rows = con.execute(
        "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'module_metrics'"
    ).fetchall()
    return {r[0] for r in rows}


def test_module_metrics_ddl_creates_both_indexes(tmp_path):
    con = duckdb.connect(str(tmp_path / "o.db"))
    con.execute(MODULE_METRICS_DDL)
    names = _index_names(con)
    con.close()
    assert "idx_module_metrics_module" in names
    assert "idx_module_metrics_run" in names


def test_module_metrics_ddl_is_idempotent_on_existing_table(tmp_path):
    con = duckdb.connect(str(tmp_path / "o.db"))
    con.execute(MODULE_METRICS_DDL)
    con.execute(MODULE_METRICS_DDL)  # re-run against an existing table — must not raise
    names = _index_names(con)
    con.close()
    assert "idx_module_metrics_run" in names
