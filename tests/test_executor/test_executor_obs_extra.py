from __future__ import annotations

import pytest

pytestmark = pytest.mark.spark

import duckdb  # noqa: E402

from aqueduct.executor.spark.executor import (  # noqa: E402
    _MODULE_METRICS_DDL,
    _update_metric,
    _write_stage_metrics,
)


def test_write_stage_metrics_success(tmp_path):
    store_dir = tmp_path / "obs"
    metrics = {
        "records_read": 100,
        "bytes_read": 200,
        "records_written": 300,
        "bytes_written": 400,
        "duration_ms": 500,
    }
    _write_stage_metrics("m1", "r1", metrics, store_dir)

    db_path = store_dir / "observability.db"
    assert db_path.exists()

    conn = duckdb.connect(str(db_path))
    row = conn.execute("SELECT * FROM module_metrics").fetchone()
    conn.close()

    assert row[0] == "r1"
    assert row[1] == "m1"
    assert row[2] == 100
    assert row[3] == 200
    assert row[4] == 300
    assert row[5] == 400
    assert row[6] == 500


def test_update_metric_success(tmp_path):
    store_dir = tmp_path / "obs"
    store_dir.mkdir()
    db_path = store_dir / "observability.db"

    # Setup initial row
    conn = duckdb.connect(str(db_path))
    conn.execute(_MODULE_METRICS_DDL)
    conn.execute(
        "INSERT INTO module_metrics (run_id, module_id, records_read, captured_at) VALUES ('r1', 'm1', 0, now())"
    )
    conn.close()

    _update_metric(store_dir, "r1", "m1", "records_read", 999)

    conn = duckdb.connect(str(db_path))
    val = conn.execute("SELECT records_read FROM module_metrics WHERE run_id='r1'").fetchone()[0]
    conn.close()
    assert val == 999


def test_write_stage_metrics_no_store_dir():
    # store_dir=None → silent no-op: returns None, persists nothing
    assert _write_stage_metrics("m1", "r1", {}, None) is None


def test_module_metrics_ddl_creates_run_id_index(tmp_path):
    """Phase 85 W9 follow-up — the Spark copy of MODULE_METRICS_DDL
    (deliberately duplicated from ``executor/models.py``, see this module's
    docstring) must carry the same ``idx_module_metrics_run`` index W8 added
    to the ``executor/models.py`` copy — `report --profile <run_id>`
    (aqueduct/stores/queries.py) filters by run_id, not module_id."""
    conn = duckdb.connect(str(tmp_path / "o.db"))
    conn.execute(_MODULE_METRICS_DDL)
    names = {
        r[0]
        for r in conn.execute(
            "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'module_metrics'"
        ).fetchall()
    }
    conn.close()
    assert "idx_module_metrics_module" in names
    assert "idx_module_metrics_run" in names
