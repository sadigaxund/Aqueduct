"""Phase 67 — `aqueduct studio` data layer (read-only DuckDB, no textual/pyspark)."""
from __future__ import annotations

import json

import duckdb
import pytest

from aqueduct.tui import data as d

pytestmark = pytest.mark.unit

_DDL = """
CREATE TABLE IF NOT EXISTS run_records (
    run_id VARCHAR, blueprint_id VARCHAR, status VARCHAR,
    started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, module_results VARCHAR
);
CREATE TABLE IF NOT EXISTS module_metrics (
    run_id VARCHAR NOT NULL, module_id VARCHAR NOT NULL,
    records_read BIGINT, bytes_read BIGINT, records_written BIGINT,
    bytes_written BIGINT, duration_ms BIGINT, captured_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS column_lineage (
    blueprint_id VARCHAR, run_id VARCHAR, channel_id VARCHAR,
    output_column VARCHAR, source_table VARCHAR, source_column VARCHAR,
    captured_at TIMESTAMPTZ
);
"""


def _db(tmp_path):
    p = tmp_path / "observability.db"
    c = duckdb.connect(str(p))
    c.execute(_DDL)
    return p, c


# ── discover_stores ─────────────────────────────────────────────────────────

def test_discover_with_store_dir(tmp_path):
    _db(tmp_path)
    stores = d.discover_stores(store_dir=str(tmp_path))
    assert len(stores) == 1 and stores[0].db_path.name == "observability.db"


def test_discover_store_dir_missing(tmp_path):
    assert d.discover_stores(store_dir=str(tmp_path)) == []


def test_discover_scans_root(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # isolate the cwd-relative flat-default check
    root = tmp_path / "obs"
    for bp in ("alpha", "beta"):
        (root / bp).mkdir(parents=True)
        duckdb.connect(str(root / bp / "observability.db")).close()
    stores = d.discover_stores(root=str(root))
    assert [s.blueprint_id for s in stores] == ["alpha", "beta"]


def test_discover_flat_default_file(tmp_path, monkeypatch):
    # The reported bug: flat .aqueduct/observability.db (default single-pipeline layout).
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".aqueduct").mkdir()
    duckdb.connect(str(tmp_path / ".aqueduct" / "observability.db")).close()
    stores = d.discover_stores()
    assert len(stores) == 1
    assert stores[0].db_path.name == "observability.db"


def test_discover_explicit_obs_path_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    custom = tmp_path / "custom" / "myobs.db"
    custom.parent.mkdir()
    duckdb.connect(str(custom)).close()
    stores = d.discover_stores(obs_path=str(custom))
    assert len(stores) == 1 and stores[0].db_path == custom


# ── list_runs ───────────────────────────────────────────────────────────────

def test_list_runs_recent_first(tmp_path):
    p, c = _db(tmp_path)
    c.execute("INSERT INTO run_records VALUES ('r1','bp','success','2026-06-18'::timestamptz, now(), '[]')")
    c.execute("INSERT INTO run_records VALUES ('r2','bp','error','2026-06-19'::timestamptz, now(), '[]')")
    c.close()
    runs = d.list_runs(p)
    assert [r.run_id for r in runs] == ["r2", "r1"]
    assert runs[0].status == "error"


# ── run_detail ──────────────────────────────────────────────────────────────

def test_run_detail_modules_and_profile(tmp_path):
    p, c = _db(tmp_path)
    mr = json.dumps([{"module_id": "a", "status": "success", "error": ""},
                     {"module_id": "b", "status": "error", "error": "boom"}])
    c.execute("INSERT INTO run_records VALUES ('r1','bp','error', now(), now(), ?)", [mr])
    c.execute("INSERT INTO module_metrics VALUES ('r1','b',NULL,NULL,5,50,900,now())")
    c.execute("INSERT INTO module_metrics VALUES ('r1','a',NULL,NULL,1,10,100,now())")
    c.close()
    det = d.run_detail(p, "r1")
    assert det is not None
    assert [m.module_id for m in det.modules] == ["a", "b"]
    assert det.modules[1].error == "boom"
    assert [pr.module_id for pr in det.profile] == ["b", "a"]  # heaviest first


def test_run_detail_missing_returns_none(tmp_path):
    p, c = _db(tmp_path)
    c.close()
    assert d.run_detail(p, "nope") is None


# ── run_sql (read-only) ─────────────────────────────────────────────────────

def test_run_sql_select(tmp_path):
    p, c = _db(tmp_path)
    c.execute("INSERT INTO run_records VALUES ('r1','bp','success', now(), now(), '[]')")
    c.close()
    cols, rows = d.run_sql(p, "SELECT run_id, status FROM run_records")
    assert cols == ["run_id", "status"]
    assert rows == [("r1", "success")]


def test_run_sql_write_is_blocked(tmp_path):
    p, c = _db(tmp_path)
    c.close()
    with pytest.raises(Exception):  # read-only connection rejects mutation
        d.run_sql(p, "CREATE TABLE evil (x INT)")


# ── lineage ─────────────────────────────────────────────────────────────────

def test_lineage_rows(tmp_path):
    p, c = _db(tmp_path)
    c.execute("INSERT INTO column_lineage VALUES ('bp','r1','ch','out','src','col', now())")
    c.close()
    rows = d.lineage(p)
    assert len(rows) == 1 and rows[0].output_column == "out"


def test_lineage_absent_table_empty(tmp_path):
    p = tmp_path / "bare.db"
    duckdb.connect(str(p)).close()  # no column_lineage table
    assert d.lineage(p) == []
