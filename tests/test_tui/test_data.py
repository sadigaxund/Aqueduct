"""Phase 67/69 — `aqueduct studio` data layer (backend-agnostic, no textual/pyspark)."""
from __future__ import annotations

import json
from types import SimpleNamespace

import duckdb
import pytest

from aqueduct.tui import data as d
from aqueduct.stores.duckdb_ import DuckDBObservabilityStore

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


def _cfg(path=None, backend="duckdb"):
    return SimpleNamespace(
        stores=SimpleNamespace(observability=SimpleNamespace(path=path, backend=backend))
    )


def _store(path):
    return DuckDBObservabilityStore(path)


# ── discover_stores ─────────────────────────────────────────────────────────

def test_discover_with_store_dir(tmp_path):
    duckdb.connect(str(tmp_path / "observability.db")).close()
    handles = d.discover_stores(_cfg(), store_dir=str(tmp_path))
    assert len(handles) == 1 and handles[0].duckdb_path == tmp_path / "observability.db"


def test_discover_store_dir_missing(tmp_path):
    assert d.discover_stores(_cfg(), store_dir=str(tmp_path)) == []


def test_discover_scans_root(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    root = tmp_path / "obs"
    for bp in ("alpha", "beta"):
        (root / bp).mkdir(parents=True)
        duckdb.connect(str(root / bp / "observability.db")).close()
    handles = d.discover_stores(_cfg(), root=str(root))
    assert [h.label for h in handles] == ["alpha", "beta"]


def test_discover_flat_default(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    obs_root = tmp_path / ".aqueduct" / "observability"
    obs_root.mkdir(parents=True)
    (obs_root / "default_bp").mkdir()
    duckdb.connect(str(obs_root / "default_bp" / "observability.db")).close()
    handles = d.discover_stores(_cfg())
    assert len(handles) == 1 and handles[0].duckdb_path.name == "observability.db"


def test_discover_postgres_single_handle(monkeypatch):
    sentinel = object()
    bundle = SimpleNamespace(observability=sentinel)
    monkeypatch.setattr("aqueduct.stores.base.get_stores", lambda cfg: bundle)
    handles = d.discover_stores(_cfg(backend="postgres"))
    assert len(handles) == 1
    assert handles[0].store is sentinel and handles[0].duckdb_path is None


# ── list_runs / run_detail / lineage (via ObservabilityStore) ────────────────

def test_list_runs_recent_first(tmp_path):
    p = tmp_path / "observability.db"
    c = duckdb.connect(str(p)); c.execute(_DDL)
    c.execute("INSERT INTO run_records VALUES ('r1','bp','success','2026-06-18'::timestamptz, now(), '[]')")
    c.execute("INSERT INTO run_records VALUES ('r2','bp','error','2026-06-19'::timestamptz, now(), '[]')")
    c.close()
    runs = d.list_runs(_store(p))
    assert [r.run_id for r in runs] == ["r2", "r1"]


def test_run_detail_modules_and_profile(tmp_path):
    p = tmp_path / "observability.db"
    c = duckdb.connect(str(p)); c.execute(_DDL)
    mr = json.dumps([{"module_id": "a", "status": "success", "error": ""},
                     {"module_id": "b", "status": "error", "error": "boom"}])
    c.execute("INSERT INTO run_records VALUES ('r1','bp','error', now(), now(), ?)", [mr])
    c.execute("INSERT INTO module_metrics VALUES ('r1','b',NULL,NULL,5,50,900,now())")
    c.execute("INSERT INTO module_metrics VALUES ('r1','a',NULL,NULL,1,10,100,now())")
    c.close()
    det = d.run_detail(_store(p), "r1")
    assert det is not None
    assert [m.module_id for m in det.modules] == ["a", "b"]
    assert [pr.module_id for pr in det.profile] == ["a", "b"]  # execution order (matches modules)


def test_run_detail_missing_returns_none(tmp_path):
    p = tmp_path / "observability.db"
    duckdb.connect(str(p)).execute(_DDL)
    assert d.run_detail(_store(p), "nope") is None


def test_lineage_rows(tmp_path):
    p = tmp_path / "observability.db"
    c = duckdb.connect(str(p)); c.execute(_DDL)
    c.execute("INSERT INTO column_lineage VALUES ('bp','r1','ch','out','src','col', now())")
    c.close()
    rows = d.lineage(_store(p))
    assert len(rows) == 1 and rows[0].output_column == "out"


def test_lineage_absent_table_empty(tmp_path):
    p = tmp_path / "bare.db"
    duckdb.connect(str(p)).close()  # no column_lineage table
    assert d.lineage(_store(p)) == []


# ── run_sql_readonly (duckdb, read-only) ─────────────────────────────────────

def test_run_sql_readonly_select(tmp_path):
    p = tmp_path / "observability.db"
    c = duckdb.connect(str(p)); c.execute(_DDL)
    c.execute("INSERT INTO run_records VALUES ('r1','bp','success', now(), now(), '[]')")
    c.close()
    cols, rows = d.run_sql_readonly(p, "SELECT run_id, status FROM run_records")
    assert cols == ["run_id", "status"] and rows == [("r1", "success")]


def test_run_sql_readonly_write_blocked(tmp_path):
    p = tmp_path / "observability.db"
    duckdb.connect(str(p)).execute(_DDL)
    with pytest.raises(Exception):
        d.run_sql_readonly(p, "CREATE TABLE evil (x INT)")
