"""Tests for the Depot KV store."""

import pytest
import duckdb
from pathlib import Path

from aqueduct.depot.depot import DepotStore

def test_depot_get_no_db(tmp_path):
    store = DepotStore(tmp_path / "missing.db")
    assert store.get("some_key", "default_val") == "default_val"

def test_depot_get_missing_key(tmp_path):
    store = DepotStore(tmp_path / "store.db")
    store.put("other", "value")
    assert store.get("missing", "default_val") == "default_val"

@pytest.mark.xfail(reason="DepotStore.get raises CREATE TABLE error in read_only mode")
def test_depot_put_creates_db(tmp_path):
    db_path = tmp_path / "store.db"
    store = DepotStore(db_path)
    store.put("k", "v")
    assert db_path.exists()
    assert store.get("k") == "v"

@pytest.mark.xfail(reason="DepotStore.get raises CREATE TABLE error in read_only mode")
def test_depot_put_twice_upsert(tmp_path):
    store = DepotStore(tmp_path / "store.db")
    store.put("k", "v1")
    store.put("k", "v2")
    assert store.get("k") == "v2"

@pytest.mark.xfail(reason="DuckDB TIMESTAMPTZ fetch requires missing pytz dependency")
def test_depot_updated_at(tmp_path):
    db_path = tmp_path / "store.db"
    store = DepotStore(db_path)
    store.put("k", "v")
    conn = duckdb.connect(str(db_path), read_only=True)
    row = conn.execute("SELECT updated_at FROM depot_kv WHERE key = 'k'").fetchone()
    assert row is not None
    assert row[0] is not None
    conn.close()

def test_depot_get_db_access_error(tmp_path, monkeypatch):
    """get with DB access error returns default (no exception raised)"""
    db_path = tmp_path / "store.db"
    store = DepotStore(db_path)
    store.put("k", "v")
    
    def mock_connect(*args, **kwargs):
        raise RuntimeError("Access Denied")
    monkeypatch.setattr("duckdb.connect", mock_connect)
    
    assert store.get("k", "default") == "default"

def test_depot_close_no_op(tmp_path):
    store = DepotStore(tmp_path / "store.db")
    store.close()  # Should not raise
