"""Tests for the Depot KV store."""

from pathlib import Path

import duckdb
import pytest
pytestmark = pytest.mark.unit

from aqueduct.depot.depot import DepotStore


def test_depot_get_no_db(tmp_path):
    store = DepotStore(tmp_path / "missing.db")
    assert store.get("some_key", "default_val") == "default_val"


def test_depot_get_missing_key(tmp_path):
    store = DepotStore(tmp_path / "store.db")
    store.put("other", "value")
    assert store.get("missing", "default_val") == "default_val"


def test_depot_put_creates_db(tmp_path):
    db_path = tmp_path / "store.db"
    store = DepotStore(db_path)
    store.put("k", "v")
    assert db_path.exists()
    assert store.get("k") == "v"


def test_depot_put_twice_upsert(tmp_path):
    store = DepotStore(tmp_path / "store.db")
    store.put("k", "v1")
    store.put("k", "v2")
    assert store.get("k") == "v2"


def test_depot_put_multiple_keys(tmp_path):
    store = DepotStore(tmp_path / "store.db")
    store.put("key_a", "alpha")
    store.put("key_b", "beta")
    assert store.get("key_a") == "alpha"
    assert store.get("key_b") == "beta"


def test_depot_updated_at(tmp_path):
    db_path = tmp_path / "store.db"
    store = DepotStore(db_path)
    store.put("k", "v")
    conn = duckdb.connect(str(db_path), read_only=True)
    row = conn.execute("SELECT CAST(updated_at AS VARCHAR) FROM depot_kv WHERE key = 'k'").fetchone()
    conn.close()
    assert row is not None
    assert row[0] is not None
    # DuckDB may format TIMESTAMPTZ as "YYYY-MM-DD HH:MM:SS..." (space) or ISO "T" sep
    assert len(row[0]) >= 19 and "-" in row[0] and ":" in row[0]


def test_depot_get_default_empty_string(tmp_path):
    store = DepotStore(tmp_path / "store.db")
    assert store.get("absent") == ""


def test_depot_get_db_access_error(tmp_path, monkeypatch):
    db_path = tmp_path / "store.db"
    store = DepotStore(db_path)
    store.put("k", "v")

    def mock_connect(*args, **kwargs):
        raise RuntimeError("Access Denied")

    monkeypatch.setattr("duckdb.connect", mock_connect)
    assert store.get("k", "default") == "default"


def test_depot_close_no_op(tmp_path):
    store = DepotStore(tmp_path / "store.db")
    assert store.close() is None
    assert store.close() is None  # idempotent — a second close is also a no-op


def test_depot_get_after_put_no_read_only_error(tmp_path):
    """get() must not fail with DDL error on read_only connection."""
    db_path = tmp_path / "store.db"
    store = DepotStore(db_path)
    store.put("watermark", "2024-01-01")
    # This was previously broken: get() ran CREATE TABLE on read_only conn
    result = store.get("watermark", "fallback")
    assert result == "2024-01-01"


def test_depot_no_args_constructor_raises_type_error():
    """`DepotStore()` with neither `db_path` nor `backend` is the documented
    (in the class docstring) error path, not a silent default."""
    with pytest.raises(TypeError, match="requires either db_path or backend"):
        DepotStore()


class _FakeBackend:
    """Generic `aqueduct.stores.DepotStore` stand-in — the façade must
    delegate to WHATEVER backend it's constructed with, not just DuckDB
    (the only backend the rest of this file's tests exercise via `db_path`).
    """

    def __init__(self):
        self.calls: list[tuple] = []
        self._data: dict[str, str] = {}

    def kv_get(self, key: str, default: str = "") -> str:
        self.calls.append(("kv_get", key, default))
        return self._data.get(key, default)

    def kv_put(self, key: str, value: str) -> None:
        self.calls.append(("kv_put", key, value))
        self._data[key] = value


def test_depot_delegates_to_generic_backend():
    backend = _FakeBackend()
    store = DepotStore(backend=backend)

    assert store.get("missing", "fallback") == "fallback"
    store.put("k", "v")
    assert store.get("k") == "v"
    assert store.close() is None

    assert ("kv_get", "missing", "fallback") in backend.calls
    assert ("kv_put", "k", "v") in backend.calls
    assert ("kv_get", "k", "") in backend.calls
