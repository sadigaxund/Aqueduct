"""Tests for the Depot KV store."""

import duckdb
import pytest

from aqueduct.depot.depot import DepotStore

pytestmark = pytest.mark.unit


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
    row = conn.execute(
        "SELECT CAST(updated_at AS VARCHAR) FROM depot_kv WHERE key = 'k'"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] is not None
    # DuckDB may format TIMESTAMPTZ as "YYYY-MM-DD HH:MM:SS..." (space) or ISO "T" sep
    assert len(row[0]) >= 19 and "-" in row[0] and ":" in row[0]


class TestPreviewDepots:
    """`preview_depots` — the shared helper preview compile paths (`aqueduct
    compile`/`drift`, patch-preview Gate 3) use to build a REAL, namespaced
    depot, matching `aqueduct run`'s own `get_stores`/`build_depot_mounts`
    wiring exactly."""

    def _cfg(self, tmp_path):
        from aqueduct.config import AqueductConfig

        return AqueductConfig(
            **{
                "stores": {
                    "depots": {"default": {"backend": "duckdb", "path": f"{tmp_path}/depot.db"}}
                }
            }
        )

    def test_resolves_real_stored_value_with_blueprint_namespacing(self, tmp_path):
        """A value written by a real `aqueduct run` (via `get_stores`, prefixed
        `<blueprint_id>:`) must be visible through `preview_depots` for the
        SAME blueprint_id — proving preview namespacing matches run namespacing."""
        import duckdb

        from aqueduct.depot.depot import DepotStore, preview_depots
        from aqueduct.stores import get_stores

        cfg = self._cfg(tmp_path)

        # Simulate what a real `aqueduct run` writes.
        run_bundle = get_stores(cfg, blueprint_id="bp1")
        DepotStore(backend=run_bundle.depot).put("watermark", "2024-01-01")

        # The raw file key carries the blueprint-id prefix.
        conn = duckdb.connect(str(tmp_path / "depot.db"), read_only=True)
        keys = [r[0] for r in conn.execute("SELECT key FROM depot_kv").fetchall()]
        conn.close()
        assert keys == ["bp1:watermark"]

        depot, depots = preview_depots(cfg, "bp1")
        assert depot.get("watermark", "MISS") == "2024-01-01"
        assert depots["default"].get("watermark", "MISS") == "2024-01-01"

    def test_different_blueprint_id_is_isolated(self, tmp_path):
        from aqueduct.depot.depot import DepotStore, preview_depots
        from aqueduct.stores import get_stores

        cfg = self._cfg(tmp_path)
        run_bundle = get_stores(cfg, blueprint_id="bp1")
        DepotStore(backend=run_bundle.depot).put("watermark", "2024-01-01")

        depot, _ = preview_depots(cfg, "bp2")
        assert depot.get("watermark", "MISS") == "MISS"

    def test_missing_key_returns_default_without_crashing(self, tmp_path):
        from aqueduct.depot.depot import preview_depots

        cfg = self._cfg(tmp_path)
        depot, _ = preview_depots(cfg, "bp1")
        assert depot.get("never_written", "fallback") == "fallback"

    def test_returned_depots_are_read_only_put_raises(self, tmp_path):
        """A preview must never write to the depot — `put()` fails loudly
        rather than silently no-opping."""
        from aqueduct.depot.depot import preview_depots
        from aqueduct.stores.base import StoreConnectionError

        cfg = self._cfg(tmp_path)
        depot, depots = preview_depots(cfg, "bp1")
        with pytest.raises(StoreConnectionError, match="read-only"):
            depot.put("k", "v")
        with pytest.raises(StoreConnectionError, match="read-only"):
            depots["default"].put("k", "v")

    def test_no_blueprint_id_uses_raw_keys(self, tmp_path):
        """`blueprint_id=None` (some non-run preview context) reads raw,
        unprefixed keys — mirrors `get_stores(cfg, blueprint_id=None)`."""
        from aqueduct.depot.depot import DepotStore, preview_depots
        from aqueduct.stores import get_stores

        cfg = self._cfg(tmp_path)
        run_bundle = get_stores(cfg, blueprint_id=None)
        DepotStore(backend=run_bundle.depot).put("raw_key", "raw_value")

        depot, _ = preview_depots(cfg, None)
        assert depot.get("raw_key", "MISS") == "raw_value"


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
