import pytest

from aqueduct.config import AqueductConfig
from aqueduct.stores.base import BackendUnsupportedError


def test_depot_store_kv_roundtrip(depot_store):
    # kv_get default-value-on-miss
    assert depot_store.kv_get("missing_key", default="fallback") == "fallback"

    # kv_put / kv_get
    depot_store.kv_put("my_key", "my_value")
    assert depot_store.kv_get("my_key", default="fail") == "my_value"

    # kv_delete
    depot_store.kv_delete("my_key")
    assert depot_store.kv_get("my_key", default="fallback") == "fallback"

    # Verify kv_get missing doesn't raise
    assert depot_store.kv_get("another_missing", default="x") == "x"


def test_depot_store_connect_idempotency(depot_store):
    if depot_store.backend == "redis":
        with pytest.raises(BackendUnsupportedError):
            with depot_store.connect():
                pass
    else:
        with depot_store.connect():
            pass
        with depot_store.connect():
            pass


def test_depot_store_location_label(depot_store):
    label = depot_store.location_label
    assert isinstance(label, str)
    assert len(label) > 0
    if depot_store.backend in ("postgres", "redis"):
        # password must be redacted: userinfo (between // and @) carries no user:pass
        if "@" in label:
            userinfo = label.split("//", 1)[1].split("@", 1)[0]
            assert ":" not in userinfo


def test_redis_depot_ok():
    cfg = AqueductConfig(
        **{
            "stores": {
                "observability": {"backend": "duckdb", "path": "obs_base"},
                "depots": {"default": {"backend": "redis", "path": "redis://localhost:6379/15"}},
            }
        }
    )
    assert cfg.stores.default_depot().backend == "redis"


def test_depot_per_blueprint_isolation_and_shared(tmp_path):
    """Default mount keys are blueprint-prefixed (isolated); shared mounts are raw."""
    import duckdb

    from aqueduct.config import AqueductConfig
    from aqueduct.depot.depot import DepotStore
    from aqueduct.stores import get_stores

    cfg = AqueductConfig(
        **{
            "stores": {
                "depots": {
                    "default": {"backend": "duckdb", "path": f"{tmp_path}/depot.db"},
                    "fleet": {"backend": "duckdb", "path": f"{tmp_path}/fleet.db", "shared": True},
                }
            }
        }
    )
    b1 = get_stores(cfg, blueprint_id="bp1")
    DepotStore(backend=b1.depot).put("wm", "10")  # isolated → bp1:wm
    DepotStore(backend=b1.depots["fleet"]).put("g", "5")  # shared → raw g

    def raw_keys(p):
        c = duckdb.connect(p, read_only=True)
        out = sorted(r[0] for r in c.execute("SELECT key FROM depot_kv").fetchall())
        c.close()
        return out

    assert raw_keys(f"{tmp_path}/depot.db") == ["bp1:wm"]
    assert raw_keys(f"{tmp_path}/fleet.db") == ["g"]

    # A different blueprint cannot see bp1's isolated key.
    b2 = get_stores(cfg, blueprint_id="bp2")
    assert DepotStore(backend=b2.depot).get("wm", "MISS") == "MISS"
    # But shares the fleet mount.
    assert DepotStore(backend=b2.depots["fleet"]).get("g", "MISS") == "5"


def test_aq_depot_named_dispatch(tmp_path):
    """@aq.depot.<name>.get resolves a named mount; unknown name errors."""
    import pytest

    from aqueduct.compiler.runtime import AqFunctions, resolve_tier1_str
    from aqueduct.config import AqueductConfig
    from aqueduct.depot.depot import DepotStore
    from aqueduct.stores import get_stores

    cfg = AqueductConfig(
        **{
            "stores": {
                "depots": {
                    "default": {"backend": "duckdb", "path": f"{tmp_path}/depot.db"},
                    "fleet": {"backend": "duckdb", "path": f"{tmp_path}/fleet.db", "shared": True},
                }
            }
        }
    )
    b = get_stores(cfg, blueprint_id="bp1")
    DepotStore(backend=b.depots["fleet"]).put("g", "5")
    reg = AqFunctions(
        depots={n: DepotStore(backend=s) for n, s in b.depots.items()}, blueprint_id="bp1"
    )
    assert resolve_tier1_str("@aq.depot.fleet.get('g')", reg) == "5"
    from aqueduct.errors import CompileError

    with pytest.raises(CompileError, match="no depot mount named 'nope'"):
        resolve_tier1_str("@aq.depot.nope.get('x')", reg)


class TestDuckDBReadOnly:
    """`_DuckDBRelational(read_only=True)` — the preview-facing read-only pathway.

    `kv_get`/`kv_delete`'s file-existence guard (`_RelationalDepotMixin`,
    ``aqueduct/stores/base.py``) must still fire BEFORE ever calling
    ``connect()`` on a missing file — a read-only ``duckdb.connect()`` raises
    on a nonexistent path, so if the guard were bypassed a preview against an
    unwritten depot would crash instead of returning the default.
    """

    def test_kv_get_missing_file_returns_default_without_connecting(self, tmp_path):
        from aqueduct.stores.duckdb_ import DuckDBDepotStore

        store = DuckDBDepotStore(tmp_path / "nope.db", read_only=True)
        assert not (tmp_path / "nope.db").exists()
        assert store.kv_get("k", default="fallback") == "fallback"
        # The guard must never have created the file (no writer ever ran).
        assert not (tmp_path / "nope.db").exists()

    def test_kv_get_existing_file_reads_real_value(self, tmp_path):
        from aqueduct.stores.duckdb_ import DuckDBDepotStore

        db_path = tmp_path / "depot.db"
        writer = DuckDBDepotStore(db_path)
        writer.kv_put("k", "real-value")

        reader = DuckDBDepotStore(db_path, read_only=True)
        assert reader.kv_get("k", default="fallback") == "real-value"

    def test_kv_put_on_read_only_store_raises(self, tmp_path):
        from aqueduct.stores.base import StoreConnectionError
        from aqueduct.stores.duckdb_ import DuckDBDepotStore

        db_path = tmp_path / "depot.db"
        DuckDBDepotStore(db_path).kv_put("k", "v")  # seed the file

        reader = DuckDBDepotStore(db_path, read_only=True)
        with pytest.raises(StoreConnectionError, match="read-only"):
            reader.kv_put("k", "new-value")
        # Confirm no silent no-op: value is unchanged.
        assert DuckDBDepotStore(db_path).kv_get("k") == "v"

    def test_kv_delete_on_read_only_store_raises(self, tmp_path):
        from aqueduct.stores.base import StoreConnectionError
        from aqueduct.stores.duckdb_ import DuckDBDepotStore

        db_path = tmp_path / "depot.db"
        DuckDBDepotStore(db_path).kv_put("k", "v")

        reader = DuckDBDepotStore(db_path, read_only=True)
        with pytest.raises(StoreConnectionError, match="read-only"):
            reader.kv_delete("k")
        assert DuckDBDepotStore(db_path).kv_get("k") == "v"

    def test_kv_delete_on_read_only_store_raises_even_for_missing_file(self, tmp_path):
        """The read-only guard fires before the file-existence check — a
        read-only store refuses a delete attempt outright rather than
        silently no-opping, regardless of whether the file exists."""
        from aqueduct.stores.base import StoreConnectionError
        from aqueduct.stores.duckdb_ import DuckDBDepotStore

        reader = DuckDBDepotStore(tmp_path / "nope.db", read_only=True)
        with pytest.raises(StoreConnectionError, match="read-only"):
            reader.kv_delete("k")

    def test_read_only_defaults_false(self, tmp_path):
        """Existing (non-preview) construction is unaffected — plain writer
        behaviour by default."""
        from aqueduct.stores.duckdb_ import DuckDBDepotStore

        store = DuckDBDepotStore(tmp_path / "depot.db")
        assert store._read_only is False
        store.kv_put("k", "v")  # must not raise
        assert store.kv_get("k") == "v"
