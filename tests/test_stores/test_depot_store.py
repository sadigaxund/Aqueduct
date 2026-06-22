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
    cfg = AqueductConfig(**{
        "stores": {
            "observability": {"backend": "duckdb", "path": "observability.db"},
            "depots": {"default": {"backend": "redis", "path": "redis://localhost:6379/15"}}
        }
    })
    assert cfg.stores.depot.backend == "redis"


def test_depot_per_blueprint_isolation_and_shared(tmp_path):
    """Default mount keys are blueprint-prefixed (isolated); shared mounts are raw."""
    import duckdb
    from aqueduct.config import AqueductConfig
    from aqueduct.stores import get_stores
    from aqueduct.depot.depot import DepotStore
    cfg = AqueductConfig(**{"stores": {"depots": {
        "default": {"backend": "duckdb", "path": f"{tmp_path}/depot.db"},
        "fleet":   {"backend": "duckdb", "path": f"{tmp_path}/fleet.db", "shared": True},
    }}})
    b1 = get_stores(cfg, blueprint_id="bp1")
    DepotStore(backend=b1.depot).put("wm", "10")           # isolated → bp1:wm
    DepotStore(backend=b1.depots["fleet"]).put("g", "5")   # shared → raw g

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
    from aqueduct.config import AqueductConfig
    from aqueduct.stores import get_stores
    from aqueduct.depot.depot import DepotStore
    from aqueduct.compiler.runtime import AqFunctions, resolve_tier1_str
    cfg = AqueductConfig(**{"stores": {"depots": {
        "default": {"backend": "duckdb", "path": f"{tmp_path}/depot.db"},
        "fleet":   {"backend": "duckdb", "path": f"{tmp_path}/fleet.db", "shared": True},
    }}})
    b = get_stores(cfg, blueprint_id="bp1")
    DepotStore(backend=b.depots["fleet"]).put("g", "5")
    reg = AqFunctions(depots={n: DepotStore(backend=s) for n, s in b.depots.items()},
                      blueprint_id="bp1")
    assert resolve_tier1_str("@aq.depot.fleet.get('g')", reg) == "5"
    with pytest.raises(RuntimeError, match="no depot mount named 'nope'"):
        resolve_tier1_str("@aq.depot.nope.get('x')", reg)
