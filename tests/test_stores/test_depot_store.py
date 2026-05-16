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
        assert ":" not in label.split("@")[0]  # rough check that password isn't there

def test_redis_depot_ok():
    cfg = AqueductConfig(**{
        "stores": {
            "observability": {"backend": "duckdb", "path": "observability.db"},
            "lineage": {"backend": "duckdb", "path": "lineage.db"},
            "depot": {"backend": "redis", "path": "redis://localhost:6379/15"}
        }
    })
    assert cfg.stores.depot.backend == "redis"
