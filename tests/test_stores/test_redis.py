import sys
import pytest
from aqueduct.stores.redis_ import RedisDepotStore

def test_redis_client_caching():
    url = "redis://localhost:6379/15"
    from aqueduct.stores.redis_ import _get_client
    # We can just check the cache
    client1 = _get_client(url)
    client2 = _get_client(url)
    
    assert client1 is client2

def test_redis_missing_driver(monkeypatch):
    monkeypatch.setitem(sys.modules, "redis", None)
    
    url = "redis://localhost:6379/15"
    store = RedisDepotStore(url)
    
    with pytest.raises(ImportError, match="pip install redis"):
        store.kv_get("test")
