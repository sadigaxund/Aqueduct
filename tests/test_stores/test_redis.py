import sys
import pytest
from tests.conftest import requires_redis, _redis_url
from aqueduct.stores.redis_ import RedisDepotStore

@requires_redis
def test_redis_client_caching():
    url = _redis_url()
    from aqueduct.stores.redis_ import _get_client
    # We can just check the cache
    client1 = _get_client(url)
    client2 = _get_client(url)
    
    assert client1 is client2

def test_redis_missing_driver(monkeypatch):
    # Clear the client cache to force an import attempt
    from aqueduct.stores.redis_ import _CLIENTS
    _CLIENTS.clear()
    
    monkeypatch.setitem(sys.modules, "redis", None)
    
    url = _redis_url()
    store = RedisDepotStore(url)
    
    with pytest.raises(ImportError, match="pip install aqueduct-core\[redis\]"):
        store.kv_put("test", "val")
