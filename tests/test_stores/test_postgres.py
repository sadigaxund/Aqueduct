import sys
import pytest
from tests.conftest import requires_postgres, _pg_dsn
from aqueduct.stores.postgres import PostgresObservabilityStore

@requires_postgres
def test_postgres_pool_caching():
    dsn = _pg_dsn()
    store1 = PostgresObservabilityStore(dsn)
    store2 = PostgresObservabilityStore(dsn)
    
    # We don't actually connect to db, just check the pool cache
    from aqueduct.stores.postgres import _get_pool
    # Create dummy pool to simulate caching
    pool1 = _get_pool(dsn)
    pool2 = _get_pool(dsn)
    
    assert pool1 is pool2

def test_postgres_missing_psycopg2(monkeypatch):
    # Clear the connection pool cache to force an import attempt
    from aqueduct.stores.postgres import _POOLS
    _POOLS.clear()
    
    monkeypatch.setitem(sys.modules, "psycopg2", None)
    monkeypatch.setitem(sys.modules, "psycopg2.pool", None)
    
    dsn = "postgresql://aq:aq@localhost:5432/aq_test_missing"
    store = PostgresObservabilityStore(dsn)
    
    with pytest.raises(ImportError, match="pip install aqueduct-core\[postgres\]"):
        with store.connect():
            pass
