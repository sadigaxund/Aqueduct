import sys
import pytest
from aqueduct.stores.postgres import PostgresObsStore

def test_postgres_pool_caching():
    dsn = "postgresql://aq:aq@localhost:5432/aq_test"
    store1 = PostgresObsStore(dsn)
    store2 = PostgresObsStore(dsn)
    
    # We don't actually connect to db, just check the pool cache
    from aqueduct.stores.postgres import _get_pool
    # Create dummy pool to simulate caching
    pool1 = _get_pool(dsn)
    pool2 = _get_pool(dsn)
    
    assert pool1 is pool2

def test_postgres_missing_psycopg2(monkeypatch):
    monkeypatch.setitem(sys.modules, "psycopg2", None)
    monkeypatch.setitem(sys.modules, "psycopg2.pool", None)
    
    dsn = "postgresql://aq:aq@localhost:5432/aq_test_missing"
    store = PostgresObsStore(dsn)
    
    with pytest.raises(ImportError, match="pip install psycopg2-binary"):
        store.connect()
