import uuid
import pytest
from tests.conftest import (
    _pg_dsn, _pg_is_reachable, _redis_url, _redis_is_reachable
)

@pytest.fixture(params=[
    "duckdb",
    pytest.param("postgres", marks=pytest.mark.integration)
])
def obs_store(request, tmp_path):
    backend = request.param
    if backend == "duckdb":
        from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
        store_path = tmp_path / "observability.db"
        yield DuckDBObservabilityStore(store_path)
    elif backend == "postgres":
        if not _pg_is_reachable():
            pytest.skip("Postgres not reachable (set AQ_PG_DSN)")
        
        dsn = _pg_dsn()
        from aqueduct.stores.postgres import PostgresObservabilityStore
        schema_name = f"obs_test_{uuid.uuid4().hex[:8]}"
        store = PostgresObservabilityStore(dsn)
        store._SCHEMA = schema_name
        yield store
        # Teardown
        import psycopg2
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        conn.close()

@pytest.fixture(params=[
    "duckdb",
    pytest.param("postgres", marks=pytest.mark.integration)
])
def observability_store(request, tmp_path):
    backend = request.param
    if backend == "duckdb":
        from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
        store_path = tmp_path / "observability.db"
        yield DuckDBObservabilityStore(store_path)
    elif backend == "postgres":
        if not _pg_is_reachable():
            pytest.skip("Postgres not reachable (set AQ_PG_DSN)")
            
        dsn = _pg_dsn()
        from aqueduct.stores.postgres import PostgresObservabilityStore
        schema_name = f"obs_test_{uuid.uuid4().hex[:8]}"
        store = PostgresObservabilityStore(dsn)
        store._SCHEMA = schema_name
        yield store
        # Teardown
        import psycopg2
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        conn.close()

class PrefixRedisDepotStore:
    def __init__(self, store, prefix):
        self._store = store
        self._prefix = prefix
    @property
    def backend(self): return self._store.backend
    @property
    def location_label(self): return self._store.location_label
    def kv_get(self, key, default=""): return self._store.kv_get(self._prefix + key, default)
    def kv_put(self, key, value): self._store.kv_put(self._prefix + key, value)
    def kv_delete(self, key): self._store.kv_delete(self._prefix + key)
    def connect(self): return self._store.connect()

@pytest.fixture(params=[
    "duckdb",
    pytest.param("postgres", marks=pytest.mark.integration),
    pytest.param("redis", marks=pytest.mark.integration)
])
def depot_store(request, tmp_path):
    backend = request.param
    if backend == "duckdb":
        from aqueduct.stores.duckdb_ import DuckDBDepotStore
        store_path = tmp_path / "depot.db"
        yield DuckDBDepotStore(store_path)
    elif backend == "postgres":
        if not _pg_is_reachable():
            pytest.skip("Postgres not reachable (set AQ_PG_DSN)")
            
        dsn = _pg_dsn()
        from aqueduct.stores.postgres import PostgresDepotStore
        schema_name = f"depot_test_{uuid.uuid4().hex[:8]}"
        store = PostgresDepotStore(dsn)
        store._SCHEMA = schema_name
        yield store
        # Teardown
        import psycopg2
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        conn.close()
    elif backend == "redis":
        if not _redis_is_reachable():
            pytest.skip("Redis not reachable (set AQ_REDIS_URL)")
            
        url = _redis_url()
        import redis
        client = redis.Redis.from_url(url, decode_responses=True)
            
        from aqueduct.stores.redis_ import RedisDepotStore
        store = RedisDepotStore(url)
        prefix = f"test_{uuid.uuid4().hex[:8]}_"
        yield PrefixRedisDepotStore(store, prefix)
        # Teardown
        for key in client.scan_iter(f"{prefix}*"):
            client.delete(key)
