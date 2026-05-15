import os
import uuid
import pytest
import contextlib

@pytest.fixture(params=[
    "duckdb",
    pytest.param("postgres", marks=pytest.mark.integration)
])
def obs_store(request, tmp_path):
    backend = request.param
    if backend == "duckdb":
        from aqueduct.stores.duckdb_ import DuckDBObsStore
        store_path = tmp_path / "obs.db"
        yield DuckDBObsStore(store_path)
    elif backend == "postgres":
        dsn = os.environ.get("AQ_PG_DSN")
        if not dsn:
            pytest.skip("AQ_PG_DSN not set")
        from aqueduct.stores.postgres import PostgresObsStore
        schema_name = f"obs_test_{uuid.uuid4().hex[:8]}"
        store = PostgresObsStore(dsn)
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
def lineage_store(request, tmp_path):
    backend = request.param
    if backend == "duckdb":
        from aqueduct.stores.duckdb_ import DuckDBLineageStore
        store_path = tmp_path / "lineage.db"
        yield DuckDBLineageStore(store_path)
    elif backend == "postgres":
        dsn = os.environ.get("AQ_PG_DSN")
        if not dsn:
            pytest.skip("AQ_PG_DSN not set")
        from aqueduct.stores.postgres import PostgresLineageStore
        schema_name = f"lineage_test_{uuid.uuid4().hex[:8]}"
        store = PostgresLineageStore(dsn)
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
        dsn = os.environ.get("AQ_PG_DSN")
        if not dsn:
            pytest.skip("AQ_PG_DSN not set")
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
        url = os.environ.get("AQ_REDIS_URL", "redis://localhost:6379/15")
        try:
            import redis
            client = redis.Redis.from_url(url, decode_responses=True)
            client.ping()
        except Exception:
            pytest.skip("Redis not reachable")
            
        from aqueduct.stores.redis_ import RedisDepotStore
        store = RedisDepotStore(url)
        prefix = f"test_{uuid.uuid4().hex[:8]}_"
        yield PrefixRedisDepotStore(store, prefix)
        # Teardown
        for key in client.scan_iter(f"{prefix}*"):
            client.delete(key)
