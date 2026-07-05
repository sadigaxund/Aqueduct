import pytest
from aqueduct.config import AqueductConfig
from aqueduct.stores.base import get_stores, StoreBundle
from aqueduct.stores.duckdb_ import DuckDBObservabilityStore, DuckDBDepotStore
from aqueduct.stores.postgres import PostgresObservabilityStore, PostgresDepotStore
from aqueduct.stores.redis_ import RedisDepotStore

def test_get_stores_factory_duckdb():
    cfg = AqueductConfig()
    bundle = get_stores(cfg)
    assert isinstance(bundle, StoreBundle)
    assert isinstance(bundle.observability, DuckDBObservabilityStore)
    # Phase 38/Phase ∞: lineage lives in the observability store — no separate store.
    assert bundle.observability is not None
    assert isinstance(bundle.depot, DuckDBDepotStore)

def test_get_stores_factory_mixed():
    cfg = AqueductConfig(**{
        "stores": {
            "observability": {"backend": "duckdb", "path": "obs_base"},
            "depots": {"default": {"backend": "redis", "path": "redis://localhost:6379/0"}}
        }
    })
    bundle = get_stores(cfg)
    assert isinstance(bundle.observability, DuckDBObservabilityStore)
    assert bundle.observability is not None  # lineage merged into observability (Phase ∞)
    assert isinstance(bundle.depot, RedisDepotStore)

def test_get_stores_factory_postgres():
    cfg = AqueductConfig(**{
        "stores": {
            "observability": {"backend": "postgres", "path": "postgresql://usr:pass@localhost:5432/aq"},
            "depots": {"default": {"backend": "postgres", "path": "postgresql://usr:pass@localhost:5432/aq"}}
        }
    })
    bundle = get_stores(cfg)
    assert isinstance(bundle.observability, PostgresObservabilityStore)
    assert bundle.observability is not None  # lineage merged into observability (Phase ∞)
    assert isinstance(bundle.depot, PostgresDepotStore)
