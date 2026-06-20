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
    # Phase 38: lineage is merged into observability — the lineage store aliases
    # it (no separate lineage.db). See get_stores.
    assert bundle.lineage is bundle.observability
    assert isinstance(bundle.depot, DuckDBDepotStore)

def test_get_stores_factory_mixed():
    cfg = AqueductConfig(**{
        "stores": {
            "observability": {"backend": "duckdb", "path": "observability.db"},
            "lineage": {"backend": "duckdb", "path": "lineage.db"},
            "depot": {"backend": "redis", "path": "redis://localhost:6379/0"}
        }
    })
    bundle = get_stores(cfg)
    assert isinstance(bundle.observability, DuckDBObservabilityStore)
    assert bundle.lineage is bundle.observability  # aliased (Phase 38)
    assert isinstance(bundle.depot, RedisDepotStore)

def test_get_stores_factory_postgres():
    cfg = AqueductConfig(**{
        "stores": {
            "observability": {"backend": "postgres", "path": "postgresql://usr:pass@localhost:5432/aq"},
            "lineage": {"backend": "postgres", "path": "postgresql://usr:pass@localhost:5432/aq"},
            "depot": {"backend": "postgres", "path": "postgresql://usr:pass@localhost:5432/aq"}
        }
    })
    bundle = get_stores(cfg)
    assert isinstance(bundle.observability, PostgresObservabilityStore)
    assert bundle.lineage is bundle.observability  # aliased (Phase 38)
    assert isinstance(bundle.depot, PostgresDepotStore)
