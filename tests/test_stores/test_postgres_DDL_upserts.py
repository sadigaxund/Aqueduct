"""Tests for Postgres DDL, upserts and portability across backends."""

from __future__ import annotations

import uuid
from pathlib import Path
from unittest.mock import patch

import pytest

from aqueduct.compiler.models import Manifest
from aqueduct.compiler.provenance import ProvenanceMap
from aqueduct.parser.models import Edge, Module, RetryPolicy
from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
from aqueduct.stores.postgres import PostgresObservabilityStore
from aqueduct.surveyor.surveyor import _DDL, _EXPLAIN_SNAPSHOT_DDL, _SIGNAL_OVERRIDES_DDL, Surveyor
from tests.conftest import _pg_dsn, requires_postgres, requires_redis

# ── Test 1: DDL contains DOUBLE PRECISION and creates clean on both ──────────

def test_surveyor_ddl_duckdb(tmp_path):
    """Verify surveyor DDL uses DOUBLE PRECISION and creates clean on DuckDB."""
    # DOUBLE PRECISION is portable and works fine on DuckDB
    assert "DOUBLE PRECISION" in _DDL
    
    db_file = tmp_path / "obs.db"
    store = DuckDBObservabilityStore(db_file)
    with store.connect() as cur:
        cur.execute(_DDL)
        cur.execute(_SIGNAL_OVERRIDES_DDL)
        cur.execute(_EXPLAIN_SNAPSHOT_DDL)
        
        # Verify table exists
        cur.execute("SELECT COUNT(*) FROM healing_outcomes")
        assert cur.fetchone()[0] == 0


@requires_postgres
def test_surveyor_ddl_postgres():
    """Verify surveyor DDL creates clean on Postgres."""
    dsn = _pg_dsn()
    schema = f"obs_ddl_{uuid.uuid4().hex[:8]}"
    store = PostgresObservabilityStore(dsn)
    store._SCHEMA = schema
    
    import psycopg2
    try:
        with store.connect() as cur:
            cur.execute(_DDL)
            cur.execute(_SIGNAL_OVERRIDES_DDL)
            cur.execute(_EXPLAIN_SNAPSHOT_DDL)
            
            cur.execute("SELECT COUNT(*) FROM healing_outcomes")
            assert cur.fetchone()[0] == 0
    finally:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        with conn.cursor() as c:
            c.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


# ── Test 2: ON CONFLICT Upserts work on both backends ──────────────────────────

def test_surveyor_upserts_duckdb(tmp_path):
    """Verify ON CONFLICT (pk) DO UPDATE works on DuckDB on re-running same run_id."""
    manifest = Manifest(
        blueprint_id="bp_test", name="Test", description="", aqueduct_version="1.0",
        context={}, spark_config={}, retry_policy=RetryPolicy(), agent=None,
        udf_registry={}, macros={}, checkpoint=False,
        provenance_map=ProvenanceMap(blueprint_id="bp_test", blueprint_path="", modules={}, context={}),
        inputs_fingerprint={},
        modules=(), edges=()
    )
    surveyor = Surveyor(manifest, store_dir=tmp_path, engine="spark")
    run_id = uuid.uuid4().hex
    
    # 1. First run records 'running'
    surveyor.start(run_id)
    with surveyor._observability.connect() as cur:
        cur.execute("SELECT status FROM run_records WHERE run_id = ?", [run_id])
        assert cur.fetchone()[0] == "running"
        
    # 2. Second run of same run_id (ON CONFLICT) updates it correctly, no PK violation
    surveyor.start(run_id)
    with surveyor._observability.connect() as cur:
        cur.execute("SELECT status FROM run_records WHERE run_id = ?", [run_id])
        assert cur.fetchone()[0] == "running"
    
    surveyor.stop()


@requires_postgres
def test_surveyor_upserts_postgres():
    """Verify ON CONFLICT (pk) DO UPDATE works on Postgres on re-running same run_id."""
    dsn = _pg_dsn()
    schema = f"obs_upsert_{uuid.uuid4().hex[:8]}"
    
    import psycopg2

    from aqueduct.stores.base import StoreBundle
    from aqueduct.stores.postgres import PostgresObservabilityStore
    
    manifest = Manifest(
        blueprint_id="bp_test", name="Test", description="", aqueduct_version="1.0",
        context={}, spark_config={}, retry_policy=RetryPolicy(), agent=None,
        udf_registry={}, macros={}, checkpoint=False,
        provenance_map=ProvenanceMap(blueprint_id="bp_test", blueprint_path="", modules={}, context={}),
        inputs_fingerprint={},
        modules=(), edges=()
    )
    
    obs_store = PostgresObservabilityStore(dsn)
    obs_store._SCHEMA = schema
    
    # Simple mock bundle
    class DummyDepot:
        backend = "postgres"
        location_label = "dummy"
    
    bundle = StoreBundle(observability=obs_store, depot=DummyDepot())
    surveyor = Surveyor(manifest, store_dir=Path("/tmp"), stores=bundle, engine="spark")
    run_id = uuid.uuid4().hex
    
    try:
        # 1. First run records 'running'
        surveyor.start(run_id)
        with surveyor._observability.connect() as cur:
            cur.execute("SELECT status FROM run_records WHERE run_id = ?", [run_id])
            assert cur.fetchone()[0] == "running"
            
        # 2. Second run of same run_id (ON CONFLICT) updates it correctly, no PK violation
        surveyor.start(run_id)
        with surveyor._observability.connect() as cur:
            cur.execute("SELECT status FROM run_records WHERE run_id = ?", [run_id])
            assert cur.fetchone()[0] == "running"
        
        surveyor.stop()
    finally:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        with conn.cursor() as c:
            c.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


# ── Test 3: Phase 33 matrix: real blueprint run w/ Postgres + Redis ───────────

@requires_postgres
@requires_redis
@pytest.mark.spark
def test_phase33_matrix_postgres_redis(spark, tmp_path):
    """Verify that a real blueprint run with Postgres relational stores + Redis depot completes successfully."""
    from aqueduct.executor.spark.executor import execute
    from aqueduct.stores.base import StoreBundle
    from aqueduct.stores.postgres import PostgresObservabilityStore
    from aqueduct.stores.redis_ import RedisDepotStore
    from tests.conftest import _redis_url
    
    dsn = _pg_dsn()
    redis_conn_url = _redis_url()
    
    obs_schema = f"obs_p33_{uuid.uuid4().hex[:8]}"
    
    obs_store = PostgresObservabilityStore(dsn)
    obs_store._SCHEMA = obs_schema
    depot_store = RedisDepotStore(redis_conn_url)
    
    in_path = str(tmp_path / "in_p33.parquet")
    out_path = str(tmp_path / "out_p33")
    spark.range(5).selectExpr("id", "id * 2 AS dbl").write.parquet(in_path)
    
    manifest = Manifest(
        blueprint_id="bp_p33", name="P33", description="", aqueduct_version="1.0",
        context={}, spark_config={}, retry_policy=RetryPolicy(), agent=None,
        udf_registry={}, macros={}, checkpoint=False,
        provenance_map=ProvenanceMap(blueprint_id="bp_p33", blueprint_path="", modules={}, context={}),
        inputs_fingerprint={},
        modules=(
            Module(id="src", type="Ingress", label="In",
                    config={"format": "parquet", "path": in_path}),
            Module(id="ch", type="Channel", label="Ch",
                    config={"op": "sql", "query": "SELECT id, dbl FROM src"}),
            Module(id="pr", type="Probe", label="Pr", attach_to="ch",
                    config={"signals": [{"type": "threshold", "expr": "COUNT(*) > 0"}]}),
            Module(id="out", type="Egress", label="Out",
                    config={"format": "parquet", "path": out_path, "mode": "overwrite"}),
        ),
        edges=(
            Edge(from_id="src", to_id="ch", port="main"),
            Edge(from_id="ch", to_id="out", port="main"),
        ),
    )
    
    import psycopg2
    try:
        # Patch get_stores to return our bundle
        bundle = StoreBundle(observability=obs_store, depot=depot_store)
        surveyor = Surveyor(manifest, store_dir=tmp_path, stores=bundle, engine="spark")
        run_id = f"run_{uuid.uuid4().hex[:8]}"
        
        surveyor.start(run_id)
        with patch("aqueduct.stores.base.get_stores", return_value=bundle):
            result = execute(
                manifest, spark, run_id=run_id, store_dir=tmp_path,
                surveyor=surveyor, depot=depot_store,
                observability_store=obs_store,
            )
            assert result.status == "success"
        surveyor.record(result)
        surveyor.stop()
        
        # Verify DDL + inserts portable and written successfully
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        with conn.cursor() as cur:
            # Observability
            cur.execute(f'SELECT COUNT(*) FROM "{obs_schema}".run_records')
            assert cur.fetchone()[0] >= 1
            cur.execute(f'SELECT COUNT(*) FROM "{obs_schema}".probe_signals')
            assert cur.fetchone()[0] >= 1
            cur.execute(f'SELECT COUNT(*) FROM "{obs_schema}".module_metrics')
            assert cur.fetchone()[0] >= 1
            
            # Lineage — column_lineage DDL lives in the surveyor's observability
            # DDL (surveyor.py _DDL), so the table is in the observability schema,
            # not the lineage store schema.
            cur.execute(f'SELECT COUNT(*) FROM "{obs_schema}".column_lineage')
            assert cur.fetchone()[0] >= 1
            
        conn.close()
    finally:
        # Clean up
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{obs_schema}" CASCADE')
        conn.close()
        
        # Prune redis keys
        import redis
        client = redis.Redis.from_url(redis_conn_url)
        client.flushdb()
