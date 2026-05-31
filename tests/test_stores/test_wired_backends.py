"""Phase 28 wired call-site integration tests — Redis/Postgres backends.

These exercise the real wiring: DepotStore watermark round-trip, write_lineage()
against the configured store, full execute() persistence, and `aqueduct signal`.

The parametrized `depot_store` / `lineage_store` fixtures (tests/test_stores/
conftest.py) always run the DuckDB backend and additionally run Postgres/Redis
when AQ_PG_DSN / AQ_REDIS_URL point at a reachable service (otherwise skipped).
"""

from __future__ import annotations

import uuid

import pytest

from aqueduct.parser.models import Module, Edge


# ── 1838: DepotStore round-trips a watermark via kv_put / kv_get ──────────────

def test_depot_store_watermark_roundtrip(depot_store):
    """DepotStore (DuckDB/Postgres/Redis) round-trips a watermark value."""
    wm_key = f"bp1:ch_inc:_watermark"
    wm_value = "2024-06-01 12:00:00"

    # absent → default
    assert depot_store.kv_get(wm_key, default="") == ""

    depot_store.kv_put(wm_key, wm_value)
    assert depot_store.kv_get(wm_key, default="") == wm_value

    # overwrite advances the watermark
    depot_store.kv_put(wm_key, "2024-07-15 08:30:00")
    assert depot_store.kv_get(wm_key, default="") == "2024-07-15 08:30:00"


# ── 1839: write_lineage() writes rows into column_lineage ─────────────────────

def test_write_lineage_into_configured_store(observability_store, tmp_path):
    """write_lineage(..., observability_store=...) inserts column_lineage rows.

    Phase 38 merged lineage into the observability store. The ``column_lineage``
    DDL must exist before ``write_lineage()`` can INSERT.
    """
    # Phase 38: column_lineage lives in observability. Run the surveyor DDL
    # to ensure the table exists before write_lineage() attempts to INSERT.
    from aqueduct.surveyor.surveyor import _DDL
    with observability_store.connect() as cur:
        cur.execute(_DDL)

    from aqueduct.compiler.lineage import write_lineage

    modules = (
        Module(id="src", type="Ingress", label="Src",
                config={"format": "parquet", "path": "/tmp/x"}),
        Module(id="ch", type="Channel", label="Ch", config={
            "op": "sql",
            "query": "SELECT id AS user_id FROM src",
        }),
    )
    edges = (Edge(from_id="src", to_id="ch", port="main"),)

    run_id = uuid.uuid4().hex
    write_lineage("bp_lineage", run_id, modules, edges,
                  observability_store=observability_store)

    with observability_store.connect() as cur:
        rows = cur.execute(
            "SELECT channel_id, output_column, source_column "
            "FROM column_lineage WHERE run_id = ?",
            [run_id],
        ).fetchall()

    assert len(rows) >= 1
    assert any(r[0] == "ch" and r[1] == "user_id" for r in rows)


# ── 1840: end-to-end execute() persistence into Postgres ──────────────────────

@pytest.mark.spark
@pytest.mark.integration
def test_execute_persists_into_postgres(spark, tmp_path):
    """execute(..., observability_store=pg) persists rows."""
    from tests.conftest import _pg_is_reachable, _pg_dsn
    if not _pg_is_reachable():
        pytest.skip("Postgres not reachable (set AQ_PG_DSN)")

    import psycopg2
    from aqueduct.stores.postgres import (
        PostgresObservabilityStore, PostgresDepotStore,
    )
    from aqueduct.stores.base import StoreBundle
    from aqueduct.surveyor.surveyor import Surveyor
    from aqueduct.executor.spark.executor import execute
    from aqueduct.compiler.models import Manifest
    from aqueduct.compiler.provenance import ProvenanceMap
    from aqueduct.parser.models import RetryPolicy

    dsn = _pg_dsn()
    obs_schema = f"obs_e2e_{uuid.uuid4().hex[:8]}"
    obs_store = PostgresObservabilityStore(dsn)
    obs_store._SCHEMA = obs_schema
    depot_store = PostgresDepotStore(dsn)
    depot_store._SCHEMA = obs_schema

    in_path = str(tmp_path / "in_e2e.parquet")
    out_path = str(tmp_path / "out_e2e")
    spark.range(5).selectExpr("id", "id * 2 AS dbl").write.parquet(in_path)

    manifest = Manifest(
        blueprint_id="bp_e2e", name="E2E", description="", aqueduct_version="1.0",
        context={}, spark_config={}, retry_policy=RetryPolicy(), agent=None,
        udf_registry={}, macros={}, checkpoint=False,
        provenance_map=ProvenanceMap(blueprint_id="bp_e2e", blueprint_path="", modules={}, context={}),
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

    bundle = StoreBundle(observability=obs_store, lineage=None, depot=depot_store)
    surveyor = Surveyor(manifest, store_dir=tmp_path, stores=bundle)
    run_id = f"run_{uuid.uuid4().hex[:8]}"

    try:
        surveyor.start(run_id)
        result = execute(
            manifest, spark, run_id=run_id, store_dir=tmp_path,
            surveyor=surveyor, depot=depot_store,
            observability_store=obs_store,
        )
        assert result.status == "success"
        surveyor.record(result)
        surveyor.stop()

        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{obs_schema}".run_records')
            assert cur.fetchone()[0] >= 1
            cur.execute(f'SELECT COUNT(*) FROM "{obs_schema}".module_metrics')
            assert cur.fetchone()[0] >= 1
            cur.execute(f'SELECT COUNT(*) FROM "{obs_schema}".column_lineage')
            assert cur.fetchone()[0] >= 1
            cur.execute(f'SELECT COUNT(*) FROM "{obs_schema}".probe_signals')
            assert cur.fetchone()[0] >= 1
        conn.close()
    finally:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{obs_schema}" CASCADE')
        conn.close()


# ── 1841: `aqueduct signal` against Postgres signal_overrides ─────────────────

@pytest.mark.integration
def test_signal_cli_postgres_roundtrip(tmp_path):
    """aqueduct signal <id> writes then reads observability.signal_overrides (Postgres)."""
    from tests.conftest import _pg_is_reachable, _pg_dsn
    if not _pg_is_reachable():
        pytest.skip("Postgres not reachable (set AQ_PG_DSN)")

    import psycopg2
    from click.testing import CliRunner
    from aqueduct.cli import cli

    dsn = _pg_dsn()
    schema = "observability"  # signal command targets the fixed obs schema

    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(
        "stores:\n"
        "  observability:\n"
        "    backend: postgres\n"
        f"    path: {dsn}\n"
        "  lineage:\n"
        "    backend: postgres\n"
        f"    path: {dsn}\n"
        "  depot:\n"
        "    backend: postgres\n"
        f"    path: {dsn}\n"
    )

    sig_id = f"probe:cli_test_{uuid.uuid4().hex[:8]}"
    runner = CliRunner()
    try:
        # Close the gate (writes a row into observability.signal_overrides)
        set_res = runner.invoke(
            cli,
            ["signal", sig_id, "--value", "false", "--config", str(cfg)],
        )
        assert set_res.exit_code == 0, set_res.output

        # Read it back (no --value/--error → status display)
        get_res = runner.invoke(
            cli,
            ["signal", sig_id, "--config", str(cfg)],
        )
        assert get_res.exit_code == 0, get_res.output
        assert sig_id in get_res.output
    finally:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                f'DELETE FROM "{schema}".signal_overrides WHERE signal_id = %s',
                [sig_id],
            )
        conn.close()
