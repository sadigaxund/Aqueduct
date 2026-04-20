"""Blueprint integration tests — full compile → execute cycle with real Spark.

Each test:
  1. Parses a blueprint YAML from tests/fixtures/blueprints/
  2. Compiles it to a Manifest
  3. Executes it against a local[*] SparkSession
  4. Asserts on ExecutionResult and optionally reads output data

These tests catch integration bugs that unit tests miss — routing logic,
frame_store key conventions, topo-sort ordering, Spark action semantics.

All I/O paths are injected via cli_overrides so blueprints stay static YAML.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from aqueduct.compiler.compiler import compile as compiler_compile
from aqueduct.executor.spark.executor import execute
from aqueduct.parser.parser import parse

BLUEPRINTS = Path(__file__).parent / "fixtures" / "blueprints"


def _run(blueprint_name: str, overrides: dict, spark: SparkSession, store_dir=None):
    """Parse, compile, and execute a blueprint fixture. Returns ExecutionResult."""
    bp_path = BLUEPRINTS / blueprint_name
    bp = parse(str(bp_path), cli_overrides=overrides)
    manifest = compiler_compile(bp, blueprint_path=bp_path)
    return execute(manifest, spark, store_dir=store_dir)


# ── Baseline ──────────────────────────────────────────────────────────────────

def test_linear_ingress_egress(spark: SparkSession, sample_data, tmp_path):
    """Ingress → Egress: all rows pass through unchanged."""
    out = tmp_path / "out"
    result = _run("bp_linear.yml", {
        "input_path": str(sample_data / "orders.parquet"),
        "output_path": str(out),
    }, spark)

    assert result.status == "success", result.module_results
    statuses = {r.module_id: r.status for r in result.module_results}
    assert statuses["src"] == "success"
    assert statuses["sink"] == "success"

    df = spark.read.parquet(str(out))
    assert df.count() == 10


# ── Channel ───────────────────────────────────────────────────────────────────

def test_channel_sql_filter(spark: SparkSession, sample_data, tmp_path):
    """Channel SQL filter: 1 null-amount row removed → 9 rows in output."""
    out = tmp_path / "out"
    result = _run("bp_channel_filter.yml", {
        "input_path": str(sample_data / "orders.parquet"),
        "output_path": str(out),
    }, spark)

    assert result.status == "success", result.module_results
    assert spark.read.parquet(str(out)).count() == 9


# ── Junction ──────────────────────────────────────────────────────────────────

def test_junction_conditional_split(spark: SparkSession, sample_data, tmp_path):
    """Junction splits by region: us_path gets 5 rows, eu_path gets 5 rows."""
    us_out = tmp_path / "us"
    eu_out = tmp_path / "eu"
    result = _run("bp_junction_conditional.yml", {
        "input_path": str(sample_data / "orders.parquet"),
        "us_path": str(us_out),
        "eu_path": str(eu_out),
    }, spark)

    assert result.status == "success", result.module_results

    us_df = spark.read.parquet(str(us_out))
    eu_df = spark.read.parquet(str(eu_out))
    assert us_df.count() == 5
    assert eu_df.count() == 5
    assert all(r["region"] == "US" for r in us_df.collect())
    assert all(r["region"] == "EU" for r in eu_df.collect())


# ── Funnel ────────────────────────────────────────────────────────────────────

def test_funnel_union_all(spark: SparkSession, sample_data, tmp_path):
    """Funnel union_all stacks two identical inputs: 10 + 10 = 20 rows."""
    out = tmp_path / "out"
    orders = str(sample_data / "orders.parquet")
    result = _run("bp_funnel_union.yml", {
        "input_a_path": orders,
        "input_b_path": orders,
        "output_path": str(out),
    }, spark)

    assert result.status == "success", result.module_results
    assert spark.read.parquet(str(out)).count() == 20


# ── Spillway ──────────────────────────────────────────────────────────────────

def test_spillway_error_routing(spark: SparkSession, sample_data, tmp_path):
    """Spillway: null-amount row routes to bad_sink; good rows go to good_sink."""
    good = tmp_path / "good"
    bad = tmp_path / "bad"
    result = _run("bp_spillway.yml", {
        "input_path": str(sample_data / "orders.parquet"),
        "good_path": str(good),
        "bad_path": str(bad),
    }, spark)

    assert result.status == "success", result.module_results

    good_df = spark.read.parquet(str(good))
    bad_df = spark.read.parquet(str(bad))

    assert good_df.count() == 9, "Good sink should have 9 non-null rows"
    assert bad_df.count() == 1, "Spillway sink should have 1 null-amount row"

    # Error rows must carry _aq_error_* metadata columns
    assert "_aq_error_module" in bad_df.columns
    assert "_aq_error_msg" in bad_df.columns
    assert "_aq_error_ts" in bad_df.columns

    # Good rows must NOT have error columns
    assert "_aq_error_module" not in good_df.columns


# ── Probe ─────────────────────────────────────────────────────────────────────

def test_probe_does_not_halt_pipeline(spark: SparkSession, sample_data, tmp_path):
    """Probe signals captured; pipeline completes successfully regardless."""
    out = tmp_path / "out"
    store = tmp_path / "signals"
    result = _run("bp_probe.yml", {
        "input_path": str(sample_data / "orders.parquet"),
        "output_path": str(out),
    }, spark, store_dir=store)

    assert result.status == "success", result.module_results

    statuses = {r.module_id: r.status for r in result.module_results}
    assert statuses.get("probe_src") == "success"
    assert statuses.get("sink") == "success"

    # Output rows unaffected by probe
    assert spark.read.parquet(str(out)).count() == 10

    # Signals DB written
    assert (store / "signals.db").exists()


# ── Regulator ─────────────────────────────────────────────────────────────────

def test_regulator_open_gate_passthrough(spark: SparkSession, sample_data, tmp_path):
    """Regulator with no surveyor: gate defaults open, all rows reach Egress."""
    out = tmp_path / "out"
    result = _run("bp_regulator_passthrough.yml", {
        "input_path": str(sample_data / "orders.parquet"),
        "output_path": str(out),
    }, spark)

    assert result.status == "success", result.module_results
    statuses = {r.module_id: r.status for r in result.module_results}
    assert statuses["gate"] == "success"
    assert statuses["sink"] == "success"
    assert spark.read.parquet(str(out)).count() == 10


def test_regulator_closed_gate_skips_downstream(spark: SparkSession, sample_data, tmp_path):
    """Regulator with closed gate (mock surveyor): Egress gets skipped."""
    class _ClosedSurveyor:
        def evaluate_regulator(self, module_id: str) -> bool:
            return False

    out = tmp_path / "out"
    bp_path = BLUEPRINTS / "bp_regulator_passthrough.yml"
    bp = parse(str(bp_path), cli_overrides={
        "input_path": str(sample_data / "orders.parquet"),
        "output_path": str(out),
    })
    manifest = compiler_compile(bp, blueprint_path=bp_path)
    result = execute(manifest, spark, surveyor=_ClosedSurveyor())

    assert result.status == "success"  # pipeline itself didn't error
    statuses = {r.module_id: r.status for r in result.module_results}
    assert statuses["gate"] == "skipped"
    assert statuses["sink"] == "skipped"

    # Output must not have been written
    assert not out.exists()


# ── Junction → Funnel → Channel regression ───────────────────────────────────

def test_junction_funnel_channel_pattern(spark: SparkSession, sample_data, tmp_path):
    """Regression: Junction branch edges must connect to Funnel, not Channel directly.

    Channel reads from Funnel output via __input__ alias.
    All rows (US + EU = 10) must appear in output with pipeline_tag column added.
    """
    out = tmp_path / "out"
    result = _run("bp_junction_funnel_channel.yml", {
        "input_path": str(sample_data / "orders.parquet"),
        "output_path": str(out),
    }, spark)

    assert result.status == "success", result.module_results

    df = spark.read.parquet(str(out))
    assert df.count() == 10, "All rows (US + EU) must be present after Funnel merge"
    assert "pipeline_tag" in df.columns
    assert all(r["pipeline_tag"] == "processed" for r in df.select("pipeline_tag").collect())


# ── Chained channels ──────────────────────────────────────────────────────────


def test_chained_channels(spark: SparkSession, sample_data, tmp_path):
    """Ingress → Channel → Channel → Egress: filter then add column.

    9 non-null rows → clean channel removes null-amount row → tagged channel
    adds 'tag' column → Egress writes 9 rows each having tag='processed'.
    """
    out = tmp_path / "out"
    result = _run("bp_chained_channels.yml", {
        "input_path": str(sample_data / "orders.parquet"),
        "output_path": str(out),
    }, spark)

    assert result.status == "success", result.module_results
    statuses = {r.module_id: r.status for r in result.module_results}
    assert statuses["clean"] == "success"
    assert statuses["tagged"] == "success"

    df = spark.read.parquet(str(out))
    assert df.count() == 9
    assert "tag" in df.columns
    assert all(r["tag"] == "processed" for r in df.select("tag").collect())


# ── Lineage written after channel pipeline ────────────────────────────────────


def test_lineage_written_after_channel_run(spark: SparkSession, sample_data, tmp_path):
    """lineage.db created in store_dir after a pipeline with Channel modules."""
    out = tmp_path / "out"
    store = tmp_path / "store"
    result = _run("bp_channel_filter.yml", {
        "input_path": str(sample_data / "orders.parquet"),
        "output_path": str(out),
    }, spark, store_dir=store)

    assert result.status == "success", result.module_results
    assert (store / "lineage.db").exists(), "lineage.db must be written after channel run"

    import duckdb
    conn = duckdb.connect(str(store / "lineage.db"))
    count = conn.execute("SELECT COUNT(*) FROM column_lineage").fetchone()[0]
    conn.close()
    assert count > 0, "At least one lineage row must be recorded"
