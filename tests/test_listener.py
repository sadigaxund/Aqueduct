"""Tests for AqueductMetricsListener and _write_stage_metrics().

Covers the 8 ⏳ items in TESTING.md §SparkListener / module_metrics.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest
from pyspark.sql import SparkSession

from aqueduct.executor.spark.listener import AqueductMetricsListener, _zero_metrics
from aqueduct.executor.spark.executor import _write_stage_metrics


# ── AqueductMetricsListener unit tests (no Spark needed) ─────────────────────


def test_listener_set_active_module_resets_accumulated():
    """set_active_module() should reset any accumulated metrics to zero."""
    listener = AqueductMetricsListener()

    # Simulate accumulated state from a prior stage callback
    with listener._lock:
        listener._accumulated["records_written"] = 999
        listener._accumulated["bytes_written"] = 12345
        listener._active_module = "old_module"

    # Setting a new active module must clear accumulator
    listener.set_active_module("new_module")

    with listener._lock:
        assert listener._active_module == "new_module"
        assert listener._accumulated == _zero_metrics()


def test_listener_collect_metrics_returns_and_resets():
    """collect_metrics() returns accumulated dict and resets to zero state."""
    listener = AqueductMetricsListener()
    listener.set_active_module("mod_a")

    # Inject fake metrics (simulating what onStageCompleted would write)
    with listener._lock:
        listener._accumulated["records_written"] = 42
        listener._accumulated["bytes_written"] = 1024
        listener._accumulated["duration_ms"] = 300

    metrics = listener.collect_metrics()

    # Returned dict must match what was accumulated
    assert metrics["records_written"] == 42
    assert metrics["bytes_written"] == 1024
    assert metrics["duration_ms"] == 300

    # State must be reset after collect
    with listener._lock:
        assert listener._accumulated == _zero_metrics()
        assert listener._active_module is None


def test_listener_collect_metrics_no_active_module_returns_zeros():
    """collect_metrics() with no active module returns all-zero dict."""
    listener = AqueductMetricsListener()
    # Do NOT call set_active_module — fresh instance has no active module
    metrics = listener.collect_metrics()

    assert metrics == _zero_metrics()
    assert all(v == 0 for v in metrics.values())


# ── _write_stage_metrics unit tests ──────────────────────────────────────────


def test_write_stage_metrics_creates_table_and_inserts(tmp_path: Path):
    """_write_stage_metrics() creates module_metrics table and inserts one row."""
    store_dir = tmp_path / "store"
    metrics = {
        "records_read": 0,
        "bytes_read": 0,
        "records_written": 100,
        "bytes_written": 4096,
        "duration_ms": 250,
    }

    _write_stage_metrics("egress_01", "run-xyz", metrics, store_dir)

    db_path = store_dir / "signals.db"
    assert db_path.exists()

    conn = duckdb.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT module_id, records_written, bytes_written, duration_ms "
            "FROM module_metrics WHERE run_id = 'run-xyz'"
        ).fetchall()
    finally:
        conn.close()

    assert len(rows) == 1
    assert rows[0][0] == "egress_01"
    assert rows[0][1] == 100
    assert rows[0][2] == 4096
    assert rows[0][3] == 250


def test_write_stage_metrics_store_dir_none_is_noop(tmp_path: Path):
    """_write_stage_metrics() with store_dir=None must not create any files."""
    _write_stage_metrics("mod", "run-1", {"records_written": 5}, store_dir=None)
    # Nothing should be created anywhere
    assert not list(tmp_path.iterdir())


# ── Executor integration: Egress → module_metrics written ────────────────────


def test_egress_writes_module_metrics_on_success(spark: SparkSession, tmp_path: Path):
    """Egress succeeds → module_metrics row exists in signals.db with correct module_id."""
    from aqueduct.compiler.models import Manifest
    from aqueduct.executor.spark.executor import execute
    from aqueduct.executor.spark.listener import AqueductMetricsListener
    from aqueduct.parser.models import Edge, Module

    # Wire a fresh listener to the shared spark session so the executor picks it up
    listener = AqueductMetricsListener()
    listener.register(spark)
    spark._aq_metrics_listener = listener  # type: ignore[attr-defined]

    in_path = str(tmp_path / "in.parquet")
    spark.range(5).write.parquet(in_path)
    out_path = str(tmp_path / "out.parquet")
    store_dir = tmp_path / "store"

    manifest = Manifest(
        blueprint_id="test.metrics_egress",
        modules=(
            Module(id="ing", type="Ingress", label="Ing", config={"format": "parquet", "path": in_path}),
            Module(id="egr", type="Egress",  label="Egr", config={"format": "parquet", "path": out_path}),
        ),
        edges=(Edge(from_id="ing", to_id="egr", port="main"),),
        context={},
        spark_config={},
    )

    result = execute(manifest, spark, run_id="run-mm1", store_dir=store_dir)
    assert result.status == "success"

    db_path = store_dir / "signals.db"
    assert db_path.exists(), "signals.db must be created when store_dir is set"

    conn = duckdb.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT module_id FROM module_metrics WHERE run_id = 'run-mm1'"
        ).fetchall()
    finally:
        conn.close()

    module_ids = {r[0] for r in rows}
    assert "egr" in module_ids, f"Expected 'egr' in module_metrics rows; got {module_ids}"


def test_egress_failure_no_module_metrics_row(spark: SparkSession, tmp_path: Path):
    """Egress failure → listener reset; no module_metrics row written for that module."""
    from aqueduct.compiler.models import Manifest
    from aqueduct.executor.spark.executor import execute
    from aqueduct.executor.spark.listener import AqueductMetricsListener
    from aqueduct.parser.models import Edge, Module

    listener = AqueductMetricsListener()
    listener.register(spark)
    spark._aq_metrics_listener = listener  # type: ignore[attr-defined]

    in_path = str(tmp_path / "in.parquet")
    spark.range(5).write.parquet(in_path)
    store_dir = tmp_path / "store"

    manifest = Manifest(
        blueprint_id="test.metrics_egress_fail",
        modules=(
            Module(id="ing",  type="Ingress", label="Ing",  config={"format": "parquet", "path": in_path}),
            Module(id="egr",  type="Egress",  label="Egr",  config={"format": "parquet", "path": "/bad/path", "mode": "broken_mode"}),
        ),
        edges=(Edge(from_id="ing", to_id="egr", port="main"),),
        context={},
        spark_config={},
    )

    result = execute(manifest, spark, run_id="run-mm-fail", store_dir=store_dir)
    assert result.status == "error"

    # Either no DB at all, or no row for the failing egress module
    db_path = store_dir / "signals.db"
    if db_path.exists():
        conn = duckdb.connect(str(db_path))
        try:
            # module_metrics table may not even exist yet on first error
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]
            if "module_metrics" in table_names:
                rows = conn.execute(
                    "SELECT module_id FROM module_metrics "
                    "WHERE run_id = 'run-mm-fail' AND module_id = 'egr'"
                ).fetchall()
                assert rows == [], f"Expected no row for failing egress; got {rows}"
        finally:
            conn.close()


# ── row_count_estimate with spark_listener reading existing module_metrics ────


def test_row_count_estimate_spark_listener_reads_module_metrics(
    spark: SparkSession, tmp_path: Path
):
    """row_count_estimate method=spark_listener: when module_metrics row exists,
    estimate equals records_written value."""
    import json as _json
    from datetime import datetime, timezone

    from aqueduct.executor.spark.probe import execute_probe, _row_count_estimate
    from aqueduct.parser.models import Module

    store_dir = tmp_path / "store"
    store_dir.mkdir(parents=True)
    db_path = store_dir / "signals.db"

    # Pre-seed module_metrics with a known records_written value
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS module_metrics (
                run_id        VARCHAR     NOT NULL,
                module_id     VARCHAR     NOT NULL,
                records_read  BIGINT,
                bytes_read    BIGINT,
                records_written BIGINT,
                bytes_written BIGINT,
                duration_ms   BIGINT,
                captured_at   TIMESTAMPTZ NOT NULL
            )
        """)
        conn.execute(
            "INSERT INTO module_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ["run-sl", "upstream_ingress", 0, 0, 77, 1024, 100,
             datetime.now(tz=timezone.utc).isoformat()],
        )
    finally:
        conn.close()

    df = spark.range(10)  # actual df doesn't matter for spark_listener method

    # Simulate how execute_probe calls _row_count_estimate
    signal_cfg = {
        "type": "row_count_estimate",
        "method": "spark_listener",
        "attach_to": "upstream_ingress",
    }
    result = _row_count_estimate(
        df,
        signal_cfg,
        probe_id="upstream_ingress",
        run_id="run-sl",
        store_dir=store_dir,
    )

    assert result["method"] == "spark_listener"
    assert result["estimate"] == 77, f"Expected estimate=77, got {result}"
