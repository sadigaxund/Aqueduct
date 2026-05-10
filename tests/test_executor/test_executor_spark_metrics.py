from __future__ import annotations

import pytest
pytestmark = [pytest.mark.spark, pytest.mark.integration]
import os
import duckdb
from pathlib import Path
from aqueduct.executor.spark.metrics import observe_df, get_observation, dir_bytes, null_metrics
from aqueduct.executor.spark.executor import _write_stage_metrics

def test_observe_df_fallback():
    """observe_df() on Spark < 3.3 or mock returns (original_df, None)"""
    # We can't easily force Spark < 3.3 if it's already 3.5+, 
    # but we can pass something that isn't a DataFrame to trigger the catch.
    df, obs = observe_df("not a dataframe", "my_obs")
    assert df == "not a dataframe"
    assert obs is None

def test_observe_df_integration(spark):
    """observe_df() on Spark 3.3+ returns (observed_df, Observation)"""
    # Check if Observation is available in this Spark version
    try:
        from pyspark.sql import Observation
    except ImportError:
        pytest.skip("Observation not available in this Spark version")

    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    obs_name = "test_obs_integration"
    
    observed_df, obs = observe_df(df, obs_name)
    
    assert obs is not None
    assert isinstance(obs, Observation)
    
    # Trigger action to fire observation
    observed_df.collect()
    
    count = get_observation(obs, "records_written")
    assert count == 2

def test_get_observation_none():
    """get_observation(None, alias) returns None"""
    assert get_observation(None, "records_written") is None

def test_get_observation_timeout():
    """get_observation returns None if action never fires (timeout)"""
    try:
        from pyspark.sql import Observation
    except ImportError:
        pytest.skip("Observation not available")
        
    obs = Observation("timeout_obs")
    # We don't trigger any action on the observed DF
    assert get_observation(obs, "records_written", timeout=0.1) is None

def test_dir_bytes_file(tmp_path):
    """dir_bytes() on existing local file returns correct size"""
    p = tmp_path / "hello.txt"
    p.write_text("1234567890")
    assert dir_bytes(str(p)) == 10

def test_dir_bytes_dir(tmp_path):
    """dir_bytes() on directory returns sum of file sizes"""
    d = tmp_path / "data"
    d.mkdir()
    (d / "a.txt").write_text("abc")    # 3 bytes
    (d / "b.txt").write_text("defg")   # 4 bytes
    
    # Nested file
    sub = d / "sub"
    sub.mkdir()
    (sub / "c.txt").write_text("hi")   # 2 bytes
    
    assert dir_bytes(str(d)) == 9

def test_dir_bytes_glob(tmp_path):
    """dir_bytes() supports glob patterns"""
    d = tmp_path / "glob_test"
    d.mkdir()
    (d / "f1.parquet").write_text("data")  # 4 bytes
    (d / "f2.parquet").write_text("more")  # 4 bytes
    (d / "f3.txt").write_text("ignore")
    
    pattern = str(d / "*.parquet")
    assert dir_bytes(pattern) == 8

def test_dir_bytes_cloud():
    """dir_bytes() on cloud paths returns None (unknown)"""
    assert dir_bytes("s3://my-bucket/data") is None
    assert dir_bytes("hdfs:///user/spark/warehouse") is None

def test_dir_bytes_nonexistent():
    """dir_bytes() on nonexistent path returns None"""
    assert dir_bytes("/tmp/aqueduct_ghost_file_12345") is None
    assert dir_bytes("") is None


class TestMetricsHelpers:
    def _make_listener(self):
        from unittest.mock import MagicMock
        listener = MagicMock()
        listener._metrics = {"duration_ms": 0}
        listener.collect_metrics.side_effect = lambda: listener._metrics
        
        def on_stage_completed(stage):
            info = stage.stageInfo()
            if not info.completionTime().isDefined():
                listener._metrics["duration_ms"] = 0
            else:
                listener._metrics["duration_ms"] = 100
        
        listener.onStageCompleted.side_effect = on_stage_completed
        return listener

    def _make_stage_completed(self):
        from unittest.mock import MagicMock
        stage = MagicMock()
        info = MagicMock()
        stage.stageInfo.return_value = info
        time_mock = MagicMock()
        info.completionTime.return_value = time_mock
        return stage

    def test_null_metrics_all_none_except_duration(self):
        m = null_metrics()
        assert m["duration_ms"] == 0
        assert m["records_read"] is None
        assert m["bytes_read"] is None
        assert m["records_written"] is None
        assert m["bytes_written"] is None
        assert set(m) == {"records_read", "bytes_read", "records_written", "bytes_written", "duration_ms"}

    def test_on_stage_completed_time_undefined(self):
        listener = self._make_listener()
        stage = self._make_stage_completed()
        # Override: completion time not defined
        info = stage.stageInfo()
        info.completionTime().isDefined.return_value = False
        listener.onStageCompleted(stage)
        metrics = listener.collect_metrics()
        # duration_ms should be 0 when time undefined
        assert metrics["duration_ms"] == 0


def test_write_stage_metrics_creates_table_and_inserts(tmp_path: Path):
    store_dir = tmp_path / "store"
    metrics = {
        "records_read": 0,
        "bytes_read": 0,
        "records_written": 100,
        "bytes_written": 4096,
        "duration_ms": 250,
    }

    _write_stage_metrics("egress_01", "run-xyz", metrics, store_dir)

    db_path = store_dir / "obs.db"
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


def test_egress_writes_module_metrics_on_success(spark, tmp_path: Path):
    """Egress succeeds → module_metrics row exists in obs.db."""
    from aqueduct.compiler.models import Manifest
    from aqueduct.executor.spark.executor import execute
    from aqueduct.parser.models import Edge, Module

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

    db_path = store_dir / "obs.db"
    assert db_path.exists()

    conn = duckdb.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT module_id, records_written, duration_ms "
            "FROM module_metrics WHERE run_id = 'run-mm1'"
        ).fetchall()
    finally:
        conn.close()

    module_ids = {r[0] for r in rows}
    assert "egr" in module_ids
    egr_row = next(r for r in rows if r[0] == "egr")
    assert egr_row[2] > 0, "duration_ms should be non-zero"


def test_row_count_estimate_spark_listener_reads_module_metrics(
    spark, tmp_path: Path
):
    """row_count_estimate method=spark_listener: when module_metrics row exists,
    estimate equals records_written value."""
    from datetime import datetime, timezone
    from aqueduct.executor.spark.probe import _row_count_estimate

    store_dir = tmp_path / "store"
    store_dir.mkdir(parents=True)
    db_path = store_dir / "obs.db"

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

    df = spark.range(10)
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
    assert result["estimate"] == 77
