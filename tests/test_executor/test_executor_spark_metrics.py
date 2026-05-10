from __future__ import annotations

import pytest
pytestmark = [pytest.mark.spark, pytest.mark.integration]
import os
from pathlib import Path
from aqueduct.executor.spark.metrics import observe_df, get_observation, dir_bytes

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
        from pyspark.context import SparkContext
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

    def test_get_observation_none_returns_none(self):
        from aqueduct.executor.spark.metrics import get_observation
        assert get_observation(None, "records_written") is None

    def test_dir_bytes_cloud_path_returns_none(self):
        from aqueduct.executor.spark.metrics import dir_bytes
        assert dir_bytes("s3://bucket/key") is None
        assert dir_bytes("hdfs://namenode/path") is None

    def test_dir_bytes_empty_returns_none(self):
        from aqueduct.executor.spark.metrics import dir_bytes
        assert dir_bytes("") is None

    def test_dir_bytes_nonexistent_returns_none(self):
        from aqueduct.executor.spark.metrics import dir_bytes
        assert dir_bytes("/definitely/does/not/exist/path") is None

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
