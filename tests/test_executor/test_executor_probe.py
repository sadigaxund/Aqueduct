from __future__ import annotations
import pytest
import duckdb
import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch
from pyspark.sql import Row

from aqueduct.executor.spark.probe import _threshold, execute_probe
from aqueduct.surveyor.surveyor import Surveyor
from aqueduct.parser.models import Module, Edge, RetryPolicy
from aqueduct.compiler.models import Manifest

pytestmark = [pytest.mark.spark, pytest.mark.integration]

def test_threshold_passed(spark):
    df = spark.createDataFrame([Row(id=1), Row(id=2)])
    sig_cfg = {"type": "threshold", "expr": "COUNT(*) > 0"}
    
    result = _threshold(df, sig_cfg)
    
    assert result["passed"] is True
    assert result["value"] is True
    assert result["expr"] == "COUNT(*) > 0"

def test_threshold_failed(spark):
    df = spark.createDataFrame([], "id INT")
    sig_cfg = {"type": "threshold", "expr": "COUNT(*) > 0"}
    
    result = _threshold(df, sig_cfg)
    
    assert result["passed"] is False
    assert result["value"] is False

def test_threshold_missing_expr(spark):
    df = spark.createDataFrame([Row(id=1)])
    sig_cfg = {"type": "threshold"}
    
    with pytest.raises(ValueError, match="threshold signal requires an 'expr' field"):
        _threshold(df, sig_cfg)

def test_execute_probe_writes_to_db(spark, tmp_path):
    df = spark.createDataFrame([Row(id=1)])
    module = Module(
        id="p1",
        type="Probe",
        label="Probe 1",
        config={
            "signals": [
                {"type": "threshold", "expr": "COUNT(*) > 0"}
            ]
        }
    )
    
    execute_probe(module, df, spark, "run_1", tmp_path)
    
    db_path = tmp_path / "obs.db"
    assert db_path.exists()
    
    conn = duckdb.connect(str(db_path))
    row = conn.execute("SELECT payload FROM probe_signals WHERE probe_id='p1'").fetchone()
    conn.close()
    
    payload = json.loads(row[0])
    assert payload["passed"] is True
    assert payload["expr"] == "COUNT(*) > 0"

def test_evaluate_regulator_passed(tmp_path):
    # Setup obs.db with a passed signal
    db_path = tmp_path / "obs.db"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE probe_signals (run_id VARCHAR, probe_id VARCHAR, signal_type VARCHAR, payload VARCHAR, captured_at TIMESTAMP DEFAULT now())")
    conn.execute(
        "INSERT INTO probe_signals (run_id, probe_id, signal_type, payload) VALUES (?, ?, ?, ?)",
        ["run_1", "p1", "threshold", json.dumps({"passed": True})]
    )
    conn.close()
    
    manifest = MagicMock()
    manifest.blueprint_id = "test_bp"
    manifest.edges = [MagicMock(from_id="p1", to_id="r1", port="signal")]
    
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    surveyor.start("run_1")
    
    assert surveyor.evaluate_regulator("r1") is True

def test_evaluate_regulator_failed(tmp_path):
    # Setup obs.db with a failed signal
    db_path = tmp_path / "obs.db"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE probe_signals (run_id VARCHAR, probe_id VARCHAR, signal_type VARCHAR, payload VARCHAR, captured_at TIMESTAMP DEFAULT now())")
    conn.execute(
        "INSERT INTO probe_signals (run_id, probe_id, signal_type, payload) VALUES (?, ?, ?, ?)",
        ["run_1", "p1", "threshold", json.dumps({"passed": False})]
    )
    conn.close()
    
    manifest = MagicMock()
    manifest.blueprint_id = "test_bp"
    manifest.edges = [MagicMock(from_id="p1", to_id="r1", port="signal")]
    
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    surveyor.start("run_1")
    
    assert surveyor.evaluate_regulator("r1") is False

def test_evaluate_regulator_no_signal_defaults_open(tmp_path):
    db_path = tmp_path / "obs.db"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE probe_signals (run_id VARCHAR, probe_id VARCHAR, signal_type VARCHAR, payload VARCHAR, captured_at TIMESTAMP DEFAULT now())")
    conn.close()
    
    manifest = MagicMock()
    manifest.blueprint_id = "test_bp"
    manifest.edges = [MagicMock(from_id="p1", to_id="r1", port="signal")]
    
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    surveyor.start("run_1")
    
    # No signal with 'passed' key -> defaults to True
    assert surveyor.evaluate_regulator("r1") is True


def test_regulator_timeout_opens_mid_poll(tmp_path):
    from aqueduct.executor.spark.executor import execute
    
    # 1. Manifest with Ingress -> Regulator
    ingress = Module(id="in1", type="Ingress", label="In", config={"format": "parquet", "path": "p"})
    regulator = Module(id="r1", type="Regulator", label="Reg", config={"timeout_seconds": 10, "on_block": "abort"})
    manifest = Manifest(
        blueprint_id="bp1",
        name="test",
        description="",
        aqueduct_version="1.0",
        context={},
        modules=(ingress, regulator),
        edges=(
            Edge(from_id="in1", to_id="r1", port="main"),
            Edge(from_id="p1", to_id="r1", port="signal") # signal source
        ),
        spark_config={},
        retry_policy=RetryPolicy(),
        agent=None,
        udf_registry={},
        macros={},
        checkpoint=False,
        provenance_map=None,
        inputs_fingerprint={}
    )
    
    mock_surveyor = MagicMock()
    # First call: False (closed), Second call: True (open)
    mock_surveyor.evaluate_regulator.side_effect = [False, True]
    
    spark = MagicMock()
    # Mock ingress producing a DF
    mock_df = MagicMock()
    with patch("aqueduct.executor.spark.executor.read_ingress", return_value=mock_df), \
         patch("time.sleep") as mock_sleep:
        
        # We need a frame_store to check results
        res = execute(manifest, spark, store_dir=tmp_path, surveyor=mock_surveyor)
        
        # Should have called evaluate_regulator twice
        assert mock_surveyor.evaluate_regulator.call_count == 2
        assert mock_sleep.call_count == 1
        
        # Regulator should be success
        reg_res = next(r for r in res.module_results if r.module_id == "r1")
        assert reg_res.status == "success"

def test_regulator_timeout_reaches_limit_aborts(tmp_path):
    from aqueduct.executor.spark.executor import execute
    
    ingress = Module(id="in1", type="Ingress", label="In", config={"format": "parquet", "path": "p"})
    regulator = Module(id="r1", type="Regulator", label="Reg", config={"timeout_seconds": 1, "on_block": "abort"})
    manifest = Manifest(
        blueprint_id="bp1",
        name="test",
        description="",
        aqueduct_version="1.0",
        context={},
        modules=(ingress, regulator),
        edges=(
            Edge(from_id="in1", to_id="r1", port="main"),
            Edge(from_id="p1", to_id="r1", port="signal")
        ),
        spark_config={},
        retry_policy=RetryPolicy(),
        agent=None,
        udf_registry={},
        macros={},
        checkpoint=False,
        provenance_map=None,
        inputs_fingerprint={}
    )
    
    mock_surveyor = MagicMock()
    mock_surveyor.evaluate_regulator.return_value = False
    
    spark = MagicMock()
    mock_df = MagicMock()
    with patch("aqueduct.executor.spark.executor.read_ingress", return_value=mock_df), \
         patch("time.sleep") as mock_sleep:
        
        res = execute(manifest, spark, store_dir=tmp_path, surveyor=mock_surveyor)
        
        assert mock_surveyor.evaluate_regulator.call_count >= 1
        
        reg_res = next(r for r in res.module_results if r.module_id == "r1")
        assert reg_res.status == "error"
        assert "Regulator gate closed; on_block=abort" in reg_res.error
