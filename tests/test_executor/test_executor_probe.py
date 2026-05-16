from __future__ import annotations
import pytest
import duckdb
import threading
import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch
from pyspark.sql import Row

from aqueduct.executor.spark.probe import _threshold, execute_probe
from aqueduct.executor.spark.executor import execute
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
    
    db_path = tmp_path / "observability.db"
    assert db_path.exists()
    
    conn = duckdb.connect(str(db_path))
    row = conn.execute("SELECT payload FROM probe_signals WHERE probe_id='p1'").fetchone()
    conn.close()
    
    payload = json.loads(row[0])
    assert payload["passed"] is True
    assert payload["expr"] == "COUNT(*) > 0"

def test_evaluate_regulator_passed(tmp_path):
    # Setup obs.db with a passed signal
    db_path = tmp_path / "observability.db"
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
    db_path = tmp_path / "observability.db"
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
    db_path = tmp_path / "observability.db"
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


def test_regulator_timeout_opens_mid_poll(spark, tmp_path):
    """Regulator gate starts closed, opens mid-poll after a signal is inserted."""
    # 1. Manifest with Ingress -> Regulator -> Egress
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).write.parquet(in_path)
    out_path = str(tmp_path / "out.parquet")
    
    ingress = Module(id="in1", type="Ingress", label="In", config={"format": "parquet", "path": in_path})
    regulator = Module(id="r1", type="Regulator", label="Reg", config={"timeout_seconds": 60, "on_block": "abort"})
    egress = Module(id="out1", type="Egress", label="Out", config={"format": "parquet", "path": out_path})
    
    manifest = Manifest(
        blueprint_id="bp_poll_success",
        name="test",
        context={},
        modules=(ingress, regulator, egress),
        edges=(
            Edge(from_id="in1", to_id="r1", port="main"),
            Edge(from_id="r1", to_id="out1", port="main"),
            Edge(from_id="p1", to_id="r1", port="signal")
        ),
        spark_config={},
    )
    
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    run_id = "run_poll_success"
    surveyor.start(run_id)
    
    # Create the table and insert initial CLOSED signal
    db_path = tmp_path / "observability.db"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS probe_signals (
            run_id      VARCHAR     NOT NULL,
            probe_id    VARCHAR     NOT NULL,
            signal_type VARCHAR     NOT NULL,
            payload     VARCHAR     NOT NULL,
            captured_at TIMESTAMPTZ NOT NULL
        );
    """)
    conn.execute(
        "INSERT INTO probe_signals (run_id, probe_id, signal_type, payload, captured_at) VALUES (?, ?, ?, ?, now())",
        [run_id, "p1", "threshold", json.dumps({"passed": False})]
    )
    conn.close()
    
    def _insert_signal():
        time.sleep(1) # Wait for execute to start polling
        conn = duckdb.connect(str(db_path))
        conn.execute(
            "INSERT INTO probe_signals (run_id, probe_id, signal_type, payload, captured_at) VALUES (?, ?, ?, ?, now())",
            [run_id, "p1", "threshold", json.dumps({"passed": True})]
        )
        conn.close()
        
    t = threading.Thread(target=_insert_signal, daemon=True)
    
    # Patch poll interval to be fast
    original_sleep = time.sleep
    def mocked_sleep(seconds):
        if seconds == 2.0: # poll_interval in executor.py
            original_sleep(0.1)
        else:
            original_sleep(seconds)

    with patch("time.sleep", side_effect=mocked_sleep):
        t.start()
        res = execute(manifest, spark, store_dir=tmp_path, surveyor=surveyor, run_id=run_id)
        t.join(timeout=5)
        
    assert res.status == "success"
    assert spark.read.parquet(out_path).count() == 5
    
    reg_res = next(r for r in res.module_results if r.module_id == "r1")
    assert reg_res.status == "success"

def test_regulator_timeout_reaches_limit_aborts(spark, tmp_path):
    """Regulator gate remains closed and times out, triggering abort."""
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).write.parquet(in_path)
    
    ingress = Module(id="in1", type="Ingress", label="In", config={"format": "parquet", "path": in_path})
    regulator = Module(id="r1", type="Regulator", label="Reg", config={"timeout_seconds": 1, "on_block": "abort"})
    
    manifest = Manifest(
        blueprint_id="bp_poll_abort",
        name="test",
        context={},
        modules=(ingress, regulator),
        edges=(
            Edge(from_id="in1", to_id="r1", port="main"),
            Edge(from_id="p1", to_id="r1", port="signal")
        ),
        spark_config={},
    )
    
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    run_id = "run_poll_abort"
    surveyor.start(run_id)
    
    db_path = tmp_path / "observability.db"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS probe_signals (
            run_id      VARCHAR     NOT NULL,
            probe_id    VARCHAR     NOT NULL,
            signal_type VARCHAR     NOT NULL,
            payload     VARCHAR     NOT NULL,
            captured_at TIMESTAMPTZ NOT NULL
        );
    """)
    conn.execute(
        "INSERT INTO probe_signals (run_id, probe_id, signal_type, payload, captured_at) VALUES (?, ?, ?, ?, now())",
        [run_id, "p1", "threshold", json.dumps({"passed": False})]
    )
    conn.close()
    
    original_sleep = time.sleep
    def mocked_sleep(seconds):
        if seconds == 2.0:
            original_sleep(0.1)
        else:
            original_sleep(seconds)

    with patch("time.sleep", side_effect=mocked_sleep):
        res = execute(manifest, spark, store_dir=tmp_path, surveyor=surveyor, run_id=run_id)
        
    assert res.status == "error"
    reg_res = next(r for r in res.module_results if r.module_id == "r1")
    assert reg_res.status == "error"
    assert "Regulator gate closed; on_block=abort" in reg_res.error
