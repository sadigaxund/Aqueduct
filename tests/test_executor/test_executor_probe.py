from __future__ import annotations

import json
import threading
import time
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from pyspark.sql import Row

from aqueduct.compiler.models import Manifest
from aqueduct.errors import ConfigError
from aqueduct.executor.spark.executor import execute
from aqueduct.executor.spark.probe import _custom, _threshold, execute_probe
from aqueduct.parser.models import Edge, Module
from aqueduct.surveyor.surveyor import Surveyor

pytestmark = [pytest.mark.spark, pytest.mark.integration]


def _probe_p99(df, sig_cfg):  # module-level so the pointer form can import it
    from pyspark.sql import functions as F

    val = df.select(F.expr("percentile(amount, 0.99)").alias("p")).collect()[0]["p"]
    return {"estimate": val, "metadata": {"col": "amount"}, "passed": val < 1000}


def _returns_list(df, cfg):  # module-level helper: accepts (df, cfg) but returns a list
    return [1, 2, 3]

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
    
    with pytest.raises(ConfigError, match="threshold signal requires an 'expr' field"):
        _threshold(df, sig_cfg)

def test_custom_inline_sql_estimate(spark):
    df = spark.createDataFrame([Row(amount=10), Row(amount=20), Row(amount=30)])
    result = _custom(df, {"type": "custom", "sql": "MAX(amount)"})
    assert result["custom"] is True
    assert result["estimate"] == 30
    assert "passed" not in result


def test_custom_inline_sql_passed_when_true(spark):
    df = spark.createDataFrame([Row(amount=10), Row(amount=20)])
    result = _custom(df, {"type": "custom", "passed_when": "MAX(amount) < 100"})
    assert result["passed"] is True


def test_custom_inline_sql_passed_when_false(spark):
    df = spark.createDataFrame([Row(amount=10), Row(amount=200)])
    result = _custom(df, {"type": "custom", "passed_when": "MAX(amount) < 100"})
    assert result["passed"] is False


def test_custom_callable_pointer(spark):
    df = spark.createDataFrame([Row(amount=10), Row(amount=20)])
    result = _custom(
        df,
        {
            "type": "custom",
            "module": "tests.test_executor.test_executor_probe",
            "entry": "_probe_p99",
        },
    )
    assert result["custom"] is True
    assert result["passed"] is True
    assert result["metadata"] == {"col": "amount"}


def test_custom_callable_must_return_dict(spark):
    df = spark.createDataFrame([Row(amount=10)])
    import sys as _sys
    _modname = _sys.modules[__name__].__name__
    with pytest.raises(ConfigError, match="must return a dict"):
        _custom(
            df,
            {
                "type": "custom",
                "module": _modname,
                "entry": "_returns_list",
            },
        )


def test_execute_probe_custom_lands_in_db(spark, tmp_path):
    df = spark.createDataFrame([Row(amount=5), Row(amount=15)])
    module = Module(
        id="cp1",
        type="Probe",
        label="Custom Probe",
        config={"signals": [{"type": "custom", "sql": "AVG(amount)", "passed_when": "MIN(amount) >= 0"}]},
    )

    execute_probe(module, df, spark, "run_c", tmp_path)

    db_path = tmp_path / "observability.db"
    conn = duckdb.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT signal_type, payload FROM probe_signals WHERE probe_id = 'cp1'"
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] == "custom"
    payload = json.loads(row[1])
    assert payload["estimate"] == 10.0
    assert payload["passed"] is True


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

def test_schema_snapshot_writes_probe_signals_only_no_sidecar(spark, tmp_path):
    """Phase 53 — a schema_snapshot probe persists its payload to probe_signals
    only; the local ``snapshots/<run_id>/*_schema.json`` sidecar is NOT written."""
    df = spark.createDataFrame([Row(id=1, name="a")])
    module = Module(
        id="schema_probe", type="Probe", label="Schema",
        config={"signals": [{"type": "schema_snapshot"}]},
    )

    execute_probe(module, df, spark, "run_snap", tmp_path)

    # Payload landed in probe_signals …
    conn = duckdb.connect(str(tmp_path / "observability.db"))
    row = conn.execute(
        "SELECT payload FROM probe_signals WHERE probe_id='schema_probe' AND signal_type='schema_snapshot'"
    ).fetchone()
    conn.close()
    assert row is not None
    payload = json.loads(row[0])
    assert {f["name"] for f in payload["fields"]} == {"id", "name"}
    # … and NO local snapshots/ sidecar directory was created.
    assert not (tmp_path / "snapshots").exists()


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
    
    surveyor = Surveyor(manifest, store_dir=tmp_path, engine="spark")
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
    
    surveyor = Surveyor(manifest, store_dir=tmp_path, engine="spark")
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
    
    surveyor = Surveyor(manifest, store_dir=tmp_path, engine="spark")
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
    
    surveyor = Surveyor(manifest, store_dir=tmp_path, engine="spark")
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
    
    surveyor = Surveyor(manifest, store_dir=tmp_path, engine="spark")
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


def test_parallel_multi_tree_probe_integration(spark, tmp_path):
    """--parallel + Probe attached in a multi-tree blueprint: Probe runs in the same thread

    and immediately after its attach_to module, so signals are written deterministically.
    """
    in_path1 = str(tmp_path / "in1.parquet")
    spark.range(5).write.parquet(in_path1)
    out_path1 = str(tmp_path / "out1.parquet")

    in_path2 = str(tmp_path / "in2.parquet")
    spark.range(10).write.parquet(in_path2)
    out_path2 = str(tmp_path / "out2.parquet")

    # Tree 1: in1 -> ch1 -> out1 (with Probe p1 attached to ch1)
    ingress1 = Module(id="in1", type="Ingress", label="In1", config={"format": "parquet", "path": in_path1})
    channel1 = Module(id="ch1", type="Channel", label="Ch1", config={"op": "sql", "query": "SELECT id FROM in1"})
    probe1 = Module(id="p1", type="Probe", label="P1", attach_to="ch1", config={
        "signals": [{"type": "threshold", "expr": "COUNT(*) > 0"}]
    })
    egress1 = Module(id="out1", type="Egress", label="Out1", config={"format": "parquet", "path": out_path1})

    # Tree 2: in2 -> out2
    ingress2 = Module(id="in2", type="Ingress", label="In2", config={"format": "parquet", "path": in_path2})
    egress2 = Module(id="out2", type="Egress", label="Out2", config={"format": "parquet", "path": out_path2})

    manifest = Manifest(
        blueprint_id="bp_parallel_multi_tree",
        name="test",
        context={},
        modules=(ingress1, channel1, probe1, egress1, ingress2, egress2),
        edges=(
            Edge(from_id="in1", to_id="ch1", port="main"),
            Edge(from_id="ch1", to_id="out1", port="main"),
            Edge(from_id="in2", to_id="out2", port="main"),
        ),
        spark_config={},
    )

    surveyor = Surveyor(manifest, store_dir=tmp_path, engine="spark")
    run_id = "run_parallel_multi_tree"
    surveyor.start(run_id)

    res = execute(manifest, spark, store_dir=tmp_path, surveyor=surveyor, run_id=run_id, parallel=True)

    assert res.status == "success"

    # Verify that tree 1 and tree 2 output files were created and correct counts exist
    assert spark.read.parquet(out_path1).count() == 5
    assert spark.read.parquet(out_path2).count() == 10

    # Verify that the Probe successfully ran and wrote a threshold signal to the DuckDB store
    db_path = tmp_path / "observability.db"
    assert db_path.exists()
    conn = duckdb.connect(str(db_path))
    row = conn.execute("SELECT payload FROM probe_signals WHERE probe_id='p1'").fetchone()
    conn.close()

    assert row is not None
    payload = json.loads(row[0])
    assert payload["passed"] is True
    assert payload["expr"] == "COUNT(*) > 0"
