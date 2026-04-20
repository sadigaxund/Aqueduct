"""Tests for the Probe executor."""

import json
from pathlib import Path

import duckdb
import pytest
from pyspark.sql import SparkSession

from aqueduct.executor.spark.probe import execute_probe
from aqueduct.parser.models import Module


def test_execute_probe_no_signals(spark: SparkSession, tmp_path: Path):
    df = spark.range(5)
    module = Module(id="p1", type="Probe", label="P1", config={})
    store_dir = tmp_path / "store"
    
    # Should return immediately without writing anything
    execute_probe(module, df, spark, "run-1", store_dir)
    assert not (store_dir / "signals.db").exists()


def test_execute_probe_unknown_signal(spark: SparkSession, tmp_path: Path, caplog):
    df = spark.range(5)
    module = Module(
        id="p1", type="Probe", label="P1", 
        config={"signals": [{"type": "unknown_signal"}, {"type": "sample_rows", "n": 2}]}
    )
    store_dir = tmp_path / "store"
    
    execute_probe(module, df, spark, "run-1", store_dir)
    db_path = store_dir / "signals.db"
    assert db_path.exists()
    
    # Warning logged for unknown signal
    assert "unknown signal type 'unknown_signal'" in caplog.text
    
    # Other signal captured
    conn = duckdb.connect(str(db_path))
    rows = conn.execute("SELECT signal_type FROM probe_signals").fetchall()
    conn.close()
    assert rows == [("sample_rows",)]


def test_execute_probe_schema_snapshot(spark: SparkSession, tmp_path: Path):
    df = spark.range(5)
    module = Module(
        id="p1", type="Probe", label="P1", 
        config={"signals": [{"type": "schema_snapshot"}]}
    )
    store_dir = tmp_path / "store"
    
    execute_probe(module, df, spark, "run-1", store_dir)
    
    # File written
    schema_file = store_dir / "signals" / "run-1" / "p1_schema.json"
    assert schema_file.exists()
    payload = json.loads(schema_file.read_text())
    assert "fields" in payload
    assert payload["fields"][0]["name"] == "id"
    
    # DB row inserted
    conn = duckdb.connect(str(store_dir / "signals.db"))
    rows = conn.execute("SELECT payload FROM probe_signals WHERE signal_type='schema_snapshot'").fetchall()
    conn.close()
    
    db_payload = json.loads(rows[0][0])
    assert db_payload == payload


def test_execute_probe_row_count_estimate_sample(spark: SparkSession, tmp_path: Path):
    df = spark.range(100)
    module = Module(
        id="p1", type="Probe", label="P1", 
        config={"signals": [{"type": "row_count_estimate", "method": "sample", "fraction": 0.5}]}
    )
    store_dir = tmp_path / "store"
    
    execute_probe(module, df, spark, "run-1", store_dir)
    
    conn = duckdb.connect(str(store_dir / "signals.db"))
    payload_str = conn.execute("SELECT payload FROM probe_signals WHERE signal_type='row_count_estimate'").fetchone()[0]
    conn.close()
    
    payload = json.loads(payload_str)
    assert payload["method"] == "sample"
    assert payload["estimate"] > 0
    assert payload["sample_count"] > 0


def test_execute_probe_row_count_estimate_spark_listener(spark: SparkSession, tmp_path: Path):
    df = spark.range(100)
    module = Module(
        id="p1", type="Probe", label="P1", 
        config={"signals": [{"type": "row_count_estimate", "method": "spark_listener"}]}
    )
    store_dir = tmp_path / "store"
    
    execute_probe(module, df, spark, "run-1", store_dir)
    
    conn = duckdb.connect(str(store_dir / "signals.db"))
    payload_str = conn.execute("SELECT payload FROM probe_signals WHERE signal_type='row_count_estimate'").fetchone()[0]
    conn.close()
    
    payload = json.loads(payload_str)
    assert payload["method"] == "spark_listener"
    assert payload["estimate"] is None


def test_execute_probe_null_rates(spark: SparkSession, tmp_path: Path):
    df = spark.range(100).selectExpr("id", "CASE WHEN id % 2 = 0 THEN NULL ELSE id END as val")
    module = Module(
        id="p1", type="Probe", label="P1", 
        config={"signals": [{"type": "null_rates", "columns": ["val"], "fraction": 1.0}]}
    )
    store_dir = tmp_path / "store"
    
    execute_probe(module, df, spark, "run-1", store_dir)
    
    conn = duckdb.connect(str(store_dir / "signals.db"))
    payload_str = conn.execute("SELECT payload FROM probe_signals WHERE signal_type='null_rates'").fetchone()[0]
    conn.close()
    
    payload = json.loads(payload_str)
    assert "val" in payload["null_rates"]
    assert payload["null_rates"]["val"] == 0.5


def test_execute_probe_null_rates_no_columns(spark: SparkSession, tmp_path: Path):
    df = spark.range(100).selectExpr("id", "CASE WHEN id % 2 = 0 THEN NULL ELSE id END as val")
    module = Module(
        id="p1", type="Probe", label="P1", 
        config={"signals": [{"type": "null_rates", "fraction": 1.0}]}
    )
    store_dir = tmp_path / "store"
    
    execute_probe(module, df, spark, "run-1", store_dir)
    
    conn = duckdb.connect(str(store_dir / "signals.db"))
    payload_str = conn.execute("SELECT payload FROM probe_signals").fetchone()[0]
    conn.close()
    
    payload = json.loads(payload_str)
    assert "val" in payload["null_rates"]
    assert "id" in payload["null_rates"]


def test_execute_probe_sample_rows(spark: SparkSession, tmp_path: Path):
    df = spark.range(100)
    module = Module(
        id="p1", type="Probe", label="P1", 
        config={"signals": [{"type": "sample_rows", "n": 10}]}
    )
    store_dir = tmp_path / "store"
    
    execute_probe(module, df, spark, "run-1", store_dir)
    
    conn = duckdb.connect(str(store_dir / "signals.db"))
    payload_str = conn.execute("SELECT payload FROM probe_signals WHERE signal_type='sample_rows'").fetchone()[0]
    conn.close()
    
    payload = json.loads(payload_str)
    assert payload["n"] == 10
    assert len(payload["rows"]) == 10


def test_execute_probe_exception_isolation(spark: SparkSession, tmp_path: Path):
    df = spark.range(5)
    # The null_rates uses df.sample(fraction) and expect 0 <= fraction <= 1.
    # Setting an invalid fraction should raise ValueError but we trap it
    module = Module(
        id="p1", type="Probe", label="P1", 
        config={"signals": [
            {"type": "null_rates", "fraction": -1.0}, 
            {"type": "sample_rows", "n": 2}
        ]}
    )
    store_dir = tmp_path / "store"
    
    execute_probe(module, df, spark, "run-1", store_dir)
    
    # sample_rows should still be captured
    conn = duckdb.connect(str(store_dir / "signals.db"))
    types = [r[0] for r in conn.execute("SELECT signal_type FROM probe_signals").fetchall()]
    conn.close()
    
    assert types == ["sample_rows"]


def test_execute_probe_global_exception(spark: SparkSession, tmp_path: Path):
    df = spark.range(5)
    module = Module(
        id="p1", type="Probe", label="P1", 
        config={"signals": [{"type": "schema_snapshot"}]}
    )
    
    store_dir = tmp_path / "store"
    (tmp_path / "store").touch() # File obstructs directory creation
    
    # Should not raise exception
    execute_probe(module, df, spark, "run-1", store_dir)
