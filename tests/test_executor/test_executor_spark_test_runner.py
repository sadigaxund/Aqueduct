"""Tests for the isolated test runner (aqueduct test)."""

from __future__ import annotations

from pathlib import Path
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, DateType
from datetime import datetime, date

from aqueduct.executor.spark.test_runner import (
    _create_df, _run_assertion, _execute_module, _run_test_case, run_test_file, TestError
)
from aqueduct.parser.models import Module

def test_create_df_supported_types(spark: SparkSession):
    schema = {
        "c_long": "long",
        "c_string": "string",
        "c_double": "double",
        "c_boolean": "boolean",
        "c_timestamp": "timestamp",
        "c_date": "date"
    }
    rows = [
        [1, "foo", 1.5, True, datetime(2026, 1, 1, 12, 0, 0), date(2026, 1, 1)],
        [2, None, 2.0, False, None, None]
    ]
    df = _create_df(spark, schema, rows)
    
    assert df.count() == 2
    types = dict(df.dtypes)
    assert types["c_long"] == "bigint"
    assert types["c_string"] == "string"
    assert types["c_double"] == "double"
    assert types["c_boolean"] == "boolean"
    assert types["c_timestamp"] == "timestamp"
    assert types["c_date"] == "date"

def test_create_df_unknown_type(spark: SparkSession):
    # unknown schema type -> passes through to Spark DDL
    # If Spark doesn't know it, it will raise. But our _spark_type just returns the string as is.
    # Let's use a type Spark knows but we don't have in _TYPE_MAP, like 'float' (which is in map) 
    # or something truly weird.
    schema = {"c": "map<string,string>"}
    rows = [[{"a": "b"}]]
    df = _create_df(spark, schema, rows)
    assert "map<string,string>" in df.schema["c"].dataType.simpleString()

def test_assertion_row_count(spark: SparkSession):
    df = spark.range(5)
    
    # Pass
    res = _run_assertion({"type": "row_count", "expected": 5}, df, spark)
    assert res.passed is True
    assert "expected 5" in res.message
    
    # Fail
    res = _run_assertion({"type": "row_count", "expected": 10}, df, spark)
    assert res.passed is False
    assert "expected 10 rows, got 5" in res.message

def test_assertion_contains(spark: SparkSession):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    
    # Pass
    res = _run_assertion({"type": "contains", "rows": [{"id": 1, "val": "a"}]}, df, spark)
    assert res.passed is True
    
    # Fail
    res = _run_assertion({"type": "contains", "rows": [{"id": 3, "val": "c"}]}, df, spark)
    assert res.passed is False
    assert "1 expected row(s) not found" in res.message
    
    # Multiple rows, one missing
    res = _run_assertion({"type": "contains", "rows": [{"id": 1}, {"id": 3}]}, df, spark)
    assert res.passed is False
    assert "{'id': 3}" in res.message

def test_assertion_sql(spark: SparkSession):
    df = spark.range(5)
    
    # Pass
    res = _run_assertion({"type": "sql", "expr": "SELECT count(*) = 5 FROM __output__"}, df, spark)
    assert res.passed is True
    
    # Fail (falsy result)
    res = _run_assertion({"type": "sql", "expr": "SELECT count(*) = 10 FROM __output__"}, df, spark)
    assert res.passed is False
    
    # Fail (empty result)
    res = _run_assertion({"type": "sql", "expr": "SELECT * FROM __output__ WHERE id > 100"}, df, spark)
    assert res.passed is False
    assert "returned no rows" in res.message
    
    # Error (bad SQL)
    res = _run_assertion({"type": "sql", "expr": "SELECT broken"}, df, spark)
    assert res.passed is False
    assert "sql expr error" in res.message

def test_execute_module_dispatch(spark: SparkSession):
    # Channel
    m = Module(id="m1", type="Channel", label="L", config={"op": "sql", "query": "SELECT * FROM in1"})
    in1 = spark.range(3)
    res = _execute_module(m, {"in1": in1}, spark)
    assert res.count() == 3
    
    # Assert
    m_assert = Module(id="a1", type="Assert", label="L", config={"rules": [{"type": "sql_row", "expr": "id > 0"}]})
    res_assert = _execute_module(m_assert, {"in": spark.createDataFrame([(0,), (1,)], ["id"])}, spark)
    assert res_assert.count() == 1 # only id=1 passes
    
    # Ingress (TestError)
    m_ingress = Module(id="i1", type="Ingress", label="L", config={})
    with pytest.raises(TestError, match="tested in isolation"):
        _execute_module(m_ingress, {}, spark)

def test_run_test_case_validation(spark: SparkSession):
    # missing module
    res = _run_test_case({"id": "t1"}, {}, spark)
    assert res.passed is False
    assert "missing 'module'" in res.error
    
    # module not in blueprint
    res = _run_test_case({"id": "t1", "module": "m1"}, {}, spark)
    assert res.passed is False
    assert "not found in blueprint" in res.error
    
    # missing inputs
    m1 = Module(id="m1", type="Channel", label="L", config={"op": "sql", "query": "..."})
    res = _run_test_case({"id": "t1", "module": "m1"}, {"m1": m1}, spark)
    assert res.passed is False
    assert "missing 'inputs'" in res.error

def test_run_test_case_junction(spark: SparkSession):
    # Junction returns a dict of DataFrames
    m = Module(id="j1", type="Junction", label="J", config={
        "mode": "conditional",
        "branches": [
            {"id": "b1", "condition": "id > 0"},
            {"id": "b2", "condition": "id <= 0"}
        ]
    })
    test_case = {
        "id": "tj",
        "module": "j1",
        "inputs": {
            "in": {"schema": {"id": "int"}, "rows": [[1], [0]]}
        },
        "assertions": [{"type": "row_count", "expected": 1}]
    }
    
    # Default branch (b1)
    res = _run_test_case(test_case, {"j1": m}, spark)
    assert res.passed is True
    
    # Specific branch (b2)
    test_case["branch"] = "b2"
    res = _run_test_case(test_case, {"j1": m}, spark)
    assert res.passed is True
    
    # Non-existent branch
    test_case["branch"] = "unknown"
    res = _run_test_case(test_case, {"j1": m}, spark)
    assert res.passed is False
    assert "branch 'unknown' not found" in res.error

def test_run_test_file_not_found(spark: SparkSession):
    with pytest.raises(FileNotFoundError):
        run_test_file(Path("non_existent"), spark)
    
    # Test file with no blueprint
    (Path("test.yml")).write_text("tests: []", encoding="utf-8")
    with pytest.raises(TestError, match="missing 'blueprint'"):
        run_test_file(Path("test.yml"), spark)
    Path("test.yml").unlink()
