"""Tests for the isolated test runner (aqueduct test)."""

from __future__ import annotations

from pathlib import Path
import pytest
pytestmark = [pytest.mark.spark, pytest.mark.integration]
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, DateType
from datetime import datetime, date

from aqueduct.executor.spark.test_runner import (
    _create_df, _run_assertion, _execute_module, _run_test_case, run_test_file, TestSchemaError
)
from aqueduct.parser.models import Module

def test_create_df_supported_types(spark: SparkSession):
    schema = {
        "c_bigint": "bigint",
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
    assert types["c_bigint"] == "bigint"
    assert types["c_string"] == "string"
    assert types["c_double"] == "double"
    assert types["c_boolean"] == "boolean"
    assert types["c_timestamp"] == "timestamp"
    assert types["c_date"] == "date"

def test_create_df_unknown_type(spark: SparkSession):
    # Schema types are Spark SQL DDL types passed directly to CREATE TEMP VIEW.
    # Spark rejects unknown types with a clear ParseException.
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
    with pytest.raises(TestSchemaError, match="tested in isolation"):
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
    with pytest.raises(TestSchemaError, match="missing 'blueprint'"):
        run_test_file(Path("test.yml"), spark)
    Path("test.yml").unlink()


def test_bundled_template_conformance(spark: SparkSession, tmp_path: Path):
    import yaml
    from aqueduct.executor.spark.test_runner import run_test_file

    # 1. Locate the aqtest.yml.template file
    template_path = Path(__file__).parents[2] / "aqueduct" / "templates" / "default" / "aqtests" / "aqtest.yml.template"
    assert template_path.exists(), f"Template not found at {template_path}"

    # 2. Parse the template as YAML
    template_content = template_path.read_text(encoding="utf-8")
    raw = yaml.safe_load(template_content)

    # 3. Assert its structure
    assert raw.get("aqueduct_test") == "1.0", f"Unexpected aqueduct_test version: {raw.get('aqueduct_test')}"
    assert raw.get("blueprint") == "blueprint.yml", f"Unexpected blueprint: {raw.get('blueprint')}"
    
    tests = raw.get("tests")
    assert isinstance(tests, list) and len(tests) > 0, "No tests list or empty in template"

    for idx, test_case in enumerate(tests):
        test_id = test_case.get("id", f"test_{idx}")
        assert isinstance(test_case.get("module"), str) and test_case["module"], f"Test {test_id} missing non-empty 'module' string"
        
        inputs = test_case.get("inputs")
        assert isinstance(inputs, dict) and inputs, f"Test {test_id} inputs must be a non-empty mapping"
        
        for input_id, input_spec in inputs.items():
            assert isinstance(input_spec.get("schema"), dict) and input_spec["schema"], f"Input {input_id} in {test_id} must have a non-empty 'schema' dict"
            assert isinstance(input_spec.get("rows"), list), f"Input {input_id} in {test_id} must have a 'rows' list"
            
        assertions = test_case.get("assertions")
        assert isinstance(assertions, list) and assertions, f"Test {test_id} must have a non-empty assertions list"
        for a_idx, assertion in enumerate(assertions):
            assert assertion.get("type") in {"row_count", "contains", "sql"}, f"Assertion {a_idx} in {test_id} has invalid/unknown type {assertion.get('type')}"

    # 4. Construct a valid minimal dummy blueprint.yml
    bp_content = """
aqueduct: '1.0'
id: bp1
name: Test Blueprint
modules:
  - id: dedup_latest
    type: Channel
    label: L
    config:
      op: sql
      query: |
        SELECT user_id, version, data
        FROM (
          SELECT *, row_number() OVER (PARTITION BY user_id ORDER BY version DESC) as rn
          FROM raw_users
        )
        WHERE rn = 1
edges: []
"""
    (tmp_path / "blueprint.yml").write_text(bp_content, encoding="utf-8")
    
    # Write the template content into a test file in the temp path
    test_file_path = tmp_path / "aqtest.yml"
    test_file_path.write_text(template_content, encoding="utf-8")

    # Run the test file
    suite_result = run_test_file(test_file_path, spark)
    assert suite_result.total == 1
    assert suite_result.passed == 1
    assert suite_result.failed == 0
    assert suite_result.success is True

