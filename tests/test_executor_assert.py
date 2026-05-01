"""Tests for the Assert executor."""

import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from aqueduct.executor.spark.assert_ import execute_assert, AssertError
from aqueduct.parser.models import Module


def test_schema_match_passes(spark: SparkSession):
    df = spark.range(5).withColumn("name", F.lit("test"))
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "schema_match", "expected": {"id": "bigint", "name": "string"}}]}
    )
    # Zero Spark action
    passing, quarantine = execute_assert(module, df, spark, "run-1", "blueprint-1")
    assert quarantine is None
    assert passing is df


def test_schema_match_fails_missing_column(spark: SparkSession):
    df = spark.range(5)
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "schema_match", "expected": {"id": "bigint", "name": "string"}, "on_fail": "abort"}]}
    )
    with pytest.raises(AssertError, match=r"schema_match: missing columns \['name'\]"):
        execute_assert(module, df, spark, "run-1", "blueprint-1")


def test_schema_match_fails_wrong_type(spark: SparkSession):
    df = spark.range(5).withColumn("name", F.lit(123))
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "schema_match", "expected": {"id": "bigint", "name": "string"}, "on_fail": "abort"}]}
    )
    with pytest.raises(AssertError, match=r"schema_match: type mismatches \['name: expected string, got int'\]"):
        execute_assert(module, df, spark, "run-1", "blueprint-1")


def test_min_rows_passes(spark: SparkSession):
    df = spark.range(10)
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "min_rows", "min": 5}]}
    )
    passing, quarantine = execute_assert(module, df, spark, "run-1", "blueprint-1")
    assert quarantine is None


def test_min_rows_fails(spark: SparkSession):
    df = spark.range(3)
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "min_rows", "min": 5, "on_fail": "abort"}]}
    )
    with pytest.raises(AssertError, match=r"min_rows: got 3, expected >= 5"):
        execute_assert(module, df, spark, "run-1", "blueprint-1")


def test_max_rows_fails_warn(spark: SparkSession, caplog):
    df = spark.range(10)
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "max_rows", "max": 5, "on_fail": "warn"}]}
    )
    passing, quarantine = execute_assert(module, df, spark, "run-1", "blueprint-1")
    assert quarantine is None
    assert "max_rows: got 10, expected <= 5" in caplog.text


def test_null_rate_passes(spark: SparkSession):
    df = spark.range(10).withColumn("val", F.when(F.col("id") < 2, None).otherwise(1))
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "null_rate", "column": "val", "max": 0.5}]}
    )
    passing, quarantine = execute_assert(module, df, spark, "run-1", "blueprint-1")
    assert quarantine is None


def test_null_rate_fails(spark: SparkSession):
    # Create deterministic nulls to ensure the sample failure
    df = spark.range(100).withColumn("val", F.lit(None).cast("int"))
    module = Module(
        id="a1", type="Assert", label="A1",
        # Set fraction=1.0 to ensure deterministic sample
        config={"rules": [{"type": "null_rate", "column": "val", "max": 0.1, "fraction": 1.0, "on_fail": "abort"}]}
    )
    with pytest.raises(AssertError, match=r"null_rate\[val\]: 100.0000% > allowed 10.0000%"):
        execute_assert(module, df, spark, "run-1", "blueprint-1")


def test_null_rate_on_fail_quarantine_warns(spark: SparkSession, caplog):
    df = spark.range(100).withColumn("val", F.lit(None).cast("int"))
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "null_rate", "column": "val", "max": 0.1, "fraction": 1.0, "on_fail": "quarantine"}]}
    )
    passing, quarantine = execute_assert(module, df, spark, "run-1", "blueprint-1")
    assert quarantine is None
    assert "on_fail=quarantine used on aggregate rule; treated as warn." in caplog.text
    assert "null_rate[val]" in caplog.text


def test_freshness_passes(spark: SparkSession):
    df = spark.range(10).withColumn("ts", F.current_timestamp())
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "freshness", "column": "ts", "max_age_hours": 24}]}
    )
    passing, quarantine = execute_assert(module, df, spark, "run-1", "blueprint-1")
    assert quarantine is None


def test_freshness_fails_warn(spark: SparkSession, caplog):
    df = spark.range(10).withColumn("ts", F.current_timestamp() - F.expr("INTERVAL 48 HOURS"))
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "freshness", "column": "ts", "max_age_hours": 24, "on_fail": "warn"}]}
    )
    passing, quarantine = execute_assert(module, df, spark, "run-1", "blueprint-1")
    assert quarantine is None
    assert "freshness: data is" in caplog.text
    assert "old, max allowed 24.0h" in caplog.text


def test_freshness_all_nulls(spark: SparkSession):
    df = spark.range(10).withColumn("ts", F.lit(None).cast("timestamp"))
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "freshness", "column": "ts", "max_age_hours": 24, "on_fail": "abort"}]}
    )
    with pytest.raises(AssertError, match=r"freshness: column has no non-null values"):
        execute_assert(module, df, spark, "run-1", "blueprint-1")


def test_sql_rule_passes(spark: SparkSession):
    df = spark.range(10).withColumn("amt", F.col("id") * 10)
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "sql", "expr": "sum(amt) == 450"}]}
    )
    passing, quarantine = execute_assert(module, df, spark, "run-1", "blueprint-1")
    assert quarantine is None


@patch("aqueduct.executor.spark.assert_._fire_rule_webhook")
def test_sql_rule_fails_webhook(mock_fire, spark: SparkSession):
    df = spark.range(10).withColumn("amt", F.col("id") * 10)
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "sql", "expr": "sum(amt) > 1000", "on_fail": {"action": "webhook", "url": "http://test"}}]}
    )
    passing, quarantine = execute_assert(module, df, spark, "run-1", "blueprint-1")
    assert quarantine is None
    mock_fire.assert_called_once()
    assert mock_fire.call_args[0][0] == "http://test"
    assert "sql assertion failed" in mock_fire.call_args[0][3]


def test_sql_row_rule_quarantine(spark: SparkSession):
    df = spark.range(10)
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "sql_row", "expr": "id % 2 == 0", "on_fail": "quarantine"}]}
    )
    passing, quarantine = execute_assert(module, df, spark, "run-1", "blueprint-1")
    assert passing.count() == 5
    assert quarantine.count() == 5
    assert "_aq_error_module" in quarantine.columns
    assert "_aq_error_msg" in quarantine.columns


def test_sql_row_rule_abort(spark: SparkSession):
    df = spark.range(10)
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "sql_row", "expr": "id > 20", "on_fail": "abort"}]}
    )
    with pytest.raises(AssertError, match=r"failed: id > 20 \(10 rows\)"):
        execute_assert(module, df, spark, "run-1", "blueprint-1")


def test_custom_rule_passes(spark: SparkSession, tmp_path):
    fn_path = tmp_path / "my_custom.py"
    fn_path.write_text("def my_check(df):\n    return {'passed': True}\n")
    
    import sys
    sys.path.insert(0, str(tmp_path))
    
    df = spark.range(10)
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "custom", "fn": "my_custom.my_check"}]}
    )
    passing, quarantine = execute_assert(module, df, spark, "run-1", "blueprint-1")
    assert quarantine is None
    
    sys.path.remove(str(tmp_path))


def test_custom_rule_quarantine(spark: SparkSession, tmp_path):
    fn_path = tmp_path / "my_custom2.py"
    fn_path.write_text(
        "def my_check(df):\n"
        "    return {'passed': False, 'message': 'test failed', 'quarantine_df': df.filter('id < 2')}\n"
    )
    
    import sys
    sys.path.insert(0, str(tmp_path))
    
    df = spark.range(5)
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "custom", "fn": "my_custom2.my_check", "on_fail": "quarantine"}]}
    )
    passing, quarantine = execute_assert(module, df, spark, "run-1", "blueprint-1")
    assert quarantine.count() == 2
    assert "_aq_error_module" in quarantine.columns
    assert quarantine.select("_aq_error_msg").first()[0] == "test failed"
    
    sys.path.remove(str(tmp_path))


def test_custom_rule_raises_exception(spark: SparkSession, tmp_path, caplog):
    fn_path = tmp_path / "my_custom3.py"
    fn_path.write_text("def my_check(df):\n    raise ValueError('boom')\n")
    
    import sys
    sys.path.insert(0, str(tmp_path))
    
    df = spark.range(5)
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "custom", "fn": "my_custom3.my_check"}]}
    )
    passing, quarantine = execute_assert(module, df, spark, "run-1", "blueprint-1")
    assert quarantine is None
    assert "custom rule 'my_custom3.my_check' raised: boom" in caplog.text
    
    sys.path.remove(str(tmp_path))


def test_custom_rule_bad_fn_path(spark: SparkSession):
    df = spark.range(5)
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "custom", "fn": "nonexistent.my_check", "on_fail": "abort"}]}
    )
    with pytest.raises(AssertError, match=r"cannot import 'nonexistent'"):
        execute_assert(module, df, spark, "run-1", "blueprint-1")


def test_multiple_aggregate_rules_batched(spark: SparkSession):
    # Checking that min_rows + freshness + sql triggers exactly 1 action is hard via test assertions
    # but we can verify it doesn't fail.
    df = spark.range(10).withColumn("ts", F.current_timestamp()).withColumn("amt", F.col("id") * 10)
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [
            {"type": "min_rows", "min": 5},
            {"type": "freshness", "column": "ts", "max_age_hours": 24},
            {"type": "sql", "expr": "sum(amt) == 450"}
        ]}
    )
    passing, quarantine = execute_assert(module, df, spark, "run-1", "blueprint-1")
    assert quarantine is None


def test_mixed_aggregate_null_rate(spark: SparkSession):
    df = spark.range(10).withColumn("val", F.lit(1))
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [
            {"type": "min_rows", "min": 5},
            {"type": "null_rate", "column": "val", "max": 0.5, "fraction": 1.0}
        ]}
    )
    passing, quarantine = execute_assert(module, df, spark, "run-1", "blueprint-1")
    assert quarantine is None


def test_on_fail_trigger_agent(spark: SparkSession):
    df = spark.range(3)
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "min_rows", "min": 5, "on_fail": "trigger_agent"}]}
    )
    with pytest.raises(AssertError, match=r"min_rows: got 3, expected >= 5") as exc_info:
        execute_assert(module, df, spark, "run-1", "blueprint-1")
    assert exc_info.value.trigger_agent is True


def test_no_rules_configured(spark: SparkSession):
    df = spark.range(5)
    module = Module(
        id="a1", type="Assert", label="A1",
        config={}
    )
    passing, quarantine = execute_assert(module, df, spark, "run-1", "blueprint-1")
    assert quarantine is None
    assert passing is df
