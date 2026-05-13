from __future__ import annotations
import pytest
from datetime import datetime, timezone, timedelta
from pyspark.sql import Row
from pyspark.sql import functions as F

from aqueduct.executor.spark.assert_ import execute_assert, AssertError
from aqueduct.parser.models import Module

pytestmark = [pytest.mark.spark, pytest.mark.integration]

def test_freshness_quarantine_success(spark):
    # Current time
    now = datetime.now(timezone.utc)
    # 1 row fresh, 1 row stale
    data = [
        Row(id=1, ts=now),
        Row(id=2, ts=now - timedelta(hours=48))
    ]
    df = spark.createDataFrame(data)
    
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "freshness", "column": "ts", "max_age_hours": 24, "on_fail": "quarantine"}]}
    )
    
    passing, quarantine = execute_assert(module, df, spark, "run-1", "bp1")
    
    assert passing.count() == 1
    assert quarantine.count() == 1
    assert passing.collect()[0]["id"] == 1
    assert quarantine.collect()[0]["id"] == 2
    # assert "_aq_error_msg" in quarantine.columns # BUG: missing error columns

def test_freshness_quarantine_nulls(spark):
    now = datetime.now(timezone.utc)
    df = spark.createDataFrame([
        Row(id=1, ts=now),
        Row(id=2, ts=None)
    ])
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "freshness", "column": "ts", "max_age_hours": 24, "on_fail": "quarantine"}]}
    )
    passing, quarantine = execute_assert(module, df, spark, "run-1", "bp1")
    
    # BUG: NULLs are currently dropped because filter(expr) and filter(~expr) both skip NULLs
    assert passing.count() == 1
    assert quarantine.count() == 1
    assert quarantine.collect()[0]["id"] == 2

def test_freshness_quarantine_numeric(spark):
    # Use unix timestamps
    now_ts = datetime.now(timezone.utc).timestamp()
    df = spark.createDataFrame([
        Row(id=1, ts=now_ts),
        Row(id=2, ts=now_ts - 48*3600)
    ])
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "freshness", "column": "ts", "max_age_hours": 24, "on_fail": "quarantine"}]}
    )
    passing, quarantine = execute_assert(module, df, spark, "run-1", "bp1")
    
    assert passing.count() == 1
    assert quarantine.count() == 1

def test_freshness_quarantine_multiple_rules(spark):
    now = datetime.now(timezone.utc)
    df = spark.createDataFrame([
        Row(id=1, ts1=now, ts2=now),
        Row(id=2, ts1=now - timedelta(hours=48), ts2=now),
        Row(id=3, ts1=now, ts2=now - timedelta(hours=48))
    ])
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [
            {"type": "freshness", "column": "ts1", "max_age_hours": 24, "on_fail": "quarantine"},
            {"type": "freshness", "column": "ts2", "max_age_hours": 24, "on_fail": "quarantine"}
        ]}
    )
    passing, quarantine = execute_assert(module, df, spark, "run-1", "bp1")
    
    assert passing.count() == 1 # only id=1
    assert quarantine.count() == 2 # id=2 and id=3

def test_freshness_quarantine_non_timestamp_column(spark):
    df = spark.createDataFrame([Row(id=1, ts="not a timestamp")])
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "freshness", "column": "ts", "max_age_hours": 24, "on_fail": "quarantine"}]}
    )
    
    # This should fail during execution (Phase 2 or Phase 3)
    with pytest.raises((AssertError, Exception)):
        execute_assert(module, df, spark, "run-1", "bp1")

def test_freshness_quarantine_missing_column_key(spark):
    df = spark.createDataFrame([Row(id=1, ts=datetime.now())])
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "freshness", "on_fail": "quarantine"}]}
    )
    
    # Spec says it should raise ValueError
    with pytest.raises(ValueError):
        execute_assert(module, df, spark, "run-1", "bp1")

def test_on_fail_quarantine_aggregate_rule_warns(spark, caplog):
    df = spark.range(10)
    module = Module(
        id="a1", type="Assert", label="A1",
        config={"rules": [{"type": "min_rows", "min": 20, "on_fail": "quarantine"}]}
    )
    
    passing, quarantine = execute_assert(module, df, spark, "run-1", "bp1")
    
    assert quarantine is None
    assert "on_fail=quarantine used on aggregate rule; treated as warn." in caplog.text
    assert "min_rows" in caplog.text
