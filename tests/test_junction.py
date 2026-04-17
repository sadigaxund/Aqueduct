"""Tests for the Junction executor."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from aqueduct.executor.junction import JunctionError, execute_junction
from aqueduct.parser.models import Module


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[*]").appName("test-junction").getOrCreate()


@pytest.fixture
def df(spark):
    return spark.createDataFrame(
        [
            ("r1", "DE", 10),
            ("r2", "US", 20),
            ("r3", "FR", 30),
            ("r4", "JP", 40),
            ("r5", "US", 50),
        ],
        ["id", "region", "val"],
    )


def test_execute_junction_unsupported_mode(df):
    module = Module(label="M", id="j1", type="Junction", config={"mode": "alien"})
    with pytest.raises(JunctionError, match="unsupported mode"):
        execute_junction(module, df)


def test_execute_junction_missing_mode(df):
    module = Module(label="M", id="j1", type="Junction", config={})
    with pytest.raises(JunctionError, match="unsupported mode"):
        execute_junction(module, df)


def test_execute_junction_empty_branches(df):
    module = Module(label="M", id="j1", type="Junction", config={"mode": "broadcast", "branches": []})
    with pytest.raises(JunctionError, match="'branches' must not be empty"):
        execute_junction(module, df)


def test_execute_junction_branch_missing_id(df):
    module = Module(label="M", id="j1", type="Junction", config={
        "mode": "broadcast",
        "branches": [{"condition": "x"}]
    })
    with pytest.raises(JunctionError, match="every branch must have an 'id'"):
        execute_junction(module, df)


# ── Conditional Mode ─────────────────────────────────────────────────────────

def test_conditional_missing_condition(df):
    module = Module(label="M", id="j1", type="Junction", config={
        "mode": "conditional",
        "branches": [{"id": "b1"}]
    })
    with pytest.raises(JunctionError, match="missing 'condition' for conditional mode"):
        execute_junction(module, df)


def test_conditional_explicit_and_else(df):
    cfg = {
        "mode": "conditional",
        "branches": [
            {"id": "eu", "condition": "region IN ('DE', 'FR')"},
            {"id": "us", "condition": "region = 'US'"},
            {"id": "other", "condition": "_else_"}
        ]
    }
    module = Module(label="M", id="j1", type="Junction", config=cfg)
    res = execute_junction(module, df)
    
    eu_rows = res["eu"].collect()
    us_rows = res["us"].collect()
    other_rows = res["other"].collect()
    
    assert len(eu_rows) == 2
    assert {"DE", "FR"} == {r.region for r in eu_rows}
    
    assert len(us_rows) == 2
    assert {r.region for r in us_rows} == {"US"}
    
    assert len(other_rows) == 1
    assert other_rows[0].region == "JP"


def test_conditional_else_only(df):
    cfg = {
        "mode": "conditional",
        "branches": [{"id": "fallback", "condition": "_else_"}]
    }
    module = Module(label="M", id="j1", type="Junction", config=cfg)
    res = execute_junction(module, df)
    assert res["fallback"].count() == 5  # Unfiltered


# ── Broadcast Mode ───────────────────────────────────────────────────────────

def test_broadcast(df):
    cfg = {
        "mode": "broadcast",
        "branches": [{"id": "b1"}, {"id": "b2"}]
    }
    module = Module(label="M", id="j1", type="Junction", config=cfg)
    res = execute_junction(module, df)
    
    assert "b1" in res
    assert "b2" in res
    assert res["b1"].count() == 5
    assert res["b2"].count() == 5
    assert res["b1"] is df
    assert res["b2"] is df


# ── Partition Mode ───────────────────────────────────────────────────────────

def test_partition_missing_key(df):
    cfg = {
        "mode": "partition",
        "branches": [{"id": "US"}]
    }
    module = Module(label="M", id="j1", type="Junction", config=cfg)
    with pytest.raises(JunctionError, match="'partition_key' is required"):
        execute_junction(module, df)


def test_partition_fallback_to_id(df):
    cfg = {
        "mode": "partition",
        "partition_key": "region",
        "branches": [{"id": "US"}, {"id": "DE"}]
    }
    module = Module(label="M", id="j1", type="Junction", config=cfg)
    res = execute_junction(module, df)
    
    assert res["US"].count() == 2
    assert res["DE"].count() == 1


def test_partition_explicit_value(df):
    cfg = {
        "mode": "partition",
        "partition_key": "region",
        "branches": [{"id": "b1", "value": "US"}, {"id": "b2", "value": "JP"}]
    }
    module = Module(label="M", id="j1", type="Junction", config=cfg)
    res = execute_junction(module, df)
    
    assert res["b1"].count() == 2
    assert res["b2"].count() == 1
