"""Tests for the Funnel executor."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from aqueduct.executor.funnel import FunnelError, execute_funnel
from aqueduct.parser.models import Module


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[*]").appName("test-funnel").getOrCreate()


@pytest.fixture
def dfs(spark):
    df1 = spark.createDataFrame([("r1", 10)], ["id", "val"])
    df2 = spark.createDataFrame([("r2", 20), ("r1", 10)], ["id", "val"])
    df3_diff_schema = spark.createDataFrame([("r3", 30, "cat")], ["id", "val", "extra"])
    df4_diff_cols = spark.createDataFrame([("a1", "red")], ["other_id", "color"])
    
    return {
        "m1": df1,
        "m2": df2,
        "m3": df3_diff_schema,
        "m4": df4_diff_cols,
    }


def test_funnel_missing_or_unsupported_mode(dfs):
    module = Module(label="M", id="f1", type="Funnel", config={"inputs": ["m1", "m2"]})
    with pytest.raises(FunnelError, match="unsupported mode"):
        execute_funnel(module, dfs)
        
    module.config["mode"] = "alien"
    with pytest.raises(FunnelError, match="unsupported mode"):
        execute_funnel(module, dfs)


def test_funnel_missing_inputs(dfs):
    module = Module(label="M", id="f1", type="Funnel", config={"mode": "union_all"})
    with pytest.raises(FunnelError, match="'inputs' must list at least two"):
        execute_funnel(module, dfs)
        
    module.config["inputs"] = ["m1"]
    with pytest.raises(FunnelError, match="'inputs' must contain at least 2 entries"):
        execute_funnel(module, dfs)


def test_funnel_unknown_input(dfs):
    module = Module(label="M", id="f1", type="Funnel", config={"mode": "union_all", "inputs": ["m1", "ghost"]})
    with pytest.raises(FunnelError, match="upstream module 'ghost' not found"):
        execute_funnel(module, dfs)


# ── union_all ────────────────────────────────────────────────────────────────

def test_union_all_strict(dfs):
    module = Module(label="M", id="f1", type="Funnel", config={"mode": "union_all", "inputs": ["m1", "m2"]})
    res = execute_funnel(module, dfs)
    assert res.count() == 3


def test_union_all_strict_mismatched(dfs):
    module = Module(label="M", id="f1", type="Funnel", config={"mode": "union_all", "inputs": ["m1", "m3"]})
    with pytest.raises(FunnelError, match="union_all failed"):
        execute_funnel(module, dfs)


def test_union_all_permissive(dfs):
    cfg = {"mode": "union_all", "inputs": ["m1", "m3"], "schema_check": "permissive"}
    module = Module(label="M", id="f1", type="Funnel", config=cfg)
    res = execute_funnel(module, dfs)
    assert res.count() == 2
    # m1 should have null in extra column
    nulls = res.filter("id = 'r1'").select("extra").collect()
    assert nulls[0][0] is None


# ── union ────────────────────────────────────────────────────────────────────

def test_union(dfs):
    # m1 has ("r1", 10), m2 has ("r2", 20) and ("r1", 10)
    # total after union distinct should be 2
    module = Module(label="M", id="f1", type="Funnel", config={"mode": "union", "inputs": ["m1", "m2"]})
    res = execute_funnel(module, dfs)
    assert res.count() == 2


# ── coalesce ─────────────────────────────────────────────────────────────────

def test_coalesce_row_aligned(spark):
    dfa = spark.createDataFrame([(1, None, "a"), (2, "b", None)], ["id", "c1", "c2"])
    dfb = spark.createDataFrame([(1, "x", "y"), (3, None, "z")], ["id", "c1", "c2"])
    
    cfg = {"mode": "coalesce", "inputs": ["a", "b"]}
    module = Module(label="M", id="f1", type="Funnel", config=cfg)
    
    res = execute_funnel(module, {"a": dfa, "b": dfb})
    rows = res.orderBy("id").collect()
    
    # 1: c1 = 'x' (from dfb, dfa was None), c2 = 'a' (from dfa)
    # 2: id=2 from dfa, id=3 from dfb ... wait, row-aligned means they just join by row position.
    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[0]["c1"] == "x"
    assert rows[0]["c2"] == "a"
    
    # second row
    assert rows[1]["id"] == 2  # from base dfa
    assert rows[1]["c1"] == "b"
    assert rows[1]["c2"] == "z"


# ── zip ──────────────────────────────────────────────────────────────────────

def test_zip_row_aligned(spark):
    dfa = spark.createDataFrame([(1, "a")], ["id1", "col_a"])
    dfb = spark.createDataFrame([(2, "b")], ["id2", "col_b"])
    
    cfg = {"mode": "zip", "inputs": ["a", "b"]}
    module = Module(label="M", id="f1", type="Funnel", config=cfg)
    res = execute_funnel(module, {"a": dfa, "b": dfb})
    
    row = res.collect()[0]
    assert row["id1"] == 1
    assert row["col_a"] == "a"
    assert row["id2"] == 2
    assert row["col_b"] == "b"


def test_zip_conflicting_columns(spark):
    dfa = spark.createDataFrame([(1, "a")], ["id", "col_a"])
    dfb = spark.createDataFrame([(2, "b")], ["id", "col_b"])
    
    cfg = {"mode": "zip", "inputs": ["a", "b"]}
    module = Module(label="M", id="f1", type="Funnel", config=cfg)
    with pytest.raises(FunnelError, match="duplicate column: 'id'"):
        execute_funnel(module, {"a": dfa, "b": dfb})
