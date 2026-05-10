"""Tests for the Funnel executor."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from aqueduct.executor.spark.funnel import FunnelError, execute_funnel
from aqueduct.parser.models import Module



@pytest.fixture
def dfs(spark):
    df1 = spark.sql("SELECT 'r1' as id, 10 as val")
    df2 = spark.sql("SELECT 'r2' as id, 20 as val UNION ALL SELECT 'r1', 10")
    df3_diff_schema = spark.sql("SELECT 'r3' as id, 30 as val, 'cat' as extra")
    df4_diff_cols = spark.sql("SELECT 'a1' as other_id, 'red' as color")
    
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
    dfa = spark.sql("SELECT 1 as id, CAST(NULL as STRING) as c1, 'a' as c2 UNION ALL SELECT 2, 'b', NULL")
    dfb = spark.sql("SELECT 1 as id, 'x' as c1, 'y' as c2 UNION ALL SELECT 3, NULL, 'z'")
    
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
    dfa = spark.sql("SELECT 1 as id1, 'a' as col_a")
    dfb = spark.sql("SELECT 2 as id2, 'b' as col_b")
    
    cfg = {"mode": "zip", "inputs": ["a", "b"]}
    module = Module(label="M", id="f1", type="Funnel", config=cfg)
    res = execute_funnel(module, {"a": dfa, "b": dfb})
    
    row = res.collect()[0]
    assert row["id1"] == 1
    assert row["col_a"] == "a"
    assert row["id2"] == 2
    assert row["col_b"] == "b"


def test_zip_conflicting_columns(spark):
    dfa = spark.sql("SELECT 1 as id, 'a' as col_a")
    dfb = spark.sql("SELECT 2 as id, 'b' as col_b")
    
    cfg = {"mode": "zip", "inputs": ["a", "b"]}
    module = Module(label="M", id="f1", type="Funnel", config=cfg)
    with pytest.raises(FunnelError, match="duplicate column: 'id'"):
        execute_funnel(module, {"a": dfa, "b": dfb})
