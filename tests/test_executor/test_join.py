"""Tests for the Executor layer: SQL Join operation."""

from __future__ import annotations
from pathlib import Path
import pytest
from pyspark.sql import SparkSession

from aqueduct.executor.spark.channel import ChannelError, execute_sql_channel
from aqueduct.parser.models import Module

FIXTURES = Path(__file__).parent.parent / "fixtures"


class TestJoinOperation:
    def test_join_missing_left(self, spark: SparkSession):
        module = Module(id="chan", type="Channel", label="C", config={
            "op": "join", "right": "source_b", "condition": "a.id = b.id"
        })
        with pytest.raises(ChannelError, match="requires 'left'"):
            execute_sql_channel(module, {"source_a": spark.range(1)}, spark)

    def test_join_missing_right(self, spark: SparkSession):
        module = Module(id="chan", type="Channel", label="C", config={
            "op": "join", "left": "source_a", "condition": "a.id = b.id"
        })
        with pytest.raises(ChannelError, match="requires 'right'"):
            execute_sql_channel(module, {"source_a": spark.range(1)}, spark)

    def test_join_invalid_type(self, spark: SparkSession):
        module = Module(id="chan", type="Channel", label="C", config={
            "op": "join", "left": "source_a", "right": "source_b", "join_type": "invalid"
        })
        with pytest.raises(ChannelError, match="invalid join_type 'invalid'"):
            execute_sql_channel(module, {"source_a": spark.range(1)}, spark)

    def test_join_missing_condition(self, spark: SparkSession):
        module = Module(id="chan", type="Channel", label="C", config={
            "op": "join", "left": "source_a", "right": "source_b", "join_type": "inner"
        })
        with pytest.raises(ChannelError, match="requires 'condition'"):
            execute_sql_channel(module, {"source_a": spark.range(1)}, spark)

    def test_join_cross_no_condition(self, spark: SparkSession):
        df1 = spark.range(1)
        df2 = spark.range(1)
        module = Module(id="chan", type="Channel", label="C", config={
            "op": "join", "left": "a", "right": "b", "join_type": "cross"
        })
        res = execute_sql_channel(module, {"a": df1, "b": df2}, spark)
        assert res.count() == 1

    def test_join_broadcast_hint(self, spark: SparkSession):
        df1 = spark.range(1)
        df2 = spark.range(1)
        module = Module(id="chan", type="Channel", label="C", config={
            "op": "join", "left": "a", "right": "b", "condition": "a.id = b.id", "broadcast_side": "right"
        })
        res = execute_sql_channel(module, {"a": df1, "b": df2}, spark)
        assert res.count() == 1

    @pytest.mark.usefixtures("spark")
    def test_join_end_to_end(self, spark, tmp_path):
        from aqueduct.compiler.compiler import compile as compiler_compile
        from aqueduct.parser.parser import parse
        from aqueduct.executor.spark.executor import execute
        
        p1 = str(tmp_path / "t1.parquet")
        p2 = str(tmp_path / "t2.parquet")
        p_out = str(tmp_path / "out.parquet")
        spark.range(1, 3).selectExpr("id as id1", "case when id=1 then 'A' else 'B' end as name").write.parquet(p1)
        spark.range(1, 4, 2).selectExpr("id as id2", "id * 100 as score").write.parquet(p2)
        
        bp_text = f"""
aqueduct: "1.0"
id: test.join
name: Join Test
modules:
  - id: t1
    type: Ingress
    label: T1
    config: {{ format: parquet, path: {p1} }}
  - id: t2
    type: Ingress
    label: T2
    config: {{ format: parquet, path: {p2} }}
  - id: joined
    type: Channel
    label: Joined
    config:
      op: join
      left: t1
      right: t2
      join_type: inner
      condition: t1.id1 = t2.id2
  - id: out
    type: Egress
    label: Out
    config: {{ format: parquet, path: {p_out} }}
edges:
  - from: t1
    to: joined
  - from: t2
    to: joined
  - from: joined
    to: out
"""
        bp_path = tmp_path / "bp.yml"
        bp_path.write_text(bp_text)
        
        bp = parse(str(bp_path))
        manifest = compiler_compile(bp)
        result = execute(manifest, spark, store_dir=tmp_path / "obs")
        
        assert result.status == "success"
        out_df = spark.read.parquet(p_out)
        assert out_df.count() == 1
        row = out_df.collect()[0]
        assert row["name"] == "A"
        assert row["score"] == 100


class TestChannelJoinQuery:
    def test_basic_inner_join(self):
        from aqueduct.executor.spark.channel import _build_join_query
        q = _build_join_query("m", {"left": "a", "right": "b", "condition": "a.id = b.id"})
        assert "INNER JOIN" in q
        assert "ON a.id = b.id" in q

    def test_left_join(self):
        from aqueduct.executor.spark.channel import _build_join_query
        q = _build_join_query("m", {"left": "a", "right": "b", "join_type": "left", "condition": "a.id = b.id"})
        assert "LEFT JOIN" in q

    def test_broadcast_right(self):
        from aqueduct.executor.spark.channel import _build_join_query
        q = _build_join_query("m", {"left": "a", "right": "b", "condition": "a.id = b.id", "broadcast_side": "right"})
        assert "BROADCAST(b)" in q

    def test_broadcast_left(self):
        from aqueduct.executor.spark.channel import _build_join_query
        q = _build_join_query("m", {"left": "a", "right": "b", "condition": "a.id = b.id", "broadcast_side": "left"})
        assert "BROADCAST(a)" in q

    def test_cross_join_no_condition(self):
        from aqueduct.executor.spark.channel import _build_join_query
        q = _build_join_query("m", {"left": "a", "right": "b", "join_type": "cross"})
        assert "CROSS JOIN" in q
        assert "ON" not in q

    def test_missing_left_raises(self):
        from aqueduct.executor.spark.channel import ChannelError, _build_join_query
        with pytest.raises(ChannelError, match="'left'"):
            _build_join_query("m", {"right": "b", "condition": "x"})

    def test_missing_right_raises(self):
        from aqueduct.executor.spark.channel import ChannelError, _build_join_query
        with pytest.raises(ChannelError, match="'right'"):
            _build_join_query("m", {"left": "a", "condition": "x"})

    def test_invalid_join_type_raises(self):
        from aqueduct.executor.spark.channel import ChannelError, _build_join_query
        with pytest.raises(ChannelError, match="invalid join_type"):
            _build_join_query("m", {"left": "a", "right": "b", "join_type": "outer", "condition": "x"})

    def test_missing_condition_non_cross_raises(self):
        from aqueduct.executor.spark.channel import ChannelError, _build_join_query
        with pytest.raises(ChannelError, match="'condition'"):
            _build_join_query("m", {"left": "a", "right": "b", "join_type": "inner"})

    def test_unsupported_op_raises(self):
        from aqueduct.executor.spark.channel import ChannelError, execute_sql_channel
        from unittest.mock import MagicMock
        from aqueduct.parser.models import Module
        mod = Module(id="m", type="Channel", label="M", config={"op": "merge"})
        with pytest.raises(ChannelError, match="unsupported"):
            execute_sql_channel(mod, {"a": MagicMock()}, MagicMock())
