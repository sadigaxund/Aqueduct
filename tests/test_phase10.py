"""Phase 10 tests: Channel op: join and SQL Macros."""

from __future__ import annotations

from pathlib import Path
import pytest
from pyspark.sql import SparkSession

from aqueduct.executor.spark.channel import ChannelError, execute_sql_channel
from aqueduct.compiler.macros import MacroError, resolve_macros, resolve_macros_in_config
from aqueduct.parser.models import Module


# ── Join tests ────────────────────────────────────────────────────────────────

def test_join_missing_left(spark: SparkSession):
    module = Module(id="chan", type="Channel", label="C", config={
        "op": "join", "right": "source_b", "condition": "a.id = b.id"
    })
    with pytest.raises(ChannelError, match="requires 'left'"):
        execute_sql_channel(module, {"source_a": spark.range(1)}, spark)

def test_join_missing_right(spark: SparkSession):
    module = Module(id="chan", type="Channel", label="C", config={
        "op": "join", "left": "source_a", "condition": "a.id = b.id"
    })
    with pytest.raises(ChannelError, match="requires 'right'"):
        execute_sql_channel(module, {"source_a": spark.range(1)}, spark)

def test_join_invalid_type(spark: SparkSession):
    module = Module(id="chan", type="Channel", label="C", config={
        "op": "join", "left": "source_a", "right": "source_b", "join_type": "invalid"
    })
    with pytest.raises(ChannelError, match="invalid join_type 'invalid'"):
        execute_sql_channel(module, {"source_a": spark.range(1)}, spark)

def test_join_missing_condition(spark: SparkSession):
    module = Module(id="chan", type="Channel", label="C", config={
        "op": "join", "left": "source_a", "right": "source_b", "join_type": "inner"
    })
    with pytest.raises(ChannelError, match="requires 'condition'"):
        execute_sql_channel(module, {"source_a": spark.range(1)}, spark)

def test_join_cross_no_condition(spark: SparkSession):
    df1 = spark.range(1)
    df2 = spark.range(1)
    module = Module(id="chan", type="Channel", label="C", config={
        "op": "join", "left": "a", "right": "b", "join_type": "cross"
    })
    res = execute_sql_channel(module, {"a": df1, "b": df2}, spark)
    assert res.count() == 1

def test_join_broadcast_hint(spark: SparkSession):
    df1 = spark.range(1)
    df2 = spark.range(1)
    module = Module(id="chan", type="Channel", label="C", config={
        "op": "join", "left": "a", "right": "b", "condition": "a.id = b.id", "broadcast_side": "right"
    })
    res = execute_sql_channel(module, {"a": df1, "b": df2}, spark)
    assert res.count() == 1

@pytest.mark.usefixtures("spark")
def test_join_end_to_end(spark, tmp_path):
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


# ── SQL Macros tests ──────────────────────────────────────────────────────────

def test_resolve_macros_simple():
    macros = {"my_fragment": "SELECT 1"}
    text = "Query: {{ macros.my_fragment }}"
    assert resolve_macros(text, macros) == "Query: SELECT 1"

def test_resolve_macros_parameterized():
    macros = {"filter": "WHERE id = {{ id_val }} AND type = {{ type_val }}"}
    text = "SELECT * FROM t {{ macros.filter(id_val=123, type_val='A') }}"
    assert resolve_macros(text, macros) == "SELECT * FROM t WHERE id = 123 AND type = A"

def test_resolve_macros_quoted_params():
    macros = {"match": "name = '{{ name }}'"}
    text = "SELECT * FROM t WHERE {{ macros.match(name='Bob') }}"
    assert resolve_macros(text, macros) == "SELECT * FROM t WHERE name = 'Bob'"

def test_resolve_macros_unknown_name():
    macros = {"a": "1"}
    with pytest.raises(MacroError, match="Macro 'b' is not defined"):
        resolve_macros("{{ macros.b }}", macros)

def test_resolve_macros_missing_param():
    macros = {"p": "{{ x }} {{ y }}"}
    with pytest.raises(MacroError, match="parameter 'y' not supplied"):
        resolve_macros("{{ macros.p(x=1) }}", macros)

def test_resolve_macros_empty_macros_returns_text():
    assert resolve_macros("{{ macros.a }}", {}) == "{{ macros.a }}"

def test_resolve_macros_no_token_returns_text():
    assert resolve_macros("plain text", {"a": "1"}) == "plain text"

def test_resolve_macros_in_config_recursion():
    macros = {"v": "123"}
    config = {
        "query": "SELECT {{ macros.v }}",
        "nested": {"val": "X: {{ macros.v }}"},
        "list": ["{{ macros.v }}", 456]
    }
    resolved = resolve_macros_in_config(config, macros)
    assert resolved["query"] == "SELECT 123"
    assert resolved["nested"]["val"] == "X: 123"
    assert resolved["list"] == ["123", 456]

@pytest.mark.usefixtures("spark")
def test_full_compile_expands_macros(tmp_path):
    from aqueduct.compiler.compiler import compile as compiler_compile
    from aqueduct.parser.parser import parse
    
    bp_text = """
aqueduct: "1.0"
id: test.macros
name: Macro Test
macros:
  base_query: "SELECT * FROM input"
  filter: "WHERE status = '{{ s }}'"
modules:
  - id: input
    type: Ingress
    label: In
    config: { format: parquet, path: /tmp/in }
  - id: filtered
    type: Channel
    label: Filtered
    config:
      op: sql
      query: "{{ macros.base_query }} {{ macros.filter(s='ACTIVE') }}"
edges:
  - from: input
    to: filtered
"""
    bp_path = tmp_path / "bp.yml"
    bp_path.write_text(bp_text)
    
    bp = parse(str(bp_path))
    manifest = compiler_compile(bp)
    
    filtered_mod = next(m for m in manifest.modules if m.id == "filtered")
    assert filtered_mod.config["query"] == "SELECT * FROM input WHERE status = 'ACTIVE'"
    assert "{{" not in filtered_mod.config["query"]

@pytest.mark.usefixtures("spark")
def test_macros_end_to_end(spark, tmp_path):
    from aqueduct.compiler.compiler import compile as compiler_compile
    from aqueduct.parser.parser import parse
    from aqueduct.executor.spark.executor import execute
    
    p_in = str(tmp_path / "in.parquet")
    p_out = str(tmp_path / "out.parquet")
    spark.range(1, 3).selectExpr("id", "case when id=1 then 'ACTIVE' else 'INACTIVE' end as status").write.parquet(p_in)
    
    bp_text = f"""
aqueduct: "1.0"
id: test.macros.e2e
name: Macro E2E
macros:
  where_active: "WHERE status = 'ACTIVE'"
modules:
  - id: ing
    type: Ingress
    label: In
    config: {{ format: parquet, path: {p_in} }}
  - id: chan
    type: Channel
    label: Chan
    config:
      op: sql
      query: "SELECT * FROM ing {{{{ macros.where_active }}}}"
  - id: eg
    type: Egress
    label: Out
    config: {{ format: parquet, path: {p_out} }}
edges:
  - from: ing
    to: chan
  - from: chan
    to: eg
"""
    bp_path = tmp_path / "bp.yml"
    bp_path.write_text(bp_text)
    
    bp = parse(str(bp_path))
    manifest = compiler_compile(bp)
    result = execute(manifest, spark, store_dir=tmp_path / "obs")
    
    assert result.status == "success"
    out_df = spark.read.parquet(p_out)
    assert out_df.count() == 1
    assert out_df.collect()[0]["id"] == 1
