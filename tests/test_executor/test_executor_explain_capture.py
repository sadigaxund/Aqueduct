import pytest
from unittest.mock import MagicMock, patch
from aqueduct.executor.spark.executor import execute
from aqueduct.compiler.compiler import compile as compiler_compile
from aqueduct.parser.parser import parse
from aqueduct.patch import explain_gate

pytestmark = [pytest.mark.spark, pytest.mark.integration]

def test_execute_explain_capture_fills_dict(spark, tmp_path):
    """execute(..., explain_capture=dict) fills the dict with per-module snapshots."""
    in_path = tmp_path / "in.parquet"
    spark.range(1, 4).selectExpr("id AS a").write.parquet(str(in_path))
    
    bp_content = f"""
aqueduct: '1.0'
id: test_explain
name: Test Explain
modules:
  - id: m1
    label: Ingress
    type: Ingress
    config: {{format: parquet, path: {in_path}}}
  - id: m2
    label: Channel
    type: Channel
    config: 
      op: sql
      query: "SELECT * FROM m1"
edges:
  - from: m1
    to: m2
"""
    bp_file = tmp_path / "bp.yml"
    bp_file.write_text(bp_content)
    bp = parse(bp_file)
    manifest = compiler_compile(bp)
    
    capture = {}
    result = execute(manifest, spark, explain_capture=capture)
    
    assert result.status == "success", f"Run failed: {result.module_results}"
    assert "m1" in capture
    assert "m2" in capture
    assert "exchange_count" in capture["m1"]
    assert "plan_text" in capture["m1"]
    assert "exchange_count" in capture["m2"]

def test_execute_explain_capture_and_surveyor_coexist(spark, tmp_path):
    """surveyor and explain_capture can both be passed and both are populated."""
    in_path = tmp_path / "in.parquet"
    spark.range(1, 4).selectExpr("id AS a").write.parquet(str(in_path))
    
    bp_content = f"""
aqueduct: '1.0'
id: test_explain_dual
name: Test Explain Dual
modules:
  - id: m1
    label: Ingress
    type: Ingress
    config: {{format: parquet, path: {in_path}}}
edges: []
"""
    bp_file = tmp_path / "bp.yml"
    bp_file.write_text(bp_content)
    bp = parse(bp_file)
    manifest = compiler_compile(bp)
    
    mock_surveyor = MagicMock()
    capture = {}
    
    execute(manifest, spark, surveyor=mock_surveyor, explain_capture=capture)
    
    assert "m1" in capture
    mock_surveyor.record_explain_snapshot.assert_called_once()
    args = mock_surveyor.record_explain_snapshot.call_args.kwargs
    assert args["module_id"] == "m1"
    assert "exchange_count" in args

def test_execute_explain_capture_skips_egress(spark, tmp_path):
    """Egress modules are never captured (they don't have DataFrames in frame_store)."""
    in_path = tmp_path / "in.parquet"
    spark.range(1, 4).selectExpr("id AS a").write.parquet(str(in_path))
    out_path = tmp_path / "out"
    
    bp_content = f"""
aqueduct: '1.0'
id: test_explain_egress
name: Test Explain Egress
modules:
  - id: m1
    label: Ingress
    type: Ingress
    config: {{format: parquet, path: {in_path}}}
  - id: out
    label: Egress
    type: Egress
    config: {{format: parquet, path: {out_path}}}
edges:
  - from: m1
    to: out
"""
    bp_file = tmp_path / "bp.yml"
    bp_file.write_text(bp_content)
    bp = parse(bp_file)
    manifest = compiler_compile(bp)
    
    capture = {}
    execute(manifest, spark, explain_capture=capture)
    
    assert "m1" in capture
    assert "out" not in capture

def test_execute_explain_capture_swallows_failure(spark, tmp_path):
    """Failure during capture for one module does not abort the run."""
    in_path = tmp_path / "in.parquet"
    spark.range(1, 4).selectExpr("id AS a").write.parquet(str(in_path))
    
    bp_content = f"""
aqueduct: '1.0'
id: test_explain_fail
name: Test Explain Fail
modules:
  - id: m1
    label: Ingress
    type: Ingress
    config: {{format: parquet, path: {in_path}}}
  - id: m2
    label: Channel
    type: Channel
    config:
      op: sql
      query: "SELECT * FROM m1"
edges:
  - from: m1
    to: m2
"""
    bp_file = tmp_path / "bp.yml"
    bp_file.write_text(bp_content)
    bp = parse(bp_file)
    manifest = compiler_compile(bp)
    
    capture = {}
    
    original_capture = explain_gate.capture_plan_snapshot
    
    def side_effect(df):
        if not hasattr(side_effect, "called"):
            side_effect.called = True
            raise RuntimeError("Capture Boom!")
        return original_capture(df)
    
    with patch("aqueduct.patch.explain_gate.capture_plan_snapshot", side_effect=side_effect):
        result = execute(manifest, spark, explain_capture=capture)
    
    assert result.status == "success"
    # m1 failed capture (swallowed), m2 succeeded
    assert "m1" not in capture
    assert "m2" in capture
