import pytest
from unittest.mock import MagicMock
from aqueduct.patch.preview import run_sandbox_gate

pytestmark = [pytest.mark.spark, pytest.mark.integration]

def test_run_sandbox_gate_forwards_explain_capture(spark, tmp_path):
    """run_sandbox_gate forwards explain_capture to execute() and it gets filled."""
    in_path = tmp_path / "in.parquet"
    spark.range(1, 2).selectExpr("id AS a").write.parquet(str(in_path))
    
    # Raw blueprint dict (post-patch)
    bp_after = {
        "aqueduct": "1.0",
        "id": "test_sandbox",
        "name": "Test Sandbox",
        "modules": [
            {
                "id": "m1",
                "label": "Ingress",
                "type": "Ingress",
                "config": {"format": "parquet", "path": str(in_path)}
            }
        ],
        "edges": []
    }
    
    capture = {}
    result = run_sandbox_gate(
        bp_after,
        blueprint_path=tmp_path / "original.yml",
        patch_id="p1",
        failed_module=None,
        spark_session=spark,
        explain_capture=capture
    )
    
    assert result.status == "pass"
    assert "m1" in capture
    assert "exchange_count" in capture["m1"]

def test_run_sandbox_gate_without_explain_capture(spark, tmp_path):
    """run_sandbox_gate works fine when explain_capture is omitted."""
    in_path = tmp_path / "in.parquet"
    spark.range(1, 2).selectExpr("id AS a").write.parquet(str(in_path))
    
    bp_after = {
        "aqueduct": "1.0",
        "id": "test_sandbox_none",
        "name": "Test Sandbox None",
        "modules": [
            {
                "id": "m1",
                "label": "Ingress",
                "type": "Ingress",
                "config": {"format": "parquet", "path": str(in_path)}
            }
        ],
        "edges": []
    }
    
    result = run_sandbox_gate(
        bp_after,
        blueprint_path=tmp_path / "original.yml",
        patch_id="p2",
        failed_module=None,
        spark_session=spark,
        explain_capture=None
    )
    
    assert result.status == "pass"
