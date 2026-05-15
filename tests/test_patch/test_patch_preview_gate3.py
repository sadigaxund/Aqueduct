"""Integration tests for Phase 29a Gate 3 sandbox replay."""

from __future__ import annotations
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

pytestmark = [pytest.mark.spark, pytest.mark.integration]

from aqueduct.patch.preview import run_gate3_sandbox
from aqueduct.executor.spark.ingress import read_ingress


def test_gate3_pass_on_valid_blueprint(spark, sample_data, tmp_path):
    # Blueprint: Ingress -> Egress
    # sample_data fixture provides orders.parquet
    orders_path = str(sample_data / "orders.parquet")
    bp = {
        "aqueduct": "1.0",
        "id": "test.gate3",
        "name": "Test Gate 3",
        "modules": [
            {"id": "in", "type": "Ingress", "label": "Input", "config": {"format": "parquet", "path": orders_path}},
            {"id": "out", "type": "Egress", "label": "Output", "config": {"format": "parquet", "path": str(tmp_path / "out"), "mode": "overwrite"}}
        ],
        "edges": [{"from": "in", "to": "out"}]
    }
    
    result = run_gate3_sandbox(
        bp,
        blueprint_path=tmp_path / "bp.yml",
        patch_id="p1",
        failed_module=None,
        sample_rows=5,
        spark_session=spark
    )
    
    assert result.status == "pass"
    assert "sandbox replay succeeded" in result.detail
    assert result.sample_rows == 5
    assert len(result.egress_targets) == 1
    assert result.egress_targets[0]["id"] == "out"
    # Verify no file was actually written to tmp_path / "out"
    assert not (tmp_path / "out").exists()


def test_gate3_fail_on_compile_error(spark, tmp_path):
    # Blueprint with cycle
    bp = {
        "aqueduct": "1.0",
        "id": "test.gate3",
        "name": "Test Cycle",
        "modules": [
            {"id": "m1", "type": "Channel", "label": "M1", "config": {"op": "sql", "query": "SELECT 1"}},
            {"id": "m2", "type": "Channel", "label": "M2", "config": {"op": "sql", "query": "SELECT 1"}}
        ],
        "edges": [
            {"from": "m1", "to": "m2"},
            {"from": "m2", "to": "m1"}
        ]
    }
    
    result = run_gate3_sandbox(
        bp,
        blueprint_path=tmp_path / "bp.yml",
        patch_id="p1",
        failed_module=None,
        spark_session=spark
    )
    
    assert result.status == "fail"
    assert "Cycle detected" in result.detail


def test_gate3_fail_on_runtime_error(spark, sample_data, tmp_path):
    # Blueprint with bad SQL
    orders_path = str(sample_data / "orders.parquet")
    bp = {
        "aqueduct": "1.0",
        "id": "test.gate3",
        "name": "Test Runtime",
        "modules": [
            {"id": "in", "type": "Ingress", "label": "In", "config": {"format": "parquet", "path": orders_path}},
            {"id": "m1", "type": "Channel", "label": "M1", "config": {"op": "sql", "query": "SELECT * FROM non_existent_table"}}
        ],
        "edges": [{"from": "in", "to": "m1"}]
    }
    
    result = run_gate3_sandbox(
        bp,
        blueprint_path=tmp_path / "bp.yml",
        patch_id="p1",
        failed_module=None,
        spark_session=spark
    )
    
    assert result.status == "fail"
    assert "non_existent_table" in result.detail


def test_ingress_sandbox_limit_honored(spark, sample_data, tmp_path):
    from aqueduct.parser.models import Module
    orders_path = str(sample_data / "orders.parquet")
    m = Module(
        id="in", type="Ingress", label="In",
        config={"format": "parquet", "path": orders_path, "sandbox_limit": 3}
    )
    df = read_ingress(m, spark)
    # orders.parquet has 10 rows. sandbox_limit=3 should return 3.
    assert df.count() == 3


# Helper to find FIXTURES
FIXTURES = Path(__file__).parent.parent / "fixtures"

def test_ingress_no_sandbox_limit(spark, sample_data):
    from aqueduct.parser.models import Module
    orders_path = str(sample_data / "orders.parquet")
    m = Module(
        id="in", type="Ingress", label="In",
        config={"format": "parquet", "path": orders_path}
    )
    df = read_ingress(m, spark)
    # No limit applied
    assert "Limit" not in df._jdf.queryExecution().optimizedPlan().toString()


def test_gate3_sample_rows_zero(spark, sample_data, tmp_path):
    orders_path = str(sample_data / "orders.parquet")
    bp = {
        "aqueduct": "1.0",
        "id": "test.gate3",
        "name": "Test Zero Limit",
        "modules": [
            {"id": "in", "type": "Ingress", "label": "In", "config": {"format": "parquet", "path": orders_path}}
        ],
        "edges": []
    }
    result = run_gate3_sandbox(
        bp, blueprint_path=tmp_path / "bp.yml", patch_id="p0",
        failed_module=None, sample_rows=0, spark_session=spark
    )
    assert result.status == "pass"
    assert result.sample_rows is None


def test_gate3_temp_file_unlinked(spark, sample_data, tmp_path):
    orders_path = str(sample_data / "orders.parquet")
    bp = {
        "aqueduct": "1.0",
        "id": "test.gate3",
        "name": "Test Temp",
        "modules": [{"id": "in", "type": "Ingress", "label": "In", "config": {"format": "parquet", "path": orders_path}}],
        "edges": []
    }
    # Mock NamedTemporaryFile to see what happens
    with patch("tempfile.NamedTemporaryFile") as mock_tmp:
        mock_file = MagicMock()
        mock_file.name = str(tmp_path / "mock.yml")
        mock_tmp.return_value.__enter__.return_value = mock_file
        
        run_gate3_sandbox(
            bp, blueprint_path=tmp_path / "bp.yml", patch_id="ptmp",
            failed_module=None, spark_session=spark
        )
        
    assert not (tmp_path / "mock.yml").exists()

def test_gate3_spark_unavailable_skips(tmp_path):
    # Mock make_spark_session to raise
    with patch("aqueduct.executor.spark.session.make_spark_session") as mock_make:
        mock_make.side_effect = Exception("Spark down")
        bp = {
            "aqueduct": "1.0",
            "id": "test.gate3",
            "name": "Test Skip",
            "modules": [{"id": "in", "type": "Ingress", "label": "In", "config": {"format": "parquet", "path": "p"}}],
            "edges": []
        }
        result = run_gate3_sandbox(
            bp, blueprint_path=tmp_path / "bp.yml", patch_id="p_skip",
            failed_module=None, spark_session=None # Force it to call make_spark_session
        )
        assert result.status == "skip"
        assert "could not start Spark: Spark down" in result.detail


def test_ingress_limit_after_filter(spark, sample_data):
    from aqueduct.parser.models import Module
    orders_path = str(sample_data / "orders.parquet")
    # orders.parquet has 10 rows. US region has 5 rows.
    # filter US (5 rows) -> limit 2 -> result 2 rows.
    m = Module(
        id="in", type="Ingress", label="In",
        config={
            "format": "parquet", "path": orders_path,
            "partition_filters": "region = 'US'",
            "sandbox_limit": 2
        }
    )
    df = read_ingress(m, spark)
    assert df.count() == 2
    # Verify both rows are US
    assert df.filter("region != 'US'").count() == 0


def test_ingress_limit_before_schema_hint(spark, sample_data):
    from aqueduct.parser.models import Module
    orders_path = str(sample_data / "orders.parquet")
    # schema_hint check should pass even with limit
    m = Module(
        id="in", type="Ingress", label="In",
        config={
            "format": "parquet", "path": orders_path,
            "sandbox_limit": 1,
            "schema_hint": {"order_id": "string", "amount": "double"}
        }
    )
    df = read_ingress(m, spark) # should not raise
    assert df.count() == 1
