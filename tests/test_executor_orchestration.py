"""Tests for the Executor orchestrator and models."""

from __future__ import annotations

import re
import uuid
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from aqueduct.compiler.models import Manifest
from aqueduct.executor.executor import ExecuteError, execute
from aqueduct.executor.models import ExecutionResult, ModuleResult
from aqueduct.parser.models import Edge, Module


@pytest.fixture
def minimal_manifest(tmp_path):
    """A valid Ingress -> Egress manifest."""
    in_path = str(tmp_path / "in.parquet")
    out_path = str(tmp_path / "out.parquet")
    
    # Create source data
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    spark.range(5).write.parquet(in_path)
    
    return Manifest(
        pipeline_id="test.pipeline",
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": out_path}),
        ),
        edges=(
            Edge(from_id="in", to_id="out", port="main"),
        ),
        context={},
        spark_config={}
    )


# ── Model Tests ───────────────────────────────────────────────────────────────

def test_execution_result_is_frozen():
    res = ExecutionResult(pipeline_id="p", run_id="r", status="success", module_results=())
    with pytest.raises(FrozenInstanceError):
        res.status = "error"  # type: ignore[misc]


def test_execution_result_to_dict():
    res = ExecutionResult(
        pipeline_id="p",
        run_id="r",
        status="success",
        module_results=(ModuleResult(module_id="m1", status="success"),)
    )
    d = res.to_dict()
    assert d["pipeline_id"] == "p"
    assert d["module_results"][0]["module_id"] == "m1"


# ── Orchestration Tests ───────────────────────────────────────────────────────

def test_execute_success(spark: SparkSession, minimal_manifest):
    result = execute(minimal_manifest, spark)
    
    assert result.status == "success"
    assert len(result.module_results) == 2
    assert all(r.status == "success" for r in result.module_results)
    
    # Verify data actually landed
    out_path = minimal_manifest.modules[1].config["path"]
    assert spark.read.parquet(out_path).count() == 5


def test_execute_unsupported_module_type(spark: SparkSession):
    manifest = Manifest(
        pipeline_id="test.unsupported",
        modules=(Module(id="m1", type="Regulator", label="R1", config={}),),
        edges=(),
        context={},
        spark_config={}
    )
    with pytest.raises(ExecuteError, match="Module type 'Regulator' .* is not supported"):
        execute(manifest, spark)


def test_execute_channel_linear(spark: SparkSession, tmp_path):
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).selectExpr("id", "id * 2 as val").write.parquet(in_path)
    
    out_path = str(tmp_path / "out.parquet")
    
    manifest = Manifest(
        pipeline_id="test.channel_linear",
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="chan", type="Channel", label="Chan", config={"op": "sql", "query": "SELECT * FROM in WHERE id > 2"}),
            Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": out_path}),
        ),
        edges=(
            Edge(from_id="in", to_id="chan", port="main"),
            Edge(from_id="chan", to_id="out", port="main"),
        ),
        context={},
        spark_config={}
    )
    
    result = execute(manifest, spark)
    assert result.status == "success"
    
    # Verify result: only id 3 and 4 should be present
    df_out = spark.read.parquet(out_path)
    assert df_out.count() == 2
    assert sorted([r[0] for r in df_out.select("id").collect()]) == [3, 4]


def test_execute_channel_multi_input(spark: SparkSession, tmp_path):
    in1_path = str(tmp_path / "in1.parquet")
    spark.range(5).selectExpr("id", "'a' as tag").write.parquet(in1_path)
    
    in2_path = str(tmp_path / "in2.parquet")
    spark.range(5).selectExpr("id", "'b' as tag").write.parquet(in2_path)
    
    out_path = str(tmp_path / "out_multi.parquet")
    
    manifest = Manifest(
        pipeline_id="test.channel_multi",
        modules=(
            Module(id="in1", type="Ingress", label="In1", config={"format": "parquet", "path": in1_path}),
            Module(id="in2", type="Ingress", label="In2", config={"format": "parquet", "path": in2_path}),
            Module(id="chan", type="Channel", label="Chan", config={
                "op": "sql", 
                "query": "SELECT id, tag FROM in1 UNION ALL SELECT id, tag FROM in2"
            }),
            Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": out_path}),
        ),
        edges=(
            Edge(from_id="in1", to_id="chan", port="main"),
            Edge(from_id="in2", to_id="chan", port="main"),
            Edge(from_id="chan", to_id="out", port="main"),
        ),
        context={},
        spark_config={}
    )
    
    result = execute(manifest, spark)
    assert result.status == "success"
    
    df_out = spark.read.parquet(out_path)
    assert df_out.count() == 10
    assert df_out.where("tag = 'a'").count() == 5
    assert df_out.where("tag = 'b'").count() == 5


def test_execute_channel_error(spark: SparkSession, tmp_path):
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).write.parquet(in_path)
    
    manifest = Manifest(
        pipeline_id="test.channel_err",
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="chan", type="Channel", label="Chan", config={"op": "sql", "query": "SELECT * FROM non_existent"}),
        ),
        edges=(
            Edge(from_id="in", to_id="chan", port="main"),
        ),
        context={},
        spark_config={}
    )
    
    result = execute(manifest, spark)
    assert result.status == "error"
    assert result.module_results[1].module_id == "chan"
    assert result.module_results[1].status == "error"
    assert "cannot be found" in result.module_results[1].error


def test_execute_channel_missing_upstream_edge(spark: SparkSession):
    # Channel with no incoming edges
    manifest = Manifest(
        pipeline_id="test.channel_no_edge",
        modules=(
            Module(id="chan", type="Channel", label="Chan", config={"op": "sql", "query": "SELECT 1"}),
        ),
        edges=(),
        context={},
        spark_config={}
    )
    
    result = execute(manifest, spark)
    assert result.status == "error"
    assert "no main-port incoming edges" in result.module_results[0].error


def test_execute_ingress_error(spark: SparkSession):
    # Manifest with non-existent path
    manifest = Manifest(
        pipeline_id="test.ingress_err",
        modules=(Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": "/ghost"}),),
        edges=(),
        context={},
        spark_config={}
    )
    result = execute(manifest, spark)
    
    assert result.status == "error"
    assert result.module_results[0].module_id == "in"
    assert result.module_results[0].status == "error"
    assert "not found" in result.module_results[0].error.lower()


def test_execute_egress_error(spark: SparkSession, tmp_path):
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).write.parquet(in_path)

    # Manifest with invalid write mode (should be caught by writer validation)
    manifest = Manifest(
        pipeline_id="test.egress_err",
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": "/foo", "mode": "hacked"}),
        ),
        edges=(Edge(from_id="in", to_id="out", port="main"),),
        context={},
        spark_config={}
    )
    result = execute(manifest, spark)
    
    assert result.status == "error"
    assert result.module_results[1].status == "error"
    assert "unsupported write mode 'hacked'" in result.module_results[1].error


def test_execute_missing_upstream_edge(spark: SparkSession, tmp_path):
    # Egress module with no incoming edge
    manifest = Manifest(
        pipeline_id="test.no_edge",
        modules=(Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": "/foo"}),),
        edges=(),
        context={},
        spark_config={}
    )
    result = execute(manifest, spark)
    
    assert result.status == "error"
    assert "no main-port edge" in result.module_results[0].error


def test_execute_run_id_auto_gen(spark: SparkSession, minimal_manifest):
    result = execute(minimal_manifest, spark)
    # Validate UUID4 format
    assert re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", result.run_id)


def test_execute_run_id_supplied(spark: SparkSession, minimal_manifest):
    my_id = "custom-run-123"
    result = execute(minimal_manifest, spark, run_id=my_id)
    assert result.run_id == my_id


def test_execute_cycle_in_manifest(spark: SparkSession):
    # Parser usually catches this, but Executor.execute checks too
    manifest = Manifest(
        pipeline_id="test.cycle",
        modules=(
            Module(id="m1", type="Ingress", label="M1", config={"format": "p", "path": "p"}),
            Module(id="m2", type="Egress", label="M2", config={"format": "p", "path": "p"}),
        ),
        edges=(
            Edge(from_id="m1", to_id="m2", port="main"),
            Edge(from_id="m2", to_id="m1", port="main"),
        ),
        context={},
        spark_config={}
    )
    with pytest.raises(ExecuteError, match="Cycle detected"):
        execute(manifest, spark)
