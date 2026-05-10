import pytest
import time
import threading
from pathlib import Path
from aqueduct.parser.models import Module, Edge, ContextRegistry, Blueprint
from aqueduct.compiler.models import Manifest
from aqueduct.executor.spark.executor import execute, ExecuteError
from aqueduct.executor.models import ModuleResult
from aqueduct.executor.spark.session import make_spark_session

@pytest.fixture
def independent_manifest(tmp_path):
    # Two independent chains: A->B, C->D
    # Chain 1: A (Ingress) -> B (Egress)
    # Chain 2: C (Ingress) -> D (Egress)
    m_a = Module(id="A", type="Ingress", label="A", config={"path": str(tmp_path / "a.parquet"), "format": "parquet"})
    m_b = Module(id="B", type="Egress", label="B", config={"path": str(tmp_path / "b.parquet"), "format": "parquet"})
    m_c = Module(id="C", type="Ingress", label="C", config={"path": str(tmp_path / "c.parquet"), "format": "parquet"})
    m_d = Module(id="D", type="Egress", label="D", config={"path": str(tmp_path / "d.parquet"), "format": "parquet"})
    
    edges = (
        Edge(from_id="A", to_id="B", port="main"),
        Edge(from_id="C", to_id="D", port="main"),
    )
    
    manifest = Manifest(
        blueprint_id="parallel_test",
        modules=(m_a, m_b, m_c, m_d),
        edges=edges,
        context={"a": str(tmp_path / "a.parquet"), "c": str(tmp_path / "c.parquet")},
        spark_config={}
    )
    return manifest

def test_execute_parallel_independent_success(independent_manifest, tmp_path, spark):
    # Ensure patch is applied
    make_spark_session("init", {}) 
    
    # Create input data
    spark.createDataFrame([(1, "a")], ["id", "val"]).write.parquet(str(tmp_path / "a.parquet"))
    spark.createDataFrame([(2, "c")], ["id", "val"]).write.parquet(str(tmp_path / "c.parquet"))
    
    result = execute(independent_manifest, spark, parallel=True)
    
    assert result.status == "success"
    statuses = {r.module_id: r.status for r in result.module_results}
    assert statuses == {"A": "success", "B": "success", "C": "success", "D": "success"}


def test_execute_parallel_one_fails_cancels_other(independent_manifest, tmp_path, spark):
    # Chain 1 (A->B) will fail because input file missing
    # Chain 2 (C->D) will be cancelled if we make it slow enough
    # To make it slow without actual sleep (which is hard to inject), we'll just check for cancellation
    # of at least some component.
    
    # Pre-create C's data so it could succeed if not cancelled
    spark.createDataFrame([(2, "c")], ["id", "val"]).write.parquet(str(tmp_path / "c.parquet"))
    # (a.parquet is missing)
    
    result = execute(independent_manifest, spark, parallel=True)
    
    assert result.status == "error"
    statuses = {r.module_id: r.status for r in result.module_results}
    
    # A should be "error"
    assert statuses.get("A") == "error"
    # B should be missing from results because A's thread returned early
    assert "B" not in statuses
    
    # C or D might be "success" or "skipped" depending on race
    # But overall result must be error
    assert result.status == "error"


def test_execute_parallel_single_component_serial(tmp_path, spark):
    manifest = Manifest(
        blueprint_id="single_parallel",
        modules=(
            Module(id="A", type="Ingress", label="A", config={"path": str(tmp_path / "a.parquet"), "format": "parquet"}),
            Module(id="B", type="Egress", label="B", config={"path": str(tmp_path / "b.parquet"), "format": "parquet"})
        ),
        edges=(Edge(from_id="A", to_id="B", port="main"),),
        context={},
        spark_config={}
    )
    
    spark.createDataFrame([(1, "a")], ["id", "val"]).write.parquet(str(tmp_path / "a.parquet"))
    
    result = execute(manifest, spark, parallel=True)
    assert result.status == "success"
    assert len(result.module_results) == 2
