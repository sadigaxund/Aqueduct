import pytest
from pathlib import Path
from aqueduct.executor.spark.executor import execute
from aqueduct.compiler.models import Manifest
from aqueduct.compiler.provenance import ProvenanceMap
from aqueduct.parser.models import Module, Edge, RetryPolicy

pytestmark = [pytest.mark.spark, pytest.mark.integration]

class MockDepot:
    def __init__(self, initial=None):
        self.data = initial or {}
    def get(self, key, default=None):
        return self.data.get(key, default)
    def put(self, key, value):
        self.data[key] = value

def create_manifest(modules, edges):
    return Manifest(
        blueprint_id="test_bp",
        name="Test",
        description="",
        aqueduct_version="1.0",
        context={},
        modules=tuple(modules),
        edges=tuple(edges),
        spark_config={},
        retry_policy=RetryPolicy(),
        agent=None,
        udf_registry={},
        macros={},
        checkpoint=False,
        provenance_map=ProvenanceMap(blueprint_id="test_bp", blueprint_path="", modules={}, context={}),
        inputs_fingerprint={}
    )

def test_incremental_watermark_no_prior(spark, tmp_path):
    """materialize=incremental, no prior watermark -> query ${ctx._watermark} replaced with sentinel '1900-01-01 00:00:00'."""
    in_path = str(tmp_path / "in.parquet")
    spark.range(5).selectExpr("CAST('2024-01-01 10:00:00' AS TIMESTAMP) as ts", "id").write.parquet(in_path)
    
    depot = MockDepot()
    manifest = create_manifest(
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="inc", type="Channel", label="Inc", config={
                "op": "sql",
                "materialize": "incremental",
                "watermark_column": "ts",
                "query": "SELECT * FROM in WHERE ts > ${ctx._watermark}"
            }),
        ),
        edges=(Edge(from_id="in", to_id="inc", port="main"),)
    )
    
    result = execute(manifest, spark, depot=depot)
    assert result.status == "success"
    
    # Check that watermark was stored in Depot
    expected_key = "test_bp:inc:_watermark"
    assert depot.get(expected_key) == "2024-01-01 10:00:00"

def test_incremental_watermark_with_prior(spark, tmp_path):
    """materialize=incremental, prior watermark in Depot -> query substituted with stored value."""
    in_path = str(tmp_path / "in_prior.parquet")
    # Rows with timestamps before and after watermark
    spark.createDataFrame([
        ("2024-01-01 10:00:00", 1),
        ("2024-01-01 12:00:00", 2),
    ], ["ts", "id"]).selectExpr("CAST(ts AS TIMESTAMP) as ts", "id").write.parquet(in_path)
    
    depot = MockDepot({"test_bp:inc_p:_watermark": "2024-01-01 11:00:00"})
    
    out_path = str(tmp_path / "out_p.parquet")
    manifest = create_manifest(
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="inc_p", type="Channel", label="Inc", config={
                "op": "sql",
                "materialize": "incremental",
                "watermark_column": "ts",
                "query": "SELECT * FROM in WHERE ts > ${ctx._watermark}"
            }),
            Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": out_path}),
        ),
        edges=(
            Edge(from_id="in", to_id="inc_p", port="main"),
            Edge(from_id="inc_p", to_id="out", port="main"),
        )
    )
    
    result = execute(manifest, spark, depot=depot)
    assert result.status == "success"
    
    # Should only have rows after 11:00:00
    df_out = spark.read.parquet(out_path)
    assert df_out.count() == 1
    assert df_out.collect()[0]["id"] == 2
    
    # New watermark should be 12:00:00
    assert depot.get("test_bp:inc_p:_watermark") == "2024-01-01 12:00:00"

def test_incremental_watermark_failure_not_updated(spark, tmp_path):
    """materialize=incremental, Channel fails -> watermark NOT updated in Depot."""
    in_path = str(tmp_path / "in_fail.parquet")
    spark.range(5).selectExpr("CAST('2024-01-01 10:00:00' AS TIMESTAMP) as ts", "id").write.parquet(in_path)
    
    depot = MockDepot({"test_bp:inc_f:_watermark": "2024-01-01 09:00:00"})
    manifest = create_manifest(
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="inc_f", type="Channel", label="Inc", config={
                "op": "sql",
                "materialize": "incremental",
                "watermark_column": "ts",
                "query": "SELECT * FROM non_existent"
            }),
        ),
        edges=(Edge(from_id="in", to_id="inc_f", port="main"),)
    )
    
    result = execute(manifest, spark, depot=depot)
    assert result.status == "error"
    
    # Watermark should still be 09:00:00
    assert depot.get("test_bp:inc_f:_watermark") == "2024-01-01 09:00:00"

def test_incremental_egress_overwrite_warning(spark, tmp_path, caplog):
    """materialize=incremental, downstream Egress has mode=overwrite -> warning logged."""
    in_path = str(tmp_path / "in_warn.parquet")
    spark.range(1).selectExpr("CAST('2024-01-01 10:00:00' AS TIMESTAMP) as ts").write.parquet(in_path)
    
    manifest = create_manifest(
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="inc", type="Channel", label="Inc", config={
                "op": "sql",
                "materialize": "incremental",
                "watermark_column": "ts",
                "query": "SELECT * FROM in"
            }),
            Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": str(tmp_path/"out"), "mode": "overwrite"}),
        ),
        edges=(
            Edge(from_id="in", to_id="inc", port="main"),
            Edge(from_id="inc", to_id="out", port="main"),
        )
    )
    
    with caplog.at_level("WARNING"):
        execute(manifest, spark)
    
    assert "mode=overwrite" in caplog.text

def test_incremental_no_materialize_no_logic(spark, tmp_path):
    """no materialize key -> normal Channel execution, no watermark logic."""
    in_path = str(tmp_path / "in_none.parquet")
    spark.range(1).selectExpr("CAST('2024-01-01 10:00:00' AS TIMESTAMP) as ts").write.parquet(in_path)
    
    depot = MockDepot()
    manifest = create_manifest(
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="inc", type="Channel", label="Inc", config={
                "op": "sql",
                "watermark_column": "ts",
                "query": "SELECT * FROM in"
            }),
        ),
        edges=(Edge(from_id="in", to_id="inc", port="main"),)
    )
    
    execute(manifest, spark, depot=depot)
    # Depot should be empty
    assert not depot.data

def test_incremental_depot_none_no_crash(spark, tmp_path):
    """materialize=incremental, depot=None -> query uses sentinel, no crash."""
    in_path = str(tmp_path / "in_nodepot.parquet")
    spark.range(1).selectExpr("CAST('2024-01-01 10:00:00' AS TIMESTAMP) as ts").write.parquet(in_path)
    
    manifest = create_manifest(
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="inc", type="Channel", label="Inc", config={
                "op": "sql",
                "materialize": "incremental",
                "watermark_column": "ts",
                "query": "SELECT * FROM in WHERE ts > ${ctx._watermark}"
            }),
        ),
        edges=(Edge(from_id="in", to_id="inc", port="main"),)
    )
    
    result = execute(manifest, spark, depot=None)
    assert result.status == "success"
