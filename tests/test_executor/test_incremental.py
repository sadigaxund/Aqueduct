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
    # Execution succeeds — sentinel substitution allowed query to run
    assert result.status == "success"
    # No Egress in this manifest, so watermark is not advanced (watermark write is
    # deferred to post-Egress; tested in test_incremental_watermark_with_prior)

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


# ── Phase 24c — Watermark sidecar helpers ─────────────────────────────────────

def test_read_watermark_sidecar_absent_returns_none(tmp_path):
    from aqueduct.executor.spark.executor import _read_watermark_sidecar
    result = _read_watermark_sidecar(tmp_path, "bp1", "channel1")
    assert result is None


def test_read_watermark_sidecar_valid_returns_value(tmp_path):
    import json
    from aqueduct.executor.spark.executor import (
        _read_watermark_sidecar,
        _watermark_sidecar_path,
    )
    path = _watermark_sidecar_path(tmp_path, "bp1", "ch1")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"watermark": "2024-06-01 00:00:00"}))
    result = _read_watermark_sidecar(tmp_path, "bp1", "ch1")
    assert result == "2024-06-01 00:00:00"


def test_read_watermark_sidecar_corrupt_json_returns_none(tmp_path):
    from aqueduct.executor.spark.executor import (
        _read_watermark_sidecar,
        _watermark_sidecar_path,
    )
    path = _watermark_sidecar_path(tmp_path, "bp1", "ch1")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("not valid json {{{{")
    result = _read_watermark_sidecar(tmp_path, "bp1", "ch1")
    assert result is None


def test_write_watermark_sidecar_atomic_rename(tmp_path):
    from aqueduct.executor.spark.executor import (
        _write_watermark_sidecar,
        _read_watermark_sidecar,
        _watermark_sidecar_path,
    )
    _write_watermark_sidecar(tmp_path, "bp1", "ch1", "2024-07-01 00:00:00", "ts", "run-123")
    result = _read_watermark_sidecar(tmp_path, "bp1", "ch1")
    assert result == "2024-07-01 00:00:00"
    # No .tmp file left behind
    path = _watermark_sidecar_path(tmp_path, "bp1", "ch1")
    assert not path.with_suffix(".json.tmp").exists()


def test_write_watermark_sidecar_store_dir_none_noop():
    from aqueduct.executor.spark.executor import _write_watermark_sidecar
    # Must not crash when store_dir is None
    _write_watermark_sidecar(None, "bp1", "ch1", "2024-07-01", "ts", "run-1")


def test_compute_watermark_from_output_parquet(spark, tmp_path):
    """parquet format → spark.read.parquet.agg(MAX).collect()"""
    from aqueduct.executor.spark.executor import _compute_watermark_from_output
    out_path = str(tmp_path / "watermark_test.parquet")
    spark.createDataFrame([
        ("2024-01-01 10:00:00", 1),
        ("2024-01-01 12:00:00", 2),
    ], ["ts", "id"]).selectExpr("CAST(ts AS TIMESTAMP) as ts", "id").write.parquet(out_path)
    result = _compute_watermark_from_output(spark, out_path, "parquet", "ts")
    assert result is not None
    assert "2024-01-01 12:00:00" in result


def test_compute_watermark_from_output_missing_path_returns_none(spark, tmp_path):
    from aqueduct.executor.spark.executor import _compute_watermark_from_output
    result = _compute_watermark_from_output(spark, str(tmp_path / "nonexistent"), "parquet", "ts")
    assert result is None


def test_compute_watermark_from_output_delta_format(spark, tmp_path):
    """delta format → spark.sql('SELECT MAX...FROM delta.`path`') called; fallback None on no Delta."""
    from aqueduct.executor.spark.executor import _compute_watermark_from_output
    from unittest.mock import MagicMock, patch

    captured = {}
    mock_result = MagicMock()
    mock_result.collect.return_value = [("2024-06-01 12:00:00",)]

    def fake_sql(query):
        captured["query"] = query
        return mock_result

    mock_spark = MagicMock()
    mock_spark.sql.side_effect = fake_sql

    result = _compute_watermark_from_output(mock_spark, "/tmp/delta_table", "delta", "ts")
    assert "delta" in captured["query"].lower()
    assert "/tmp/delta_table" in captured["query"]
    assert "MAX" in captured["query"]
    assert result == "2024-06-01 12:00:00"


def test_compute_watermark_from_output_delta_format_none_fallback(spark, tmp_path):
    """delta format with no Delta Lake → returns None (non-fatal)."""
    from aqueduct.executor.spark.executor import _compute_watermark_from_output
    # Real spark session without delta — sql will raise; should return None
    result = _compute_watermark_from_output(spark, str(tmp_path / "no_delta"), "delta", "ts")
    assert result is None


def test_incremental_sidecar_written_after_egress(spark, tmp_path):
    """incremental Channel + Egress succeeds → sidecar written at store_dir/watermarks/."""
    from aqueduct.executor.spark.executor import execute, _read_watermark_sidecar
    in_path = str(tmp_path / "in_sidecar.parquet")
    spark.createDataFrame([
        ("2024-03-01 08:00:00", 1),
        ("2024-03-01 10:00:00", 2),
    ], ["ts", "id"]).selectExpr("CAST(ts AS TIMESTAMP) as ts", "id").write.parquet(in_path)

    out_path = str(tmp_path / "out_sidecar.parquet")
    store_dir = tmp_path / "store"

    manifest = create_manifest(
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="inc", type="Channel", label="Inc", config={
                "op": "sql",
                "materialize": "incremental",
                "watermark_column": "ts",
                "query": "SELECT * FROM in WHERE ts > ${ctx._watermark}",
            }),
            Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": out_path, "mode": "overwrite"}),
        ),
        edges=(
            Edge(from_id="in", to_id="inc", port="main"),
            Edge(from_id="inc", to_id="out", port="main"),
        )
    )

    result = execute(manifest, spark, store_dir=store_dir)
    assert result.status == "success"

    wm = _read_watermark_sidecar(store_dir, "test_bp", "inc")
    assert wm is not None
    assert "2024-03-01 10:00:00" in wm


def test_incremental_sidecar_priority_over_depot(spark, tmp_path):
    """incremental Channel → sidecar read takes priority over Depot value."""
    import json
    from aqueduct.executor.spark.executor import execute, _watermark_sidecar_path, _read_watermark_sidecar

    in_path = str(tmp_path / "in_prio.parquet")
    spark.createDataFrame([
        ("2024-05-01 10:00:00", 1),
        ("2024-05-01 15:00:00", 2),
    ], ["ts", "id"]).selectExpr("CAST(ts AS TIMESTAMP) as ts", "id").write.parquet(in_path)

    out_path = str(tmp_path / "out_prio.parquet")
    store_dir = tmp_path / "store2"

    # Pre-write a sidecar with an older watermark (sidecar should win over depot)
    sidecar_path = _watermark_sidecar_path(store_dir, "test_bp", "inc_prio")
    sidecar_path.parent.mkdir(parents=True, exist_ok=True)
    sidecar_path.write_text(json.dumps({"watermark": "2024-05-01 12:00:00"}))

    depot = MockDepot({"test_bp:inc_prio:_watermark": "2024-05-01 09:00:00"})

    manifest = create_manifest(
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="inc_prio", type="Channel", label="Inc", config={
                "op": "sql",
                "materialize": "incremental",
                "watermark_column": "ts",
                "query": "SELECT * FROM in WHERE ts > ${ctx._watermark}",
            }),
            Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": out_path, "mode": "overwrite"}),
        ),
        edges=(
            Edge(from_id="in", to_id="inc_prio", port="main"),
            Edge(from_id="inc_prio", to_id="out", port="main"),
        )
    )

    result = execute(manifest, spark, depot=depot, store_dir=store_dir)
    assert result.status == "success"
    # Only row after sidecar watermark (12:00:00) should pass: row 2 at 15:00:00
    df_out = spark.read.parquet(out_path)
    assert df_out.count() == 1


def test_incremental_egress_fail_sidecar_not_written(spark, tmp_path):
    """incremental Channel + Egress fails → sidecar NOT written, watermark NOT advanced."""
    from aqueduct.executor.spark.executor import execute, _read_watermark_sidecar

    in_path = str(tmp_path / "in_fail2.parquet")
    spark.range(1).selectExpr("CAST('2024-01-01 10:00:00' AS TIMESTAMP) as ts").write.parquet(in_path)

    store_dir = tmp_path / "store_fail"

    manifest = create_manifest(
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="inc", type="Channel", label="Inc", config={
                "op": "sql",
                "materialize": "incremental",
                "watermark_column": "ts",
                "query": "SELECT * FROM in WHERE ts > ${ctx._watermark}",
            }),
            # Force egress failure by writing to a bad path
            Module(id="out", type="Egress", label="Out", config={
                "format": "parquet",
                "path": "/proc/sys/kernel/not_writable_path/",
                "mode": "overwrite",
            }),
        ),
        edges=(
            Edge(from_id="in", to_id="inc", port="main"),
            Edge(from_id="inc", to_id="out", port="main"),
        )
    )

    result = execute(manifest, spark, store_dir=store_dir)
    assert result.status == "error"
    wm = _read_watermark_sidecar(store_dir, "test_bp", "inc")
    assert wm is None


def test_pending_watermarks_not_populated_for_normal_channel(spark, tmp_path):
    """_pending_watermarks NOT populated for non-incremental Channel."""
    from aqueduct.executor.spark.executor import execute, _read_watermark_sidecar

    in_path = str(tmp_path / "in_normal.parquet")
    out_path = str(tmp_path / "out_normal.parquet")
    spark.range(3).selectExpr("CAST('2024-01-01' AS TIMESTAMP) as ts").write.parquet(in_path)
    store_dir = tmp_path / "store_normal"

    manifest = create_manifest(
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="ch", type="Channel", label="Ch", config={"op": "sql", "query": "SELECT * FROM in"}),
            Module(id="out", type="Egress", label="Out", config={"format": "parquet", "path": out_path, "mode": "overwrite"}),
        ),
        edges=(
            Edge(from_id="in", to_id="ch", port="main"),
            Edge(from_id="ch", to_id="out", port="main"),
        )
    )

    result = execute(manifest, spark, store_dir=store_dir)
    assert result.status == "success"
    # No watermark sidecar for non-incremental channel
    wm = _read_watermark_sidecar(store_dir, "test_bp", "ch")
    assert wm is None


def test_incremental_sidecar_written_depot_none(spark, tmp_path):
    """incremental Channel, depot=None → sidecar still written after Egress write."""
    from aqueduct.executor.spark.executor import execute, _read_watermark_sidecar

    in_path = str(tmp_path / "in_depot_none.parquet")
    out_path = str(tmp_path / "out_depot_none")
    spark.range(3).selectExpr("CAST('2024-06-01 12:00:00' AS TIMESTAMP) as ts").write.parquet(in_path)
    store_dir = tmp_path / "store_depot_none"

    manifest = create_manifest(
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="inc", type="Channel", label="Inc", config={
                "op": "sql",
                "materialize": "incremental",
                "watermark_column": "ts",
                "query": "SELECT * FROM in WHERE ts > ${ctx._watermark}",
            }),
            Module(id="out", type="Egress", label="Out", config={
                "format": "parquet",
                "path": out_path,
                "mode": "overwrite",
            }),
        ),
        edges=(
            Edge(from_id="in", to_id="inc", port="main"),
            Edge(from_id="inc", to_id="out", port="main"),
        )
    )

    result = execute(manifest, spark, store_dir=store_dir, depot=None)
    assert result.status == "success"
    wm = _read_watermark_sidecar(store_dir, "test_bp", "inc")
    assert wm is not None



def test_watermark_computed_from_output_not_channel_df(spark, tmp_path):
    """Watermark MAX computed from Egress output path, not the lazy Channel df (no double-scan)."""
    from aqueduct.executor.spark.executor import execute, _read_watermark_sidecar
    from unittest.mock import patch

    in_path = str(tmp_path / "in_nodbl.parquet")
    out_path = str(tmp_path / "out_nodbl")
    spark.range(3).selectExpr("CAST('2024-09-01 00:00:00' AS TIMESTAMP) as ts").write.parquet(in_path)
    store_dir = tmp_path / "store_nodbl"

    call_log = []
    import aqueduct.executor.spark.executor as executor_mod
    original_compute = executor_mod._compute_watermark_from_output

    def spy_compute(spark_session, path, fmt, wm_col):
        call_log.append({"path": path, "fmt": fmt})
        return original_compute(spark_session, path, fmt, wm_col)

    manifest = create_manifest(
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="inc", type="Channel", label="Inc", config={
                "op": "sql",
                "materialize": "incremental",
                "watermark_column": "ts",
                "query": "SELECT * FROM in WHERE ts > ${ctx._watermark}",
            }),
            Module(id="out", type="Egress", label="Out", config={
                "format": "parquet",
                "path": out_path,
                "mode": "overwrite",
            }),
        ),
        edges=(
            Edge(from_id="in", to_id="inc", port="main"),
            Edge(from_id="inc", to_id="out", port="main"),
        )
    )

    with patch("aqueduct.executor.spark.executor._compute_watermark_from_output", side_effect=spy_compute):
        result = execute(manifest, spark, store_dir=store_dir)

    assert result.status == "success"
    # _compute_watermark_from_output called exactly once, with the Egress output path
    assert len(call_log) == 1
    assert call_log[0]["path"] == out_path
