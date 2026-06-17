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


# ── Phase 53 — legacy watermark-sidecar migration (sidecar removed) ───────────
# The local sidecar was dropped; the watermark now lives in the Depot. These
# cover the one-time migration of a sidecar left by a pre-Phase-53 release.

class _FakeDepot:
    def __init__(self):
        self.kv = {}
    def put(self, key, value):
        self.kv[key] = value


def _write_legacy_sidecar(store_dir, bp, ch, value):
    import json
    from aqueduct.executor.spark.executor import _legacy_watermark_sidecar_path
    p = _legacy_watermark_sidecar_path(store_dir, bp, ch)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"watermark": value}))
    return p


def test_migrate_legacy_sidecar_absent_returns_empty(tmp_path):
    from aqueduct.executor.spark.executor import _migrate_legacy_watermark_sidecar
    assert _migrate_legacy_watermark_sidecar(tmp_path, "bp1", "ch1", "k", _FakeDepot()) == ""


def test_migrate_legacy_sidecar_writes_depot_and_deletes_file(tmp_path):
    from aqueduct.executor.spark.executor import _migrate_legacy_watermark_sidecar
    p = _write_legacy_sidecar(tmp_path, "bp1", "ch1", "2024-07-01 00:00:00")
    depot = _FakeDepot()
    out = _migrate_legacy_watermark_sidecar(tmp_path, "bp1", "ch1", "bp1:ch1:_watermark", depot)
    assert out == "2024-07-01 00:00:00"
    assert depot.kv["bp1:ch1:_watermark"] == "2024-07-01 00:00:00"
    assert not p.exists(), "sidecar must be deleted after migration"


def test_migrate_legacy_sidecar_no_depot_keeps_file(tmp_path):
    from aqueduct.executor.spark.executor import _migrate_legacy_watermark_sidecar
    p = _write_legacy_sidecar(tmp_path, "bp1", "ch1", "2024-07-01 00:00:00")
    # No depot configured → value still returned for this run, file left in place.
    out = _migrate_legacy_watermark_sidecar(tmp_path, "bp1", "ch1", "k", None)
    assert out == "2024-07-01 00:00:00"
    assert p.exists()


def test_migrate_legacy_sidecar_none_store_dir(tmp_path):
    from aqueduct.executor.spark.executor import _migrate_legacy_watermark_sidecar
    assert _migrate_legacy_watermark_sidecar(None, "bp1", "ch1", "k", _FakeDepot()) == ""


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


def test_watermark_depot_only_no_sidecar(spark, tmp_path):
    """Phase 53 — a full incremental run persists the watermark to the Depot and
    creates NO local ``watermarks/*.json`` sidecar under store_dir."""
    in_path = str(tmp_path / "in_wm.parquet")
    out_path = str(tmp_path / "out_wm")
    spark.createDataFrame(
        [("2024-01-01 10:00:00", 1), ("2024-01-01 12:00:00", 2)],
        ["ts", "id"],
    ).selectExpr("CAST(ts AS TIMESTAMP) as ts", "id").write.parquet(in_path)

    store_dir = tmp_path / "store_wm"
    depot = MockDepot()
    manifest = create_manifest(
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="inc", type="Channel", label="Inc", config={
                "op": "sql", "materialize": "incremental", "watermark_column": "ts",
                "query": "SELECT * FROM in WHERE ts > ${ctx._watermark}",
            }),
            Module(id="out", type="Egress", label="Out", config={
                "format": "parquet", "path": out_path, "mode": "append",
            }),
        ),
        edges=(
            Edge(from_id="in", to_id="inc", port="main"),
            Edge(from_id="inc", to_id="out", port="main"),
        ),
    )

    result = execute(manifest, spark, depot=depot, store_dir=store_dir)
    assert result.status == "success"
    # Watermark advanced in the Depot …
    assert depot.get("test_bp:inc:_watermark") == "2024-01-01 12:00:00"
    # … and NO local sidecar was written.
    assert not (store_dir / "watermarks").exists()


def test_watermark_no_depot_warns_and_persists_nothing(spark, tmp_path, caplog):
    """Phase 53 — incremental Egress with no Depot logs a warning and persists
    nothing (next run re-scans); still no local sidecar."""
    in_path = str(tmp_path / "in_nd.parquet")
    out_path = str(tmp_path / "out_nd")
    spark.createDataFrame(
        [("2024-01-01 10:00:00", 1)], ["ts", "id"],
    ).selectExpr("CAST(ts AS TIMESTAMP) as ts", "id").write.parquet(in_path)

    store_dir = tmp_path / "store_nd"
    manifest = create_manifest(
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="inc", type="Channel", label="Inc", config={
                "op": "sql", "materialize": "incremental", "watermark_column": "ts",
                "query": "SELECT * FROM in WHERE ts > ${ctx._watermark}",
            }),
            Module(id="out", type="Egress", label="Out", config={
                "format": "parquet", "path": out_path, "mode": "append",
            }),
        ),
        edges=(
            Edge(from_id="in", to_id="inc", port="main"),
            Edge(from_id="inc", to_id="out", port="main"),
        ),
    )

    with caplog.at_level("WARNING"):
        result = execute(manifest, spark, depot=None, store_dir=store_dir)
    assert result.status == "success"
    assert "no depot is configured" in caplog.text
    assert "re-scans all source data" in caplog.text
    assert not (store_dir / "watermarks").exists()


def test_watermark_computed_from_output_not_channel_df(spark, tmp_path):
    """Watermark MAX computed from Egress output path, not the lazy Channel df (no double-scan)."""
    from aqueduct.executor.spark.executor import execute
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
