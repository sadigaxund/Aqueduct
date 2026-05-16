"""Tests for run_maintenance() in aqueduct/executor/spark/egress.py (Phase 25a)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch, call

import pytest

pytestmark = [pytest.mark.spark, pytest.mark.integration]

from aqueduct.executor.spark.egress import run_maintenance


class TestRunMaintenance:
    def test_no_optimize_no_vacuum_no_sql(self, spark):
        """Both optimize and vacuum absent → no SQL executed."""
        mock_spark = MagicMock()
        result = run_maintenance(mock_spark, "m1", "/tmp/delta", {})
        mock_spark.sql.assert_not_called()
        assert result == {"optimize_ms": None, "vacuum_ms": None}

    def test_optimize_true_runs_optimize_sql(self, spark):
        """optimize: true → OPTIMIZE delta.`path` SQL executed."""
        mock_spark = MagicMock()
        result = run_maintenance(mock_spark, "m1", "/tmp/delta_tbl", {"optimize": True})
        calls = [str(c) for c in mock_spark.sql.call_args_list]
        assert any("OPTIMIZE" in c and "/tmp/delta_tbl" in c for c in calls)
        assert result["optimize_ms"] is not None

    def test_optimize_with_zorder_by_list(self, spark):
        """zorder_by: [col] → ZORDER BY (col) appended."""
        mock_spark = MagicMock()
        result = run_maintenance(
            mock_spark, "m1", "/tmp/tbl",
            {"optimize": True, "zorder_by": ["event_date"]}
        )
        calls = [str(c) for c in mock_spark.sql.call_args_list]
        assert any("ZORDER BY" in c and "event_date" in c for c in calls)

    def test_optimize_without_zorder_no_zorder_clause(self, spark):
        """zorder_by omitted → no ZORDER clause in SQL."""
        mock_spark = MagicMock()
        run_maintenance(mock_spark, "m1", "/tmp/tbl", {"optimize": True})
        calls = [str(c) for c in mock_spark.sql.call_args_list]
        assert not any("ZORDER" in c for c in calls)

    def test_vacuum_runs_vacuum_sql(self, spark):
        """vacuum: 168 → VACUUM delta.`path` RETAIN 168 HOURS."""
        mock_spark = MagicMock()
        result = run_maintenance(mock_spark, "m1", "/tmp/tbl", {"vacuum": 168})
        calls = [str(c) for c in mock_spark.sql.call_args_list]
        assert any("VACUUM" in c and "168" in c for c in calls)
        assert result["vacuum_ms"] is not None

    def test_optimize_failure_returns_none_ms(self, spark):
        """OPTIMIZE failure → warning logged, returns optimize_ms=None, pipeline continues."""
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = Exception("Delta not initialized")
        result = run_maintenance(mock_spark, "m1", "/tmp/tbl", {"optimize": True})
        assert result["optimize_ms"] is None

    def test_vacuum_failure_returns_none_ms(self, spark):
        """VACUUM failure → warning logged, returns vacuum_ms=None, pipeline continues."""
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = Exception("Not a delta table")
        result = run_maintenance(mock_spark, "m1", "/tmp/tbl", {"vacuum": 168})
        assert result["vacuum_ms"] is None

    def test_zorder_by_string_treated_as_single_column(self, spark):
        """zorder_by as string (not list) → treated as single column."""
        mock_spark = MagicMock()
        run_maintenance(mock_spark, "m1", "/tmp/tbl", {"optimize": True, "zorder_by": "event_date"})
        calls = [str(c) for c in mock_spark.sql.call_args_list]
        assert any("ZORDER BY" in c and "event_date" in c for c in calls)


# ── Executor integration: maintenance block ───────────────────────────────────

def test_egress_with_maintenance_block_calls_run_maintenance(spark, tmp_path):
    """Egress with maintenance: block → run_maintenance called after successful write."""
    from aqueduct.executor.spark.executor import execute
    from aqueduct.compiler.models import Manifest
    from aqueduct.compiler.provenance import ProvenanceMap
    from aqueduct.parser.models import Module, Edge, RetryPolicy

    in_path = str(tmp_path / "in_maint.parquet")
    out_path = str(tmp_path / "out_maint")
    spark.range(3).write.parquet(in_path)

    manifest = Manifest(
        blueprint_id="bp1", name="Test", description="", aqueduct_version="1.0",
        context={}, spark_config={}, retry_policy=RetryPolicy(), agent=None,
        udf_registry={}, macros={}, checkpoint=False,
        provenance_map=ProvenanceMap(blueprint_id="bp1", blueprint_path="", modules={}, context={}),
        inputs_fingerprint={},
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="out", type="Egress", label="Out", config={
                "format": "parquet",
                "path": out_path,
                "mode": "overwrite",
                "maintenance": {"optimize": False},  # no-op maintenance, just verify it's called
            }),
        ),
        edges=(Edge(from_id="in", to_id="out", port="main"),),
    )

    with patch("aqueduct.executor.spark.executor.run_maintenance") as mock_maint:
        mock_maint.return_value = {"optimize_ms": None, "vacuum_ms": None}
        result = execute(manifest, spark)

    assert result.status == "success"
    mock_maint.assert_called_once()


def test_egress_without_maintenance_block_no_call(spark, tmp_path):
    """Egress with no maintenance: block → run_maintenance NOT called."""
    from aqueduct.executor.spark.executor import execute
    from aqueduct.compiler.models import Manifest
    from aqueduct.compiler.provenance import ProvenanceMap
    from aqueduct.parser.models import Module, Edge, RetryPolicy

    in_path = str(tmp_path / "in_nomaint.parquet")
    out_path = str(tmp_path / "out_nomaint")
    spark.range(3).write.parquet(in_path)

    manifest = Manifest(
        blueprint_id="bp2", name="Test", description="", aqueduct_version="1.0",
        context={}, spark_config={}, retry_policy=RetryPolicy(), agent=None,
        udf_registry={}, macros={}, checkpoint=False,
        provenance_map=ProvenanceMap(blueprint_id="bp2", blueprint_path="", modules={}, context={}),
        inputs_fingerprint={},
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="out", type="Egress", label="Out", config={
                "format": "parquet",
                "path": out_path,
                "mode": "overwrite",
            }),
        ),
        edges=(Edge(from_id="in", to_id="out", port="main"),),
    )

    with patch("aqueduct.executor.spark.executor.run_maintenance") as mock_maint:
        result = execute(manifest, spark)

    assert result.status == "success"
    mock_maint.assert_not_called()


# ── obs.db maintenance_metrics ────────────────────────────────────────────────

def test_maintenance_timing_written_to_obs_db(spark, tmp_path):
    """Egress with maintenance block → timing rows written to maintenance_metrics in obs.db."""
    from aqueduct.executor.spark.executor import execute, _write_maintenance_metrics
    from aqueduct.compiler.models import Manifest
    from aqueduct.compiler.provenance import ProvenanceMap
    from aqueduct.parser.models import Module, Edge, RetryPolicy
    import duckdb

    in_path = str(tmp_path / "in_obs.parquet")
    out_path = str(tmp_path / "out_obs")
    store_dir = tmp_path / "store_obs"
    spark.range(3).write.parquet(in_path)

    manifest = Manifest(
        blueprint_id="bp_obs", name="Test", description="", aqueduct_version="1.0",
        context={}, spark_config={}, retry_policy=RetryPolicy(), agent=None,
        udf_registry={}, macros={}, checkpoint=False,
        provenance_map=ProvenanceMap(blueprint_id="bp_obs", blueprint_path="", modules={}, context={}),
        inputs_fingerprint={},
        modules=(
            Module(id="in", type="Ingress", label="In", config={"format": "parquet", "path": in_path}),
            Module(id="out", type="Egress", label="Out", config={
                "format": "parquet",
                "path": out_path,
                "mode": "overwrite",
                "maintenance": {"optimize": False},
            }),
        ),
        edges=(Edge(from_id="in", to_id="out", port="main"),),
    )

    with patch("aqueduct.executor.spark.executor.run_maintenance") as mock_maint:
        mock_maint.return_value = {"optimize_ms": 42, "vacuum_ms": None}
        result = execute(manifest, spark, store_dir=store_dir)

    assert result.status == "success"
    db_path = store_dir / "observability.db"
    assert db_path.exists()
    conn = duckdb.connect(str(db_path))
    rows = conn.execute("SELECT * FROM maintenance_metrics WHERE module_id = 'out'").fetchall()
    conn.close()
    assert len(rows) == 1
    assert rows[0][2] == 42  # optimize_ms column


def test_write_maintenance_metrics_store_dir_none_no_crash():
    """_write_maintenance_metrics with store_dir=None → no-op, no crash."""
    from aqueduct.executor.spark.executor import _write_maintenance_metrics
    _write_maintenance_metrics("m1", "run-1", {"optimize_ms": 10, "vacuum_ms": None}, None)


def test_write_maintenance_metrics_db_error_debug_only(tmp_path, caplog):
    """_write_maintenance_metrics failure → debug log only, no exception."""
    from aqueduct.executor.spark.executor import _write_maintenance_metrics
    import logging

    store_dir = tmp_path / "store_fail"
    store_dir.mkdir()
    # Write a corrupt/non-duckdb file at obs.db path to trigger DB error
    (store_dir / "observability.db").write_text("not a duckdb file")

    with caplog.at_level(logging.DEBUG):
        _write_maintenance_metrics("m1", "run-1", {"optimize_ms": 5}, store_dir)
