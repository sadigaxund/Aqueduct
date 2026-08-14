"""End-to-end tests for ``aqueduct.executor.orchestrator.run_polyglot`` against
REAL Spark and DuckDB sessions (Phase 81 step 3) — the runtime half of the
cross-engine handoff. Covers: transport both directions, the spill lifecycle
(delete on success, keep on failure, resume-skip), the orphan sweep wired
through a real run, and a disjoint different-engine Blueprint running as two
independent flows with zero handoffs.

Needs a real SparkSession (``local[1]``, Java 17) and DuckDB's bare
``:memory:`` connection — both engines' capability declarations and
``ExecutorProtocol``s auto-register via ``aqueduct.executor.capabilities.
load_engines()`` (the ``aqueduct.engines`` entry points), so no explicit
capability-registration import is needed here.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

pytestmark = [pytest.mark.spark, pytest.mark.integration]

from pyspark.sql import SparkSession

from aqueduct.compiler.compiler import compile as ccompile
from aqueduct.executor.duckdb_.egress import EgressError
from aqueduct.executor.models import ExecutionStatus
from aqueduct.executor.orchestrator import run_polyglot
from aqueduct.executor.models import manifest_hash as _manifest_hash
from aqueduct.models import ModuleType
from aqueduct.parser.parser import parse_dict
from aqueduct.surveyor.surveyor import Surveyor


def _bp(modules, edges):
    d = {
        "aqueduct": "1.0",
        "id": "polyglot_test_bp",
        "name": "t",
        "modules": modules,
        "edges": edges,
    }
    return parse_dict(d, base_dir=Path("/tmp"))


def _handoff_id(manifest):
    ids = [m.id for m in manifest.modules if m.type == ModuleType.Handoff]
    assert len(ids) == 1, "expected exactly one handoff module"
    return ids[0]


def _module_metrics_row(store_dir: Path, run_id: str, module_id: str):
    con = duckdb.connect(str(store_dir / "observability.db"))
    try:
        return con.execute(
            "SELECT bytes_written, bytes_read FROM module_metrics WHERE run_id = ? AND module_id = ?",
            [run_id, module_id],
        ).fetchone()
    finally:
        con.close()


# ── Spark -> handoff -> DuckDB ───────────────────────────────────────────────


def test_spark_to_duckdb_handoff_e2e_success(spark: SparkSession, tmp_path):
    in_path = str(tmp_path / "in.parquet")
    out_path = str(tmp_path / "out")
    spark.range(5).withColumnRenamed("id", "n").write.parquet(in_path)

    bp = _bp(
        [
            {
                "id": "in",
                "label": "in",
                "type": "Ingress",
                "engine": "spark",
                "config": {"format": "parquet", "path": in_path},
            },
            {
                "id": "out",
                "label": "out",
                "type": "Egress",
                "engine": "duckdb",
                "config": {"format": "parquet", "path": out_path, "mode": "overwrite"},
            },
        ],
        edges=[{"from": "in", "to": "out"}],
    )
    manifest = ccompile(bp, engine="spark")
    assert len(manifest.islands) == 2
    handoff_id = _handoff_id(manifest)

    handoff_root = str(tmp_path / "handoff_root")
    store_dir = tmp_path / "obs"
    surveyor = Surveyor(manifest, store_dir, engine="spark")
    run_id = "run-spark-to-duckdb"
    surveyor.start(run_id)

    result = run_polyglot(
        manifest,
        run_id=run_id,
        handoff_root=handoff_root,
        store_dir=store_dir,
        surveyor=surveyor,
        master_url="local[1]",
    )

    assert result.status == ExecutionStatus.SUCCESS, result.module_results
    statuses = {r.module_id: r.status for r in result.module_results}
    assert statuses["in"] == ExecutionStatus.SUCCESS
    assert statuses[handoff_id] == ExecutionStatus.SUCCESS
    assert statuses["out"] == ExecutionStatus.SUCCESS

    # Data actually crossed the boundary. DuckDB Egress writes `path` as a
    # single file (COPY ... TO), not a directory of parts.
    written = duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{out_path}')").fetchone()[0]
    assert written == 5

    # bytes_written recorded for the handoff module.
    bytes_written, _ = _module_metrics_row(store_dir, run_id, handoff_id)
    assert bytes_written is not None and bytes_written > 0

    # Spill deleted on success.
    manifest_h = _manifest_hash(manifest)
    run_dir = Path(handoff_root) / manifest_h / run_id
    assert not run_dir.exists()


def test_duckdb_to_spark_handoff_e2e_success(spark: SparkSession, tmp_path):
    """Reverse direction — DuckDB produces, Spark consumes."""
    in_path = str(tmp_path / "in.parquet")
    out_path = str(tmp_path / "out.parquet")
    duckdb.sql("SELECT range AS n FROM range(7)").to_parquet(in_path)

    bp = _bp(
        [
            {
                "id": "in",
                "label": "in",
                "type": "Ingress",
                "engine": "duckdb",
                "config": {"format": "parquet", "path": in_path},
            },
            {
                "id": "out",
                "label": "out",
                "type": "Egress",
                "engine": "spark",
                "config": {"format": "parquet", "path": out_path, "mode": "overwrite"},
            },
        ],
        edges=[{"from": "in", "to": "out"}],
    )
    manifest = ccompile(bp, engine="spark")
    handoff_id = _handoff_id(manifest)

    handoff_root = str(tmp_path / "handoff_root")
    store_dir = tmp_path / "obs"
    surveyor = Surveyor(manifest, store_dir, engine="spark")
    run_id = "run-duckdb-to-spark"
    surveyor.start(run_id)

    result = run_polyglot(
        manifest,
        run_id=run_id,
        handoff_root=handoff_root,
        store_dir=store_dir,
        surveyor=surveyor,
        master_url="local[1]",
    )

    assert result.status == ExecutionStatus.SUCCESS, result.module_results
    assert spark.read.parquet(out_path).count() == 7

    bytes_written, _ = _module_metrics_row(store_dir, run_id, handoff_id)
    assert bytes_written is not None and bytes_written > 0

    manifest_h = _manifest_hash(manifest)
    run_dir = Path(handoff_root) / manifest_h / run_id
    assert not run_dir.exists()


# ── Hub type fidelity across the boundary: timestamp_tz / timestamp_ntz ─────
#
# Regression coverage for the defect fixed in `aqueduct/executor/spark/
# session.py`: Spark's OWN default for `spark.sql.parquet.outputTimestampType`
# is `INT96`, which carries no Parquet logical-type annotation, so a hub
# `timestamp_tz` column (Spark's plain `timestamp`) written on the Spark side
# of a handoff used to silently read back on DuckDB as a naive `TIMESTAMP`
# instead of `TIMESTAMPTZ` — no error, just quiet loss of timezone awareness.
# `make_spark_session` now sets `TIMESTAMP_MICROS` as Aqueduct's own default at
# session creation. Verified to fail without that default: temporarily
# reverting the conf in `session.py` and rerunning
# `test_timestamp_tz_survives_spark_to_duckdb_handoff` makes it fail (DuckDB's
# `DESCRIBE` reports `TIMESTAMP`, not `TIMESTAMP WITH TIME ZONE`).


def test_timestamp_tz_survives_spark_to_duckdb_handoff(spark: SparkSession, tmp_path):
    """The regression test for the defect: a hub `timestamp_tz` column must
    keep its timezone-awareness crossing a real Spark -> DuckDB handoff."""
    in_path = str(tmp_path / "in.parquet")
    out_path = str(tmp_path / "out")
    spark.sql("SELECT TIMESTAMP'2024-06-15 10:30:00' AS ts, 1 AS n").write.parquet(in_path)

    bp = _bp(
        [
            {
                "id": "in",
                "label": "in",
                "type": "Ingress",
                "engine": "spark",
                "config": {"format": "parquet", "path": in_path},
            },
            {
                "id": "out",
                "label": "out",
                "type": "Egress",
                "engine": "duckdb",
                "config": {"format": "parquet", "path": out_path, "mode": "overwrite"},
            },
        ],
        edges=[{"from": "in", "to": "out"}],
    )
    manifest = ccompile(bp, engine="spark")
    handoff_root = str(tmp_path / "handoff_root")
    store_dir = tmp_path / "obs"
    surveyor = Surveyor(manifest, store_dir, engine="spark")
    run_id = "run-tz-fidelity"
    surveyor.start(run_id)

    result = run_polyglot(
        manifest,
        run_id=run_id,
        handoff_root=handoff_root,
        store_dir=store_dir,
        surveyor=surveyor,
        master_url="local[1]",
    )
    assert result.status == ExecutionStatus.SUCCESS, result.module_results

    described = duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{out_path}')").fetchall()
    col_types = {row[0]: row[1] for row in described}
    assert col_types["ts"] == "TIMESTAMP WITH TIME ZONE", (
        "timestamp_tz lost timezone-awareness crossing the Spark -> DuckDB "
        f"handoff (got {col_types['ts']!r} — see the regression note above)"
    )


def test_timestamp_ntz_unaffected_by_output_timestamp_type_default(spark: SparkSession, tmp_path):
    """`timestamp_ntz` was never affected by the defect (Spark always writes
    it with a modern, correctly-annotated logical type); confirm it still
    round-trips as a naive TIMESTAMP now that the session default changed."""
    in_path = str(tmp_path / "in.parquet")
    out_path = str(tmp_path / "out")
    spark.sql("SELECT CAST('2024-06-15 10:30:00' AS TIMESTAMP_NTZ) AS ts, 1 AS n").write.parquet(
        in_path
    )

    bp = _bp(
        [
            {
                "id": "in",
                "label": "in",
                "type": "Ingress",
                "engine": "spark",
                "config": {"format": "parquet", "path": in_path},
            },
            {
                "id": "out",
                "label": "out",
                "type": "Egress",
                "engine": "duckdb",
                "config": {"format": "parquet", "path": out_path, "mode": "overwrite"},
            },
        ],
        edges=[{"from": "in", "to": "out"}],
    )
    manifest = ccompile(bp, engine="spark")
    handoff_root = str(tmp_path / "handoff_root")
    store_dir = tmp_path / "obs"
    surveyor = Surveyor(manifest, store_dir, engine="spark")
    run_id = "run-ntz-fidelity"
    surveyor.start(run_id)

    result = run_polyglot(
        manifest,
        run_id=run_id,
        handoff_root=handoff_root,
        store_dir=store_dir,
        surveyor=surveyor,
        master_url="local[1]",
    )
    assert result.status == ExecutionStatus.SUCCESS, result.module_results

    described = duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{out_path}')").fetchall()
    col_types = {row[0]: row[1] for row in described}
    assert col_types["ts"] == "TIMESTAMP"


def test_timestamp_tz_survives_duckdb_to_spark_handoff(spark: SparkSession, tmp_path):
    """Reverse direction is unaffected in both variants: DuckDB's own Parquet
    writer always annotates the logical type correctly."""
    in_path = str(tmp_path / "in.parquet")
    out_path = str(tmp_path / "out.parquet")
    duckdb.sql("SELECT TIMESTAMPTZ '2024-06-15 10:30:00+00' AS ts, 1 AS n").to_parquet(in_path)

    bp = _bp(
        [
            {
                "id": "in",
                "label": "in",
                "type": "Ingress",
                "engine": "duckdb",
                "config": {"format": "parquet", "path": in_path},
            },
            {
                "id": "out",
                "label": "out",
                "type": "Egress",
                "engine": "spark",
                "config": {"format": "parquet", "path": out_path, "mode": "overwrite"},
            },
        ],
        edges=[{"from": "in", "to": "out"}],
    )
    manifest = ccompile(bp, engine="spark")
    handoff_root = str(tmp_path / "handoff_root")
    store_dir = tmp_path / "obs"
    surveyor = Surveyor(manifest, store_dir, engine="spark")
    run_id = "run-tz-duckdb-to-spark"
    surveyor.start(run_id)

    result = run_polyglot(
        manifest,
        run_id=run_id,
        handoff_root=handoff_root,
        store_dir=store_dir,
        surveyor=surveyor,
        master_url="local[1]",
    )
    assert result.status == ExecutionStatus.SUCCESS, result.module_results

    row = spark.read.parquet(out_path).collect()[0]
    field_type = spark.read.parquet(out_path).schema["ts"].dataType.typeName()
    assert field_type == "timestamp"  # Spark's tz-aware instant type (hub timestamp_tz)


# ── Spill kept on failure, reused on resume ──────────────────────────────────


def test_spill_kept_on_failure_and_reused_on_resume(spark: SparkSession, tmp_path, monkeypatch):
    """Island A (spark) succeeds and spills; island B (duckdb Egress) fails on
    the FIRST attempt (a simulated transient fault, unrelated to the
    Blueprint). Re-running with ``resume_run_id`` set to the failed run must
    skip island A entirely (it is marked ``skipped``, never re-executed) and
    read its already-materialized spill — the resume story."""
    in_path = str(tmp_path / "in.parquet")
    out_path = str(tmp_path / "out")
    spark.range(4).withColumnRenamed("id", "n").write.parquet(in_path)

    bp = _bp(
        [
            {
                "id": "in",
                "label": "in",
                "type": "Ingress",
                "engine": "spark",
                "config": {"format": "parquet", "path": in_path},
            },
            {
                "id": "out",
                "label": "out",
                "type": "Egress",
                "engine": "duckdb",
                "config": {"format": "parquet", "path": out_path, "mode": "overwrite"},
            },
        ],
        edges=[{"from": "in", "to": "out"}],
    )
    manifest = ccompile(bp, engine="spark")
    manifest_h = _manifest_hash(manifest)
    handoff_root = str(tmp_path / "handoff_root")
    store_dir = tmp_path / "obs"

    import aqueduct.executor.duckdb_.executor as duckdb_exec_mod

    real_write_egress = duckdb_exec_mod.write_egress
    call_count = {"n": 0}

    def flaky_write_egress(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise EgressError("simulated transient failure")
        return real_write_egress(*args, **kwargs)

    monkeypatch.setattr(duckdb_exec_mod, "write_egress", flaky_write_egress)

    surveyor1 = Surveyor(manifest, store_dir, engine="spark")
    run_id_1 = "run-fail-1"
    surveyor1.start(run_id_1)
    result1 = run_polyglot(
        manifest,
        run_id=run_id_1,
        handoff_root=handoff_root,
        store_dir=store_dir,
        surveyor=surveyor1,
        master_url="local[1]",
    )
    assert result1.status == ExecutionStatus.ERROR
    statuses1 = {r.module_id: r.status for r in result1.module_results}
    assert statuses1["in"] == ExecutionStatus.SUCCESS  # write side completed

    run1_dir = Path(handoff_root) / manifest_h / run_id_1
    assert run1_dir.exists()  # kept — keep_on_failure defaults to True

    # Second attempt: the transient fault is gone (call_count now > 1 on the
    # next invocation), resuming from the failed run.
    surveyor2 = Surveyor(manifest, store_dir, engine="spark")
    run_id_2 = "run-fail-2-resumed"
    surveyor2.start(run_id_2)
    result2 = run_polyglot(
        manifest,
        run_id=run_id_2,
        handoff_root=handoff_root,
        store_dir=store_dir,
        surveyor=surveyor2,
        master_url="local[1]",
        resume_run_id=run_id_1,
    )

    assert result2.status == ExecutionStatus.SUCCESS, result2.module_results
    statuses2 = {r.module_id: r.status for r in result2.module_results}
    assert statuses2["in"] == ExecutionStatus.SKIPPED  # NOT re-executed
    assert statuses2["out"] == ExecutionStatus.SUCCESS

    written = duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{out_path}')").fetchone()[0]
    assert written == 4

    # The resumed-FROM spill has now served its entire documented purpose
    # ("a rerun reads this instead of recomputing it") and is released. It
    # used to survive forever: the run's own cleanup only ever targeted its
    # OWN run_id directory, and run-1's `run_records` row stays `error`, so
    # `sweep_orphan_spills` kept exempting it under `keep_on_failure` too.
    assert not run1_dir.exists(), "a successful resume must release the spill it consumed"
    # This run's own directory goes as well — the ordinary delete-on-success.
    assert not (Path(handoff_root) / manifest_h / run_id_2).exists()


# ── Orphan sweep wired through a real run ────────────────────────────────────


def test_orphan_sweep_runs_at_the_start_of_run_polyglot(spark: SparkSession, tmp_path):
    in_path = str(tmp_path / "in.parquet")
    out_path = str(tmp_path / "out")
    spark.range(2).withColumnRenamed("id", "n").write.parquet(in_path)

    bp = _bp(
        [
            {
                "id": "in",
                "label": "in",
                "type": "Ingress",
                "engine": "spark",
                "config": {"format": "parquet", "path": in_path},
            },
            {
                "id": "out",
                "label": "out",
                "type": "Egress",
                "engine": "duckdb",
                "config": {"format": "parquet", "path": out_path, "mode": "overwrite"},
            },
        ],
        edges=[{"from": "in", "to": "out"}],
    )
    manifest = ccompile(bp, engine="spark")
    manifest_h = _manifest_hash(manifest)
    handoff_root = str(tmp_path / "handoff_root")
    store_dir = tmp_path / "obs"

    # Pre-seed a stale (terminal success, no cleanup ran) spill directory and
    # a kept-failure directory under the SAME manifest hash, before the real
    # observability store or run_records rows for the live run exist.
    stale_dir = Path(handoff_root) / manifest_h / "stale-old-run" / "some_edge"
    stale_dir.mkdir(parents=True)
    (stale_dir / "part-0.parquet").write_bytes(b"x")

    kept_dir = Path(handoff_root) / manifest_h / "kept-failed-run" / "some_edge"
    kept_dir.mkdir(parents=True)
    (kept_dir / "part-0.parquet").write_bytes(b"x")

    store_dir.mkdir(parents=True)
    obs_path = store_dir / "observability.db"
    con = duckdb.connect(str(obs_path))
    from aqueduct.surveyor.ddl import _DDL

    con.execute(_DDL)
    con.execute(
        "INSERT INTO run_records (run_id, blueprint_id, status, started_at, finished_at) "
        "VALUES ('stale-old-run', 'bp', 'success', now() - INTERVAL 1 HOUR, now() - INTERVAL 55 MINUTE)"
    )
    con.execute(
        "INSERT INTO run_records (run_id, blueprint_id, status, started_at, finished_at) "
        "VALUES ('kept-failed-run', 'bp', 'error', now() - INTERVAL 1 HOUR, now() - INTERVAL 55 MINUTE)"
    )
    con.close()

    surveyor = Surveyor(manifest, store_dir, engine="spark")
    run_id = "run-live"
    surveyor.start(run_id)
    result = run_polyglot(
        manifest,
        run_id=run_id,
        handoff_root=handoff_root,
        store_dir=store_dir,
        surveyor=surveyor,
        master_url="local[1]",
    )

    assert result.status == ExecutionStatus.SUCCESS, result.module_results
    assert not (Path(handoff_root) / manifest_h / "stale-old-run").exists()
    assert (Path(handoff_root) / manifest_h / "kept-failed-run").exists()


# ── Disjoint different-engine components, zero handoffs ─────────────────────


def test_disjoint_different_engine_components_run_as_two_pure_flows(spark: SparkSession, tmp_path):
    spark_in = str(tmp_path / "spark_in.parquet")
    spark_out = str(tmp_path / "spark_out")
    duckdb_in = str(tmp_path / "duckdb_in.parquet")
    duckdb_out = str(tmp_path / "duckdb_out")

    spark.range(3).withColumnRenamed("id", "n").write.parquet(spark_in)
    duckdb.sql("SELECT range AS n FROM range(6)").to_parquet(duckdb_in)

    bp = _bp(
        [
            {
                "id": "s_in",
                "label": "s_in",
                "type": "Ingress",
                "engine": "spark",
                "config": {"format": "parquet", "path": spark_in},
            },
            {
                "id": "s_out",
                "label": "s_out",
                "type": "Egress",
                "engine": "spark",
                "config": {"format": "parquet", "path": spark_out, "mode": "overwrite"},
            },
            {
                "id": "d_in",
                "label": "d_in",
                "type": "Ingress",
                "engine": "duckdb",
                "config": {"format": "parquet", "path": duckdb_in},
            },
            {
                "id": "d_out",
                "label": "d_out",
                "type": "Egress",
                "engine": "duckdb",
                "config": {"format": "parquet", "path": duckdb_out, "mode": "overwrite"},
            },
        ],
        edges=[
            {"from": "s_in", "to": "s_out"},
            {"from": "d_in", "to": "d_out"},
        ],
    )
    manifest = ccompile(bp, engine="spark")
    assert not any(m.type == ModuleType.Handoff for m in manifest.modules)
    assert len(manifest.islands) == 2

    handoff_root = str(tmp_path / "handoff_root")
    store_dir = tmp_path / "obs"
    surveyor = Surveyor(manifest, store_dir, engine="spark")
    run_id = "run-disjoint"
    surveyor.start(run_id)

    result = run_polyglot(
        manifest,
        run_id=run_id,
        handoff_root=handoff_root,
        store_dir=store_dir,
        surveyor=surveyor,
        master_url="local[1]",
    )

    assert result.status == ExecutionStatus.SUCCESS, result.module_results
    assert spark.read.parquet(spark_out).count() == 3
    written = duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{duckdb_out}')").fetchone()[0]
    assert written == 6


# ── record_result / failed_engine — the CLI integration seam ────────────────
#
# `aqueduct/cli/run.py`'s healing loop needs the returned `FailureContext`
# itself (it drives the whole heal decision), so it calls `run_polyglot(...,
# record_result=False)` and does its own `surveyor.record(result, exc=...,
# engine=result.failed_engine)` afterwards — the same shape the single-engine
# path already uses. These tests guard that seam directly.


def test_record_result_false_skips_the_internal_surveyor_record(spark: SparkSession, tmp_path):
    from unittest.mock import MagicMock

    in_path = str(tmp_path / "in.parquet")
    out_path = str(tmp_path / "out")
    spark.range(3).withColumnRenamed("id", "n").write.parquet(in_path)

    bp = _bp(
        [
            {
                "id": "in",
                "label": "in",
                "type": "Ingress",
                "engine": "spark",
                "config": {"format": "parquet", "path": in_path},
            },
            {
                "id": "out",
                "label": "out",
                "type": "Egress",
                "engine": "duckdb",
                "config": {"format": "parquet", "path": out_path, "mode": "overwrite"},
            },
        ],
        edges=[{"from": "in", "to": "out"}],
    )
    manifest = ccompile(bp, engine="spark")
    handoff_root = str(tmp_path / "handoff_root")
    store_dir = tmp_path / "obs"

    mock_surveyor = MagicMock()
    result = run_polyglot(
        manifest,
        run_id="run-no-record",
        handoff_root=handoff_root,
        store_dir=store_dir,
        surveyor=mock_surveyor,
        master_url="local[1]",
        record_result=False,
    )

    assert result.status == ExecutionStatus.SUCCESS
    mock_surveyor.record.assert_not_called()


def test_record_result_true_default_still_records_internally(spark: SparkSession, tmp_path):
    """The pre-existing, independently-tested contract (surveyor.record()
    called internally) must survive unchanged for a caller that doesn't pass
    record_result at all."""
    from unittest.mock import MagicMock

    in_path = str(tmp_path / "in.parquet")
    out_path = str(tmp_path / "out")
    spark.range(3).withColumnRenamed("id", "n").write.parquet(in_path)

    bp = _bp(
        [
            {
                "id": "in",
                "label": "in",
                "type": "Ingress",
                "engine": "spark",
                "config": {"format": "parquet", "path": in_path},
            },
            {
                "id": "out",
                "label": "out",
                "type": "Egress",
                "engine": "duckdb",
                "config": {"format": "parquet", "path": out_path, "mode": "overwrite"},
            },
        ],
        edges=[{"from": "in", "to": "out"}],
    )
    manifest = ccompile(bp, engine="spark")
    handoff_root = str(tmp_path / "handoff_root")
    store_dir = tmp_path / "obs"

    mock_surveyor = MagicMock()
    result = run_polyglot(
        manifest,
        run_id="run-default-record",
        handoff_root=handoff_root,
        store_dir=store_dir,
        surveyor=mock_surveyor,
        master_url="local[1]",
    )

    assert result.status == ExecutionStatus.SUCCESS
    mock_surveyor.record.assert_called_once_with(result, engine=None)


def test_failed_engine_names_the_failing_island_not_the_default(
    spark: SparkSession, tmp_path, monkeypatch
):
    """A structural failure raised straight out of one island's ``execute()``
    (not caught as a per-module ModuleResult) must still be attributed to
    THAT island's engine on the returned ExecutionResult — the gap fixed by
    wrapping `call_execute()` in `AqueductError` inside `run_polyglot()`."""
    import aqueduct.executor.duckdb_.executor as duckdb_exec_mod
    from aqueduct.executor.duckdb_.executor import ExecuteError as DuckDBExecuteError

    in_path = str(tmp_path / "in.parquet")
    out_path = str(tmp_path / "out")
    spark.range(3).withColumnRenamed("id", "n").write.parquet(in_path)

    bp = _bp(
        [
            {
                "id": "in",
                "label": "in",
                "type": "Ingress",
                "engine": "spark",
                "config": {"format": "parquet", "path": in_path},
            },
            {
                "id": "out",
                "label": "out",
                "type": "Egress",
                "engine": "duckdb",
                "config": {"format": "parquet", "path": out_path, "mode": "overwrite"},
            },
        ],
        edges=[{"from": "in", "to": "out"}],
    )
    manifest = ccompile(bp, engine="spark")
    handoff_root = str(tmp_path / "handoff_root")
    store_dir = tmp_path / "obs"

    def _boom(*args, **kwargs):
        raise DuckDBExecuteError("simulated structural failure — never a ModuleResult")

    monkeypatch.setattr(duckdb_exec_mod, "execute", _boom)

    result = run_polyglot(
        manifest,
        run_id="run-structural-fail",
        handoff_root=handoff_root,
        store_dir=store_dir,
        master_url="local[1]",
    )

    assert result.status == ExecutionStatus.ERROR
    assert result.failed_engine == "duckdb"
    assert any(
        r.module_id == "_executor" and "simulated structural failure" in (r.error or "")
        for r in result.module_results
    )
    # The upstream spark island's own module result is still present — the
    # exception from the duckdb island doesn't erase what already ran.
    assert any(r.module_id == "in" for r in result.module_results)
