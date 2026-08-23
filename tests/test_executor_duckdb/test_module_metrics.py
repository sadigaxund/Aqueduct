"""DuckDB executor `module_metrics` parity (Phase 85 D1 / audit finding #5).

Before this, `aqueduct/executor/duckdb_/executor.py` wrote a `module_metrics`
row ONLY for the synthetic Handoff module — every real module type (Ingress,
Channel, Junction, Funnel, Egress) wrote nothing, so `report --profile`/
`--trend` and the query layer's `run_detail()` were permanently empty for any
DuckDB blueprint. These tests cover the per-module write added to close that
gap, mirroring Spark's own `_write_stage_metrics` call sites/column set
(`aqueduct/executor/spark/executor.py`) — see `write_egress`'s and
`duckdb_/executor.py`'s dispatch-branch docstrings for exactly which fields
are genuinely derivable on this engine without an extra scan.
"""

from __future__ import annotations

import duckdb
import pytest

from aqueduct.executor.duckdb_.executor import execute
from aqueduct.executor.models import ExecutionStatus
from aqueduct.models import Edge, Manifest, Module
from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
from aqueduct.stores.queries import run_detail
from aqueduct.surveyor.surveyor import Surveyor

pytestmark = pytest.mark.duckdb


def _module(id_, type_, config, **kw):
    return Module(id=id_, type=type_, label=id_, config=config, **kw)


def _write_parquet(con, tmp_path, name, rows_sql):
    path = str(tmp_path / f"{name}.parquet")
    con.sql(f"COPY ({rows_sql}) TO '{path}' (FORMAT PARQUET)")
    return path


def _module_metrics_rows(store_dir, run_id):
    """Read every module_metrics row for run_id straight off the observability
    DB — bypassing the query layer so these tests exercise the executor's
    write, not `run_detail`'s merge (that gets its own dedicated test)."""
    store = DuckDBObservabilityStore(store_dir / "observability.db")
    with store.connect() as cur:
        cur.execute(
            "SELECT module_id, records_read, bytes_read, records_written, "
            "bytes_written, duration_ms FROM module_metrics WHERE run_id = ?",
            [run_id],
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ── One row per real module, non-NULL duration ─────────────────────────────


def test_multi_module_run_writes_one_module_metrics_row_per_module(duckdb_con, tmp_path):
    src_path = _write_parquet(
        duckdb_con, tmp_path, "src", "SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,'c')) t(id, name)"
    )
    out_path = str(tmp_path / "out.parquet")
    store_dir = tmp_path / "obs"

    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}),
        _module("ch", "Channel", {"op": "filter", "condition": "id > 1"}),
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
    )
    edges = (
        Edge(from_id="ing", to_id="ch", port="main"),
        Edge(from_id="ch", to_id="eg", port="main"),
    )
    manifest = Manifest(
        blueprint_id="test_bp", context={}, modules=modules, edges=edges, engine_config={}
    )

    result = execute(manifest, duckdb_con, run_id="r_mm1", store_dir=store_dir)
    assert result.status == ExecutionStatus.SUCCESS

    rows = _module_metrics_rows(store_dir, "r_mm1")
    by_module = {r["module_id"]: r for r in rows}

    # Exactly one row per real module — no double-write, none skipped.
    assert set(by_module) == {"ing", "ch", "eg"}
    assert len(rows) == 3

    # Every module's duration is measured (non-NULL) on this engine now,
    # matching Spark's parity (Ingress/Channel/Junction/Funnel/Egress all
    # write duration_ms — see spark/executor.py's `_write_stage_metrics`
    # call sites).
    for module_id, row in by_module.items():
        assert row["duration_ms"] is not None, f"{module_id} duration_ms is NULL"
        assert row["duration_ms"] >= 0

    # Egress is the one module type this engine can derive a real row count
    # for (COPY's own returned Count — see write_egress's docstring), so it
    # should carry a real records_written, not NULL.
    assert by_module["eg"]["records_written"] == 2  # id > 1 keeps rows 2 and 3


# ── D3: a genuinely zero-row write is a real 0, never NULL ─────────────────


def test_zero_row_egress_stores_records_written_as_real_zero(duckdb_con, tmp_path):
    src_path = _write_parquet(
        duckdb_con, tmp_path, "src", "SELECT * FROM (VALUES (1),(2),(3)) t(id)"
    )
    out_path = str(tmp_path / "empty_out.parquet")
    store_dir = tmp_path / "obs"

    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}),
        # Filters out every row — the Egress below writes a genuinely empty relation.
        _module("ch", "Channel", {"op": "filter", "condition": "id > 100"}),
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
    )
    edges = (
        Edge(from_id="ing", to_id="ch", port="main"),
        Edge(from_id="ch", to_id="eg", port="main"),
    )
    manifest = Manifest(
        blueprint_id="test_bp", context={}, modules=modules, edges=edges, engine_config={}
    )

    result = execute(manifest, duckdb_con, run_id="r_mm_zero", store_dir=store_dir)
    assert result.status == ExecutionStatus.SUCCESS

    rows = _module_metrics_rows(store_dir, "r_mm_zero")
    eg_row = next(r for r in rows if r["module_id"] == "eg")

    # The D3 case: a real 0, distinguishable from "not recorded" (None).
    assert eg_row["records_written"] == 0
    assert eg_row["records_written"] is not None
    assert isinstance(eg_row["records_written"], int)


# ── Handoff's existing write-side/read-side merge stays uncorrupted ────────


def test_handoff_write_read_rows_merge_uncorrupted_via_run_detail(tmp_path):
    con = duckdb.connect(":memory:")
    store_dir = tmp_path / "obs"
    spill_dir = tmp_path / "spill"

    src_path = _write_parquet(con, tmp_path, "src", "SELECT * FROM (VALUES (1),(2),(3)) t(id)")

    # Island A: Ingress -> Handoff (WRITE side).
    write_modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}),
        _module("ho", "Handoff", {}),
    )
    write_edges = (Edge(from_id="ing", to_id="ho", port="main"),)
    write_manifest = Manifest(
        blueprint_id="test_bp",
        context={},
        modules=write_modules,
        edges=write_edges,
        engine_config={},
    )
    r1 = execute(
        write_manifest,
        con,
        run_id="r_handoff",
        store_dir=store_dir,
        handoff_spill_uris={"ho": str(spill_dir)},
    )
    assert r1.status == ExecutionStatus.SUCCESS

    # Island B: Handoff (READ side) -> Egress.
    out_path = str(tmp_path / "out.parquet")
    read_modules = (
        _module("ho", "Handoff", {}),
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
    )
    read_edges = (Edge(from_id="ho", to_id="eg", port="main"),)
    read_manifest = Manifest(
        blueprint_id="test_bp",
        context={},
        modules=read_modules,
        edges=read_edges,
        engine_config={},
    )
    r2 = execute(
        read_manifest,
        con,
        run_id="r_handoff",
        store_dir=store_dir,
        handoff_spill_uris={"ho": str(spill_dir)},
    )
    assert r2.status == ExecutionStatus.SUCCESS

    # module_metrics carries TWO rows for "ho" under the same run_id (write
    # side + read side) — confirm both landed before checking the merge.
    raw_rows = _module_metrics_rows(store_dir, "r_handoff")
    ho_raw = [r for r in raw_rows if r["module_id"] == "ho"]
    assert len(ho_raw) == 2

    # Record the run so `run_detail()` (the query layer `report --profile`
    # reads through) can find it.
    manifest_for_surveyor = Manifest(
        blueprint_id="test_bp",
        context={},
        modules=write_modules + (_module("eg", "Egress", {}),),
        edges=(),
        engine_config={},
    )
    surveyor = Surveyor(manifest_for_surveyor, store_dir, engine="duckdb")
    surveyor.start("r_handoff")
    combined_module_results = tuple(r1.module_results) + tuple(r2.module_results)
    from aqueduct.executor.models import ExecutionResult

    combined = ExecutionResult(
        blueprint_id="test_bp",
        run_id="r_handoff",
        status=ExecutionStatus.SUCCESS,
        module_results=combined_module_results,
    )
    surveyor.record(combined)

    detail = run_detail(surveyor.observability, "r_handoff")
    assert detail is not None

    profile_by_id = {p.module_id: p for p in detail.profile}
    # Every module — including the twice-written Handoff — merges into
    # exactly ONE ProfileRow, never a duplicate.
    assert len(detail.profile) == len(profile_by_id)
    assert "ho" in profile_by_id

    ho_profile = profile_by_id["ho"]
    # The merge coalesced the write-side row's bytes_written and the
    # read-side row's bytes_read onto the SAME ProfileRow — neither field
    # was dropped by a last-row-wins overwrite.
    assert ho_profile.bytes_written is not None
    assert ho_profile.bytes_read is not None
    # Duration sums both sides' measured time (see run_detail's merge
    # comment) — never None when both sides recorded a real duration.
    assert ho_profile.duration_ms is not None


# ── report --profile-shaped data is non-empty for a DuckDB run ─────────────


def test_run_detail_profile_nonempty_for_duckdb_run(duckdb_con, tmp_path):
    src_path = _write_parquet(
        duckdb_con, tmp_path, "src", "SELECT * FROM (VALUES (1,'a'),(2,'b')) t(id, name)"
    )
    out_path = str(tmp_path / "out.parquet")
    store_dir = tmp_path / "obs"

    modules = (
        _module("ing", "Ingress", {"format": "parquet", "path": src_path}),
        _module("eg", "Egress", {"format": "parquet", "path": out_path, "mode": "overwrite"}),
    )
    edges = (Edge(from_id="ing", to_id="eg", port="main"),)
    manifest = Manifest(
        blueprint_id="test_bp", context={}, modules=modules, edges=edges, engine_config={}
    )

    result = execute(manifest, duckdb_con, run_id="r_profile", store_dir=store_dir)
    assert result.status == ExecutionStatus.SUCCESS

    surveyor = Surveyor(manifest, store_dir, engine="duckdb")
    surveyor.start("r_profile")
    surveyor.record(result)

    detail = run_detail(surveyor.observability, "r_profile")
    assert detail is not None
    # Before Phase 85 D1 this was empty for every DuckDB run (audit finding
    # #5) — report --profile/--trend had nothing to show.
    assert len(detail.profile) > 0
    profile_by_id = {p.module_id: p for p in detail.profile}
    assert profile_by_id["eg"].records_written == 2
    assert profile_by_id["eg"].duration_ms is not None
    assert profile_by_id["ing"].duration_ms is not None
