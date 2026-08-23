"""Phase 85 D2 (warnings/notes persistence) + D8 (run_records.engine column)."""

from __future__ import annotations

import json

import duckdb
import pytest

from aqueduct.compiler.models import Manifest
from aqueduct.executor.models import ExecutionResult, ModuleResult
from aqueduct.surveyor.surveyor import Surveyor

pytestmark = pytest.mark.unit


@pytest.fixture
def manifest():
    return Manifest(
        blueprint_id="test.blueprint", modules=(), edges=(), context={}, engine_config={}
    )


def test_warnings_and_notes_survive_round_trip_into_module_results(manifest, tmp_path):
    surveyor = Surveyor(manifest, store_dir=tmp_path, engine="duckdb")
    run_id = "run-warnings"
    surveyor.start(run_id)

    result = ExecutionResult(
        blueprint_id="p1",
        run_id=run_id,
        status="success",
        module_results=(
            ModuleResult(
                module_id="m1",
                status="success",
                warnings=(("perf_wide_shuffle", "shuffle partitions look high"),),
                notes=("Probe m1: row_count_estimate ~ 1000",),
            ),
        ),
    )
    surveyor.record(result)

    conn = duckdb.connect(str(tmp_path / "observability.db"))
    raw = conn.execute(
        "SELECT module_results FROM run_records WHERE run_id = ?", [run_id]
    ).fetchone()[0]
    conn.close()

    module_results = json.loads(raw)
    assert len(module_results) == 1
    m1 = module_results[0]
    assert m1["warnings"] == [["perf_wide_shuffle", "shuffle partitions look high"]]
    assert m1["notes"] == ["Probe m1: row_count_estimate ~ 1000"]


def test_warnings_and_notes_default_to_empty_lists(manifest, tmp_path):
    surveyor = Surveyor(manifest, store_dir=tmp_path, engine="duckdb")
    run_id = "run-no-warnings"
    surveyor.start(run_id)

    result = ExecutionResult(
        blueprint_id="p1",
        run_id=run_id,
        status="success",
        module_results=(ModuleResult(module_id="m1", status="success"),),
    )
    surveyor.record(result)

    conn = duckdb.connect(str(tmp_path / "observability.db"))
    raw = conn.execute(
        "SELECT module_results FROM run_records WHERE run_id = ?", [run_id]
    ).fetchone()[0]
    conn.close()

    m1 = json.loads(raw)[0]
    assert m1["warnings"] == []
    assert m1["notes"] == []


def test_run_records_engine_column_populated_on_start_and_record(manifest, tmp_path):
    surveyor = Surveyor(manifest, store_dir=tmp_path, engine="duckdb")
    run_id = "run-engine"
    surveyor.start(run_id)

    conn = duckdb.connect(str(tmp_path / "observability.db"))
    engine_after_start = conn.execute(
        "SELECT engine FROM run_records WHERE run_id = ?", [run_id]
    ).fetchone()[0]
    conn.close()
    assert engine_after_start == "duckdb"

    result = ExecutionResult(
        blueprint_id="p1",
        run_id=run_id,
        status="success",
        module_results=(ModuleResult(module_id="m1", status="success"),),
    )
    surveyor.record(result)

    conn = duckdb.connect(str(tmp_path / "observability.db"))
    engine_after_record = conn.execute(
        "SELECT engine FROM run_records WHERE run_id = ?", [run_id]
    ).fetchone()[0]
    conn.close()
    assert engine_after_record == "duckdb"


def test_run_records_engine_column_respects_record_override(manifest, tmp_path):
    """Polyglot runs pass `Surveyor.record(result, engine=<failing island>)`
    overriding the Surveyor's own construction-time engine."""
    surveyor = Surveyor(manifest, store_dir=tmp_path, engine="spark")
    run_id = "run-engine-override"
    surveyor.start(run_id)

    result = ExecutionResult(
        blueprint_id="p1",
        run_id=run_id,
        status="success",
        module_results=(ModuleResult(module_id="m1", status="success"),),
    )
    surveyor.record(result, engine="duckdb")

    conn = duckdb.connect(str(tmp_path / "observability.db"))
    engine = conn.execute("SELECT engine FROM run_records WHERE run_id = ?", [run_id]).fetchone()[0]
    conn.close()
    assert engine == "duckdb"
