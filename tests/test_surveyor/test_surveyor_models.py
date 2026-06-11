"""Tests for Surveyor data models."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError

import pytest
pytestmark = pytest.mark.unit

from aqueduct.surveyor.models import FailureContext, RunRecord


def test_run_record_frozen():
    record = RunRecord(
        run_id="r1",
        blueprint_id="p1",
        status="running",
        started_at="2024-01-01T00:00:00Z",
        finished_at=None,
        module_results=()
    )
    with pytest.raises(FrozenInstanceError):
        record.status = "success"  # type: ignore[misc]


def test_run_record_to_dict():
    record = RunRecord(
        run_id="r1",
        blueprint_id="p1",
        status="success",
        started_at="2024-01-01T00:00:00Z",
        finished_at="2024-01-01T00:01:00Z",
        module_results=({"id": "m1", "status": "success"},)
    )
    d = record.to_dict()
    assert d["run_id"] == "r1"
    assert d["status"] == "success"
    assert d["module_results"] == [{"id": "m1", "status": "success"}]


def test_failure_context_frozen():
    ctx = FailureContext(
        run_id="r1",
        blueprint_id="p1",
        failed_module="m1",
        error_message="oops",
        stack_trace=None,
        manifest_json="{}",
        started_at="2024-01-01T00:00:00Z",
        finished_at="2024-01-01T00:01:00Z"
    )
    with pytest.raises(FrozenInstanceError):
        ctx.error_message = "hacked"  # type: ignore[misc]


def test_failure_context_to_dict():
    ctx = FailureContext(
        run_id="r1",
        blueprint_id="p1",
        failed_module="m1",
        error_message="oops",
        stack_trace="trace",
        manifest_json='{"id": "p1"}',
        started_at="2024-01-01T00:00:00Z",
        finished_at="2024-01-01T00:01:00Z"
    )
    d = ctx.to_dict()
    assert d["run_id"] == "r1"
    assert d["failed_module"] == "m1"
    assert d["stack_trace"] == "trace"
    assert d["manifest_json"] == '{"id": "p1"}'


def test_failure_context_to_json():
    ctx = FailureContext(
        run_id="r1",
        blueprint_id="p1",
        failed_module="m1",
        error_message="oops",
        stack_trace=None,
        manifest_json="{}",
        started_at="2024-01-01T00:00:00Z",
        finished_at="2024-01-01T00:01:00Z"
    )
    js = ctx.to_json()
    data = json.loads(js)
    assert data["run_id"] == "r1"
    assert data["error_message"] == "oops"


def test_failure_context_to_dict_includes_doctor_hints():
    """FailureContext.to_dict() includes doctor_hints list."""
    ctx = FailureContext(
        run_id="r1",
        blueprint_id="p1",
        failed_module="m1",
        error_message="oops",
        stack_trace=None,
        manifest_json="{}",
        started_at="2024-01-01T00:00:00Z",
        finished_at="2024-01-01T00:01:00Z",
        doctor_hints=("warn: bad path", "fail: missing schema"),
    )
    d = ctx.to_dict()
    assert "doctor_hints" in d
    assert d["doctor_hints"] == ["warn: bad path", "fail: missing schema"]


def test_failure_context_doctor_hints_empty_by_default():
    """FailureContext.doctor_hints defaults to empty tuple; to_dict() yields []."""
    ctx = FailureContext(
        run_id="r1",
        blueprint_id="p1",
        failed_module="m1",
        error_message="oops",
        stack_trace=None,
        manifest_json="{}",
        started_at="2024-01-01T00:00:00Z",
        finished_at="2024-01-01T00:01:00Z",
    )
    assert ctx.doctor_hints == ()
    d = ctx.to_dict()
    assert d["doctor_hints"] == []


def test_failure_context_phase35_defaults():
    """Phase 35 defaults for structured error fields."""
    ctx = FailureContext(
        run_id="r1",
        blueprint_id="p1",
        failed_module="m1",
        error_message="oops",
        stack_trace=None,
        manifest_json="{}",
        started_at="2024-01-01T00:00:00Z",
        finished_at="2024-01-01T00:01:00Z",
    )
    assert ctx.error_class is None
    assert ctx.root_exception is None
    assert ctx.sql_state is None
    assert ctx.suggested_columns == ()
    assert ctx.object_name is None


def test_failure_context_phase35_frozen():
    """Phase 35: mutating any new field raises FrozenInstanceError."""
    ctx = FailureContext(
        run_id="r1",
        blueprint_id="p1",
        failed_module="m1",
        error_message="oops",
        stack_trace=None,
        manifest_json="{}",
        started_at="2024-01-01T00:00:00Z",
        finished_at="2024-01-01T00:01:00Z",
    )
    with pytest.raises(FrozenInstanceError):
        ctx.error_class = "X"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        ctx.suggested_columns = ("a",)  # type: ignore[misc]


def test_failure_context_phase35_to_dict():
    """Phase 35: to_dict() round-trips the 5 new keys; suggested_columns serialized as list."""
    ctx = FailureContext(
        run_id="r1",
        blueprint_id="p1",
        failed_module="m1",
        error_message="oops",
        stack_trace=None,
        manifest_json="{}",
        started_at="2024-01-01T00:00:00Z",
        finished_at="2024-01-01T00:01:00Z",
        error_class="UNRESOLVED_COLUMN",
        root_exception={"type": "AnalysisException", "message": "msg"},
        sql_state="42000",
        suggested_columns=("a", "b"),
        object_name="c",
    )
    d = ctx.to_dict()
    assert d["error_class"] == "UNRESOLVED_COLUMN"
    assert d["root_exception"] == {"type": "AnalysisException", "message": "msg"}
    assert d["sql_state"] == "42000"
    assert d["suggested_columns"] == ["a", "b"]
    assert d["object_name"] == "c"


# ── Phase 45 — healing_outcomes DDL + record + successful_patch_ids ───────────

def test_healing_outcomes_ddl_has_phase45_columns():
    """Fresh DB DDL includes failure_signature + resolution columns."""
    import duckdb
    from aqueduct.surveyor.surveyor import _DDL
    conn = duckdb.connect(":memory:")
    conn.execute(_DDL)
    cols = {r[0] for r in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'healing_outcomes'").fetchall()}
    assert "failure_signature" in cols
    assert "resolution" in cols
    conn.close()


def test_phase45_migration_idempotent_on_fresh_db():
    """_PHASE45_MIGRATION_DDL runs without error on a fresh DB."""
    import duckdb
    from aqueduct.surveyor.surveyor import _DDL, _PHASE45_MIGRATION_DDL
    conn = duckdb.connect(":memory:")
    conn.execute(_DDL)
    conn.execute(_PHASE45_MIGRATION_DDL)
    conn.execute(_PHASE45_MIGRATION_DDL)  # second call must not raise
    conn.close()


def test_phase45_migration_adds_missing_columns():
    """Pre-Phase-45 DB (no columns) → migration adds them idempotently."""
    import duckdb
    from aqueduct.surveyor.surveyor import _PHASE45_MIGRATION_DDL
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE healing_outcomes (
            id VARCHAR PRIMARY KEY,
            run_id VARCHAR, blueprint_id VARCHAR, failed_module VARCHAR,
            failure_category VARCHAR, model VARCHAR, patch_id VARCHAR,
            confidence FLOAT, patch_applied BOOLEAN,
            run_success_after_patch BOOLEAN, applied_at TIMESTAMPTZ,
            prompt_version VARCHAR
        )
    """)
    cols_before = {r[0] for r in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'healing_outcomes'").fetchall()}
    assert "failure_signature" not in cols_before
    conn.execute(_PHASE45_MIGRATION_DDL)
    cols_after = {r[0] for r in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'healing_outcomes'").fetchall()}
    assert "failure_signature" in cols_after
    assert "resolution" in cols_after
    conn.execute(_PHASE45_MIGRATION_DDL)  # idempotent
    conn.close()


def test_record_healing_outcome_persists_failure_signature(tmp_path):
    """record_healing_outcome persists failure_signature and resolution."""
    import duckdb
    from aqueduct.surveyor.surveyor import Surveyor, _DDL
    from aqueduct.compiler.models import Manifest

    manifest = Manifest(blueprint_id="bp1", context={}, modules=(), edges=(), spark_config={})
    store_dir = tmp_path / "obs"
    store_dir.mkdir()
    surveyor = Surveyor(manifest, store_dir=store_dir)
    surveyor.start("run-fs-1")

    surveyor.record_healing_outcome(
        run_id="run-fs-1", failed_module="m1", failure_category="test",
        model="claude", patch_id="fix-1", confidence=0.9,
        patch_applied=True, run_success_after_patch=True,
        failure_signature="abc123hash", resolution="llm",
    )
    surveyor.stop()

    conn = duckdb.connect(str(store_dir / "observability.db"))
    row = conn.execute("SELECT failure_signature, resolution FROM healing_outcomes WHERE patch_id = 'fix-1'").fetchone()
    conn.close()
    assert row is not None
    assert row[0] == "abc123hash"
    assert row[1] == "llm"


def test_record_healing_outcome_defaults_resolution_llm(tmp_path):
    """record_healing_outcome defaults resolution to 'llm'."""
    import duckdb
    from aqueduct.surveyor.surveyor import Surveyor, _DDL
    from aqueduct.compiler.models import Manifest

    manifest = Manifest(blueprint_id="bp1", context={}, modules=(), edges=(), spark_config={})
    store_dir = tmp_path / "obs2"
    store_dir.mkdir()
    surveyor = Surveyor(manifest, store_dir=store_dir)
    surveyor.start("run-dflt-1")

    surveyor.record_healing_outcome(
        run_id="run-dflt-1", failed_module="m1", failure_category="test",
        model="claude", patch_id="fix-dflt", confidence=0.9,
        patch_applied=True, run_success_after_patch=True,
    )
    surveyor.stop()

    conn = duckdb.connect(str(store_dir / "observability.db"))
    row = conn.execute("SELECT resolution FROM healing_outcomes WHERE patch_id = 'fix-dflt'").fetchone()
    conn.close()
    assert row[0] == "llm"


def test_successful_patch_ids_returns_matching_patches(tmp_path):
    """successful_patch_ids returns patch_ids with run_success_after_patch=true."""
    from aqueduct.surveyor.surveyor import Surveyor, _DDL
    from aqueduct.compiler.models import Manifest

    manifest = Manifest(blueprint_id="bp1", context={}, modules=(), edges=(), spark_config={})
    store_dir = tmp_path / "obs3"
    store_dir.mkdir()
    surveyor = Surveyor(manifest, store_dir=store_dir)
    surveyor.start("run-sp-1")

    surveyor.record_healing_outcome(
        run_id="run-sp-1", failed_module="m1", failure_category="test",
        model="claude", patch_id="success-1", confidence=0.9,
        patch_applied=True, run_success_after_patch=True,
    )
    surveyor.record_healing_outcome(
        run_id="run-sp-2", failed_module="m1", failure_category="test",
        model="claude", patch_id="failed-1", confidence=0.9,
        patch_applied=True, run_success_after_patch=False,
    )
    surveyor.stop()

    ids = surveyor.successful_patch_ids()
    assert "success-1" in ids
    assert "failed-1" not in ids


def test_successful_patch_ids_empty_on_store_error(tmp_path):
    """successful_patch_ids returns empty set when store is down."""
    from aqueduct.surveyor.surveyor import Surveyor
    from aqueduct.compiler.models import Manifest

    manifest = Manifest(blueprint_id="bp1", context={}, modules=(), edges=(), spark_config={})
    surveyor = Surveyor(manifest, store_dir=tmp_path / "missing")
    # start() never called → _observability is None
    ids = surveyor.successful_patch_ids()
    assert ids == set()


# ── Phase 46 — model_cascade_position ──────────────────────────────────────────

def test_healing_outcomes_ddl_has_model_cascade_position():
    """Fresh DB DDL includes model_cascade_position column."""
    import duckdb
    from aqueduct.surveyor.surveyor import _DDL
    conn = duckdb.connect(":memory:")
    conn.execute(_DDL)
    cols = {r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'healing_outcomes'"
    ).fetchall()}
    assert "model_cascade_position" in cols
    conn.close()


def test_phase46_migration_adds_model_cascade_position():
    """Pre-Phase-46 DB gets model_cascade_position via _PHASE45_MIGRATION_DDL."""
    import duckdb
    from aqueduct.surveyor.surveyor import _PHASE45_MIGRATION_DDL
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE healing_outcomes (
            id VARCHAR PRIMARY KEY,
            run_id VARCHAR, blueprint_id VARCHAR, failed_module VARCHAR,
            failure_category VARCHAR, model VARCHAR, patch_id VARCHAR,
            confidence FLOAT, patch_applied BOOLEAN,
            run_success_after_patch BOOLEAN, applied_at TIMESTAMPTZ,
            prompt_version VARCHAR, failure_signature VARCHAR, resolution VARCHAR
        )
    """)
    cols_before = {r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'healing_outcomes'"
    ).fetchall()}
    assert "model_cascade_position" not in cols_before
    conn.execute(_PHASE45_MIGRATION_DDL)
    cols_after = {r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'healing_outcomes'"
    ).fetchall()}
    assert "model_cascade_position" in cols_after
    conn.execute(_PHASE45_MIGRATION_DDL)  # idempotent
    conn.close()


def test_record_healing_outcome_persists_model_cascade_position(tmp_path):
    """record_healing_outcome persists model_cascade_position."""
    import duckdb
    from aqueduct.surveyor.surveyor import Surveyor, _DDL
    from aqueduct.compiler.models import Manifest

    manifest = Manifest(blueprint_id="bp1", context={}, modules=(), edges=(), spark_config={})
    store_dir = tmp_path / "obs_cp"
    store_dir.mkdir()
    surveyor = Surveyor(manifest, store_dir=store_dir)
    surveyor.start("run-cp-1")

    surveyor.record_healing_outcome(
        run_id="run-cp-1", failed_module="m1", failure_category="test",
        model="claude", patch_id="fix-cp-1", confidence=0.9,
        patch_applied=True, run_success_after_patch=True,
        model_cascade_position=1,
    )
    surveyor.stop()

    conn = duckdb.connect(str(store_dir / "observability.db"))
    row = conn.execute(
        "SELECT model_cascade_position FROM healing_outcomes WHERE patch_id = 'fix-cp-1'"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == 1


def test_record_healing_outcome_defaults_model_cascade_position_none(tmp_path):
    """record_healing_outcome defaults model_cascade_position to None."""
    import duckdb
    from aqueduct.surveyor.surveyor import Surveyor
    from aqueduct.compiler.models import Manifest

    manifest = Manifest(blueprint_id="bp1", context={}, modules=(), edges=(), spark_config={})
    store_dir = tmp_path / "obs_cp_dflt"
    store_dir.mkdir()
    surveyor = Surveyor(manifest, store_dir=store_dir)
    surveyor.start("run-cp-dflt-1")

    surveyor.record_healing_outcome(
        run_id="run-cp-dflt-1", failed_module="m1", failure_category="test",
        model="claude", patch_id="fix-cp-dflt", confidence=0.9,
        patch_applied=True, run_success_after_patch=True,
    )
    surveyor.stop()

    conn = duckdb.connect(str(store_dir / "observability.db"))
    row = conn.execute(
        "SELECT model_cascade_position FROM healing_outcomes WHERE patch_id = 'fix-cp-dflt'"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] is None


def test_record_healing_outcome_persists_cascade_model(tmp_path):
    """record_healing_outcome persists model + model_cascade_position for cascade tiers."""
    import duckdb
    from aqueduct.surveyor.surveyor import Surveyor
    from aqueduct.compiler.models import Manifest

    manifest = Manifest(blueprint_id="bp1", context={}, modules=(), edges=(), spark_config={})
    store_dir = tmp_path / "obs_cp"
    store_dir.mkdir()
    surveyor = Surveyor(manifest, store_dir=store_dir)
    surveyor.start("run-tm-1")

    surveyor.record_healing_outcome(
        run_id="run-tm-1", failed_module="m1", failure_category="test",
        model="gpt4", patch_id="fix-tm-1", confidence=0.9,
        patch_applied=True, run_success_after_patch=True,
        model_cascade_position=1,
    )
    surveyor.stop()

    conn = duckdb.connect(str(store_dir / "observability.db"))
    row = conn.execute(
        "SELECT model, model_cascade_position FROM healing_outcomes WHERE patch_id = 'fix-tm-1'"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == "gpt4"
    assert row[1] == 1
