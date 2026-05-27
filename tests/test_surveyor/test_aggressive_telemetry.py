import pytest
import duckdb
from pathlib import Path
from unittest.mock import MagicMock
from aqueduct.compiler.models import Manifest
from aqueduct.executor.models import ExecutionResult, ModuleResult
from aqueduct.surveyor.surveyor import Surveyor

pytestmark = pytest.mark.unit

@pytest.fixture
def manifest():
    return Manifest(
        blueprint_id="test.blueprint",
        modules=(),
        edges=(),
        context={},
        spark_config={}
    )

def test_surveyor_legacy_db_migration(manifest, tmp_path):
    """Start with a legacy DB lacking parent_run_id and verify migration adds it."""
    db_path = tmp_path / "observability.db"
    
    # 1. Create a legacy table structure manually
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE run_records (
            run_id VARCHAR PRIMARY KEY,
            blueprint_id VARCHAR,
            status VARCHAR,
            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ,
            module_results JSON
        )
    """)
    conn.execute("""
        CREATE TABLE healing_outcomes (
            id VARCHAR PRIMARY KEY,
            run_id VARCHAR,
            failed_module VARCHAR,
            failure_category VARCHAR,
            model VARCHAR,
            patch_id VARCHAR,
            confidence DOUBLE,
            patch_applied BOOLEAN,
            run_success_after_patch BOOLEAN,
            applied_at TIMESTAMPTZ,
            prompt_version VARCHAR
        )
    """)
    
    # Insert some initial data
    conn.execute("INSERT INTO run_records VALUES ('legacy-run', 'test.blueprint', 'success', '2026-01-01T00:00:00Z', '2026-01-01T00:00:01Z', '[]')")
    conn.execute("INSERT INTO healing_outcomes VALUES ('outcome-1', 'legacy-run', 'm1', 'err', 'model', 'patch-1', 0.9, true, true, '2026-01-01T00:00:00Z', 'v1')")
    conn.close()
    
    # 2. Instantiate and start Surveyor to trigger migration
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    surveyor.start("new-run")
    
    # 3. Verify columns were added and old rows exist with NULL on the new column
    conn = duckdb.connect(str(db_path))
    
    # Check run_records
    rows = conn.execute("SELECT run_id, parent_run_id FROM run_records ORDER BY run_id").fetchall()
    assert len(rows) == 2
    assert rows[0] == ("legacy-run", None)
    assert rows[1] == ("new-run", None)
    
    # Check healing_outcomes
    outcomes = conn.execute("SELECT id, parent_run_id FROM healing_outcomes").fetchall()
    assert outcomes[0] == ("outcome-1", None)
    
    surveyor.stop()
    conn.close()

def test_aggressive_iteration_recording(manifest, tmp_path):
    """Verify aggressive heal records multiple iterations with correct parent_run_id relationship."""
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    
    outer_run_id = "outer-run"
    surveyor.start(outer_run_id)
    
    # Iteration 0: Reuses outer run_id
    result0 = ExecutionResult(
        blueprint_id=manifest.blueprint_id,
        run_id=outer_run_id,
        status="error",
        module_results=(ModuleResult(module_id="m1", status="error", error="Boom"),)
    )
    surveyor.record(result0)
    
    # Iteration 1: Fresh run_id
    iter1_run_id = "iter1-run"
    surveyor.register_iteration(run_id=iter1_run_id, parent_run_id=outer_run_id)
    result1 = ExecutionResult(
        blueprint_id=manifest.blueprint_id,
        run_id=iter1_run_id,
        status="error",
        module_results=(ModuleResult(module_id="m1", status="error", error="Boom"),)
    )
    surveyor.record(result1)
    
    # Verify DB persistence
    conn = duckdb.connect(str(tmp_path / "observability.db"))
    rows = conn.execute("SELECT run_id, parent_run_id FROM run_records ORDER BY run_id").fetchall()
    conn.close()
    
    assert len(rows) == 2
    assert rows[0] == (iter1_run_id, outer_run_id)
    assert rows[1] == (outer_run_id, None)
    
    surveyor.stop()

def test_surveyor_record_on_conflict(manifest, tmp_path):
    """Verify record() uses ON CONFLICT DO UPDATE to update the status/finished_at cleanly."""
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    run_id = "conflict-run"
    surveyor.start(run_id)
    
    # Initial failure
    res_fail = ExecutionResult(blueprint_id="p", run_id=run_id, status="error", module_results=())
    surveyor.record(res_fail)
    
    conn = duckdb.connect(str(tmp_path / "observability.db"))
    assert conn.execute("SELECT status FROM run_records WHERE run_id = ?", [run_id]).fetchone()[0] == "error"
    
    # Record again as patched/success
    res_ok = ExecutionResult(blueprint_id="p", run_id=run_id, status="success", module_results=())
    surveyor.record(res_ok, patched=True)
    
    assert conn.execute("SELECT status FROM run_records WHERE run_id = ?", [run_id]).fetchone()[0] == "patched"
    conn.close()
    
    surveyor.stop()

def test_cross_iteration_join(manifest, tmp_path):
    """Verify that COALESCE(parent_run_id, run_id) returns all iterations of a heal."""
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    outer_run_id = "outer-run"
    surveyor.start(outer_run_id)
    
    # Iteration 0
    res0 = ExecutionResult(blueprint_id="p", run_id=outer_run_id, status="error", module_results=())
    surveyor.record(res0)
    
    # Iteration 1
    surveyor.register_iteration(run_id="inner-1", parent_run_id=outer_run_id)
    res1 = ExecutionResult(blueprint_id="p", run_id="inner-1", status="success", module_results=())
    surveyor.record(res1, patched=True)
    
    conn = duckdb.connect(str(tmp_path / "observability.db"))
    q = "SELECT run_id FROM run_records WHERE COALESCE(parent_run_id, run_id) = ? ORDER BY run_id"
    results = [r[0] for r in conn.execute(q, [outer_run_id]).fetchall()]
    conn.close()
    
    assert results == ["inner-1", "outer-run"]
    surveyor.stop()

def test_heal_attempts_no_duplicate_on_stop_reason(manifest, tmp_path):
    """Verify heal_attempts uses update_heal_attempt_stop_reason to modify the row in-place."""
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    surveyor.start("run-attempts")
    
    # 1. Record an attempt (initially no stop_reason)
    mock_record = MagicMock()
    mock_record.attempt_num = 1
    mock_record.tokens_in = 50
    mock_record.tokens_out = 60
    mock_record.latency_ms = 120
    mock_record.gate_that_rejected = "sandbox"
    mock_record.escalated = False
    mock_record.signature = MagicMock(error_class="ErrClass", hash="h1")
    mock_record.signature.where = "src"
    mock_record.signature.normalized_message = "normalized msg"
    
    surveyor.record_heal_attempt(run_id="run-attempts", attempt_record=mock_record, stop_reason=None)
    
    conn = duckdb.connect(str(tmp_path / "observability.db"))
    rows = conn.execute("SELECT attempt_num, stop_reason FROM heal_attempts").fetchall()
    assert len(rows) == 1
    assert rows[0] == (1, None)
    
    # 2. Update stop reason
    surveyor.update_heal_attempt_stop_reason(run_id="run-attempts", attempt_num=1, stop_reason="solved")
    
    rows_after = conn.execute("SELECT attempt_num, stop_reason FROM heal_attempts").fetchall()
    assert len(rows_after) == 1
    assert rows_after[0] == (1, "solved")
    
    conn.close()
    surveyor.stop()
