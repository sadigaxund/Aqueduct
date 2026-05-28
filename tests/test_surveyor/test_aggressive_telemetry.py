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
