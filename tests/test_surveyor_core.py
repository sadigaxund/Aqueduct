"""Integration tests for the Surveyor core class and DuckDB persistence."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from aqueduct.compiler.models import Manifest
from aqueduct.executor.models import ExecutionResult, ModuleResult
from aqueduct.surveyor.surveyor import Surveyor


@pytest.fixture
def manifest():
    return Manifest(
        pipeline_id="test.pipeline",
        modules=(),
        edges=(),
        context={},
        spark_config={}
    )


def test_surveyor_lifecycle_start(manifest, tmp_path):
    store_dir = tmp_path / "store"
    surveyor = Surveyor(manifest, store_dir=store_dir)
    
    run_id = "run-123"
    surveyor.start(run_id)
    
    # Verify directory and DB creation
    db_path = store_dir / "runs.db"
    assert db_path.exists()
    
    # Verify 'running' record insertion
    import duckdb
    conn = duckdb.connect(str(db_path))
    res = conn.execute("SELECT status, pipeline_id FROM run_records WHERE run_id = 'run-123'").fetchone()
    assert res == ("running", "test.pipeline")
    
    surveyor.stop()
    conn.close()


def test_surveyor_record_before_start_raises(manifest, tmp_path):
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    result = ExecutionResult(pipeline_id="p", run_id="r", status="success", module_results=())
    
    with pytest.raises(RuntimeError, match="Surveyor.start\(\) must be called before record\(\)"):
        surveyor.record(result)


def test_surveyor_record_success(manifest, tmp_path):
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    run_id = "run-success"
    surveyor.start(run_id)
    
    result = ExecutionResult(
        pipeline_id="p1", 
        run_id=run_id, 
        status="success", 
        module_results=(ModuleResult(module_id="m1", status="success"),)
    )
    
    ctx = surveyor.record(result)
    assert ctx is None
    
    import duckdb
    conn = duckdb.connect(str(tmp_path / "runs.db"))
    status = conn.execute("SELECT status FROM run_records WHERE run_id = ?", [run_id]).fetchone()[0]
    assert status == "success"
    
    surveyor.stop()
    conn.close()


def test_surveyor_record_failure(manifest, tmp_path):
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    run_id = "run-fail"
    surveyor.start(run_id)
    
    result = ExecutionResult(
        pipeline_id="p1", 
        run_id=run_id, 
        status="error", 
        module_results=(
            ModuleResult(module_id="m1", status="success"),
            ModuleResult(module_id="m2", status="error", error="Boom!"),
        )
    )
    
    # Mock webhook to isolate DB logic
    with patch("aqueduct.surveyor.surveyor.fire_webhook") as mock_hook:
        ctx = surveyor.record(result)
        
        assert ctx is not None
        assert ctx.failed_module == "m2"
        assert ctx.error_message == "Boom!"
        assert ctx.run_id == run_id
        
        # Verify DB persistence
        import duckdb
        conn = duckdb.connect(str(tmp_path / "runs.db"))
        
        # Check run_records
        status = conn.execute("SELECT status FROM run_records WHERE run_id = ?", [run_id]).fetchone()[0]
        assert status == "error"
        
        # Check failure_contexts
        err = conn.execute("SELECT error_message FROM failure_contexts WHERE run_id = ?", [run_id]).fetchone()[0]
        assert err == "Boom!"
        
        surveyor.stop()
        conn.close()


def test_surveyor_orchestration_webhook_firing(manifest, tmp_path):
    url = "http://webhook.test"
    surveyor = Surveyor(manifest, store_dir=tmp_path, webhook_url=url)
    run_id = "run-webhook"
    surveyor.start(run_id)
    
    # 1. Success -> No Webhook
    res_success = ExecutionResult(pipeline_id="p", run_id=run_id, status="success", module_results=())
    with patch("aqueduct.surveyor.surveyor.fire_webhook") as mock_hook:
        surveyor.record(res_success)
        mock_hook.assert_not_called()
    
    # 2. Failure -> Webhook fired
    res_fail = ExecutionResult(pipeline_id="p", run_id=run_id, status="error", module_results=())
    with patch("aqueduct.surveyor.surveyor.fire_webhook") as mock_hook:
        surveyor.record(res_fail)
        mock_hook.assert_called_once()
        args, kwargs = mock_hook.call_args
        assert args[0] == url
        assert args[1]["run_id"] == run_id
    
    surveyor.stop()


def test_surveyor_stop_idempotent(manifest, tmp_path):
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    surveyor.start("r1")
    surveyor.stop()
    surveyor.stop()  # Should not raise


def test_surveyor_multi_run_persistence(manifest, tmp_path):
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    
    # Run 1
    surveyor.start("run-1")
    surveyor.record(ExecutionResult(pipeline_id="p", run_id="run-1", status="success", module_results=()))
    surveyor.stop()
    
    # Run 2
    surveyor.start("run-2")
    surveyor.record(ExecutionResult(pipeline_id="p", run_id="run-2", status="success", module_results=()))
    surveyor.stop()
    
    import duckdb
    conn = duckdb.connect(str(tmp_path / "runs.db"))
    count = conn.execute("SELECT COUNT(*) FROM run_records").fetchone()[0]
    assert count == 2
    conn.close()
