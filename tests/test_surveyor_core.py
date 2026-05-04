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
        blueprint_id="test.blueprint",
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
    db_path = store_dir / "obs.db"
    assert db_path.exists()
    
    # Verify 'running' record insertion
    import duckdb
    conn = duckdb.connect(str(db_path))
    res = conn.execute("SELECT status, blueprint_id FROM run_records WHERE run_id = 'run-123'").fetchone()
    assert res == ("running", "test.blueprint")
    
    surveyor.stop()
    conn.close()


def test_surveyor_record_before_start_raises(manifest, tmp_path):
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    result = ExecutionResult(blueprint_id="p", run_id="r", status="success", module_results=())
    
    with pytest.raises(RuntimeError, match="Surveyor.start\(\) must be called before record\(\)"):
        surveyor.record(result)


def test_surveyor_record_success(manifest, tmp_path):
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    run_id = "run-success"
    surveyor.start(run_id)
    
    result = ExecutionResult(
        blueprint_id="p1", 
        run_id=run_id, 
        status="success", 
        module_results=(ModuleResult(module_id="m1", status="success"),)
    )
    
    ctx = surveyor.record(result)
    assert ctx is None
    
    import duckdb
    conn = duckdb.connect(str(tmp_path / "obs.db"))
    status = conn.execute("SELECT status FROM run_records WHERE run_id = ?", [run_id]).fetchone()[0]
    assert status == "success"
    
    surveyor.stop()
    conn.close()


def test_surveyor_record_failure(manifest, tmp_path):
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    run_id = "run-fail"
    surveyor.start(run_id)
    
    result = ExecutionResult(
        blueprint_id="p1", 
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
        conn = duckdb.connect(str(tmp_path / "obs.db"))
        
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
    res_success = ExecutionResult(blueprint_id="p", run_id=run_id, status="success", module_results=())
    with patch("aqueduct.surveyor.surveyor.fire_webhook") as mock_hook:
        surveyor.record(res_success)
        mock_hook.assert_not_called()
    
    # 2. Failure -> Webhook fired
    res_fail = ExecutionResult(blueprint_id="p", run_id=run_id, status="error", module_results=())
    with patch("aqueduct.surveyor.surveyor.fire_webhook") as mock_hook:
        surveyor.record(res_fail)
        mock_hook.assert_called_once()
        args, kwargs = mock_hook.call_args
        assert args[0].url == url
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
    surveyor.record(ExecutionResult(blueprint_id="p", run_id="run-1", status="success", module_results=()))
    surveyor.stop()
    
    # Run 2
    surveyor.start("run-2")
    surveyor.record(ExecutionResult(blueprint_id="p", run_id="run-2", status="success", module_results=()))
    surveyor.stop()
    
    import duckdb
    conn = duckdb.connect(str(tmp_path / "obs.db"))
    count = conn.execute("SELECT COUNT(*) FROM run_records").fetchone()[0]
    assert count == 2
    conn.close()


def test_surveyor_get_probe_signal_no_db(manifest, tmp_path):
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    res = surveyor.get_probe_signal("p1")
    assert res == []


def test_surveyor_get_probe_signal_with_db(manifest, tmp_path):
    store_dir = tmp_path / "store"
    import duckdb
    store_dir.mkdir(parents=True)
    conn = duckdb.connect(str(store_dir / "obs.db"))
    conn.execute("""
        CREATE TABLE probe_signals (
            run_id VARCHAR, probe_id VARCHAR, signal_type VARCHAR, payload JSON, captured_at TIMESTAMPTZ
        )
    """)
    conn.execute("INSERT INTO probe_signals VALUES (?, ?, ?, ?, ?)", ['r1', 'p1', 's1', json.dumps({"a":1}), '2025-01-01T00:00:00Z'])
    conn.execute("INSERT INTO probe_signals VALUES (?, ?, ?, ?, ?)", ['r1', 'p1', 's2', json.dumps({"a":2}), '2025-01-02T00:00:00Z'])
    conn.execute("INSERT INTO probe_signals VALUES (?, ?, ?, ?, ?)", ['r1', 'p2', 's1', json.dumps({"a":3}), '2025-01-03T00:00:00Z'])
    conn.close()
    
    surveyor = Surveyor(manifest, store_dir=store_dir)
    res = surveyor.get_probe_signal("p1")
    assert len(res) == 2
    assert res[0]["signal_type"] == "s2"
    assert res[1]["signal_type"] == "s1"
    assert res[0]["payload"] == {"a": 2}


def test_surveyor_get_probe_signal_filtered(manifest, tmp_path):
    store_dir = tmp_path / "store"
    import duckdb
    store_dir.mkdir(parents=True)
    conn = duckdb.connect(str(store_dir / "obs.db"))
    conn.execute("""
        CREATE TABLE probe_signals (
            run_id VARCHAR, probe_id VARCHAR, signal_type VARCHAR, payload JSON, captured_at TIMESTAMPTZ
        )
    """)
    conn.execute("INSERT INTO probe_signals VALUES (?, ?, ?, ?, ?)", ['r1', 'p1', 's1', json.dumps({"a":1}), '2025-01-01T00:00:00Z'])
    conn.execute("INSERT INTO probe_signals VALUES (?, ?, ?, ?, ?)", ['r1', 'p1', 's2', json.dumps({"a":2}), '2025-01-02T00:00:00Z'])
    conn.close()
    
    surveyor = Surveyor(manifest, store_dir=store_dir)
    res = surveyor.get_probe_signal("p1", signal_type="s1")
    assert len(res) == 1
    assert res[0]["signal_type"] == "s1"


# ── Regulator Evaluation Tests ────────────────────────────────────────────────

def test_surveyor_regulator_no_start(tmp_path):
    from aqueduct.compiler.models import Manifest
    manifest = Manifest(blueprint_id="p", modules=(), edges=(), context={}, spark_config={})
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    assert surveyor.evaluate_regulator("reg1") is True

def test_surveyor_regulator_no_signal_port_edge(tmp_path):
    from aqueduct.compiler.models import Manifest
    manifest = Manifest(blueprint_id="p", modules=(), edges=(), context={}, spark_config={})
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    surveyor.start("run1")
    assert surveyor.evaluate_regulator("reg1") is True

def test_surveyor_regulator_no_signals_db(tmp_path):
    from aqueduct.parser.models import Edge, Module
    from aqueduct.compiler.models import Manifest
    manifest = Manifest(
        blueprint_id="p1",
        modules=(),
        edges=(Edge(from_id="probe1", to_id="reg1", port="signal"),),
        context={}, spark_config={}
    )
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    surveyor.start("run1")
    assert surveyor.evaluate_regulator("reg1") is True

def test_surveyor_regulator_no_rows(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir(parents=True)
    import duckdb
    conn = duckdb.connect(str(store_dir / "obs.db"))
    conn.execute("CREATE TABLE probe_signals (run_id VARCHAR, probe_id VARCHAR, payload JSON, captured_at TIMESTAMPTZ)")
    conn.close()
    
    from aqueduct.parser.models import Edge
    from aqueduct.compiler.models import Manifest
    manifest = Manifest(
        blueprint_id="p1", modules=(),
        edges=(Edge(from_id="probe1", to_id="reg1", port="signal"),),
        context={}, spark_config={}
    )
    surveyor = Surveyor(manifest, store_dir=store_dir)
    surveyor.start("run1")
    assert surveyor.evaluate_regulator("reg1") is True

def test_surveyor_regulator_no_passed_key(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir(parents=True)
    import duckdb, json
    conn = duckdb.connect(str(store_dir / "obs.db"))
    conn.execute("CREATE TABLE probe_signals (run_id VARCHAR, probe_id VARCHAR, payload JSON, captured_at TIMESTAMPTZ)")
    conn.execute("INSERT INTO probe_signals VALUES (?, ?, ?, ?)", ['run1', 'probe1', json.dumps({"other":1}), '2025-01-01T00:00:00Z'])
    conn.close()
    
    from aqueduct.parser.models import Edge
    from aqueduct.compiler.models import Manifest
    manifest = Manifest(
        blueprint_id="p1", modules=(),
        edges=(Edge(from_id="probe1", to_id="reg1", port="signal"),),
        context={}, spark_config={}
    )
    surveyor = Surveyor(manifest, store_dir=store_dir)
    surveyor.start("run1")
    assert surveyor.evaluate_regulator("reg1") is True

def test_surveyor_regulator_passed_none(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir(parents=True)
    import duckdb, json
    conn = duckdb.connect(str(store_dir / "obs.db"))
    conn.execute("CREATE TABLE probe_signals (run_id VARCHAR, probe_id VARCHAR, payload JSON, captured_at TIMESTAMPTZ)")
    conn.execute("INSERT INTO probe_signals VALUES (?, ?, ?, ?)", ['run1', 'probe1', json.dumps({"passed":None}), '2025-01-01T00:00:00Z'])
    conn.close()
    
    from aqueduct.parser.models import Edge
    from aqueduct.compiler.models import Manifest
    manifest = Manifest(
        blueprint_id="p1", modules=(),
        edges=(Edge(from_id="probe1", to_id="reg1", port="signal"),),
        context={}, spark_config={}
    )
    surveyor = Surveyor(manifest, store_dir=store_dir)
    surveyor.start("run1")
    assert surveyor.evaluate_regulator("reg1") is True

def test_surveyor_regulator_passed_false(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir(parents=True)
    import duckdb, json
    conn = duckdb.connect(str(store_dir / "obs.db"))
    conn.execute("CREATE TABLE probe_signals (run_id VARCHAR, probe_id VARCHAR, payload JSON, captured_at TIMESTAMPTZ)")
    conn.execute("INSERT INTO probe_signals VALUES (?, ?, ?, ?)", ['run1', 'probe1', json.dumps({"passed":False}), '2025-01-01T00:00:00Z'])
    conn.close()
    
    from aqueduct.parser.models import Edge
    from aqueduct.compiler.models import Manifest
    manifest = Manifest(
        blueprint_id="p1", modules=(),
        edges=(Edge(from_id="probe1", to_id="reg1", port="signal"),),
        context={}, spark_config={}
    )
    surveyor = Surveyor(manifest, store_dir=store_dir)
    surveyor.start("run1")
    assert surveyor.evaluate_regulator("reg1") is False

def test_surveyor_regulator_passed_true(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir(parents=True)
    import duckdb, json
    conn = duckdb.connect(str(store_dir / "obs.db"))
    conn.execute("CREATE TABLE probe_signals (run_id VARCHAR, probe_id VARCHAR, payload JSON, captured_at TIMESTAMPTZ)")
    conn.execute("INSERT INTO probe_signals VALUES (?, ?, ?, ?)", ['run1', 'probe1', json.dumps({"passed":True}), '2025-01-01T00:00:00Z'])
    conn.close()
    
    from aqueduct.parser.models import Edge
    from aqueduct.compiler.models import Manifest
    manifest = Manifest(
        blueprint_id="p1", modules=(),
        edges=(Edge(from_id="probe1", to_id="reg1", port="signal"),),
        context={}, spark_config={}
    )
    surveyor = Surveyor(manifest, store_dir=store_dir)
    surveyor.start("run1")
    assert surveyor.evaluate_regulator("reg1") is True

def test_surveyor_regulator_uses_newest_row(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir(parents=True)
    import duckdb, json
    conn = duckdb.connect(str(store_dir / "obs.db"))
    conn.execute("CREATE TABLE probe_signals (run_id VARCHAR, probe_id VARCHAR, payload JSON, captured_at TIMESTAMPTZ)")
    conn.execute("INSERT INTO probe_signals VALUES (?, ?, ?, ?)", ['run1', 'probe1', json.dumps({"passed":False}), '2025-01-01T00:00:00Z'])
    conn.execute("INSERT INTO probe_signals VALUES (?, ?, ?, ?)", ['run1', 'probe1', json.dumps({"passed":True}), '2025-01-02T00:00:00Z'])
    conn.close()
    
    from aqueduct.parser.models import Edge
    from aqueduct.compiler.models import Manifest
    manifest = Manifest(
        blueprint_id="p1", modules=(),
        edges=(Edge(from_id="probe1", to_id="reg1", port="signal"),),
        context={}, spark_config={}
    )
    surveyor = Surveyor(manifest, store_dir=store_dir)
    surveyor.start("run1")
    assert surveyor.evaluate_regulator("reg1") is True

def test_surveyor_regulator_duckdb_exception(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir(parents=True)
    (store_dir / "obs.db").write_text("not a db")
    
    from aqueduct.parser.models import Edge
    from aqueduct.compiler.models import Manifest
    manifest = Manifest(
        blueprint_id="p1", modules=(),
        edges=(Edge(from_id="probe1", to_id="reg1", port="signal"),),
        context={}, spark_config={}
    )
    surveyor = Surveyor(manifest, store_dir=store_dir)
    surveyor.start("run1")
    assert surveyor.evaluate_regulator("reg1") is True

