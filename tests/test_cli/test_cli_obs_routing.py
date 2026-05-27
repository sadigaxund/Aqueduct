import pytest
import os
import shutil
import duckdb
from pathlib import Path
from unittest.mock import MagicMock
from click.testing import CliRunner
from aqueduct.cli import cli, _resolve_obs_db

pytestmark = [pytest.mark.integration]

@pytest.fixture(autouse=True)
def clean_aqueduct_dir():
    # Clean any local .aqueduct directory to avoid cross-test contamination
    local_aq = Path(".aqueduct")
    if local_aq.exists():
        shutil.rmtree(local_aq)
    yield
    if local_aq.exists():
        shutil.rmtree(local_aq)

def test_resolve_obs_db_store_dir(tmp_path):
    """_resolve_obs_db honours store_dir parameter first."""
    mock_cfg = MagicMock()
    mock_cfg.stores.observability.path = ".aqueduct/observability.db"
    
    # Store dir does not have the db file yet -> returns None
    assert _resolve_obs_db(mock_cfg, str(tmp_path), "run-1") is None
    
    # Touch db file -> returns path
    db_file = tmp_path / "observability.db"
    db_file.touch()
    assert _resolve_obs_db(mock_cfg, str(tmp_path), "run-1") == db_file

def test_resolve_obs_db_explicit_path(tmp_path):
    """_resolve_obs_db honours explicit config path."""
    explicit_db = tmp_path / "custom" / "my_obs.db"
    explicit_db.parent.mkdir(parents=True)
    explicit_db.touch()
    
    mock_cfg = MagicMock()
    mock_cfg.stores.observability.path = str(explicit_db)
    
    # Without store_dir, should use explicit path from config
    assert _resolve_obs_db(mock_cfg, None, "run-1") == explicit_db

def test_resolve_obs_db_per_pipeline_glob(tmp_path):
    """_resolve_obs_db globs per-pipeline dirs when using default obs path and run_id is supplied."""
    mock_cfg = MagicMock()
    mock_cfg.stores.observability.path = ".aqueduct/observability.db"
    
    # 1. Create a dummy per-pipeline DB
    pipeline_dir = Path(".aqueduct/observability/my_pipeline")
    pipeline_dir.mkdir(parents=True)
    db_path = pipeline_dir / "observability.db"
    
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE run_records (run_id VARCHAR PRIMARY KEY)")
    conn.execute("INSERT INTO run_records VALUES ('run-abc')")
    conn.close()
    
    # Resolve for run-abc -> should find db_path
    resolved = _resolve_obs_db(mock_cfg, None, "run-abc")
    assert resolved == db_path
    
    # Resolve for non-existent run-xyz -> should fall back/return None (since legacy shared path doesn't exist)
    assert _resolve_obs_db(mock_cfg, None, "run-xyz") is None

def test_runs_command_unions_all_databases(tmp_path):
    """runs command lists runs from all per-pipeline databases in union."""
    # 1. Create two per-pipeline databases
    p1_dir = Path(".aqueduct/observability/pipeline_1")
    p1_dir.mkdir(parents=True)
    db1 = p1_dir / "observability.db"
    conn1 = duckdb.connect(str(db1))
    conn1.execute("CREATE TABLE run_records (run_id VARCHAR PRIMARY KEY, blueprint_id VARCHAR, status VARCHAR, started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, module_results JSON)")
    conn1.execute("INSERT INTO run_records VALUES ('run-1', 'pipeline_1', 'success', '2026-05-26T10:00:00Z', '2026-05-26T10:00:05Z', '[]')")
    conn1.close()
    
    p2_dir = Path(".aqueduct/observability/pipeline_2")
    p2_dir.mkdir(parents=True)
    db2 = p2_dir / "observability.db"
    conn2 = duckdb.connect(str(db2))
    conn2.execute("CREATE TABLE run_records (run_id VARCHAR PRIMARY KEY, blueprint_id VARCHAR, status VARCHAR, started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, module_results JSON)")
    conn2.execute("INSERT INTO run_records VALUES ('run-2', 'pipeline_2', 'error', '2026-05-26T11:00:00Z', '2026-05-26T11:00:05Z', '[]')")
    conn2.close()
    
    # 2. Run 'aqueduct runs' in text mode
    runner = CliRunner()
    result = runner.invoke(cli, ["runs"])
    assert result.exit_code == 0
    assert "run-1" in result.output
    assert "run-2" in result.output
    assert "pipeline_1" in result.output
    assert "pipeline_2" in result.output
    
    # 3. Run 'aqueduct runs' in JSON mode (Phase 30b requirement)
    result_json = runner.invoke(cli, ["runs", "--format", "json"])
    assert result_json.exit_code == 0
    import json
    data = json.loads(result_json.output)
    run_ids = {r["run_id"] for r in data}
    assert run_ids == {"run-1", "run-2"}

def test_heal_command_per_pipeline_routing(tmp_path):
    """aqueduct heal successfully resolves database and finds FailureContext for per-pipeline routed run."""
    from aqueduct.surveyor.surveyor import Surveyor
    from aqueduct.compiler.models import Manifest
    import datetime
    
    # Create the per-pipeline DB with FailureContext using Surveyor
    p1_dir = Path(".aqueduct/observability/pipeline_1")
    p1_dir.mkdir(parents=True, exist_ok=True)
    
    manifest = Manifest(
        blueprint_id="pipeline_1",
        modules=(),
        edges=(),
        context={},
        spark_config={}
    )
    
    surveyor = Surveyor(manifest, store_dir=p1_dir)
    surveyor.start("run-fail-1")
    
    now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()
    db1 = p1_dir / "observability.db"
    conn = duckdb.connect(str(db1))
    conn.execute(
        """
        INSERT INTO failure_contexts
        (run_id, blueprint_id, failed_module, error_message, stack_trace, manifest_json, provenance_json, started_at, finished_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            "run-fail-1",
            "pipeline_1",
            "module_x",
            "Value too large",
            "traceback...",
            '{"id": "pipeline_1", "modules": []}',
            '{}',
            now_str,
            now_str
        ]
    )
    conn.close()
    surveyor.stop()
    
    # Run 'aqueduct heal run-fail-1 --print-prompt'
    # --print-prompt should print prompt and exit 0 without calling LLM
    runner = CliRunner()
    result = runner.invoke(cli, ["heal", "run-fail-1", "--print-prompt"])
    assert result.exit_code == 0
    assert "module_x" in result.output
    assert "Value too large" in result.output
