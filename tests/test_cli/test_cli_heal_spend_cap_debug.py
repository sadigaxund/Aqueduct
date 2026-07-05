import pytest
from click.testing import CliRunner
from aqueduct.cli import cli
from pathlib import Path
import json
import uuid
import duckdb
import datetime as _dt

@pytest.mark.spark
@pytest.mark.integration
def test_heal_spend_cap_blocks_loop(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    bp_path = tmp_path / "bp.yml"
    bp_path.write_text("""
aqueduct: '1.0'
id: fail_bp
name: Fail BP
agent:
  approval: auto
  max_heal_attempts_per_hour: 1
  max_patches: 2
modules:
  - id: m1
    type: Ingress
    label: M1
    config: {format: csv, path: /non/existent}
edges: []
""", encoding="utf-8")
    
    obs_dir = tmp_path / ".aqueduct" / "observability" / "fail_bp"
    obs_dir.mkdir(parents=True)
    obs_db = obs_dir / "observability.db"
    
    conn = duckdb.connect(str(obs_db))
    # Match EXACT schema from surveyor.py
    conn.execute("""
        CREATE TABLE healing_outcomes (
            id VARCHAR PRIMARY KEY,
            run_id VARCHAR NOT NULL,
            failed_module VARCHAR,
            failure_category VARCHAR,
            model VARCHAR,
            patch_id VARCHAR,
            confidence DOUBLE,
            patch_applied BOOLEAN,
            run_success_after_patch BOOLEAN,
            applied_at VARCHAR
        )
    """)
    now_iso = _dt.datetime.now(_dt.timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO healing_outcomes (id, run_id, applied_at) VALUES (?, ?, ?)",
        [str(uuid.uuid4()), "prior-run", now_iso]
    )
    # Double check it's there
    count = conn.execute("SELECT COUNT(*) FROM healing_outcomes").fetchone()[0]
    print(f"DEBUG: Pre-run count in {obs_db} = {count}")
    conn.close()
    
    with monkeypatch.context() as m:
        m.setattr("aqueduct.cli._agent_usable", lambda *a: True)
        def fail_if_called(*a, **k):
            pytest.fail("generate_agent_patch should NOT have been called due to spend-cap")
        m.setattr("aqueduct.agent.generate_agent_patch", fail_if_called)
        
        config_path = tmp_path / "aqueduct.yml"
        config_path.write_text("""
danger:
  allow_multi_patch: true
""", encoding="utf-8")
        
        result = runner.invoke(cli, ["run", str(bp_path), "--config", str(config_path), "--allow-multi-patch"])
    
    print(f"DEBUG: CLI Output:\n{result.output}")
    assert "Agent rate-limit reached" in result.output
    assert "max_heal_attempts_per_hour=1" in result.output
    assert result.exit_code == 2
