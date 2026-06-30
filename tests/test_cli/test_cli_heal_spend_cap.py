import pytest
from click.testing import CliRunner
from aqueduct.cli import cli
from pathlib import Path
import json
import uuid
import duckdb
import datetime as _dt
from aqueduct.surveyor.surveyor import Surveyor

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
    
    with monkeypatch.context() as m:
        m.setattr("aqueduct.cli._agent_usable", lambda *a: True)
        m.setattr(Surveyor, "count_recent_heal_attempts", lambda self, within_minutes=60: 1)
        
        def fail_if_called(*a, **k):
            pytest.fail("generate_agent_patch should NOT have been called due to spend-cap")
        m.setattr("aqueduct.agent.generate_agent_patch", fail_if_called)
        
        config_path = tmp_path / "aqueduct.yml"
        config_path.write_text("danger:\n  allow_multi_patch: true\n", encoding="utf-8")
        
        result = runner.invoke(cli, ["run", str(bp_path), "--config", str(config_path), "--allow-multi-patch"])
    
    assert "Agent rate-limit reached" in result.output
    assert result.exit_code == 2   # DATA_OR_RUNTIME: spend-cap doesn't stage a patch

@pytest.mark.spark
@pytest.mark.integration
def test_heal_spend_cap_skipped_when_none(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    bp_path = tmp_path / "bp.yml"
    bp_path.write_text("""
aqueduct: '1.0'
id: ok_bp
name: OK BP
agent:
  approval: auto
  max_heal_attempts_per_hour: null
  max_patches: 2
modules:
  - id: m1
    type: Ingress
    label: M1
    config: {format: csv, path: /non/existent}
edges: []
""", encoding="utf-8")
    
    patch_called = False
    def mock_patch(*a, **k):
        nonlocal patch_called
        patch_called = True
        from aqueduct.agent import AgentPatchResult
        return AgentPatchResult(patch=None, attempts=1, stop_reason="defer_to_human")

    with monkeypatch.context() as m:
        m.setattr("aqueduct.cli._agent_usable", lambda *a: True)
        m.setattr("aqueduct.agent.generate_agent_patch", mock_patch)
        
        config_path = tmp_path / "aqueduct.yml"
        config_path.write_text("danger:\n  allow_multi_patch: true\n", encoding="utf-8")
        
        result = runner.invoke(cli, ["run", str(bp_path), "--config", str(config_path), "--allow-multi-patch"])
    
    assert patch_called is True
    assert "Agent rate-limit reached" not in result.output

@pytest.mark.spark
@pytest.mark.integration
def test_heal_spend_cap_blueprint_overrides_engine(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    bp_path = tmp_path / "bp.yml"
    # Blueprint has cap=1
    bp_path.write_text("""
aqueduct: '1.0'
id: override_bp
name: Override BP
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
    
    with monkeypatch.context() as m:
        m.setattr("aqueduct.cli._agent_usable", lambda *a: True)
        # 2 rows in DB
        m.setattr(Surveyor, "count_recent_heal_attempts", lambda self, within_minutes=60: 2)
        
        def fail_if_called(*a, **k):
            pytest.fail("generate_agent_patch should NOT have been called due to blueprint spend-cap")
        m.setattr("aqueduct.agent.generate_agent_patch", fail_if_called)
        
        config_path = tmp_path / "aqueduct.yml"
        # Engine has cap=10
        config_path.write_text("""
agent:
  max_heal_attempts_per_hour: 10
danger:
  allow_multi_patch: true
""", encoding="utf-8")
        
        result = runner.invoke(cli, ["run", str(bp_path), "--config", str(config_path), "--allow-multi-patch"])
    
    # If BP wins, 2 >= 1 -> BLOCKS
    assert "Agent rate-limit reached" in result.output
    assert "max_heal_attempts_per_hour=1" in result.output
