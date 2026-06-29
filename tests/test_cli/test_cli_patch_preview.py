import json
import pytest
from unittest.mock import MagicMock, patch
from click.testing import CliRunner
from pathlib import Path

from aqueduct.cli import cli
from aqueduct.patch.preview import LineageGateResult, SandboxGateResult, LineageWarning

pytestmark = [pytest.mark.spark, pytest.mark.integration]

@pytest.fixture
def setup(tmp_path):
    bp_path = tmp_path / "blueprint.yml"
    bp_path.write_text("""
aqueduct: '1.0'
id: test_bp
name: Test Blueprint
modules:
  - id: src
    type: Ingress
    label: Source
    config: {path: data.csv}
edges: []
""")
    patch_path = tmp_path / "patch.json"
    patch_path.write_text(json.dumps({
        "patch_id": "P001",
        "rationale": "test",
        "confidence": 0.95,
        "operations": [{"op": "replace_module_label", "module_id": "src", "label": "New Label"}]
    }))
    
    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text("""
stores:
  observability: { path: .aqueduct/obs.db }
  depots: {default: { path: .aqueduct/depot.db }}
deployment: { engine: spark, target: local }
""")
    
    return bp_path, patch_path, config_path

@patch("aqueduct.patch.preview.run_lineage_gate")
def test_patch_preview_text_gate2_pass(mock_g2, setup):
    """aqueduct patch preview --blueprint bp.yml patch.json -> exit 0 on Gate 2 pass"""
    bp_path, patch_path, _ = setup
    runner = CliRunner()
    
    mock_g2.return_value = LineageGateResult(
        status="pass", duration_ms=10, touched_modules=["src"], warnings=[]
    )
    
    result = runner.invoke(cli, ["patch", "preview", str(patch_path), "--blueprint", str(bp_path)])
    
    assert result.exit_code == 0
    assert "Patch P001" in result.output
    assert "rationale: test" in result.output
    assert "confidence: 95%" in result.output
    assert "status:          pass" in result.output
    assert "no downstream column-consumption regressions detected" in result.output

@patch("aqueduct.patch.preview.run_lineage_gate")
def test_patch_preview_text_gate2_warn(mock_g2, setup):
    """Gate 2 warnings do not cause non-zero exit"""
    bp_path, patch_path, _ = setup
    runner = CliRunner()
    
    mock_g2.return_value = LineageGateResult(
        status="warn", 
        duration_ms=10, 
        touched_modules=["src"], 
        warnings=[LineageWarning(
            severity="warn",
            channel_id="src",
            consumer_module="down",
            missing_column="col1",
            detail="col1 missing"
        )]
    )
    
    result = runner.invoke(cli, ["patch", "preview", str(patch_path), "--blueprint", str(bp_path)])
    
    assert result.exit_code == 0
    assert "status:          warn" in result.output
    assert "⚠ col1 missing" in result.output

@patch("aqueduct.patch.preview.run_lineage_gate")
@patch("aqueduct.patch.preview.run_sandbox_gate")
def test_patch_preview_sandbox_json(mock_g3, mock_g2, setup):
    """aqueduct patch preview ... --sandbox --format json emits detailed report"""
    bp_path, patch_path, config_path = setup
    runner = CliRunner()
    
    mock_g2.return_value = LineageGateResult(status="pass", duration_ms=10, touched_modules=["src"], warnings=[])
    mock_g3.return_value = SandboxGateResult(
        status="pass", 
        duration_ms=50, 
        sample_rows=100, 
        detail="OK",
        egress_targets=[{"id": "sink1", "format": "csv", "path": "out.csv"}]
    )
    
    result = runner.invoke(cli, [
        "patch", "preview", str(patch_path), 
        "--blueprint", str(bp_path), 
        "--sandbox", "--sample", "100",
        "--format", "json",
        "--config", str(config_path)
    ])
    
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["patch_id"] == "P001"
    assert data["lineage"]["status"] == "pass"
    assert data["sandbox"]["status"] == "pass"
    assert data["sandbox"]["sample_rows"] == 100
    assert data["sandbox"]["egress_targets"][0]["id"] == "sink1"

@patch("aqueduct.patch.preview.run_lineage_gate")
@patch("aqueduct.patch.preview.run_sandbox_gate")
def test_patch_preview_gate3_fail_exit2(mock_g3, mock_g2, setup):
    """Gate 3 fail causes exit code 2"""
    bp_path, patch_path, config_path = setup
    runner = CliRunner()
    
    mock_g2.return_value = LineageGateResult(status="pass", duration_ms=10, touched_modules=[], warnings=[])
    mock_g3.return_value = SandboxGateResult(status="fail", duration_ms=50, sample_rows=0, detail="Runtime error")
    
    result = runner.invoke(cli, [
        "patch", "preview", str(patch_path), 
        "--blueprint", str(bp_path), 
        "--sandbox", 
        "--config", str(config_path)
    ])
    
    assert result.exit_code == 2
    assert "status:      fail" in result.output
    assert "detail:      Runtime error" in result.output

@patch("aqueduct.patch.apply._check_guardrails")
def test_patch_preview_gate1_blocked(mock_guard, setup):
    """Gate 1 blocked exits with code 2 before Gate 2 runs"""
    bp_path, patch_path, _ = setup
    runner = CliRunner()
    
    from aqueduct.patch.apply import PatchError
    mock_guard.side_effect = PatchError("Guardrail violation")
    
    result = runner.invoke(cli, ["patch", "preview", str(patch_path), "--blueprint", str(bp_path)])
    
    assert result.exit_code == 2
    assert "✗ Guardrails gate blocked: Guardrail violation" in result.output

