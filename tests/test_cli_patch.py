import json
import pytest
import subprocess
from pathlib import Path
from click.testing import CliRunner
from aqueduct.cli import cli

@pytest.fixture
def cli_setup(tmp_path):
    # Setup project structure
    project = tmp_path / "project"
    project.mkdir()
    
    bp_path = project / "blueprint.yml"
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
    
    config_path = project / "aqueduct.yml"
    config_path.write_text(f"""
stores:
  depot: {{path: "{project}/depot.db"}}
  obs: {{path: "{project}/obs.db"}}
""")
    
    patches_dir = project / "patches"
    patches_dir.mkdir()
    (patches_dir / "pending").mkdir()
    (patches_dir / "applied").mkdir()
    (patches_dir / "rejected").mkdir()
    
    sample_patch = {
        "patch_id": "P001",
        "rationale": "Test patch",
        "operations": [
            {
                "op": "set_module_config_key",
                "module_id": "src",
                "key": "path",
                "value": "new_data.csv"
            }
        ]
    }

    patch_file = patches_dir / "pending" / "P001.json"
    patch_file.write_text(json.dumps(sample_patch))
    
    return project, bp_path, patch_file

def test_patch_list_pending(cli_setup):
    project, bp_path, _ = cli_setup
    runner = CliRunner()
    result = runner.invoke(cli, ["patch", "list", "--blueprint", str(bp_path)])
    assert result.exit_code == 0
    assert "P001.json" in result.output
    assert "pending" in result.output

def test_patch_apply_success(cli_setup):
    project, bp_path, patch_file = cli_setup
    runner = CliRunner()
    result = runner.invoke(cli, ["patch", "apply", str(patch_file), "--blueprint", str(bp_path)])
    assert result.exit_code == 0
    assert "✓ patch applied" in result.output
    
    # Verify blueprint updated
    import yaml
    bp_data = yaml.safe_load(bp_path.read_text())
    assert bp_data["modules"][0]["config"]["path"] == "new_data.csv"
    
    # Verify archived
    assert (project / "patches" / "applied" / "P001.json").exists()

def test_patch_reject_success(cli_setup):
    project, bp_path, patch_file = cli_setup
    runner = CliRunner()
    result = runner.invoke(cli, ["patch", "reject", str(patch_file), "--reason", "Too risky"])
    assert result.exit_code == 0
    assert "✓ patch rejected" in result.output
    
    # Verify moved to rejected
    assert (project / "patches" / "rejected" / "P001.json").exists()
    assert not patch_file.exists()
    
    # Verify reason recorded
    data = json.loads((project / "patches" / "rejected" / "P001.json").read_text())
    assert data["rejection_reason"] == "Too risky"

def test_patch_commit_mock_git(cli_setup, monkeypatch):
    project, bp_path, patch_file = cli_setup
    runner = CliRunner()
    
    # Apply first
    runner.invoke(cli, ["patch", "apply", str(patch_file), "--blueprint", str(bp_path)])
    
    # Mock subprocess.run for git
    mock_run_calls = []
    def mock_run(args, **kwargs):
        mock_run_calls.append(args)
        if "log" in args:
            # Return error so _uncommitted_applied_patches treats everything as uncommitted
            return subprocess.CompletedProcess(args, 1, stdout="", stderr="fatal: not a git repository")
        return subprocess.CompletedProcess(args, 0, stdout="mock_output", stderr="")

    
    monkeypatch.setattr(subprocess, "run", mock_run)
    
    result = runner.invoke(cli, ["patch", "commit", "--blueprint", str(bp_path)])
    assert result.exit_code == 0
    assert "✓ committed" in result.output
    
    # Check git calls
    cmds = [" ".join(c) if isinstance(c, list) else c for c in mock_run_calls]
    assert any("git add" in c for c in cmds)
    assert any("git commit" in c for c in cmds)

def test_patch_discard_mock_git(cli_setup, monkeypatch):
    project, bp_path, patch_file = cli_setup
    runner = CliRunner()
    
    # Apply first
    runner.invoke(cli, ["patch", "apply", str(patch_file), "--blueprint", str(bp_path)])
    assert (project / "patches" / "applied" / "P001.json").exists()
    
    # Mock git
    mock_run_calls = []
    def mock_run(args, **kwargs):
        mock_run_calls.append(args)
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
    monkeypatch.setattr(subprocess, "run", mock_run)
    
    result = runner.invoke(cli, ["patch", "discard", "--blueprint", str(bp_path)])
    assert result.exit_code == 0
    assert "✓ blueprint restored" in result.output
    
    # Verify patch moved back to pending
    assert not (project / "patches" / "applied" / "P001.json").exists()
    assert (project / "patches" / "pending" / "P001.json").exists()
    
    # Check git checkout called
    cmds = [" ".join(c) if isinstance(c, list) else c for c in mock_run_calls]
    assert any("git checkout HEAD" in c for c in cmds)
