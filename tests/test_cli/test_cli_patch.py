import json
import pytest
pytestmark = pytest.mark.integration
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
  depots: {{default: {{path: "{project}/depot.db"}}}}
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

def test_patch_reject_not_found_with_pending_parent(cli_setup):
    project, bp_path, patch_file = cli_setup
    runner = CliRunner()
    
    # Path with pending/ parent but file doesn't exist
    ghost_path = project / "patches" / "pending" / "ghost.json"
    result = runner.invoke(cli, ["patch", "reject", str(ghost_path), "--reason", "Test"])
    
    from aqueduct.exit_codes import DATA_OR_RUNTIME
    assert result.exit_code == DATA_OR_RUNTIME
    assert "✗ reject failed: Patch 'ghost' not found" in result.output

def test_patch_reject_bare_slug(cli_setup):
    project, bp_path, patch_file = cli_setup
    runner = CliRunner()
    
    # Use just the patch ID, relies on --patches-dir or CWD logic
    # Since we don't change CWD, we pass --patches-dir
    patches_dir = project / "patches"
    result = runner.invoke(cli, ["patch", "reject", "P001", "--patches-dir", str(patches_dir), "--reason", "Test slug"])
    
    assert result.exit_code == 0
    assert (project / "patches" / "rejected" / "P001.json").exists()

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

def test_patch_commit_one_patch(cli_setup, monkeypatch):
    project, bp_path, patch_file = cli_setup
    runner = CliRunner()
    
    # Write a second patch file so we have 1 applied
    patch_data = {"patch_id": "P001", "rationale": "Fixing the bug", "operations": [{"op": "set_module_config_key", "module_id": "src", "key": "path", "value": "new.csv"}]}
    patch_file.write_text(json.dumps(patch_data))
    apply_result = runner.invoke(cli, ["patch", "apply", str(patch_file), "--blueprint", str(bp_path)])
    assert apply_result.exit_code == 0, apply_result.output
    
    captured_msg = ""
    def mock_run(args, **kwargs):
        nonlocal captured_msg
        if "log" in args:
            return subprocess.CompletedProcess(args, 1, stdout="", stderr="fatal")
        if "commit" in args and "-m" in args:
            idx = args.index("-m")
            captured_msg = args[idx+1]
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", mock_run)
    result = runner.invoke(cli, ["patch", "commit", "--blueprint", str(bp_path)])
    assert result.exit_code == 0
    assert "Fixing the bug" in captured_msg
    assert "---aqueduct---" in captured_msg
    assert "P001" in captured_msg
    assert "set_module_config_key" in captured_msg

def test_patch_commit_multiple_patches(cli_setup, monkeypatch):
    project, bp_path, patch_file = cli_setup
    runner = CliRunner()
    
    # Apply first - use a valid op on 'src'
    patch_data1 = {"patch_id": "P001", "rationale": "Fix 1", "operations": [{"op": "set_module_config_key", "module_id": "src", "key": "path", "value": "a.csv"}]}
    patch_file.write_text(json.dumps(patch_data1))
    apply_result1 = runner.invoke(cli, ["patch", "apply", str(patch_file), "--blueprint", str(bp_path)])
    assert apply_result1.exit_code == 0, apply_result1.output
    
    # Apply second
    patch_file2 = project / "patches" / "pending" / "P002.json"
    patch_data2 = {"patch_id": "P002", "rationale": "Fix 2", "operations": [{"op": "set_module_config_key", "module_id": "src", "key": "path", "value": "b.csv"}]}
    patch_file2.write_text(json.dumps(patch_data2))
    apply_result2 = runner.invoke(cli, ["patch", "apply", str(patch_file2), "--blueprint", str(bp_path)])
    assert apply_result2.exit_code == 0, apply_result2.output
    
    captured_msg = ""
    def mock_run(args, **kwargs):
        nonlocal captured_msg
        if "log" in args:
            return subprocess.CompletedProcess(args, 1, stdout="", stderr="fatal")
        if "commit" in args and "-m" in args:
            idx = args.index("-m")
            captured_msg = args[idx+1]
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", mock_run)
    result = runner.invoke(cli, ["patch", "commit", "--blueprint", str(bp_path)])
    assert result.exit_code == 0
    assert "2 patches applied" in captured_msg
    assert "P001" in captured_msg
    assert "P002" in captured_msg
    assert "set_module_config_key" in captured_msg
    # Check deduplication
    assert captured_msg.count("set_module_config_key") == 1

def test_patch_commit_not_in_git(cli_setup, monkeypatch):
    project, bp_path, patch_file = cli_setup
    runner = CliRunner()
    runner.invoke(cli, ["patch", "apply", str(patch_file), "--blueprint", str(bp_path)])
    
    def mock_run(args, **kwargs):
        if isinstance(args, list) and len(args) > 1 and args[1] == "add":
            return subprocess.CompletedProcess(args, 128, stdout=b"", stderr=b"fatal: not a git repository")
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
    
    monkeypatch.setattr(subprocess, "run", mock_run)
    result = runner.invoke(cli, ["patch", "commit", "--blueprint", str(bp_path)])
    from aqueduct.exit_codes import DATA_OR_RUNTIME
    assert result.exit_code == DATA_OR_RUNTIME
    assert "fatal: not a git repository" in result.output

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
