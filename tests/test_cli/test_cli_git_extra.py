import pytest
from pathlib import Path
from click.testing import CliRunner
from unittest.mock import MagicMock, patch
import subprocess
import json
from aqueduct.cli import cli

@pytest.fixture
def git_setup(tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    
    bp_path = project / "blueprint.yml"
    bp_path.write_text("aqueduct: '1.0'\nid: test_bp\nname: Test BP\nmodules: []\nedges: []")
    
    patches_dir = project / "patches"
    patches_dir.mkdir()
    applied_dir = patches_dir / "applied"
    applied_dir.mkdir()
    
    # Create an applied patch
    p1 = applied_dir / "P001.json"
    p1.write_text(json.dumps({"applied_at": "2026-05-10T12:00:00Z"}))
    
    return project, bp_path, p1

def test_run_warning_uncommitted_patches(git_setup):
    project, bp_path, p1 = git_setup
    runner = CliRunner()
    
    # Mock subprocess.run to return a commit timestamp older than the patch
    def mock_run(args, **kwargs):
        if "git" in args and "log" in args:
            return MagicMock(returncode=0, stdout="2026-05-10T10:00:00Z\n")
        return MagicMock(returncode=0, stdout="")

    with patch("subprocess.run", side_effect=mock_run):
        # We also need to mock execute to avoid full run
        with patch("aqueduct.executor.get_executor") as mock_get_exec:
            from aqueduct.executor.models import ExecutionResult
            mock_exec = MagicMock()
            mock_get_exec.return_value = mock_exec
            mock_exec.return_value = ExecutionResult(
                blueprint_id="test_bp", run_id="r1", status="success", module_results=()
            )
            
            result = runner.invoke(cli, ["run", str(bp_path)])
            
            assert "applied patch(es) not yet committed to git" in result.output

def test_run_no_warning_when_committed(git_setup):
    project, bp_path, p1 = git_setup
    runner = CliRunner()
    
    # Mock subprocess.run to return a commit timestamp newer than the patch
    def mock_run(args, **kwargs):
        if "git" in args and "log" in args:
            return MagicMock(returncode=0, stdout="2026-05-10T14:00:00Z\n")
        return MagicMock(returncode=0, stdout="")

    with patch("subprocess.run", side_effect=mock_run):
        with patch("aqueduct.executor.get_executor") as mock_get_exec:
            from aqueduct.executor.models import ExecutionResult
            mock_exec = MagicMock()
            mock_get_exec.return_value = mock_exec
            mock_exec.return_value = ExecutionResult(
                blueprint_id="test_bp", run_id="r1", status="success", module_results=()
            )
            
            result = runner.invoke(cli, ["run", str(bp_path)])
            
            assert "applied patch(es) not yet committed to git" not in result.output
