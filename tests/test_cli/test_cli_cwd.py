import os
from pathlib import Path
from click.testing import CliRunner
from aqueduct.cli import cli

def test_cwd_restoration_after_run(tmp_path):
    """Verify that 'aqueduct run' restores the original CWD even after success."""
    runner = CliRunner()
    initial_cwd = os.getcwd()
    
    # Create a dummy project
    project_dir = tmp_path / "repro_project"
    project_dir.mkdir()
    (project_dir / "blueprint.yml").write_text("aqueduct: '1.0'\nid: repro\nname: Repro\nmodules: []\nedges: []")
    (project_dir / "aqueduct.yml").write_text("deployment:\n  engine: spark\n  master_url: local[1]\n")
    
    try:
        # Mock executor to avoid Spark overhead
        from unittest.mock import patch
        with patch("aqueduct.executor.get_executor") as mock_get_exec:
            mock_exec = mock_get_exec.return_value
            from aqueduct.executor.models import ExecutionResult
            mock_exec.return_value = ExecutionResult(
                blueprint_id="repro", run_id="r1", status="success", module_results=()
            )
            
            # Invoke the command
            result = runner.invoke(cli, ["run", str(project_dir / "blueprint.yml")])
            assert result.exit_code == 0
            
        final_cwd = os.getcwd()
        assert final_cwd == initial_cwd, f"CWD was not restored! Expected {initial_cwd}, got {final_cwd}"
        
    finally:
        # Safety restoration
        os.chdir(initial_cwd)

def test_cwd_restoration_after_failure(tmp_path):
    """Verify that 'aqueduct run' restores the original CWD even after a parse error."""
    runner = CliRunner()
    initial_cwd = os.getcwd()
    
    # Create a broken project
    project_dir = tmp_path / "broken_project"
    project_dir.mkdir()
    (project_dir / "blueprint.yml").write_text("this is not yaml: : :")
    (project_dir / "aqueduct.yml").write_text("deployment: {engine: spark}")
    
    try:
        # Invoke the command
        result = runner.invoke(cli, ["run", str(project_dir / "blueprint.yml")])
        assert result.exit_code != 0
            
        final_cwd = os.getcwd()
        assert final_cwd == initial_cwd, f"CWD was not restored after failure! Expected {initial_cwd}, got {final_cwd}"
        
    finally:
        os.chdir(initial_cwd)
