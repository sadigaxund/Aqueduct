import os
import shutil
import subprocess
from pathlib import Path
from click.testing import CliRunner
import pytest
pytestmark = pytest.mark.integration
from aqueduct.cli import cli

@pytest.fixture
def empty_cwd(tmp_path, monkeypatch):
    """Run tests in an empty temporary directory."""
    cwd = tmp_path / "my-project"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    return cwd

def test_init_scaffold_success(empty_cwd):
    runner = CliRunner()
    result = runner.invoke(cli, ["init"])
    
    assert result.exit_code == 0
    assert "✓ my-project ready" in result.output
    
    # Check files (actual names used in cli.py)
    assert (empty_cwd / "aqueduct.yml.template").exists()
    assert (empty_cwd / "blueprints" / "blueprint.yml.template").exists()
    assert (empty_cwd / "patches" / "pending").is_dir()
    assert (empty_cwd / "patches" / "rejected").is_dir()
    assert (empty_cwd / "arcades").is_dir()
    assert (empty_cwd / "aqtests").is_dir()
    assert (empty_cwd / "aqtests" / "aqtest.yml.template").exists()
    assert (empty_cwd / "aqscenarios").is_dir()
    assert (empty_cwd / "aqscenarios" / "aqscenario.yml.template").exists()
    
    # Check .gitignore exists and has correct contents
    assert (empty_cwd / ".gitignore").exists()
    gitignore_content = (empty_cwd / ".gitignore").read_text()
    assert "spark-warehouse/" in gitignore_content
    assert "artifacts/" in gitignore_content
    assert "metastore_db/" in gitignore_content
    assert ".aqueduct/" in gitignore_content
    assert ".env" in gitignore_content
    assert "patches/pending/*" in gitignore_content
    assert "!patches/pending/.gitkeep" in gitignore_content


def test_init_existing_files_skipped(empty_cwd):
    # Pre-create template file
    (empty_cwd / "aqueduct.yml.template").write_text("existing")
    
    runner = CliRunner()
    result = runner.invoke(cli, ["init"])
    
    assert result.exit_code == 0
    assert "skip    aqueduct.yml.template  (already exists)" in result.output
    assert (empty_cwd / "aqueduct.yml.template").read_text() == "existing"


def test_init_existing_gitignore_skipped(empty_cwd):
    # Pre-create gitignore
    (empty_cwd / ".gitignore").write_text("user-custom-content")
    
    runner = CliRunner()
    result = runner.invoke(cli, ["init"])
    
    assert result.exit_code == 0
    assert "skip    .gitignore  (already exists)" in result.output
    assert (empty_cwd / ".gitignore").read_text() == "user-custom-content"


def test_init_git_integration(empty_cwd, monkeypatch):
    mock_calls = []
    def mock_run(args, **kwargs):
        mock_calls.append(args)
        if "rev-parse" in args:
            return subprocess.CompletedProcess(args, 1, stdout="", stderr="fatal: not a git repository")
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
    
    monkeypatch.setattr(subprocess, "run", mock_run)
    
    runner = CliRunner()
    result = runner.invoke(cli, ["init"])
    
    assert result.exit_code == 0
    # Should have called git init and git commit
    assert any("init" in args for args in mock_calls)
    assert any("commit" in args for args in mock_calls)


def test_init_git_not_installed(empty_cwd, monkeypatch):
    def mock_run(args, **kwargs):
        if args and args[0] == "git":
            raise FileNotFoundError("[Errno 2] No such file or directory: 'git'")
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
    
    monkeypatch.setattr(subprocess, "run", mock_run)
    
    runner = CliRunner()
    result = runner.invoke(cli, ["init"])
    
    assert result.exit_code == 0
    assert "⚠ git not found" in result.output
    assert "✓ my-project ready" in result.output


def test_init_git_actual(empty_cwd):
    # Real git integration check on empty_cwd
    # Pre-initialize and configure git identity to ensure commit succeeds in headless/CI environments
    subprocess.run(["git", "init"], cwd=empty_cwd)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=empty_cwd)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=empty_cwd)

    runner = CliRunner()
    result = runner.invoke(cli, ["init"])
    
    assert result.exit_code == 0
    
    # Verify git log tracks the initial commit
    git_log = subprocess.run(["git", "log", "--oneline"], capture_output=True, text=True, cwd=empty_cwd)
    assert git_log.returncode == 0
    assert "init: aqueduct project (my-project)" in git_log.stdout
    
    # Verify .gitignore is committed (git ls-files shows it)
    git_files = subprocess.run(["git", "ls-files"], capture_output=True, text=True, cwd=empty_cwd)
    assert git_files.returncode == 0
    files = git_files.stdout.splitlines()
    assert ".gitignore" in files
    assert "aqueduct.yml.template" in files

