import pytest
from pathlib import Path
from click.testing import CliRunner
from aqueduct.cli import cli, _patches_root_from_blueprint

pytestmark = pytest.mark.unit

def test_patches_root_from_blueprint_with_aqueduct_yml(tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    (project / "aqueduct.yml").write_text("")
    
    bp_dir = project / "blueprints" / "subdir"
    bp_dir.mkdir(parents=True)
    bp_path = bp_dir / "test.yml"
    
    root = _patches_root_from_blueprint(bp_path)
    assert root == project / "patches"

def test_patches_root_from_blueprint_no_aqueduct_yml(tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    
    bp_dir = project / "blueprints" / "subdir"
    bp_dir.mkdir(parents=True)
    bp_path = bp_dir / "test.yml"
    
    root = _patches_root_from_blueprint(bp_path)
    assert root == bp_dir / "patches"

def test_patch_list_no_blueprint_uses_cwd_walk_up(tmp_path, monkeypatch):
    project = tmp_path / "project"
    project.mkdir()
    (project / "aqueduct.yml").write_text("")
    
    # Create patch dir in project root
    patches_dir = project / "patches"
    patches_dir.mkdir()
    (patches_dir / "pending").mkdir()
    
    # We are deep in some subdirectory
    deep_dir = project / "some" / "deep" / "dir"
    deep_dir.mkdir(parents=True)
    
    monkeypatch.chdir(deep_dir)
    
    runner = CliRunner()
    result = runner.invoke(cli, ["patch", "list"])
    
    assert result.exit_code == 0
    assert "No pending patches found in" in result.output
    assert str(project / "patches") in result.output

def test_patch_reject_no_patches_dir_uses_cwd_walk_up(tmp_path, monkeypatch):
    project = tmp_path / "project"
    project.mkdir()
    (project / "aqueduct.yml").write_text("")
    
    # Create patch dir in project root
    patches_dir = project / "patches"
    patches_dir.mkdir()
    (patches_dir / "pending").mkdir()
    (patches_dir / "rejected").mkdir()
    
    # Create a pending patch
    (patches_dir / "pending" / "P123.json").write_text("{}")
    
    # We are deep in some subdirectory
    deep_dir = project / "some" / "deep" / "dir"
    deep_dir.mkdir(parents=True)
    
    monkeypatch.chdir(deep_dir)
    
    runner = CliRunner()
    result = runner.invoke(cli, ["patch", "reject", "P123", "--reason", "test reason"])
    
    assert result.exit_code == 0
    assert "✓ patch rejected  id=P123" in result.output
    assert (patches_dir / "rejected" / "P123.json").exists()
