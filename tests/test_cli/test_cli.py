# tests/test_cli.py
import json
import pytest
pytestmark = [pytest.mark.spark, pytest.mark.integration]
from pathlib import Path
from click.testing import CliRunner
from aqueduct.cli import cli

FIXTURES = Path(__file__).parent.parent / "fixtures"

def test_validate_valid_blueprint():
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", str(FIXTURES / "valid_minimal.yml")])
    assert result.exit_code == 0
    assert "✓" in result.output

def test_validate_invalid_blueprint():
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", str(FIXTURES / "invalid_schema.yml")])
    assert result.exit_code == 1
    assert "✗" in result.output

def test_compile_outputs_json():
    runner = CliRunner()
    result = runner.invoke(cli, ["compile", str(FIXTURES / "valid_minimal.yml")])
    assert result.exit_code == 0
    manifest = json.loads(result.output)
    assert manifest["blueprint_id"] == "blueprint.hello.world"
    assert "modules" in manifest
    assert "edges" in manifest

def test_cli_run_writes_depot_last_run_id(tmp_path):
    runner = CliRunner()
    bp_path = tmp_path / "bp.yml"
    # Use absolute path for input to be robust against chdir
    input_fixture = (FIXTURES / "valid_minimal.yml").resolve()
    bp_path.write_text(f"""
aqueduct: '1.0'
id: t1
name: t1
modules:
  - id: in
    type: Ingress
    label: In
    config:
      format: csv
      path: {input_fixture}
edges: []
""")
    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text(f"""
stores:
  depot:
    path: "{tmp_path}/depot.db"
""")
    
    # Run once
    res1 = runner.invoke(cli, ["run", str(bp_path), "--config", str(config_path)])
    assert res1.exit_code == 0, res1.output
    from aqueduct.depot.depot import DepotStore
    store = DepotStore(tmp_path / "depot.db")
    run_1_id = store.get("_last_run_id")
    assert run_1_id != ""
    store.close()
    
    # Run twice
    res2 = runner.invoke(cli, ["run", str(bp_path), "--config", str(config_path)])
    assert res2.exit_code == 0
    
    store = DepotStore(tmp_path / "depot.db")
    run_2_id = store.get("_last_run_id")
    assert run_2_id != ""
    assert run_2_id != run_1_id
    store.close()

def test_compile_execution_date_parsing():
    runner = CliRunner()
    result = runner.invoke(cli, ["compile", str(FIXTURES / "valid_minimal.yml"), "--execution-date", "2026-01-15"])
    assert result.exit_code == 0
    assert "blueprint_id" in result.output

def test_compile_invalid_execution_date():
    runner = CliRunner()
    result = runner.invoke(cli, ["compile", str(FIXTURES / "valid_minimal.yml"), "--execution-date", "not-a-date"])
    assert result.exit_code != 0
    assert "must be YYYY-MM-DD" in result.output
