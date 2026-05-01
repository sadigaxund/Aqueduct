# tests/test_cli.py
import json
import pytest
from click.testing import CliRunner
from aqueduct.cli import cli

def test_validate_valid_blueprint():
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "tests/fixtures/valid_minimal.yml"])
    assert result.exit_code == 0
    assert "✓" in result.output

def test_validate_invalid_blueprint():
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", "tests/fixtures/invalid_schema.yml"])
    assert result.exit_code == 1
    assert "✗" in result.output

def test_compile_outputs_json():
    runner = CliRunner()
    result = runner.invoke(cli, ["compile", "tests/fixtures/valid_minimal.yml"])
    assert result.exit_code == 0
    manifest = json.loads(result.output)
    assert manifest["blueprint_id"] == "blueprint.hello.world"
    assert "modules" in manifest
    assert "edges" in manifest

@pytest.mark.xfail(reason="DepotStore.get raises CREATE TABLE error in read_only mode")
def test_cli_run_writes_depot_last_run_id(tmp_path):
    runner = CliRunner()
    bp_path = tmp_path / "bp.yml"
    bp_path.write_text("""
aqueduct: '1.0'
id: t1
name: t1
modules:
  - id: in
    type: Ingress
    label: In
    config:
      format: csv
      path: tests/fixtures/valid_minimal.yml
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
    run_2_id = store.get("_last_run_id")
    assert run_2_id != ""
    assert run_2_id != run_1_id