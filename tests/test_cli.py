# tests/test_cli.py
import json
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
    assert manifest["pipeline_id"] == "pipeline.hello.world"
    assert "modules" in manifest
    assert "edges" in manifest