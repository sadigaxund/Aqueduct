"""Tests for 'aqueduct test' CLI command."""

from __future__ import annotations

import pytest
pytestmark = [pytest.mark.spark, pytest.mark.integration]
from pathlib import Path
from click.testing import CliRunner
from aqueduct.cli import cli

from unittest.mock import patch

@pytest.fixture(autouse=True)
def mock_spark_stop():
    with patch("pyspark.sql.SparkSession.stop"):
        yield

@pytest.fixture
def test_setup(tmp_path):
    # Blueprint
    bp_path = tmp_path / "bp.yml"
    bp_path.write_text("""
aqueduct: '1.0'
id: bp1
name: Test Blueprint
modules:
  - id: m1
    type: Channel
    label: L
    config:
      op: sql
      query: "SELECT id * 2 as val FROM in1"
edges: []
""")
    
    # Passing Test
    pass_test = tmp_path / "pass.aqtest.yml"
    pass_test.write_text("""
aqueduct_test: "1.0"
blueprint: bp.yml
tests:
  - id: t_pass
    module: m1
    inputs:
      in1:
        schema: {id: int}
        rows: [[1], [5]]
    assertions:
      - type: row_count
        expected: 2
      - type: contains
        rows: [{val: 2}, {val: 10}]
""")

    # Failing Test
    fail_test = tmp_path / "fail.aqtest.yml"
    fail_test.write_text("""
aqueduct_test: "1.0"
blueprint: bp.yml
tests:
  - id: t_fail
    module: m1
    inputs:
      in1:
        schema: {id: int}
        rows: [[1]]
    assertions:
      - type: row_count
        expected: 5
""")

    return tmp_path

def test_cli_test_pass(test_setup):
    runner = CliRunner()
    result = runner.invoke(cli, ["test", str(test_setup / "pass.aqtest.yml")])
    assert result.exit_code == 0
    assert "1 passed" in result.output
    assert "0 failed" in result.output
    assert "t_pass" in result.output
    assert "✓" in result.output

def test_cli_test_fail(test_setup):
    runner = CliRunner()
    result = runner.invoke(cli, ["test", str(test_setup / "fail.aqtest.yml")])
    assert result.exit_code == 1
    assert "0 passed" in result.output
    assert "1 failed" in result.output
    assert "t_fail" in result.output
    assert "✗" in result.output
    assert "expected 5 rows, got 1" in result.output

def test_cli_test_bad_blueprint(tmp_path):
    # Test file with missing blueprint
    bad_test = tmp_path / "bad.aqtest.yml"
    bad_test.write_text("""
aqueduct_test: "1.0"
blueprint: missing.yml
tests: []
""")
    runner = CliRunner()
    result = runner.invoke(cli, ["test", str(bad_test)])
    assert result.exit_code == 1
    assert "Blueprint not found" in result.output

def test_cli_test_invalid_yaml(tmp_path):
    import yaml
    bad_yaml = tmp_path / "bad.yml"
    bad_yaml.write_text("invalid: [", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(cli, ["test", str(bad_yaml)])
    assert result.exit_code == 1
    # Click might wrap the exception or just show it in output
    # Based on previous failure, result.output was empty and exception was in result
    assert result.exception is not None

def test_cli_test_quiet(test_setup):
    runner = CliRunner()
    result = runner.invoke(cli, ["test", str(test_setup / "pass.aqtest.yml"), "--quiet"])
    assert result.exit_code == 0
    assert "1 passed" in result.output

def test_cli_test_blueprint_override(test_setup, tmp_path):
    bp2 = tmp_path / "bp2.yml"
    bp2.write_text("""
aqueduct: '1.0'
id: bp2
name: BP2
modules:
  - id: m1
    type: Channel
    label: L
    config: {op: sql, query: "SELECT 42 as val"}
edges: []
""")
    runner = CliRunner()
    result = runner.invoke(cli, ["test", str(test_setup / "pass.aqtest.yml"), "--blueprint", str(bp2)])
    assert result.exit_code == 1
    assert "0 passed" in result.output
