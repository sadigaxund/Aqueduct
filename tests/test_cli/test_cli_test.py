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
    from aqueduct.exit_codes import DATA_OR_RUNTIME
    assert result.exit_code == DATA_OR_RUNTIME
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
    from aqueduct.exit_codes import DATA_OR_RUNTIME
    assert result.exit_code == DATA_OR_RUNTIME
    assert "0 passed" in result.output


def test_cli_test_master_routing_defaults_to_local(test_setup, tmp_path):
    # Create aqueduct.yml with deployment.master_url set to cluster
    cfg_path = tmp_path / "aqueduct.yml"
    cfg_path.write_text("""
aqueduct_config: "1.0"
deployment:
  engine: spark
  target: standalone
  master_url: "spark://h:7077"
  env: cluster
""")

    from aqueduct.executor.spark.test_runner import TestSuiteResult
    
    with patch("aqueduct.executor.spark.session.make_spark_session") as mock_make, \
         patch("aqueduct.executor.spark.test_runner.run_test_file", return_value=TestSuiteResult(total=1, passed=1, failed=0, results=[])):
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            "test", 
            str(test_setup / "pass.aqtest.yml"),
            "--config", str(cfg_path)
        ])
        
        # Check stderr notice was printed because master_url is non-local and no --master was passed
        assert "(test: ignoring deployment.master_url='spark://h:7077'; running on local[*] — pass --master to override)" in result.output
        
        # Check make_spark_session was called with master_url="local[*]"
        mock_make.assert_called_once()
        kwargs = mock_make.call_args[1]
        assert kwargs["master_url"] == "local[*]"


def test_cli_test_master_routing_local_config_no_notice(test_setup, tmp_path):
    # Create aqueduct.yml with local or unset master_url
    cfg_path = tmp_path / "aqueduct.yml"
    cfg_path.write_text("""
aqueduct_config: "1.0"
deployment:
  master_url: "local[*]"
""")

    from aqueduct.executor.spark.test_runner import TestSuiteResult
    
    with patch("aqueduct.executor.spark.session.make_spark_session") as mock_make, \
         patch("aqueduct.executor.spark.test_runner.run_test_file", return_value=TestSuiteResult(total=1, passed=1, failed=0, results=[])):
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            "test", 
            str(test_setup / "pass.aqtest.yml"),
            "--config", str(cfg_path)
        ])
        
        # No notice printed
        assert "ignoring deployment.master_url" not in result.output
        
        # make_spark_session received local[*]
        mock_make.assert_called_once()
        kwargs = mock_make.call_args[1]
        assert kwargs["master_url"] == "local[*]"


def test_cli_test_master_routing_custom_master(test_setup, tmp_path):
    # Check custom master passed verbatim to make_spark_session, no notice emitted
    cfg_path = tmp_path / "aqueduct.yml"
    cfg_path.write_text("""
aqueduct_config: "1.0"
deployment:
  target: local
  master_url: "local[2]"
""")

    from aqueduct.executor.spark.test_runner import TestSuiteResult
    
    with patch("aqueduct.executor.spark.session.make_spark_session") as mock_make, \
         patch("aqueduct.executor.spark.test_runner.run_test_file", return_value=TestSuiteResult(total=1, passed=1, failed=0, results=[])):
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            "test", 
            str(test_setup / "pass.aqtest.yml"),
            "--config", str(cfg_path),
            "--master", "spark://custom-host:7077"
        ])
        
        # No notice printed
        assert "ignoring deployment.master_url" not in result.output
        
        # make_spark_session received the custom master
        mock_make.assert_called_once()
        kwargs = mock_make.call_args[1]
        assert kwargs["master_url"] == "spark://custom-host:7077"


