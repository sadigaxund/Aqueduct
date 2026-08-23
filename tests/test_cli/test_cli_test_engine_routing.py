"""`aqueduct test` must route through `cfg.deployment.engine`, not silently
substitute Spark (Phase 85 Wave 4 P1).

``tooling.test_runner`` is declared ``unsupported`` for DuckDB
(``aqueduct/executor/duckdb_/capabilities.yml``) — there is no
``duckdb_/test_runner.py`` at all. A DuckDB-deployed project must get a loud
refusal citing that declared hint, never a silent Spark run through Spark's
different SQL/type semantics. No real Spark session is built on this path,
so this stays a pure unit test (no ``pytest.mark.spark`` needed).
"""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from aqueduct.cli import cli
from aqueduct.executor.capabilities import get_capabilities
from aqueduct.exit_codes import CONFIG_ERROR

pytestmark = pytest.mark.unit


@pytest.fixture
def duckdb_test_setup(tmp_path):
    bp_path = tmp_path / "bp.yml"
    bp_path.write_text(
        """
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
"""
    )
    test_file = tmp_path / "pass.aqtest.yml"
    test_file.write_text(
        """
aqueduct_test: "1.0"
blueprint: bp.yml
tests:
  - id: t_pass
    module: m1
    inputs:
      in1:
        schema: {id: int}
        rows: [[1]]
    assertions:
      - type: row_count
        expected: 1
"""
    )
    cfg_path = tmp_path / "aqueduct.yml"
    cfg_path.write_text(
        """
aqueduct_config: "2.0"
deployment:
  engine: duckdb
"""
    )
    return test_file, cfg_path


def test_cli_test_duckdb_refuses_loudly_no_silent_spark_run(duckdb_test_setup, monkeypatch):
    test_file, cfg_path = duckdb_test_setup

    # Poison the Spark session factory so any invocation would be caught by
    # this test failing loudly, instead of the refusal being trivially true
    # only because Spark happens to also fail for unrelated reasons.
    def _boom(*a, **kw):
        raise AssertionError("aqueduct test must not build a Spark session for a duckdb project")

    monkeypatch.setattr("aqueduct.executor.spark.session.make_spark_session", _boom)

    runner = CliRunner()
    result = runner.invoke(cli, ["test", str(test_file), "--config", str(cfg_path)])

    assert result.exit_code == CONFIG_ERROR, result.output
    assert "does not support engine 'duckdb'" in result.output

    # The refusal must surface the DECLARED capability-leaf hint, not
    # invented wording — assert against the real hint text so this test
    # breaks (not silently passes) if the hint's wording ever changes.
    hint = get_capabilities("duckdb").verdict("tooling.test_runner").hint
    assert hint, "tooling.test_runner must declare a hint when unsupported"
    assert hint in result.output


def test_duckdb_declares_test_runner_unsupported():
    """Falsifies the fixture assumption above: if DuckDB ever ships a real
    test runner, this test (and the CLI refusal) must be revisited together
    rather than one silently drifting from the other."""
    leaf = get_capabilities("duckdb").verdict("tooling.test_runner")
    from aqueduct.executor.capabilities import Support

    assert leaf.support == Support.UNSUPPORTED
