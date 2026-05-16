"""ISSUE-026 regression: CLI commands must not tear down the shared SparkSession.

`aqueduct test` / `aqueduct doctor` call `stop_spark_session()`. Under pytest the
`AQ_TESTING` env guard (set by tests/conftest.py) makes that a no-op, so the
session-scoped `spark` fixture survives a CLI invocation mid-suite.

Deliberately NO `patch("pyspark.sql.SparkSession.stop")` here — the whole point
is to exercise the real guard, not the old test-side workaround.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.spark, pytest.mark.integration]

from click.testing import CliRunner

from aqueduct.cli import cli


@pytest.fixture
def bp_and_test(tmp_path):
    bp = tmp_path / "bp.yml"
    bp.write_text(
        "aqueduct: '1.0'\n"
        "id: bp1\n"
        "name: Test Blueprint\n"
        "modules:\n"
        "  - id: m1\n"
        "    type: Channel\n"
        "    label: L\n"
        "    config:\n"
        "      op: sql\n"
        "      query: \"SELECT id * 2 as val FROM in1\"\n"
        "edges: []\n"
    )
    aqt = tmp_path / "pass.aqtest.yml"
    aqt.write_text(
        'aqueduct_test: "1.0"\n'
        "blueprint: bp.yml\n"
        "tests:\n"
        "  - id: t_pass\n"
        "    module: m1\n"
        "    inputs:\n"
        "      in1:\n"
        "        schema: {id: int}\n"
        "        rows: [[1], [5]]\n"
        "    assertions:\n"
        "      - type: row_count\n"
        "        expected: 2\n"
    )
    return tmp_path, aqt


def test_cli_test_then_doctor_keeps_shared_session(spark, bp_and_test):
    """`aqueduct test` then `aqueduct doctor` must leave the shared spark fixture alive."""
    _tmp, aqt = bp_and_test
    runner = CliRunner()

    # Sanity: shared session healthy before any CLI invocation.
    assert spark.range(1).count() == 1

    test_res = runner.invoke(cli, ["test", str(aqt)])
    assert test_res.exit_code == 0, test_res.output
    # Session-scoped fixture must still be usable — old bug killed the SparkContext.
    assert spark.range(1).count() == 1

    # doctor (no --skip-spark) creates+stops a session via stop_spark_session too.
    doctor_res = runner.invoke(cli, ["doctor"])
    # exit code may be non-zero (no aqueduct.yml in cwd) — irrelevant here.
    assert "Traceback" not in doctor_res.output
    assert spark.range(1).count() == 1
