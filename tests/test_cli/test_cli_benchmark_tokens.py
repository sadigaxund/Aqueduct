"""`aqueduct benchmark --format json` must carry the per-pair token totals.

`ScenarioResult` has `tokens_in_total`/`tokens_out_total`, `run_scenario`
populates both from the agent result, and `benchmark_store` persists them as
their own columns. The `--format json` emitter in `aqueduct/cli/benchmark.py`
hand-builds its per-pair dict and never listed either key, so the one output
users read could not answer "what did this model cost" without opening the
DuckDB store separately. Same omission shape as `stop_reason`, which
`test_cli_benchmark_stop_reason.py` covers.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from aqueduct import exit_codes
from aqueduct.cli import cli
from aqueduct.surveyor.scenario import ScenarioResult

pytestmark = pytest.mark.integration


def _write_inputs(tmp_path):
    scenario_path = tmp_path / "test.aqscenario.yml"
    scenario_path.write_text(
        "aqueduct_scenario: '1.0'\nid: test_sc\n"
        "inject_failure:\n  module: m\n  error_message: 'Simulated'"
    )
    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text("agent:\n  provider: openai_compat\n  model: m1\n")
    return scenario_path, config_path


def _result(**over) -> ScenarioResult:
    base = dict(
        scenario_id="test_sc",
        model="m1",
        passed=False,
        patch_valid=False,
        patch_applies=False,
        patch=None,
        duration_seconds=1.0,
        attempts_to_parse=1,
        reprompt_errors=[],
        failures=["no patch"],
    )
    base.update(over)
    return ScenarioResult(**base)


def _run_json(tmp_path, results) -> dict:
    scenario_path, config_path = _write_inputs(tmp_path)
    with patch("aqueduct.surveyor.scenario.run_benchmark") as mock_run:
        mock_run.return_value = results
        res = CliRunner().invoke(
            cli,
            ["benchmark", str(scenario_path), "--config", str(config_path), "--format", "json"],
        )
    assert res.exit_code in (0, exit_codes.DATA_OR_RUNTIME), (res.exit_code, res.output)
    brace = res.output.find("{")
    assert brace != -1, f"no JSON body in output: {res.output!r}"
    return json.loads(res.output[brace:].strip())


def test_token_totals_are_present_in_the_json_pair(tmp_path):
    data = _run_json(
        tmp_path,
        {"test_sc": {"m1": _result(tokens_in_total=4231, tokens_out_total=907)}},
    )
    pair = data["test_sc"]["m1"]
    assert "tokens_in_total" in pair and "tokens_out_total" in pair, (
        "per-pair JSON dropped the token totals that ScenarioResult and the "
        "benchmark store both carry"
    )
    assert pair["tokens_in_total"] == 4231
    assert pair["tokens_out_total"] == 907


def test_token_totals_are_plain_ints(tmp_path):
    """Emitted as JSON numbers, not strings, so a consumer can sum them."""
    data = _run_json(
        tmp_path,
        {"test_sc": {"m1": _result(tokens_in_total=10, tokens_out_total=20)}},
    )
    pair = data["test_sc"]["m1"]
    assert isinstance(pair["tokens_in_total"], int)
    assert isinstance(pair["tokens_out_total"], int)


def test_a_result_with_no_token_usage_reports_zero(tmp_path):
    """`ScenarioResult` defaults both fields to 0, so an unmeasured pair
    reports 0 rather than null. No invented value either way."""
    pair = _run_json(tmp_path, {"test_sc": {"m1": _result()}})["test_sc"]["m1"]
    assert pair["tokens_in_total"] == 0
    assert pair["tokens_out_total"] == 0
