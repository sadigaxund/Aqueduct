"""`aqueduct benchmark --format json` must carry the terminating axis.

The defect: a heal that OUR OWN budget terminated was reported to the user as
"the model failed to produce a valid patch after 1 attempt". Two real runs of
the same scenario, one 84.1s / 3 attempts / PASS, one 140.6s / 1 attempt / FAIL
— the second was `agent.budget.max_seconds` ending the heal mid-call, and
nothing in the output said so.

The cause was NOT the tracker. `BudgetTracker` sets `budget_seconds_exceeded`
on every seconds-axis path (`check_stop`, `mark_budget_seconds_exceeded`),
`generate_agent_patch` returns it on `AgentPatchResult.stop_reason`,
`run_scenario` copies it onto `ScenarioResult.stop_reason`, and
`benchmark_store` persists it. The `--format json` emitter in
`aqueduct/cli/benchmark.py` hand-builds its per-pair dict and simply never
listed the key — so the one output a user actually reads reported `None` for
every pair, PASS and FAIL alike.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from aqueduct import exit_codes
from aqueduct.agent.budget import StopReason
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
        duration_seconds=140.6,
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
    # A failing pair legitimately exits DATA_OR_RUNTIME; the JSON body is
    # emitted either way and is what this file is about.
    assert res.exit_code in (0, exit_codes.DATA_OR_RUNTIME), (res.exit_code, res.output)
    brace = res.output.find("{")
    assert brace != -1, f"no JSON body in output: {res.output!r}"
    return json.loads(res.output[brace:].strip())


def test_budget_terminated_pair_reports_the_axis(tmp_path):
    data = _run_json(
        tmp_path,
        {"test_sc": {"m1": _result(stop_reason=StopReason.BUDGET_SECONDS_EXCEEDED)}},
    )
    pair = data["test_sc"]["m1"]
    assert "stop_reason" in pair, (
        "per-pair JSON dropped stop_reason — a budget-terminated heal is "
        "indistinguishable from a model failure"
    )
    assert pair["stop_reason"] == "budget_seconds_exceeded"


def test_a_passing_pair_reports_solved_not_null(tmp_path):
    """Proves the key is really populated, not emitted as a constant None."""
    data = _run_json(
        tmp_path,
        {"test_sc": {"m1": _result(passed=True, failures=[], stop_reason=StopReason.SOLVED)}},
    )
    assert data["test_sc"]["m1"]["stop_reason"] == "solved"


def test_stop_reason_is_a_plain_json_string(tmp_path):
    """`StopReason` is a `StrEnum`; the emitted value must survive
    `json.dumps` as a bare string, not as an enum repr."""
    data = _run_json(
        tmp_path,
        {"test_sc": {"m1": _result(stop_reason=StopReason.BUDGET_TOKENS_EXCEEDED)}},
    )
    value = data["test_sc"]["m1"]["stop_reason"]
    assert isinstance(value, str)
    assert value == "budget_tokens_exceeded"


def test_absent_stop_reason_stays_null(tmp_path):
    """No invented default — a result that genuinely carries no stop reason
    emits null rather than a made-up axis."""
    data = _run_json(tmp_path, {"test_sc": {"m1": _result()}})
    assert data["test_sc"]["m1"]["stop_reason"] is None
