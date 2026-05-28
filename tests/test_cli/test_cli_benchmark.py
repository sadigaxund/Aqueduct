import json
import pytest
from pathlib import Path
from click.testing import CliRunner
from unittest.mock import patch, MagicMock

from aqueduct.cli import cli
from aqueduct.surveyor.scenario import ScenarioResult

pytestmark = pytest.mark.integration


def test_benchmark_no_target_exits_1():
    runner = CliRunner()
    result = runner.invoke(cli, ["benchmark"])
    assert result.exit_code == 1
    assert "✗ provide a scenario file or directory" in result.output


@patch("aqueduct.surveyor.scenario.run_benchmark")
def test_benchmark_positional_file(mock_run_benchmark, tmp_path):
    scenario_path = tmp_path / "test.aqscenario.yml"
    scenario_path.write_text("aqueduct_scenario: '1.0'\nid: test_sc\ninject_failure:\n  module: m\n  error_message: 'Simulated'")
    
    mock_run_benchmark.return_value = {
        "test_sc": {
            "claude-sonnet-4-6": ScenarioResult(
                scenario_id="test_sc",
                model="claude-sonnet-4-6",
                passed=True,
                patch_valid=True,
                patch_applies=True,
                patch=None,
                confidence=0.9,
                duration_seconds=1.5,
                attempts_to_parse=1,
                reprompt_errors=[],
                root_cause_match=True,
                category_match=True,
                failures=[],
            )
        }
    }
    
    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text("agent:\n  provider: openai_compat\n  model: claude-sonnet-4-6\n")

    runner = CliRunner()
    result = runner.invoke(cli, ["benchmark", str(scenario_path), "--config", str(config_path)])
    
    assert result.exit_code == 0, f"Failed with: {result.output}"
    # Verify mock was called with the single scenario file path
    mock_run_benchmark.assert_called_once()
    called_path = mock_run_benchmark.call_args[1]["scenarios_dir"]
    assert called_path == Path(scenario_path)


@patch("aqueduct.surveyor.scenario.run_benchmark")
def test_benchmark_positional_dir(mock_run_benchmark, tmp_path):
    scenarios_dir = tmp_path / "scenarios"
    scenarios_dir.mkdir()
    
    mock_run_benchmark.return_value = {}

    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text("agent:\n  provider: openai_compat\n  model: claude-sonnet-4-6\n")

    runner = CliRunner()
    result = runner.invoke(cli, ["benchmark", str(scenarios_dir), "--config", str(config_path)])
    
    assert result.exit_code == 0
    called_path = mock_run_benchmark.call_args[1]["scenarios_dir"]
    assert called_path == Path(scenarios_dir)


@patch("aqueduct.surveyor.scenario.run_benchmark")
def test_benchmark_positional_precedence(mock_run_benchmark, tmp_path):
    scenarios_dir_1 = tmp_path / "scenarios1"
    scenarios_dir_1.mkdir()
    scenarios_dir_2 = tmp_path / "scenarios2"
    scenarios_dir_2.mkdir()
    
    mock_run_benchmark.return_value = {}

    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text("agent:\n  provider: openai_compat\n  model: claude-sonnet-4-6\n")

    runner = CliRunner()
    # scenarios_dir_1 is positional, scenarios_dir_2 is --scenarios
    result = runner.invoke(cli, ["benchmark", str(scenarios_dir_1), "--scenarios", str(scenarios_dir_2), "--config", str(config_path)])
    
    assert result.exit_code == 0
    # positional scenarios_dir_1 must take precedence over --scenarios scenarios_dir_2
    called_path = mock_run_benchmark.call_args[1]["scenarios_dir"]
    assert called_path == Path(scenarios_dir_1)


@patch("aqueduct.surveyor.scenario.run_benchmark")
def test_benchmark_scenarios_option_accepts_file(mock_run_benchmark, tmp_path):
    scenario_path = tmp_path / "test.aqscenario.yml"
    scenario_path.write_text("aqueduct_scenario: '1.0'\nid: test_sc\ninject_failure:\n  module: m\n  error_message: 'Simulated'")
    
    mock_run_benchmark.return_value = {}

    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text("agent:\n  provider: openai_compat\n  model: claude-sonnet-4-6\n")

    runner = CliRunner()
    result = runner.invoke(cli, ["benchmark", "--scenarios", str(scenario_path), "--config", str(config_path)])
    
    assert result.exit_code == 0
    called_path = mock_run_benchmark.call_args[1]["scenarios_dir"]
    assert called_path == Path(scenario_path)


@patch("aqueduct.surveyor.scenario.run_benchmark")
def test_benchmark_overrides_precedence(mock_run_benchmark, tmp_path):
    scenarios_dir = tmp_path / "scenarios"
    scenarios_dir.mkdir()
    
    mock_run_benchmark.return_value = {}

    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text("agent:\n  provider: anthropic\n  model: my-sonnet\n  base_url: http://default-url\n  provider_options:\n    ollama_num_thread: 4\n")

    runner = CliRunner()
    # Pass --provider, --base-url, --model overrides
    result = runner.invoke(cli, [
        "benchmark", str(scenarios_dir),
        "--config", str(config_path),
        "--provider", "openai_compat",
        "--base-url", "http://my-override-url",
        "--model", "my-override-model",
    ])
    
    assert result.exit_code == 0
    # Assert run_benchmark arguments
    called_kwargs = mock_run_benchmark.call_args[1]
    assert called_kwargs["provider"] == "openai_compat"
    assert called_kwargs["base_url"] == "http://my-override-url"
    assert called_kwargs["models"] == ["my-override-model"]
    # provider_options still sourced from cfg.agent (not flag-settable)
    assert called_kwargs["provider_options"] == {"ollama_num_thread": 4}


@patch("aqueduct.surveyor.scenario.run_benchmark")
def test_benchmark_no_overrides_uses_cfg_agent(mock_run_benchmark, tmp_path):
    scenarios_dir = tmp_path / "scenarios"
    scenarios_dir.mkdir()
    
    mock_run_benchmark.return_value = {}

    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text("agent:\n  provider: openai_compat\n  model: cfg-model\n  base_url: http://cfg-url\n  provider_options:\n    ollama_num_thread: 8\n")

    runner = CliRunner()
    result = runner.invoke(cli, [
        "benchmark", str(scenarios_dir),
        "--config", str(config_path),
    ])
    
    assert result.exit_code == 0
    called_kwargs = mock_run_benchmark.call_args[1]
    assert called_kwargs["provider"] == "openai_compat"
    assert called_kwargs["base_url"] == "http://cfg-url"
    assert called_kwargs["models"] == ["cfg-model"]
    assert called_kwargs["provider_options"] == {"ollama_num_thread": 8}


@patch("aqueduct.surveyor.scenario.run_benchmark")
def test_benchmark_timeout_override_precedence(mock_run_benchmark, tmp_path):
    scenarios_dir = tmp_path / "scenarios"
    scenarios_dir.mkdir()
    
    mock_run_benchmark.return_value = {}

    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text("agent:\n  provider: openai_compat\n  model: cfg-model\n  timeout: 300\n")

    runner = CliRunner()
    
    # 1. With flag --timeout 600
    result = runner.invoke(cli, [
        "benchmark", str(scenarios_dir),
        "--config", str(config_path),
        "--timeout", "600",
    ])
    assert result.exit_code == 0
    called_kwargs = mock_run_benchmark.call_args[1]
    assert called_kwargs["timeout"] == 600.0

    # 2. Omitted flag (should use config value of 300)
    result = runner.invoke(cli, [
        "benchmark", str(scenarios_dir),
        "--config", str(config_path),
    ])
    assert result.exit_code == 0
    called_kwargs = mock_run_benchmark.call_args[1]
    assert called_kwargs["timeout"] == 300.0


@patch("aqueduct.surveyor.scenario.run_benchmark")
def test_benchmark_timeout_zero_unbounded(mock_run_benchmark, tmp_path):
    scenarios_dir = tmp_path / "scenarios"
    scenarios_dir.mkdir()
    
    mock_run_benchmark.return_value = {}

    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text("agent:\n  provider: openai_compat\n  model: cfg-model\n")

    runner = CliRunner()
    
    # --timeout 0 maps to None
    result = runner.invoke(cli, [
        "benchmark", str(scenarios_dir),
        "--config", str(config_path),
        "--timeout", "0",
    ])
    assert result.exit_code == 0
    called_kwargs = mock_run_benchmark.call_args[1]
    assert called_kwargs["timeout"] is None


@patch("aqueduct.surveyor.scenario.run_benchmark")
def test_benchmark_output_json_includes_patch(mock_run_benchmark, tmp_path):
    scenario_path = tmp_path / "test.aqscenario.yml"
    scenario_path.write_text("aqueduct_scenario: '1.0'\nid: test_sc\ninject_failure:\n  module: m\n  error_message: 'Simulated'")
    
    from aqueduct.patch.grammar import PatchSpec
    dummy_patch = PatchSpec.model_validate({
        "patch_id": "dummy-fix",
        "rationale": "Mock rationale",
        "operations": [{"op": "replace_module_label", "module_id": "in", "label": "New Label"}],
    })
    
    mock_run_benchmark.return_value = {
        "test_sc": {
            "claude-sonnet-4-6": ScenarioResult(
                scenario_id="test_sc",
                model="claude-sonnet-4-6",
                passed=True,
                patch_valid=True,
                patch_applies=True,
                patch=dummy_patch,
                confidence=0.9,
                duration_seconds=1.5,
                attempts_to_parse=1,
                reprompt_errors=[],
                root_cause_match=True,
                category_match=True,
                failures=[],
                violated_guardrails=["replace_module_config"],
            )
        }
    }
    
    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text("agent:\n  provider: openai_compat\n  model: claude-sonnet-4-6\n")

    runner = CliRunner()
    result = runner.invoke(cli, ["benchmark", str(scenario_path), "--config", str(config_path), "--format", "json"])
    
    assert result.exit_code == 0
    # Benchmark UX overhaul added multiple banner lines on stderr/stdout
    # (``↻ benchmark scenarios=...``, ``[benchmark] N scenarios × M models``,
    # plus per-pair verdict separators). Strip everything before the first
    # ``{`` so the JSON body parses regardless of how many decorative lines
    # the CLI prints around it.
    output_text = result.output
    brace_pos = output_text.find("{")
    assert brace_pos != -1, f"No JSON body in output: {output_text!r}"
    data = json.loads(output_text[brace_pos:].strip())
    model_res = data["test_sc"]["claude-sonnet-4-6"]
    assert "patch" in model_res
    assert model_res["patch"]["patch_id"] == "dummy-fix"
    assert model_res.get("violated_guardrails") == ["replace_module_config"]


@patch("aqueduct.surveyor.scenario.run_benchmark")
def test_benchmark_table_failure_stderr(mock_run_benchmark, tmp_path):
    scenario_path = tmp_path / "test.aqscenario.yml"
    scenario_path.write_text("aqueduct_scenario: '1.0'\nid: test_sc\ninject_failure:\n  module: m\n  error_message: 'Simulated'")
    
    # 1. With failure in table mode -> prints rerun warning to stderr and exits 1
    mock_run_benchmark.return_value = {
        "test_sc": {
            "claude-sonnet-4-6": ScenarioResult(
                scenario_id="test_sc",
                model="claude-sonnet-4-6",
                passed=False,
                patch_valid=True,
                patch_applies=False,
                patch=None,
                confidence=0.5,
                duration_seconds=1.5,
                attempts_to_parse=1,
                reprompt_errors=[],
                root_cause_match=False,
                category_match=False,
                failures=["Assertion failed"],
            )
        }
    }
    
    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text("agent:\n  provider: openai_compat\n  model: claude-sonnet-4-6\n")

    runner = CliRunner()
    result = runner.invoke(cli, ["benchmark", str(scenario_path), "--config", str(config_path)])
    
    assert result.exit_code == 1
    assert "(1 failed — rerun with --format json" in result.stderr

    # 2. With all pass in table mode -> warning NOT printed
    mock_run_benchmark.return_value = {
        "test_sc": {
            "claude-sonnet-4-6": ScenarioResult(
                scenario_id="test_sc",
                model="claude-sonnet-4-6",
                passed=True,
                patch_valid=True,
                patch_applies=True,
                patch=None,
                confidence=0.9,
                duration_seconds=1.5,
                attempts_to_parse=1,
                reprompt_errors=[],
                root_cause_match=True,
                category_match=True,
                failures=[],
            )
        }
    }
    result = runner.invoke(cli, ["benchmark", str(scenario_path), "--config", str(config_path)])
    assert result.exit_code == 0
    assert "rerun with --format json" not in result.stderr

    # 3. With failure in json mode -> warning NOT printed
    mock_run_benchmark.return_value = {
        "test_sc": {
            "claude-sonnet-4-6": ScenarioResult(
                scenario_id="test_sc",
                model="claude-sonnet-4-6",
                passed=False,
                patch_valid=True,
                patch_applies=False,
                patch=None,
                confidence=0.5,
                duration_seconds=1.5,
                attempts_to_parse=1,
                reprompt_errors=[],
                root_cause_match=False,
                category_match=False,
                failures=["Assertion failed"],
            )
        }
    }
    result = runner.invoke(cli, ["benchmark", str(scenario_path), "--config", str(config_path), "--format", "json"])
    assert result.exit_code == 1
    assert "rerun with --format json" not in result.stderr



# ── Phase 33 Part A — persist / regression gate / benchmark-diff ──────────────

def _passing_result(scenario_id="s1", model="m1"):
    """A minimal passing ScenarioResult with Phase 33 fields."""
    return ScenarioResult(
        scenario_id=scenario_id,
        model=model,
        passed=True,
        patch_valid=True,
        patch_applies=True,
        patch=None,
        confidence=0.9,
        duration_seconds=1.0,
        attempts_to_parse=1,
        reprompt_errors=[],
        failures=[],
        soft_failures=[],
        prompt_version="1.0",
        provider="anthropic",
        base_url=None,
    )


@patch("aqueduct.surveyor.scenario.run_benchmark")
def test_benchmark_persist_default_writes_db(mock_run, tmp_path):
    """aqueduct benchmark <dir> writes to <dir>/.aqueduct/benchmark.duckdb and prints persisted line."""
    scenarios_dir = tmp_path / "scenarios"
    scenarios_dir.mkdir()
    mock_run.return_value = {"s1": {"m1": _passing_result()}}

    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text("agent:\n  provider: anthropic\n  model: m1\n")

    runner = CliRunner()
    result = runner.invoke(cli, ["benchmark", str(scenarios_dir), "--config", str(cfg)])

    assert result.exit_code == 0, result.output
    store = scenarios_dir / ".aqueduct" / "benchmark.duckdb"
    assert store.exists()
    assert "persisted 1 benchmark row(s)" in result.output


@patch("aqueduct.surveyor.scenario.run_benchmark")
def test_benchmark_no_persist_skips_db(mock_run, tmp_path):
    """aqueduct benchmark --no-persist <dir> does NOT create benchmark.duckdb."""
    scenarios_dir = tmp_path / "scenarios"
    scenarios_dir.mkdir()
    mock_run.return_value = {"s1": {"m1": _passing_result()}}

    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text("agent:\n  provider: anthropic\n  model: m1\n")

    runner = CliRunner()
    result = runner.invoke(cli, [
        "benchmark", str(scenarios_dir), "--config", str(cfg), "--no-persist",
    ])

    assert result.exit_code == 0
    store = scenarios_dir / ".aqueduct" / "benchmark.duckdb"
    assert not store.exists()


@patch("aqueduct.surveyor.scenario.run_benchmark")
def test_benchmark_store_path_override(mock_run, tmp_path):
    """aqueduct benchmark --store-path <override> writes to the override path."""
    scenarios_dir = tmp_path / "scenarios"
    scenarios_dir.mkdir()
    mock_run.return_value = {"s1": {"m1": _passing_result()}}

    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text("agent:\n  provider: anthropic\n  model: m1\n")
    override = tmp_path / "custom.duckdb"

    runner = CliRunner()
    result = runner.invoke(cli, [
        "benchmark", str(scenarios_dir), "--config", str(cfg),
        "--store-path", str(override),
    ])

    assert result.exit_code == 0, result.output
    assert override.exists()
    assert not (scenarios_dir / ".aqueduct" / "benchmark.duckdb").exists()


@patch("aqueduct.surveyor.scenario.run_benchmark")
def test_benchmark_gate_on_regression_with_regression_exits_1(mock_run, tmp_path):
    """aqueduct benchmark --gate-on-regression with regression → exit 1 + stderr."""
    scenarios_dir = tmp_path / "scenarios"
    scenarios_dir.mkdir()
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text("agent:\n  provider: anthropic\n  model: m1\n")

    runner = CliRunner()

    # First run — writes baseline (passing)
    mock_run.return_value = {"s1": {"m1": _passing_result()}}
    runner.invoke(cli, ["benchmark", str(scenarios_dir), "--config", str(cfg)])

    # Second run — failing result → regression vs baseline
    mock_run.return_value = {
        "s1": {"m1": ScenarioResult(
            scenario_id="s1", model="m1",
            passed=False, patch_valid=True, patch_applies=False,
            patch=None, confidence=0.9, duration_seconds=1.0,
            attempts_to_parse=1, reprompt_errors=[], failures=["regression"],
            soft_failures=[], prompt_version="1.0", provider="anthropic", base_url=None,
        )}
    }
    result = runner.invoke(cli, [
        "benchmark", str(scenarios_dir), "--config", str(cfg), "--gate-on-regression",
    ])

    assert result.exit_code == 1
    assert "regression(s) detected" in result.stderr


@patch("aqueduct.surveyor.scenario.run_benchmark")
def test_benchmark_gate_on_regression_no_regression_exits_0(mock_run, tmp_path):
    """aqueduct benchmark --gate-on-regression with no regression → exit 0."""
    scenarios_dir = tmp_path / "scenarios"
    scenarios_dir.mkdir()
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text("agent:\n  provider: anthropic\n  model: m1\n")

    runner = CliRunner()
    mock_run.return_value = {"s1": {"m1": _passing_result()}}

    # Baseline run
    runner.invoke(cli, ["benchmark", str(scenarios_dir), "--config", str(cfg)])
    # Second run — same outcome, no regression
    result = runner.invoke(cli, [
        "benchmark", str(scenarios_dir), "--config", str(cfg), "--gate-on-regression",
    ])
    assert result.exit_code == 0


@patch("aqueduct.surveyor.scenario.run_benchmark")
def test_benchmark_gate_on_regression_with_no_persist_is_ignored(mock_run, tmp_path):
    """aqueduct benchmark --gate-on-regression --no-persist → stderr note about ignored."""
    scenarios_dir = tmp_path / "scenarios"
    scenarios_dir.mkdir()
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text("agent:\n  provider: anthropic\n  model: m1\n")

    mock_run.return_value = {"s1": {"m1": _passing_result()}}
    runner = CliRunner()
    result = runner.invoke(cli, [
        "benchmark", str(scenarios_dir), "--config", str(cfg),
        "--gate-on-regression", "--no-persist",
    ])

    assert result.exit_code == 0
    assert "--no-persist set" in result.stderr


def test_benchmark_diff_missing_store_exits_1(tmp_path, monkeypatch):
    """aqueduct benchmark-diff with missing store → exit 1 + 'benchmark store not found'."""
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["benchmark-diff"])
    assert result.exit_code == 1
    assert "benchmark store not found" in result.output


@patch("aqueduct.surveyor.scenario.run_benchmark")
def test_benchmark_diff_reads_store_exits_1_on_regression(mock_run, tmp_path):
    """aqueduct benchmark-diff --store-path reads store, exits 1 if regression."""
    scenarios_dir = tmp_path / "scenarios"
    scenarios_dir.mkdir()
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text("agent:\n  provider: anthropic\n  model: m1\n")

    store = tmp_path / "custom.duckdb"
    runner = CliRunner()

    # First run — baseline (passing)
    mock_run.return_value = {"s1": {"m1": _passing_result()}}
    runner.invoke(cli, ["benchmark", str(scenarios_dir), "--config", str(cfg),
                        "--store-path", str(store)])

    # Second run — regression
    mock_run.return_value = {
        "s1": {"m1": ScenarioResult(
            scenario_id="s1", model="m1",
            passed=False, patch_valid=True, patch_applies=False,
            patch=None, confidence=0.9, duration_seconds=1.0,
            attempts_to_parse=1, reprompt_errors=[], failures=["err"],
            soft_failures=[], prompt_version="1.0", provider="anthropic", base_url=None,
        )}
    }
    runner.invoke(cli, ["benchmark", str(scenarios_dir), "--config", str(cfg),
                        "--store-path", str(store)])

    result = runner.invoke(cli, ["benchmark-diff", "--store-path", str(store)])
    assert result.exit_code == 1
    assert "regression(s) detected" in result.stderr


@patch("aqueduct.surveyor.scenario.run_benchmark")
def test_benchmark_diff_scenario_model_filter(mock_run, tmp_path):
    """aqueduct benchmark-diff --scenario sX --model mY filters output to one pair."""
    scenarios_dir = tmp_path / "scenarios"
    scenarios_dir.mkdir()
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text("agent:\n  provider: anthropic\n  model: m1\n")

    store = tmp_path / "filter.duckdb"
    mock_run.return_value = {
        "s1": {"m1": _passing_result("s1", "m1")},
        "s2": {"m1": _passing_result("s2", "m1")},
    }
    runner = CliRunner()
    runner.invoke(cli, ["benchmark", str(scenarios_dir), "--config", str(cfg),
                        "--store-path", str(store)])

    result = runner.invoke(cli, [
        "benchmark-diff", "--store-path", str(store),
        "--scenario", "s1", "--model", "m1",
    ])
    assert result.exit_code == 0
    assert "s1" in result.output
    assert "s2" not in result.output
