"""Unit tests for aqueduct/surveyor/scenario.py — Phase 22 scenario runner."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.unit

from aqueduct.agent.budget import StopReason
from aqueduct.errors import ScenarioError
from aqueduct.surveyor.scenario import (
    AqScenario,
    ScenarioResult,
    _check_assertions,
    format_benchmark_table,
    load_scenario,
    run_scenario,
)

# ── Helpers ───────────────────────────────────────────────────────────────────

_MINIMAL_BP_YAML = """\
aqueduct: "1.0"
id: test.scenario.bp
name: Test

modules:
  - id: src
    type: Ingress
    label: Source
    config:
      format: parquet
      path: /tmp/in

  - id: sink
    type: Egress
    label: Sink
    config:
      format: parquet
      path: /tmp/out
      mode: overwrite

edges:
  - from: src
    to: sink
"""

_MINIMAL_SCENARIO = """\
aqueduct_scenario: "1.0"
id: test_scenario
description: Minimal test scenario
blueprint: blueprint.yml
inject_failure:
  module: src
  error_message: "AnalysisException: Column 'x' not found"
"""


def _write_scenario(tmp_path: Path, scenario_text: str = _MINIMAL_SCENARIO) -> Path:
    bp = tmp_path / "blueprint.yml"
    bp.write_text(_MINIMAL_BP_YAML)
    sc = tmp_path / "test.aqscenario.yml"
    sc.write_text(scenario_text)
    return sc


def _fake_patch(**kwargs):
    """Minimal PatchSpec-like object for assertion tests."""
    spec = MagicMock()
    spec.operations = []
    spec.confidence = kwargs.get("confidence", 0.9)
    spec.root_cause = kwargs.get("root_cause", "")
    spec.category = kwargs.get("category", "")
    return spec


# ── load_scenario ─────────────────────────────────────────────────────────────


class TestLoadScenario:
    def test_valid_scenario_parsed(self, tmp_path):
        sc = _write_scenario(tmp_path)
        scenario = load_scenario(sc)
        assert scenario.id == "test_scenario"
        assert scenario.inject_failure["module"] == "src"
        assert scenario.source_path == sc.resolve()

    def test_missing_version_raises(self, tmp_path):
        sc = tmp_path / "bad.aqscenario.yml"
        sc.write_text("id: x\ninject_failure: {module: m1}\n")
        with pytest.raises(ScenarioError, match="missing or unsupported aqueduct_scenario version"):
            load_scenario(sc)

    def test_unsupported_version_raises(self, tmp_path):
        sc = tmp_path / "bad.aqscenario.yml"
        sc.write_text("aqueduct_scenario: '99.0'\nid: x\ninject_failure: {module: m1}\n")
        with pytest.raises(ScenarioError, match="missing or unsupported aqueduct_scenario version"):
            load_scenario(sc)

    def test_missing_id_raises(self, tmp_path):
        sc = tmp_path / "bad.aqscenario.yml"
        sc.write_text("aqueduct_scenario: '1.0'\ninject_failure: {module: m1}\n")
        with pytest.raises(ScenarioError, match="missing 'id'"):
            load_scenario(sc)

    def test_missing_inject_failure_raises(self, tmp_path):
        sc = tmp_path / "bad.aqscenario.yml"
        sc.write_text("aqueduct_scenario: '1.0'\nid: x\n")
        with pytest.raises(ScenarioError, match="missing 'inject_failure'"):
            load_scenario(sc)

    def test_version_int_1_accepted(self, tmp_path):
        sc = tmp_path / "ok.aqscenario.yml"
        sc.write_text("aqueduct_scenario: 1\nid: x\ninject_failure: {module: m}\n")
        s = load_scenario(sc)
        assert s.id == "x"

    def test_optional_fields_default(self, tmp_path):
        sc = _write_scenario(tmp_path)
        s = load_scenario(sc)
        # description, blueprint, expected_patch, assertions all have defaults
        assert s.description == "Minimal test scenario"
        assert isinstance(s.assertions, list)
        assert isinstance(s.expected_patch, dict)


# ── _check_assertions ─────────────────────────────────────────────────────────


class TestCheckAssertions:
    def test_patch_is_valid_true_patch_none_fails(self):
        failures, soft_failures, patch_valid, *_ = _check_assertions(
            [{"patch_is_valid": True}], patch=None, blueprint_path=None
        )
        assert not patch_valid
        assert any("patch is None" in f for f in failures)
        assert soft_failures == []

    def test_patch_is_valid_true_patch_present_passes(self):
        failures, soft_failures, patch_valid, *_ = _check_assertions(
            [{"patch_is_valid": True}], patch=_fake_patch(), blueprint_path=None
        )
        assert patch_valid
        assert failures == []
        assert soft_failures == []

    def test_min_confidence_below_threshold_fails(self):
        p = _fake_patch(confidence=0.5)
        failures, soft_failures, *_ = _check_assertions(
            [{"min_confidence": 0.8}], patch=p, blueprint_path=None
        )
        assert failures == []
        assert any("min_confidence" in f for f in soft_failures)

    def test_min_confidence_above_threshold_passes(self):
        p = _fake_patch(confidence=0.95)
        failures, soft_failures, *_ = _check_assertions(
            [{"min_confidence": 0.8}], patch=p, blueprint_path=None
        )
        assert failures == []
        assert soft_failures == []

    def test_max_attempts_exceeded_fails(self):
        p = _fake_patch()
        failures, soft_failures, *_ = _check_assertions(
            [{"max_attempts": 1}], patch=p, blueprint_path=None, attempts=3
        )
        assert failures == []
        assert any("max_attempts" in f for f in soft_failures)

    def test_max_attempts_within_limit_passes(self):
        p = _fake_patch()
        failures, soft_failures, *_ = _check_assertions(
            [{"max_attempts": 3}], patch=p, blueprint_path=None, attempts=2
        )
        assert failures == []
        assert soft_failures == []

    def test_expected_category_match_passes(self):
        p = _fake_patch(category="schema_drift")
        failures, soft_failures, _, _, _, category_match, _, _ = _check_assertions(
            [{"expected_category": "schema_drift"}], patch=p, blueprint_path=None
        )
        assert category_match is True
        assert failures == []
        assert soft_failures == []

    def test_expected_category_mismatch_fails(self):
        p = _fake_patch(category="format_mismatch")
        failures, soft_failures, _, _, _, category_match, _, _ = _check_assertions(
            [{"expected_category": "schema_drift"}], patch=p, blueprint_path=None
        )
        assert category_match is False
        assert failures == []
        assert any("expected_category" in f for f in soft_failures)

    def test_root_cause_contains_match_passes(self):
        p = _fake_patch(root_cause="column 'event_ts' was renamed to 'event_time'")
        failures, soft_failures, _, _, root_cause_match, _, _, _ = _check_assertions(
            [{"root_cause_contains": "event_time"}], patch=p, blueprint_path=None
        )
        assert root_cause_match is True
        assert failures == []
        assert soft_failures == []

    def test_root_cause_contains_no_match_fails(self):
        p = _fake_patch(root_cause="unrelated error")
        failures, soft_failures, _, _, root_cause_match, _, _, _ = _check_assertions(
            [{"root_cause_contains": "event_time"}], patch=p, blueprint_path=None
        )
        assert root_cause_match is False
        assert failures == []
        assert any("root_cause_contains" in f for f in soft_failures)

    def test_patch_applies_true_patch_none_fails(self):
        """patch_applies=true + patch=None → failure."""
        failures, soft_failures, *_ = _check_assertions(
            [{"patch_applies": True}], patch=None, blueprint_path=None
        )
        assert any("cannot check" in f for f in failures)
        assert soft_failures == []

    def test_patch_applies_nonexistent_blueprint_skipped(self, tmp_path):
        """patch_applies=true + blueprint path doesn't exist → warning only, no failure."""
        p = _fake_patch()
        missing = tmp_path / "does_not_exist.yml"
        failures, soft_failures, *_ = _check_assertions(
            [{"patch_applies": True}], patch=p, blueprint_path=missing
        )
        # Skipped silently — no failure added
        assert failures == []
        assert soft_failures == []

    def _defer_patch(self):
        p = _fake_patch()
        op = MagicMock()
        op.op = "defer_to_human"
        p.operations = [op]
        return p

    def test_allow_defer_true_defer_passes(self):
        """allow_defer: true + LLM defers → PASS (gating satisfied)."""
        failures, soft_failures, *_ = _check_assertions(
            [{"patch_is_valid": True, "allow_defer": True}],
            patch=self._defer_patch(),
            blueprint_path=None,
        )
        assert failures == []

    def test_no_allow_defer_defer_fails(self):
        """no allow_defer assertion + LLM defers → FAIL with guidance message."""
        failures, soft_failures, *_ = _check_assertions(
            [{"patch_is_valid": True}],
            patch=self._defer_patch(),
            blueprint_path=None,
        )
        assert any("add allow_defer: true" in f for f in failures)

    def test_allow_defer_true_regular_patch_fails(self):
        """allow_defer: true + LLM produces real patch → FAIL."""
        p = _fake_patch()
        failures, soft_failures, *_ = _check_assertions(
            [{"allow_defer": True}],
            patch=p,
            blueprint_path=None,
        )
        assert any("expected defer_to_human" in f for f in failures)


# ── run_scenario ──────────────────────────────────────────────────────────────


class TestRunScenario:
    def test_bad_blueprint_path_returns_failed_result(self, tmp_path):
        """Scenario with non-existent blueprint path → ScenarioResult(passed=False, failures=[...])."""
        sc = tmp_path / "test.aqscenario.yml"
        sc.write_text(
            "aqueduct_scenario: '1.0'\nid: bad_bp\n"
            "inject_failure:\n  module: m1\n  error_message: boom\n"
            "blueprint: no_such_file.yml\n"
        )
        scenario = load_scenario(sc)
        result = run_scenario(
            scenario,
            model="claude-3",
            patches_dir=tmp_path / "patches",
        )
        assert isinstance(result, ScenarioResult)
        assert result.passed is False
        assert len(result.failures) >= 1
        assert "FailureContext" in result.failures[0] or "not found" in result.failures[0].lower()

    def test_agent_returns_none_patch_invalid(self, tmp_path):
        """run_scenario: Agent returns None → ScenarioResult(passed=False, patch_valid=False)."""
        sc = _write_scenario(tmp_path)
        scenario = load_scenario(sc)

        # Mock generate_agent_patch to return a result with patch=None
        mock_result = MagicMock()
        mock_result.patch = None
        mock_result.attempts = 0
        mock_result.reprompt_errors = []

        with patch("aqueduct.agent.generate_agent_patch", return_value=mock_result):
            result = run_scenario(
                scenario,
                model="claude-3",
                patches_dir=tmp_path / "patches",
            )

        assert result.patch_valid is False
        assert result.passed is False

    def test_run_scenario_soft_split_and_diag_score(self, tmp_path):
        # Create a scenario containing:
        # - patch_is_valid: true (gating)
        # - patch_applies: true (gating)
        # - root_cause_contains: "column" (scoring)
        # - expected_category: "schema_drift" (scoring)
        # - min_confidence: 0.8 (scoring)
        sc_text = """aqueduct_scenario: "1.0"
id: test_soft
description: Test soft split
blueprint: blueprint.yml
inject_failure:
  module: src
  error_message: "boom"
assertions:
  - patch_is_valid: true
  - patch_applies: true
  - root_cause_contains: "column"
  - expected_category: "schema_drift"
  - min_confidence: 0.8
"""
        sc_path = _write_scenario(tmp_path, sc_text)
        scenario = load_scenario(sc_path)

        # Mock generate_agent_patch to return a valid patch but with:
        # - confidence = 0.5 (miss)
        # - category = "other" (miss)
        # - root_cause = "column missing" (hit)
        from aqueduct.patch.grammar import PatchSpec

        patch_obj = PatchSpec(
            patch_id="fix-1",
            rationale="test",
            confidence=0.5,
            category="other",
            root_cause="column missing",
            operations=[{"op": "replace_module_label", "module_id": "src", "label": "New Label"}],
        )

        mock_result = MagicMock()
        mock_result.patch = patch_obj
        mock_result.attempts = 1
        mock_result.reprompt_errors = []

        # We mock _try_apply_patch in scenario.py to succeed so patch_applies passes
        from aqueduct.surveyor.scenario import ApplyOutcome

        with (
            patch("aqueduct.agent.generate_agent_patch", return_value=mock_result),
            patch(
                "aqueduct.surveyor.scenario._try_apply_patch",
                return_value=ApplyOutcome(True, "", None, {}),
            ),
        ):
            result = run_scenario(
                scenario,
                model="claude-3",
                patches_dir=tmp_path / "patches",
            )

        # 1. Check gating vs soft split
        assert result.passed is True  # correct fix passes even with imperfect diagnosis!
        assert len(result.failures) == 0
        assert len(result.soft_failures) == 2  # min_confidence and expected_category missed

        # 2. Check diag_score
        # root_cause_contains is a hit (1/1), expected_category is a miss (0/1)
        # So diag_score = 0.5
        assert result.diag_score == 0.5

    def test_run_scenario_expected_patch_gating(self, tmp_path):
        # Create a scenario containing expected_patch that will fail
        sc_text = """aqueduct_scenario: "1.0"
id: test_gating
description: Test expected patch gating
blueprint: blueprint.yml
inject_failure:
  module: src
  error_message: "boom"
assertions:
  - patch_is_valid: true
  - patch_applies: true
expected_patch:
  effect:
    module: src
    config_contains:
      path: "/expected/path"
"""
        sc_path = _write_scenario(tmp_path, sc_text)
        scenario = load_scenario(sc_path)

        from aqueduct.patch.grammar import PatchSpec

        patch_obj = PatchSpec(
            patch_id="fix-1",
            rationale="test",
            operations=[{"op": "replace_module_label", "module_id": "src", "label": "New Label"}],
        )

        mock_result = MagicMock()
        mock_result.patch = patch_obj
        mock_result.attempts = 1
        mock_result.reprompt_errors = []

        from aqueduct.surveyor.scenario import ApplyOutcome

        with (
            patch("aqueduct.agent.generate_agent_patch", return_value=mock_result),
            patch(
                "aqueduct.surveyor.scenario._try_apply_patch",
                return_value=ApplyOutcome(
                    True,
                    "",
                    None,
                    {"modules": [{"id": "src", "config": {"path": "/wrong/path"}}]},
                ),
            ),
        ):
            result = run_scenario(
                scenario,
                model="claude-3",
                patches_dir=tmp_path / "patches",
            )

        # expected_patch is a hard/gating blocker
        assert result.passed is False
        assert len(result.failures) == 1
        assert "/wrong/path" in result.failures[0]
        assert "/expected/path" in result.failures[0]

    def test_run_scenario_populates_violated_guardrails(self, tmp_path):
        sc_text = """aqueduct_scenario: "1.0"
id: test_guardrails
blueprint: blueprint.yml
inject_failure:
  module: src
  error_message: "boom"
assertions:
  - patch_is_valid: true
  - patch_applies: true
"""
        sc_path = _write_scenario(tmp_path, sc_text)
        scenario = load_scenario(sc_path)

        from aqueduct.patch.grammar import PatchSpec

        patch_obj = PatchSpec(
            patch_id="fix-1",
            rationale="test",
            operations=[{"op": "remove_module", "module_id": "src"}],
        )
        mock_result = MagicMock()
        mock_result.patch = patch_obj
        mock_result.attempts = 1
        mock_result.reprompt_errors = []

        # Return a non-None violated_guardrails from _try_apply_patch
        from aqueduct.surveyor.scenario import ApplyOutcome

        with (
            patch("aqueduct.agent.generate_agent_patch", return_value=mock_result),
            patch(
                "aqueduct.surveyor.scenario._try_apply_patch",
                return_value=ApplyOutcome(False, "violated", ["replace_module_config"], None),
            ),
        ):
            result = run_scenario(scenario, model="claude-3", patches_dir=tmp_path / "patches")

        assert result.violated_guardrails == ["replace_module_config"]


# ── Phase 75 — agentic mode benchmark plumbing ──────────────────────────────


class TestRunScenarioAgenticMode:
    """Minimal benchmark plumbing (design item 6): agent.mode: agentic must be
    threadable through run_scenario/run_benchmark for a live A/B, with a
    ToolBox built from the scenario's own compiled Manifest. No live LLM
    call — generate_agent_patch is mocked."""

    def test_agentic_mode_builds_toolbox_and_completes(self, tmp_path):
        sc = _write_scenario(tmp_path)
        scenario = load_scenario(sc)

        from aqueduct.patch.grammar import PatchSpec

        patch_obj = PatchSpec(
            patch_id="fix-agentic",
            rationale="test",
            root_cause="rc",
            operations=[
                {"op": "set_module_config_key", "module_id": "src", "key": "path", "value": "y.csv"}
            ],
        )
        mock_result = MagicMock()
        mock_result.patch = patch_obj
        mock_result.attempts = 1
        mock_result.reprompt_errors = []

        captured_kwargs = {}

        def _fake_generate_agent_patch(*args, **kwargs):
            captured_kwargs.update(kwargs)
            return mock_result

        with patch("aqueduct.agent.generate_agent_patch", side_effect=_fake_generate_agent_patch):
            result = run_scenario(
                scenario,
                model="claude-3",
                patches_dir=tmp_path / "patches",
                mode="agentic",
                max_tool_calls=5,
            )

        assert isinstance(result, ScenarioResult)
        assert captured_kwargs["mode"] == "agentic"
        assert captured_kwargs["max_tool_calls"] == 5
        # A ToolBox must have been built (not None) for an agentic-mode run.
        from aqueduct.agent.toolbox import ToolBox

        assert isinstance(captured_kwargs["toolbox"], ToolBox)
        # Scenarios never start Spark — session-bound tools must degrade.
        assert captured_kwargs["toolbox"].spark_session is None

    def test_oneshot_mode_default_builds_no_toolbox(self, tmp_path):
        sc = _write_scenario(tmp_path)
        scenario = load_scenario(sc)

        mock_result = MagicMock()
        mock_result.patch = None
        mock_result.attempts = 1
        mock_result.reprompt_errors = []

        captured_kwargs = {}

        def _fake_generate_agent_patch(*args, **kwargs):
            captured_kwargs.update(kwargs)
            return mock_result

        with patch("aqueduct.agent.generate_agent_patch", side_effect=_fake_generate_agent_patch):
            run_scenario(scenario, model="claude-3", patches_dir=tmp_path / "patches")

        assert captured_kwargs["mode"] == "oneshot"
        assert captured_kwargs["toolbox"] is None


# ── format_benchmark_table ────────────────────────────────────────────────────


def _make_result(
    scenario_id: str,
    model: str,
    *,
    passed: bool = True,
    confidence: float | None = 0.9,
    patch_valid: bool = True,
    patch_applies: bool = True,
    violated_guardrails: list[str] | None = None,
) -> ScenarioResult:
    return ScenarioResult(
        scenario_id=scenario_id,
        model=model,
        passed=passed,
        patch_valid=patch_valid,
        patch_applies=patch_applies,
        failures=[] if passed else ["assertion failed"],
        patch=None,
        duration_seconds=1.5,
        confidence=confidence,
        attempts_to_parse=1,
        violated_guardrails=violated_guardrails,
    )


class TestFormatBenchmarkTable:
    def test_single_model_single_scenario_shape(self):
        """Single model × single scenario → table has expected columns and PASS row."""
        results = {
            "scenario_a": {"claude-3": _make_result("scenario_a", "claude-3")},
        }
        table = format_benchmark_table(results, models=["claude-3"])
        assert "claude-3" in table
        assert "scenario_a" in table
        assert "PASS" in table

    def test_failed_scenario_shows_fail(self):
        results = {
            "scenario_a": {"gpt-4": _make_result("scenario_a", "gpt-4", passed=False)},
        }
        table = format_benchmark_table(results, models=["gpt-4"])
        assert "FAIL" in table

    def test_summary_rows_present(self):
        """Parse rate, Apply rate, Pass rate, Avg confidence rows appear."""
        results = {
            "s1": {"m1": _make_result("s1", "m1")},
            "s2": {"m1": _make_result("s2", "m1", passed=False, confidence=None)},
        }
        table = format_benchmark_table(results, models=["m1"])
        assert "Parse rate" in table
        assert "Apply rate" in table
        assert "Pass rate" in table
        assert "Avg confidence" in table

    def test_multiple_models_multiple_scenarios(self):
        """Multi-model table has all model names in header."""
        results = {
            "s1": {
                "claude-3": _make_result("s1", "claude-3"),
                "gpt-4": _make_result("s1", "gpt-4", passed=False),
            },
        }
        table = format_benchmark_table(results, models=["claude-3", "gpt-4"])
        assert "claude-3" in table
        assert "gpt-4" in table

    def test_empty_results_returns_no_results(self):
        table = format_benchmark_table({}, models=["m1"])
        assert table == "(no results)"

    def test_format_benchmark_table_guardrail_clean_reports_dash_when_none(self):
        """Guardrail-clean row reports `—` when every result has violated_guardrails is None."""
        results = {
            "s1": {"m1": _make_result("s1", "m1", violated_guardrails=None)},
            "s2": {"m1": _make_result("s2", "m1", violated_guardrails=None)},
        }
        table = format_benchmark_table(results, models=["m1"])
        guardrail_line = [line for line in table.split("\n") if "Guardrail-clean" in line][0]
        assert "—" in guardrail_line

    def test_format_benchmark_table_guardrail_clean_reports_correct_percentage(self):
        """Reports the correct percentage excluding N/A rows."""
        results = {
            # N/A (no guardrails defined on blueprint)
            "s1": {"m1": _make_result("s1", "m1", violated_guardrails=None)},
            # Defined and clean
            "s2": {"m1": _make_result("s2", "m1", violated_guardrails=[])},
            # Defined and clean
            "s3": {"m1": _make_result("s3", "m1", violated_guardrails=[])},
            # Defined and violated
            "s4": {"m1": _make_result("s4", "m1", violated_guardrails=["replace_module_config"])},
        }
        table = format_benchmark_table(results, models=["m1"])
        guardrail_line = [line for line in table.split("\n") if "Guardrail-clean" in line][0]
        # 2 clean out of 3 defined = 67%
        assert "67%" in guardrail_line

    def test_missing_model_result_shows_dash(self):
        """Model missing for a scenario → shows — placeholder."""
        results = {
            "s1": {"m1": _make_result("s1", "m1")},  # m2 missing
        }
        table = format_benchmark_table(results, models=["m1", "m2"])
        assert "—" in table

    def test_table_displays_diag_score(self):
        """d% appears in cell when diag_score is set, and Diag score summary row is averaged."""
        results = {
            "s1": {
                "m1": ScenarioResult(
                    scenario_id="s1",
                    model="m1",
                    passed=True,
                    patch_valid=True,
                    patch_applies=True,
                    failures=[],
                    patch=None,
                    duration_seconds=1.0,
                    confidence=0.9,
                    attempts_to_parse=1,
                    diag_score=0.5,
                )
            },
            "s2": {
                "m1": ScenarioResult(
                    scenario_id="s2",
                    model="m1",
                    passed=False,
                    patch_valid=True,
                    patch_applies=False,
                    failures=["fail"],
                    patch=None,
                    duration_seconds=1.0,
                    confidence=None,
                    attempts_to_parse=1,
                    diag_score=1.0,
                )
            },
        }
        table = format_benchmark_table(results, models=["m1"])

        # Benchmark table format overhaul (1.1.0): cells use middle-dot
        # separators ``PASS · 0.90 · 50% · 1s`` instead of the old ``d50%``
        # prefix. Diag score appears as a percentage subfield, no ``d`` glyph.
        assert "PASS" in table
        assert "50%" in table
        assert "FAIL" in table
        assert "100%" in table
        # Diag score summary row at the bottom.
        assert "Diag score" in table

        # Check Diag score summary row: (0.5 + 1.0) / 2 = 0.75 -> 75%
        assert "Diag score" in table
        assert "75%" in table

    def test_table_no_diag_score_displays_dash(self):
        """When diag_score is None, d% is omitted and summary row displays —."""
        results = {
            "s1": {
                "m1": ScenarioResult(
                    scenario_id="s1",
                    model="m1",
                    passed=True,
                    patch_valid=True,
                    patch_applies=True,
                    failures=[],
                    patch=None,
                    duration_seconds=1.0,
                    confidence=0.9,
                    attempts_to_parse=1,
                    diag_score=None,
                )
            }
        }
        table = format_benchmark_table(results, models=["m1"])

        # Cell has no d%
        s1_line = [line for line in table.split("\n") if "s1" in line][0]
        # In s1 row, verify the d% indicator is omitted (e.g. no "d" character in cell details)
        assert "d" not in s1_line.split("s1")[1]

        # Summary row has —
        assert "Diag score" in table
        # Find the line containing "Diag score" and assert it ends with "—" or has it
        diag_line = [line for line in table.split("\n") if "Diag score" in line][0]
        assert "—" in diag_line


# ── _try_apply_patch ──────────────────────────────────────────────────────────


class TestTryApplyPatch:
    def _create_bp(self, tmp_path: Path, guardrails: str | None = None) -> Path:
        bp_path = tmp_path / "blueprint.yml"
        content = _MINIMAL_BP_YAML
        if guardrails:
            agent_block = f"\nagent:\n  guardrails:\n    {guardrails}\n"
            content += agent_block
        bp_path.write_text(content)
        return bp_path

    def _make_patch(self, op: str, **kwargs):
        from aqueduct.patch.grammar import PatchSpec

        return PatchSpec(
            patch_id="test", rationale="test", operations=[{"op": op, "module_id": "src", **kwargs}]
        )

    def test_no_guardrails_block_returns_none(self, tmp_path):
        from aqueduct.surveyor.scenario import _try_apply_patch

        bp_path = self._create_bp(tmp_path)
        patch = self._make_patch("set_module_config_key", key="path", value="/new")

        outcome = _try_apply_patch(patch, bp_path)
        success, violated, patched_dict = (
            outcome.applied,
            outcome.violated_guardrails,
            outcome.patched_dict,
        )
        assert success is True
        assert violated is None
        assert patched_dict is not None
        assert patched_dict["modules"][0]["config"]["path"] == "/new"

    def test_defined_and_clean_returns_empty_list(self, tmp_path):
        from aqueduct.surveyor.scenario import _try_apply_patch

        bp_path = self._create_bp(tmp_path, "forbidden_ops: [replace_module_config]")
        patch = self._make_patch("set_module_config_key", key="path", value="/new")

        outcome = _try_apply_patch(patch, bp_path)
        success, violated, patched_dict = (
            outcome.applied,
            outcome.violated_guardrails,
            outcome.patched_dict,
        )
        assert success is True
        assert violated == []
        assert patched_dict is not None

    def test_forbidden_ops_violation(self, tmp_path):
        from aqueduct.surveyor.scenario import _try_apply_patch

        bp_path = self._create_bp(tmp_path, "forbidden_ops: [replace_module_config]")
        patch = self._make_patch("replace_module_config", config={"path": "/new"})

        outcome = _try_apply_patch(patch, bp_path)
        success, err, violated, patched_dict = (
            outcome.applied,
            outcome.error,
            outcome.violated_guardrails,
            outcome.patched_dict,
        )
        assert success is False
        assert "guardrails violated" in err
        assert "replace_module_config" in err
        assert isinstance(violated, list)
        assert len(violated) == 1
        assert "replace_module_config" in violated[0]
        assert patched_dict is None

    def test_allowed_paths_violation(self, tmp_path):
        from aqueduct.surveyor.scenario import _try_apply_patch

        bp_path = self._create_bp(tmp_path, "allowed_paths: [blueprints/orders.yml]")
        patch = self._make_patch("set_module_config_key", key="path", value="data/other.csv")

        outcome = _try_apply_patch(patch, bp_path)
        success, err, violated, patched_dict = (
            outcome.applied,
            outcome.error,
            outcome.violated_guardrails,
            outcome.patched_dict,
        )
        assert success is False
        assert "guardrails violated" in err
        assert "blueprints/orders.yml" in err
        assert isinstance(violated, list)
        assert len(violated) == 1
        assert patched_dict is None

    def test_parse_compile_failure_returns_none_patched_dict(self, tmp_path):
        from aqueduct.surveyor.scenario import _try_apply_patch
        from aqueduct.patch.grammar import PatchSpec

        bp_path = self._create_bp(tmp_path)
        # Apply a patch that breaks the blueprint (invalid type for format fails parsing)
        patch = PatchSpec(
            patch_id="test",
            rationale="test",
            operations=[
                {
                    "op": "set_module_config_key",
                    "module_id": "src",
                    "key": "format",
                    "value": ["a list"],
                }
            ],
        )

        outcome = _try_apply_patch(patch, bp_path)
        success, err, violated, patched_dict = (
            outcome.applied,
            outcome.error,
            outcome.violated_guardrails,
            outcome.patched_dict,
        )
        assert success is False
        assert (
            "ParseError" in err
            or "validation" in err.lower()
            or "unhashable" in err.lower()
            or "list" in err.lower()
        )
        assert violated is None
        assert patched_dict is None


# ── _normalize_sql ────────────────────────────────────────────────────────────


class TestNormalizeSql:
    def test_collapses_whitespace(self):
        from aqueduct.surveyor.scenario import _normalize_sql

        result = _normalize_sql("SELECT  a , b  FROM  t")
        assert "a" in result and "b" in result and "t" in result
        assert "  " not in result

    def test_equivalent_queries_produce_same_canonical_form(self):
        from aqueduct.surveyor.scenario import _normalize_sql

        r1 = _normalize_sql("SELECT a, b FROM t")
        r2 = _normalize_sql("select   a,b from   t")
        assert r1.lower() == r2.lower()

    def test_event_time_substring_found_after_normalization(self):
        from aqueduct.surveyor.scenario import _normalize_sql

        full = "SELECT CAST(event_ts AS timestamp) AS event_time FROM t"
        assert "event_time" in _normalize_sql(full).lower()

    def test_fallback_on_malformed_sql_does_not_crash(self):
        from aqueduct.surveyor.scenario import _normalize_sql

        result = _normalize_sql("@@@INVALID!!!SQL###")
        assert isinstance(result, str)

    def test_fallback_is_lowercase_and_collapsed(self):
        """When sqlglot.parse_one raises, result equals ' '.join(text.lower().split())."""
        from unittest.mock import patch as mock_patch
        from aqueduct.surveyor.scenario import _normalize_sql

        text = "NOT SQL  multiple   spaces"
        with mock_patch("sqlglot.parse_one", side_effect=Exception("parse error")):
            result = _normalize_sql(text)
        assert result == " ".join(text.lower().split())


# ── _check_expected_effect ────────────────────────────────────────────────────

_SAMPLE_PATCHED_DICT = {
    "modules": [
        {
            "id": "clean_events",
            "config": {
                "query": "SELECT event_time FROM events_raw",
                "path": "data/events.csv",
                "header": True,
                "max_rows": 1000,
            },
        }
    ]
}


class TestCheckExpectedEffect:
    def _call(self, expected, patched_dict=_SAMPLE_PATCHED_DICT):
        from aqueduct.surveyor.scenario import _check_expected_effect

        return _check_expected_effect(expected, patched_dict)

    def test_empty_expected_returns_no_failures(self):
        assert self._call({}) == []

    def test_missing_module_key_returns_failure(self):
        # effect dict must be non-empty (truthy) to reach the module check;
        # an empty effect {} is falsy and falls into the legacy-ops branch.
        failures = self._call({"effect": {"config_contains": {"query": "x"}}})
        assert len(failures) == 1
        assert "module" in failures[0] and "required" in failures[0]

    def test_nonexistent_module_returns_failure(self):
        failures = self._call({"effect": {"module": "ghost_module"}})
        assert len(failures) == 1
        assert "ghost_module" in failures[0]
        assert "not found" in failures[0]

    def test_sql_key_substring_present_no_failures(self):
        failures = self._call(
            {
                "effect": {
                    "module": "clean_events",
                    "config_contains": {"query": "event_time"},
                }
            }
        )
        assert failures == []

    def test_sql_key_substring_absent_reports_failure(self):
        failures = self._call(
            {
                "effect": {
                    "module": "clean_events",
                    "config_contains": {"query": "nonexistent_column"},
                }
            }
        )
        assert len(failures) == 1
        assert "nonexistent_column" in failures[0]
        assert "AST-normalized" in failures[0] or "normalized" in failures[0].lower()

    def test_non_sql_key_substring_present_no_failures(self):
        failures = self._call(
            {
                "effect": {
                    "module": "clean_events",
                    "config_contains": {"path": "events"},
                }
            }
        )
        assert failures == []

    def test_non_sql_key_substring_absent_reports_failure(self):
        failures = self._call(
            {
                "effect": {
                    "module": "clean_events",
                    "config_contains": {"path": "nonexistent/path"},
                }
            }
        )
        assert len(failures) == 1
        assert "nonexistent/path" in failures[0]
        assert "AST-normalized" not in failures[0]

    def test_bool_true_strict_equality_pass(self):
        failures = self._call(
            {"effect": {"module": "clean_events", "config_contains": {"header": True}}}
        )
        assert failures == []

    def test_int_strict_equality_pass(self):
        failures = self._call(
            {"effect": {"module": "clean_events", "config_contains": {"max_rows": 1000}}}
        )
        assert failures == []

    def test_bool_wrong_value_fails(self):
        failures = self._call(
            {"effect": {"module": "clean_events", "config_contains": {"header": False}}}
        )
        assert len(failures) == 1
        assert "header" in failures[0]

    def test_int_expected_does_not_match_superstring_actual(self):
        """Audit-fixed 2026-08: `isinstance(x, (bool, int, float)) and
        isinstance(x, bool) is not False` reduces to `isinstance(x, bool)`
        — a genuine int/float expected_val never took the strict-equality
        branch at all and silently fell through to the substring path,
        where config_contains: {retries: 1} PASSED against an actual of
        11 (str(1) is a substring of str(11)). Must now fail."""
        patched = {"modules": [{"id": "m", "config": {"retries": 11}}]}
        failures = self._call(
            {"effect": {"module": "m", "config_contains": {"retries": 1}}},
            patched_dict=patched,
        )
        assert len(failures) == 1
        assert "retries" in failures[0]

    def test_bool_expected_does_not_match_numeric_actual(self):
        """Python's `1 == True` / `0 == False` must not let a numeric
        actual satisfy a boolean expectation — a config field that should
        be `true`/`false` but ended up `1`/`0` (e.g. through a lossy
        round-trip) is a real, distinguishable defect, not a pass."""
        patched = {"modules": [{"id": "m", "config": {"enabled": 1}}]}
        failures = self._call(
            {"effect": {"module": "m", "config_contains": {"enabled": True}}},
            patched_dict=patched,
        )
        assert len(failures) == 1
        assert "enabled" in failures[0]

    def test_numeric_expected_does_not_match_bool_actual(self):
        """The reverse direction: a numeric expectation must not accept a
        bool actual just because `True == 1` in Python."""
        patched = {"modules": [{"id": "m", "config": {"max_rows": True}}]}
        failures = self._call(
            {"effect": {"module": "m", "config_contains": {"max_rows": 1}}},
            patched_dict=patched,
        )
        assert len(failures) == 1
        assert "max_rows" in failures[0]

    def test_float_expected_strict_equality_pass(self):
        patched = {"modules": [{"id": "m", "config": {"threshold": 0.5}}]}
        failures = self._call(
            {"effect": {"module": "m", "config_contains": {"threshold": 0.5}}},
            patched_dict=patched,
        )
        assert failures == []

    def test_patched_dict_none_fails_the_effect_block(self):
        """An effect that could not be graded must FAIL, not pass.

        This asserted ``failures == []`` until 2026-08-15, i.e. it pinned the
        bug: a scenario stating an ``effect`` whose patch Gate 1 refused was
        scored PASS, because the grader returned "no failures" for "never
        graded". Every shipped gallery scenario with an effect also asserts
        ``patch_applies: true``, which failed first — so the hole was latent,
        and exactly the kind that stops being latent the day someone writes a
        scenario without that assertion.
        """
        from aqueduct.surveyor.scenario import _check_expected_effect

        failures = _check_expected_effect(
            {"effect": {"module": "clean_events", "config_contains": {"query": "event_time"}}},
            None,
            apply_error="engine-config policy refused the write: denied key",
        )
        assert len(failures) == 1
        assert "never applied" in failures[0]
        # The cause is NAMED, not just "grading skipped" — the whole point is
        # that a reader learns why without cross-referencing another failure.
        assert "denied key" in failures[0]

    def test_ungradeable_effect_reports_one_line_not_a_cascade(self):
        """Positive control on the shape of the failure above.

        The reason the None branch skipped grading in the first place was to
        avoid burying a refusal under per-key noise. That concern is still
        valid, so the failure must stay a SINGLE line even for an effect with
        many sub-expectations — otherwise this fix trades a silent pass for
        the noise it was avoiding.
        """
        from aqueduct.surveyor.scenario import _check_expected_effect

        failures = _check_expected_effect(
            {
                "effect": {
                    "module": "clean_events",
                    "config_contains": {"query": "event_time", "format": "parquet"},
                    "engine_config_changed": {"spark": ["spark.sql.shuffle.partitions"]},
                }
            },
            None,
        )
        assert len(failures) == 1

    def test_effect_absent_still_passes_when_the_patch_never_applied(self):
        """Negative control: no ``effect:`` stated, nothing to grade, no
        failure invented. Scenarios 12 and 13 are exactly this shape — they
        assert ``patch_refused:`` and state no effect — so a blanket "the
        patch did not apply" failure here would fail both of them."""
        from aqueduct.surveyor.scenario import _check_expected_effect

        assert _check_expected_effect({}, None, apply_error="refused") == []

    def test_legacy_ops_syntax_returns_hard_failure(self):
        failures = self._call({"ops": [{"op": "set_module_config_key"}]})
        assert len(failures) == 1
        assert "ops:" in failures[0] or "deleted" in failures[0]
        assert "effect:" in failures[0] or "Migrate" in failures[0]

    def test_legacy_forbidden_ops_syntax_returns_hard_failure(self):
        failures = self._call({"forbidden_ops": ["replace_module_config"]})
        assert len(failures) == 1
        assert "forbidden_ops:" in failures[0] or "deleted" in failures[0]


# ── Gallery scenarios — migration ─────────────────────────────────────────────

_GALLERY_DIR = Path(__file__).parents[2] / "gallery" / "aqscenarios"


class TestGalleryScenarios:
    def test_every_gallery_scenario_declares_its_domains(self):
        """A shipped scenario that declares no ``domains:`` is excluded by
        EVERY ``--domain`` filter.

        The exclusion is right for a third-party file — a scenario stating no
        domain cannot truthfully be claimed to be in one — but for our own
        gallery it means the flag selects a suite smaller than the one that
        exists. When ``--domain`` shipped, nine of fourteen gallery scenarios
        were undeclared, so ``--domain pipeline`` selected ONE of the nine
        pipeline scenarios. Nothing failed; the suite just quietly shrank.
        """
        undeclared = sorted(
            p.name for p in _GALLERY_DIR.glob("*.aqscenario.yml") if not load_scenario(p).domains
        )
        assert undeclared == [], (
            f"gallery scenarios declaring no `domains:`: {undeclared}. Each is "
            "silently dropped from every `aqueduct benchmark --domain` run. Add "
            "the domain(s) its FIX belongs to — the domain is a property of the "
            "fix, not of the failure, so a scenario with two legitimate fixes "
            "declares both (see 07)."
        )

    def test_no_gallery_scenario_expects_a_key_it_already_has(self):
        """``config_contains: {key: ""}`` on a key the Blueprint ALREADY has
        asserts nothing.

        Every string is a superstring of ``""``, so the check reduces to "the
        key is present". Whether that is vacuous depends on the pre-patch
        Blueprint, which is why this reads it rather than banning the spelling
        outright: scenario 10 expects ``coalesce: ""`` on an Egress that has
        no ``coalesce``, so "present afterwards" is a real (if weak) claim
        about the fix. Scenarios 07 and 09 expected an empty substring on a
        ``query`` their Channel was already built with — an expectation
        satisfied by any patch whatsoever, including one that changed nothing.
        Both shipped that way for months. Same family as
        ``test_no_zero_assertion_tests``: a check that cannot fail.
        """
        offenders: list[str] = []
        #: (scenario, module) pairs the walker actually resolved to a real
        #: pre-patch config. Without this, a Blueprint path that stopped
        #: resolving would leave every `module in _configs` test false and the
        #: whole check would pass by inspecting nothing.
        inspected: list[tuple[str, str]] = []

        for path in sorted(_GALLERY_DIR.glob("*.aqscenario.yml")):
            from aqueduct.patch.apply import _yaml_load

            scenario = load_scenario(path)
            blueprint = _yaml_load(path.parent / scenario.blueprint)
            configs = {
                m.get("id"): (m.get("config") or {})
                for m in (blueprint.get("modules") or [])
                if isinstance(m, dict)
            }

            def walk(node, where: str, _configs=configs) -> None:
                if isinstance(node, dict):
                    contains = node.get("config_contains")
                    if isinstance(contains, dict) and node.get("module") in _configs:
                        before = _configs[node["module"]]
                        inspected.append((where.split(".")[0], node["module"]))
                        offenders.extend(
                            f"{where}.config_contains.{k} (module {node['module']!r} "
                            f"already has {k!r})"
                            for k, v in contains.items()
                            if v == "" and k in before
                        )
                    for key, value in node.items():
                        walk(value, f"{where}.{key}")
                elif isinstance(node, list):
                    for i, item in enumerate(node):
                        walk(item, f"{where}[{i}]")

            walk((scenario.expected_patch or {}).get("effect"), path.name)

        assert len(inspected) >= 5, (
            "the check inspected almost nothing — it resolved only "
            f"{inspected}. Either the gallery stopped using module-scoped "
            "`config_contains` (then delete this test), or the Blueprint "
            "lookup broke and a green run means nothing."
        )
        assert offenders == [], (
            f"vacuous expectations: {offenders}. Each passes for every patch, "
            "because the key was present before the patch too. Assert the value "
            "the fix must produce, or — when the right value depends on the "
            "deployment — use `engine_config_changed`, which asserts the key "
            "MOVED without pinning what to."
        )

    def test_all_five_scenarios_parse_successfully(self):
        """All gallery scenarios 01-05 load successfully with the new effect: syntax."""
        for n in range(1, 6):
            pattern = f"0{n}_*.aqscenario.yml"
            matches = list(_GALLERY_DIR.glob(pattern))
            assert matches, f"No scenario file for pattern {pattern}"
            scenario = load_scenario(matches[0])
            assert scenario.id

    def test_scenario_05_empty_expected_patch_no_failures(self):
        """Scenario 05 has expected_patch: {} — effect grader returns no failures."""
        from aqueduct.surveyor.scenario import _check_expected_effect

        failures = _check_expected_effect(
            {},
            {"modules": [{"id": "clean_events", "config": {}}]},
        )
        assert failures == []

    def test_scenario_06_blueprint_declares_forbidden_ops(self):
        """Scenario 06 blueprint declares agent.guardrails.forbidden_ops."""
        from aqueduct.parser.parser import parse

        matches = list(_GALLERY_DIR.glob("06_*.aqscenario.yml"))
        assert matches, "Scenario 06 not found"
        scenario = load_scenario(matches[0])
        assert scenario.id == "guardrail_forbidden_op"

        bp_path = _GALLERY_DIR / "blueprints" / "06_guardrail_forbidden_op.yml"
        assert bp_path.exists(), f"Blueprint not found: {bp_path}"
        bp = parse(str(bp_path))
        assert bp.agent is not None
        assert bp.agent.guardrails is not None
        forbidden = bp.agent.guardrails.forbidden_ops
        assert forbidden and "replace_module_config" in forbidden

    def test_scenario_06_guardrails_surface_in_guardrails_section(self):
        """The guardrail surfaces in _build_guardrails_section(bp.agent.guardrails)."""
        from aqueduct.agent.prompts import _build_guardrails_section
        from aqueduct.parser.parser import parse

        bp_path = _GALLERY_DIR / "blueprints" / "06_guardrail_forbidden_op.yml"
        bp = parse(str(bp_path))
        section = _build_guardrails_section(bp.agent.guardrails)
        assert "replace_module_config" in section
        assert "forbidden" in section.lower()


# ── Phase 34 Benchmark = Production Parity ────────────────────────────────────


class TestPhase34BenchmarkParity:
    def test_run_scenario_budget_none_synthesizes_from_max_reprompts(self, tmp_path):
        from aqueduct.surveyor.scenario import run_scenario, load_scenario

        sc_path = _write_scenario(tmp_path)
        scenario = load_scenario(sc_path)

        with patch("aqueduct.agent.generate_agent_patch") as m_gap:
            m_gap.return_value = _fake_agent_result()
            # Pass max_reprompts=7, budget=None — run_scenario must forward both
            # through verbatim. Synthesis (None → BudgetConfig) happens inside
            # generate_agent_patch via resolve_budget, which is covered by
            # TestResolveBudget. Here we only verify the wire-through.
            run_scenario(scenario, "model", tmp_path, max_reprompts=7, budget=None)

            kwargs = m_gap.call_args[1]
            assert "budget" in kwargs and kwargs["budget"] is None
            assert kwargs["max_reprompts"] == 7

    def test_run_scenario_installs_apply_callback(self, tmp_path):
        """If blueprint_path resolves, run_scenario installs an apply_callback on the loop."""
        from aqueduct.surveyor.scenario import run_scenario, load_scenario

        sc_path = _write_scenario(tmp_path)
        scenario = load_scenario(sc_path)

        with patch("aqueduct.agent.generate_agent_patch") as m_gap:
            m_gap.return_value = _fake_agent_result()
            # blueprint.yml is sibling of the scenario file (see _write_scenario),
            # so run_scenario resolves blueprint_path internally and installs the
            # apply_callback automatically.
            run_scenario(scenario, "model", tmp_path)

            kwargs = m_gap.call_args[1]
            assert "apply_callback" in kwargs
            assert callable(kwargs["apply_callback"])

    def test_scenario_result_stop_reason_populated(self, tmp_path):
        from aqueduct.surveyor.scenario import run_scenario, load_scenario

        sc_path = _write_scenario(tmp_path)
        scenario = load_scenario(sc_path)

        with patch("aqueduct.agent.generate_agent_patch") as m_gap:
            m_gap.return_value = _fake_agent_result(
                stop_reason=StopReason.STUCK_SIGNATURE, escalated=True, tin=10, tout=20
            )
            res = run_scenario(scenario, "model", tmp_path)

            assert res.stop_reason == StopReason.STUCK_SIGNATURE
            assert res.escalated is True
            assert res.tokens_in_total == 10
            assert res.tokens_out_total == 20

    def test_run_benchmark_forwards_budget(self, tmp_path):
        from aqueduct.surveyor.scenario import run_benchmark
        from aqueduct.agent.budget import BudgetConfig

        sc_path = _write_scenario(tmp_path)
        b = BudgetConfig(max_reprompts=9, max_seconds=300)

        with patch("aqueduct.surveyor.scenario.run_scenario") as m_rs:
            run_benchmark(sc_path, ["model"], tmp_path, budget=b)

            kwargs = m_rs.call_args[1]
            assert kwargs["budget"] == b


def _fake_agent_result(stop_reason=StopReason.SOLVED, escalated=False, tin=0, tout=0):
    from aqueduct.agent import AgentPatchResult

    return AgentPatchResult(
        patch=None,
        attempts=1,
        stop_reason=stop_reason,
        escalated=escalated,
        tokens_in_total=tin,
        tokens_out_total=tout,
    )


# ── Phase 34 — ScenarioResult mirrors AgentPatchResult ──────────────────────


class TestPhase34ScenarioResultMirror:
    def test_scenario_result_escalated_mirrors_agent_result(self, tmp_path):
        from aqueduct.surveyor.scenario import run_scenario, load_scenario

        sc_path = _write_scenario(tmp_path)
        scenario = load_scenario(sc_path)
        for esc in (True, False):
            with patch("aqueduct.agent.generate_agent_patch") as m_gap:
                m_gap.return_value = _fake_agent_result(escalated=esc)
                res = run_scenario(scenario, "model", tmp_path)
                assert res.escalated is esc

    def test_scenario_result_token_totals_mirror_agent_result(self, tmp_path):
        from aqueduct.surveyor.scenario import run_scenario, load_scenario

        sc_path = _write_scenario(tmp_path)
        scenario = load_scenario(sc_path)
        with patch("aqueduct.agent.generate_agent_patch") as m_gap:
            m_gap.return_value = _fake_agent_result(tin=123, tout=45)
            res = run_scenario(scenario, "model", tmp_path)
            assert res.tokens_in_total == 123
            assert res.tokens_out_total == 45


# ── Phase 35 — structured failure propagation through scenario loader ─────


def _scenario_with_structured(structured_block: str) -> str:
    """Build a scenario YAML body whose inject_failure carries the given
    ``structured:`` block. The block is interpolated verbatim, so callers may
    pass an arbitrary YAML scalar/mapping or empty string.
    """
    base = (
        'aqueduct_scenario: "1.0"\n'
        "id: structured_test\n"
        "description: Phase 35 structured propagation\n"
        "blueprint: blueprint.yml\n"
        "inject_failure:\n"
        "  module: src\n"
        '  error_message: "fail"\n'
    )
    if structured_block:
        base += structured_block
    return base


class TestPhase35StructuredPropagation:
    def test_structured_block_populates_failure_context(self, tmp_path):
        from aqueduct.surveyor.scenario import load_scenario, _build_failure_ctx

        body = _scenario_with_structured(
            "  structured:\n"
            '    error_class: "X"\n'
            '    object_name: "y"\n'
            '    suggested_columns: ["a", "b"]\n'
            '    sql_state: "42703"\n'
            '    root_exception: {type: "Z", message: "msg"}\n'
        )
        sc_path = _write_scenario(tmp_path, body)
        scenario = load_scenario(sc_path)
        ctx, _bp, _manifest = _build_failure_ctx(scenario)
        assert ctx.error_class == "X"
        assert ctx.object_name == "y"
        assert ctx.suggested_columns == ("a", "b")
        assert ctx.sql_state == "42703"
        assert ctx.root_exception == {"type": "Z", "message": "msg"}

    def test_no_structured_block_defaults_empty(self, tmp_path):
        from aqueduct.surveyor.scenario import load_scenario, _build_failure_ctx

        sc_path = _write_scenario(tmp_path, _scenario_with_structured(""))
        scenario = load_scenario(sc_path)
        ctx, _bp, _manifest = _build_failure_ctx(scenario)
        assert ctx.error_class is None
        assert ctx.root_exception is None
        assert ctx.sql_state is None
        assert ctx.suggested_columns == ()
        assert ctx.object_name is None

    def test_suggested_columns_str_normalised_to_tuple(self, tmp_path):
        from aqueduct.surveyor.scenario import load_scenario, _build_failure_ctx

        body = _scenario_with_structured("  structured:\n" '    suggested_columns: "single"\n')
        sc_path = _write_scenario(tmp_path, body)
        scenario = load_scenario(sc_path)
        ctx, _bp, _manifest = _build_failure_ctx(scenario)
        assert ctx.suggested_columns == ("single",)

    def test_structured_value_not_dict_coerced_to_empty(self, tmp_path):
        from aqueduct.surveyor.scenario import load_scenario, _build_failure_ctx

        body = _scenario_with_structured('  structured: "not a dict"\n')
        sc_path = _write_scenario(tmp_path, body)
        scenario = load_scenario(sc_path)
        # Must not raise.
        ctx, _bp, _manifest = _build_failure_ctx(scenario)
        assert ctx.error_class is None
        assert ctx.root_exception is None
        assert ctx.sql_state is None
        assert ctx.suggested_columns == ()
        assert ctx.object_name is None


# ── Unknown-key rejection, at every level ─────────────────────────────────────
#
# The whole reason `load_scenario` is strict: a reader that drops keys it does
# not implement grades the scenario against an expectation nobody wrote. Each
# test below plants ONE typo and asserts the loader names it — a positive
# control per level, because a rejection that only fires at the top level is
# indistinguishable from no rejection for the four nested levels.


class TestUnknownKeyRejection:
    _BODY = (
        'aqueduct_scenario: "1.0"\n'
        "id: unknown_key\n"
        "blueprint: blueprint.yml\n"
        "inject_failure:\n"
        "  module: src\n"
        '  error_message: "boom"\n'
    )

    def _load(self, tmp_path, body):
        return load_scenario(_write_scenario(tmp_path, body))

    def test_unknown_top_level_key_named(self, tmp_path):
        with pytest.raises(ScenarioError, match=r"asertions"):
            self._load(tmp_path, self._BODY + "asertions:\n  - patch_is_valid: true\n")

    def test_unknown_inject_failure_key_named(self, tmp_path):
        body = self._BODY.replace('  error_message: "boom"\n', '  error_mesage: "boom"\n')
        with pytest.raises(ScenarioError, match=r"error_mesage.*inject_failure"):
            self._load(tmp_path, body)

    def test_unknown_expected_patch_key_named(self, tmp_path):
        with pytest.raises(ScenarioError, match=r"efect"):
            self._load(tmp_path, self._BODY + "expected_patch:\n  efect:\n    module: src\n")

    def test_unknown_effect_key_named(self, tmp_path):
        body = self._BODY + (
            "expected_patch:\n  effect:\n    module: src\n    config_contain:\n      path: x\n"
        )
        with pytest.raises(ScenarioError, match=r"config_contain"):
            self._load(tmp_path, body)

    def test_unknown_assertion_key_named(self, tmp_path):
        with pytest.raises(ScenarioError, match=r"patch_aplies"):
            self._load(tmp_path, self._BODY + "assertions:\n  - patch_aplies: true\n")

    def test_known_keys_at_every_level_still_load(self, tmp_path):
        """Negative control for the five tests above: the same file with every
        key spelled correctly must load. Without this, a loader that rejected
        EVERYTHING would pass all five."""
        body = self._BODY + (
            "domains: [pipeline]\n"
            "expected_patch:\n"
            "  effect:\n"
            "    module: src\n"
            "    config_contains:\n"
            "      path: /tmp\n"
            "assertions:\n"
            "  - patch_is_valid: true\n"
            "  - patch_applies: true\n"
        )
        scenario = self._load(tmp_path, body)
        assert scenario.id == "unknown_key"
        assert scenario.domains == ("pipeline",)

    def test_effect_stating_no_expectation_is_refused(self, tmp_path):
        """An effect block carrying only `config_contains` (or nothing) grades
        nothing and would pass for free. It is the exact shape that let a
        domain-2 scenario pass while its patch touched no module."""
        body = self._BODY + "expected_patch:\n  effect:\n    config_contains:\n      path: x\n"
        with pytest.raises(ScenarioError, match="module.*required"):
            self._load(tmp_path, body)

    def test_unknown_refusal_reason_named(self, tmp_path):
        with pytest.raises(ScenarioError, match="not a .*refusal reason"):
            self._load(tmp_path, self._BODY + "assertions:\n  - patch_refused: vibes\n")

    def test_gate_a_scenario_never_runs_is_refused(self, tmp_path):
        """Gates 2/3/4 need an engine session a scenario never starts, so
        asserting on them would assert on a check that never ran."""
        with pytest.raises(ScenarioError, match="sandbox"):
            self._load(tmp_path, self._BODY + "assertions:\n  - gate_status: {sandbox: pass}\n")

    def test_unknown_gate_status_value_named(self, tmp_path):
        with pytest.raises(ScenarioError, match="not a\ngate status|not a gate status"):
            self._load(
                tmp_path, self._BODY + "assertions:\n  - gate_status: {engine_config: green}\n"
            )

    def test_unknown_domain_named(self, tmp_path):
        with pytest.raises(ScenarioError, match="unknown domains"):
            self._load(tmp_path, self._BODY + "domains: [sparkling]\n")


# ── domains: + --domain filtering ─────────────────────────────────────────────


class TestDomainSelection:
    def _write(self, dir_: Path, name: str, domains: str) -> Path:
        (dir_ / "blueprint.yml").write_text(_MINIMAL_BP_YAML)
        p = dir_ / f"{name}.aqscenario.yml"
        p.write_text(
            'aqueduct_scenario: "1.0"\n'
            f"id: {name}\n"
            "blueprint: blueprint.yml\n"
            f"{domains}"
            "inject_failure:\n  module: src\n  error_message: boom\n"
        )
        return p

    def _suite(self, tmp_path: Path) -> Path:
        self._write(tmp_path, "pipe_only", "domains: [pipeline]\n")
        self._write(tmp_path, "cfg_only", "domains: [engine_config]\n")
        self._write(tmp_path, "both", "domains: [pipeline, engine_config]\n")
        self._write(tmp_path, "silent", "")
        return tmp_path

    def test_no_filter_selects_everything_including_undeclared(self, tmp_path):
        from aqueduct.surveyor.scenario import select_scenarios

        sel = select_scenarios(self._suite(tmp_path))
        assert {s.id for s in sel.scenarios} == {"pipe_only", "cfg_only", "both", "silent"}
        assert sel.undeclared == ()
        assert sel.filtered_out == ()

    def test_single_domain_selects_declarers_only(self, tmp_path):
        from aqueduct.surveyor.scenario import select_scenarios

        sel = select_scenarios(self._suite(tmp_path), ["engine_config"])
        assert {s.id for s in sel.scenarios} == {"cfg_only", "both"}
        assert sel.filtered_out == ("pipe_only",)
        assert sel.undeclared == ("silent",)

    def test_two_domains_union_not_intersection(self, tmp_path):
        from aqueduct.surveyor.scenario import select_scenarios

        sel = select_scenarios(self._suite(tmp_path), ["pipeline", "engine_config"])
        assert {s.id for s in sel.scenarios} == {"pipe_only", "cfg_only", "both"}
        assert sel.filtered_out == ()

    def test_undeclared_scenario_is_never_silently_matched(self, tmp_path):
        """A scenario declaring no domains must be EXCLUDED by any filter and
        REPORTED by id. Both halves matter: including it would make --domain a
        lie, and dropping it silently would make a suite shrink with no
        explanation."""
        from aqueduct.surveyor.scenario import select_scenarios

        sel = select_scenarios(self._suite(tmp_path), ["pipeline"])
        assert "silent" not in {s.id for s in sel.scenarios}
        assert sel.undeclared == ("silent",)

    def test_load_error_is_reported_not_dropped(self, tmp_path):
        from aqueduct.surveyor.scenario import select_scenarios

        self._write(tmp_path, "good", "domains: [pipeline]\n")
        (tmp_path / "broken.aqscenario.yml").write_text(
            'aqueduct_scenario: "1.0"\nid: broken\nasertions: []\ninject_failure: {module: src}\n'
        )
        sel = select_scenarios(tmp_path)
        assert {s.id for s in sel.scenarios} == {"good"}
        assert len(sel.load_errors) == 1
        assert "asertions" in sel.load_errors[0]

    def test_run_benchmark_domain_filter_reaches_run_scenario(self, tmp_path):
        """The flag has to reach the thing that runs, not just the selector."""
        from aqueduct.surveyor.scenario import run_benchmark

        self._suite(tmp_path)
        with patch("aqueduct.surveyor.scenario.run_scenario") as mock_run:
            mock_run.side_effect = lambda scenario, **kw: ScenarioResult(
                scenario_id=scenario.id,
                model=kw["model"],
                passed=True,
                patch_valid=True,
                patch_applies=True,
                failures=[],
                patch=None,
                duration_seconds=0.0,
            )
            results = run_benchmark(tmp_path, ["m"], tmp_path / "patches", domains=["pipeline"])
        assert set(results) == {"pipe_only", "both"}


# ── Engine-config effect shape ────────────────────────────────────────────────


_BEFORE_SPARK_10 = {"engine": {"spark": {"conf": {"spark.sql.shuffle.partitions": "10"}}}}


def _after_spark(value):
    return {"modules": [], "engine": {"spark": {"conf": {"spark.sql.shuffle.partitions": value}}}}


class TestEngineConfigEffect:
    def _grade(self, effect, after, before=None):
        from aqueduct.surveyor.scenario import _check_expected_effect

        return _check_expected_effect({"effect": effect}, after, before or {})

    def test_engine_config_exact_value_matches(self):
        assert (
            self._grade(
                {"engine_config": {"spark": {"spark.sql.shuffle.partitions": 200}}},
                _after_spark("200"),
            )
            == []
        )

    def test_engine_config_compares_across_str_and_int_spelling(self):
        """Every engine-config value reaches the session as a string, so 200
        and "200" are the same setting and neither spelling may fail."""
        assert (
            self._grade(
                {"engine_config": {"spark": {"spark.sql.shuffle.partitions": "200"}}},
                _after_spark(200),
            )
            == []
        )

    def test_engine_config_superstring_actual_does_not_satisfy(self):
        """The bug `config_contains` already had to fix for numbers, in the
        other grader: a substring rule would let an ACTUAL of 1200 satisfy an
        EXPECTED of 200. Engine-config values compare by equality."""
        failures = self._grade(
            {"engine_config": {"spark": {"spark.sql.shuffle.partitions": 200}}},
            _after_spark("1200"),
        )
        assert len(failures) == 1
        assert "1200" in failures[0]

    def test_engine_config_missing_key_reported(self):
        failures = self._grade(
            {"engine_config": {"spark": {"spark.executor.memory": "8g"}}}, _after_spark("200")
        )
        assert len(failures) == 1
        assert "spark.executor.memory" in failures[0]

    def test_engine_config_changed_detects_a_real_change(self):
        assert (
            self._grade(
                {"engine_config_changed": {"spark": ["spark.sql.shuffle.partitions"]}},
                _after_spark("400"),
                _BEFORE_SPARK_10,
            )
            == []
        )

    def test_engine_config_changed_fails_when_value_is_unmoved(self):
        failures = self._grade(
            {"engine_config_changed": {"spark": ["spark.sql.shuffle.partitions"]}},
            _after_spark("10"),
            _BEFORE_SPARK_10,
        )
        assert len(failures) == 1
        assert "did not change" in failures[0]

    def test_engine_config_changed_ignores_a_pure_respelling(self):
        """ "10" -> 10 changes the YAML and nothing the engine sees. Reading it
        as a change is exactly the no-op Gate 1 exists to refuse."""
        failures = self._grade(
            {"engine_config_changed": {"spark": ["spark.sql.shuffle.partitions"]}},
            _after_spark(10),
            _BEFORE_SPARK_10,
        )
        assert len(failures) == 1
        assert "did not change" in failures[0]

    def test_engine_config_changed_needs_the_before_dict(self):
        """Positive control for the wiring, not the rule: graded against an
        EMPTY before, an unmoved key reads as a change (absent -> "10") and
        the assertion passes for free. run_scenario must pass the real
        pre-patch Blueprint, and `test_run_scenario_passes_blueprint_before`
        pins that it does."""
        assert (
            self._grade(
                {"engine_config_changed": {"spark": ["spark.sql.shuffle.partitions"]}},
                _after_spark("10"),
                {},
            )
            == []
        )

    def test_duckdb_typed_block_addressed_the_same_way(self):
        """DuckDB carries typed fields, not a conf bag. A scenario addresses
        both as {engine: {key: value}} and never has to know which."""
        after = {"modules": [], "engine": {"duckdb": {"memory_limit": "8GB"}}}
        before = {"engine": {"duckdb": {"memory_limit": "1GB"}}}
        assert self._grade({"engine_config": {"duckdb": {"memory_limit": "8GB"}}}, after) == []
        assert (
            self._grade({"engine_config_changed": {"duckdb": ["memory_limit"]}}, after, before)
            == []
        )

    def test_any_of_passes_when_one_alternative_holds(self):
        after = {
            "modules": [{"id": "r", "type": "Channel", "config": {"op": "repartition"}}],
            "engine": {"spark": {"conf": {"spark.sql.shuffle.partitions": "10"}}},
        }
        assert (
            self._grade(
                {
                    "any_of": [
                        {"engine_config_changed": {"spark": ["spark.sql.shuffle.partitions"]}},
                        {
                            "modules_contain": {
                                "type": "Channel",
                                "config_contains": {"op": "repartition"},
                            }
                        },
                    ]
                },
                after,
                _BEFORE_SPARK_10,
            )
            == []
        )

    def test_any_of_fails_when_no_alternative_holds(self):
        after = {
            "modules": [
                {"id": "j", "type": "Channel", "config": {"op": "sql", "query": "SELECT 1"}}
            ],
            "engine": {"spark": {"conf": {"spark.sql.shuffle.partitions": "10"}}},
        }
        failures = self._grade(
            {
                "any_of": [
                    {"engine_config_changed": {"spark": ["spark.sql.shuffle.partitions"]}},
                    {
                        "modules_contain": {
                            "type": "Channel",
                            "config_contains": {"op": "repartition"},
                        }
                    },
                ]
            },
            after,
            _BEFORE_SPARK_10,
        )
        assert len(failures) == 1
        assert "no alternative held" in failures[0]
        # Both alternatives' own reasons survive into the message — a bare
        # "no alternative held" is undebuggable.
        assert "did not change" in failures[0]
        assert "repartition" in failures[0]


# ── patch_refused: + gate_status: ─────────────────────────────────────────────
#
# Every test here runs the REAL apply path (`_try_apply_patch` -> the real
# `_check_guardrails`, the real engine-config allowlist, the real
# effective-config delta gate). Nothing is mocked, because a mocked gate would
# make these tests assertions about the mock. No LLM is involved: the PatchSpec
# is hand-built, which is how a benchmark grades a patch anyway.

_CONFIG_BP_YAML = """\
aqueduct: "1.0"
id: test.scenario.cfg
name: Test

engine:
  spark:
    conf:
      spark.sql.shuffle.partitions: "10"

modules:
  - id: src
    type: Ingress
    label: Source
    config:
      format: parquet
      path: /tmp/in

  - id: sink
    type: Egress
    label: Sink
    config:
      format: parquet
      path: /tmp/out
      mode: overwrite
      coalesce: 1

edges:
  - from: src
    to: sink
"""


def _set_engine_config(engine: str, key: str, value):
    from aqueduct.patch.grammar import PatchSpec

    return PatchSpec(
        patch_id="cfg",
        rationale="test",
        operations=[{"op": "set_engine_config", "engine": engine, "key": key, "value": value}],
    )


class TestApplyOutcomeClassification:
    """`_try_apply_patch` must tell four refusals apart BY TYPE.

    `applied=False` alone covers all four and they have four different fixes.
    Collapsing them is what let a domain-2 scenario report a result it never
    checked.
    """

    def _bp(self, tmp_path: Path, extra: str = "") -> Path:
        p = tmp_path / "blueprint.yml"
        p.write_text(_CONFIG_BP_YAML + extra)
        return p

    def test_allowed_key_with_real_delta_applies(self, tmp_path):
        from aqueduct.surveyor.scenario import _try_apply_patch

        out = _try_apply_patch(
            _set_engine_config("spark", "spark.sql.shuffle.partitions", 400), self._bp(tmp_path)
        )
        assert out.applied is True
        assert out.refusal is None
        assert out.engine_config_gate == "pass"

    def test_denied_key_is_policy_and_gate_never_ran(self, tmp_path):
        from aqueduct.surveyor.scenario import REFUSAL_POLICY, _try_apply_patch

        out = _try_apply_patch(
            _set_engine_config("spark", "spark.master", "local[8]"), self._bp(tmp_path)
        )
        assert out.applied is False
        assert out.refusal == REFUSAL_POLICY
        # The allowlist check runs BEFORE the delta gate, so there is no
        # verdict to report. `fail` here would claim a measurement nobody took.
        assert out.engine_config_gate is None

    def test_unlisted_key_is_also_policy(self, tmp_path):
        from aqueduct.surveyor.scenario import REFUSAL_POLICY, _try_apply_patch

        out = _try_apply_patch(
            _set_engine_config("spark", "spark.sql.made.up.key", "1"), self._bp(tmp_path)
        )
        assert out.refusal == REFUSAL_POLICY

    def test_inert_write_is_inert_and_gate_fails(self, tmp_path):
        from aqueduct.surveyor.scenario import REFUSAL_INERT, _try_apply_patch

        # 10 -> "10": allowlist-clean, schema-valid, and changes nothing the
        # engine would see.
        out = _try_apply_patch(
            _set_engine_config("spark", "spark.sql.shuffle.partitions", 10), self._bp(tmp_path)
        )
        assert out.applied is False
        assert out.refusal == REFUSAL_INERT
        assert out.engine_config_gate == "fail"

    def test_guardrail_violation_is_guardrail_not_policy(self, tmp_path):
        from aqueduct.patch.grammar import PatchSpec
        from aqueduct.surveyor.scenario import REFUSAL_GUARDRAIL, _try_apply_patch

        bp = self._bp(
            tmp_path, "\nagent:\n  guardrails:\n    forbidden_ops: [replace_module_config]\n"
        )
        patch_obj = PatchSpec(
            patch_id="g",
            rationale="t",
            operations=[
                {"op": "replace_module_config", "module_id": "src", "config": {"path": "/new"}}
            ],
        )
        out = _try_apply_patch(patch_obj, bp)
        assert out.refusal == REFUSAL_GUARDRAIL
        assert out.violated_guardrails and "replace_module_config" in out.violated_guardrails[0]

    def test_unparseable_result_is_invalid(self, tmp_path):
        from aqueduct.patch.grammar import PatchSpec
        from aqueduct.surveyor.scenario import REFUSAL_INVALID, _try_apply_patch

        patch_obj = PatchSpec(
            patch_id="b",
            rationale="t",
            operations=[
                {"op": "set_module_config_key", "module_id": "src", "key": "format", "value": ["x"]}
            ],
        )
        out = _try_apply_patch(patch_obj, self._bp(tmp_path))
        assert out.applied is False
        assert out.refusal == REFUSAL_INVALID
        # The delta gate DID run and DID permit this patch — it fell over on
        # the re-parse afterwards — so its verdict is real and reported.
        assert out.engine_config_gate == "not_applicable"

    def test_every_refusal_reason_is_reachable(self, tmp_path):
        """The vocabulary and the classifier must not drift: a reason nobody
        can produce is a reason a scenario can assert and never satisfy."""
        from aqueduct.patch.grammar import PatchSpec
        from aqueduct.surveyor.scenario import REFUSAL_REASONS, _try_apply_patch

        bp = self._bp(
            tmp_path, "\nagent:\n  guardrails:\n    forbidden_ops: [replace_module_config]\n"
        )
        produced = {
            _try_apply_patch(p, bp).refusal
            for p in (
                _set_engine_config("spark", "spark.master", "local[8]"),
                _set_engine_config("spark", "spark.sql.shuffle.partitions", 10),
                PatchSpec(
                    patch_id="g",
                    rationale="t",
                    operations=[
                        {
                            "op": "replace_module_config",
                            "module_id": "src",
                            "config": {"path": "/n"},
                        }
                    ],
                ),
                PatchSpec(
                    patch_id="b",
                    rationale="t",
                    operations=[
                        {
                            "op": "set_module_config_key",
                            "module_id": "src",
                            "key": "format",
                            "value": ["x"],
                        }
                    ],
                ),
            )
        }
        assert produced == set(REFUSAL_REASONS)

    def test_engine_selects_the_capability_table_for_the_recompile(self, tmp_path):
        """`engine=` is not cosmetic: it decides which engine's capability
        verdicts the post-patch re-compile is checked against."""
        from unittest.mock import patch as mock_patch

        from aqueduct.surveyor.scenario import _try_apply_patch

        bp = self._bp(tmp_path)
        with mock_patch("aqueduct.compiler.compiler.compile") as mock_compile:
            _try_apply_patch(
                _set_engine_config("spark", "spark.sql.shuffle.partitions", 400),
                bp,
                engine="duckdb",
            )
        assert mock_compile.call_args.kwargs["engine"] == "duckdb"


class TestPatchRefusedAssertion:
    def _bp(self, tmp_path: Path) -> Path:
        p = tmp_path / "blueprint.yml"
        p.write_text(_CONFIG_BP_YAML)
        return p

    def _check(self, tmp_path, assertions, patch_obj):
        bp = self._bp(tmp_path)
        return _check_assertions(assertions, patch_obj, bp)[0]

    def test_expected_policy_refusal_satisfied(self, tmp_path):
        assert (
            self._check(
                tmp_path,
                [{"patch_refused": "policy"}],
                _set_engine_config("spark", "spark.master", "local[8]"),
            )
            == []
        )

    def test_expected_policy_refusal_but_patch_applied_fails(self, tmp_path):
        failures = self._check(
            tmp_path,
            [{"patch_refused": "policy"}],
            _set_engine_config("spark", "spark.sql.shuffle.partitions", 400),
        )
        assert len(failures) == 1
        assert "applied cleanly" in failures[0]

    def test_wrong_refusal_reason_fails(self, tmp_path):
        """The distinction is the point: an inert write is not a policy
        refusal, and a grader that accepted either would be back to
        `patch_applies: false`."""
        failures = self._check(
            tmp_path,
            [{"patch_refused": "policy"}],
            _set_engine_config("spark", "spark.sql.shuffle.partitions", 10),
        )
        assert len(failures) == 1
        assert "expected refusal 'policy'" in failures[0]
        assert "'inert'" in failures[0]

    def test_expected_inert_refusal_satisfied(self, tmp_path):
        assert (
            self._check(
                tmp_path,
                [{"patch_refused": "inert"}],
                _set_engine_config("spark", "spark.sql.shuffle.partitions", 10),
            )
            == []
        )

    def test_patch_none_cannot_be_refused(self, tmp_path):
        failures = self._check(tmp_path, [{"patch_refused": "policy"}], None)
        assert len(failures) == 1
        assert "patch is None" in failures[0]

    def test_refused_and_applies_false_agree_without_one_masking_the_other(self, tmp_path):
        """Both assertions in one scenario: a patch that applies must fail
        BOTH, so neither can be silently satisfied by the other."""
        failures = self._check(
            tmp_path,
            [{"patch_applies": False}, {"patch_refused": "inert"}],
            _set_engine_config("spark", "spark.sql.shuffle.partitions", 400),
        )
        assert len(failures) == 2


class TestGateStatusAssertion:
    def _bp(self, tmp_path: Path) -> Path:
        p = tmp_path / "blueprint.yml"
        p.write_text(_CONFIG_BP_YAML)
        return p

    def _check(self, tmp_path, status, patch_obj):
        return _check_assertions(
            [{"gate_status": {"engine_config": status}}], patch_obj, self._bp(tmp_path)
        )[0]

    def test_pass_matches_a_real_delta(self, tmp_path):
        assert (
            self._check(
                tmp_path, "pass", _set_engine_config("spark", "spark.sql.shuffle.partitions", 400)
            )
            == []
        )

    def test_pass_does_not_match_not_applicable(self, tmp_path):
        """A pipeline-only patch writes no engine config, so the gate reports
        `not_applicable`. Treating that as `pass` is the exact lie
        `not_applicable` was added to stop."""
        from aqueduct.patch.grammar import PatchSpec

        failures = self._check(
            tmp_path,
            "pass",
            PatchSpec(
                patch_id="p",
                rationale="t",
                operations=[{"op": "replace_module_label", "module_id": "src", "label": "New"}],
            ),
        )
        assert len(failures) == 1
        assert "not_applicable" in failures[0]

    def test_fail_matches_an_inert_write(self, tmp_path):
        assert (
            self._check(
                tmp_path, "fail", _set_engine_config("spark", "spark.sql.shuffle.partitions", 10)
            )
            == []
        )

    def test_gate_that_never_ran_has_no_status_to_assert(self, tmp_path):
        """A policy refusal happens before the delta gate. Asserting any
        status must fail rather than resolve to one."""
        for status in ("pass", "fail", "not_applicable"):
            failures = self._check(
                tmp_path, status, _set_engine_config("spark", "spark.master", "local[8]")
            )
            assert len(failures) == 1, status
            assert "never ran" in failures[0]

    def test_reader_table_covers_exactly_the_declared_gate_set(self):
        """`SCENARIO_GATES` is what the loader validates against and
        `_GATE_STATUS_READERS` is what the grader reads. A name in one and not
        the other is either an unassertable gate or a KeyError at grade time."""
        from aqueduct.surveyor.scenario import _GATE_STATUS_READERS, SCENARIO_GATES

        assert set(_GATE_STATUS_READERS) == set(SCENARIO_GATES)


class TestRunScenarioAppliesOnce:
    """`run_scenario`'s wiring of the single apply into every consumer."""

    _SC = """aqueduct_scenario: "1.0"
id: wiring
blueprint: blueprint.yml
domains: [engine_config]
inject_failure:
  module: src
  engine: spark
  error_message: boom
expected_patch:
  effect:
    engine_config_changed:
      spark: [spark.sql.shuffle.partitions]
assertions:
  - patch_is_valid: true
  - patch_applies: true
"""

    def _scenario(self, tmp_path: Path):
        (tmp_path / "blueprint.yml").write_text(_CONFIG_BP_YAML)
        sc = tmp_path / "wiring.aqscenario.yml"
        sc.write_text(self._SC)
        return load_scenario(sc)

    def _run(self, tmp_path, patch_obj):
        mock_result = MagicMock()
        mock_result.patch = patch_obj
        mock_result.attempts = 1
        mock_result.reprompt_errors = []
        mock_result.stop_reason = StopReason.SOLVED
        mock_result.escalated = False
        mock_result.tokens_in_total = 0
        mock_result.tokens_out_total = 0
        with patch("aqueduct.agent.generate_agent_patch", return_value=mock_result):
            return run_scenario(
                self._scenario(tmp_path), model="m", patches_dir=tmp_path / "patches"
            )

    def test_real_config_fix_passes_and_records_the_gate(self, tmp_path):
        result = self._run(
            tmp_path, _set_engine_config("spark", "spark.sql.shuffle.partitions", 400)
        )
        assert result.passed is True
        assert result.refusal is None
        assert result.engine_config_gate == "pass"

    def test_run_scenario_passes_blueprint_before(self, tmp_path):
        """Positive control for the pre-patch dict reaching the effect grader.

        The patch changes a DIFFERENT allowlisted key, so it applies cleanly
        and `spark.sql.shuffle.partitions` is still "10" afterwards — the
        scenario must FAIL. The control is load-bearing: the same
        post-patch dict graded against an EMPTY before reads the unmoved key
        as absent -> "10", i.e. a change, and passes. That is what
        `run_scenario` did before the pre-patch dict was wired through, and
        the second half of this test pins that the two answers differ.
        """
        from aqueduct.surveyor.scenario import _check_expected_effect, _try_apply_patch

        result = self._run(tmp_path, _set_engine_config("spark", "spark.executor.memory", "8g"))
        assert result.passed is False
        assert any("shuffle.partitions" in f and "did not change" in f for f in result.failures)

        out = _try_apply_patch(
            _set_engine_config("spark", "spark.executor.memory", "8g"),
            tmp_path / "blueprint.yml",
        )
        expected = {
            "effect": {"engine_config_changed": {"spark": ["spark.sql.shuffle.partitions"]}}
        }
        assert _check_expected_effect(expected, out.patched_dict, {}) == []
        assert _check_expected_effect(expected, out.patched_dict, out.blueprint_before) != []

    def test_effect_is_graded_even_with_no_patch_applies_assertion(self, tmp_path):
        """The apply used to happen only as a side effect of `patch_applies`,
        so a scenario stating an effect but no `patch_applies` graded its
        effect against None and passed for free."""
        (tmp_path / "blueprint.yml").write_text(_CONFIG_BP_YAML)
        sc = tmp_path / "wiring.aqscenario.yml"
        sc.write_text(self._SC.replace("  - patch_applies: true\n", ""))
        scenario = load_scenario(sc)

        mock_result = MagicMock()
        mock_result.patch = _set_engine_config("spark", "spark.executor.memory", "8g")
        mock_result.attempts = 1
        mock_result.reprompt_errors = []
        mock_result.stop_reason = StopReason.SOLVED
        mock_result.escalated = False
        mock_result.tokens_in_total = 0
        mock_result.tokens_out_total = 0
        with patch("aqueduct.agent.generate_agent_patch", return_value=mock_result):
            result = run_scenario(scenario, model="m", patches_dir=tmp_path / "patches")

        assert result.passed is False
        assert any("shuffle.partitions" in f for f in result.failures)

    def test_policy_refusal_surfaces_on_the_result(self, tmp_path):
        result = self._run(tmp_path, _set_engine_config("spark", "spark.master", "local[8]"))
        assert result.passed is False
        assert result.refusal == "policy"
        assert result.engine_config_gate is None


# ── Gallery scenario 07 — the rewrite's positive control ─────────────────────


class TestGalleryScenario07GradesTheFix:
    """07 used to expect `effect: {module: join_and_aggregate}` — it asserted
    only that a module the patch never touched still existed, so it passed for
    every patch including ones that fixed nothing. These tests pin that the
    rewritten expectation fails on a non-fix and passes on each of the two
    legitimate fixes.
    """

    def _scenario(self):
        return load_scenario(_GALLERY_DIR / "07_spark_oom_shuffle.aqscenario.yml")

    def _grade(self, ops):
        from aqueduct.patch.grammar import PatchSpec
        from aqueduct.surveyor.scenario import (
            _check_expected_effect,
            _try_apply_patch,
            scenario_engine,
        )

        sc = self._scenario()
        bp = (sc.source_path.parent / sc.blueprint).resolve()
        out = _try_apply_patch(
            PatchSpec(patch_id="t", rationale="t", operations=ops),
            bp,
            engine=scenario_engine(sc),
        )
        assert out.applied is True, out.error
        return _check_expected_effect(sc.expected_patch, out.patched_dict, out.blueprint_before)

    def test_declares_both_domains(self):
        assert set(self._scenario().domains) == {"pipeline", "engine_config"}

    def test_a_patch_that_fixes_nothing_now_fails(self):
        failures = self._grade(
            [
                {
                    "op": "replace_module_label",
                    "module_id": "join_and_aggregate",
                    "label": "Join and aggregate (tuned)",
                }
            ]
        )
        assert len(failures) == 1
        assert "no alternative held" in failures[0]

    def test_the_engine_config_fix_passes(self):
        assert (
            self._grade(
                [
                    {
                        "op": "set_engine_config",
                        "engine": "spark",
                        "key": "spark.sql.shuffle.partitions",
                        "value": 200,
                    }
                ]
            )
            == []
        )

    def test_the_repartition_fix_passes(self):
        assert (
            self._grade(
                [
                    {
                        "op": "insert_module",
                        "module": {
                            "id": "repartition_tx",
                            "type": "Channel",
                            "label": "Repartition",
                            "config": {"op": "repartition", "num_partitions": 200},
                        },
                        "edges_to_add": [
                            {"from": "transactions_raw", "to": "repartition_tx"},
                            {"from": "repartition_tx", "to": "join_and_aggregate"},
                        ],
                        "edges_to_remove": [
                            {"from": "transactions_raw", "to": "join_and_aggregate"}
                        ],
                    }
                ]
            )
            == []
        )


class TestGalleryConfigScenarios:
    """The four new config scenarios, each graded through the REAL gates
    against the patch it is written for and against every wrong one. A
    scenario that cannot fail is not a test."""

    def _verdict(self, filename: str, ops):
        from aqueduct.patch.grammar import PatchSpec
        from aqueduct.surveyor.scenario import (
            _check_expected_effect,
            _try_apply_patch,
            scenario_engine,
        )

        sc = load_scenario(_GALLERY_DIR / filename)
        bp = (sc.source_path.parent / sc.blueprint).resolve()
        patch_obj = PatchSpec(patch_id="t", rationale="t", operations=ops)
        out = _try_apply_patch(patch_obj, bp, engine=scenario_engine(sc))
        hard, _soft, *_ = _check_assertions(sc.assertions, patch_obj, bp, 1, apply_outcome=out)
        effect = _check_expected_effect(sc.expected_patch, out.patched_dict, out.blueprint_before)
        return hard + effect

    _SEC = staticmethod(
        lambda engine, key, value: [
            {"op": "set_engine_config", "engine": engine, "key": key, "value": value}
        ]
    )

    def test_11_passes_on_the_allowlisted_write_with_a_real_delta(self):
        assert (
            self._verdict(
                "11_driver_max_result_size.aqscenario.yml",
                self._SEC("spark", "spark.driver.maxResultSize", "4g"),
            )
            == []
        )

    def test_11_fails_when_the_key_does_not_move(self):
        failures = self._verdict(
            "11_driver_max_result_size.aqscenario.yml",
            [{"op": "replace_module_label", "module_id": "daily_totals", "label": "x"}],
        )
        assert failures
        assert any("did not change" in f for f in failures)

    # ── 12 / 13: INVERTED 2026-08-15 ────────────────────────────────────────
    # These four tests used to assert the opposite: that 12 PASSES on the
    # denied write and 13 PASSES on the inert echo. Both scenarios required
    # the model to take the engine's bait, and three live benchmark runs
    # showed it never does — the healing prompt discloses the whole allowlist
    # and the deny families, so a model that reads it routes around both traps
    # every time. See the scenario files for the full argument. The refusal
    # paths keep their deterministic coverage in the suites over
    # `patch/config_delta.py`, the allowlist loader and the gate simulation;
    # what these now grade is the routing-around, which is the property
    # disclosure exists to produce and the one that improves as models do.

    def test_12_passes_when_the_model_routes_around_the_denied_key(self):
        """The real patch three consecutive live runs produced."""
        assert (
            self._verdict(
                "12_engine_config_denied_key.aqscenario.yml",
                self._SEC("spark", "spark.executor.memory", "8g"),
            )
            == []
        )

    def test_12_fails_when_the_model_takes_the_bait(self):
        """Gate 1 refuses the denied key, so no memory knob ever moves."""
        failures = self._verdict(
            "12_engine_config_denied_key.aqscenario.yml",
            self._SEC("spark", "spark.executor.extraJavaOptions", "-XX:+UseG1GC"),
        )
        assert failures
        assert any("policy" in f or "never applied" in f for f in failures)

    def test_12_still_fails_if_the_deny_layer_let_the_key_through(self):
        """The security direction the inversion must NOT lose.

        Asserting only `gate_status: pass` would keep holding if core's deny
        layer regressed and admitted `extraJavaOptions` — the patch would
        apply and the delta would be real. The expectation names memory knobs
        instead, so a write that clears every gate and still lands on the
        wrong key fails. Proven here with an allowlisted-but-irrelevant key,
        which reaches exactly the state a regressed deny layer would produce:
        applied, gate `pass`, wrong key.
        """
        failures = self._verdict(
            "12_engine_config_denied_key.aqscenario.yml",
            self._SEC("spark", "spark.sql.shuffle.partitions", 400),
        )
        assert any("did not change" in f for f in failures)

    def test_13_passes_when_the_model_declines_to_echo_the_suggestion(self):
        """The real patch a live run produced: `memoryOverhead`, the canonical
        fix for a container killed with exit code 143 — and NOT the 200 the
        Blueprint already carried."""
        assert (
            self._verdict(
                "13_engine_config_inert_write.aqscenario.yml",
                self._SEC("spark", "spark.executor.memoryOverhead", "1g"),
            )
            == []
        )

    def test_13_fails_on_the_inert_echo_and_on_a_denied_key(self):
        inert = self._verdict(
            "13_engine_config_inert_write.aqscenario.yml",
            self._SEC("spark", "spark.sql.shuffle.partitions", 200),
        )
        assert inert, "echoing the already-configured value must not pass"
        policy = self._verdict(
            "13_engine_config_inert_write.aqscenario.yml",
            self._SEC("spark", "spark.jars", "x.jar"),
        )
        assert policy, "a denied key must not pass"

    # ── config_not_contains / scenario 09 ───────────────────────────────────

    _CLEAN_JOIN = (
        "SELECT o.*, c.name, c.email\nFROM orders_raw o\n"
        "JOIN customers_raw c ON o.customer_id = c.customer_id\n"
    )

    def _set_query(self, sql: str):
        return [
            {
                "op": "set_module_config_key",
                "module_id": "join_enriched",
                "key": "query",
                "value": sql,
            }
        ]

    def test_09_passes_on_the_engine_config_route(self):
        assert (
            self._verdict(
                "09_broadcast_join_timeout.aqscenario.yml",
                self._SEC("spark", "spark.sql.autoBroadcastJoinThreshold", -1),
            )
            == []
        )

    def test_09_passes_on_the_hint_removal_route(self):
        """The route three consecutive live runs took, and which scored FAIL
        until `config_not_contains` existed — a correct patch marked wrong."""
        assert (
            self._verdict(
                "09_broadcast_join_timeout.aqscenario.yml", self._set_query(self._CLEAN_JOIN)
            )
            == []
        )

    def test_09_fails_when_the_hint_survives_in_lower_case(self):
        """Why the matcher is case-insensitive. SQL hint syntax is
        case-tolerant, so a case-SENSITIVE rule would report the hint as
        removed while it is still there — a false PASS in the one direction a
        negative matcher must never fail."""
        failures = self._verdict(
            "09_broadcast_join_timeout.aqscenario.yml",
            self._set_query(
                "SELECT /*+ broadcast(customers_raw) */ o.*, c.name\n"
                "FROM orders_raw o\nJOIN customers_raw c ON o.customer_id = c.customer_id\n"
            ),
        )
        assert failures

    def test_09_fails_when_the_query_is_gutted(self):
        """`config_not_contains` alone would pass for a patch that deleted the
        query outright — every substring is absent from nothing. The paired
        positive expectation is what stops the grader scoring vandalism as a
        fix."""
        failures = self._verdict(
            "09_broadcast_join_timeout.aqscenario.yml", self._set_query("SELECT 1")
        )
        assert failures

    def test_config_not_contains_treats_a_missing_key_as_satisfied(self):
        """Removing the whole key is a stronger form of "this text is gone"."""
        from aqueduct.surveyor.scenario import _check_absent_substrings

        assert _check_absent_substrings("w", {"query": "BROADCAST"}, {}) == []
        assert _check_absent_substrings("w", {"query": "BROADCAST"}, {"query": "SELECT 1"}) == []
        assert _check_absent_substrings(
            "w", {"query": "BROADCAST"}, {"query": "/*+ BROADCAST(t) */"}
        )

    def test_config_not_contains_rejects_an_empty_needle(self):
        """An empty string is in every value, so the assertion could never
        fail — the same vacuous shape the gallery guard bans elsewhere."""
        from aqueduct.surveyor.scenario import _check_absent_substrings

        failures = _check_absent_substrings("w", {"query": ""}, {"query": "SELECT 1"})
        assert len(failures) == 1
        assert "never fail" in failures[0]

    def test_config_not_contains_requires_a_module(self):
        """It grades ONE named module's config, exactly like config_contains,
        so without `module:` there is nothing to grade and the loader must
        say so rather than silently checking nothing."""
        from aqueduct.surveyor.scenario import validate_expected_patch

        errors = validate_expected_patch({"effect": {"config_not_contains": {"query": "X"}}})
        assert errors
        assert any("module" in e and "config_not_contains" in e for e in errors)

    def test_14_passes_on_the_duckdb_typed_field_write(self):
        assert (
            self._verdict(
                "14_duckdb_memory_limit.aqscenario.yml",
                self._SEC("duckdb", "memory_limit", "8GB"),
            )
            == []
        )

    def test_14_fails_when_the_write_targets_the_wrong_engine(self):
        """The engine-agnostic path's own failure mode: a perfectly valid
        Spark write scores nothing for a DuckDB scenario."""
        failures = self._verdict(
            "14_duckdb_memory_limit.aqscenario.yml",
            self._SEC("spark", "spark.executor.memory", "8g"),
        )
        assert any("memory_limit" in f and "did not change" in f for f in failures)

    def test_14_targets_duckdb(self):
        from aqueduct.surveyor.scenario import scenario_engine

        sc = load_scenario(_GALLERY_DIR / "14_duckdb_memory_limit.aqscenario.yml")
        assert scenario_engine(sc) == "duckdb"

    def test_every_gallery_scenario_still_loads(self):
        files = sorted(_GALLERY_DIR.glob("*.aqscenario.yml"))
        assert len(files) >= 14
        for f in files:
            assert load_scenario(f).id
