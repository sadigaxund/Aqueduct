"""Unit tests for aqueduct/surveyor/scenario.py — Phase 22 scenario runner."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.unit

from aqueduct.surveyor.scenario import (
    AqScenario,
    ScenarioResult,
    _check_assertions,
    _check_expected_patch,
    _match_op_spec,
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
        with pytest.raises(ValueError, match="missing or unsupported aqueduct_scenario version"):
            load_scenario(sc)

    def test_unsupported_version_raises(self, tmp_path):
        sc = tmp_path / "bad.aqscenario.yml"
        sc.write_text("aqueduct_scenario: '99.0'\nid: x\ninject_failure: {module: m1}\n")
        with pytest.raises(ValueError, match="missing or unsupported aqueduct_scenario version"):
            load_scenario(sc)

    def test_missing_id_raises(self, tmp_path):
        sc = tmp_path / "bad.aqscenario.yml"
        sc.write_text("aqueduct_scenario: '1.0'\ninject_failure: {module: m1}\n")
        with pytest.raises(ValueError, match="missing 'id'"):
            load_scenario(sc)

    def test_missing_inject_failure_raises(self, tmp_path):
        sc = tmp_path / "bad.aqscenario.yml"
        sc.write_text("aqueduct_scenario: '1.0'\nid: x\n")
        with pytest.raises(ValueError, match="missing 'inject_failure'"):
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


# ── _match_op_spec ────────────────────────────────────────────────────────────

class TestMatchOpSpec:
    def test_exact_key_match_true(self):
        spec = {"op": "set_module_config_key", "module_id": "m1"}
        actual = {"op": "set_module_config_key", "module_id": "m1", "key": "path", "value": "/tmp/x"}
        assert _match_op_spec(spec, actual) is True

    def test_value_contains_substring_true(self):
        spec = {"op": "set_module_config_key", "value_contains": "event_time"}
        actual = {"op": "set_module_config_key", "key": "query", "value": "SELECT event_time FROM t"}
        assert _match_op_spec(spec, actual) is True

    def test_value_contains_substring_false(self):
        spec = {"op": "set_module_config_key", "value_contains": "missing_col"}
        actual = {"op": "set_module_config_key", "value": "SELECT id FROM t"}
        assert _match_op_spec(spec, actual) is False

    def test_partial_spec_only_op_matches_any(self):
        spec = {"op": "replace_module_config"}
        actual = {"op": "replace_module_config", "module_id": "sink", "config": {}}
        assert _match_op_spec(spec, actual) is True

    def test_wrong_field_value_false(self):
        spec = {"op": "set_module_config_key", "module_id": "m2"}
        actual = {"op": "set_module_config_key", "module_id": "m1"}
        assert _match_op_spec(spec, actual) is False

    def test_empty_spec_matches_anything(self):
        assert _match_op_spec({}, {"op": "anything"}) is True


# ── _check_expected_patch ─────────────────────────────────────────────────────

class TestCheckExpectedPatch:
    def _patch_with_ops(self, *ops):
        patch = MagicMock()
        patch.operations = [MagicMock(**{"model_dump.return_value": op}) for op in ops]
        return patch

    def test_all_ops_matched_no_failures(self):
        p = self._patch_with_ops(
            {"op": "set_module_config_key", "module_id": "m1", "key": "path", "value": "/new"},
        )
        expected = {"ops": [{"op": "set_module_config_key", "module_id": "m1"}]}
        failures = _check_expected_patch(p, expected)
        assert failures == []

    def test_unmatched_expected_op_failure_message(self):
        p = self._patch_with_ops({"op": "replace_module_config", "module_id": "m1", "config": {}})
        expected = {"ops": [{"op": "set_module_config_key", "value_contains": "ghost"}]}
        failures = _check_expected_patch(p, expected)
        assert len(failures) == 1
        assert "no generated op matches" in failures[0]
        assert "Generated ops:" in failures[0]

    def test_forbidden_op_present_failure_message(self):
        p = self._patch_with_ops({"op": "replace_module_config", "module_id": "m1", "config": {}})
        expected = {"forbidden_ops": ["replace_module_config"]}
        failures = _check_expected_patch(p, expected)
        assert any("forbidden" in f for f in failures)

    def test_empty_expected_no_failures(self):
        p = self._patch_with_ops({"op": "set_module_config_key"})
        assert _check_expected_patch(p, {}) == []

    def test_expected_patch_module_id_only_matching(self):
        # Spec only has module_id; actual op name is different but module_id matches -> PASS
        p = self._patch_with_ops(
            {"op": "set_module_config_key", "module_id": "src", "key": "path", "value": "/new"},
        )
        expected = {"ops": [{"module_id": "src"}]}
        assert _check_expected_patch(p, expected) == []

        # Spec has module_id; actual op targeting different module -> FAIL
        expected_bad = {"ops": [{"module_id": "other"}]}
        failures = _check_expected_patch(p, expected_bad)
        assert len(failures) == 1
        assert "no generated op matches" in failures[0]


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
        failures, soft_failures, _, _, _, category_match = _check_assertions(
            [{"expected_category": "schema_drift"}], patch=p, blueprint_path=None
        )
        assert category_match is True
        assert failures == []
        assert soft_failures == []

    def test_expected_category_mismatch_fails(self):
        p = _fake_patch(category="format_mismatch")
        failures, soft_failures, _, _, _, category_match = _check_assertions(
            [{"expected_category": "schema_drift"}], patch=p, blueprint_path=None
        )
        assert category_match is False
        assert failures == []
        assert any("expected_category" in f for f in soft_failures)

    def test_root_cause_contains_match_passes(self):
        p = _fake_patch(root_cause="column 'event_ts' was renamed to 'event_time'")
        failures, soft_failures, _, _, root_cause_match, _ = _check_assertions(
            [{"root_cause_contains": "event_time"}], patch=p, blueprint_path=None
        )
        assert root_cause_match is True
        assert failures == []
        assert soft_failures == []

    def test_root_cause_contains_no_match_fails(self):
        p = _fake_patch(root_cause="unrelated error")
        failures, soft_failures, _, _, root_cause_match, _ = _check_assertions(
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
            operations=[{"op": "replace_module_label", "module_id": "src", "label": "New Label"}]
        )
        
        mock_result = MagicMock()
        mock_result.patch = patch_obj
        mock_result.attempts = 1
        mock_result.reprompt_errors = []

        # We mock _try_apply_patch in scenario.py to succeed so patch_applies passes
        with patch("aqueduct.agent.generate_agent_patch", return_value=mock_result), \
             patch("aqueduct.surveyor.scenario._try_apply_patch", return_value=(True, "")):
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
expected_patch:
  ops:
    - op: replace_module_config
      module_id: src
"""
        sc_path = _write_scenario(tmp_path, sc_text)
        scenario = load_scenario(sc_path)

        # Mock agent return to produce a patch with different operations (causes expected_patch to fail)
        from aqueduct.patch.grammar import PatchSpec
        patch_obj = PatchSpec(
            patch_id="fix-1",
            rationale="test",
            confidence=0.9,
            category="other",
            root_cause="test",
            operations=[{"op": "replace_module_label", "module_id": "src", "label": "New Label"}]
        )
        
        mock_result = MagicMock()
        mock_result.patch = patch_obj
        mock_result.attempts = 1
        mock_result.reprompt_errors = []

        with patch("aqueduct.agent.generate_agent_patch", return_value=mock_result):
            result = run_scenario(
                scenario,
                model="claude-3",
                patches_dir=tmp_path / "patches",
            )

        # expected_patch is a hard/gating blocker
        assert result.passed is False
        assert len(result.failures) == 1
        assert "no generated op matches" in result.failures[0]



# ── format_benchmark_table ────────────────────────────────────────────────────

def _make_result(scenario_id: str, model: str, *, passed: bool = True,
                 confidence: float | None = 0.9, patch_valid: bool = True,
                 patch_applies: bool = True) -> ScenarioResult:
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
            }
        }
        table = format_benchmark_table(results, models=["m1"])
        
        # Check cell format for PASS with d50%
        # diag_score=0.5 -> d50%
        assert "PASS" in table
        assert "d50%" in table
        
        # Check cell format for FAIL with d100%
        # diag_score=1.0 -> d100%
        assert "FAIL" in table
        assert "d100%" in table
        
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

