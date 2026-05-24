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
             patch("aqueduct.surveyor.scenario._try_apply_patch", return_value=(True, "", None, {})):
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
            operations=[{"op": "replace_module_label", "module_id": "src", "label": "New Label"}]
        )
        
        mock_result = MagicMock()
        mock_result.patch = patch_obj
        mock_result.attempts = 1
        mock_result.reprompt_errors = []

        with patch("aqueduct.agent.generate_agent_patch", return_value=mock_result), \
             patch("aqueduct.surveyor.scenario._try_apply_patch", return_value=(True, "", None, {"modules": [{"id": "src", "config": {"path": "/wrong/path"}}]})):
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
            patch_id="fix-1", rationale="test",
            operations=[{"op": "remove_module", "module_id": "src"}]
        )
        mock_result = MagicMock()
        mock_result.patch = patch_obj
        mock_result.attempts = 1
        mock_result.reprompt_errors = []

        # Return a non-None violated_guardrails from _try_apply_patch
        with patch("aqueduct.agent.generate_agent_patch", return_value=mock_result), \
             patch("aqueduct.surveyor.scenario._try_apply_patch", return_value=(False, "violated", ["replace_module_config"], None)):
            result = run_scenario(
                scenario, model="claude-3", patches_dir=tmp_path / "patches"
            )

        assert result.violated_guardrails == ["replace_module_config"]



# ── format_benchmark_table ────────────────────────────────────────────────────

def _make_result(scenario_id: str, model: str, *, passed: bool = True,
                 confidence: float | None = 0.9, patch_valid: bool = True,
                 patch_applies: bool = True, violated_guardrails: list[str] | None = None) -> ScenarioResult:
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
        violated_guardrails=violated_guardrails
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
            patch_id="test",
            rationale="test",
            operations=[{"op": op, "module_id": "src", **kwargs}]
        )

    def test_no_guardrails_block_returns_none(self, tmp_path):
        from aqueduct.surveyor.scenario import _try_apply_patch
        bp_path = self._create_bp(tmp_path)
        patch = self._make_patch("set_module_config_key", key="path", value="/new")
        
        success, err, violated, patched_dict = _try_apply_patch(patch, bp_path)
        assert success is True
        assert violated is None
        assert patched_dict is not None
        assert patched_dict["modules"][0]["config"]["path"] == "/new"

    def test_defined_and_clean_returns_empty_list(self, tmp_path):
        from aqueduct.surveyor.scenario import _try_apply_patch
        bp_path = self._create_bp(tmp_path, "forbidden_ops: [replace_module_config]")
        patch = self._make_patch("set_module_config_key", key="path", value="/new")
        
        success, err, violated, patched_dict = _try_apply_patch(patch, bp_path)
        assert success is True
        assert violated == []
        assert patched_dict is not None

    def test_forbidden_ops_violation(self, tmp_path):
        from aqueduct.surveyor.scenario import _try_apply_patch
        bp_path = self._create_bp(tmp_path, "forbidden_ops: [replace_module_config]")
        patch = self._make_patch("replace_module_config", config={"path": "/new"})
        
        success, err, violated, patched_dict = _try_apply_patch(patch, bp_path)
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
        
        success, err, violated, patched_dict = _try_apply_patch(patch, bp_path)
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
            operations=[{"op": "set_module_config_key", "module_id": "src", "key": "format", "value": ["a list"]}]
        )
        
        success, err, violated, patched_dict = _try_apply_patch(patch, bp_path)
        assert success is False
        assert "ParseError" in err or "validation" in err.lower() or "unhashable" in err.lower() or "list" in err.lower()
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
        failures = self._call({
            "effect": {
                "module": "clean_events",
                "config_contains": {"query": "event_time"},
            }
        })
        assert failures == []

    def test_sql_key_substring_absent_reports_failure(self):
        failures = self._call({
            "effect": {
                "module": "clean_events",
                "config_contains": {"query": "nonexistent_column"},
            }
        })
        assert len(failures) == 1
        assert "nonexistent_column" in failures[0]
        assert "AST-normalized" in failures[0] or "normalized" in failures[0].lower()

    def test_non_sql_key_substring_present_no_failures(self):
        failures = self._call({
            "effect": {
                "module": "clean_events",
                "config_contains": {"path": "events"},
            }
        })
        assert failures == []

    def test_non_sql_key_substring_absent_reports_failure(self):
        failures = self._call({
            "effect": {
                "module": "clean_events",
                "config_contains": {"path": "nonexistent/path"},
            }
        })
        assert len(failures) == 1
        assert "nonexistent/path" in failures[0]
        assert "AST-normalized" not in failures[0]

    def test_bool_true_strict_equality_pass(self):
        failures = self._call({
            "effect": {"module": "clean_events", "config_contains": {"header": True}}
        })
        assert failures == []

    def test_int_strict_equality_pass(self):
        failures = self._call({
            "effect": {"module": "clean_events", "config_contains": {"max_rows": 1000}}
        })
        assert failures == []

    def test_bool_wrong_value_fails(self):
        failures = self._call({
            "effect": {"module": "clean_events", "config_contains": {"header": False}}
        })
        assert len(failures) == 1
        assert "header" in failures[0]

    def test_patched_dict_none_skips_grader(self):
        from aqueduct.surveyor.scenario import _check_expected_effect
        failures = _check_expected_effect(
            {"effect": {"module": "clean_events", "config_contains": {"query": "event_time"}}},
            None,
        )
        assert failures == []

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
        from aqueduct.agent import _build_guardrails_section
        from aqueduct.parser.parser import parse
        bp_path = _GALLERY_DIR / "blueprints" / "06_guardrail_forbidden_op.yml"
        bp = parse(str(bp_path))
        section = _build_guardrails_section(bp.agent.guardrails)
        assert "replace_module_config" in section
        assert "forbidden" in section.lower()
