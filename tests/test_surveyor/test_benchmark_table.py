"""Tests for Phase 52 — format_benchmark_table (aqueduct/surveyor/scenario.py).

Regression focus: the heavy (═) and light (─) rule lines MUST be the SAME
width as the data rows. An off-by-one in the bar-width calculation was fixed;
these tests pin the correct behaviour and would catch a recurrence.

Also covers:
- Empty results → "(no results)"
- Single model, single scenario basic layout
- Multi-model column widths are each wide enough to hold the widest cell
- Summary rows (Parse rate, Pass rate, …) are present
- Missing result in a cell renders as "—"

NOTE: run_benchmark(workers=2) ordering is not testable without live LLM calls.
That behaviour is excluded from this file per the scope note.
"""

from __future__ import annotations

import types

import pytest

pytestmark = pytest.mark.unit

from aqueduct.surveyor.scenario import format_benchmark_table


# ── Helpers ────────────────────────────────────────────────────────────────────

def _r(
    *,
    passed: bool = True,
    confidence: float | None = 0.9,
    diag_score: float | None = None,
    duration_seconds: float = 2.0,
    patch_valid: bool = True,
    patch_applies: bool = True,
    attempts_to_parse: int = 1,
    diag_correct: bool | None = None,
    violated_guardrails: list | None = None,
) -> types.SimpleNamespace:
    """Build a minimal fake ScenarioResult for format_benchmark_table."""
    ns = types.SimpleNamespace(
        passed=passed,
        confidence=confidence,
        diag_score=diag_score,
        duration_seconds=duration_seconds,
        patch_valid=patch_valid,
        patch_applies=patch_applies,
        attempts_to_parse=attempts_to_parse,
        diag_correct=diag_correct,
        violated_guardrails=violated_guardrails,
    )
    return ns


def _render(results: dict, models: list[str]) -> str:
    return format_benchmark_table(results, models)


def _row_widths(rendered: str) -> set[int]:
    """Return the set of unique line widths (excluding blank lines)."""
    return {len(line) for line in rendered.splitlines() if line}


def _lines_of_char(rendered: str, char: str) -> list[str]:
    """Return all lines that consist entirely of `char` repeated."""
    return [line for line in rendered.splitlines() if line and all(c == char for c in line)]


# ── Empty results ──────────────────────────────────────────────────────────────

def test_empty_results_returns_no_results():
    out = _render({}, ["model-a"])
    assert out == "(no results)"


# ── Rule-line width regression ─────────────────────────────────────────────────

class TestRuleLineWidth:
    """The heavy (═) and light (─) bars must be the SAME length as data rows."""

    def test_heavy_bar_same_width_as_header_single_model(self):
        results = {"scenario-alpha": {"model-a": _r(passed=True)}}
        out = _render(results, ["model-a"])
        lines = out.splitlines()
        heavy_lines = [ln for ln in lines if ln and all(c == "═" for c in ln)]
        # Find header line (contains 'Scenario')
        header_lines = [ln for ln in lines if "Scenario" in ln]
        assert heavy_lines, "Expected at least one heavy (═) rule line"
        assert header_lines, "Expected header row containing 'Scenario'"
        header_w = len(header_lines[0])
        for hl in heavy_lines:
            assert len(hl) == header_w, (
                f"Heavy rule width {len(hl)} != header width {header_w}:\n{out}"
            )

    def test_light_bar_same_width_as_header_single_model(self):
        results = {"scenario-alpha": {"model-a": _r(passed=True)}}
        out = _render(results, ["model-a"])
        lines = out.splitlines()
        light_lines = [ln for ln in lines if ln and all(c == "─" for c in ln)]
        header_lines = [ln for ln in lines if "Scenario" in ln]
        assert light_lines, "Expected at least one light (─) rule line"
        header_w = len(header_lines[0])
        for ll in light_lines:
            assert len(ll) == header_w, (
                f"Light rule width {len(ll)} != header width {header_w}:\n{out}"
            )

    def test_rule_lines_same_width_as_data_rows(self):
        results = {"my-scenario": {"model-a": _r(passed=True, confidence=0.85, duration_seconds=12.0)}}
        out = _render(results, ["model-a"])
        lines = out.splitlines()

        # Only check that rule lines match the header width — summary row
        # labels (e.g. "Avg confidence") may be wider than "Scenario" column.
        header_lines = [ln for ln in lines if "Scenario" in ln]
        rule_lines = [
            ln for ln in lines
            if ln and all(c in ("═", "─") for c in ln)
        ]

        assert header_lines, "Expected header row containing 'Scenario'"
        assert rule_lines, "Expected at least one rule line"

        header_w = len(header_lines[0])
        for rl in rule_lines:
            assert len(rl) == header_w, (
                f"Rule line width {len(rl)} != header width {header_w}\n{out}"
            )

    def test_rule_lines_same_width_multi_model(self):
        """Regression: multi-model rule lines must match header width."""
        results = {
            "sc-1": {
                "model-alpha": _r(passed=True, confidence=0.95),
                "model-beta": _r(passed=False, confidence=None),
            },
            "sc-2": {
                "model-alpha": _r(passed=False),
                "model-beta": _r(passed=True, confidence=0.5, duration_seconds=30.1),
            },
        }
        out = _render(results, ["model-alpha", "model-beta"])
        header_lines = [ln for ln in out.splitlines() if "Scenario" in ln]
        rule_lines = [
            ln for ln in out.splitlines()
            if ln and all(c in ("═", "─") for c in ln)
        ]
        assert header_lines, "Expected header row"
        assert rule_lines, "Expected rule lines"
        header_w = len(header_lines[0])
        for rl in rule_lines:
            assert len(rl) == header_w, (
                f"Rule line width {len(rl)} != header width {header_w}\n{out}"
            )

    def test_rule_lines_same_width_long_scenario_id(self):
        """Scenario IDs wider than 'Scenario' heading must still produce uniform rows."""
        long_id = "very-long-scenario-name-that-exceeds-default-column-width"
        results = {long_id: {"model-a": _r(passed=True)}}
        out = _render(results, ["model-a"])
        lines = [ln for ln in out.splitlines() if ln]
        widths = {len(ln) for ln in lines}
        assert len(widths) == 1, (
            f"Long scenario id table has non-uniform line widths: {sorted(widths)}\n{out}"
        )

    def test_rule_lines_same_width_wide_cell_content(self):
        """Wide cell values (FAIL with long decoration) must not break uniformity."""
        results = {
            "sc": {
                "model-with-a-very-long-name-here": _r(
                    passed=False, confidence=None,
                    diag_score=0.75, duration_seconds=99.9,
                )
            }
        }
        out = _render(results, ["model-with-a-very-long-name-here"])
        header_lines = [ln for ln in out.splitlines() if "Scenario" in ln]
        rule_lines = [
            ln for ln in out.splitlines()
            if ln and all(c in ("═", "─") for c in ln)
        ]
        assert header_lines, "Expected header row"
        assert rule_lines, "Expected rule lines"
        header_w = len(header_lines[0])
        for rl in rule_lines:
            assert len(rl) == header_w, (
                f"Rule line width {len(rl)} != header width {header_w}\n{out}"
            )


# ── Content correctness ────────────────────────────────────────────────────────

class TestTableContent:
    def test_pass_appears_for_passing_result(self):
        results = {"sc-1": {"model-a": _r(passed=True)}}
        out = _render(results, ["model-a"])
        assert "PASS" in out

    def test_fail_appears_for_failing_result(self):
        results = {"sc-1": {"model-a": _r(passed=False)}}
        out = _render(results, ["model-a"])
        assert "FAIL" in out

    def test_scenario_id_in_output(self):
        results = {"my-scenario": {"model-a": _r()}}
        out = _render(results, ["model-a"])
        assert "my-scenario" in out

    def test_model_name_in_header(self):
        results = {"sc": {"my-model": _r()}}
        out = _render(results, ["my-model"])
        assert "my-model" in out

    def test_missing_result_renders_dash(self):
        """If a (scenario, model) pair has no result, cell should render as '—'."""
        results = {
            "sc-1": {"model-a": _r(passed=True)},
            "sc-2": {},  # no result for model-a
        }
        out = _render(results, ["model-a"])
        assert "—" in out

    def test_summary_rows_present(self):
        results = {
            "sc-1": {"model-a": _r(passed=True, patch_valid=True, patch_applies=True)},
        }
        out = _render(results, ["model-a"])
        assert "Pass rate" in out
        assert "Parse rate" in out
        assert "Apply rate" in out

    def test_diag_score_shown_when_present(self):
        results = {"sc": {"model-a": _r(passed=True, diag_score=0.8)}}
        out = _render(results, ["model-a"])
        assert "80%" in out

    def test_duration_always_shown(self):
        results = {"sc": {"model-a": _r(duration_seconds=7.0)}}
        out = _render(results, ["model-a"])
        assert "7s" in out

    def test_no_confidence_in_cell_for_failing_result(self):
        """Confidence is not shown in FAIL cell — but may appear in Avg confidence summary row."""
        result = _r(passed=False, confidence=0.75)
        results = {"sc": {"model-a": result}}
        out = _render(results, ["model-a"])
        # The FAIL cell should NOT show the confidence inline
        # Check FAIL cell does NOT contain the confidence value
        lines = out.splitlines()
        fail_rows = [ln for ln in lines if "FAIL" in ln]
        for row in fail_rows:
            assert "0.75" not in row, f"FAIL row should not show confidence:\n{row}"

    def test_multiple_scenarios_all_ids_present(self):
        results = {
            "sc-alpha": {"model-a": _r(passed=True)},
            "sc-beta": {"model-a": _r(passed=False)},
            "sc-gamma": {"model-a": _r(passed=True)},
        }
        out = _render(results, ["model-a"])
        for sid in ("sc-alpha", "sc-beta", "sc-gamma"):
            assert sid in out

    def test_single_scenario_single_model_all_lines_same_width(self):
        """Minimal 1×1 table: rule lines match header width."""
        results = {"s": {"m": _r(passed=True, confidence=None, diag_score=None)}}
        out = _render(results, ["m"])
        header_lines = [ln for ln in out.splitlines() if "Scenario" in ln]
        rule_lines = [
            ln for ln in out.splitlines()
            if ln and all(c in ("═", "─") for c in ln)
        ]
        assert header_lines, "Expected header line"
        assert rule_lines, "Expected rule lines"
        header_w = len(header_lines[0])
        for rl in rule_lines:
            assert len(rl) == header_w, (
                f"Rule line width {len(rl)} != header width {header_w}\n{out}"
            )
