"""Unit tests for aqueduct/patch/explain_gate.py — Gate 4 plan-regression logic.

No Spark needed: `capture_plan_snapshot` / `run_explain_gate` operate on plain
dicts and strings. These tests cover the Spark-Connect honesty fix — an empty
captured plan (both `_jdf` paths failed) must report "unavailable," not a
false regression against a real baseline.
"""

from __future__ import annotations

import pytest

from aqueduct.patch.explain_gate import (
    _count_markers,
    capture_plan_snapshot,
    run_explain_gate,
)

pytestmark = pytest.mark.unit


def test_count_markers_empty_plan_is_zero_tuple():
    assert _count_markers("") == (0, 0, 0)


def test_capture_plan_snapshot_marks_unavailable_on_empty_plan():
    """When `_formatted_plan` can't extract anything (e.g. Connect), the
    snapshot must flag plan_available=False instead of just zero counts."""

    class _FailingDf:
        # No _jdf attribute at all — mirrors a Spark Connect DataFrame.
        pass

    snap = capture_plan_snapshot(_FailingDf())
    assert snap["plan_available"] is False
    assert snap["exchange_count"] == 0
    assert snap["plan_text"] == ""


def test_capture_plan_snapshot_available_when_plan_present():
    class _Jdf:
        def queryExecution(self):
            class _QE:
                def toString(self):
                    return "== Physical Plan ==\nExchange hashpartitioning\n"

            return _QE()

    class _Df:
        _jdf = _Jdf()

        @property
        def sparkSession(self):
            raise AttributeError("force fallback path")

    snap = capture_plan_snapshot(_Df())
    assert snap["plan_available"] is True
    assert snap["exchange_count"] >= 1


def test_run_explain_gate_skip_when_all_plans_unavailable():
    """If every touched module's plan capture failed, the gate must report
    skip — not compare all-zero counts against a real baseline (which used
    to read as a false 'lost broadcast hint' regression)."""
    baseline = {
        "m1": {
            "exchange_count": 2,
            "python_udf_count": 0,
            "broadcast_count": 1,
            "run_id": "run-1",
        },
    }
    after = {
        "m1": {
            "exchange_count": 0,
            "python_udf_count": 0,
            "broadcast_count": 0,
            "plan_text": "",
            "plan_available": False,
        },
    }
    result = run_explain_gate(baseline, after, touched_modules=["m1"])
    assert result.status == "skip"
    assert "unavailable" in result.detail
    assert result.regressions == []


def test_run_explain_gate_partial_unavailable_still_compares_rest():
    baseline = {
        "m1": {"exchange_count": 1, "python_udf_count": 0, "broadcast_count": 1, "run_id": "run-1"},
        "m2": {"exchange_count": 1, "python_udf_count": 0, "broadcast_count": 1, "run_id": "run-1"},
    }
    after = {
        "m1": {"exchange_count": 0, "python_udf_count": 0, "broadcast_count": 0, "plan_available": False},
        "m2": {"exchange_count": 1, "python_udf_count": 0, "broadcast_count": 1, "plan_available": True},
    }
    result = run_explain_gate(baseline, after, touched_modules=["m1", "m2"])
    assert result.status == "pass"
    assert "skipped" in result.detail


def test_run_explain_gate_missing_plan_available_key_defaults_true():
    """Older explain_snapshot rows / callers that predate this flag must
    still be compared (back-compat), not silently skipped."""
    baseline = {
        "m1": {"exchange_count": 1, "python_udf_count": 0, "broadcast_count": 1, "run_id": "run-1"},
    }
    after = {
        "m1": {"exchange_count": 3, "python_udf_count": 0, "broadcast_count": 1},
    }
    result = run_explain_gate(baseline, after, touched_modules=["m1"])
    assert result.status == "warn"
    assert len(result.regressions) == 1


def test_run_explain_gate_real_regression_still_warns():
    baseline = {
        "m1": {"exchange_count": 1, "python_udf_count": 0, "broadcast_count": 1, "run_id": "run-1"},
    }
    after = {
        "m1": {"exchange_count": 1, "python_udf_count": 0, "broadcast_count": 0, "plan_available": True},
    }
    result = run_explain_gate(baseline, after, touched_modules=["m1"])
    assert result.status == "warn"
    assert result.regressions[0].metric == "broadcast"
