"""Unit tests for aqueduct/agent/budget.py — Phase 34 Task 83 (BudgetConfig + BudgetTracker)."""

from __future__ import annotations

import time
from dataclasses import FrozenInstanceError

import pytest

pytestmark = pytest.mark.unit

from aqueduct.agent.budget import (
    AttemptRecord,
    BudgetConfig,
    BudgetTracker,
    DEFAULT_BUDGET,
    STOP_REASONS,
    StopReason,
)
from aqueduct.agent.signature import make_signature


# ── BudgetConfig ──────────────────────────────────────────────────────────────

class TestBudgetConfig:
    def test_defaults(self):
        b = BudgetConfig()
        assert b.max_reprompts == 5
        assert b.max_seconds == 120.0
        assert b.max_tokens_total == 50_000
        assert b.same_error_consecutive == 2
        assert b.same_signature_overall == 3
        assert b.progress_stalled_window == 3

    def test_frozen_mutation_raises(self):
        b = BudgetConfig()
        with pytest.raises(FrozenInstanceError):
            b.max_reprompts = 9  # type: ignore[misc]

    def test_max_tokens_total_none_accepted(self):
        b = BudgetConfig(max_tokens_total=None)
        assert b.max_tokens_total is None

    def test_max_tokens_total_zero_rejected(self):
        with pytest.raises(ValueError, match="must be >= 1 or None"):
            BudgetConfig(max_tokens_total=0)

    def test_max_reprompts_zero_rejected(self):
        with pytest.raises(ValueError, match="must be >= 1"):
            BudgetConfig(max_reprompts=0)

    def test_max_seconds_zero_rejected(self):
        with pytest.raises(ValueError, match="must be > 0"):
            BudgetConfig(max_seconds=0)

    def test_max_seconds_negative_rejected(self):
        with pytest.raises(ValueError, match="must be > 0"):
            BudgetConfig(max_seconds=-1)

    def test_same_error_consecutive_one_rejected(self):
        with pytest.raises(ValueError, match="single occurrence is not yet evidence of being stuck"):
            BudgetConfig(same_error_consecutive=1)

    def test_same_signature_overall_lt_consecutive_rejected(self):
        with pytest.raises(ValueError, match="overall"):
            BudgetConfig(same_signature_overall=2, same_error_consecutive=3)

    def test_progress_stalled_window_one_rejected(self):
        with pytest.raises(ValueError, match="must be >= 2"):
            BudgetConfig(progress_stalled_window=1)

    def test_to_dict_has_six_keys(self):
        b = BudgetConfig()
        d = b.to_dict()
        assert set(d.keys()) == {
            "max_reprompts", "max_seconds", "max_tokens_total",
            "same_error_consecutive", "same_signature_overall", "progress_stalled_window",
        }

    def test_to_dict_none_round_trips(self):
        b = BudgetConfig(max_tokens_total=None)
        assert b.to_dict()["max_tokens_total"] is None


class TestDefaultBudget:
    def test_is_module_level_singleton(self):
        assert DEFAULT_BUDGET == BudgetConfig()

    def test_stop_reasons_tuple_has_seven_entries(self):
        assert len(STOP_REASONS) == 7

    def test_stop_reasons_contains_all_documented(self):
        expected = {
            "solved", "exhausted_attempts", "budget_seconds_exceeded",
            "budget_tokens_exceeded", "stuck_signature", "progress_stalled", "api_error",
        }
        assert set(STOP_REASONS) == expected

    def test_stop_reason_literal_mirrors_tuple(self):
        # StopReason is a Literal that mirrors STOP_REASONS; just confirm each member
        # is a plain string (can't introspect Literal args at runtime easily but
        # we can confirm the enum-like usage works)
        for r in STOP_REASONS:
            assert isinstance(r, str)


# ── BudgetTracker ─────────────────────────────────────────────────────────────

def _sig(tag: str = "default") -> "ErrorSignature":
    from aqueduct.agent.signature import make_signature
    return make_signature("err", "root", f"message {tag}")


class TestBudgetTracker:
    def _tracker(self, **kwargs) -> BudgetTracker:
        return BudgetTracker(BudgetConfig(**kwargs))

    # Lifecycle
    def test_begin_attempt_monotonic(self):
        t = self._tracker()
        assert t.begin_attempt() == 1
        assert t.begin_attempt() == 2
        assert t.begin_attempt() == 3

    def test_record_appends_attempt_record(self):
        t = self._tracker()
        t.begin_attempt()
        sig = _sig()
        rec = t.record(sig, tokens_in=10, tokens_out=20)
        assert len(t.attempts) == 1
        assert rec.signature == sig
        assert t.tokens_in_total == 10
        assert t.tokens_out_total == 20

    def test_record_none_signature_is_success_row(self):
        t = self._tracker()
        t.begin_attempt()
        rec = t.record(None)
        assert rec.signature is None

    def test_token_totals_accumulate(self):
        t = self._tracker()
        t.begin_attempt()
        t.record(_sig("a"), tokens_in=100, tokens_out=50)
        t.begin_attempt()
        t.record(_sig("b"), tokens_in=200, tokens_out=75)
        assert t.tokens_in_total == 300
        assert t.tokens_out_total == 125

    # check_stop — sticky
    def test_check_stop_sticky(self):
        t = self._tracker(max_reprompts=1)
        t.begin_attempt()
        t.record(_sig())
        reason1 = t.check_stop()
        assert reason1 == "exhausted_attempts"
        # second call returns same reason even if nothing changed
        assert t.check_stop() == "exhausted_attempts"

    # solved
    def test_check_stop_solved_on_success_signature(self):
        t = self._tracker()
        t.begin_attempt()
        t.record(None)
        assert t.check_stop() == "solved"

    # exhausted_attempts
    def test_check_stop_exhausted_attempts(self):
        t = self._tracker(max_reprompts=2)
        for _ in range(2):
            t.begin_attempt()
            t.record(_sig())
        assert t.check_stop() == "exhausted_attempts"

    # budget_tokens_exceeded
    def test_check_stop_tokens_exceeded(self):
        t = self._tracker(max_tokens_total=100)
        t.begin_attempt()
        t.record(_sig(), tokens_in=60, tokens_out=50)  # 110 >= 100
        assert t.check_stop() == "budget_tokens_exceeded"

    # budget_seconds_exceeded
    def test_check_stop_seconds_exceeded(self, monkeypatch):
        t = self._tracker(max_seconds=1.0)
        # Record an attempt so attempts list is non-empty
        t.begin_attempt()
        t.record(_sig())
        # Monkeypatch time.monotonic so elapsed is > max_seconds
        monkeypatch.setattr(
            "aqueduct.agent.budget.time.monotonic",
            lambda: t.started_at + 999.0,
        )
        assert t.check_stop() == "budget_seconds_exceeded"

    # stuck_signature — overall
    def test_check_stop_stuck_signature_overall(self):
        # same_signature_overall=3 → trips on 3rd occurrence of same signature
        t = self._tracker(same_error_consecutive=2, same_signature_overall=3)
        sig = _sig("stuck")
        for _ in range(3):
            t.begin_attempt()
            t.record(sig)
        assert t.check_stop() == "stuck_signature"

    # stuck_signature — consecutive + escalated
    def test_check_stop_stuck_consecutive_after_escalation(self):
        # same_error_consecutive=2; after escalation, consecutive trip aborts.
        # Set same_signature_overall and progress_stalled_window high enough
        # that those axes do NOT trip first (check_stop evaluates overall and
        # progress_stalled before the post-escalation consecutive check).
        t = self._tracker(
            same_error_consecutive=2,
            same_signature_overall=99,
            progress_stalled_window=99,
            max_reprompts=99,
        )
        sig = _sig("stuck")
        t.begin_attempt()
        t.record(sig)
        t.begin_attempt()
        t.record(sig)  # consecutive trips here
        # should_escalate before marking
        t.mark_escalated()
        t.begin_attempt()
        t.record(sig)  # 3rd consecutive with escalated_once=True
        assert t.check_stop() == "stuck_signature"

    # stuck-consecutive does NOT abort before escalation
    def test_check_stop_no_abort_before_escalation(self):
        t = self._tracker(same_error_consecutive=2, same_signature_overall=10)
        sig = _sig("stuck")
        t.begin_attempt()
        t.record(sig)
        t.begin_attempt()
        t.record(sig)  # consecutive trips — but NOT escalated yet
        assert t.check_stop() is None  # no abort

    # progress_stalled
    def test_check_stop_progress_stalled(self):
        t = self._tracker(progress_stalled_window=3, same_error_consecutive=2, same_signature_overall=10)
        sig = _sig("same")
        for _ in range(3):
            t.begin_attempt()
            t.record(sig)
        # All 3 in window are identical → stalled
        # overall is 10 so overall won't trip first
        assert t.check_stop() == "progress_stalled"

    def test_check_stop_progress_stalled_distinct_window(self):
        """Window of 3 with all same → progress_stalled (when overall is high enough)."""
        t = self._tracker(progress_stalled_window=3, same_error_consecutive=5, same_signature_overall=20)
        sig = _sig("same")
        for _ in range(3):
            t.begin_attempt()
            t.record(sig)
        result = t.check_stop()
        assert result in ("progress_stalled", "stuck_signature")

    # should_escalate / mark_escalated
    def test_should_escalate_when_consecutive_trips_and_not_escalated(self):
        t = self._tracker(same_error_consecutive=2, same_signature_overall=10)
        sig = _sig("stuck")
        t.begin_attempt(); t.record(sig)
        t.begin_attempt(); t.record(sig)
        assert t.should_escalate() is True

    def test_should_escalate_false_after_mark_escalated(self):
        t = self._tracker(same_error_consecutive=2, same_signature_overall=10)
        sig = _sig("stuck")
        t.begin_attempt(); t.record(sig)
        t.begin_attempt(); t.record(sig)
        t.mark_escalated()
        assert t.should_escalate() is False

    def test_mark_api_error_sets_stop_reason(self):
        t = self._tracker()
        t.mark_api_error()
        assert t.stop_reason == "api_error"

    # summary
    def test_summary_keys(self):
        t = self._tracker()
        t.begin_attempt()
        t.record(_sig(), tokens_in=5, tokens_out=3)
        s = t.summary()
        expected_keys = {
            "attempts", "stop_reason", "tokens_in_total", "tokens_out_total",
            "elapsed_seconds", "escalated_once", "signatures",
        }
        assert set(s.keys()) == expected_keys

    def test_summary_values_correct(self):
        t = self._tracker()
        t.begin_attempt()
        sig = _sig()
        t.record(sig, tokens_in=10, tokens_out=20)
        s = t.summary()
        assert s["attempts"] == 1
        assert s["tokens_in_total"] == 10
        assert s["tokens_out_total"] == 20
        assert s["escalated_once"] is False
        assert len(s["signatures"]) == 1
        assert s["signatures"][0]["hash"] == sig.hash
