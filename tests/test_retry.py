"""Unit tests for retry helpers in aqueduct/executor/executor.py."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from aqueduct.executor.spark.executor import _backoff_seconds, _is_retriable, _with_retry
from aqueduct.parser.models import RetryPolicy


def _policy(**kwargs) -> RetryPolicy:
    """Build a RetryPolicy with test-safe defaults (no jitter, short base)."""
    defaults = dict(
        max_attempts=3,
        backoff_strategy="exponential",
        backoff_base_seconds=10,
        backoff_max_seconds=600,
        jitter=False,
        on_exhaustion="abort",
        transient_errors=(),
        non_transient_errors=(),
        deadline_seconds=None,
    )
    defaults.update(kwargs)
    return RetryPolicy(**defaults)


# ── Schema round-trip ─────────────────────────────────────────────────────────


class TestRetryPolicySchema:
    def test_deadline_seconds_round_trips(self, tmp_path):
        from aqueduct.parser.parser import parse

        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            "retry_policy:\n  deadline_seconds: 3600\n"
            "modules:\n  - id: m\n    type: Channel\n    label: M\n"
            "edges: []\n"
        )
        bp = parse(bp_file)
        assert bp.retry_policy.deadline_seconds == 3600

    def test_deadline_seconds_default_is_none(self, tmp_path):
        from aqueduct.parser.parser import parse

        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            "modules:\n  - id: m\n    type: Channel\n    label: M\n"
            "edges: []\n"
        )
        bp = parse(bp_file)
        assert bp.retry_policy.deadline_seconds is None


# ── _is_retriable ─────────────────────────────────────────────────────────────


class TestIsRetriable:
    def test_both_empty_always_retriable(self):
        assert _is_retriable(RuntimeError("anything"), _policy()) is True

    def test_transient_nonempty_matching_retriable(self):
        policy = _policy(transient_errors=("timeout", "connection"))
        assert _is_retriable(RuntimeError("connection reset"), policy) is True

    def test_transient_nonempty_non_matching_not_retriable(self):
        policy = _policy(transient_errors=("timeout",))
        assert _is_retriable(RuntimeError("permission denied"), policy) is False

    def test_non_transient_blocks_even_if_transient_matches(self):
        policy = _policy(transient_errors=("conn",), non_transient_errors=("fatal",))
        assert _is_retriable(RuntimeError("fatal conn error"), policy) is False

    def test_non_transient_blocks_with_empty_transient_list(self):
        policy = _policy(non_transient_errors=("OOM",))
        assert _is_retriable(RuntimeError("OOM killed"), policy) is False

    def test_non_transient_not_matching_falls_through_to_true(self):
        policy = _policy(non_transient_errors=("OOM",))
        assert _is_retriable(RuntimeError("timeout"), policy) is True


# ── _backoff_seconds ──────────────────────────────────────────────────────────


class TestBackoffSeconds:
    def test_exponential_attempt_0(self):
        p = _policy(backoff_strategy="exponential", backoff_base_seconds=10)
        assert _backoff_seconds(0, p) == pytest.approx(10.0)

    def test_exponential_attempt_1(self):
        p = _policy(backoff_strategy="exponential", backoff_base_seconds=10)
        assert _backoff_seconds(1, p) == pytest.approx(20.0)

    def test_exponential_attempt_2(self):
        p = _policy(backoff_strategy="exponential", backoff_base_seconds=10)
        assert _backoff_seconds(2, p) == pytest.approx(40.0)

    def test_linear_attempt_0(self):
        p = _policy(backoff_strategy="linear", backoff_base_seconds=5)
        assert _backoff_seconds(0, p) == pytest.approx(5.0)

    def test_linear_attempt_1(self):
        p = _policy(backoff_strategy="linear", backoff_base_seconds=5)
        assert _backoff_seconds(1, p) == pytest.approx(10.0)

    def test_linear_attempt_2(self):
        p = _policy(backoff_strategy="linear", backoff_base_seconds=5)
        assert _backoff_seconds(2, p) == pytest.approx(15.0)

    def test_fixed_all_attempts_same(self):
        p = _policy(backoff_strategy="fixed", backoff_base_seconds=7)
        for attempt in range(5):
            assert _backoff_seconds(attempt, p) == pytest.approx(7.0)

    def test_cap_enforced(self):
        p = _policy(
            backoff_strategy="exponential",
            backoff_base_seconds=100,
            backoff_max_seconds=200,
        )
        # 100 * 2^2 = 400, capped at 200
        assert _backoff_seconds(2, p) == pytest.approx(200.0)

    def test_jitter_false_exact_value(self):
        p = _policy(backoff_strategy="fixed", backoff_base_seconds=10, jitter=False)
        assert _backoff_seconds(0, p) == pytest.approx(10.0)

    def test_jitter_true_in_expected_range(self):
        p = _policy(backoff_strategy="fixed", backoff_base_seconds=10, jitter=True)
        for _ in range(30):
            result = _backoff_seconds(0, p)
            assert 5.0 <= result <= 10.0 + 1e-9


# ── _with_retry ───────────────────────────────────────────────────────────────


class TestWithRetry:
    def test_success_first_attempt_no_sleep(self):
        p = _policy(max_attempts=3)
        with patch("aqueduct.executor.spark.executor.time.sleep") as mock_sleep:
            result = _with_retry(lambda: 42, p, "mod")
        assert result == 42
        mock_sleep.assert_not_called()

    def test_fail_then_succeed_returns_result(self):
        p = _policy(max_attempts=3)
        calls = []

        def fn():
            calls.append(1)
            if len(calls) < 2:
                raise RuntimeError("transient")
            return "ok"

        with patch("aqueduct.executor.spark.executor.time.sleep"):
            result = _with_retry(fn, p, "mod")
        assert result == "ok"
        assert len(calls) == 2

    def test_all_attempts_exhausted_raises_last_exception(self):
        p = _policy(max_attempts=3)
        calls = []

        def fn():
            calls.append(1)
            raise RuntimeError("always fails")

        with patch("aqueduct.executor.spark.executor.time.sleep"):
            with pytest.raises(RuntimeError, match="always fails"):
                _with_retry(fn, p, "mod")
        assert len(calls) == 3

    def test_non_retriable_bails_after_one_attempt(self):
        p = _policy(max_attempts=5, non_transient_errors=("fatal",))
        calls = []

        def fn():
            calls.append(1)
            raise RuntimeError("fatal error — no retry")

        with patch("aqueduct.executor.spark.executor.time.sleep"):
            with pytest.raises(RuntimeError):
                _with_retry(fn, p, "mod")
        assert len(calls) == 1

    def test_deadline_exceeded_stops_retry(self):
        p = _policy(max_attempts=10, deadline_seconds=5)

        def always_fails():
            raise RuntimeError("deadline test")

        # attempt 0: monotonic → 0.0 (first_failure_at=0.0, elapsed=0 < 5, sleep)
        # attempt 1: monotonic → 10.0 (elapsed=10 >= 5, break)
        with patch("aqueduct.executor.spark.executor.time.monotonic", side_effect=[0.0, 10.0]):
            with patch("aqueduct.executor.spark.executor.time.sleep"):
                with pytest.raises(RuntimeError, match="deadline test"):
                    _with_retry(always_fails, p, "mod")


# ── _module_retry_policy ──────────────────────────────────────────────────────


class TestModuleRetryPolicy:
    def test_no_on_failure_returns_manifest_policy(self):
        from aqueduct.executor.spark.executor import _module_retry_policy

        manifest_policy = _policy(max_attempts=5)
        module = _make_module("m", on_failure=None)
        result = _module_retry_policy(module, manifest_policy)
        assert result is manifest_policy

    def test_valid_on_failure_overrides_policy(self):
        from aqueduct.executor.spark.executor import _module_retry_policy

        manifest_policy = _policy(max_attempts=1)
        module = _make_module(
            "m",
            on_failure={"max_attempts": 4, "backoff_strategy": "fixed"},
        )
        result = _module_retry_policy(module, manifest_policy)
        assert result.max_attempts == 4
        assert result.backoff_strategy == "fixed"

    def test_invalid_on_failure_key_raises_execute_error(self):
        from aqueduct.executor.spark.executor import ExecuteError, _module_retry_policy

        module = _make_module("m", on_failure={"not_a_real_field": 99})
        with pytest.raises(ExecuteError, match="invalid keys"):
            _module_retry_policy(module, _policy())


def _make_module(mid: str, **kwargs):
    from aqueduct.parser.models import Module

    return Module(id=mid, type="Ingress", label=mid, config={}, **kwargs)
