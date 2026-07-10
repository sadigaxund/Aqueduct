"""Unit tests for retry helpers in aqueduct/executor/executor.py."""

from __future__ import annotations

from unittest.mock import patch

import pytest
pytestmark = [pytest.mark.spark, pytest.mark.integration]

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

    # ── Phase 70 — Blueprint-authored `module.retry` override ─────────────────

    def test_module_retry_used_when_no_on_failure(self):
        from aqueduct.executor.spark.executor import _module_retry_policy

        manifest_policy = _policy(max_attempts=1)
        module_retry = _policy(max_attempts=3)
        module = _make_module("m", retry=module_retry)
        result = _module_retry_policy(module, manifest_policy)
        assert result is module_retry

    def test_neither_on_failure_nor_retry_returns_manifest_policy(self):
        from aqueduct.executor.spark.executor import _module_retry_policy

        manifest_policy = _policy(max_attempts=5)
        module = _make_module("m")
        result = _module_retry_policy(module, manifest_policy)
        assert result is manifest_policy

    def test_on_failure_takes_precedence_over_module_retry(self):
        """on_failure is an LLM-patch write target (heal-time override); it
        beats the Blueprint-authored `retry:` block (authoring-time override)."""
        from aqueduct.executor.spark.executor import _module_retry_policy

        manifest_policy = _policy(max_attempts=1)
        module_retry = _policy(max_attempts=3)
        module = _make_module(
            "m", retry=module_retry, on_failure={"max_attempts": 7},
        )
        result = _module_retry_policy(module, manifest_policy)
        assert result.max_attempts == 7


def _make_module(mid: str, **kwargs):
    from aqueduct.parser.models import Module

    return Module(id=mid, type="Ingress", label=mid, config={}, **kwargs)


class TestOnRetryExhausted:
    """_on_retry_exhausted behavior — executor.py."""

    def _make_policy(self, on_exhaustion: str):
        from aqueduct.parser.models import RetryPolicy
        return RetryPolicy(max_attempts=1, on_exhaustion=on_exhaustion)

    def _make_module(self, mid: str = "m1"):
        from aqueduct.parser.models import Module
        return Module(id=mid, type="Ingress", label="L", config={})

    def test_abort_returns_false_with_fail_result(self):
        from aqueduct.executor.spark.executor import _on_retry_exhausted
        from aqueduct.executor.models import ModuleResult

        policy = self._make_policy("abort")
        module = self._make_module()
        results: list[ModuleResult] = []
        gate_closed, fail_result = _on_retry_exhausted(
            RuntimeError("boom"), policy, module, "pipe.id", "run-1", results
        )
        assert gate_closed is False
        assert fail_result is not None
        assert fail_result.status == "error"
        assert fail_result.trigger_agent is False

    def test_alert_only_returns_true_with_none(self):
        from aqueduct.executor.spark.executor import _on_retry_exhausted
        from aqueduct.executor.models import ModuleResult

        policy = self._make_policy("alert_only")
        module = self._make_module()
        results: list[ModuleResult] = []
        gate_closed, fail_result = _on_retry_exhausted(
            RuntimeError("non-critical"), policy, module, "pipe.id", "run-1", results
        )
        assert gate_closed is True
        assert fail_result is None

    def test_trigger_agent_returns_false_with_trigger_agent_true(self):
        from aqueduct.executor.spark.executor import _on_retry_exhausted
        from aqueduct.executor.models import ModuleResult

        policy = self._make_policy("trigger_agent")
        module = self._make_module()
        results: list[ModuleResult] = []
        gate_closed, fail_result = _on_retry_exhausted(
            RuntimeError("agent needed"), policy, module, "pipe.id", "run-1", results
        )
        assert gate_closed is False
        assert fail_result is not None
        assert fail_result.trigger_agent is True

    def test_abort_records_module_result_as_error(self):
        from aqueduct.executor.spark.executor import _on_retry_exhausted
        from aqueduct.executor.models import ModuleResult

        policy = self._make_policy("abort")
        module = self._make_module("failing_module")
        results: list[ModuleResult] = []
        _on_retry_exhausted(RuntimeError("boom"), policy, module, "pipe.id", "run-1", results)
        assert len(results) == 1
        assert results[0].module_id == "failing_module"
        assert results[0].status == "error"


class TestOnExhaustionAlertOnlyIntegration:
    """alert_only: module fails but blueprint reports success (gate_closed path)."""

    def test_alert_only_blueprint_continues(self, spark, tmp_path):
        from aqueduct.compiler.models import Manifest
        from aqueduct.executor.spark.executor import execute
        from aqueduct.parser.models import Edge, Module, RetryPolicy

        # Non-existent path causes IngressError on module "bad_src"
        # A second Ingress → Egress succeeds
        good_path = str(tmp_path / "good.parquet")
        out_path = str(tmp_path / "out.parquet")
        spark.range(3).write.parquet(good_path)

        policy = RetryPolicy(max_attempts=1, on_exhaustion="alert_only")

        manifest = Manifest(
            blueprint_id="test.alert_only",
            retry_policy=policy,
            modules=(
                Module(
                    id="bad_src", type="Ingress", label="Bad",
                    config={"format": "parquet", "path": "/nonexistent/does_not_exist.parquet"},
                ),
                Module(
                    id="good_src", type="Ingress", label="Good",
                    config={"format": "parquet", "path": good_path},
                ),
                Module(
                    id="sink", type="Egress", label="Sink",
                    config={"format": "parquet", "path": out_path},
                ),
            ),
            edges=(
                Edge(from_id="good_src", to_id="sink", port="main"),
            ),
            context={},
            spark_config={},
        )

        result = execute(manifest, spark)
        # bad_src fails but alert_only propagates GATE_CLOSED — blueprint finishes
        statuses = {r.module_id: r.status for r in result.module_results}
        assert statuses.get("bad_src") == "error"
        # The overall blueprint should not abort with status="error" due to alert_only
        # bad_src has no downstream, so sink is still reachable and succeeds
        assert statuses.get("sink") == "success"
