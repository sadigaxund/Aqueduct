"""Phase 85 F-15 — unify the three warning-emission paths under one suppress set.

Before this change only Path A (`aqueduct.warnings.emit`, `[aqueduct:rule_id]`
via `warnings.warn`) respected `warnings.suppress`. Path B
(`executor/models.py::_add_module_warning`, feeding `ModuleResult.warnings`,
rendered nested by `run.py`) and Path C (the `runtime_retry_*` family in
`executor/spark/executor.py` / `executor/duckdb_/executor.py`, previously bare
`logger.warning` calls) silently ignored it.

These tests exercise Path B and Path C directly — no Spark/DuckDB session
required, since `_add_module_warning`/`_with_retry` are pure Python — so they
run under `-m "not spark"`.
"""

from __future__ import annotations

import logging

import pytest

from aqueduct.executor.duckdb_.executor import _with_retry
from aqueduct.executor.models import _add_module_warning, _collect_module_warnings
from aqueduct.models import RetryPolicy
from aqueduct.warnings import set_default_suppress

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _reset_suppress():
    """`_DEFAULT_SUPPRESS` is a process-global — never leak between tests."""
    set_default_suppress(None)
    _collect_module_warnings()  # drain anything left on this thread
    yield
    set_default_suppress(None)
    _collect_module_warnings()


def _always_fails():
    raise ValueError("boom")


def test_probe_assert_rule_suppressed_never_enters_module_result():
    """Path B: a probe/assert rule_id in `warnings.suppress` must not reach
    `ModuleResult.warnings` at all — not merely be hidden when rendered."""
    set_default_suppress(["runtime_probe_blocked"])
    _add_module_warning("runtime_probe_blocked", "Probe 'p1': blocked")
    assert _collect_module_warnings() == ()


def test_probe_assert_rule_unsuppressed_appears_exactly_once():
    """An unsuppressed Path B warning must still reach the collector, and
    exactly once — no double-print regression from the suppression check."""
    set_default_suppress([])
    _add_module_warning("runtime_assert", "Assert [not_null]: col1 has nulls")
    collected = _collect_module_warnings()
    assert collected == (("runtime_assert", "Assert [not_null]: col1 has nulls"),)


def test_runtime_retry_deadline_suppressed_via_config():
    """Path C: `runtime_retry_deadline` was NEVER suppressible before this
    change (bare `logger.warning`, no rule_id lookup). It must be now."""
    set_default_suppress(["runtime_retry_deadline"])
    policy = RetryPolicy(max_attempts=3, deadline_seconds=0)

    with pytest.raises(ValueError):
        _with_retry(_always_fails, policy, "mod_a")

    assert _collect_module_warnings() == ()


def test_runtime_retry_deadline_unsuppressed_reaches_collector_once(caplog):
    """Same scenario, no suppression: the collector gets the warning exactly
    once, AND the logger still carries the full `[rule_id] message` line
    (unaffected — it stays available for `--log-format json` / caplog)."""
    set_default_suppress([])
    policy = RetryPolicy(max_attempts=3, deadline_seconds=0)

    with caplog.at_level(logging.WARNING):
        with pytest.raises(ValueError):
            _with_retry(_always_fails, policy, "mod_b")

    collected = _collect_module_warnings()
    assert len(collected) == 1
    rule_id, message = collected[0]
    assert rule_id == "runtime_retry_deadline"
    assert "mod_b" in message
    assert "not retrying" in message

    retry_records = [r for r in caplog.records if "[runtime_retry_deadline]" in r.getMessage()]
    assert len(retry_records) == 1


def test_runtime_retry_waiting_suppressed_via_config():
    """`runtime_retry_waiting` fires on every non-terminal retry attempt."""
    set_default_suppress(["runtime_retry_waiting"])
    policy = RetryPolicy(
        max_attempts=2,
        backoff_strategy="fixed",
        backoff_base_seconds=0,
        jitter=False,
    )

    with pytest.raises(ValueError):
        _with_retry(_always_fails, policy, "mod_c")

    # `runtime_retry_exhausted` (the final-attempt rule) is NOT suppressed
    # here — only `runtime_retry_waiting` is — so it must still appear.
    collected = _collect_module_warnings()
    assert all(rid != "runtime_retry_waiting" for rid, _ in collected)
    assert any(rid == "runtime_retry_exhausted" for rid, _ in collected)


def test_wildcard_suppresses_all_three_paths():
    """The `'*'` wildcard silences Path A's predicate, Path B, and Path C —
    they all now share `aqueduct.warnings.is_suppressed()`."""
    from aqueduct.warnings import is_suppressed

    set_default_suppress(["*"])

    # Path A's predicate directly.
    assert is_suppressed("anything_at_all")

    # Path B.
    _add_module_warning("runtime_probe_blocked", "msg")
    assert _collect_module_warnings() == ()

    # Path C.
    policy = RetryPolicy(max_attempts=3, deadline_seconds=0)
    with pytest.raises(ValueError):
        _with_retry(_always_fails, policy, "mod_d")
    assert _collect_module_warnings() == ()
