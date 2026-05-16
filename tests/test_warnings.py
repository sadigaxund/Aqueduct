"""Tests for aqueduct/warnings.py — AqueductWarning, emit(), suppression, CLI formatter."""

from __future__ import annotations

import warnings

import pytest

pytestmark = pytest.mark.unit

from aqueduct import warnings as aqw


@pytest.fixture(autouse=True)
def _reset_warnings_globals():
    """warnings.py holds process-global state — reset it around every test."""
    saved_suppress = set(aqw._DEFAULT_SUPPRESS)
    saved_silence = aqw._DEFAULT_SILENCE_ALL
    saved_installed = aqw._INSTALLED
    saved_formatwarning = warnings.formatwarning
    aqw.set_default_suppress([], silence_all=False)
    yield
    aqw._DEFAULT_SUPPRESS = saved_suppress
    aqw._DEFAULT_SILENCE_ALL = saved_silence
    aqw._INSTALLED = saved_installed
    warnings.formatwarning = saved_formatwarning


# ── AqueductWarning category ──────────────────────────────────────────────────

def test_aqueduct_warning_subclasses_user_warning():
    assert issubclass(aqw.AqueductWarning, UserWarning)


# ── emit() basic ──────────────────────────────────────────────────────────────

def test_emit_warns_with_category_and_prefix():
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        aqw.emit("perf_8c", "slow query detected")
    assert len(rec) == 1
    assert rec[0].category is aqw.AqueductWarning
    assert str(rec[0].message) == "[aqueduct:perf_8c] slow query detected"


# ── emit() explicit suppress ──────────────────────────────────────────────────

def test_emit_explicit_suppress_match_is_noop():
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        aqw.emit("perf_8c", "msg", suppress={"perf_8c"})
    assert rec == []


def test_emit_explicit_suppress_no_match_still_warns():
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        aqw.emit("perf_8c", "msg", suppress={"other_rule"})
    assert len(rec) == 1


# ── set_default_suppress() ────────────────────────────────────────────────────

def test_default_suppress_makes_emit_noop_without_explicit_arg():
    aqw.set_default_suppress(["kafka_checkpoint_stale"])
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        aqw.emit("kafka_checkpoint_stale", "msg")
    assert rec == []


def test_silence_all_silences_every_emit():
    aqw.set_default_suppress([], silence_all=True)
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        aqw.emit("any_rule_id", "msg")
        aqw.emit("another", "msg2")
    assert rec == []


def test_explicit_suppress_arg_takes_priority_over_default():
    # Default suppresses rule_A; explicit suppress only lists rule_B.
    # Explicit arg wins → rule_A is NOT in active set → still warns.
    aqw.set_default_suppress(["rule_A"])
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        aqw.emit("rule_A", "msg", suppress={"rule_B"})
    assert len(rec) == 1


# ── emit() never raises ───────────────────────────────────────────────────────

def test_emit_never_raises_on_internal_error(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("internal failure")

    monkeypatch.setattr(aqw._w, "warn", boom)
    # Must not propagate
    aqw.emit("rule", "msg")


# ── install_cli_formatter() ───────────────────────────────────────────────────

def test_install_cli_formatter_idempotent():
    aqw._INSTALLED = False
    aqw.install_cli_formatter()
    first = warnings.formatwarning
    aqw.install_cli_formatter()  # second call → no-op
    assert warnings.formatwarning is first
    assert aqw._INSTALLED is True


def test_cli_formatter_renders_aq_warn_for_aqueduct_warning():
    aqw._INSTALLED = False
    aqw.install_cli_formatter()
    out = warnings.formatwarning(
        "[aqueduct:perf_8c] slow query", aqw.AqueductWarning, "f.py", 10
    )
    assert out == "AQ-WARN [perf_8c] slow query\n"


def test_cli_formatter_keeps_default_for_non_aqueduct_warning():
    aqw._INSTALLED = False
    aqw.install_cli_formatter()
    out = warnings.formatwarning("deprecated thing", DeprecationWarning, "f.py", 20)
    assert "AQ-WARN" not in out
    assert "deprecated thing" in out
