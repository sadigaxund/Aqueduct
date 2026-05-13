"""Tests for _check_heal_guardrails() in aqueduct/cli.py."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.unit

from aqueduct.cli import _check_heal_guardrails


def _make_ctx(error_type=None, stack_trace=None):
    ctx = MagicMock()
    ctx.error_type = error_type
    ctx.stack_trace = stack_trace
    return ctx


def _make_guardrails(heal_on=(), never_heal=()):
    g = MagicMock()
    g.heal_on_errors = list(heal_on)
    g.never_heal_errors = list(never_heal)
    return g


# ── parse round-trip ──────────────────────────────────────────────────────────

def test_heal_on_errors_never_heal_parse_from_yaml(tmp_path):
    """heal_on_errors + never_heal_errors parse from YAML → GuardrailsConfig fields populated."""
    import yaml
    from aqueduct.parser.schema import GuardrailsSchema

    data = {
        "heal_on_errors": ["EmptyDataset", "SchemaError"],
        "never_heal_errors": ["OutOfMemory"],
    }
    g = GuardrailsSchema(**data)
    assert list(g.heal_on_errors) == ["EmptyDataset", "SchemaError"]
    assert list(g.never_heal_errors) == ["OutOfMemory"]


# ── never_heal_errors takes priority ─────────────────────────────────────────

def test_never_heal_blocks_on_error_type_match():
    ctx = _make_ctx(error_type="EmptyDataset")
    g = _make_guardrails(heal_on=["EmptyDataset"], never_heal=["EmptyDataset"])
    should_heal, reason = _check_heal_guardrails(ctx, g)
    assert not should_heal
    assert "EmptyDataset" in reason


def test_never_heal_blocks_on_stack_class():
    ctx = _make_ctx(stack_trace="Traceback (most recent call last):\n  SparkException: oops")
    g = _make_guardrails(never_heal=["SparkException"])
    should_heal, reason = _check_heal_guardrails(ctx, g)
    assert not should_heal
    assert "SparkException" in reason


def test_never_heal_priority_over_heal_on():
    ctx = _make_ctx(error_type="Recoverable")
    g = _make_guardrails(heal_on=["Recoverable"], never_heal=["Recoverable"])
    should_heal, reason = _check_heal_guardrails(ctx, g)
    assert not should_heal


# ── heal_on_errors whitelist ─────────────────────────────────────────────────

def test_heal_on_non_empty_error_type_matches_proceeds():
    ctx = _make_ctx(error_type="EmptyDataset")
    g = _make_guardrails(heal_on=["EmptyDataset", "SchemaError"])
    should_heal, reason = _check_heal_guardrails(ctx, g)
    assert should_heal
    assert reason == ""


def test_heal_on_non_empty_error_type_no_match_blocks():
    ctx = _make_ctx(error_type="UnknownError")
    g = _make_guardrails(heal_on=["EmptyDataset"])
    should_heal, reason = _check_heal_guardrails(ctx, g)
    assert not should_heal
    assert "UnknownError" in reason or "heal_on_errors" in reason


def test_heal_on_empty_no_restriction():
    ctx = _make_ctx(error_type="AnythingGoes")
    g = _make_guardrails(heal_on=[])
    should_heal, reason = _check_heal_guardrails(ctx, g)
    assert should_heal


# ── never_heal empty ─────────────────────────────────────────────────────────

def test_never_heal_empty_no_restriction():
    ctx = _make_ctx(error_type=None)
    g = _make_guardrails(never_heal=[])
    should_heal, reason = _check_heal_guardrails(ctx, g)
    assert should_heal


# ── error_type None falls back to stack class ─────────────────────────────────

def test_error_type_none_uses_stack_class():
    ctx = _make_ctx(
        error_type=None,
        stack_trace="pyspark.errors.SparkException: boom"
    )
    g = _make_guardrails(heal_on=["SparkException"])
    should_heal, reason = _check_heal_guardrails(ctx, g)
    assert should_heal


def test_error_type_and_stack_class_either_match():
    ctx = _make_ctx(
        error_type="DataQualityError",
        stack_trace="pyspark.errors.SparkException: boom"
    )
    # heal_on only includes the stack class, not error_type
    g = _make_guardrails(heal_on=["SparkException"])
    should_heal, reason = _check_heal_guardrails(ctx, g)
    assert should_heal
