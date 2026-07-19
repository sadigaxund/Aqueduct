"""Tests for curated system-prompt rules in aqueduct/agent/prompts.py.

Covers the schema_hint type-mismatch / field-not-found guidance added to
steer the LLM away from proposing a no-op patch (re-setting a schema_hint
to the value that is already current).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from aqueduct.agent.loop import PROMPT_VERSION
from aqueduct.agent.prompts import _build_system_prompt

pytestmark = pytest.mark.unit


def test_system_prompt_includes_schema_hint_type_mismatch_rule(tmp_path: Path):
    patches_dir = tmp_path / "patches"
    patches_dir.mkdir()
    system_prompt = _build_system_prompt(patches_dir)

    assert "schema_hint type mismatch" in system_prompt
    # The rule must call out which side of the message is which.
    assert "expected" in system_prompt and "actual" in system_prompt
    assert "no-op" in system_prompt


def test_system_prompt_includes_schema_hint_field_not_found_rule(tmp_path: Path):
    patches_dir = tmp_path / "patches"
    patches_dir.mkdir()
    system_prompt = _build_system_prompt(patches_dir)

    assert "schema_hint field" in system_prompt
    assert "not found in source schema" in system_prompt
    assert "Available columns" in system_prompt


def test_prompt_version_bumped_for_schema_hint_rule():
    # This rule change touches _SYSTEM_PROMPT_TEMPLATE body, so per the
    # PROMPT_VERSION bump policy (AGENTS.md) it must be reflected here.
    # Phase 75 bumped 1.6 -> 1.7 for the agentic-mode tools addendum.
    # Phase 78 bumped 1.7 -> 1.8 for the DuckDB pack's PREDICTED_SCHEMA_DRIFT
    # rule bullet (Spark's composed prompt is unchanged; version is global).
    assert PROMPT_VERSION == "1.8"


def test_schema_hint_rule_never_leaks_defer_op_token(tmp_path: Path):
    # Regression: the rule text must not contain the literal op token —
    # when allow_defer=False the op is stripped from the schema, and the
    # model must not learn an op it isn't offered (Phase 41 invariant).
    patches_dir = tmp_path / "patches"
    patches_dir.mkdir()
    system_prompt = _build_system_prompt(patches_dir, allow_defer=False)
    assert "defer_to_human" not in system_prompt
