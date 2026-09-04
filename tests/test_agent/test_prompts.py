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
    # Phase 79 item 6 bumped 1.8 -> 1.9 for the DuckDB pack's out-of-memory /
    # capacity-exhaustion defer rule (Spark's composed prompt is unchanged).
    # Phase 82 bumped 1.9 -> 1.10: the op table the LLM sees changed when the
    # engine-named `set_spark_config` was replaced by `set_engine_config`, which
    # is exactly what the bump policy covers (the composed prompt's body).
    # Cross-engine remediation bumped 1.10 -> 1.11: the composed prompt gained
    # the "Engine/session config (`set_engine_config`)" section, which renders
    # the target engine's whole `engine_config_allowlist.yml` (allow entries
    # with type/enum/range, deny entries with their `reason`) so the model is
    # told what it may write instead of discovering it through Gate 1
    # rejections. New prompt body → bump.
    # Phase 88 bumped 1.11 -> 1.12: one bump covering both new-domain prompt
    # changes together — the `declare_dependency` never-installs sentence in
    # "Other rules", and the required `defer_reason` bucket list in the
    # runtime-assembled defer section.
    # 2.2.0 security workstream item B bumped 1.12 -> 1.13: the scaffold
    # gains an "Untrusted data" instruction block plus untrusted-data
    # sentinel markers wrapped around the user prompt's error message and
    # root-cause/stack-trace section.
    # Phase 92 cleanup bumped 1.13 -> 1.14: the signature-keyed heal cache
    # (and the coaching-examples section it fed) is gone — every heal now
    # falls back to the chronological "do NOT repeat" section.
    # Phase 92 cleanup bumped 1.14 -> 1.15: the tool-calling heal mode is
    # removed — the "Tools available" addendum never renders, and the
    # "Untrusted data" block no longer mentions tool_result content.
    assert PROMPT_VERSION == "1.15"


def test_schema_hint_rule_never_leaks_defer_op_token(tmp_path: Path):
    # Regression: the rule text must not contain the literal op token —
    # when allow_defer=False the op is stripped from the schema, and the
    # model must not learn an op it isn't offered (Phase 41 invariant).
    patches_dir = tmp_path / "patches"
    patches_dir.mkdir()
    system_prompt = _build_system_prompt(patches_dir, allow_defer=False)
    assert "defer_to_human" not in system_prompt
