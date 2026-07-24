"""Tests for macro hint inclusion in user prompts.

When a Blueprint defines ``macros:`` the user prompt should include an
instruction about the ``replace_macro`` operation.
"""

import pytest
from pathlib import Path
from aqueduct.surveyor.models import FailureContext
from aqueduct.agent.prompts import build_prompt


def test_user_prompt_includes_replace_macro_hint(tmp_path: Path):
    yaml = "aqueduct: 1.0\nmacros:\n  mymacro: \"SELECT 1\"\n"
    fc = FailureContext(
        run_id="run-1",
        blueprint_id="bp",
        failed_module="mod",
        error_message="boom",
        stack_trace=None,
        manifest_json="{}",
        started_at="2026-01-01T00:00:00Z",
        finished_at="2026-01-01T00:01:00Z",
        blueprint_source_yaml=yaml,
     engine="spark",)
    patches_dir = tmp_path / "patches"
    patches_dir.mkdir()
    user_prompt = build_prompt(fc, patches_dir)["user"]
    assert "replace_macro" in user_prompt
    assert "{{ macros." in user_prompt
