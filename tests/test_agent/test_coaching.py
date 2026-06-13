"""Unit tests for Phase 45 coaching section and system prompt threading."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

pytestmark = pytest.mark.unit


def _write_applied_patch(p: Path, patch_id: str, sig_hash: str, error_class="E", where="m1", msg="x"):
    p.parent.mkdir(parents=True, exist_ok=True)
    import json as _j
    p.write_text(_j.dumps({
        "patch_id": patch_id,
        "rationale": "fix",
        "operations": [{"op": "set_module_config_key", "module_id": "m1", "key": "k", "value": "v"}],
        "_aq_meta": {
            "failure_signature": {"hash": sig_hash, "error_class": error_class, "where": where, "normalized_message": msg},
            "failure_signature_coarse": sig_hash[:8],
        },
    }))


class TestBuildCoachingSection:
    def test_empty_applied_dir_returns_empty(self, tmp_path):
        from aqueduct.agent.prompts import _build_coaching_section
        from aqueduct.surveyor.models import FailureContext
        ctx = FailureContext(
            run_id="r1", blueprint_id="b1", failed_module="m1",
            error_message="err", stack_trace="", manifest_json="{}",
            started_at="2020-01-01", finished_at="2020-01-01",
            error_class="UNRESOLVED_COLUMN",
        )
        result = _build_coaching_section(ctx, tmp_path)
        assert result == ""

    def test_renders_tier_labels(self, tmp_path):
        from aqueduct.agent.prompts import _build_coaching_section
        from aqueduct.agent.signature import from_failure_context
        from aqueduct.surveyor.models import FailureContext
        ctx = FailureContext(
            run_id="r1", blueprint_id="b1", failed_module="m1",
            error_message="column not found", stack_trace="", manifest_json="{}",
            started_at="2020-01-01", finished_at="2020-01-01",
            error_class="UNRESOLVED_COLUMN",
        )
        exact, _ = from_failure_context(ctx)
        _write_applied_patch(tmp_path / "applied" / "001_fix.json", "fix-1", exact.hash)
        result = _build_coaching_section(ctx, tmp_path)
        assert "Past validated fixes" in result
        assert "m1" in result
        assert "fix-1" in result

    def test_fallback_to_empty_on_exception(self, tmp_path):
        from aqueduct.agent.prompts import _build_coaching_section
        # None ctx should cause exception in from_failure_context → empty
        result = _build_coaching_section(None, tmp_path)
        assert result == ""


class TestBuildSystemPromptCoaching:
    def test_coaching_off_uses_legacy_section(self, tmp_path):
        from aqueduct.agent.prompts import _build_system_prompt
        result = _build_system_prompt(
            patches_dir=tmp_path,
            engine_prompt_context=None,
            blueprint_prompt_context=None,
            last_apply_error=None,
            coaching=False,
            failure_ctx=None,
        )
        # No coaching section, no "do NOT repeat" (empty patches dir)
        assert "Past validated fixes" not in result

    def test_coaching_on_without_failure_ctx_falls_to_legacy(self, tmp_path):
        from aqueduct.agent.prompts import _build_system_prompt
        result = _build_system_prompt(
            patches_dir=tmp_path,
            coaching=True,
            failure_ctx=None,
        )
        assert "Past validated fixes" not in result

    def test_last_apply_error_appended_in_both_paths(self, tmp_path):
        from aqueduct.agent.prompts import _build_system_prompt
        result = _build_system_prompt(
            patches_dir=tmp_path,
            coaching=True,
            failure_ctx=None,
            last_apply_error="previous fix failed",
        )
        assert "previous fix failed" in result


class TestBuildPromptCoaching:
    def test_build_prompt_threads_coaching(self, tmp_path):
        from aqueduct.agent.prompts import build_prompt
        from aqueduct.surveyor.models import FailureContext
        ctx = FailureContext(
            run_id="r1", blueprint_id="b1", failed_module="m1",
            error_message="err", stack_trace="",
            manifest_json='{"id": "b1", "modules": [], "edges": []}',
            started_at="2020-01-01", finished_at="2020-01-01",
        )
        result = build_prompt(ctx, tmp_path, coaching=True)
        assert "system" in result
        assert "user" in result

    def test_build_prompt_coaching_false(self, tmp_path):
        from aqueduct.agent.prompts import build_prompt
        from aqueduct.surveyor.models import FailureContext
        ctx = FailureContext(
            run_id="r1", blueprint_id="b1", failed_module="m1",
            error_message="err", stack_trace="",
            manifest_json='{"id": "b1", "modules": [], "edges": []}',
            started_at="2020-01-01", finished_at="2020-01-01",
        )
        result = build_prompt(ctx, tmp_path, coaching=False)
        assert "system" in result
        assert "user" in result
