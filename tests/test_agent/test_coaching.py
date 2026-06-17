"""Unit tests for Phase 53 coaching section — served from the patch_index table."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from aqueduct.surveyor.models import FailureContext


pytestmark = pytest.mark.unit


def _make_obs_store(db_path: Path):
    from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
    s = DuckDBObservabilityStore(db_path)
    with s.connect() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS patch_index (
                patch_id           VARCHAR PRIMARY KEY,
                blueprint_id       VARCHAR,
                run_id             VARCHAR,
                status             VARCHAR NOT NULL,
                object_key         VARCHAR NOT NULL,
                signature          VARCHAR,
                signature_coarse   VARCHAR,
                error_class        VARCHAR,
                where_field        VARCHAR,
                normalized_message VARCHAR,
                rationale          VARCHAR,
                ops                JSON,
                source             VARCHAR,
                prompt_version     VARCHAR,
                created_at         VARCHAR NOT NULL,
                updated_at         VARCHAR NOT NULL
            )
        """)
    return s


def _stamp_applied(store, patch_id, sig_hash, error_class="E", where="m1", msg="x"):
    with store.connect() as cur:
        cur.execute(
            "INSERT OR REPLACE INTO patch_index "
            "(patch_id, object_key, blueprint_id, status, source, "
            " signature, signature_coarse, error_class, where_field, "
            " normalized_message, rationale, ops, created_at, updated_at) "
            "VALUES (?, ?, ?, 'applied', 'llm', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [patch_id, f"/obj/{patch_id}.json", "bp1",
             sig_hash, sig_hash[:8], error_class, where, msg,
             "fix", '["set_module_config_key"]',
             "2025-01-01T00:00:00", "2025-01-01T00:00:00"],
        )


class TestBuildCoachingSection:
    def test_empty_store_returns_empty(self, tmp_path):
        from aqueduct.agent.prompts import _build_coaching_section
        ctx = FailureContext(
            run_id="r1", blueprint_id="b1", failed_module="m1",
            error_message="err", stack_trace="", manifest_json="{}",
            started_at="2020-01-01", finished_at="2020-01-01",
            error_class="UNRESOLVED_COLUMN",
        )
        obs_store = _make_obs_store(tmp_path / "obs.db")
        result = _build_coaching_section(ctx, obs_store)
        assert result == ""

    def test_renders_tier_labels(self, tmp_path):
        from aqueduct.agent.prompts import _build_coaching_section
        from aqueduct.agent.signature import from_failure_context
        ctx = FailureContext(
            run_id="r1", blueprint_id="b1", failed_module="m1",
            error_message="column not found", stack_trace="", manifest_json="{}",
            started_at="2020-01-01", finished_at="2020-01-01",
            error_class="UNRESOLVED_COLUMN",
        )
        exact, _ = from_failure_context(ctx)
        obs_store = _make_obs_store(tmp_path / "obs.db")
        _stamp_applied(obs_store, "fix-1", exact.hash)
        result = _build_coaching_section(ctx, obs_store)
        assert "Past validated fixes" in result
        assert "m1" in result
        assert "fix-1" in result

    def test_fallback_to_empty_on_exception(self, tmp_path):
        from aqueduct.agent.prompts import _build_coaching_section
        result = _build_coaching_section(None, None)
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
        ctx = FailureContext(
            run_id="r1", blueprint_id="b1", failed_module="m1",
            error_message="err", stack_trace="",
            manifest_json='{"id": "b1", "modules": [], "edges": []}',
            started_at="2020-01-01", finished_at="2020-01-01",
        )
        result = build_prompt(ctx, tmp_path, coaching=True, obs_store=None)
        assert "system" in result
        assert "user" in result

    def test_build_prompt_coaching_false(self, tmp_path):
        from aqueduct.agent.prompts import build_prompt
        ctx = FailureContext(
            run_id="r1", blueprint_id="b1", failed_module="m1",
            error_message="err", stack_trace="",
            manifest_json='{"id": "b1", "modules": [], "edges": []}',
            started_at="2020-01-01", finished_at="2020-01-01",
        )
        result = build_prompt(ctx, tmp_path, coaching=False, obs_store=None)
        assert "system" in result
        assert "user" in result
