"""Unit tests for LLM self-healing loop in aqueduct/surveyor/llm.py."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from aqueduct.patch.grammar import PatchSpec
from aqueduct.surveyor.llm import (
    MAX_REPROMPTS,
    _parse_patch_spec,
    archive_patch,
    generate_llm_patch,
    stage_patch_for_human,
)
from aqueduct.surveyor.models import FailureContext


# ── helpers ───────────────────────────────────────────────────────────────────

_MINIMAL_BP_YAML = """\
aqueduct: "1.0"
id: test.auto.apply
name: Test Auto Apply

modules:
  - id: m1
    type: Ingress
    label: Source
    config:
      format: parquet
      path: /tmp/data

  - id: sink
    type: Egress
    label: Sink
    config:
      format: parquet
      path: /tmp/out
      mode: overwrite

edges:
  - from: m1
    to: sink
"""


def _patch_spec(**kwargs) -> PatchSpec:
    defaults = {
        "patch_id": "test-fix",
        "rationale": "Test fix rationale",
        "operations": [
            {
                "op": "replace_module_config",
                "module_id": "m1",
                "config": {"format": "parquet", "path": "/tmp/new_data"},
            }
        ],
    }
    defaults.update(kwargs)
    return PatchSpec.model_validate(defaults)


def _failure_ctx(**kwargs) -> FailureContext:
    defaults = dict(
        run_id="run-123",
        blueprint_id="test.pipe",
        failed_module="m1",
        error_message="Something went wrong",
        stack_trace=None,
        manifest_json=json.dumps({"blueprint_id": "test.pipe"}),
        started_at="2024-01-01T00:00:00+00:00",
        finished_at="2024-01-01T00:01:00+00:00",
    )
    defaults.update(kwargs)
    return FailureContext(**defaults)


def _valid_patch_json() -> str:
    return json.dumps(
        {
            "patch_id": "test-fix",
            "rationale": "Test fix rationale",
            "operations": [
                {
                    "op": "replace_module_config",
                    "module_id": "m1",
                    "config": {"format": "parquet", "path": "/tmp/new"},
                }
            ],
        }
    )


# ── _parse_patch_spec ─────────────────────────────────────────────────────────


class TestParsePatchSpec:
    def test_valid_json_parses(self):
        spec = _parse_patch_spec(_valid_patch_json())
        assert spec.patch_id == "test-fix"

    def test_markdown_fenced_json_stripped(self):
        fenced = f"```json\n{_valid_patch_json()}\n```"
        spec = _parse_patch_spec(fenced)
        assert spec.patch_id == "test-fix"

    def test_markdown_fenced_no_lang_stripped(self):
        fenced = f"```\n{_valid_patch_json()}\n```"
        spec = _parse_patch_spec(fenced)
        assert spec.patch_id == "test-fix"

    def test_invalid_json_raises(self):
        from json import JSONDecodeError

        with pytest.raises((JSONDecodeError, ValueError)):
            _parse_patch_spec("not json at all")


# ── stage_patch_for_human ─────────────────────────────────────────────────────


class TestStageForHuman:
    def test_creates_pending_file(self, tmp_path):
        patches_dir = tmp_path / "patches"
        spec = _patch_spec()
        ctx = _failure_ctx()
        stage_patch_for_human(spec, patches_dir, ctx)
        assert (patches_dir / "pending" / "test-fix.json").exists()

    def test_pending_file_contains_aq_meta(self, tmp_path):
        patches_dir = tmp_path / "patches"
        spec = _patch_spec()
        ctx = _failure_ctx(run_id="run-456", blueprint_id="my.pipe")
        stage_patch_for_human(spec, patches_dir, ctx)
        data = json.loads((patches_dir / "pending" / "test-fix.json").read_text())
        assert "_aq_meta" in data
        assert data["_aq_meta"]["run_id"] == "run-456"
        assert data["_aq_meta"]["blueprint_id"] == "my.pipe"

    def test_returns_none(self, tmp_path):
        spec = _patch_spec()
        result = stage_patch_for_human(spec, tmp_path / "patches", _failure_ctx())
        assert result is None


# ── archive_patch ─────────────────────────────────────────────────────────────


class TestArchivePatch:
    def test_creates_applied_file(self, tmp_path):
        patches_dir = tmp_path / "patches"
        spec = _patch_spec(patch_id="archive-me")
        ctx = _failure_ctx()
        archive_patch(spec, patches_dir, ctx, mode="auto")
        assert (patches_dir / "applied" / "archive-me.json").exists()

    def test_applied_file_contains_meta(self, tmp_path):
        patches_dir = tmp_path / "patches"
        spec = _patch_spec(patch_id="archive-me")
        ctx = _failure_ctx(run_id="run-789")
        archive_patch(spec, patches_dir, ctx, mode="auto")
        data = json.loads((patches_dir / "applied" / "archive-me.json").read_text())
        assert "_aq_meta" in data
        assert data["_aq_meta"]["run_id"] == "run-789"
        assert data["_aq_meta"]["approval_mode"] == "auto"
        assert "applied_at" in data["_aq_meta"]

    def test_apply_patch_file_modifies_blueprint(self, tmp_path):
        from aqueduct.patch.apply import apply_patch_file

        bp_file = tmp_path / "blueprint.yml"
        bp_file.write_text(_MINIMAL_BP_YAML)
        patches_dir = tmp_path / "patches"
        spec = _patch_spec()

        patch_path = patches_dir / "pending" / "test-fix.json"
        patch_path.parent.mkdir(parents=True)
        patch_path.write_text(spec.model_dump_json())

        # apply_patch_file(blueprint_path, patch_path, patches_dir)
        apply_patch_file(bp_file, patch_path, patches_dir=patches_dir)

        import yaml
        patched = yaml.safe_load(bp_file.read_text())
        m1_config = next(m["config"] for m in patched["modules"] if m["id"] == "m1")
        assert m1_config["path"] == "/tmp/new_data"


# ── generate_llm_patch ───────────────────────────────────────────────────────


class TestGenerateLlmPatch:
    def test_no_api_key_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        ctx = _failure_ctx()
        result = generate_llm_patch(
            failure_ctx=ctx,
            model="claude-sonnet-4-6",
            patches_dir=tmp_path / "patches",
        )
        assert result is None

    def test_always_invalid_response_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")

        def bad_llm(*_args, **_kw):
            return "not valid json {{{"

        monkeypatch.setattr("aqueduct.surveyor.llm._call_llm", bad_llm)

        result = generate_llm_patch(
            failure_ctx=_failure_ctx(),
            model="claude-sonnet-4-6",
            patches_dir=tmp_path / "patches",
        )
        assert result is None

    def test_valid_response_returns_patch_spec(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")

        def mock_llm(*_args, **_kw):
            return _valid_patch_json()

        monkeypatch.setattr("aqueduct.surveyor.llm._call_llm", mock_llm)

        result = generate_llm_patch(
            failure_ctx=_failure_ctx(),
            model="claude-sonnet-4-6",
            patches_dir=tmp_path / "patches",
        )
        assert result is not None
        assert result.patch_id == "test-fix"

    def test_reprompts_on_invalid_then_succeeds(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        call_count = []

        def flaky_llm(*_args, **_kw):
            call_count.append(1)
            if len(call_count) < 2:
                return "invalid json"
            return _valid_patch_json()

        monkeypatch.setattr("aqueduct.surveyor.llm._call_llm", flaky_llm)

        result = generate_llm_patch(
            failure_ctx=_failure_ctx(),
            model="claude-sonnet-4-6",
            patches_dir=tmp_path / "patches",
        )
        assert result is not None
        assert len(call_count) == 2


# ── Surveyor integration ──────────────────────────────────────────────────────


class TestSurveyorLlmIntegration:
    # NOTE: LLM triggering was moved from Surveyor.record() to cli.py in Phase 8.
    # Surveyor.record() now only persists the run outcome and fires webhooks.
    # These tests document the current behavior (LLM NOT called from record()).

    def _make_manifest_with_approval(self, approval_mode: str):
        from aqueduct.compiler.models import Manifest
        from aqueduct.parser.models import AgentConfig

        return Manifest(
            blueprint_id="test.pipe",
            modules=(),
            edges=(),
            context={},
            spark_config={},
            agent=AgentConfig(approval_mode=approval_mode),
        )

    def test_record_failure_returns_failure_context(self, tmp_path):
        from aqueduct.executor.models import ExecutionResult, ModuleResult
        from aqueduct.surveyor.surveyor import Surveyor

        manifest = self._make_manifest_with_approval("auto")
        surveyor = Surveyor(manifest, store_dir=tmp_path / "store")
        surveyor.start("run-001")

        result = ExecutionResult(
            blueprint_id="test.pipe",
            run_id="run-001",
            status="error",
            module_results=(ModuleResult(module_id="m1", status="error", error="fail"),),
        )
        ctx = surveyor.record(result)
        surveyor.stop()

        assert ctx is not None
        assert ctx.failed_module == "m1"

    def test_record_success_returns_none(self, tmp_path):
        from aqueduct.executor.models import ExecutionResult
        from aqueduct.surveyor.surveyor import Surveyor

        manifest = self._make_manifest_with_approval("auto")
        surveyor = Surveyor(manifest, store_dir=tmp_path / "store")
        surveyor.start("run-002")

        result = ExecutionResult(
            blueprint_id="test.pipe",
            run_id="run-002",
            status="success",
            module_results=(),
        )
        ctx = surveyor.record(result)
        surveyor.stop()

        assert ctx is None
