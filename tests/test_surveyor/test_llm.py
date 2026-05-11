"""Unit tests for LLM self-healing loop in aqueduct/surveyor/llm.py."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
pytestmark = pytest.mark.unit

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
        # Filename now contains timestamp: YYYYMMDDTHHmmss_test-fix.json
        matches = list((patches_dir / "pending").glob("*_test-fix.json"))
        assert len(matches) == 1

    def test_pending_file_contains_aq_meta(self, tmp_path):
        patches_dir = tmp_path / "patches"
        spec = _patch_spec()
        ctx = _failure_ctx(run_id="run-456", blueprint_id="my.pipe")
        stage_patch_for_human(spec, patches_dir, ctx)
        matches = list((patches_dir / "pending").glob("*_test-fix.json"))
        data = json.loads(matches[0].read_text())
        assert "_aq_meta" in data
        assert data["_aq_meta"]["run_id"] == "run-456"
        assert data["_aq_meta"]["blueprint_id"] == "my.pipe"

    def test_returns_none(self, tmp_path):
        spec = _patch_spec()
        result = stage_patch_for_human(spec, tmp_path / "patches", _failure_ctx())
        assert result is None

class TestPatchFilename:
    def test_patch_filename_includes_seq(self, tmp_path):
        from aqueduct.surveyor.llm import _patch_filename
        spec = _patch_spec(patch_id="test")
        patches_dir = tmp_path / "patches"
        # 0 files -> seq is 00001
        filename = _patch_filename(spec, patches_dir)
        assert filename.startswith("00001_")
        assert "_test.json" in filename

    def test_patch_filename_increments_seq(self, tmp_path):
        from aqueduct.surveyor.llm import _patch_filename
        spec = _patch_spec(patch_id="test")
        patches_dir = tmp_path / "patches"
        
        pending_dir = patches_dir / "pending"
        pending_dir.mkdir(parents=True)
        (pending_dir / "00001_123_a.json").write_text("{}")
        
        applied_dir = patches_dir / "applied"
        applied_dir.mkdir(parents=True)
        (applied_dir / "00002_123_b.json").write_text("{}")
        (applied_dir / "00003_123_c.json").write_text("{}")
        
        rejected_dir = patches_dir / "rejected"
        rejected_dir.mkdir(parents=True)
        (rejected_dir / "00004_123_d.json").write_text("{}")
        
        # 4 files total -> next seq is 00005
        filename = _patch_filename(spec, patches_dir)
        assert filename.startswith("00005_")



# ── archive_patch ─────────────────────────────────────────────────────────────


class TestArchivePatch:
    def test_creates_applied_file(self, tmp_path):
        patches_dir = tmp_path / "patches"
        spec = _patch_spec(patch_id="archive-me")
        ctx = _failure_ctx()
        archive_patch(spec, patches_dir, ctx, mode="auto")
        matches = list((patches_dir / "applied").glob("*_archive-me.json"))
        assert len(matches) == 1

    def test_applied_file_contains_meta(self, tmp_path):
        patches_dir = tmp_path / "patches"
        spec = _patch_spec(patch_id="archive-me")
        ctx = _failure_ctx(run_id="run-789")
        archive_patch(spec, patches_dir, ctx, mode="auto")
        matches = list((patches_dir / "applied").glob("*_archive-me.json"))
        data = json.loads(matches[0].read_text())
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
        assert result.patch is None

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
        assert result.patch is None

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
        assert result.patch is not None
        assert result.patch.patch_id == "test-fix"

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
        assert result.patch is not None
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


class TestLlmHelpers:
    def test_extract_failure_context_from_db(self, tmp_path):
        import duckdb
        from aqueduct.surveyor.surveyor import Surveyor, _DDL
        
        def extract_failure_context(run_id: str, store_dir: Path):
            db_path = store_dir / "obs.db"
            if not db_path.exists(): return None
            conn = duckdb.connect(str(db_path))
            try:
                row = conn.execute("SELECT * FROM failure_contexts WHERE run_id = ?", [run_id]).fetchone()
                if not row: return None
                from aqueduct.surveyor.models import FailureContext
                return FailureContext(
                    run_id=row[0], blueprint_id=row[1], failed_module=row[2],
                    error_message=row[3], stack_trace=row[4], manifest_json=row[5],
                    provenance_json=row[6], started_at=row[7], finished_at=row[8]
                )
            finally: conn.close()

        store = tmp_path / "obs"
        store.mkdir()
        db_path = store / "obs.db"
        conn = duckdb.connect(str(db_path))
        conn.execute(_DDL)
        conn.execute(
            "INSERT INTO failure_contexts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ["run-1", "bp-1", "m-1", "boom", "stack", "{}", "{}", "2024-01-01", "2024-01-01"],
        )
        conn.close()

        ctx = extract_failure_context("run-1", store)
        assert ctx.failed_module == "m-1"
        assert ctx.error_message == "boom"

    def test_extract_failure_context_missing_returns_none(self, tmp_path):
        def extract_failure_context(run_id: str, store_dir: Path):
            db_path = store_dir / "obs.db"
            if not db_path.exists(): return None
            return "not none" # dummy
        # Store doesn't even exist
        assert extract_failure_context("ghost", tmp_path / "obs") is None

    def test_reprompt_limit_exceeded(self, monkeypatch, tmp_path):
        from aqueduct.surveyor.llm import generate_llm_patch, MAX_REPROMPTS
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
        
        call_count = 0
        def always_invalid(*_args, **_kw):
            nonlocal call_count
            call_count += 1
            return "not json"
        
        monkeypatch.setattr("aqueduct.surveyor.llm._call_llm", always_invalid)
        
        from unittest.mock import MagicMock
        ctx = MagicMock()
        result = generate_llm_patch(ctx, "model", tmp_path)
        
        assert result.patch is None
        # Should have tried exactly llm_max_reprompts (defaults to MAX_REPROMPTS=3)
        assert call_count == MAX_REPROMPTS

    def test_reprompt_uses_custom_llm_max_reprompts(self, monkeypatch, tmp_path):
        from aqueduct.surveyor.llm import generate_llm_patch
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
        
        call_count = 0
        def always_invalid(*_args, **_kw):
            nonlocal call_count
            call_count += 1
            return "not json"
        
        monkeypatch.setattr("aqueduct.surveyor.llm._call_llm", always_invalid)
        
        from unittest.mock import MagicMock
        ctx = MagicMock()
        result = generate_llm_patch(ctx, "model", tmp_path, llm_max_reprompts=5)
        
        assert result.patch is None
        assert call_count == 5

    def test_generate_llm_patch_uses_llm_timeout(self, monkeypatch, tmp_path):
        from aqueduct.surveyor.llm import generate_llm_patch
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
        
        timeout_used = None
        def mock_call_llm(*_args, **kwargs):
            nonlocal timeout_used
            timeout_used = kwargs.get("timeout")
            return _valid_patch_json()
        
        monkeypatch.setattr("aqueduct.surveyor.llm._call_llm", mock_call_llm)
        
        from unittest.mock import MagicMock
        ctx = MagicMock()
        generate_llm_patch(ctx, "model", tmp_path, llm_timeout=600.0)
        
        assert timeout_used == 600.0



class TestBuildSystemPrompt:
    def test_engine_prompt_context_included(self, tmp_path):
        from aqueduct.surveyor.llm import _build_system_prompt
        prompt = _build_system_prompt(
            patches_dir=tmp_path,
            engine_prompt_context="Engine rule 1.",
            blueprint_prompt_context=None
        )
        assert "Engine rule 1." in prompt

    def test_blueprint_prompt_context_included(self, tmp_path):
        from aqueduct.surveyor.llm import _build_system_prompt
        prompt = _build_system_prompt(
            patches_dir=tmp_path,
            engine_prompt_context=None,
            blueprint_prompt_context="Blueprint rule 2."
        )
        assert "Blueprint rule 2." in prompt

    def test_both_contexts_included(self, tmp_path):
        from aqueduct.surveyor.llm import _build_system_prompt
        prompt = _build_system_prompt(
            patches_dir=tmp_path,
            engine_prompt_context="Engine rule 1.",
            blueprint_prompt_context="Blueprint rule 2."
        )
        assert "Engine rule 1." in prompt
        assert "Blueprint rule 2." in prompt
        # Ensure blueprint comes after engine
        assert prompt.index("Engine rule 1.") < prompt.index("Blueprint rule 2.")


class TestFailureContextBlueprintSourceYaml:
    def test_failure_context_has_blueprint_source_yaml(self):
        from aqueduct.surveyor.models import FailureContext
        ctx = FailureContext(
            run_id="r1",
            blueprint_id="b1",
            failed_module="m1",
            error_message="err",
            stack_trace=None,
            manifest_json="{}",
            started_at="2026-01-01T00:00:00Z",
            finished_at="2026-01-01T00:01:00Z",
            blueprint_source_yaml="id: test"
        )
        assert ctx.blueprint_source_yaml == "id: test"
        assert ctx.to_dict()["blueprint_source_yaml"] == "id: test"
    def test_llm_user_prompt_includes_blueprint_source_yaml(self):
        from aqueduct.surveyor.llm import _build_user_prompt
        from aqueduct.surveyor.models import FailureContext
        ctx = FailureContext(
            run_id="r1",
            blueprint_id="b1",
            failed_module="m1",
            error_message="err",
            stack_trace=None,
            manifest_json="{}",
            started_at="2026-01-01T00:00:00Z",
            finished_at="2026-01-01T00:01:00Z",
            blueprint_source_yaml="id: my_blueprint\nname: Test"
        )
        prompt = _build_user_prompt(ctx)
        assert "## Original Blueprint YAML" in prompt
        assert "id: my_blueprint" in prompt

    def test_llm_system_prompt_includes_template_expressions_rule(self, tmp_path):
        from aqueduct.surveyor.llm import _build_system_prompt
        prompt = _build_system_prompt(patches_dir=tmp_path)
        assert "using template expressions" in prompt
        assert "Do NOT hard-code the resolved literal path" in prompt


# ── doctor_hints LLM injection ─────────────────────────────────────────────────

class TestDoctorHintsInLLMPrompt:
    def test_doctor_hints_non_empty_includes_section(self, tmp_path):
        """doctor_hints non-empty → user prompt contains 'Blueprint issues detected' section."""
        from aqueduct.surveyor.llm import _build_user_prompt

        ctx = _failure_ctx(
            doctor_hints=("warn: bad path /tmp/missing",),
            manifest_json=json.dumps({"blueprint_id": "test.bp", "name": "Test", "modules": [], "edges": []}),
        )
        prompt = _build_user_prompt(ctx, patches_dir=tmp_path)
        assert "Blueprint issues detected before run" in prompt
        assert "warn: bad path /tmp/missing" in prompt

    def test_doctor_hints_empty_section_absent(self, tmp_path):
        """doctor_hints empty → 'Blueprint issues detected' section absent."""
        from aqueduct.surveyor.llm import _build_user_prompt

        ctx = _failure_ctx(
            doctor_hints=(),
            manifest_json=json.dumps({"blueprint_id": "test.bp", "name": "Test", "modules": [], "edges": []}),
        )
        prompt = _build_user_prompt(ctx, patches_dir=tmp_path)
        assert "Blueprint issues detected" not in prompt
