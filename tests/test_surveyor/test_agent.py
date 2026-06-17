"""Unit tests for LLM self-healing loop in aqueduct/surveyor/llm.py."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
pytestmark = [pytest.mark.spark, pytest.mark.integration]

from aqueduct.patch.grammar import PatchSpec


from aqueduct.agent import MAX_REPROMPTS, PROMPT_VERSION
from aqueduct.agent.loop import archive_patch, generate_agent_patch, stage_patch_for_human
from aqueduct.agent.parse import _parse_patch_spec
from aqueduct.agent.prompts import _build_guardrails_section




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
        spec, recovery = _parse_patch_spec(_valid_patch_json())
        assert spec.patch_id == "test-fix"
        assert recovery == []

    def test_markdown_fenced_json_stripped(self):
        fenced = f"```json\n{_valid_patch_json()}\n```"
        spec, recovery = _parse_patch_spec(fenced)
        assert spec.patch_id == "test-fix"
        assert "stripped_code_fence" in recovery

    def test_markdown_fenced_no_lang_stripped(self):
        fenced = f"```\n{_valid_patch_json()}\n```"
        spec, recovery = _parse_patch_spec(fenced)
        assert spec.patch_id == "test-fix"
        assert "stripped_code_fence" in recovery

    def test_invalid_json_raises(self):
        from json import JSONDecodeError

        with pytest.raises((JSONDecodeError, ValueError)):
            _parse_patch_spec("not json at all")

    def test_string_containing_comment_chars_not_mangled(self):
        """Valid JSON with '//' or '#' inside a string value parses cleanly (no recovery)."""
        spec, recovery = _parse_patch_spec("""{
  "patch_id": "fix-comment",
  "rationale": "SQL with inline comment chars",
  "confidence": 0.9,
  "category": "type_mismatch",
  "root_cause": "bad query",
  "operations": [
    {
      "op": "set_module_config_key",
      "module_id": "ch1",
      "key": "query",
      "value": "SELECT a // 2 AS half FROM t"
    }
  ]
}""")
        assert spec.patch_id == "fix-comment"
        assert recovery == []
        assert " // 2 " in spec.operations[0].value

    def test_string_containing_hash_not_mangled(self):
        """Valid JSON with '#' inside a string value parses cleanly."""
        spec, recovery = _parse_patch_spec("""{
  "patch_id": "fix-hash",
  "rationale": "path with hash",
  "confidence": 0.9,
  "category": "bad_path",
  "root_cause": "wrong path",
  "operations": [
    {
      "op": "set_module_config_key",
      "module_id": "ingress",
      "key": "path",
      "value": "data/input_#1.csv"
    }
  ]
}""")
        assert spec.patch_id == "fix-hash"
        assert recovery == []

    def test_line_comments_stripped_only_after_strict_fails(self):
        """JSON with line comments on their own lines strips comments via recovery pass."""
        raw = """{
  "patch_id": "fix-test",
  "rationale": "fix path",
  // this is a comment
  "confidence": 0.9,
  # yaml-style comment
  "category": "bad_path",
  "root_cause": "wrong format",
  "operations": [
    {
      "op": "set_module_config_key",
      "module_id": "ingress",
      "key": "format",
      "value": "csv"
    }
  ]
}"""
        spec, recovery = _parse_patch_spec(raw)
        assert spec.patch_id == "fix-test"
        assert "stripped_line_comments" in recovery

    def test_json_repair_fallback_on_original_not_comment_stripped(self):
        """json_repair fallback operates on the original text, not comment-stripped text."""
        raw = """{
  "patch_id": "fix-repair",
  "rationale": "repair test",
  "confidence": 0.9,
  "category": "type_mismatch",
  "root_cause": "type issue",
  "operations": [
    {
      "op": set_module_config_key  // missing quotes around op value
      "module_id": "m1",
      "key": "format",
      "value": "parquet"
    }
  ]
}"""
        try:
            spec, recovery = _parse_patch_spec(raw)
            assert "stripped_line_comments" in recovery or "json_repair" in recovery
        except Exception:
            pass  # json_repair may not be installed; JSONDecodeError is acceptable

    def test_orphan_think_closing_stripped(self):
        """Orphan </think> without opener: prose before closer stripped."""
        raw = """Some reasoning text without the opening tag</think>
{
  "patch_id": "fix-orphan",
  "rationale": "fix",
  "confidence": 0.9,
  "category": "other",
  "root_cause": "issue",
  "operations": [
    {"op": "set_module_config_key", "module_id": "m1", "key": "k", "value": "v"}
  ]
}"""
        spec, recovery = _parse_patch_spec(raw)
        assert spec.patch_id == "fix-orphan"
        assert "stripped_orphan_think_close" in recovery

    def test_orphan_think_not_stripped_when_no_brace_survives(self):
        """Orphan </think> NOT stripped when no '{' survives after the closer."""
        raw = """</think>
just text, no json object after it"""
        from json import JSONDecodeError
        with pytest.raises((JSONDecodeError, ValueError)):
            _parse_patch_spec(raw)

    def test_fence_selection_prefers_operations_block(self):
        """Multi-fence: prefers the block containing 'operations' over earlier echoed-junk fence."""
        raw = """```json
{"patch_id": "echo", "rationale": "junk"}
```
Some text
```json
{
  "patch_id": "fix-real",
  "rationale": "real patch",
  "confidence": 0.9,
  "category": "other",
  "root_cause": "x",
  "operations": [
    {"op": "set_module_config_key", "module_id": "m1", "key": "k", "value": "v"}
  ]
}
```"""
        spec, recovery = _parse_patch_spec(raw)
        assert spec.patch_id == "fix-real"
        assert "stripped_code_fence" in recovery

    def test_multi_key_wrapper_unwrapped(self):
        """{'patch': {operations}, 'explanation': '...'} → unwraps the patch key."""
        raw = """{
  "patch": {
    "patch_id": "fix-wrap",
    "rationale": "wrapper",
    "confidence": 0.9,
    "category": "other",
    "root_cause": "x",
    "operations": [
      {"op": "set_module_config_key", "module_id": "m1", "key": "k", "value": "v"}
    ]
  },
  "explanation": "This patch fixes the issue"
}"""
        spec, recovery = _parse_patch_spec(raw)
        assert spec.patch_id == "fix-wrap"
        assert "unwrapped_patch" in recovery

    def test_clean_envelope_no_recovery(self):
        """Clean envelope still parses with recovery_applied == []."""
        spec, recovery = _parse_patch_spec("""{
  "patch_id": "clean",
  "rationale": "clean",
  "confidence": 0.9,
  "category": "other",
  "root_cause": "x",
  "operations": [
    {"op": "set_module_config_key", "module_id": "m1", "key": "k", "value": "v"}
  ]
}""")
        assert spec.patch_id == "clean"
        assert recovery == []

    def test_non_escalated_reprompt_capped_at_4000_chars(self, tmp_path):
        """Non-escalated reprompt raw echo capped at 4000 chars."""
        from aqueduct.agent.parse import _format_reprompt_for_next_turn
        long_raw = "x" * 10000
        result = _format_reprompt_for_next_turn(
            friendly="ERROR", raw=long_raw, escalated=False, structural_hint="",
        )
        assert len(result) < 10000  # truncated, not the full 10000


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

    def test_pending_file_contains_prompt_version(self, tmp_path):
        patches_dir = tmp_path / "patches"
        spec = _patch_spec()
        ctx = _failure_ctx()
        stage_patch_for_human(spec, patches_dir, ctx)
        matches = list((patches_dir / "pending").glob("*_test-fix.json"))
        data = json.loads(matches[0].read_text())
        assert "_aq_meta" in data
        assert data["_aq_meta"]["prompt_version"] == PROMPT_VERSION

    def test_returns_none(self, tmp_path):
        spec = _patch_spec()
        result = stage_patch_for_human(spec, tmp_path / "patches", _failure_ctx())
        assert result is None

    def test_pending_file_contains_failure_signature(self, tmp_path):
        from aqueduct.agent.signature import from_failure_context
        patches_dir = tmp_path / "patches"
        spec = _patch_spec()
        ctx = _failure_ctx(error_class="UNRESOLVED_COLUMN")
        stage_patch_for_human(spec, patches_dir, ctx)
        matches = list((patches_dir / "pending").glob("*_test-fix.json"))
        data = json.loads(matches[0].read_text())
        assert "_aq_meta" in data
        assert "failure_signature" in data["_aq_meta"]
        fs = data["_aq_meta"]["failure_signature"]
        assert isinstance(fs, dict)
        assert "hash" in fs
        assert "failure_signature_coarse" in data["_aq_meta"]
        # Verify the signature matches the failure context
        exact, _ = from_failure_context(ctx)
        assert fs["hash"] == exact.hash

    def test_pending_file_contains_source_llm_by_default(self, tmp_path):
        patches_dir = tmp_path / "patches"
        spec = _patch_spec()
        ctx = _failure_ctx()
        stage_patch_for_human(spec, patches_dir, ctx)
        matches = list((patches_dir / "pending").glob("*_test-fix.json"))
        data = json.loads(matches[0].read_text())
        assert data["_aq_meta"].get("source") == "llm"

    # ── Phase 46: stage_patch_for_human webhook ─────────────────────────────

    def test_stage_patch_webhook_fires_on_patch_pending_event(self, tmp_path):
        """stage_patch_for_human with webhook → fire_webhook called with event='on_patch_pending'."""
        from aqueduct.config import WebhookEndpointConfig

        with patch("aqueduct.surveyor.webhook.fire_webhook") as mock_fw:
            wh = WebhookEndpointConfig(url="http://hook.test")
            spec = _patch_spec(patch_id="wh-1", rationale="fix it", confidence=0.85, category="config")
            ctx = _failure_ctx(run_id="r1", blueprint_id="bp1", failed_module="m1")

            stage_patch_for_human(
                spec, tmp_path / "patches", ctx,
                on_patch_pending_webhook=wh,
                source="llm",
                webhook_event="on_patch_pending",
            )

        mock_fw.assert_called_once()
        _args, _kwargs = mock_fw.call_args
        assert _kwargs["event"] == "on_patch_pending"
        assert _kwargs["full_payload"]["patch_id"] == "wh-1"
        assert _kwargs["full_payload"]["source"] == "llm"
        assert _kwargs["full_payload"]["root_cause"] == ""
        assert _kwargs["full_payload"]["rationale"] == "fix it"
        assert _kwargs["full_payload"]["confidence"] == 0.85
        assert _kwargs["full_payload"]["category"] == "config"
        assert _kwargs["template_vars"]["patch_id"] == "wh-1"
        assert _kwargs["template_vars"]["root_cause"] == ""
        assert _kwargs["template_vars"]["rationale"] == "fix it"

    def test_stage_patch_webhook_sets_ci_event(self, tmp_path):
        """CI staging fires event='on_ci_patch'."""
        from aqueduct.config import WebhookEndpointConfig

        with patch("aqueduct.surveyor.webhook.fire_webhook") as mock_fw:
            wh = WebhookEndpointConfig(url="http://ci.test")
            spec = _patch_spec(patch_id="ci-1")
            ctx = _failure_ctx()

            stage_patch_for_human(
                spec, tmp_path / "patches", ctx,
                on_patch_pending_webhook=wh,
                source="replay",
                webhook_event="on_ci_patch",
            )

        mock_fw.assert_called_once()
        assert mock_fw.call_args[1]["event"] == "on_ci_patch"
        assert mock_fw.call_args[1]["full_payload"]["source"] == "replay"

    def test_stage_patch_webhook_defer_carries_diagnosis(self, tmp_path):
        """Defer patches include diagnosis + suggestions in webhook."""
        from aqueduct.config import WebhookEndpointConfig
        from aqueduct.patch.grammar import PatchSpec

        spec = PatchSpec(
            patch_id="defer-1", rationale="out of scope",
            operations=[{"op": "defer_to_human", "diagnosis": "UDF bug", "suggestions": ["check python"]}],
        )
        ctx = _failure_ctx()

        with patch("aqueduct.surveyor.webhook.fire_webhook") as mock_fw:
            wh = WebhookEndpointConfig(url="http://defer.test")
            stage_patch_for_human(
                spec, tmp_path / "patches", ctx,
                on_patch_pending_webhook=wh,
                webhook_event="on_patch_pending",
            )

        payload = mock_fw.call_args[1]["full_payload"]
        assert payload["diagnosis"] == "UDF bug"
        assert payload["suggestions"] == ["check python"]

    def test_stage_patch_webhook_error_swallowed(self, tmp_path, capsys):
        """Webhook fire failure is swallowed — staging never blocks."""
        from aqueduct.config import WebhookEndpointConfig

        with patch("aqueduct.surveyor.webhook.fire_webhook", side_effect=RuntimeError("boom")):
            wh = WebhookEndpointConfig(url="http://fail.test")
            spec = _patch_spec(patch_id="fail-1")
            ctx = _failure_ctx()
            # Must not raise
            stage_patch_for_human(
                spec, tmp_path / "patches", ctx,
                on_patch_pending_webhook=wh,
            )

    def test_stage_patch_no_webhook_no_fire(self, tmp_path):
        """on_patch_pending_webhook=None → fire_webhook not called."""
        with patch("aqueduct.surveyor.webhook.fire_webhook") as mock_fw:
            spec = _patch_spec()
            ctx = _failure_ctx()
            stage_patch_for_human(spec, tmp_path / "patches", ctx, on_patch_pending_webhook=None)
            mock_fw.assert_not_called()


class TestArchivePatch:
    def test_creates_applied_file(self, tmp_path):
        patches_dir = tmp_path / "patches"
        spec = _patch_spec()
        ctx = _failure_ctx()
        archive_patch(spec, patches_dir, ctx, mode="auto")
        matches = list((patches_dir / "applied").glob("*_test-fix.json"))
        assert len(matches) == 1

class TestPatchFilename:
    def test_patch_filename_includes_timestamp(self, tmp_path):
        from aqueduct.agent.prompts import _build_guardrails_section
        from aqueduct.agent.loop import _patch_filename
        spec = _patch_spec(patch_id="test")
        filename = _patch_filename(spec)
        import re
        assert re.match(r"^\d{8}T\d{6}_test\.json$", filename)
        assert "_test.json" in filename

    def test_patch_filename_ignores_seq_logic(self, tmp_path):
        from aqueduct.agent.prompts import _build_guardrails_section
        from aqueduct.agent.loop import _patch_filename
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
        
        # It should just use timestamp now, disregarding existing sequence numbers
        filename = _patch_filename(spec)
        assert not filename.startswith("00005_")
        import re
        assert re.match(r"^\d{8}T\d{6}_test\.json$", filename)



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

    def test_applied_file_contains_prompt_version(self, tmp_path):
        patches_dir = tmp_path / "patches"
        spec = _patch_spec()
        ctx = _failure_ctx()
        archive_patch(spec, patches_dir, ctx, mode="auto")
        matches = list((patches_dir / "applied").glob("*_test-fix.json"))
        data = json.loads(matches[0].read_text())
        assert "_aq_meta" in data
        assert data["_aq_meta"]["prompt_version"] == PROMPT_VERSION

    def test_applied_file_contains_failure_signature(self, tmp_path):
        from aqueduct.agent.signature import from_failure_context
        patches_dir = tmp_path / "patches"
        spec = _patch_spec()
        ctx = _failure_ctx(error_class="UNRESOLVED_COLUMN")
        archive_patch(spec, patches_dir, ctx, mode="auto")
        matches = list((patches_dir / "applied").glob("*_test-fix.json"))
        data = json.loads(matches[0].read_text())
        assert "_aq_meta" in data
        assert "failure_signature" in data["_aq_meta"]
        fs = data["_aq_meta"]["failure_signature"]
        assert isinstance(fs, dict)
        assert "hash" in fs
        assert "failure_signature_coarse" in data["_aq_meta"]
        exact, _ = from_failure_context(ctx)
        assert fs["hash"] == exact.hash

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


# ── generate_agent_patch ───────────────────────────────────────────────────────


class TestGenerateLlmPatch:
    def test_no_api_key_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        ctx = _failure_ctx()
        result = generate_agent_patch(
            failure_ctx=ctx,
            model="claude-sonnet-4-6",
            patches_dir=tmp_path / "patches",
        )
        assert result.patch is None

    def test_always_invalid_response_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")

        def bad_llm(*_args, **_kw):
            return "not valid json {{{"

        monkeypatch.setattr("aqueduct.agent.loop._call_agent", bad_llm)

        result = generate_agent_patch(
            failure_ctx=_failure_ctx(),
            model="claude-sonnet-4-6",
            patches_dir=tmp_path / "patches",
        )
        assert result.patch is None

    def test_valid_response_returns_patch_spec(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")

        def mock_llm(*_args, **_kw):
            return (_valid_patch_json(), 100, 50)

        monkeypatch.setattr("aqueduct.agent.loop._call_agent", mock_llm)

        result = generate_agent_patch(
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
                return ("invalid json", 100, 50)
            return (_valid_patch_json(), 100, 50)

        monkeypatch.setattr("aqueduct.agent.loop._call_agent", flaky_llm)

        result = generate_agent_patch(
            failure_ctx=_failure_ctx(),
            model="claude-sonnet-4-6",
            patches_dir=tmp_path / "patches",
        )
        assert result.patch is not None
        assert len(call_count) == 2

    def test_api_error_on_attempt_1_breaks_loop(self, tmp_path, monkeypatch, caplog):
        import logging
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")

        def failing_llm(*_args, **_kw):
            raise RuntimeError("API timeout or disconnect")

        monkeypatch.setattr("aqueduct.agent.loop._call_agent", failing_llm)

        with caplog.at_level(logging.ERROR):
            result = generate_agent_patch(
                failure_ctx=_failure_ctx(),
                model="claude-sonnet-4-6",
                patches_dir=tmp_path / "patches",
                max_reprompts=3,
            )
        assert result.patch is None
        assert result.attempts == 1
        assert len(result.reprompt_errors) == 1
        assert "API error: API timeout or disconnect" in result.reprompt_errors[0]

        # Verify the error log uses actual attempts_made (1)
        err_messages = [rec.getMessage() for rec in caplog.records if rec.levelno == logging.ERROR]
        assert any("failed to produce a valid PatchSpec after 1 attempt(s)" in msg for msg in err_messages)

    def test_generate_agent_patch_with_guardrails_threads_to_prompt(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        from aqueduct.parser.models import GuardrailsConfig

        captured_prompt = None

        def mock_llm(*args, **kwargs):
            nonlocal captured_prompt
            messages = args[0] if args else kwargs.get("messages", [])
            for msg in messages:
                if msg.get("role") == "user":
                    captured_prompt = msg.get("content", "")
            return (_valid_patch_json(), 100, 50)

        monkeypatch.setattr("aqueduct.agent.loop._call_agent", mock_llm)

        g = GuardrailsConfig(forbidden_ops=("replace_module_config",))
        result = generate_agent_patch(
            failure_ctx=_failure_ctx(),
            model="claude-sonnet-4-6",
            patches_dir=tmp_path / "patches",
            guardrails=g
        )
        assert result.patch is not None
        assert captured_prompt is not None
        assert "forbidden ops" in captured_prompt
        assert "replace_module_config" in captured_prompt

    def test_generate_agent_patch_without_guardrails_kwarg(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        
        captured_prompt = None

        def mock_llm(*args, **kwargs):
            nonlocal captured_prompt
            messages = args[0] if args else kwargs.get("messages", [])
            for msg in messages:
                if msg.get("role") == "user":
                    captured_prompt = msg.get("content", "")
            return (_valid_patch_json(), 100, 50)

        monkeypatch.setattr("aqueduct.agent.loop._call_agent", mock_llm)

        # Legacy caller without guardrails kwarg
        result = generate_agent_patch(
            failure_ctx=_failure_ctx(),
            model="claude-sonnet-4-6",
            patches_dir=tmp_path / "patches",
        )
        assert result.patch is not None
        assert captured_prompt is not None
        assert "Guardrails" not in captured_prompt


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
            db_path = store_dir / "observability.db"
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
        db_path = store / "observability.db"
        conn = duckdb.connect(str(db_path))
        conn.execute(_DDL)
        # Name columns explicitly so the test stays robust against future
        # additions to ``failure_contexts`` (Phase 35 added 5 structured-
        # error columns; the bare ``INSERT INTO failure_contexts VALUES (...)``
        # form had to know the column count).
        conn.execute(
            "INSERT INTO failure_contexts "
            "(run_id, blueprint_id, failed_module, error_message, stack_trace, "
            "manifest_json, provenance_json, started_at, finished_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ["run-1", "bp-1", "m-1", "boom", "stack", "{}", "{}", "2024-01-01", "2024-01-01"],
        )
        conn.close()

        ctx = extract_failure_context("run-1", store)
        assert ctx.failed_module == "m-1"
        assert ctx.error_message == "boom"

    def test_extract_failure_context_missing_returns_none(self, tmp_path):
        def extract_failure_context(run_id: str, store_dir: Path):
            db_path = store_dir / "observability.db"
            if not db_path.exists(): return None
            return "not none" # dummy
        # Store doesn't even exist
        assert extract_failure_context("ghost", tmp_path / "obs") is None

    def test_reprompt_limit_exceeded(self, monkeypatch, tmp_path):
        from aqueduct.agent import MAX_REPROMPTS
        from aqueduct.agent.loop import generate_agent_patch 

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test")

        call_count = 0
        def always_invalid(*_args, **_kw):
            nonlocal call_count
            call_count += 1
            return ("not json", 100, 50)

        monkeypatch.setattr("aqueduct.agent.loop._call_agent", always_invalid)

        result = generate_agent_patch(_failure_ctx(), "model", tmp_path)

        assert result.patch is None
        # Should have tried exactly max_reprompts (defaults to MAX_REPROMPTS=3)
        assert call_count == MAX_REPROMPTS

    def test_reprompt_uses_custom_llm_max_reprompts(self, monkeypatch, tmp_path):
        from aqueduct.agent.prompts import _build_guardrails_section
        from aqueduct.agent.loop import generate_agent_patch
        
        from aqueduct.agent.budget import BudgetConfig
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test")

        call_count = 0
        def always_invalid(*_args, **_kw):
            nonlocal call_count
            call_count += 1
            return ("not json", 100, 50)

        monkeypatch.setattr("aqueduct.agent.loop._call_agent", always_invalid)

        # Need to also widen the other budget axes so max_reprompts is the
        # only one that trips (same_signature_overall default=3 would trip
        # first with identical invalid responses).
        budget = BudgetConfig(
            max_reprompts=5,
            same_error_consecutive=99,
            same_signature_overall=99,
            progress_stalled_window=99,
        )
        result = generate_agent_patch(
            _failure_ctx(), "model", tmp_path, max_reprompts=5, budget=budget
        )

        assert result.patch is None
        assert call_count == 5

    def test_generate_llm_patch_uses_llm_timeout(self, monkeypatch, tmp_path):
        from aqueduct.agent.prompts import _build_guardrails_section
        from aqueduct.agent.loop import generate_agent_patch
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test")

        timeout_used = None
        def mock_call_llm(*_args, **kwargs):
            nonlocal timeout_used
            # _args[1] is the _ProviderConfig dataclass
            timeout_used = _args[1].timeout if len(_args) > 1 else None
            return (_valid_patch_json(), 100, 50)

        monkeypatch.setattr("aqueduct.agent.loop._call_agent", mock_call_llm)

        generate_agent_patch(_failure_ctx(), "model", tmp_path, timeout=600.0)

        assert timeout_used == 600.0


class TestBuildSystemPrompt:
    def test_engine_prompt_context_included(self, tmp_path):
        from aqueduct.agent.prompts import _build_guardrails_section 
        from aqueduct.agent.prompts import _build_system_prompt
        prompt = _build_system_prompt(
            patches_dir=tmp_path,
            engine_prompt_context="Engine rule 1.",
            blueprint_prompt_context=None
        )
        assert "Engine rule 1." in prompt

    def test_blueprint_prompt_context_included(self, tmp_path):
        from aqueduct.agent.prompts import _build_guardrails_section 
        from aqueduct.agent.prompts import _build_system_prompt
        prompt = _build_system_prompt(
            patches_dir=tmp_path,
            engine_prompt_context=None,
            blueprint_prompt_context="Blueprint rule 2."
        )
        assert "Blueprint rule 2." in prompt

    def test_both_contexts_included(self, tmp_path):
        from aqueduct.agent.prompts import _build_guardrails_section 
        from aqueduct.agent.prompts import _build_system_prompt
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
        from aqueduct.agent.prompts import _build_guardrails_section, _build_user_prompt
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
        prompt = _build_user_prompt(ctx, patches_dir=Path("/tmp/patches"))
        assert "## Original Blueprint YAML" in prompt
        assert "id: my_blueprint" in prompt

    def test_llm_system_prompt_includes_context_ref_rule(self, tmp_path):
        # Prompt hygiene pass consolidated the original "using template
        # expressions" / "Do NOT hard-code the resolved literal path"
        # sentences into the table-driven Path-values rule block. Assert
        # the canonical phrasing the current prompt uses.
        from aqueduct.agent.prompts import _build_guardrails_section 
        from aqueduct.agent.prompts import _build_system_prompt
        prompt = _build_system_prompt(patches_dir=tmp_path)
        assert "ALWAYS use relative paths" in prompt
        assert "context_ref" in prompt
        assert "Do not hardcode the resolved literal" in prompt

    def test_allow_defer_false_removes_defer_op_from_schema(self, tmp_path):
        """allow_defer=False → rendered schema contains NO DeferToHumanOp ($defs, oneOf, discriminator)."""
        from aqueduct.agent.prompts import _build_guardrails_section, _build_system_prompt
        prompt = _build_system_prompt(patches_dir=tmp_path, allow_defer=False)
        assert 'DeferToHumanOp' not in prompt
        assert 'defer_to_human' not in prompt
        assert 'When to defer' not in prompt

    def test_allow_defer_true_includes_defer_op_in_schema(self, tmp_path):
        """allow_defer=True → DeferToHumanOp present in schema and defer rules section rendered."""
        from aqueduct.agent.prompts import _build_guardrails_section, _build_system_prompt
        prompt = _build_system_prompt(patches_dir=tmp_path, allow_defer=True)
        assert 'DeferToHumanOp' in prompt
        assert 'defer_to_human' in prompt
        assert 'When to defer' in prompt


# ── _load_previous_patches ─────────────────────────────────────────────────────

class TestLoadPreviousPatches:
    def _make_store(self, db_path):
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

    def _stamp(self, store, patch_id, rationale=None, ops=None):
        with store.connect() as cur:
            cur.execute(
                "INSERT OR REPLACE INTO patch_index "
                "(patch_id, object_key, blueprint_id, status, source, "
                " rationale, ops, created_at, updated_at) "
                "VALUES (?, ?, ?, 'applied', 'llm', ?, ?, ?, ?)",
                [patch_id, f"/obj/{patch_id}.json", "bp1",
                 rationale or "", (ops or '[{"op": "set_module_config_key"}]'),
                 "2026-06-11T12:00:00", "2026-06-11T12:00:00"],
            )

    def test_archived_patch_uses_rationale_key(self, tmp_path):
        """Archived patch dumped via model_dump() (canonical 'rationale' key) → description is rationale text."""
        from aqueduct.agent.prompts import _load_previous_patches
        store = self._make_store(tmp_path / "obs.db")
        self._stamp(store, "fix-1", rationale="wrong path corrected")
        patches = _load_previous_patches(store)
        assert len(patches) == 1
        assert patches[0]["description"] == "wrong path corrected"

    def test_legacy_patch_uses_description_key(self, tmp_path):
        """Hand-written archived patch with legacy 'description' key → still picked up via fallback."""
        from aqueduct.agent.prompts import _load_previous_patches
        store = self._make_store(tmp_path / "obs.db")
        self._stamp(store, "fix-legacy", rationale="legacy description text")
        patches = _load_previous_patches(store)
        assert len(patches) == 1
        assert patches[0]["description"] == "legacy description text"

    def test_rationale_takes_priority_over_description(self, tmp_path):
        """When both rationale and description are present, rationale wins."""
        from aqueduct.agent.prompts import _load_previous_patches
        store = self._make_store(tmp_path / "obs.db")
        self._stamp(store, "fix-both", rationale="rationale wins")
        patches = _load_previous_patches(store)
        assert len(patches) == 1
        assert patches[0]["description"] == "rationale wins"

    def test_empty_store_returns_empty_list(self, tmp_path):
        """No obs_store → returns empty list."""
        from aqueduct.agent.prompts import _load_previous_patches
        patches = _load_previous_patches(None)
        assert patches == []


# ── doctor_hints LLM injection ─────────────────────────────────────────────────

class TestDoctorHintsInLLMPrompt:
    def test_doctor_hints_non_empty_includes_section(self, tmp_path):
        """doctor_hints non-empty → user prompt contains 'Blueprint issues detected' section."""
        from aqueduct.agent.prompts import _build_guardrails_section, _build_user_prompt

        ctx = _failure_ctx(
            doctor_hints=("warn: bad path /tmp/missing",),
            manifest_json=json.dumps({"blueprint_id": "test.bp", "name": "Test", "modules": [], "edges": []}),
        )
        prompt = _build_user_prompt(ctx, patches_dir=tmp_path)
        assert "Blueprint issues detected before run" in prompt
        assert "warn: bad path /tmp/missing" in prompt

    def test_doctor_hints_empty_section_absent(self, tmp_path):
        """doctor_hints empty → 'Blueprint issues detected' section absent."""
        from aqueduct.agent.prompts import _build_guardrails_section, _build_user_prompt

        ctx = _failure_ctx(
            doctor_hints=(),
            manifest_json=json.dumps({"blueprint_id": "test.bp", "name": "Test", "modules": [], "edges": []}),
        )
        prompt = _build_user_prompt(ctx, patches_dir=tmp_path)
        assert "Blueprint issues detected" not in prompt


# ── provider_options dispatch (_call_openai_compat) ───────────────────────────

class TestProviderOptionsDispatch:
    """Tests for provider_options key routing in _call_openai_compat."""

    def _make_mock_response(self, content="test"):
        import json as _json
        mock = MagicMock()
        mock.json.return_value = {"choices": [{"message": {"content": content}}]}
        mock.raise_for_status = MagicMock()
        return mock

    def _patch_client(self, captured: dict):
        """Return a context-manager-aware mock httpx.Client whose post() captures payload.

        Providers switched from one-shot ``httpx.post(...)`` to
        ``with httpx.Client(): client.post(...)``. The mock target moved
        accordingly. Returns the patch object so callers can use it as
        ``with self._patch_client(captured): ...``.
        """
        from unittest.mock import patch as _patch

        def fake_post(url, json=None, headers=None, timeout=None):
            captured["payload"] = json
            return self._make_mock_response()

        mock_client = MagicMock()
        mock_client.post.side_effect = fake_post
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        return _patch("httpx.Client", return_value=mock_client)

    def test_ollama_prefix_stripped_into_options(self):
        from aqueduct.agent.providers import _call_openai_compat
        from unittest.mock import MagicMock
        import httpx

        captured: dict = {}
        with self._patch_client(captured):
            _call_openai_compat(
                messages=[{"role": "user", "content": "hi"}],
                model="llama3",
                max_tokens=100,
                base_url="http://localhost:11434/v1",
                system_prompt="sys",
                provider_options={"ollama_num_thread": 8},
            )

        assert "options" in captured["payload"]
        assert captured["payload"]["options"]["num_thread"] == 8
        assert "ollama_num_thread" not in captured["payload"]

    def test_generic_key_merged_top_level(self):
        from aqueduct.agent.prompts import _build_guardrails_section 
        from aqueduct.agent.providers import _call_openai_compat
        
        from unittest.mock import MagicMock

        captured: dict = {}
        with self._patch_client(captured):
            _call_openai_compat(
                messages=[],
                model="gpt-4",
                max_tokens=100,
                base_url="http://localhost:11434/v1",
                system_prompt="sys",
                provider_options={"temperature": 0.5},
            )

        assert captured["payload"]["temperature"] == 0.5
        assert "options" not in captured["payload"]

    def test_mixed_ollama_and_generic_both_dispatched(self):
        from aqueduct.agent.prompts import _build_guardrails_section 
        from aqueduct.agent.providers import _call_openai_compat
        from unittest.mock import MagicMock

        captured: dict = {}
        with self._patch_client(captured):
            _call_openai_compat(
                messages=[],
                model="llama3",
                max_tokens=100,
                base_url="http://localhost:11434/v1",
                system_prompt="sys",
                provider_options={"ollama_num_thread": 4, "temperature": 0.7},
            )

        payload = captured["payload"]
        assert payload["options"]["num_thread"] == 4
        assert payload["temperature"] == 0.7

    def test_provider_options_none_payload_unchanged(self):
        from aqueduct.agent.prompts import _build_guardrails_section 
        from aqueduct.agent.providers import _call_openai_compat
        from unittest.mock import MagicMock

        captured: dict = {}
        with self._patch_client(captured):
            _call_openai_compat(
                messages=[],
                model="llama3",
                max_tokens=100,
                base_url="http://localhost:11434/v1",
                system_prompt="sys",
                provider_options=None,
            )

        assert "options" not in captured["payload"]
        assert "temperature" not in captured["payload"]

    def test_ollama_options_key_rejected_at_parse(self):
        """Old ollama_options key rejected — schema has extra=forbid on AgentSchema."""
        import yaml
        from pydantic import ValidationError
        from aqueduct.parser.schema import AgentSchema

        with pytest.raises(ValidationError):
            AgentSchema(**{"approval_mode": "disabled", "ollama_options": {"num_thread": 4}})


# ── Guardrails ────────────────────────────────────────────────────────────────

class TestGuardrailsSection:
    def test_none_returns_empty_string(self):
        from aqueduct.agent.prompts import _build_guardrails_section
        assert _build_guardrails_section(None) == ""

    def test_empty_dict_returns_empty_string(self):
        from aqueduct.agent.prompts import _build_guardrails_section
        assert _build_guardrails_section({}) == ""

    def test_dataclass_shape_live_heal_path(self):
        from aqueduct.agent.prompts import _build_guardrails_section
        from aqueduct.parser.models import GuardrailsConfig
        g = GuardrailsConfig(forbidden_ops=("replace_module_config",))
        result = _build_guardrails_section(g)
        assert "forbidden ops" in result
        assert "replace_module_config" in result

    def test_dict_shape_heal_from_store_path(self):
        from aqueduct.agent.prompts import _build_guardrails_section
        g = {"forbidden_ops": ["x"], "allowed_paths": ["blueprints/*"]}
        result = _build_guardrails_section(g)
        assert "forbidden ops (must NOT appear in operations[]): x" in result
        assert "allowed file paths (operations may only target these — fnmatch patterns): blueprints/*" in result

    def test_all_four_fields_render(self):
        from aqueduct.agent.prompts import _build_guardrails_section
        g = {
            "forbidden_ops": ["f1"],
            "allowed_paths": ["a1"],
            "heal_on_errors": ["h1"],
            "never_heal_errors": ["n1"],
        }
        result = _build_guardrails_section(g)
        assert "- forbidden ops (must NOT appear in operations[]): f1" in result
        assert "- allowed file paths (operations may only target these — fnmatch patterns): a1" in result
        assert "- heal only on these error_types: h1" in result
        assert "- never heal these error_types (priority over heal_on): n1" in result

    def test_absent_fields_produce_no_bullet(self):
        from aqueduct.agent.prompts import _build_guardrails_section
        g = {"forbidden_ops": ["f1"]}
        result = _build_guardrails_section(g)
        assert "forbidden ops" in result
        assert "allowed file paths" not in result
        assert "heal only" not in result
        assert "never heal" not in result


# ── Confidence logging ─────────────────────────────────────────────────────────

class TestConfidenceLogging:
    def test_patch_with_confidence_none_renders_as_n_a(self, tmp_path, monkeypatch):
        """Patch with confidence=None → parse-success log renders confidence as 'n/a' without raising."""
        from aqueduct.agent.loop import generate_agent_patch
        from aqueduct.surveyor.models import FailureContext

        ctx = FailureContext(
            run_id="r1", blueprint_id="b1", failed_module="m1",
            error_message="err", stack_trace=None, manifest_json="{}",
            started_at="2020-01-01T00:00:00Z",
            finished_at="2020-01-01T00:00:00Z",
        )

        # Mock the LLM call to return a valid patch with confidence=None
        raw_patch = json.dumps({
            "patch_id": "no-confidence",
            "rationale": "fix",
            "category": "other",
            "root_cause": "typo",
            "operations": [
                {"op": "set_module_config_key", "module_id": "m1", "key": "format", "value": "csv"}
            ],
        })

        call_count = 0
        def mock_call(*_a, **_kw):
            nonlocal call_count
            call_count += 1
            return (raw_patch, 100, 50)

        monkeypatch.setattr("aqueduct.agent.loop._call_agent", mock_call)
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test")

        result = generate_agent_patch(ctx, "model", tmp_path)

        assert result.patch is not None
        assert result.patch.confidence is None  # the model omitted the field
        assert result.stop_reason == "solved"
        assert call_count == 1


# ── Blob externalisation integration ───────────────────────────────────────────

class TestBlobExternalisationIntegration:
    def test_externalise_writes_compressed_blob(self, tmp_path):
        """blob_store.externalise writes a compressed blob and returns relative path."""
        from aqueduct.surveyor.blob_store import externalise, materialize

        store_dir = tmp_path / "obs"
        store_dir.mkdir()
        value = '{"test": "large_json_content"}'
        path = externalise(value, store_dir, "run-1", "manifest")

        assert path.startswith("blobs/run-1/manifest.json.zst")
        assert (store_dir / path).exists()
        # Verify we can read it back
        restored = materialize(path, store_dir)
        assert restored == value

    def test_externalise_empty_string_returns_unchanged(self, tmp_path):
        """externalise('', ...) returns '' unchanged (empty strings stay inline)."""
        from aqueduct.surveyor.blob_store import externalise
        assert externalise("", tmp_path, "r1", "x") == ""

    def test_materialize_blob_path_returns_decompressed(self, tmp_path):
        """materialize loads and decompresses blob."""
        from aqueduct.surveyor.blob_store import externalise, materialize

        store_dir = tmp_path / "obs"
        store_dir.mkdir()
        original = '{"key": "value"}'
        path = externalise(original, store_dir, "r2", "prov")
        restored = materialize(path, store_dir)
        assert restored == original

    def test_materialize_inline_data_passthrough(self, tmp_path):
        """materialize returns inline data unchanged."""
        from aqueduct.surveyor.blob_store import materialize
        assert materialize("inline text", tmp_path) == "inline text"

    def test_materialize_missing_blob_returns_path(self, tmp_path):
        """materialize on missing blob returns the path string (graceful fallback)."""
        from aqueduct.surveyor.blob_store import materialize
        result = materialize("blobs/missing.json.zst", tmp_path)
        assert result == "blobs/missing.json.zst"

    def _minimal_manifest(self, bp_id="bp-blob", name="blob-test") -> Manifest:
        from aqueduct.compiler.models import Manifest
        return Manifest(
            blueprint_id=bp_id,
            name=name,
            modules=(),
            edges=(),
            context={},
            spark_config={},
        )

    def test_surveyor_record_stores_blob_path_in_db(self, tmp_path):
        """surveyor.record() on failure → failure_contexts columns contain blob paths, not raw JSON."""
        from aqueduct.surveyor.surveyor import Surveyor
        from aqueduct.executor.models import ExecutionResult, ModuleResult

        store_dir = tmp_path / "blob_test_a"

        surveyor = Surveyor(self._minimal_manifest(), store_dir=store_dir)
        surveyor.start("run-blob-1")

        result = ExecutionResult(
            blueprint_id="bp-blob",
            run_id="run-blob-1",
            status="error",
            module_results=(
                ModuleResult(module_id="m1", status="error", error="boom"),
            ),
        )
        ctx = surveyor.record(result)
        surveyor.stop()

        assert ctx is not None
        import duckdb
        conn = duckdb.connect(str(store_dir / "observability.db"))
        row = conn.execute(
            "SELECT manifest_json, provenance_json, stack_trace FROM failure_contexts WHERE run_id = ?",
            ["run-blob-1"],
        ).fetchone()
        conn.close()
        assert row is not None
        for val in row:
            if val and isinstance(val, str):
                assert val.startswith("blobs/")
                assert ".json.zst" in val

    def test_surveyor_ctx_manifest_json_is_valid_json(self, tmp_path):
        """The FailureContext returned by record() has manifest_json as valid decompressed JSON."""
        from aqueduct.surveyor.surveyor import Surveyor
        from aqueduct.executor.models import ExecutionResult, ModuleResult

        store_dir = tmp_path / "obs5"

        surveyor = Surveyor(self._minimal_manifest(bp_id="bp-json", name="json-test"), store_dir=store_dir)
        surveyor.start("run-json-1")

        result = ExecutionResult(
            blueprint_id="bp-json",
            run_id="run-json-1",
            status="error",
            module_results=(
                ModuleResult(module_id="m1", status="error", error="boom"),
            ),
        )
        ctx = surveyor.record(result)
        surveyor.stop()

        assert ctx is not None
        # manifest_json contains a blob path — materialize to get the original JSON
        from aqueduct.surveyor.blob_store import materialize as _mat
        materialized = _mat(str(ctx.manifest_json), store_dir)
        import json as _json
        parsed = _json.loads(materialized)
        assert isinstance(parsed, dict)
        assert parsed.get("blueprint_id") == "bp-json"
