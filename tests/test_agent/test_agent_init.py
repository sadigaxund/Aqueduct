"""Unit tests for aqueduct/agent/__init__.py and aqueduct/config.py — Phase 34."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

pytestmark = pytest.mark.unit

from aqueduct.agent import (
    AgentPatchResult,
    _detect_structural_error,
    _format_reprompt_for_next_turn,
    _PATCH_SKELETON,
    _REPROMPT_TEMPLATE,
    _REPROMPT_TEMPLATE_ESCALATED,
    generate_agent_patch,
    resolve_budget,
)
from aqueduct.agent.budget import BudgetConfig
from aqueduct.config import AgentBudgetConfig, AgentConnectionConfig


# ── _detect_structural_error ──────────────────────────────────────────────────

class TestDetectStructuralError:
    def _make_exc(self, json_str: str) -> ValidationError:
        from aqueduct.patch.grammar import PatchSpec
        try:
            PatchSpec.model_validate_json(json_str)
        except ValidationError as e:
            return e
        pytest.fail("Expected ValidationError")

    def test_non_validation_error_returns_none(self):
        assert _detect_structural_error(ValueError("x"), "{}") is None

    def test_operations_not_missing_returns_none(self):
        # Invalid op but operations is present
        raw = '{"patch_id": "p", "rationale": "r", "operations": [{}]}'
        exc = self._make_exc(raw)
        assert _detect_structural_error(exc, raw) is None

    def test_operations_missing_but_no_op_fields_at_root(self):
        raw = '{"patch_id": "p", "rationale": "r"}'
        exc = self._make_exc(raw)
        assert _detect_structural_error(exc, raw) is None

    def test_operations_missing_and_op_fields_at_root_returns_hint(self):
        raw = '{"op": "set_module_config_key", "module_id": "m1"}'
        exc = self._make_exc(raw)
        hint = _detect_structural_error(exc, raw)
        assert hint is not None
        assert "op fields" in hint
        assert "'op'" in hint
        assert "'module_id'" in hint
        assert "TOP level" in hint


# ── _format_reprompt_for_next_turn ───────────────────────────────────────────

class TestFormatReprompt:
    def test_not_escalated_no_hint_uses_standard_template(self):
        res = _format_reprompt_for_next_turn(
            friendly="ERROR", raw='{"x": 1}', escalated=False, structural_hint=""
        )
        assert "ERROR" in res
        assert '{"x": 1}' in res
        assert "skeleton" not in res.lower()

    def test_escalated_uses_escalated_template(self):
        res = _format_reprompt_for_next_turn(
            friendly="ERROR", raw='{"x": 1}', escalated=True, structural_hint=""
        )
        assert "ERROR" in res
        assert _PATCH_SKELETON in res
        # Raw evidence appears, but only inside the "DO NOT edit this" block —
        # small models forget what they sent across turns without it. The
        # skeleton remains the positive anchor.
        assert '{"x": 1}' in res
        assert "DO NOT edit" in res

    def test_structural_hint_forces_escalated_template(self):
        res = _format_reprompt_for_next_turn(
            friendly="ERROR", raw='{"x": 1}', escalated=False, structural_hint="HINT"
        )
        assert "ERROR" in res
        assert "HINT" in res
        assert _PATCH_SKELETON in res
        # Evidence echoed under the DO-NOT-edit label (see comment above).
        assert '{"x": 1}' in res
        assert "DO NOT edit" in res


# ── resolve_budget ────────────────────────────────────────────────────────────

class TestResolveBudget:
    def test_copies_from_agent_budget_config(self):
        abc = AgentBudgetConfig(max_reprompts=7, same_error_consecutive=4)
        b = resolve_budget(abc)
        assert b.max_reprompts == 7
        assert b.same_error_consecutive == 4

    def test_none_max_reprompts_7(self):
        b = resolve_budget(None, max_reprompts=7)
        assert b.max_reprompts == 7
        assert b.max_seconds == 120.0  # default

    def test_none_max_reprompts_0_clamps_to_1(self):
        b = resolve_budget(None, max_reprompts=0)
        assert b.max_reprompts == 1

    def test_none_max_reprompts_none_uses_defaults(self):
        b = resolve_budget(None, max_reprompts=None)
        assert b == BudgetConfig()


# ── AgentBudgetConfig ─────────────────────────────────────────────────────────

class TestAgentBudgetConfig:
    def test_frozen_and_extra_forbid(self):
        b = AgentBudgetConfig()
        with pytest.raises(Exception):
            b.max_reprompts = 9  # type: ignore[misc]
        with pytest.raises(ValidationError):
            AgentBudgetConfig(unknown_field=1)

    def test_defaults_match_dataclass(self):
        ac = AgentBudgetConfig()
        bc = BudgetConfig()
        assert ac.max_reprompts == bc.max_reprompts
        assert ac.max_seconds == bc.max_seconds

    def test_agent_connection_config_budget_default_is_none(self):
        cfg = AgentConnectionConfig()
        assert cfg.budget is None


# ── generate_agent_patch ──────────────────────────────────────────────────────

class TestGenerateAgentPatch:
    @pytest.fixture
    def mock_call(self):
        with patch("aqueduct.agent._call_agent") as m:
            yield m

    @pytest.fixture
    def fctx(self):
        from aqueduct.surveyor.models import FailureContext
        return FailureContext(
            run_id="run1", blueprint_id="bp1", failed_module="m1",
            error_message="msg", stack_trace="", manifest_json="{}",
            started_at="2020-01-01T00:00:00Z", finished_at="2020-01-01T00:00:00Z",
        )

    def test_single_attempt_success(self, mock_call, fctx, tmp_path):
        valid_json = '{"patch_id": "p", "rationale": "r", "operations": [{"op": "set_module_config_key", "module_id": "m1", "key": "k", "value": "v"}]}'
        mock_call.return_value = (valid_json, 10, 20)
        
        res = generate_agent_patch(fctx, "model", tmp_path)
        
        assert res.patch is not None
        assert res.attempts == 1
        assert res.stop_reason == "solved"
        assert len(res.attempt_records) == 1
        assert res.attempt_records[0].signature is None
        assert res.tokens_in_total == 10

    def test_two_attempt_success(self, mock_call, fctx, tmp_path):
        invalid_json = '{"patch_id": "p"}'
        valid_json = '{"patch_id": "p", "rationale": "r", "operations": [{"op": "set_module_config_key", "module_id": "m1", "key": "k", "value": "v"}]}'
        mock_call.side_effect = [
            (invalid_json, 10, 20),
            (valid_json, 15, 25),
        ]
        
        res = generate_agent_patch(fctx, "model", tmp_path, max_reprompts=3)
        
        assert res.patch is not None
        assert res.attempts == 2
        assert res.stop_reason == "solved"
        assert len(res.attempt_records) == 2
        assert res.attempt_records[0].signature is not None
        assert res.attempt_records[1].signature is None
        assert res.tokens_in_total == 25
        assert res.tokens_out_total == 45

    def test_apply_callback_gate_rejection(self, mock_call, fctx, tmp_path):
        valid_json = '{"patch_id": "p", "rationale": "r", "operations": [{"op": "set_module_config_key", "module_id": "m1", "key": "k", "value": "v"}]}'
        mock_call.return_value = (valid_json, 10, 20)
        
        cb_calls = 0
        def _cb(p):
            nonlocal cb_calls
            cb_calls += 1
            if cb_calls == 1:
                return (False, "guardrail_violation", "msg", None)
            return (True, None, None, None)
            
        res = generate_agent_patch(fctx, "model", tmp_path, max_reprompts=3, apply_callback=_cb)
        
        assert res.attempts == 2
        assert cb_calls == 2
        assert res.attempt_records[0].gate_that_rejected == "apply"
        assert res.attempt_records[0].signature.error_class == "guardrail_violation"

    def test_apply_callback_exception_caught(self, mock_call, fctx, tmp_path):
        valid_json = '{"patch_id": "p", "rationale": "r", "operations": [{"op": "set_module_config_key", "module_id": "m1", "key": "k", "value": "v"}]}'
        mock_call.return_value = (valid_json, 10, 20)
        
        def _cb(p):
            raise RuntimeError("unexpected")
            
        res = generate_agent_patch(fctx, "model", tmp_path, max_reprompts=1, apply_callback=_cb)
        
        assert res.patch is None
        assert res.attempts == 1
        assert res.attempt_records[0].gate_that_rejected == "apply"
        assert res.attempt_records[0].signature.error_class == "RuntimeError"

    def test_provider_exception_caught(self, mock_call, fctx, tmp_path):
        mock_call.side_effect = Exception("api down")
        
        res = generate_agent_patch(fctx, "model", tmp_path)
        
        assert res.patch is None
        assert res.stop_reason == "api_error"
        assert len(res.attempt_records) == 1
        assert res.attempt_records[0].gate_that_rejected == "provider"

    def test_on_attempt_invoked(self, mock_call, fctx, tmp_path):
        mock_call.return_value = ('{"patch_id": "p", "rationale": "r", "operations": [{"op": "set_module_config_key", "module_id": "m1", "key": "k", "value": "v"}]}', 10, 20)
        
        cb_calls = 0
        def _cb(rec):
            nonlocal cb_calls
            cb_calls += 1
            
        generate_agent_patch(fctx, "model", tmp_path, on_attempt=_cb)
        
        assert cb_calls == 1

    def test_stuck_consecutive_escalation(self, mock_call, fctx, tmp_path):
        invalid_json = '{"patch_id": "p"}'
        mock_call.return_value = (invalid_json, 10, 20)
        
        budget = BudgetConfig(max_reprompts=5, same_error_consecutive=2, same_signature_overall=10)
        res = generate_agent_patch(fctx, "model", tmp_path, budget=budget)
        
        assert res.attempts == 3  # 1st fail, 2nd fail (trips consecutive), 3rd is escalated fail (aborts)
        assert res.escalated is True
        assert res.attempt_records[2].escalated is True


# ── _call_anthropic / _call_openai_compat ────────────────────────────────────

class TestCallProviders:
    """Provider transport tests.

    Both providers were switched from one-shot ``httpx.post(...)`` to
    ``with httpx.Client(): client.post(...)`` so the socket is torn down
    on Ctrl+C. The mock target is now ``httpx.Client`` — calls go through
    the context manager's ``post()`` method. Per CLAUDE.md no live LLM
    calls in pytest; all four tests are network-free.
    """

    def _mock_client(self, json_payload: dict):
        mock_resp = MagicMock()
        mock_resp.json.return_value = json_payload
        mock_resp.raise_for_status = MagicMock()
        mock_client = MagicMock()
        mock_client.post.return_value = mock_resp
        # `with httpx.Client(...) as client:` resolves to the mock client.
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        return mock_client, mock_resp

    @patch("httpx.Client")
    def test_call_anthropic_returns_tuple(self, mock_client_cls, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
        mock_client, _ = self._mock_client({
            "content": [{"text": "hello"}],
            "usage": {"input_tokens": 10, "output_tokens": 20},
        })
        mock_client_cls.return_value = mock_client

        from aqueduct.agent import _call_anthropic
        text, tin, tout = _call_anthropic([], "model", 100, "system")
        assert text == "hello"
        assert tin == 10
        assert tout == 20

    @patch("httpx.Client")
    def test_call_anthropic_temperature_override(self, mock_client_cls, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
        mock_client, _ = self._mock_client({"content": [{"text": "hello"}]})
        mock_client_cls.return_value = mock_client

        from aqueduct.agent import _call_anthropic
        _call_anthropic([], "model", 100, "system", temperature_override=0.8)

        kwargs = mock_client.post.call_args[1]
        assert kwargs["json"]["temperature"] == 0.8

    @patch("httpx.Client")
    def test_call_openai_compat_returns_tuple(self, mock_client_cls):
        mock_client, _ = self._mock_client({
            "choices": [{"message": {"content": "hello"}}],
            "usage": {"prompt_tokens": 15, "completion_tokens": 25},
        })
        mock_client_cls.return_value = mock_client

        from aqueduct.agent import _call_openai_compat
        text, tin, tout = _call_openai_compat([], "model", 100, "http://test", "system")
        assert text == "hello"
        assert tin == 15
        assert tout == 25

    @patch("httpx.Client")
    def test_call_openai_compat_temperature_override(self, mock_client_cls):
        mock_client, _ = self._mock_client(
            {"choices": [{"message": {"content": "hello"}}]}
        )
        mock_client_cls.return_value = mock_client

        from aqueduct.agent import _call_openai_compat
        _call_openai_compat(
            [], "model", 100, "http://test", "system",
            provider_options={"ollama_temperature": 0.1},
            temperature_override=0.8,
        )

        kwargs = mock_client.post.call_args[1]
        assert kwargs["json"]["temperature"] == 0.8
        assert kwargs["json"]["options"]["temperature"] == 0.8


# ── Phase 34/35 coverage additions ────────────────────────────────────────────


_VALID_PATCH_JSON = (
    '{"patch_id": "p", "rationale": "r", "operations": '
    '[{"op": "set_module_config_key", "module_id": "m1", "key": "k", "value": "v"}]}'
)


def _make_fctx(**overrides):
    """Build a FailureContext with sane defaults; overrides update keyword args."""
    from aqueduct.surveyor.models import FailureContext

    base = dict(
        run_id="run1",
        blueprint_id="bp1",
        failed_module="m1",
        error_message="msg",
        stack_trace="",
        manifest_json="{}",
        started_at="2020-01-01T00:00:00Z",
        finished_at="2020-01-01T00:00:00Z",
    )
    base.update(overrides)
    return FailureContext(**base)


class TestGenerateAgentPatchPhase34Extra:
    """Additional Phase 34 coverage on top of TestGenerateAgentPatch."""

    @pytest.fixture
    def fctx(self):
        return _make_fctx()

    def test_budget_none_synthesizes_from_max_reprompts(self, fctx, tmp_path):
        # When budget=None, the loop must synthesize a BudgetConfig with the
        # caller's max_reprompts (clamped to >=1) and dataclass defaults for
        # the other axes. Verify by: (a) solving on attempt 1 — confirms the
        # synthesized budget did not abort prematurely; (b) inspecting that
        # the stop reason is "solved" and only 1 LLM call was made.
        with patch("aqueduct.agent._call_agent") as mock_call:
            mock_call.return_value = (_VALID_PATCH_JSON, 10, 20)
            res = generate_agent_patch(
                fctx, "model", tmp_path, budget=None, max_reprompts=4
            )
        assert res.attempts == 1
        assert res.stop_reason == "solved"

    def test_apply_callback_success_returns_solved(self, fctx, tmp_path):
        cb_calls = 0

        def _cb(p):
            nonlocal cb_calls
            cb_calls += 1
            return (True, None, None, None)

        with patch("aqueduct.agent._call_agent") as mock_call:
            mock_call.return_value = (_VALID_PATCH_JSON, 10, 20)
            res = generate_agent_patch(
                fctx, "model", tmp_path, apply_callback=_cb, max_reprompts=3
            )
        assert res.stop_reason == "solved"
        assert res.attempts == 1
        assert cb_calls == 1
        assert res.patch is not None

    def test_token_totals_accumulate_across_attempts(self, fctx, tmp_path):
        invalid_json = '{"patch_id": "p"}'
        with patch("aqueduct.agent._call_agent") as mock_call:
            mock_call.side_effect = [
                (invalid_json, 100, 50),
                (_VALID_PATCH_JSON, 80, 60),
            ]
            res = generate_agent_patch(
                fctx, "model", tmp_path, max_reprompts=3
            )
        assert res.tokens_in_total == 180
        assert res.tokens_out_total == 110


class TestAgentPatchResultBackwardCompat:
    def test_agent_patch_result_positional_kwargs_backward_compat(self):
        from aqueduct.agent import AgentPatchResult
        from aqueduct.patch.grammar import PatchSpec

        p = PatchSpec(
            patch_id="p", rationale="r",
            operations=[{"op": "set_module_config_key",
                         "module_id": "m1", "key": "k", "value": "v"}],
        )
        r = AgentPatchResult(patch=p, attempts=1)
        assert r.stop_reason is None
        assert r.tokens_in_total == 0
        assert r.tokens_out_total == 0
        assert r.attempt_records == []
        assert r.escalated is False


class TestFormatRepromptExtra:
    def test_escalated_template_contains_patch_skeleton_verbatim(self):
        res = _format_reprompt_for_next_turn(
            friendly="ERR", raw='{"x": 1}',
            escalated=True, structural_hint="",
        )
        assert _PATCH_SKELETON in res


class TestBuildRootCauseSection:
    """Coverage for aqueduct.agent._build_root_cause_section (Phase 35)."""

    def test_no_structured_fields_renders_stack_trace(self):
        from aqueduct.agent import _build_root_cause_section

        ctx = _make_fctx(stack_trace="Traceback line\n  at foo.bar(Foo.java:42)")
        out = _build_root_cause_section(ctx)
        assert "## Stack trace" in out
        assert "Traceback line" in out
        assert "at foo.bar(Foo.java:42)" in out

    def test_no_structured_and_no_stack_trace_renders_placeholder(self):
        from aqueduct.agent import _build_root_cause_section

        ctx = _make_fctx(stack_trace=None)
        out = _build_root_cause_section(ctx)
        assert "## Stack trace" in out
        assert "(no stack trace)" in out

    def test_error_class_only_omits_stack_trace(self):
        from aqueduct.agent import _build_root_cause_section

        ctx = _make_fctx(
            stack_trace="should-not-appear",
            error_class="UNRESOLVED_COLUMN.WITH_SUGGESTION",
        )
        out = _build_root_cause_section(ctx)
        assert "## Root cause (structured)" in out
        assert "- **Error class**:" in out
        assert "UNRESOLVED_COLUMN.WITH_SUGGESTION" in out
        assert "## Stack trace" not in out

    def test_suggested_columns_rendered_as_backticked_list(self):
        from aqueduct.agent import _build_root_cause_section

        ctx = _make_fctx(suggested_columns=("a", "b"))
        out = _build_root_cause_section(ctx)
        assert "- **Actual columns available**: `a`, `b`" in out

    def test_root_exception_renders_type_and_message(self):
        from aqueduct.agent import _build_root_cause_section

        ctx = _make_fctx(root_exception={"type": "ValueError", "message": "boom"})
        out = _build_root_cause_section(ctx)
        assert "ValueError" in out
        assert "boom" in out

        # Type only — empty message: type still present
        ctx2 = _make_fctx(root_exception={"type": "ValueError", "message": ""})
        out2 = _build_root_cause_section(ctx2)
        assert "ValueError" in out2
        # No trailing " — " when message is empty
        assert "— " not in out2.split("ValueError", 1)[1].splitlines()[0]

    def test_object_name_and_sql_state_both_rendered(self):
        from aqueduct.agent import _build_root_cause_section

        ctx = _make_fctx(object_name="event_ts", sql_state="42703")
        out = _build_root_cause_section(ctx)
        assert "- **Offending object**: `event_ts`" in out
        assert "- **SQL state**: `42703`" in out


class TestUserPromptTemplate:
    def test_user_prompt_template_lacks_truncated_marker(self):
        from aqueduct.agent import _USER_PROMPT_TEMPLATE
        assert "Stack trace (truncated)" not in _USER_PROMPT_TEMPLATE

    def test_user_prompt_template_has_root_cause_placeholder(self):
        from aqueduct.agent import _USER_PROMPT_TEMPLATE
        assert "{root_cause_section}" in _USER_PROMPT_TEMPLATE

    def test_truncate_stack_and_constant_not_importable(self):
        import aqueduct.agent as agent_mod
        # Phase 35 retired _truncate_stack + _STACK_TRACE_MAX_LINES; the raw
        # stack trace is now rendered verbatim by _build_root_cause_section.
        assert not hasattr(agent_mod, "_truncate_stack")
        assert not hasattr(agent_mod, "_STACK_TRACE_MAX_LINES")

    def test_build_user_prompt_with_structured_ctx_uses_root_cause_block(self, tmp_path):
        from aqueduct.agent import _build_user_prompt

        ctx = _make_fctx(
            error_class="X",
            stack_trace="should-not-appear",
        )
        out = _build_user_prompt(ctx, tmp_path)
        assert "Root cause (structured)" in out
        assert "## Stack trace" not in out

    def test_build_user_prompt_with_legacy_ctx_uses_raw_trace(self, tmp_path):
        from aqueduct.agent import _build_user_prompt

        trace = "line1\nline2\nline3"
        ctx = _make_fctx(stack_trace=trace)
        out = _build_user_prompt(ctx, tmp_path)
        assert trace in out
        # Phase 35 removed truncation — no marker should appear in the output.
        assert "... (truncated)" not in out


class TestLoadConfigTwoPass:
    def test_load_config_two_pass_survives_budget_block(self, tmp_path):
        from aqueduct.config import load_config

        cfg_path = tmp_path / "aqueduct.yml"
        cfg_path.write_text(
            'aqueduct_config: "1.0"\n'
            "agent:\n"
            "  budget:\n"
            "    max_reprompts: 8\n"
            "    max_seconds: 200\n"
        )
        cfg = load_config(cfg_path)
        assert cfg.agent.budget is not None
        assert cfg.agent.budget.max_reprompts == 8
        assert cfg.agent.budget.max_seconds == 200
