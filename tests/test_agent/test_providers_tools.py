"""Tests for agentic-mode tool-use provider dispatch (Phase 75).

Follows the same ``httpx.Client`` mocking convention as
``test_agent_init.py::TestCallProviders`` — no live LLM calls (CLAUDE.md).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from aqueduct.agent.providers import ToolCallState, _call_anthropic, _call_openai_compat

pytestmark = pytest.mark.unit


class _FakeToolBox:
    """Minimal ToolBox stand-in — records calls, returns a canned result."""

    def __init__(self, result=None):
        self.calls: list[tuple[str, dict]] = []
        self._result = result if result is not None else {"ok": True}

    def declarations(self):
        return [
            {
                "name": "read_blueprint",
                "description": "the blueprint yaml",
                "params_schema": {"type": "object", "properties": {}},
            },
        ]

    def call(self, name, args):
        self.calls.append((name, dict(args)))
        return self._result


def _mock_client_sequence(responses: list[dict]):
    """A ``with httpx.Client() as client:`` mock that returns each response
    in order across successive ``.post()`` calls."""
    mock_resps = []
    for payload in responses:
        r = MagicMock()
        r.json.return_value = payload
        r.raise_for_status = MagicMock()
        mock_resps.append(r)
    mock_client = MagicMock()
    mock_client.post.side_effect = mock_resps
    mock_client.__enter__.return_value = mock_client
    mock_client.__exit__.return_value = False
    return mock_client


class TestAnthropicToolUse:
    @patch("httpx.Client")
    def test_single_tool_call_then_final_answer(self, mock_client_cls, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
        toolbox = _FakeToolBox(result={"yaml": "id: bp\n"})
        tool_state = ToolCallState()

        mock_client_cls.return_value = _mock_client_sequence([
            {
                "content": [{"type": "tool_use", "id": "tu1", "name": "read_blueprint", "input": {}}],
                "usage": {"input_tokens": 100, "output_tokens": 10},
            },
            {
                "content": [{"type": "text", "text": '{"patch_id": "fix-1"}'}],
                "usage": {"input_tokens": 50, "output_tokens": 20},
            },
        ])

        text, tin, tout = _call_anthropic(
            [{"role": "user", "content": "fix it"}], "model", 100, "system",
            tools=toolbox.declarations(), toolbox=toolbox, max_tool_calls=8,
            tool_state=tool_state,
        )

        assert text == '{"patch_id": "fix-1"}'
        assert tin == 150  # accumulated across both round-trips
        assert tout == 30
        assert tool_state.tool_calls_used == 1
        assert tool_state.supported is True
        assert toolbox.calls == [("read_blueprint", {})]

    @patch("httpx.Client")
    def test_no_tool_use_returns_immediately(self, mock_client_cls, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
        toolbox = _FakeToolBox()
        tool_state = ToolCallState()
        mock_client_cls.return_value = _mock_client_sequence([
            {"content": [{"type": "text", "text": "done"}], "usage": {}},
        ])

        text, tin, tout = _call_anthropic(
            [], "model", 100, "system",
            tools=toolbox.declarations(), toolbox=toolbox, max_tool_calls=8,
            tool_state=tool_state,
        )
        assert text == "done"
        assert tool_state.tool_calls_used == 0
        assert toolbox.calls == []

    @patch("httpx.Client")
    def test_max_tool_calls_forces_final_no_tools_turn(self, mock_client_cls, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
        toolbox = _FakeToolBox()
        tool_state = ToolCallState()

        # Two tool_use turns (exceeding max_tool_calls=1 after the first),
        # then a forced final text answer.
        mock_client_cls.return_value = _mock_client_sequence([
            {"content": [{"type": "tool_use", "id": "tu1", "name": "read_blueprint", "input": {}}], "usage": {}},
            {"content": [{"type": "text", "text": "final"}], "usage": {}},
        ])

        text, _, _ = _call_anthropic(
            [], "model", 100, "system",
            tools=toolbox.declarations(), toolbox=toolbox, max_tool_calls=1,
            tool_state=tool_state,
        )
        assert text == "final"
        assert tool_state.tool_calls_used == 1
        # Second POST must have been made WITHOUT "tools" in the payload.
        second_call_kwargs = mock_client_cls.return_value.post.call_args_list[1][1]
        assert "tools" not in second_call_kwargs["json"]

    @patch("httpx.Client")
    def test_no_toolbox_is_byte_identical_to_pre_phase_75(self, mock_client_cls, monkeypatch):
        """tools=None, toolbox=None must behave exactly like the old call shape."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
        mock_client_cls.return_value = _mock_client_sequence([
            {"content": [{"text": "hello"}], "usage": {"input_tokens": 1, "output_tokens": 1}},
        ])
        text, tin, tout = _call_anthropic([], "model", 100, "system")
        assert text == "hello"


class TestOpenAICompatToolUse:
    @patch("httpx.Client")
    def test_single_tool_call_then_final_answer(self, mock_client_cls):
        toolbox = _FakeToolBox(result={"yaml": "id: bp\n"})
        tool_state = ToolCallState()

        mock_client_cls.return_value = _mock_client_sequence([
            {
                "choices": [{"message": {
                    "content": None,
                    "tool_calls": [{
                        "id": "call_1",
                        "function": {"name": "read_blueprint", "arguments": "{}"},
                    }],
                }}],
                "usage": {"prompt_tokens": 40, "completion_tokens": 5},
            },
            {
                "choices": [{"message": {"content": '{"patch_id": "fix-2"}', "tool_calls": None}}],
                "usage": {"prompt_tokens": 20, "completion_tokens": 10},
            },
        ])

        text, tin, tout = _call_openai_compat(
            [{"role": "user", "content": "fix it"}], "model", 100, "http://test", "system",
            tools=toolbox.declarations(), toolbox=toolbox, max_tool_calls=8,
            tool_state=tool_state,
        )
        assert text == '{"patch_id": "fix-2"}'
        assert tin == 60
        assert tout == 15
        assert tool_state.tool_calls_used == 1
        assert toolbox.calls == [("read_blueprint", {})]

    @patch("httpx.Client")
    def test_capability_probe_degrades_and_warns_once(self, mock_client_cls, caplog):
        import httpx

        toolbox = _FakeToolBox()
        tool_state = ToolCallState()

        reject_resp = MagicMock()
        reject_resp.status_code = 400
        reject_resp.text = '{"error": "unsupported parameter: tools"}'

        def _raise(*a, **k):
            raise httpx.HTTPStatusError("bad request", request=MagicMock(), response=reject_resp)

        reject_resp.raise_for_status.side_effect = _raise

        ok_resp = MagicMock()
        ok_resp.status_code = 200
        ok_resp.json.return_value = {
            "choices": [{"message": {"content": '{"patch_id": "fix-3"}', "tool_calls": None}}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 5},
        }
        ok_resp.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.post.side_effect = [reject_resp, ok_resp]
        mock_client_cls.return_value = mock_client

        with caplog.at_level("WARNING"):
            text, _, _ = _call_openai_compat(
                [], "model", 100, "http://test", "system",
                tools=toolbox.declarations(), toolbox=toolbox, max_tool_calls=8,
                supports_tools="auto", tool_state=tool_state,
            )

        assert text == '{"patch_id": "fix-3"}'
        assert tool_state.supported is False
        assert any("agent_tools_unsupported" in r.message for r in caplog.records)
        # Second POST must not carry "tools".
        second_kwargs = mock_client.post.call_args_list[1][1]
        assert "tools" not in second_kwargs["json"]

    @patch("httpx.Client")
    def test_no_toolbox_is_byte_identical_to_pre_phase_75(self, mock_client_cls):
        mock_client_cls.return_value = _mock_client_sequence([
            {"choices": [{"message": {"content": "hello"}}], "usage": {}},
        ])
        text, _, _ = _call_openai_compat([], "model", 100, "http://test", "system")
        assert text == "hello"
