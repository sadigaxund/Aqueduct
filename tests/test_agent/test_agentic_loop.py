"""End-to-end (mocked httpx) tests for agentic-mode heal — the tool-use loop
integrated into ``generate_agent_patch`` (Phase 75, design item 4).

No live LLM calls (CLAUDE.md) — every test mocks ``httpx.Client``.
"""

from __future__ import annotations

import json
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from aqueduct.agent.loop import AgentRunConfig, generate_agent_patch
from aqueduct.agent.toolbox import ToolBox
from aqueduct.surveyor.models import FailureContext

pytestmark = pytest.mark.unit


def _failure_ctx() -> FailureContext:
    return FailureContext(
        run_id="r1", blueprint_id="bp1", failed_module="ingress1",
        error_message="path not found", stack_trace=None,
        manifest_json=json.dumps({
            "blueprint_id": "bp1",
            "modules": [{"id": "ingress1", "type": "Ingress", "config": {}}],
            "edges": [],
        }),
        started_at="2026-01-01T00:00:00Z", finished_at="2026-01-01T00:00:01Z",
        blueprint_source_yaml="id: bp1\nmodules: []\n",
    )


def _toolbox(fc: FailureContext) -> ToolBox:
    manifest = types.SimpleNamespace(modules=(), base_dir="")
    return ToolBox(manifest=manifest, failure_ctx=fc)


_VALID_PATCH = {
    "patch_id": "fix-1",
    "rationale": "fix path",
    "confidence": 0.9,
    "category": "bad_path",
    "root_cause": "wrong path",
    "operations": [
        {"op": "set_module_config_key", "module_id": "ingress1", "key": "path", "value": "y.csv"},
    ],
}


def _mock_client_sequence(responses: list[dict]):
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


class TestAgenticModeAnthropic:
    @patch("httpx.Client")
    def test_tool_call_then_patch(self, mock_client_cls, monkeypatch, tmp_path):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
        fc = _failure_ctx()
        mock_client_cls.return_value = _mock_client_sequence([
            {
                "content": [{"type": "tool_use", "id": "tu1", "name": "read_blueprint", "input": {}}],
                "usage": {"input_tokens": 100, "output_tokens": 20},
            },
            {
                "content": [{"type": "text", "text": json.dumps(_VALID_PATCH)}],
                "usage": {"input_tokens": 50, "output_tokens": 30},
            },
        ])

        result = generate_agent_patch(
            agent_cfg=AgentRunConfig(
                failure_ctx=fc, model="claude-x", patches_dir=tmp_path / "patches",
                provider="anthropic", toolbox=_toolbox(fc), mode="agentic",
            ),
        )

        assert result.patch is not None
        assert result.patch.patch_id == "fix-1"
        assert result.stop_reason == "solved"
        assert result.attempts == 1
        assert result.attempt_records[0].tool_calls == 1

    @patch("httpx.Client")
    def test_oneshot_mode_ignores_toolbox(self, mock_client_cls, monkeypatch, tmp_path):
        """mode='oneshot' (the default) must never send tools even if a
        ToolBox is supplied — a caller that resolved agentic but degrades
        (e.g. cascade tier override) must fall back safely."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
        fc = _failure_ctx()
        mock_client_cls.return_value = _mock_client_sequence([
            {"content": [{"text": json.dumps(_VALID_PATCH)}], "usage": {"input_tokens": 10, "output_tokens": 10}},
        ])

        result = generate_agent_patch(
            agent_cfg=AgentRunConfig(
                failure_ctx=fc, model="claude-x", patches_dir=tmp_path / "patches",
                provider="anthropic", toolbox=_toolbox(fc), mode="oneshot",
            ),
        )
        assert result.patch is not None
        assert result.attempt_records[0].tool_calls == 0
        # No "tools" key should have been sent on the single POST.
        call_kwargs = mock_client_cls.return_value.post.call_args[1]
        assert "tools" not in call_kwargs["json"]

    @patch("httpx.Client")
    def test_reprompt_turn_resets_per_attempt_tool_cap(self, mock_client_cls, monkeypatch, tmp_path):
        """A reprompt after an invalid first patch gets its OWN fresh
        max_tool_calls budget (design item 4 — caps compose per-attempt)."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test")
        fc = _failure_ctx()
        mock_client_cls.return_value = _mock_client_sequence([
            # Attempt 1: one tool call, then an INVALID patch (missing operations).
            {
                "content": [{"type": "tool_use", "id": "tu1", "name": "read_blueprint", "input": {}}],
                "usage": {"input_tokens": 10, "output_tokens": 5},
            },
            {
                "content": [{"type": "text", "text": '{"patch_id": "bad"}'}],
                "usage": {"input_tokens": 10, "output_tokens": 5},
            },
            # Attempt 2 (reprompt): one tool call again, then a valid patch.
            {
                "content": [{"type": "tool_use", "id": "tu2", "name": "read_blueprint", "input": {}}],
                "usage": {"input_tokens": 10, "output_tokens": 5},
            },
            {
                "content": [{"type": "text", "text": json.dumps(_VALID_PATCH)}],
                "usage": {"input_tokens": 10, "output_tokens": 5},
            },
        ])

        result = generate_agent_patch(
            agent_cfg=AgentRunConfig(
                failure_ctx=fc, model="claude-x", patches_dir=tmp_path / "patches",
                provider="anthropic", toolbox=_toolbox(fc), mode="agentic",
                max_reprompts=3,
            ),
        )
        assert result.patch is not None
        assert result.attempts == 2
        # Each attempt independently made exactly 1 tool call.
        assert [r.tool_calls for r in result.attempt_records] == [1, 1]
