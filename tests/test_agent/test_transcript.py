"""Tests for aqueduct.agent.transcript — healing conversation display."""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from aqueduct.agent.budget import AttemptRecord
from aqueduct.agent.transcript import TranscriptWriter

pytestmark = pytest.mark.unit


def _rec(**kwargs) -> AttemptRecord:
    defaults = {
        "attempt_num": 1,
        "signature": None,
        "tokens_in": 0,
        "tokens_out": 0,
        "latency_ms": 0,
        "gate_that_rejected": None,
        "escalated": False,
        "model_cascade_position": None,
        "tool_calls": 0,
    }
    defaults.update(kwargs)
    return AttemptRecord(**defaults)


class TestTranscriptWriterTerse:
    def test_terse_one_liner(self):
        lines: list[str] = []
        tw = TranscriptWriter(verbose=False, write=lines.append)
        rec = _rec(attempt_num=2, tokens_in=500, tokens_out=800, gate_that_rejected="schema")
        tw.write(rec, None, model="gpt-4o")
        output = " ".join(lines)
        assert "turn 2" in output
        assert "500 in → 800 out" in output
        assert "invalid patch (schema)" in output

    def test_terse_with_cascade_tier(self):
        lines: list[str] = []
        tw = TranscriptWriter(verbose=False, write=lines.append)
        rec = _rec(attempt_num=1, tokens_in=100, tokens_out=200,
                   gate_that_rejected=None, model_cascade_position=1)
        tw.write(rec, None, model="deepseek-v3", cascade_position=1)
        output = " ".join(lines)
        assert "tier 2 · deepseek-v3" in output
        assert "patch accepted" in output

    def test_terse_cache_hit(self):
        lines: list[str] = []
        tw = TranscriptWriter(verbose=False, write=lines.append)
        rec = _rec(attempt_num=1, gate_that_rejected=None)
        tw.write(rec, None, cache_status="replay")
        output = " ".join(lines)
        assert "replayed (zero-token)" in output

    def test_terse_escalated(self):
        lines: list[str] = []
        tw = TranscriptWriter(verbose=False, write=lines.append)
        rec = _rec(attempt_num=3, tokens_in=200, tokens_out=300,
                   gate_that_rejected="apply", escalated=True)
        tw.write(rec, None, model="claude-3")
        output = " ".join(lines)
        assert "escalated" in output
        assert "rejected (guardrails)" in output


class TestTranscriptWriterVerbose:
    def test_verbose_turn_block(self):
        lines: list[str] = []
        tw = TranscriptWriter(verbose=True, write=lines.append)
        rec = _rec(attempt_num=2, tokens_in=500, tokens_out=800,
                   gate_that_rejected="apply", model_cascade_position=0)
        from aqueduct.patch.grammar import PatchSpec
        ps = PatchSpec(
            patch_id="p1", rationale="fix typo", root_cause="bad config",
            confidence=0.85,
            operations=[{"op": "set_module_config_key", "module_id": "m1", "key": "path", "value": "/good"}],
        )
        tw.write(rec, ps, model="deepseek-v3", cascade_position=0, reprompt_reason="guardrail_violation")
        output = "\n".join(lines)
        assert "turn 2" in output
        assert "rejected (guardrails)" in output
        assert "tier 1 · deepseek-v3" in output   # tier branch node
        assert "500 in → 800 out" in output
        assert "fix typo" in output
        assert "bad config" in output
        assert "guardrail_violation" in output

    def test_verbose_with_escalation(self):
        lines: list[str] = []
        tw = TranscriptWriter(verbose=True, write=lines.append)
        rec = _rec(attempt_num=5, tokens_in=300, tokens_out=400,
                   gate_that_rejected="schema", escalated=True)
        tw.write(rec, None, model="claude-opus")
        output = "\n".join(lines)
        assert "turn 5" in output
        assert "invalid patch (schema)" in output
        assert "stuck-detection escalated" in output


class TestTranscriptWriterToolCalls:
    """Phase 75 — agentic-mode tool-call rendering."""

    def test_terse_shows_tool_call_count(self):
        lines: list[str] = []
        tw = TranscriptWriter(verbose=False, write=lines.append)
        rec = _rec(attempt_num=1, tokens_in=100, tokens_out=50, tool_calls=3)
        tw.write(rec, None, model="claude-x")
        output = " ".join(lines)
        assert "3 tool calls" in output

    def test_terse_omits_tool_call_count_when_zero(self):
        lines: list[str] = []
        tw = TranscriptWriter(verbose=False, write=lines.append)
        rec = _rec(attempt_num=1, tokens_in=100, tokens_out=50, tool_calls=0)
        tw.write(rec, None, model="claude-x")
        output = " ".join(lines)
        assert "tool call" not in output

    def test_verbose_renders_one_line_per_tool_call(self):
        lines: list[str] = []
        tw = TranscriptWriter(verbose=True, write=lines.append)
        rec = _rec(attempt_num=1, tokens_in=100, tokens_out=50, tool_calls=2)
        rec._aq_tool_calls = [
            {"name": "read_blueprint", "args_summary": "", "duration_ms": 5, "result_preview": "{'available': True}"},
            {"name": "list_runs", "args_summary": "limit=5", "duration_ms": 12, "result_preview": "[...]"},
        ]
        tw.write(rec, None, model="claude-x")
        output = "\n".join(lines)
        assert "tool: read_blueprint()" in output
        assert "tool: list_runs(limit=5)" in output
        assert "12ms" in output


class TestTranscriptWriterSummary:
    def test_summary(self):
        lines: list[str] = []
        tw = TranscriptWriter(verbose=True, write=lines.append)
        tw.summary("solved", 3, 1500, 2000, model="gpt-4o")
        output = " ".join(lines)
        assert "patch generated" in output   # solved → ✓ patch generated
        assert "3 turn" in output
        assert "└─" in output                # terminal close node
