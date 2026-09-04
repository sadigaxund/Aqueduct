"""Tests for aqueduct.agent.transcript — healing conversation display."""

from __future__ import annotations

import pytest

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

    def test_terse_first_tier_node_is_suppressed(self):
        """Phase 85 Wave 2 ruling: the FIRST cascade tier this session is
        already announced by the caller's ◆ header line, so the
        ``├─`` branch node must not repeat it — only an escalation to a
        later tier gets one."""
        lines: list[str] = []
        tw = TranscriptWriter(verbose=False, write=lines.append)
        rec = _rec(
            attempt_num=1,
            tokens_in=100,
            tokens_out=200,
            gate_that_rejected=None,
            model_cascade_position=1,
        )
        tw.write(rec, None, model="deepseek-v3", cascade_position=1)
        output = " ".join(lines)
        assert "├─" not in output
        assert "patch accepted" in output
        assert "patch #1" in output

    def test_terse_escalation_to_later_tier_emits_node(self):
        lines: list[str] = []
        tw = TranscriptWriter(verbose=False, write=lines.append)
        tw.write(
            _rec(attempt_num=1, gate_that_rejected="apply"), None, model="qwen", cascade_position=0
        )
        tw.write(
            _rec(attempt_num=1, gate_that_rejected=None),
            None,
            model="deepseek-v3",
            cascade_position=1,
        )
        output = "\n".join(lines)
        assert "├─ tier 2 · deepseek-v3" in output

    def test_terse_escalated(self):
        lines: list[str] = []
        tw = TranscriptWriter(verbose=False, write=lines.append)
        rec = _rec(
            attempt_num=3, tokens_in=200, tokens_out=300, gate_that_rejected="apply", escalated=True
        )
        tw.write(rec, None, model="claude-3")
        output = " ".join(lines)
        assert "escalated" in output
        assert "rejected (guardrails)" in output


class TestTranscriptWriterVerbose:
    def test_verbose_turn_block(self):
        lines: list[str] = []
        tw = TranscriptWriter(verbose=True, write=lines.append)
        rec = _rec(
            attempt_num=2,
            tokens_in=500,
            tokens_out=800,
            gate_that_rejected="apply",
            model_cascade_position=0,
        )
        from aqueduct.patch.grammar import PatchSpec

        ps = PatchSpec(
            patch_id="p1",
            rationale="fix typo",
            root_cause="bad config",
            confidence=0.85,
            operations=[
                {"op": "set_module_config_key", "module_id": "m1", "key": "path", "value": "/good"}
            ],
        )
        tw.write(
            rec, ps, model="deepseek-v3", cascade_position=0, reprompt_reason="guardrail_violation"
        )
        output = "\n".join(lines)
        assert "turn 2" in output
        assert "rejected (guardrails)" in output
        assert "├─" not in output  # first tier this session — no branch node
        assert "500 in → 800 out" in output
        assert "fix typo" in output
        assert "bad config" in output
        assert "guardrail_violation" in output

    def test_verbose_with_escalation(self):
        lines: list[str] = []
        tw = TranscriptWriter(verbose=True, write=lines.append)
        rec = _rec(
            attempt_num=5,
            tokens_in=300,
            tokens_out=400,
            gate_that_rejected="schema",
            escalated=True,
        )
        tw.write(rec, None, model="claude-opus")
        output = "\n".join(lines)
        assert "turn 5" in output
        assert "invalid patch (schema)" in output
        assert "stuck-detection escalated" in output
        # Audit-fixed 2026-08: this line hardcoded "temperature=0.9" while
        # loop.py actually applies providers._ESCALATION_TEMPERATURE (0.8)
        # on escalation — pin against the real constant so a future tune
        # can't silently leave the transcript wrong again.
        from aqueduct.agent.providers import _ESCALATION_TEMPERATURE

        assert f"temperature={_ESCALATION_TEMPERATURE}" in output
        assert "temperature=0.9" not in output


class TestTranscriptWriterUnreachable:
    """Phase 85 Wave 2 · SCREEN 5 — ⊘ (unreachable/no-creds) must render
    distinctly from ✗ (model ran, patch rejected)."""

    def test_provider_gate_renders_unreachable_glyph(self):
        lines: list[str] = []
        tw = TranscriptWriter(verbose=False, write=lines.append)
        rec = _rec(attempt_num=1, gate_that_rejected="provider", model_cascade_position=0)
        rec._aq_detail = "connection refused at localhost:11434"
        rec._aq_hint = "cannot reach http://localhost:11434. Check the LLM server is running."
        tw.write(rec, None, model="qwen2.5-coder:7b", cascade_position=0)
        output = "\n".join(lines)
        assert "⊘" in output
        assert "unreachable" in output
        assert "connection refused at localhost:11434" in output
        # No repeated "tier 1 · model" here — tier 1's identity is already
        # in the caller's ◆ header line (audit-fixed 2026-08-23: the golden
        # SCREEN 5 run showed the escalation node AND this line both naming
        # "tier 2 · claude-sonnet-4-6").
        assert "✗" not in output

    def test_provider_gate_no_credentials_distinct_word(self):
        lines: list[str] = []
        tw = TranscriptWriter(verbose=False, write=lines.append)
        rec = _rec(attempt_num=1, gate_that_rejected="provider", model_cascade_position=1)
        rec._aq_detail = "ANTHROPIC_API_KEY environment variable not set."
        tw.write(rec, None, model="claude-sonnet-4-6", cascade_position=1)
        output = "\n".join(lines)
        assert "⊘ no credentials" in output

    def test_apply_gate_rejection_still_uses_cross_glyph(self):
        """A model that DID run and had its patch rejected stays ✗, never ⊘."""
        lines: list[str] = []
        tw = TranscriptWriter(verbose=False, write=lines.append)
        rec = _rec(attempt_num=1, gate_that_rejected="apply")
        tw.write(rec, None, model="qwen2.5-coder:7b")
        output = "\n".join(lines)
        assert "✗" in output
        assert "⊘" not in output

    def test_summary_api_error_uses_unreachable_close(self):
        lines: list[str] = []
        tw = TranscriptWriter(verbose=False, write=lines.append)
        tw.summary("api_error", 2, 0, 0, model=None)
        output = " ".join(lines)
        assert output.startswith("└ ⊘")
        assert "no agent was reached" in output
        assert "turn" not in output  # no per-turn tally for a never-reached agent


class TestTranscriptWriterGateLadder:
    """Phase 85 Wave 2 · SCREEN 3 — the candidate line names the killing
    gate NUMBER; a fully-passing ladder collapses to one line at -v."""

    def test_terse_candidate_shows_killing_gate_number(self):
        from aqueduct.agent.signature import make_signature

        lines: list[str] = []
        tw = TranscriptWriter(verbose=False, write=lines.append)
        sig = make_signature(
            "validation_rejected",
            "validate",
            "Sandbox gate: replay failed on 1 000-row sample",
            engine="duckdb",
        )
        rec = _rec(attempt_num=1, gate_that_rejected="validate", signature=sig)
        tw.write(rec, None, model="qwen2.5-coder:7b")
        output = "\n".join(lines)
        assert "patch #1" in output
        assert "3 sandbox" in output

    def test_verbose_passing_ladder_collapses_to_one_line(self):
        lines: list[str] = []
        tw = TranscriptWriter(verbose=True, write=lines.append)
        # turn 1 rejected by the sandbox gate (deep_loop active)…
        from aqueduct.agent.signature import make_signature

        sig = make_signature(
            "validation_rejected", "validate", "Sandbox gate: boom", engine="duckdb"
        )
        tw.write(_rec(attempt_num=1, gate_that_rejected="validate", signature=sig), None, model="m")
        # …turn 2 accepted — the ladder passed everything, one collapsed line.
        lines.clear()
        tw.write(_rec(attempt_num=2, gate_that_rejected=None), None, model="m")
        output = "\n".join(lines)
        ladder_lines = [ln for ln in lines if "policy" in ln and "lineage" in ln]
        assert len(ladder_lines) == 1
        assert "✓" in ladder_lines[0]
        assert "gates 1-4 passed" in output


class TestTranscriptWriterWrapping:
    """Audit-fixed 2026-08-23: detail/hint/reprompt/tool-preview lines used
    to be bare f-strings handed straight to the write callback, never
    touching `wrap_line` — a real connection-refused hint or an absolute
    retry path rendered as ONE line far past the terminal width, escaping
    the `│` gutter entirely. `wrap_line` reads `AQ_FORCE_TTY`/`COLUMNS`
    (see `aqueduct/cli/render/width.py`), so these tests pin both."""

    _LONG_HINT = (
        "cannot reach http://localhost:11434/v1. Check the LLM server is "
        "running and the host is on a routable network "
        "(`curl -sS http://localhost:11434/v1/models` or `ping <host>`)."
    )

    def test_provider_hint_wraps_under_gutter_on_tty(self, monkeypatch):
        from aqueduct.cli.render.width import display_width

        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        monkeypatch.setenv("COLUMNS", "80")
        lines: list[str] = []
        tw = TranscriptWriter(verbose=False, write=lines.append)
        rec = _rec(attempt_num=1, gate_that_rejected="provider")
        rec._aq_detail = "connection refused"
        rec._aq_hint = self._LONG_HINT
        tw.write(rec, None, model="qwen2.5-coder:7b")
        assert lines, "nothing emitted"
        for line in lines:
            assert display_width(line) <= 80, f"line escapes 80 cols: {line!r}"
        # The hint alone is ~180 display columns — it MUST have actually
        # wrapped into more than one line, not just been left short.
        assert len(lines) > 2

    def test_provider_hint_piped_stays_one_full_logical_record(self, monkeypatch):
        monkeypatch.setenv("AQ_FORCE_TTY", "0")
        monkeypatch.delenv("COLUMNS", raising=False)
        lines: list[str] = []
        tw = TranscriptWriter(verbose=False, write=lines.append)
        rec = _rec(attempt_num=1, gate_that_rejected="provider")
        rec._aq_detail = "connection refused"
        rec._aq_hint = self._LONG_HINT
        tw.write(rec, None, model="qwen2.5-coder:7b")
        hint_lines = [ln for ln in lines if self._LONG_HINT in ln]
        assert len(hint_lines) == 1, "piped hint must stay ONE untouched record"

    def test_verbose_reprompt_wraps_on_tty(self, monkeypatch):
        from aqueduct.cli.render.width import display_width

        monkeypatch.setenv("AQ_FORCE_TTY", "1")
        monkeypatch.setenv("COLUMNS", "80")
        lines: list[str] = []
        tw = TranscriptWriter(verbose=True, write=lines.append)
        long_reason = "the model kept qualifying the wrong table alias " * 4
        rec = _rec(attempt_num=1, gate_that_rejected="apply")
        tw.write(rec, None, model="m", reprompt_reason=long_reason)
        for line in lines:
            assert display_width(line) <= 80, f"line escapes 80 cols: {line!r}"
        assert any("reprompt:" in ln for ln in lines)


class TestTranscriptWriterSummary:
    def test_summary(self):
        lines: list[str] = []
        tw = TranscriptWriter(verbose=True, write=lines.append)
        tw.summary("solved", 3, 1500, 2000, model="gpt-4o")
        output = " ".join(lines)
        assert "patch generated" in output  # solved → ✓ patch generated
        assert "3 turn" in output
        assert "└" in output  # terminal close node

    def test_every_stop_reason_has_a_human_phrase(self):
        """Audit-fixed 2026-08: `_STOP_PHRASE` was missing `progress_stalled`
        — an unmapped stop_reason falls back to the raw enum string
        (`✗ progress_stalled`) instead of a readable phrase like every other
        reason gets. Pin against the real StopReason enum (not a hardcoded
        list) so a future StopReason member can't silently repeat this."""
        from aqueduct.agent.budget import StopReason

        for reason in StopReason:
            phrase = TranscriptWriter._STOP_PHRASE.get(reason.value)
            assert phrase is not None, f"StopReason.{reason.name} has no _STOP_PHRASE entry"
            assert phrase != reason.value, f"StopReason.{reason.name} falls back to the raw value"
