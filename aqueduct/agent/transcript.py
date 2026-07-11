"""Healing conversation display — turn-by-turn transcript renderer.

Engine-agnostic (no pyspark, no click).  Returns strings so the CLI funnel
(``cli/output.py``) handles colour, redaction, and formatting.  Shared by
``run``, ``heal``, and the benchmark/scenario runner so all three callers
print the same transcript shape.

Usage::

    from aqueduct.agent.transcript import TranscriptWriter

    writer = TranscriptWriter(verbose=True, total_attempts=5)
    for rec in loop:
        ...generate patch, get patch_spec...
        writer.write(rec, patch_spec, model="gpt-4o", cascade_position=0)
"""

from __future__ import annotations

from typing import Any

# -v raw-response dump is capped so a verbose model can't flood the terminal.
_MAX_RAW_LINES = 40

def _cost_str(tokens_in: int, tokens_out: int, model: str | None = None) -> str:
    """Token usage as ``<in>→<out> tok`` (input prompt → output patch).

    Dollar-cost estimates were removed deliberately: a hardcoded price table is
    plain wrong for self-hosted models (an Ollama call is free, not the cloud
    deepseek/qwen rate) and only approximate for cloud ones — false precision in
    the heal transcript. Tokens are the exact signal. ``model`` is unused, kept
    for call-site compatibility."""
    ti = tokens_in if isinstance(tokens_in, int) else 0
    to = tokens_out if isinstance(tokens_out, int) else 0
    if ti == 0 and to == 0:
        return ""  # nothing was spent (provider error / cache hit) — omit noise
    return f"tokens: {ti:,} in → {to:,} out"


def _cache_label(cache_status: str | None) -> str:
    if cache_status == "replay":
        return "replayed (zero-token)"
    if cache_status == "pending":
        return "pending (zero-token)"
    if cache_status == "coaching":
        return "coached (zero-token)"
    return ""


def _gate_label(gate: str | None) -> str:
    if gate is None:
        return "accepted"
    labels: dict[str, str] = {
        "schema": "invalid patch (schema)",
        "apply": "rejected (guardrails)",
        "provider": "provider error",
        "budget": "budget exhausted",
        "defer_rejected": "deferred (not allowed)",
        "validate": "rejected (validation)",
        "budget_seconds": "budget seconds exceeded",
    }
    return labels.get(gate, gate or "accepted")


def _tier_label(cascade_position: int | None, model: str | None) -> str:
    if cascade_position is None:
        return ""
    tier_num = cascade_position + 1
    model_short = model or "?"
    return f"tier {tier_num} ({model_short})"


# ── Public API ────────────────────────────────────────────────────────────


class TranscriptWriter:
    """Turns per-attempt data into terse or verbose transcript lines.

    Accepts a ``write`` callback that receives strings; the caller
    routes them through ``cli/output.emit`` / ``.warn`` for colours
    and redaction.  Engine-agnostic — no ``pyspark``, no ``click``.
    """

    _RAIL = "\u2502"      # \u2502  vertical rail down the heal branch
    _NODE = "\u251c\u2500"  # \u251c\u2500 a tier branch node
    _END = "\u2514\u2500"   # \u2514\u2500 the closing summary node

    def __init__(
        self,
        *,
        verbose: bool = False,
        write: Any = None,
        streamed: bool = False,
    ) -> None:
        self._verbose = verbose
        self._write = write
        # When the response was streamed live (run on a TTY), skip the post-hoc
        # raw-response block in -v — it would just repeat what already scrolled by.
        self._streamed = streamed
        self._attempts_seen = 0
        # Tree state: a new cascade tier opens a fresh branch; its turns nest.
        self._cur_tier: Any = " unset"
        self._turn_in_tier = 0

    def header(self, attempt_num: int, total_attempts: int, *, resolve: str = "llm") -> None:
        """Open the heal branch \u2014 a bare rail under the caller's section header."""
        self._attempts_seen = attempt_num
        self._total_attempts = total_attempts
        self._cur_tier = " unset"
        self._turn_in_tier = 0
        self._emit(self._RAIL)

    def _emit(self, line: str) -> None:
        if self._write is not None:
            self._write(line)

    def _open_tier_if_new(self, cascade_position: int | None, model: str | None) -> None:
        """Emit a tier branch node the first time we see this cascade position."""
        if cascade_position == self._cur_tier:
            return
        if self._cur_tier != " unset":
            self._emit(self._RAIL)  # blank rail separating tiers
        self._cur_tier = cascade_position
        self._turn_in_tier = 0
        model_short = model or "?"
        if cascade_position is None:
            self._emit(f"{self._NODE} {model_short}")
        else:
            self._emit(f"{self._NODE} tier {cascade_position + 1} \u00b7 {model_short}")

    def write(
        self,
        rec: Any,
        patch_spec: Any | None = None,
        *,
        model: str | None = None,
        cascade_position: int | None = None,
        cache_status: str | None = None,
        reprompt_reason: str | None = None,
    ) -> None:
        """Render one attempt as 1-2 lines (terse) or a full block (verbose).

        Args:
            rec: ``AttemptRecord`` from ``BudgetTracker.record()``.
            patch_spec: The parsed ``PatchSpec`` (None on parse/API failure).
            model: Model name that produced this attempt (cascade tier's model).
            cascade_position: 0-based tier index (None outside cascade).
            cache_status: ``"replay"`` / ``"pending"`` / ``"coaching"`` or None.
            reprompt_reason: Reason fed back to the model on rejection.
        """
        if cascade_position is None:
            cascade_position = getattr(rec, "model_cascade_position", None)
        self._open_tier_if_new(cascade_position, model)
        self._turn_in_tier += 1
        if self._verbose:
            self._write_verbose(
                rec, patch_spec, model=model, cascade_position=cascade_position,
                cache_status=cache_status, reprompt_reason=reprompt_reason,
            )
        else:
            self._write_terse(rec, model=model, cache_status=cache_status)

    def _verdict(self, rec: Any, cache_status: str | None) -> str:
        """One-glyph outcome + short reason for a single turn."""
        cache = _cache_label(cache_status)
        if cache:
            return f"✓ {cache}"
        if rec.gate_that_rejected is None:
            return "✓ patch accepted"
        if rec.gate_that_rejected == "provider":
            base = "✗ provider error"
        else:
            base = f"✗ {_gate_label(rec.gate_that_rejected)}"
        detail = getattr(rec, "_aq_detail", None)
        return f"{base} — {detail}" if detail else base

    def _write_terse(
        self,
        rec: Any,
        *,
        model: str | None = None,
        cache_status: str | None = None,
    ) -> None:
        parts = [f"turn {rec.attempt_num}", self._verdict(rec, cache_status)]
        if not _cache_label(cache_status):
            cost = _cost_str(rec.tokens_in, rec.tokens_out, model)
            if cost:
                parts.append(cost)
        # Phase 75 — agentic-mode tool-call count (0 / absent in oneshot mode).
        n_tools = getattr(rec, "tool_calls", 0) or 0
        if n_tools:
            parts.append(f"{n_tools} tool call{'s' if n_tools != 1 else ''}")
        if rec.escalated:
            parts.append("escalated")
        self._emit(f"{self._RAIL}   · " + " · ".join(parts))
        hint = getattr(rec, "_aq_hint", None)
        if hint:
            self._emit(f"{self._RAIL}     ⓘ {hint.strip()}")

    def _write_verbose(
        self,
        rec: Any,
        patch_spec: Any | None,
        *,
        model: str | None = None,
        cascade_position: int | None = None,
        cache_status: str | None = None,
        reprompt_reason: str | None = None,
    ) -> None:
        rail = self._RAIL
        sub = f"{rail}     \u21b3 "

        # Turn header \u2014 same verdict line as terse, then the deep detail below.
        self._emit(f"{rail}   \u00b7 turn {rec.attempt_num}  {self._verdict(rec, cache_status)}")

        cache = _cache_label(cache_status)
        if not cache:
            cost = _cost_str(rec.tokens_in, rec.tokens_out, model)
            if cost:
                self._emit(f"{sub}{cost}")

        # Phase 75 — one line per tool call this attempt: name, args summary,
        # duration, truncated (already-redacted) result preview.
        for call in (getattr(rec, "_aq_tool_calls", None) or ()):
            name = call.get("name", "?")
            args = call.get("args_summary", "")
            dur = call.get("duration_ms", 0)
            preview = call.get("result_preview", "")
            self._emit(f"{sub}tool: {name}({args}) · {dur}ms → {preview}")

        # Raw model output verbatim \u2014 the core of -v: lets the user see exactly
        # what the model returned (especially when it would not parse) and decide
        # whether the prompt or the blueprint needs more guiding context.
        raw = getattr(rec, "_aq_raw", None)
        if raw and raw.strip() and not self._streamed:
            self._emit(f"{rail}     raw response:")
            shown = raw.strip().splitlines()
            for line in shown[:_MAX_RAW_LINES]:
                self._emit(f"{rail}     \u2506 {line}")
            if len(shown) > _MAX_RAW_LINES:
                self._emit(f"{rail}     \u2506 \u2026 ({len(shown) - _MAX_RAW_LINES} more line(s))")

        # Parsed patch details (only when parsing succeeded).
        if patch_spec is not None:
            ops = ", ".join(
                f"{o.op}({getattr(o, 'module_id', '') or getattr(o, 'key', '') or ''})"
                for o in patch_spec.operations
            )
            conf = (
                f"{patch_spec.confidence:.0%}"
                if patch_spec.confidence is not None else "n/a"
            )
            self._emit(f"{sub}parsed: {ops or '(none)'} \u00b7 confidence {conf}")
            if patch_spec.rationale:
                self._emit(f"{sub}rationale: {patch_spec.rationale}")
            if patch_spec.root_cause:
                self._emit(f"{sub}root cause: {patch_spec.root_cause}")

        # What we fed back to the model after a rejection.
        if reprompt_reason:
            self._emit(f"{sub}reprompt: {reprompt_reason[:300]}")

        if rec.escalated:
            self._emit(f"{sub}stuck-detection escalated (temperature=0.9)")

        hint = getattr(rec, "_aq_hint", None)
        if hint:
            self._emit(f"{rail}     \u24d8 {hint.strip()}")

    # Human-readable closing reasons (stop_reason → phrase).
    _STOP_PHRASE: dict[str, str] = {
        "solved": "patch generated",
        "api_error": "all tiers unreachable",
        "exhausted_attempts": "out of attempts",
        "stuck_signature": "stuck (repeating error)",
        "deferred": "deferred to human",
        "budget_seconds_exceeded": "time budget exceeded",
        "budget_tokens_exceeded": "token budget exceeded",
    }

    def summary(self, stop_reason: str | None, attempts: int, tokens_in: int, tokens_out: int, model: str | None = None) -> None:
        """Close the heal branch with a terminal └─ node."""
        cost = _cost_str(tokens_in, tokens_out, model)
        reason = stop_reason or "unknown"
        ok = reason == "solved"
        phrase = self._STOP_PHRASE.get(reason, reason)
        icon = "✓" if ok else "✗"
        bits = [f"{icon} {phrase}", f"{attempts} turn(s)"]
        if cost:
            bits.append(cost)
        self._emit(f"{self._END} " + " · ".join(bits))
