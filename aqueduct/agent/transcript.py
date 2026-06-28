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

# ── Price table (USD per 1k tokens) ──────────────────────────────────────
# Derived from public API pages as of mid-2026.  Used for cost estimates.
# Keys: provider/model name or prefix.
_TOKEN_PRICE: dict[str, tuple[float, float]] = {
    # (input price per 1M, output price per 1M) → per-1k values below
    "claude-sonnet-4": (3.00 / 1000, 15.00 / 1000),
    "claude-3.5": (3.00 / 1000, 15.00 / 1000),
    "claude-3": (3.00 / 1000, 15.00 / 1000),
    "claude-opus-4": (15.00 / 1000, 75.00 / 1000),
    "claude-haiku": (0.80 / 1000, 4.00 / 1000),
    "gpt-4": (30.00 / 1000, 60.00 / 1000),
    "gpt-4o": (2.50 / 1000, 10.00 / 1000),
    "gpt-4o-mini": (0.15 / 1000, 0.60 / 1000),
    "gpt-3.5": (0.50 / 1000, 1.50 / 1000),
    "o1": (15.00 / 1000, 60.00 / 1000),
    "o3-mini": (1.10 / 1000, 4.40 / 1000),
    "deepseek": (0.14 / 1000, 0.28 / 1000),
    "qwen": (0.12 / 1000, 0.24 / 1000),
}


def _estimate_model(model: str) -> tuple[float, float]:
    """Return (input_usd_per_1k, output_usd_per_1k) or (0, 0) if unknown."""
    model_lower = model.lower()
    for key, prices in _TOKEN_PRICE.items():
        if model_lower.startswith(key):
            return prices
    return (0.0, 0.0)


def _cost_str(tokens_in: int, tokens_out: int, model: str | None) -> str:
    if model is None:
        return ""
    ti = tokens_in if isinstance(tokens_in, int) else 0
    to = tokens_out if isinstance(tokens_out, int) else 0
    if ti == 0 and to == 0:
        return ""  # nothing was spent (provider error / cache hit) — omit noise
    inp, outp = _estimate_model(model)
    if inp == 0.0 and outp == 0.0:
        return f"{tokens_in}→{tokens_out} tokens"
    # inp/outp are USD per 1k tokens (price-per-1M ÷ 1000 in the table), so the
    # token counts must be scaled to thousands — earlier this multiplied raw
    # tokens by the per-1k rate, overstating cost ~1000×.
    cost = (ti / 1000.0) * inp + (to / 1000.0) * outp
    approx = "\u2248"
    return f"{tokens_in}\u2192{tokens_out} tokens \u00b7 {approx}${cost:.4f}"


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
    ) -> None:
        self._verbose = verbose
        self._write = write
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

        # Raw model output verbatim \u2014 the core of -v: lets the user see exactly
        # what the model returned (especially when it would not parse) and decide
        # whether the prompt or the blueprint needs more guiding context.
        raw = getattr(rec, "_aq_raw", None)
        if raw and raw.strip():
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
