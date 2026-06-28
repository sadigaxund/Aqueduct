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
    inp, outp = _estimate_model(model)
    if inp == 0.0 and outp == 0.0:
        return f"{tokens_in}→{tokens_out} tokens"
    cost = (ti * inp) + (to * outp)
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

    def __init__(
        self,
        *,
        verbose: bool = False,
        write: Any = None,
    ) -> None:
        self._verbose = verbose
        self._write = write
        self._attempts_seen = 0

    def header(self, attempt_num: int, total_attempts: int, *, resolve: str = "llm") -> None:
        """Print the turn header (before the LLM call)."""
        self._attempts_seen = attempt_num
        tag = f"{attempt_num}/{total_attempts}" if total_attempts > 1 else str(attempt_num)
        prefix = "replay" if resolve == "replay" else "attempt"
        self._emit(f"  \u21bb {prefix} {tag}")

    def _emit(self, line: str) -> None:
        if self._write is not None:
            self._write(line)

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
        if self._verbose:
            self._write_verbose(
                rec, patch_spec, model=model, cascade_position=cascade_position,
                cache_status=cache_status, reprompt_reason=reprompt_reason,
            )
        else:
            self._write_terse(
                rec, model=model, cascade_position=cascade_position,
                cache_status=cache_status,
            )

    def _write_terse(
        self,
        rec: Any,
        *,
        model: str | None = None,
        cascade_position: int | None = None,
        cache_status: str | None = None,
    ) -> None:
        parts: list[str] = [f"  attempt {rec.attempt_num}"]

        tier = _tier_label(cascade_position, model)
        if tier:
            parts.append(tier)

        cache = _cache_label(cache_status)
        if cache:
            parts.append(cache)
        else:
            parts.append(_cost_str(rec.tokens_in, rec.tokens_out, model))

        gate = _gate_label(rec.gate_that_rejected)
        parts.append(gate)

        if rec.escalated:
            parts.append("escalated")

        self._emit(" · ".join(parts))

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
        indent = "  "
        tier = _tier_label(cascade_position, model)
        seps = ("\n" + indent + "       ").join  # aligns under the label prefix

        # Status line
        gate = _gate_label(rec.gate_that_rejected)
        status_icon = "\u2713" if rec.gate_that_rejected is None else "\u2717"
        label = f"{indent}{status_icon} turn {rec.attempt_num}  ({gate})"
        if tier:
            label += f"  [{tier}]"
        self._emit(label)

        # Use (tokens + cost)
        cache = _cache_label(cache_status)
        if cache:
            self._emit(f"{indent}     \u21b3 {cache}")
        else:
            cost = _cost_str(rec.tokens_in, rec.tokens_out, model)
            if cost:
                self._emit(f"{indent}     \u21b3 {cost}")

        # Patch details (only when successfully parsed)
        if patch_spec is not None:
            ops = ", ".join(
                f"{o.op}({getattr(o, 'module_id', '') or getattr(o, 'key', '') or ''})"
                for o in patch_spec.operations
            )
            parts = [f"ops: {ops}" if ops else "ops: (none)"]
            if patch_spec.rationale:
                parts.append(f"rationale: {patch_spec.rationale}")
            if patch_spec.root_cause:
                parts.append(f"root cause: {patch_spec.root_cause}")
            conf = (
                f"{patch_spec.confidence:.0%}"
                if patch_spec.confidence is not None else "n/a"
            )
            parts.append(f"confidence: {conf}")
            self._emit(f"{indent}     \u21b3 {seps(parts)}")

        # Rejection detail
        if reprompt_reason:
            reason = reprompt_reason[:200]
            self._emit(f"{indent}     \u21b3 reprompt: {reason}")

        # Stuck/escalation
        if rec.escalated:
            self._emit(f"{indent}     \u21b3 stuck-detection escalated (temperature=0.9)")

    def summary(self, stop_reason: str | None, attempts: int, tokens_in: int, tokens_out: int, model: str | None = None) -> None:
        """Print a summary line after the loop completes."""
        cost = _cost_str(tokens_in, tokens_out, model)
        reason = stop_reason or "unknown"
        self._emit(f"  heal complete: {attempts} attempt(s) · {cost} · stop={reason}")
