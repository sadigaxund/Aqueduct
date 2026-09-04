"""Healing conversation display — turn-by-turn transcript renderer.

Engine-agnostic (no pyspark, no click).  Returns strings so the CLI funnel
(``cli/output.py``) handles colour, redaction, and formatting.  Shared by
``run``, ``heal``, and the benchmark/scenario runner so all three callers
print the same transcript shape.

Phase 85 Wave 2 redesign — owner-ratified shape (``tmp/phase85/mockups.txt``,
SCREENS 2-5): a flat ``│`` gutter for the whole heal-block body (``├─`` only
when a NEW cascade tier opens after the first, ``└`` only at the terminal
close — no deeper nesting). The ``◆ self-healing …`` header line itself is
built by the caller (``cli/run.py``/``cli/heal.py``) since it needs colour
and cascade/model context this module deliberately doesn't import (no
click); ``header()`` here only opens the bare rail underneath it.

Usage::

    from aqueduct.agent.transcript import TranscriptWriter

    writer = TranscriptWriter(verbose=True, total_attempts=5)
    for rec in loop:
        ...generate patch, get patch_spec...
        writer.write(rec, patch_spec, model="gpt-4o", cascade_position=0)
"""

from __future__ import annotations

from typing import Any

from aqueduct.agent.providers import _ESCALATION_TEMPERATURE

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


# ── Gate-ladder parsing (SCREEN 3 · Q8 "candidate line shows the killing
# gate NUMBER") ──────────────────────────────────────────────────────────
# Display-only: no new gate machinery. `cli/run.py`'s deep_loop
# `validate_callback` already runs the numbered gate pyramid (1 policy is
# enforced in-loop via `apply_callback`/`_check_guardrails` before a
# candidate ever reaches here — a candidate that reaches `write()` with
# `gate_that_rejected in (None, "validate")` has, by construction, already
# passed gate 1) and folds any FAILING gate into one string via
# `ErrorSignature.normalized_message` — "Lineage gate: … | Sandbox gate: …
# | Explain gate: …" (see `cli/run.py::_validate_cb`). Parsing that string
# back into (number, name) is the one place this module infers gate
# identity without a second copy of gate-numbering logic.
#
# NOTE: `normalized_message` is `signature.py::_normalize_message`'s
# HASH-normalized text (lowercased, digits → "N", quoted spans → "X") —
# fine for a case-insensitive substring match on the gate's own name, but
# not fit to show the user as the failure detail (a byte count would read
# as "N bytes"). No caller currently stashes the raw `vfeedback` string
# anywhere reachable from here — that would be a one-line addition at
# `aqueduct/agent/loop.py`'s deep-loop validation-rejection branch
# (`rec._aq_detail = vfeedback`), which Wave 2 does not own. See the
# worker report: this module intentionally shows the gate NUMBER/NAME
# only, not the mangled message text, until that line is added.
_GATE_NUMBERS: tuple[tuple[str, int, str], ...] = (
    ("lineage gate", 2, "lineage"),
    ("sandbox gate", 3, "sandbox"),
    ("resolvability gate", 4, "resolvability"),
)


def _first_failing_gate(message: str | None) -> tuple[int, str] | None:
    """First ``(number, short_name)`` gate named in a validate-callback
    rejection message, or None when the message names no known gate."""
    if not message:
        return None
    m = message.lower()
    for prefix, num, name in _GATE_NUMBERS:
        if prefix in m:
            return num, name
    return None


def _classify_provider_issue(detail: str | None) -> str:
    """Short status word for a ``gate_that_rejected == "provider"`` attempt.

    This is the SCREEN 5 ⊘ distinction: a provider-gate rejection means the
    model never ran at all (network/credentials), as opposed to a model
    that ran and had its patch rejected (any other gate). ``detail`` is
    ``rec._aq_detail`` — already-classified text set by
    ``aqueduct/agent/loop.py``'s exception handler (connection errors,
    the ``ANTHROPIC_API_KEY environment variable not set`` message, …).
    Classifying it a second time here (rather than inventing a new
    upstream field) keeps this module's only reuse of existing data."""
    d = (detail or "").lower()
    if "api_key" in d or "credential" in d or "not set" in d:
        return "no credentials"
    if (
        "connect" in d
        or "refused" in d
        or "no route to host" in d
        or "name or service not known" in d
        or "timeout" in d
        or "timed out" in d
        or "unreachable" in d
    ):
        return "unreachable"
    return "provider error"


# ── Public API ────────────────────────────────────────────────────────────


class TranscriptWriter:
    """Turns per-attempt data into terse or verbose transcript lines.

    Accepts a ``write`` callback that receives strings; the caller
    routes them through ``cli/output.emit`` / ``.warn`` for colours
    and redaction.  Engine-agnostic — no ``pyspark``, no ``click``.
    """

    _RAIL = "│"  # │  flat body gutter — the ONLY gutter glyph most lines use
    _NODE = "├─"  # ├─ — a tier branch node (escalation ONLY, never the first tier)
    _END = "└"  # └  the closing summary node (no trailing dash — owner ruling)

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
        # First tier is announced by the caller's ◆ header line (which already
        # names "tier 1/N · model") — so `_open_tier_if_new` suppresses the
        # branch node for it and only emits one on a LATER escalation.
        self._first_tier_open = True
        # Set once any turn in this heal session was rejected by the deep-loop
        # gate pyramid (`gate_that_rejected == "validate"`) — the signal that
        # `agent.deep_loop` is active, so an accepted turn afterward is a
        # candidate that has been through the full numbered ladder, not just
        # the in-loop guardrail check. Drives "✓ gates 1-4 passed" wording.
        self._deep_loop_seen = False

    def header(self, attempt_num: int, total_attempts: int, *, resolve: str = "llm") -> None:
        """Open the heal branch — a bare rail under the caller's section header."""
        self._attempts_seen = attempt_num
        self._total_attempts = total_attempts
        self._cur_tier = " unset"
        self._turn_in_tier = 0
        self._first_tier_open = True
        self._deep_loop_seen = False
        self._emit(self._RAIL)

    def _emit(self, line: str) -> None:
        if self._write is not None:
            self._write(line)

    def _emit_wrapped(self, gutter: str, text: str, *, hang: int = 0) -> None:
        """Emit ``text`` through the shared wrap primitive under ``gutter``.

        Audit-fixed 2026-08-23: every detail/hint/reprompt/tool-preview line
        below used to be a single ``self._emit(f"{gutter}{text}")`` call — a
        bare f-string handed straight to the write callback, never touching
        ``wrap_line``. On an 80-column TTY a real connection-refused hint or
        an absolute retry path rendered as ONE ~180-column line, escaping the
        gutter entirely — exactly what ``wrap_line`` exists to prevent (see
        its module docstring and SCREEN 2/6 of ``tmp/phase85/mockups.txt``).
        Piped/CI is unaffected either way — ``wrap_line`` already returns the
        text untouched, one full logical record, when not a TTY.

        ``aqueduct.cli.render.wrap`` is imported lazily here (not at module
        level) to keep this module's "no click at import time" contract —
        ``wrap_line`` itself has no hard click dependency (only two of its
        internal helpers import click lazily, for ANSI styling of the
        truncation-tail/hint lines), so this is a runtime layering choice,
        not a real cycle.
        """
        from aqueduct.cli.render.wrap import wrap_line as _wrap_line

        for line in _wrap_line(text, gutter=gutter, hang=hang, err=True, verbose=self._verbose):
            self._emit(line)

    def _open_tier_if_new(self, cascade_position: int | None, model: str | None) -> None:
        """Emit a ``├─`` branch node ONLY when this is an escalation to a
        cascade tier beyond the first — the first tier is already named by
        the caller's ◆ header line, so repeating it here would duplicate
        the same fact two lines apart."""
        if cascade_position == self._cur_tier:
            return
        was_first = self._cur_tier == " unset"
        self._cur_tier = cascade_position
        self._turn_in_tier = 0
        if was_first:
            # First tier ever seen this session — already announced by the
            # header. No node, no blank rail; just record state.
            self._first_tier_open = False
            return
        self._emit(self._RAIL)  # blank rail separating tiers
        model_short = model or "?"
        if cascade_position is None:
            self._emit(f"{self._NODE} {model_short}")
        else:
            self._emit(f"{self._NODE} tier {cascade_position + 1} · {model_short}")

    def write(
        self,
        rec: Any,
        patch_spec: Any | None = None,
        *,
        model: str | None = None,
        cascade_position: int | None = None,
        reprompt_reason: str | None = None,
    ) -> None:
        """Render one attempt as 1-2 lines (terse) or a full block (verbose).

        Args:
            rec: ``AttemptRecord`` from ``BudgetTracker.record()``.
            patch_spec: The parsed ``PatchSpec`` (None on parse/API failure).
            model: Model name that produced this attempt (cascade tier's model).
            cascade_position: 0-based tier index (None outside cascade).
            reprompt_reason: Reason fed back to the model on rejection.
        """
        if cascade_position is None:
            cascade_position = getattr(rec, "model_cascade_position", None)
        self._open_tier_if_new(cascade_position, model)
        self._turn_in_tier += 1
        if rec.gate_that_rejected == "validate":
            self._deep_loop_seen = True
        if self._verbose:
            self._write_verbose(
                rec,
                patch_spec,
                model=model,
                cascade_position=cascade_position,
                reprompt_reason=reprompt_reason,
            )
        else:
            self._write_terse(rec, model=model, cascade_position=cascade_position)

    def _verdict(self, rec: Any) -> tuple[str, str | None]:
        """One-glyph outcome + short reason for a single turn.

        Returns ``(verdict, extra_detail)`` — ``extra_detail`` is a fuller
        explanation the caller may print on its own indented line (kept
        separate from the compact verdict so the default one-liner stays
        one line even when the detail is long)."""
        gate = rec.gate_that_rejected
        if gate is None:
            return (
                ("✓ gates 1-4 passed" if self._deep_loop_seen else "✓ patch accepted"),
                None,
            )
        if gate == "provider":
            detail = getattr(rec, "_aq_detail", None)
            return f"⊘ {_classify_provider_issue(detail)}", detail
        if gate == "validate":
            msg = getattr(getattr(rec, "signature", None), "normalized_message", None)
            hit = _first_failing_gate(msg)
            if hit is not None:
                num, name = hit
                # No clean detail line here — `msg` is the hash-normalized
                # text (see the module-level note above `_GATE_NUMBERS`),
                # not fit to show verbatim.
                return f"✗ rejected — {num} {name}", None
            return f"✗ {_gate_label(gate)}", None
        detail = getattr(rec, "_aq_detail", None)
        base = f"✗ {_gate_label(gate)}"
        return (f"{base} — {detail}" if detail else base), None

    def _write_terse(
        self,
        rec: Any,
        *,
        model: str | None = None,
        cascade_position: int | None = None,
    ) -> None:
        verdict, detail = self._verdict(rec)
        gate = rec.gate_that_rejected
        if gate == "provider":
            # A provider-gate rejection means the model was never reached —
            # SCREEN 5's per-tier ⊘ line, not a "turn" (no conversation
            # happened). No "tier N · model" prefix here: tier 1's identity
            # is already in the caller's ◆ header line, and any LATER
            # tier's identity is already in the `├─ tier N · model`
            # escalation node `_open_tier_if_new` just emitted — repeating
            # it here duplicated it (audit-fixed 2026-08-23: SCREEN 5's
            # golden run showed "├─ tier 2 · claude-sonnet-4-6" immediately
            # followed by "tier 2 claude-sonnet-4-6 · ⊘ …").
            self._emit(f"{self._RAIL} {verdict}")
            if detail:
                self._emit_wrapped(f"{self._RAIL}     ", detail)
            hint = getattr(rec, "_aq_hint", None)
            if hint:
                self._emit_wrapped(f"{self._RAIL}     ", f"ⓘ {hint.strip()}", hang=2)
            return

        parts = [f"turn {rec.attempt_num}"]
        if gate is None or gate == "validate":
            parts.append(f"patch #{rec.attempt_num}")
        parts.append(verdict)
        n_tools = getattr(rec, "tool_calls", 0) or 0
        if n_tools:
            parts.append(f"{n_tools} tool call{'s' if n_tools != 1 else ''}")
        cost = _cost_str(rec.tokens_in, rec.tokens_out, model)
        if cost:
            parts.append(cost)
        if rec.escalated:
            parts.append("escalated")
        self._emit(f"{self._RAIL} " + " · ".join(parts))
        if detail and gate not in (None,):
            self._emit_wrapped(f"{self._RAIL}     ", detail)
        hint = getattr(rec, "_aq_hint", None)
        if hint:
            self._emit_wrapped(f"{self._RAIL}     ", f"ⓘ {hint.strip()}", hang=2)

    def _write_verbose(
        self,
        rec: Any,
        patch_spec: Any | None,
        *,
        model: str | None = None,
        cascade_position: int | None = None,
        reprompt_reason: str | None = None,
    ) -> None:
        rail = self._RAIL
        sub = f"{rail}   "  # verbose sub-fields — rationale, ladder, tool, reprompt

        gate = rec.gate_that_rejected
        verdict, detail = self._verdict(rec)

        if gate == "provider":
            # See the identical note in `_write_terse` — no repeated
            # "tier N · model" prefix; the header/escalation node already
            # named it.
            self._emit(f"{rail} {verdict}")
            if detail:
                self._emit_wrapped(f"{rail}     ", detail)
            hint = getattr(rec, "_aq_hint", None)
            if hint:
                self._emit_wrapped(f"{rail}     ", f"ⓘ {hint.strip()}", hang=2)
            return

        # Turn header — op type when parsed, else the plain verdict.
        op_desc = ""
        if patch_spec is not None and getattr(patch_spec, "operations", None):
            ops = patch_spec.operations
            first = ops[0]
            op_target = getattr(first, "module_id", "") or getattr(first, "key", "") or ""
            op_desc = f" {first.op} on {op_target}" if op_target else f" {first.op}"
        patch_label = f"patch #{rec.attempt_num}" if gate in (None, "validate") else None
        head_bits = [f"turn {rec.attempt_num}"]
        if patch_label:
            head_bits.append(patch_label)
        head = f"{rail} " + " · ".join(head_bits)
        if op_desc:
            head += f" ·{op_desc}"
        # Always show the verdict — op_desc alone would bury a rejection on
        # a syntactically valid patch (e.g. gate="apply") a level down, and
        # a clean accept still needs "✓ gates 1-4 passed" vs "✓ patch
        # accepted" to say whether the numbered ladder ran at all.
        head += f"  {verdict}"
        self._emit(head)

        cost = _cost_str(rec.tokens_in, rec.tokens_out, model)
        if cost:
            self._emit(f"{sub}{cost}")

        if patch_spec is not None:
            if patch_spec.rationale:
                self._emit_wrapped(sub, f"rationale: {patch_spec.rationale}", hang=2)
            if patch_spec.root_cause:
                self._emit_wrapped(sub, f"root cause: {patch_spec.root_cause}", hang=2)
            conf = f"{patch_spec.confidence:.0%}" if patch_spec.confidence is not None else "n/a"
            ops_str = ", ".join(
                f"{o.op}({getattr(o, 'module_id', '') or getattr(o, 'key', '') or ''})"
                for o in patch_spec.operations
            )
            self._emit_wrapped(sub, f"parsed: {ops_str or '(none)'} · confidence {conf}", hang=2)

        # Gate ladder — only meaningful once `agent.deep_loop` is active
        # (`gate == "validate"` on rejection, or an accepted turn once we've
        # already seen a validate rejection this session). A fully-passing
        # ladder collapses to one line; a failure/warning expands with detail
        # on the offending gate (owner ruling, SCREEN 3 notes).
        if gate == "validate" or (gate is None and self._deep_loop_seen):
            msg = getattr(getattr(rec, "signature", None), "normalized_message", None)
            hit = _first_failing_gate(msg) if gate == "validate" else None
            self._emit(f"{sub}gate ladder")
            if hit is None:
                self._emit(f"{sub}  1 policy ✓  2 lineage ✓  3 sandbox ✓  4 explain ✓")
            else:
                num, name = hit
                for g_num, g_name in (
                    (1, "policy"),
                    (2, "lineage"),
                    (3, "sandbox"),
                    (4, "explain"),
                ):
                    if g_num < num:
                        self._emit(f"{sub}  {g_num} {g_name}    ✓")
                    elif g_num == num:
                        # No clean detail here — see the module-level note
                        # above `_GATE_NUMBERS` on why the raw gate message
                        # isn't reachable from this record today.
                        self._emit(f"{sub}  {g_num} {g_name}    ✗  rejected")
                    else:
                        self._emit(f"{sub}  {g_num} {g_name}    —  not reached")

        # Raw model output verbatim — the core of -v: lets the user see exactly
        # what the model returned (especially when it would not parse) and decide
        # whether the prompt or the blueprint needs more guiding context.
        raw = getattr(rec, "_aq_raw", None)
        if raw and raw.strip() and not self._streamed:
            self._emit(f"{rail}   response · attempt {rec.attempt_num}")
            shown = raw.strip().splitlines()
            for line in shown[:_MAX_RAW_LINES]:
                self._emit(f"{rail}   ┆ {line}")
            if len(shown) > _MAX_RAW_LINES:
                self._emit(f"{rail}   ┆ … ({len(shown) - _MAX_RAW_LINES} more line(s))")

        # Phase 75 — one line per tool call this attempt: name, args summary,
        # duration, truncated (already-redacted) result preview.
        for call in getattr(rec, "_aq_tool_calls", None) or ():
            name = call.get("name", "?")
            args = call.get("args_summary", "")
            dur = call.get("duration_ms", 0)
            preview = call.get("result_preview", "")
            self._emit_wrapped(sub, f"tool: {name}({args}) · {dur}ms → {preview}", hang=2)

        if gate not in (None, "validate", "provider") and detail:
            self._emit_wrapped(sub, f"{_gate_label(gate)}: {detail}", hang=2)

        # What we fed back to the model after a rejection.
        if reprompt_reason:
            self._emit_wrapped(sub, f"reprompt: {reprompt_reason}", hang=2)

        if rec.escalated:
            self._emit(f"{sub}stuck-detection escalated (temperature={_ESCALATION_TEMPERATURE})")

        hint = getattr(rec, "_aq_hint", None)
        if hint:
            self._emit_wrapped(f"{rail}   ", f"ⓘ {hint.strip()}", hang=2)

    # ── -vv raw layer (SCREEN 4) ────────────────────────────────────────────
    def raw_block(self, header: str, text: str, *, max_lines: int = _MAX_RAW_LINES) -> None:
        """Frame a raw text block (streamed turn text, engine/sandbox replay
        log lines) under a ``┆`` inner gutter with a dim header naming what
        it is. TTY callers cap to ``max_lines`` + ``(N more lines)``; the
        caller decides piped-vs-TTY (this module has no click/TTY access) by
        only calling with the full, uncapped text and letting the funnel's
        ``wrap_line``/echo layer decide truncation — so on a real TTY the
        caller should pass an already-capped ``max_lines`` and on piped
        output pass a very large one (or omit it) for full text.

        Not wired to any caller yet for PROMPT text specifically — Aqueduct
        does not currently capture the prompt string on ``AttemptRecord``
        (only the raw model response, `_aq_raw`); adding that capture point
        lives in ``aqueduct/agent/loop.py``, which Wave 2 does not own. See
        the worker report for this gap. Streamed turn text and any
        engine/sandbox log text the caller already has is framed by this
        method today.
        """
        rail = self._RAIL
        self._emit(f"{rail}   {header}")
        lines = text.strip("\n").splitlines() or [""]
        shown = lines[:max_lines]
        for line in shown:
            self._emit(f"{rail}   ┆ {line}")
        if len(lines) > max_lines:
            self._emit(f"{rail}   ┆ ({len(lines) - max_lines} more lines)")

    # Human-readable closing reasons (stop_reason → phrase).
    _STOP_PHRASE: dict[str, str] = {
        "solved": "patch generated",
        "api_error": "healing unavailable — no agent was reached",
        "exhausted_attempts": "out of attempts",
        "stuck_signature": "stuck (repeating error)",
        "progress_stalled": "stalled (no new error signatures)",
        "deferred": "deferred to human",
        "budget_seconds_exceeded": "time budget exceeded",
        "budget_tokens_exceeded": "token budget exceeded",
    }

    def summary(
        self,
        stop_reason: str | None,
        attempts: int,
        tokens_in: int,
        tokens_out: int,
        model: str | None = None,
    ) -> None:
        """Close the heal branch with a terminal └ node.

        ``stop_reason == "api_error"`` (every reachable tier failed to
        connect / lacked credentials) renders with ``⊘`` — SCREEN 5's
        distinction: nothing here was REJECTED, nothing ever ran — as
        opposed to ``✗`` for every other non-``solved`` reason, where a
        model DID run and its output was refused."""
        cost = _cost_str(tokens_in, tokens_out, model)
        reason = stop_reason or "unknown"
        ok = reason == "solved"
        phrase = self._STOP_PHRASE.get(reason, reason)
        if ok:
            icon = "✓"
        elif reason == "api_error":
            icon = "⊘"
        else:
            icon = "✗"
        if reason == "api_error":
            # No real "turns" happened — every bit is a per-tier connection
            # failure already printed above. Omit the turn/token tally.
            self._emit(f"{self._END} {icon} {phrase}")
            return
        bits = [f"{icon} {phrase}", f"{attempts} turn(s)"]
        if cost:
            bits.append(cost)
        self._emit(f"{self._END} " + " · ".join(bits))
