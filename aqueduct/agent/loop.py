"""Orchestration loop and patch I/O for the LLM agent.

This module owns:
  - The main ``generate_agent_patch`` loop
  - Patch staging and archiving (``stage_patch_for_human``, ``archive_patch``)
  - The ``AgentPatchResult`` dataclass returned to callers
  - Timestamp helpers for patch filenames
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from aqueduct.agent.budget import (
    BudgetConfig,
    BudgetTracker,
)
from aqueduct.agent.parse import (
    _detect_structural_error,
    _format_reprompt_error,
    _format_reprompt_for_next_turn,
    _parse_patch_spec,
)
from aqueduct.agent.prompts import _build_user_prompt
from aqueduct.agent.providers import (
    _ESCALATION_TEMPERATURE,
    _ProviderConfig,
    _call_agent,
    _format_llm_error_hint,
)
from aqueduct.agent.signature import (
    from_apply_error,
    from_exception,
    from_failure_context,
    from_json_decode_error,
    from_validation_error,
)
from aqueduct.patch.grammar import PatchSpec
from aqueduct.redaction import redact as _redact
from aqueduct.surveyor.models import FailureContext

if TYPE_CHECKING:
    from aqueduct.config import WebhookEndpointConfig

logger = logging.getLogger(__name__)

# Bump manually when the system prompt changes significantly.
# Stored in patch _aq_meta so benchmark results can be correlated to prompt versions.
# 1.2 — Phase 45: signature-matched coaching section replaces the chronological
#       patch-history section; recovery-pass polish (orphan </think>, fence
#       selection, multi-key wrapper unwrap, reprompt char cap).
# 1.3 — Phase 47: replace_macro op added to the grammar/schema; macro hint
#       now points at it for in-macro root causes.
PROMPT_VERSION = "1.3"


@dataclass
class AgentPatchResult:
    """Return value of ``generate_agent_patch`` — patch + attempt metadata.

    * ``stop_reason`` — which BudgetConfig axis terminated the loop.
    * ``tokens_in_total`` / ``tokens_out_total`` — provider-reported usage.
    * ``attempt_records`` — per-attempt structured log fed to ``heal_attempts``.
    * ``escalated`` — True iff stuck-detection escalation was applied at least once.
    """

    patch: PatchSpec | None
    attempts: int
    reprompt_errors: list[str] = field(default_factory=list)
    stop_reason: str | None = None
    tokens_in_total: int = 0
    tokens_out_total: int = 0
    attempt_records: list[Any] = field(default_factory=list)
    escalated: bool = False
    recovery_applied: list[str] = field(default_factory=list)
    # Phase 46 — the model that actually produced this result (under cascade
    # the top-level agent.model differs from the producing tier's model) and
    # its 0-based tier index (None outside cascade). None on Phase 45 replay
    # results — no LLM was involved.
    model: str | None = None
    model_cascade_position: int | None = None


# ── Timestamp helpers ────────────────────────────────────────────────────


def _utcnow() -> str:
    """ISO-8601 UTC timestamp, e.g. 2026-06-04T12:34:56.789+00:00."""
    return datetime.now(tz=timezone.utc).isoformat()


def _patch_filename(patch_spec: PatchSpec) -> str:
    """Generate structured filename: {YYYYMMDDTHHmmss}_{slug}.json."""
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{ts}_{patch_spec.patch_id}.json"


# ── Patch I/O ────────────────────────────────────────────────────────────


def stage_patch_for_human(
    patch_spec: PatchSpec,
    patches_dir: Path,
    failure_ctx: FailureContext,
    on_patch_pending_webhook: WebhookEndpointConfig | None = None,
    source: str = "llm",
    webhook_event: str = "on_patch_pending",
) -> None:
    """Write patch to ``patches/pending/`` for human review.

    Args:
        on_patch_pending_webhook: When set, fires the on_patch_pending webhook
            with patch metadata. Errors are logged and never block staging.
        source: Provenance of the patch — ``"llm"`` for a fresh agent patch,
            ``"replay"`` when the heal cache re-staged a previously validated
            patch with zero LLM tokens.
        webhook_event: Envelope event name — ``"on_ci_patch"`` when the caller
            passes the ci endpoint config instead of the pending one.
    """
    pending_dir = patches_dir / "pending"
    pending_dir.mkdir(parents=True, exist_ok=True)
    filename = _patch_filename(patch_spec)
    out_path = pending_dir / filename
    payload = patch_spec.model_dump()
    sig_exact, sig_coarse = from_failure_context(failure_ctx)
    payload["_aq_meta"] = {
        "run_id": failure_ctx.run_id,
        "blueprint_id": failure_ctx.blueprint_id,
        "failed_module": failure_ctx.failed_module,
        "staged_at": _utcnow(),
        "prompt_version": PROMPT_VERSION,
        # Full dict (not just hash): coaching renders error_class/where/message
        # as the few-shot "failure" half without re-reading failure_contexts.
        "failure_signature": sig_exact.to_dict(),
        "failure_signature_coarse": sig_coarse.hash,
        "source": source,
    }
    out_path.write_text(json.dumps(_redact(payload), indent=2), encoding="utf-8")
    logger.info(
        "LLM patch staged for human review: %s  "
        "(apply with: aqueduct patch apply %s --blueprint <path>)",
        out_path, out_path,
    )

    if on_patch_pending_webhook is not None:
        try:
            from aqueduct.surveyor.webhook import fire_webhook
            # Phase 46 — the agent's structured diagnosis rides along so a
            # Slack/Teams message can say WHAT broke and WHY the fix should
            # work, not just "a patch is pending". Zero extra LLM calls —
            # these fields come from the PatchSpec the heal already produced.
            _diagnosis: dict[str, Any] = {
                "root_cause": patch_spec.root_cause or "",
                "rationale": patch_spec.rationale or "",
                "confidence": patch_spec.confidence,
                "category": patch_spec.category or "",
            }
            _first_op = patch_spec.operations[0] if patch_spec.operations else None
            if _first_op is not None and getattr(_first_op, "op", "") == "defer_to_human":
                _diagnosis["diagnosis"] = getattr(_first_op, "diagnosis", "")
                _diagnosis["suggestions"] = list(getattr(_first_op, "suggestions", ()) or ())
            fire_webhook(
                on_patch_pending_webhook,
                full_payload={
                    "patch_id": patch_spec.patch_id,
                    "run_id": failure_ctx.run_id,
                    "blueprint_id": failure_ctx.blueprint_id,
                    "failed_module": failure_ctx.failed_module,
                    "patch_path": str(out_path),
                    "source": source,
                    **_diagnosis,
                },
                template_vars={
                    "run_id": failure_ctx.run_id,
                    "blueprint_id": failure_ctx.blueprint_id,
                    "failed_module": failure_ctx.failed_module or "",
                    "patch_id": patch_spec.patch_id,
                    "root_cause": str(_diagnosis["root_cause"]),
                    "rationale": str(_diagnosis["rationale"]),
                    "confidence": f"{patch_spec.confidence:.2f}" if patch_spec.confidence is not None else "n/a",
                    "category": str(_diagnosis["category"]),
                },
                event=webhook_event,
            )
        except Exception:
            logger.debug("Webhook fire failed", exc_info=True)
            # webhook errors must never block staging


def archive_patch(
    patch_spec: PatchSpec,
    patches_dir: Path,
    failure_ctx: FailureContext,
    mode: str,
) -> None:
    """Write patch to ``patches/applied/`` with metadata."""
    applied_dir = patches_dir / "applied"
    applied_dir.mkdir(parents=True, exist_ok=True)
    filename = _patch_filename(patch_spec)
    archive_path = applied_dir / filename
    payload = patch_spec.model_dump()
    sig_exact, sig_coarse = from_failure_context(failure_ctx)
    payload["_aq_meta"] = {
        "run_id": failure_ctx.run_id,
        "blueprint_id": failure_ctx.blueprint_id,
        "failed_module": failure_ctx.failed_module,
        "applied_at": _utcnow(),
        "approval_mode": mode,
        "prompt_version": PROMPT_VERSION,
        "failure_signature": sig_exact.to_dict(),
        "failure_signature_coarse": sig_coarse.hash,
    }
    archive_path.write_text(json.dumps(_redact(payload), indent=2), encoding="utf-8")


# ── Main orchestration loop ──────────────────────────────────────────────


def generate_agent_patch(
    failure_ctx: FailureContext,
    model: str,
    patches_dir: Path,
    provider: str = "anthropic",
    base_url: str | None = None,
    max_tokens: int = 4096,
    provider_options: dict[str, Any] | None = None,
    timeout: float = 120.0,
    max_reprompts: int = 3,
    engine_prompt_context: str | None = None,
    blueprint_prompt_context: str | None = None,
    last_apply_error: str | None = None,
    guardrails: Any = None,
    budget: BudgetConfig | None = None,
    allow_defer: bool = False,
    deep_loop: bool = False,
    validate_callback: Callable[[Any], tuple[bool, str]] | None = None,
    apply_callback: Callable[[PatchSpec], tuple[bool, str | None, str | None, str | None]] | None = None,
    on_attempt: Callable[[Any], None] | None = None,
    model_cascade_position: int | None = None,
    memory_coaching: bool = True,
    retry_max_retries: int = 2,
    retry_backoff_seconds: float = 2.0,
) -> AgentPatchResult:
    """Call the LLM and return an AgentPatchResult with patch + attempt metadata.

    Each iteration:
      1. Call provider (records tokens + latency).
      2. Parse response — on schema/JSON failure record a signature and reprompt.
      3. If ``apply_callback`` is supplied, invoke it on the parsed PatchSpec.
      4. ``BudgetTracker.check_stop()`` decides whether to continue.
      5. If ``tracker.should_escalate()`` is True, the next attempt uses the
         escalated reprompt template + bumped sampling temperature.

    ``apply_callback`` signature::

        def cb(patch: PatchSpec) -> tuple[bool, str|None, str|None, str|None]:
            return success, error_class, error_message, where

    ``on_attempt`` is invoked with the AttemptRecord after each turn —
    used by the Surveyor to persist into ``heal_attempts``.
    """
    if budget is None:
        budget = BudgetConfig(max_reprompts=max(1, max_reprompts))
    tracker = BudgetTracker(budget)

    cfg = _ProviderConfig(
        model=model,
        max_tokens=max_tokens,
        provider=provider,
        base_url=base_url,
        provider_options=provider_options,
        timeout=timeout,
        patches_dir=patches_dir,
        engine_prompt_context=engine_prompt_context,
        blueprint_prompt_context=blueprint_prompt_context,
        allow_defer=allow_defer,
        failure_ctx=failure_ctx,
        coaching=memory_coaching,
        retry_max_retries=retry_max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
    )

    messages: list[dict[str, Any]] = [
        {
            "role": "user",
            "content": _build_user_prompt(failure_ctx, patches_dir, guardrails=guardrails),
        }
    ]

    patch_spec: PatchSpec | None = None
    reprompt_errors: list[str] = []
    escalate_next = False

    while True:
        attempt_num = tracker.begin_attempt()
        temperature_override = _ESCALATION_TEMPERATURE if escalate_next else None
        logger.info("── Heal attempt %d/%d ──", attempt_num, budget.max_reprompts)

        # Phase 40: per-call deadline for mid-call budget enforcement.
        # Uses min(agent.timeout, remaining budget seconds) so neither
        # constraint is violated — the tighter one wins.
        deadline = min(cfg.timeout, tracker.remaining_seconds())
        if deadline <= 0:
            # Budget exhausted before this call — record a zero-token
            # attempt and terminate with budget_seconds_exceeded.
            rec = tracker.record(
                None,
                tokens_in=0, tokens_out=0, latency_ms=0,
                gate_that_rejected="budget", escalated=escalate_next,
                model_cascade_position=model_cascade_position,
            )
            if on_attempt is not None:
                try:
                    on_attempt(rec)
                except Exception:
                    logger.debug("on_attempt callback raised; ignoring", exc_info=True)
            tracker.mark_budget_seconds_exceeded()
            break

        t_start = time.monotonic()
        try:
            raw, tokens_in, tokens_out = _call_agent(
                messages, cfg, patches_dir,
                last_apply_error=last_apply_error,
                temperature_override=temperature_override,
                deadline=deadline,
            )
        except Exception as exc:
            latency_ms = int((time.monotonic() - t_start) * 1000)

            # Phase 40: distinguish budget-driven timeout from API timeout.
            # When the per-call deadline was constrained by the budget
            # (deadline < cfg.timeout), any TimeoutException means the
            # budget ran out mid-call — terminate with budget_seconds_exceeded
            # instead of a generic api_error.
            import httpx
            if isinstance(exc, httpx.TimeoutException) and deadline < cfg.timeout:
                logger.warning(
                    "LLM API call timed out on budget deadline %.1fs "
                    "(attempt %d/%d): %s",
                    deadline, attempt_num, budget.max_reprompts, exc,
                )
                reprompt_errors.append(f"Budget seconds exceeded: {exc}")
                sig = from_exception(exc, where="provider")
                rec = tracker.record(
                    sig,
                    tokens_in=0, tokens_out=0, latency_ms=latency_ms,
                    gate_that_rejected="budget", escalated=escalate_next,
                    model_cascade_position=model_cascade_position,
                )
                if on_attempt is not None:
                    try:
                        on_attempt(rec)
                    except Exception:
                        logger.debug("on_attempt callback raised; ignoring", exc_info=True)
                tracker.mark_budget_seconds_exceeded()
                break

            hint = _format_llm_error_hint(exc, timeout=timeout, base_url=base_url, model=model)
            logger.error(
                "LLM API call failed (attempt %d/%d): %s%s",
                attempt_num, budget.max_reprompts, exc, hint,
            )
            reprompt_errors.append(f"API error: {exc}")
            sig = from_exception(exc, where="provider")
            rec = tracker.record(
                sig,
                tokens_in=0, tokens_out=0, latency_ms=latency_ms,
                gate_that_rejected="provider", escalated=escalate_next,
                model_cascade_position=model_cascade_position,
            )
            if on_attempt is not None:
                try:
                    on_attempt(rec)
                except Exception:
                    logger.debug("on_attempt callback raised; ignoring", exc_info=True)
            tracker.mark_api_error()
            break

        latency_ms = int((time.monotonic() - t_start) * 1000)
        logger.info(
            "  ⚡ LLM: %d → %d tokens, %dms",
            tokens_in, tokens_out, latency_ms,
        )
        logger.debug("LLM raw response (attempt %d):\n%s", attempt_num, raw)

        # ── Parse phase ────────────────────────────────────────────────
        parse_exc: BaseException | None = None
        recovery_applied: list[str] = []
        try:
            patch_spec, recovery_applied = _parse_patch_spec(raw)
        except (ValidationError, json.JSONDecodeError, ValueError) as exc:
            parse_exc = exc

        if parse_exc is not None:
            friendly = _format_reprompt_error(parse_exc, raw)
            reprompt_errors.append(friendly)
            logger.warning(
                "LLM patch response invalid (attempt %d/%d):\n%s",
                attempt_num, budget.max_reprompts, friendly,
            )
            if isinstance(parse_exc, ValidationError):
                sig = from_validation_error(parse_exc)
            elif isinstance(parse_exc, json.JSONDecodeError):
                sig = from_json_decode_error(parse_exc)
            else:
                sig = from_exception(parse_exc, where="parse")

            rec = tracker.record(
                sig,
                tokens_in=tokens_in, tokens_out=tokens_out, latency_ms=latency_ms,
                gate_that_rejected="schema", escalated=escalate_next,
                model_cascade_position=model_cascade_position,
            )
            if on_attempt is not None:
                try:
                    on_attempt(rec)
                except Exception:
                    logger.debug("on_attempt callback raised; ignoring", exc_info=True)

            stop = tracker.check_stop()
            if stop is not None and stop != "solved":
                patch_spec = None
                break

            escalate_next = tracker.should_escalate()
            if escalate_next:
                tracker.mark_escalated()
                logger.info(
                    "stuck-detection escalation triggered on attempt %d "
                    "(temperature=%.2f, escalated template)",
                    attempt_num, _ESCALATION_TEMPERATURE,
                )

            structural_hint = _detect_structural_error(parse_exc, raw) or ""
            reprompt_msg = _format_reprompt_for_next_turn(
                friendly=friendly, raw=raw, escalated=escalate_next,
                structural_hint=structural_hint,
            )
            messages.append({"role": "assistant", "content": raw})
            messages.append({"role": "user", "content": reprompt_msg})
            continue

        # Log successful parse with confidence and op summary
        _ops_summary = ", ".join(
            f"{o.op}({getattr(o, 'module_id', '') or getattr(o, 'key', '') or ''})"
            for o in patch_spec.operations
        )
        _conf_str = (
            f"{patch_spec.confidence:.2f}" if patch_spec.confidence is not None else "n/a"
        )
        logger.info(
            "  ✓ Parsed: %s (confidence %s, %d op%s: %s)",
            patch_spec.patch_id,
            _conf_str,
            len(patch_spec.operations),
            "s" if len(patch_spec.operations) != 1 else "",
            _ops_summary or "(none)",
        )

        # ── Phase 41: defer_to_human detection ─────────────────────────────
        # Runs before the apply callback — defer makes zero Blueprint
        # changes, so there is nothing to guardrail, compile-check, or sandbox.
        has_defer = any(op.op == "defer_to_human" for op in patch_spec.operations)
        if has_defer:
            if not allow_defer:
                # Model produced defer when not allowed (should be rare — the
                # prompt hides defer when allow_defer=False). Reprompt so it
                # produces a real fix.
                friendly = (
                    "defer_to_human is not enabled for this blueprint. "
                    "Produce a real PatchSpec with Blueprint-mutating operations."
                )
                reprompt_errors.append(friendly)
                logger.warning(
                    "LLM deferred when allow_defer=False (attempt %d/%d)",
                    attempt_num, budget.max_reprompts,
                )
                sig = from_apply_error(
                    "defer_rejected", friendly, where="loop",
                )
                rec = tracker.record(
                    sig,
                    tokens_in=tokens_in, tokens_out=tokens_out, latency_ms=latency_ms,
                    gate_that_rejected="defer_rejected", escalated=escalate_next,
                    model_cascade_position=model_cascade_position,
                )
                if on_attempt is not None:
                    try:
                        on_attempt(rec)
                    except Exception:
                        logger.debug("on_attempt callback raised; ignoring", exc_info=True)

                stop = tracker.check_stop()
                if stop is not None and stop != "solved":
                    patch_spec = None
                    break

                escalate_next = tracker.should_escalate()
                if escalate_next:
                    tracker.mark_escalated()
                    logger.info(
                        "stuck-detection escalation triggered on attempt %d "
                        "(temperature=%.2f, escalated template)",
                        attempt_num, _ESCALATION_TEMPERATURE,
                    )

                reprompt_msg = _format_reprompt_for_next_turn(
                    friendly=friendly, raw=raw, escalated=escalate_next,
                    structural_hint="",
                )
                messages.append({"role": "assistant", "content": raw})
                messages.append({"role": "user", "content": reprompt_msg})
                patch_spec = None
                continue

            # Defer is allowed — terminate the loop cleanly.
            rec = tracker.record(
                None,
                tokens_in=tokens_in, tokens_out=tokens_out, latency_ms=latency_ms,
                gate_that_rejected=None, escalated=escalate_next,
                model_cascade_position=model_cascade_position,
            )
            if on_attempt is not None:
                try:
                    on_attempt(rec)
                except Exception:
                    logger.debug("on_attempt callback raised; ignoring", exc_info=True)
            tracker.mark_deferred()
            break

        # ── Phase 43: in-conversation validation (deep_loop) ────────────────
        # When deep_loop is enabled, sandbox / lineage / explain gates run
        # INSIDE the LLM conversation.  If the patch fails validation, the
        # rejection feedback is injected as a user message and the model
        # retries immediately — same conversation, learned context.
        # Without deep_loop: gates run only in the apply_callback (post-hoc).
        if deep_loop and validate_callback is not None:
            # Phase 46: gate work (sandbox replay = Spark time) is excluded
            # from max_seconds — the budget caps LLM-conversation time only.
            with tracker.pause_clock():
                try:
                    validated, vfeedback = validate_callback(patch_spec)
                except Exception as vcb_exc:
                    logger.debug("validate_callback raised; treating as validation fail", exc_info=True)
                    validated, vfeedback = False, f"Validation error: {vcb_exc}"

            if not validated:
                logger.info(
                    "Deep-loop validation rejected patch (attempt %d/%d): %s",
                    attempt_num, budget.max_reprompts,
                    vfeedback[:200] if vfeedback else "(no detail)",
                )
                reprompt_errors.append(f"Validation rejected: {vfeedback}")
                sig = from_apply_error(
                    "validation_rejected", vfeedback or "(no detail)", where="validate",
                )
                rec = tracker.record(
                    sig,
                    tokens_in=tokens_in, tokens_out=tokens_out, latency_ms=latency_ms,
                    gate_that_rejected="validate", escalated=escalate_next,
                    model_cascade_position=model_cascade_position,
                )
                if on_attempt is not None:
                    try:
                        on_attempt(rec)
                    except Exception:
                        logger.debug("on_attempt callback raised; ignoring", exc_info=True)

                stop = tracker.check_stop()
                if stop is not None and stop != "solved":
                    patch_spec = None
                    break

                escalate_next = tracker.should_escalate()
                if escalate_next:
                    tracker.mark_escalated()

                reprompt_msg = _format_reprompt_for_next_turn(
                    friendly=vfeedback or "Validation rejected your patch.",
                    raw=raw, escalated=escalate_next,
                    structural_hint="",
                )
                messages.append({"role": "assistant", "content": raw})
                messages.append({"role": "user", "content": reprompt_msg})
                patch_spec = None
                continue

            # Validation passed — only reached when deep_loop + validate_callback
            # returned (True, "").
            logger.info("  ✓ Validation: PASS")

        # ── Apply phase (optional callback) ─────────────────────────────
        if apply_callback is not None:
            try:
                apply_result = apply_callback(patch_spec)
            except Exception as cb_exc:
                logger.debug("apply_callback raised; treating as gate rejection", exc_info=True)
                apply_result = (False, type(cb_exc).__name__, str(cb_exc), None)

            ok, err_class, err_msg, err_where = (apply_result + (None,) * 4)[:4]
            if ok:
                rec = tracker.record(
                    None,
                    tokens_in=tokens_in, tokens_out=tokens_out, latency_ms=latency_ms,
                    gate_that_rejected=None, escalated=escalate_next,
                    model_cascade_position=model_cascade_position,
                )
                if on_attempt is not None:
                    try:
                        on_attempt(rec)
                    except Exception:
                        logger.debug("on_attempt callback raised; ignoring", exc_info=True)
                tracker.check_stop()  # sets 'solved'
                logger.info("  ✓ Applied: patch accepted by guardrails + apply")
                break

            # Gate rejection — record signature, reprompt with the gate's error.
            sig = from_apply_error(
                err_class or "apply_error",
                err_msg or "(no message)",
                where=err_where,
            )
            friendly = (
                f"Your PatchSpec parsed but was rejected by the apply gate "
                f"({err_class or 'apply_error'}): {err_msg or '(no message)'}"
            )
            reprompt_errors.append(friendly)
            logger.warning(
                "Patch apply gate rejected attempt %d/%d: %s",
                attempt_num, budget.max_reprompts, friendly,
            )
            rec = tracker.record(
                sig,
                tokens_in=tokens_in, tokens_out=tokens_out, latency_ms=latency_ms,
                gate_that_rejected="apply", escalated=escalate_next,
                model_cascade_position=model_cascade_position,
            )
            if on_attempt is not None:
                try:
                    on_attempt(rec)
                except Exception:
                    logger.debug("on_attempt callback raised; ignoring", exc_info=True)

            stop = tracker.check_stop()
            if stop is not None and stop != "solved":
                patch_spec = None
                break

            escalate_next = tracker.should_escalate()
            if escalate_next:
                tracker.mark_escalated()
                logger.info(
                    "stuck-detection escalation triggered on attempt %d "
                    "(temperature=%.2f, escalated template)",
                    attempt_num, _ESCALATION_TEMPERATURE,
                )

            reprompt_msg = _format_reprompt_for_next_turn(
                friendly=friendly, raw=raw, escalated=escalate_next,
                structural_hint="",
            )
            messages.append({"role": "assistant", "content": raw})
            messages.append({"role": "user", "content": reprompt_msg})
            patch_spec = None
            continue

        # No apply_callback — legacy schema-only success exits the loop.
        rec = tracker.record(
            None,
            tokens_in=tokens_in, tokens_out=tokens_out, latency_ms=latency_ms,
            gate_that_rejected=None, escalated=escalate_next,
            model_cascade_position=model_cascade_position,
        )
        if on_attempt is not None:
            try:
                on_attempt(rec)
            except Exception:
                logger.debug("on_attempt callback raised; ignoring", exc_info=True)
        tracker.check_stop()
        break

    if patch_spec is None:
        logger.info(
            "── Heal complete: no patch (%d attempts, stop_reason=%s, %d tokens in, %d out) ──",
            tracker.current_attempt, tracker.stop_reason,
            tracker.tokens_in_total, tracker.tokens_out_total,
        )
        logger.error(
            "LLM agent failed to produce a valid PatchSpec after %d attempt(s) "
            "for blueprint %r run %r (stop_reason=%s)",
            tracker.current_attempt, failure_ctx.blueprint_id, failure_ctx.run_id,
            tracker.stop_reason,
        )
    else:
        logger.info(
            "── Heal complete: %s (confidence %s, %d ops, stop_reason=%s, %d tokens in, %d out) ──",
            patch_spec.patch_id,
            f"{patch_spec.confidence:.2f}" if patch_spec.confidence is not None else "n/a",
            len(patch_spec.operations), tracker.stop_reason,
            tracker.tokens_in_total, tracker.tokens_out_total,
        )
    return AgentPatchResult(
        patch=patch_spec,
        attempts=tracker.current_attempt,
        reprompt_errors=reprompt_errors,
        stop_reason=tracker.stop_reason,
        tokens_in_total=tracker.tokens_in_total,
        tokens_out_total=tracker.tokens_out_total,
        attempt_records=list(tracker.attempts),
        escalated=tracker.escalated_once,
        recovery_applied=recovery_applied if patch_spec is not None else [],
        model=model,
        model_cascade_position=model_cascade_position,
    )
