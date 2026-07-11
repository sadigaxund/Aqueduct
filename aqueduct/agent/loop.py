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
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from aqueduct.agent.budget import (
    BudgetConfig,
    BudgetTracker,
    StopReason,
)
from aqueduct.agent.constants import DEFAULT_LLM_TIMEOUT, DEFAULT_MAX_TOKENS
from aqueduct.agent.parse import (
    AgentParseError,
    _detect_structural_error,
    _format_reprompt_error,
    _format_reprompt_for_next_turn,
    _parse_patch_spec,
)
from aqueduct.agent.prompts import _build_user_prompt
from aqueduct.agent.providers import (
    _ESCALATION_TEMPERATURE,
    ToolCallState,
    _call_agent,
    _format_llm_error_hint,
    _ProviderConfig,
)
from aqueduct.agent.signature import (
    from_apply_error,
    from_exception,
    from_failure_context,
    from_json_decode_error,
    from_validation_error,
)
from aqueduct.patch.grammar import PATCH_META_KEY, PatchSpec
from aqueduct.redaction import redact as _redact
from aqueduct.surveyor.models import FailureContext
from aqueduct.utils import utcnow_iso

if TYPE_CHECKING:
    from aqueduct.config import WebhookEndpointConfig
    from aqueduct.stores.base import ObservabilityStore
    from aqueduct.stores.object_store import PatchStore

logger = logging.getLogger(__name__)

# Bump manually when the system prompt changes significantly.
# Stored in patch _aq_meta so benchmark results can be correlated to prompt versions.
# 1.2 — Phase 45: signature-matched coaching section replaces the chronological
#       patch-history section; recovery-pass polish (orphan </think>, fence
#       selection, multi-key wrapper unwrap, reprompt char cap).
# 1.3 — Phase 47: replace_macro op added to the grammar/schema; macro hint
#       now points at it for in-macro root causes.
# 1.4 — drift rule: PREDICTED_SCHEMA_DRIFT means the source changed; do NOT
#       flip Ingress format/header/options — fix downstream or do nothing.
# 1.5 — schema_hint type-mismatch / field-not-found rule: `expected` in the
#       type-mismatch message is the CURRENT declared value (re-proposing it
#       is a no-op); `actual` is what Spark inferred. field-not-found lists
#       real columns — align the key to one of them, never re-type it.
# 1.6 — schema_hint rule reworded: no literal `defer_to_human` token in the
#       static template (leaked the op name when allow_defer=False strips it
#       from the schema — Phase 41 invariant: the model can't produce an op
#       it doesn't know exists).
# 1.7 — 2026-07-11: agentic mode (agent.mode: agentic) — the system prompt
#       gains a "Tools available" addendum on any turn where tool-use is
#       offered (`prompts._TOOLS_SECTION`). Oneshot-mode heals (the default)
#       never render it — their prompt is byte-identical to 1.6.
PROMPT_VERSION = "1.7"


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


@dataclass(frozen=True)
class AgentRunConfig:
    """All inputs for one ``generate_agent_patch`` invocation.

    Collapses the ~25 keyword args into a single frozen object so CLI call
    sites build it once and the function body reads from ``cfg.*``.
    Defaults are byte-identical to the old ``generate_agent_patch`` signature.
    """

    failure_ctx: FailureContext
    model: str
    patches_dir: Path
    provider: str = "anthropic"
    base_url: str | None = None
    api_key: str | None = None
    max_tokens: int = DEFAULT_MAX_TOKENS
    provider_options: dict[str, Any] | None = None
    timeout: float = DEFAULT_LLM_TIMEOUT
    max_reprompts: int = 3
    engine_prompt_context: str | None = None
    blueprint_prompt_context: str | None = None
    last_apply_error: str | None = None
    guardrails: Any = None
    budget: BudgetConfig | None = None
    allow_defer: bool = False
    deep_loop: bool = False
    validate_callback: Callable[[Any], tuple[bool, str]] | None = None
    apply_callback: Callable[[PatchSpec], tuple[bool, str | None, str | None, str | None]] | None = None
    on_attempt: Callable[[Any], None] | None = None
    on_token: Callable[[str, str], None] | None = None  # live SSE sink: (kind, text)
    model_cascade_position: int | None = None
    memory_coaching: bool = True
    retry_max_retries: int = 2
    retry_backoff_seconds: float = 2.0
    obs_store: ObservabilityStore | None = None
    # Phase 75 — agentic mode. `toolbox` is None ⇒ byte-identical oneshot
    # behaviour regardless of `mode` (a caller that resolved mode="agentic"
    # but couldn't build a ToolBox — e.g. no manifest — degrades safely).
    toolbox: Any = None
    mode: str = "oneshot"
    max_tool_calls: int = 8
    supports_tools: bool | str = "auto"


# ── Timestamp helpers ────────────────────────────────────────────────────


def _utcnow() -> str:
    """ISO-8601 UTC timestamp, e.g. 2026-06-04T12:34:56.789+00:00."""
    return utcnow_iso()


def _patch_filename(patch_spec: PatchSpec) -> str:
    """Generate structured filename: {YYYYMMDDTHHmmss}_{slug}.json."""
    ts = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%S")
    return f"{ts}_{patch_spec.patch_id}.json"


def _fire_on_attempt(on_attempt, rec) -> None:
    """Best-effort invoke an on_attempt callback.  Never raises."""
    if on_attempt is not None:
        try:
            on_attempt(rec)
        except Exception:
            logger.debug("on_attempt callback raised; ignoring", exc_info=True)


def _check_budget_and_escalate(tracker, attempt_num: int) -> tuple[bool, bool]:
    """Check budget stop + stuck-detection escalation.  Returns (should_break, escalate_next)."""
    stop = tracker.check_stop()
    if stop is not None and stop != StopReason.SOLVED:
        return True, False
    escalate_next = tracker.should_escalate()
    if escalate_next:
        tracker.mark_escalated()
        logger.info(
            "stuck-detection escalation triggered on attempt %d "
            "(temperature=%.2f, escalated template)",
            attempt_num, _ESCALATION_TEMPERATURE,
        )
    return False, escalate_next


def _patch_store_for(patches_dir: Path, patch_store: PatchStore | None) -> PatchStore:
    """Return the given PatchStore or a local one rooted at *patches_dir*.

    The local default reproduces the historical ``patches/`` directory exactly,
    so callers that pass only ``patches_dir`` keep writing real files."""
    if patch_store is not None:
        return patch_store
    from aqueduct.stores.object_store import make_patch_store
    return make_patch_store("local", "", patches_dir)


def _record_patch_index(
    obs_store: ObservabilityStore | None,
    *,
    patch_spec: PatchSpec,
    failure_ctx: FailureContext,
    status: str,
    object_key: str,
    source: str,
) -> None:
    """Upsert the ``patch_index`` row for a staged/archived patch.

    Best-effort and never raises — index gaps degrade the heal cache to a fresh
    LLM call, never a crash. No-op when no observability store is available."""
    if obs_store is None:
        return
    try:
        from aqueduct.patch import index as _ix
        sig_exact, sig_coarse = from_failure_context(failure_ctx)
        row = _ix.PatchIndexRow(
            patch_id=patch_spec.patch_id,
            status=status,
            object_key=object_key,
            blueprint_id=failure_ctx.blueprint_id or "",
            run_id=failure_ctx.run_id or "",
            signature=sig_exact.hash,
            signature_coarse=sig_coarse.hash,
            error_class=sig_exact.error_class or "",
            where_field=sig_exact.where or "",
            normalized_message=sig_exact.normalized_message or "",
            rationale=patch_spec.rationale or patch_spec.root_cause or "",
            ops=[op.op for op in patch_spec.operations],
            source=source,
            prompt_version=PROMPT_VERSION,
        )
        with obs_store.connect() as cur:
            _ix.ensure_schema(cur)
            _ix.upsert(cur, row)
    except Exception:
        logger.warning("patch_index upsert failed for %s", patch_spec.patch_id, exc_info=True)


# ── Patch I/O ────────────────────────────────────────────────────────────


def stage_patch_for_human(
    patch_spec: PatchSpec,
    patches_dir: Path,
    failure_ctx: FailureContext,
    on_patch_pending_webhook: WebhookEndpointConfig | None = None,
    source: str = "llm",
    webhook_event: str = "on_patch_pending",
    patch_store: PatchStore | None = None,
    obs_store: ObservabilityStore | None = None,
) -> None:
    """Write a patch body to ``patches/pending/`` and index it for review.

    Args:
        on_patch_pending_webhook: When set, fires the on_patch_pending webhook
            with patch metadata. Errors are logged and never block staging.
        source: Provenance of the patch — ``"llm"`` for a fresh agent patch,
            ``"replay"`` when the heal cache re-staged a previously validated
            patch with zero LLM tokens.
        webhook_event: Envelope event name — ``"on_ci_patch"`` when the caller
            passes the ci endpoint config instead of the pending one.
        patch_store: Object store for the body. None → a local store rooted at
            *patches_dir* (historical on-disk layout).
        obs_store: Observability store for the ``patch_index`` upsert. None skips
            indexing (the heal cache then can't reuse this pending patch).
    """
    ps = _patch_store_for(patches_dir, patch_store)
    filename = _patch_filename(patch_spec)
    payload = patch_spec.model_dump()
    sig_exact, sig_coarse = from_failure_context(failure_ctx)
    payload[PATCH_META_KEY] = {
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
    object_key = ps.write_pending(filename, _redact(payload))
    _record_patch_index(
        obs_store, patch_spec=patch_spec, failure_ctx=failure_ctx,
        status="pending", object_key=object_key, source=source,
    )
    out_path = f"{ps.location_label}/{object_key}"
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
                    "patch_path": out_path,
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
    patch_store: PatchStore | None = None,
    obs_store: ObservabilityStore | None = None,
) -> None:
    """Write a patch body to ``patches/applied/`` and mark it applied in the index."""
    ps = _patch_store_for(patches_dir, patch_store)
    filename = _patch_filename(patch_spec)
    payload = patch_spec.model_dump()
    sig_exact, sig_coarse = from_failure_context(failure_ctx)
    payload[PATCH_META_KEY] = {
        "run_id": failure_ctx.run_id,
        "blueprint_id": failure_ctx.blueprint_id,
        "failed_module": failure_ctx.failed_module,
        "applied_at": _utcnow(),
        "approval_mode": mode,
        "prompt_version": PROMPT_VERSION,
        "failure_signature": sig_exact.to_dict(),
        "failure_signature_coarse": sig_coarse.hash,
    }
    object_key = ps.write_applied(filename, _redact(payload))
    _record_patch_index(
        obs_store, patch_spec=patch_spec, failure_ctx=failure_ctx,
        status="applied", object_key=object_key, source="llm",
    )


# ── Main orchestration loop ──────────────────────────────────────────────


def generate_agent_patch(
    failure_ctx: FailureContext | None = None,
    model: str | None = None,
    patches_dir: Path | None = None,
    provider: str = "anthropic",
    base_url: str | None = None,
    api_key: str | None = None,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    provider_options: dict[str, Any] | None = None,
    timeout: float = DEFAULT_LLM_TIMEOUT,
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
    on_token: Callable[[str, str], None] | None = None,
    model_cascade_position: int | None = None,
    memory_coaching: bool = True,
    retry_max_retries: int = 2,
    retry_backoff_seconds: float = 2.0,
    obs_store: ObservabilityStore | None = None,
    agent_cfg: AgentRunConfig | None = None,
    toolbox: Any = None,
    mode: str = "oneshot",
    max_tool_calls: int = 8,
    supports_tools: bool | str = "auto",
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
    if agent_cfg is not None:
        failure_ctx = agent_cfg.failure_ctx
        model = agent_cfg.model
        patches_dir = agent_cfg.patches_dir
        provider = agent_cfg.provider
        base_url = agent_cfg.base_url
        api_key = agent_cfg.api_key
        max_tokens = agent_cfg.max_tokens
        provider_options = agent_cfg.provider_options
        timeout = agent_cfg.timeout
        max_reprompts = agent_cfg.max_reprompts
        engine_prompt_context = agent_cfg.engine_prompt_context
        blueprint_prompt_context = agent_cfg.blueprint_prompt_context
        last_apply_error = agent_cfg.last_apply_error
        guardrails = agent_cfg.guardrails
        budget = agent_cfg.budget
        allow_defer = agent_cfg.allow_defer
        deep_loop = agent_cfg.deep_loop
        validate_callback = agent_cfg.validate_callback
        apply_callback = agent_cfg.apply_callback
        on_attempt = agent_cfg.on_attempt
        on_token = agent_cfg.on_token
        model_cascade_position = agent_cfg.model_cascade_position
        memory_coaching = agent_cfg.memory_coaching
        retry_max_retries = agent_cfg.retry_max_retries
        retry_backoff_seconds = agent_cfg.retry_backoff_seconds
        obs_store = agent_cfg.obs_store
        toolbox = agent_cfg.toolbox
        mode = agent_cfg.mode
        max_tool_calls = agent_cfg.max_tool_calls
        supports_tools = agent_cfg.supports_tools

    if agent_cfg is None:
        if failure_ctx is None:
            raise TypeError("generate_agent_patch: failure_ctx is required when agent_cfg is not provided")
        if model is None:
            raise TypeError("generate_agent_patch: model is required when agent_cfg is not provided")
        if patches_dir is None:
            raise TypeError("generate_agent_patch: patches_dir is required when agent_cfg is not provided")

    if budget is None:
        budget = BudgetConfig(max_reprompts=max(1, max_reprompts))
    tracker = BudgetTracker(budget)

    # Phase 75 — agentic mode only takes effect when BOTH mode="agentic" AND
    # a ToolBox was actually built by the caller. A caller that resolved
    # mode="agentic" but has no manifest to build one (e.g. a legacy call
    # site) degrades to oneshot instead of raising.
    _agentic = mode == "agentic" and toolbox is not None
    _effective_toolbox = toolbox if _agentic else None
    # Persists across every attempt in THIS heal call — tool-use capability
    # (openai_compat's supports_tools: auto probe) doesn't change between
    # reprompt turns, but `tool_calls_used` is reset per attempt below (the
    # cap is per-attempt, not per-heal — design item 2).
    _tool_state = ToolCallState() if _agentic else None

    cfg = _ProviderConfig(
        model=model,
        max_tokens=max_tokens,
        provider=provider,
        base_url=base_url,
        api_key=api_key,
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
        obs_store=obs_store,
        toolbox=_effective_toolbox,
        max_tool_calls=max(1, max_tool_calls),
        supports_tools=supports_tools,
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

    # Per-turn raw model output, surfaced to the -v transcript (verbatim view of
    # what the model returned — the key diagnostic when a patch won't parse).
    # Transient display data; never persisted to heal_attempts. Reset each turn,
    # set after a successful provider call; None on a failed call.
    _turn_raw: dict[str, str | None] = {"v": None}

    def _fire_turn(rec) -> None:
        rec._aq_raw = _turn_raw["v"]
        # Phase 75 — per-call tool telemetry for THIS attempt only (verbose
        # transcript rendering); empty in oneshot mode / no tool calls made.
        rec._aq_tool_calls = list(_tool_state.tool_call_log) if _tool_state else []
        _fire_on_attempt(on_attempt=on_attempt, rec=rec)

    def _record(*args, **kwargs):
        """``tracker.record`` wrapper that auto-attaches this attempt's tool-call
        count (Phase 75) — every one of the ~10 call sites below stays unchanged
        otherwise. 0 when not in agentic mode."""
        kwargs.setdefault("tool_calls", _tool_state.tool_calls_used if _tool_state else 0)
        return tracker.record(*args, **kwargs)

    while True:
        _turn_raw["v"] = None
        # The per-attempt tool-call cap (design item 2) resets every attempt —
        # capability state (_tool_state.supported) persists across attempts.
        if _tool_state is not None:
            _tool_state.tool_calls_used = 0
            _tool_state.tool_call_log = []
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
            rec = _record(
                None,
                tokens_in=0, tokens_out=0, latency_ms=0,
                gate_that_rejected="budget", escalated=escalate_next,
                model_cascade_position=model_cascade_position,
            )
            _fire_turn(rec)
            tracker.mark_budget_seconds_exceeded()
            break

        t_start = time.monotonic()
        try:
            raw, tokens_in, tokens_out = _call_agent(
                messages, cfg, patches_dir,
                last_apply_error=last_apply_error,
                temperature_override=temperature_override,
                deadline=deadline,
                on_token=on_token,
                tool_state=_tool_state,
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
                # Demoted to debug — the transcript renders this on the turn line
                # (✗ budget exhausted) + the hint below; a loose warning here would
                # also interleave above the tree.
                logger.debug(
                    "LLM API call timed out on budget deadline %.1fs "
                    "(attempt %d/%d): %s",
                    deadline, attempt_num, budget.max_reprompts, exc,
                )
                reprompt_errors.append(f"Budget seconds exceeded: {exc}")
                sig = from_exception(exc, where="provider")
                rec = _record(
                    sig,
                    tokens_in=0, tokens_out=0, latency_ms=latency_ms,
                    gate_that_rejected="budget", escalated=escalate_next,
                    model_cascade_position=model_cascade_position,
                )
                # The total-heal budget (agent.budget.max_seconds), NOT agent.timeout,
                # capped this call — point the user at the right knob.
                rec._aq_detail = f"hit the {deadline:.0f}s heal budget mid-call"
                rec._aq_hint = (
                    f"the {budget.max_seconds:.0f}s agent.budget.max_seconds budget capped this "
                    f"call (agent.timeout was {cfg.timeout:.0f}s) — raise agent.budget.max_seconds "
                    f"(and agent.timeout) for slow/local models"
                )
                _fire_turn(rec)
                tracker.mark_budget_seconds_exceeded()
                break

            hint = _format_llm_error_hint(exc, timeout=timeout, base_url=base_url, model=model)
            # Demoted to debug: the transcript renders the failure inline on the
            # tier's turn line (✗ provider error — <reason>) + the hint, so this
            # full line would just duplicate it. Still available with -v / json.
            logger.debug(
                "LLM API call failed (attempt %d/%d): %s%s",
                attempt_num, budget.max_reprompts, exc, hint,
            )
            reprompt_errors.append(f"API error: {exc}")
            sig = from_exception(exc, where="provider")
            rec = _record(
                sig,
                tokens_in=0, tokens_out=0, latency_ms=latency_ms,
                gate_that_rejected="provider", escalated=escalate_next,
                model_cascade_position=model_cascade_position,
            )
            # Transcript display data (transient — not persisted to heal_attempts).
            _reason = str(exc).split(" for url")[0].replace("Client error ", "").replace(
                "Server error ", "").strip(" '\"") or type(exc).__name__
            rec._aq_detail = _reason
            rec._aq_hint = hint.strip().removeprefix("hint:").strip() or None
            _fire_turn(rec)
            tracker.mark_api_error()
            break

        latency_ms = int((time.monotonic() - t_start) * 1000)
        logger.info(
            "  ⚡ LLM: %d → %d tokens, %dms",
            tokens_in, tokens_out, latency_ms,
        )
        logger.debug("LLM raw response (attempt %d):\n%s", attempt_num, raw)
        _turn_raw["v"] = raw  # captured for the -v transcript

        # ── Parse phase ────────────────────────────────────────────────
        parse_exc: BaseException | None = None
        recovery_applied: list[str] = []
        try:
            patch_spec, recovery_applied = _parse_patch_spec(raw)
        except (ValidationError, json.JSONDecodeError, ValueError, AgentParseError) as exc:
            parse_exc = exc

        if parse_exc is not None:
            friendly = _format_reprompt_error(parse_exc, raw)
            reprompt_errors.append(friendly)
            # Demoted to debug — the transcript shows the parse failure inline
            # (✗ invalid patch (schema) — <first line>). Full text via -v / json.
            logger.debug(
                "LLM patch response invalid (attempt %d/%d):\n%s",
                attempt_num, budget.max_reprompts, friendly,
            )
            if isinstance(parse_exc, ValidationError):
                sig = from_validation_error(parse_exc)
            elif isinstance(parse_exc, json.JSONDecodeError):
                sig = from_json_decode_error(parse_exc)
            else:
                sig = from_exception(parse_exc, where="parse")

            rec = _record(
                sig,
                tokens_in=tokens_in, tokens_out=tokens_out, latency_ms=latency_ms,
                gate_that_rejected="schema", escalated=escalate_next,
                model_cascade_position=model_cascade_position,
            )
            _short = friendly.strip().splitlines()[0] if friendly.strip() else ""
            rec._aq_detail = _short[:120] or None
            _fire_turn(rec)

            _should_break, escalate_next = _check_budget_and_escalate(tracker, attempt_num)
            if _should_break:
                patch_spec = None
                break

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
                rec = _record(
                    sig,
                    tokens_in=tokens_in, tokens_out=tokens_out, latency_ms=latency_ms,
                    gate_that_rejected="defer_rejected", escalated=escalate_next,
                    model_cascade_position=model_cascade_position,
                )
                _fire_turn(rec)

                _should_break, escalate_next = _check_budget_and_escalate(tracker, attempt_num)
                if _should_break:
                    patch_spec = None
                    break

                reprompt_msg = _format_reprompt_for_next_turn(
                    friendly=friendly, raw=raw, escalated=escalate_next,
                    structural_hint="",
                )
                messages.append({"role": "assistant", "content": raw})
                messages.append({"role": "user", "content": reprompt_msg})
                patch_spec = None
                continue

            # Defer is allowed — terminate the loop cleanly.
            rec = _record(
                None,
                tokens_in=tokens_in, tokens_out=tokens_out, latency_ms=latency_ms,
                gate_that_rejected=None, escalated=escalate_next,
                model_cascade_position=model_cascade_position,
            )
            _fire_turn(rec)
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
                rec = _record(
                    sig,
                    tokens_in=tokens_in, tokens_out=tokens_out, latency_ms=latency_ms,
                    gate_that_rejected="validate", escalated=escalate_next,
                    model_cascade_position=model_cascade_position,
                )
                _fire_turn(rec)

                stop = tracker.check_stop()
                if stop is not None and stop != StopReason.SOLVED:
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
                rec = _record(
                    None,
                    tokens_in=tokens_in, tokens_out=tokens_out, latency_ms=latency_ms,
                    gate_that_rejected=None, escalated=escalate_next,
                    model_cascade_position=model_cascade_position,
                )
                _fire_turn(rec)
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
            rec = _record(
                sig,
                tokens_in=tokens_in, tokens_out=tokens_out, latency_ms=latency_ms,
                gate_that_rejected="apply", escalated=escalate_next,
                model_cascade_position=model_cascade_position,
            )
            _fire_turn(rec)

            _should_break, escalate_next = _check_budget_and_escalate(tracker, attempt_num)
            if _should_break:
                patch_spec = None
                break

            reprompt_msg = _format_reprompt_for_next_turn(
                friendly=friendly, raw=raw, escalated=escalate_next,
                structural_hint="",
            )
            messages.append({"role": "assistant", "content": raw})
            messages.append({"role": "user", "content": reprompt_msg})
            patch_spec = None
            continue

        # No apply_callback — legacy schema-only success exits the loop.
        rec = _record(
            None,
            tokens_in=tokens_in, tokens_out=tokens_out, latency_ms=latency_ms,
            gate_that_rejected=None, escalated=escalate_next,
            model_cascade_position=model_cascade_position,
        )
        _fire_turn(rec)
        tracker.check_stop()
        break

    if patch_spec is None:
        logger.info(
            "── Heal complete: no patch (%d attempts, stop_reason=%s, %d tokens in, %d out) ──",
            tracker.current_attempt, tracker.stop_reason,
            tracker.tokens_in_total, tracker.tokens_out_total,
        )
        # Demoted to debug — the transcript's └─ close node already states the
        # outcome (✗ <reason> · N turn(s)); the caller prints the terminal line.
        logger.debug(
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
