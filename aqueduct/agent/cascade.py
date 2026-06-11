"""Phase 44 — Multi-model healing cascade.

Try cheap models first, escalate to expensive ones only when cheap
models get stuck.  ~70% of heals are path typos / column renames a 7B
model handles in one shot — this cascade avoids spending Claude tokens
on problems a local model already solved.
"""

from __future__ import annotations

import dataclasses
import logging
from pathlib import Path
from typing import Any, Callable

from aqueduct.agent.budget import BudgetConfig
from aqueduct.agent.loop import AgentPatchResult, generate_agent_patch
from aqueduct.parser.models import CascadeTierConfig
from aqueduct.surveyor.models import FailureContext

logger = logging.getLogger("aqueduct.agent.cascade")

# Stop reasons that trigger escalation to the next tier. ``deferred`` is
# checked BEFORE the patch-presence check in the loop body — a defer result
# carries a non-None patch (the diagnosis), but a cheaper tier saying
# "I can't fix this" should escalate, not end the cascade.
_ESCALATION_REASONS: frozenset[str] = frozenset(
    {"stuck_signature", "exhausted_attempts", "deferred"}
)


def generate_cascade_patch(
    tiers: list[CascadeTierConfig],
    failure_ctx: FailureContext,
    patches_dir: Path,
    *,
    # ── Defaults shared across all tiers (overridden per-tier) ──────
    provider: str = "anthropic",
    base_url: str | None = None,
    provider_options: dict[str, Any] | None = None,
    timeout: float = 120.0,
    max_tokens: int = 4096,
    max_reprompts: int = 3,
    engine_prompt_context: str | None = None,
    blueprint_prompt_context: str | None = None,
    guardrails: Any = None,
    budget: BudgetConfig | None = None,
    allow_defer: bool = False,
    deep_loop: bool = False,
    last_apply_error: str | None = None,
    memory_coaching: bool = True,
    # ── Callbacks shared across all tiers ───────────────────────────
    apply_callback: Callable | None = None,
    validate_callback: Callable | None = None,
    on_attempt: Callable | None = None,
) -> AgentPatchResult:
    """Try each tier in order; escalate on stuck / exhausted / deferred.

    ``budget``, ``allow_defer``, and ``deep_loop`` are the top-level
    ``agent.*`` defaults; per-tier fields override them, missing tier fields
    inherit. A tier's budget is the top-level budget with ``max_reprompts``
    / ``max_seconds`` swapped for the tier's own values when set — the
    remaining axes (token cap, stuck/stall detection) carry over unchanged.

    Returns the **last** ``AgentPatchResult`` — the caller inspects
    ``.patch`` and ``.stop_reason`` to decide what to do next. A mid-cascade
    defer is treated as escalation (its diagnosis patch is discarded); a
    defer on the FINAL tier is returned to the caller for staging.
    """
    n = len(tiers)
    result: AgentPatchResult | None = None
    base_budget = budget if budget is not None else BudgetConfig(
        max_reprompts=max(1, max_reprompts),
    )

    for idx, tier in enumerate(tiers, start=1):
        logger.info("── Cascade tier %d/%d: %s ──", idx, n, tier.model)

        budget_cfg = dataclasses.replace(
            base_budget,
            max_reprompts=tier.max_reprompts if tier.max_reprompts is not None else base_budget.max_reprompts,
            max_seconds=float(tier.max_seconds) if tier.max_seconds is not None else base_budget.max_seconds,
        )

        result = generate_agent_patch(
            failure_ctx=failure_ctx,
            model=tier.model,
            patches_dir=patches_dir,
            provider=tier.provider if tier.provider is not None else provider,
            base_url=tier.base_url if tier.base_url is not None else base_url,
            provider_options=tier.provider_options if tier.provider_options is not None else provider_options,
            timeout=tier.timeout if tier.timeout is not None else timeout,
            max_tokens=tier.max_tokens if tier.max_tokens is not None else max_tokens,
            max_reprompts=budget_cfg.max_reprompts,
            engine_prompt_context=engine_prompt_context,
            blueprint_prompt_context=blueprint_prompt_context,
            last_apply_error=last_apply_error,
            guardrails=guardrails,
            budget=budget_cfg,
            allow_defer=tier.allow_defer if tier.allow_defer is not None else allow_defer,
            deep_loop=tier.deep_loop if tier.deep_loop is not None else deep_loop,
            apply_callback=apply_callback,
            validate_callback=validate_callback,
            on_attempt=on_attempt,
            model_cascade_position=idx - 1,
            memory_coaching=memory_coaching,
        )

        # Escalation check runs BEFORE the patch check: a defer result carries
        # a non-None patch (the diagnosis), and on any non-final tier that
        # means "this model gave up" — try the next tier.
        if result.stop_reason in _ESCALATION_REASONS and idx < n:
            logger.info("⚠  %s — escalating to tier %d", result.stop_reason, idx + 1)
            continue

        if result.patch is not None:
            logger.info("── Cascade complete: %s (tier %d/%d, stop_reason=%s) ──",
                         result.patch.patch_id, idx, n, result.stop_reason)
            return result

        if result.stop_reason in _ESCALATION_REASONS:
            continue  # final tier — fall through to the exhausted log below

        # Hard failure (api_error, budget_seconds_exceeded, …) — don't escalate.
        logger.warning("── Cascade aborted: %s (tier %d/%d) ──",
                       result.stop_reason, idx, n)
        return result

    # All tiers exhausted without success.
    logger.info("── Cascade exhausted: %d tier(s), stop_reason=%s ──",
                n, result.stop_reason if result else "(no result)")
    return result  # type: ignore[return-value]
