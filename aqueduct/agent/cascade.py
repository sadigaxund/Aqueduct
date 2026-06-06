"""Phase 44 — Multi-model healing cascade.

Try cheap models first, escalate to expensive ones only when cheap
models get stuck.  ~70% of heals are path typos / column renames a 7B
model handles in one shot — this cascade avoids spending Claude tokens
on problems a local model already solved.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable

from aqueduct.agent.budget import BudgetConfig
from aqueduct.agent.loop import AgentPatchResult, generate_agent_patch
from aqueduct.parser.models import CascadeTierConfig
from aqueduct.surveyor.models import FailureContext

logger = logging.getLogger("aqueduct.agent.cascade")

# Stop reasons that trigger escalation to the next tier.
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
    # ── Callbacks shared across all tiers ───────────────────────────
    apply_callback: Callable | None = None,
    validate_callback: Callable | None = None,
    on_attempt: Callable | None = None,
) -> AgentPatchResult:
    """Try each tier in order; escalate on stuck / exhausted / deferred.

    Returns the **last** ``AgentPatchResult`` — the caller inspects
    ``.patch`` and ``.stop_reason`` to decide what to do next.
    """
    n = len(tiers)
    result: AgentPatchResult | None = None

    for idx, tier in enumerate(tiers, start=1):
        logger.info("── Cascade tier %d/%d: %s ──", idx, n, tier.model)

        budget_cfg = BudgetConfig(
            max_reprompts=tier.max_reprompts if tier.max_reprompts is not None else max_reprompts,
            max_seconds=tier.max_seconds if tier.max_seconds is not None else 120.0,
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
            max_reprompts=tier.max_reprompts if tier.max_reprompts is not None else max_reprompts,
            engine_prompt_context=engine_prompt_context,
            blueprint_prompt_context=blueprint_prompt_context,
            guardrails=guardrails,
            budget=budget_cfg,
            allow_defer=tier.allow_defer if tier.allow_defer is not None else False,
            deep_loop=tier.deep_loop if tier.deep_loop is not None else False,
            apply_callback=apply_callback,
            validate_callback=validate_callback,
            on_attempt=on_attempt,
            model_cascade_position=idx - 1,
        )

        if result.patch is not None:
            logger.info("── Cascade complete: %s (tier %d/%d) ──",
                         result.patch.patch_id, idx, n)
            return result

        if result.stop_reason in _ESCALATION_REASONS:
            logger.info("⚠  %s — escalating to tier %d", result.stop_reason, idx + 1)
            continue

        # Hard failure (api_error, budget_seconds_exceeded, …) — don't escalate.
        logger.warning("── Cascade aborted: %s (tier %d/%d) ──",
                       result.stop_reason, idx, n)
        return result

    # All tiers exhausted without success.
    logger.info("── Cascade exhausted: %d tier(s), stop_reason=%s ──",
                n, result.stop_reason if result else "(no result)")
    return result  # type: ignore[return-value]
