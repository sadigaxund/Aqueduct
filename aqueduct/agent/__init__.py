"""LLM-driven patch loop for the Aqueduct self-healing engine.

On blueprint failure, packages a trimmed failure context as a prompt, calls the
configured LLM provider, validates the response as a PatchSpec, and either
applies it automatically or writes it to ``patches/pending/`` for human review.

Module layout (use the file name as your first search filter):
  prompts.py     — prompt templates + construction helpers (_build_user_prompt, _build_system_prompt)
  providers.py   — Anthropic + OpenAI-compatible HTTP dispatch (_call_anthropic, _call_openai_compat)
  parse.py       — PatchSpec parsing, reprompt formatting, structural error detection
  loop.py        — ``generate_agent_patch`` orchestration loop + patch I/O
  cascade.py     — multi-model healing cascade (generate_cascade_patch)
  budget.py      — multi-axis budget tracking (BudgetConfig, BudgetTracker)
  signature.py   — error signature engine (ErrorSignature, from_* helpers)
  memory.py      — Phase 45 signature memory (pending reuse, replay, coaching)

All providers use httpx — no optional SDK dependency required.

Supported providers:
  - "anthropic"     → Anthropic Messages API (ANTHROPIC_API_KEY env var)
  - "openai_compat" → any OpenAI-compatible endpoint (vLLM, LM Studio, Ollama, …)
"""

from __future__ import annotations

from typing import Any

from aqueduct.agent.budget import BudgetConfig as BudgetConfig
from aqueduct.agent.cascade import generate_cascade_patch as generate_cascade_patch
from aqueduct.agent.loop import (
    AgentPatchResult as AgentPatchResult,
    PROMPT_VERSION as PROMPT_VERSION,
    archive_patch as archive_patch,
    generate_agent_patch as generate_agent_patch,
    stage_patch_for_human as stage_patch_for_human,
)
from aqueduct.agent.prompts import build_prompt as build_prompt

# Backward-compatible alias for the legacy single-axis knob.
# External callers can pass max_reprompts=... or configure the full
# BudgetConfig via agent.budget: in aqueduct.yml.
MAX_REPROMPTS = 3


def resolve_budget(
    pydantic_budget: Any = None,
    *,
    max_reprompts: int | None = None,
) -> BudgetConfig:
    """Build a runtime ``BudgetConfig`` from the pydantic ``AgentBudgetConfig``.

    Lets the CLI keep using its existing ``max_reprompts`` resolution flow as
    the fallback while opting users into the full multi-axis budget when the
    ``agent.budget:`` block is present in ``aqueduct.yml``.
    """
    if pydantic_budget is not None:
        same_err = int(pydantic_budget.same_error_consecutive)
        same_sig = int(pydantic_budget.same_signature_overall)
        if same_sig < same_err and "same_signature_overall" not in getattr(pydantic_budget, "model_fields_set", set()):
            same_sig = same_err

        return BudgetConfig(
            max_reprompts=int(pydantic_budget.max_reprompts),
            max_seconds=float(pydantic_budget.max_seconds),
            max_tokens_total=pydantic_budget.max_tokens_total,
            same_error_consecutive=same_err,
            same_signature_overall=same_sig,
            progress_stalled_window=int(pydantic_budget.progress_stalled_window),
        )
    if max_reprompts is not None:
        return BudgetConfig(max_reprompts=max(1, int(max_reprompts)))
    return BudgetConfig()
