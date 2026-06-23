"""Immutable AST dataclasses for the parsed Blueprint.

All types use @dataclass(frozen=True) — prevents accidental mutation in
downstream layers (Compiler, Planner, Executor). Use dataclasses.replace()
to produce modified copies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class ModuleType(StrEnum):
    """Canonical module type names for the 9 blueprint module kinds."""
    Ingress = "Ingress"
    Channel = "Channel"
    Egress = "Egress"
    Junction = "Junction"
    Funnel = "Funnel"
    Probe = "Probe"
    Regulator = "Regulator"
    Arcade = "Arcade"
    Assert = "Assert"


@dataclass(frozen=True)
class ContextRegistry:
    """Fully-resolved Tier 0 context. Keys are dot-notation strings."""

    values: dict[str, Any]


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 1
    backoff_strategy: str = "exponential"
    backoff_base_seconds: int = 30
    backoff_max_seconds: int = 600
    jitter: bool = True
    on_exhaustion: str = "trigger_agent"
    transient_errors: tuple[Any, ...] = ()
    non_transient_errors: tuple[str, ...] = ()
    deadline_seconds: int | None = None  # give up after N seconds from first failure


@dataclass(frozen=True)
class GuardrailsConfig:
    forbidden_ops: tuple[str, ...] = ()   # PatchSpec op names blocked from auto-apply
    allowed_paths: tuple[str, ...] = ()   # fnmatch patterns for config path values; empty = unrestricted
    heal_on_errors: tuple[str, ...] = ()  # LLM only fires when error_type matches; empty = no restriction
    never_heal_errors: tuple[str, ...] = ()  # LLM never fires when error_type matches; takes priority


@dataclass(frozen=True)
class CascadeTierConfig:
    """Phase 44 — Per-tier config in a multi-model healing cascade."""
    model: str
    provider: str | None = None
    base_url: str | None = None
    api_key: str | None = None
    provider_options: dict | None = None
    timeout: float | None = None
    max_tokens: int | None = None
    max_reprompts: int | None = None
    max_seconds: float | None = None
    deep_loop: bool | None = None
    allow_defer: bool | None = None


@dataclass(frozen=True)
class AgentConfig:
    approval_mode: str = "disabled"       # YAML key `approval`: "disabled" | "human" | "auto" | "ci"
    on_pending_patches: str = "warn"      # "ignore" | "warn" | "block"
    # `max_patches` (default 1). Multi-patch loop opt-in: set > 1 AND
    # `danger.allow_multi_patch: true`.
    max_patches: int = 1
    # Connection fields — None = inherit from aqueduct.yml agent: defaults
    provider: str | None = None
    base_url: str | None = None
    api_key: str | None = None
    model: str | None = None
    provider_options: dict | None = None
    timeout: float | None = None
    max_reprompts: int | None = None
    # Guardrail policy — deterministically enforced in apply_patch
    guardrails: GuardrailsConfig = field(default_factory=GuardrailsConfig)
    # Minimum LLM confidence to auto-apply patch (below threshold → escalate to human)
    confidence_threshold: float = 0.7
    # What to do when patch is generated but fails to fix the pipeline: stage | discard | abort
    on_heal_failure: str = "stage"
    # Phase 41: allow the LLM to emit defer_to_human when the failure is not
    # healable at the Blueprint level. Default False — the LLM must always produce
    # a real patch unless explicitly permitted to defer.
    allow_defer: bool = False
    # Phase 43: run sandbox/lineage/explain gates inside the LLM conversation
    # so the model sees rejection feedback and retries in-context instead of
    # starting a fresh conversation each time. Default False preserves the
    # current behaviour (gates run post-hoc via apply_callback).
    deep_loop: bool = False
    # Phase 44: multi-model healing cascade
    cascade: tuple[CascadeTierConfig, ...] | None = None
    # Extra context appended to LLM system prompt for this blueprint only (after engine-level prompt_context)
    prompt_context: str | None = None
    # Spend-cap: max LLM healing attempts per rolling 60-minute window for this blueprint.
    # None = unlimited. When exceeded, Surveyor blocks the LLM call.
    max_heal_attempts_per_hour: int | None = None
    # "full_run" | "sandbox" | None (= inherit from engine default).
    # Controls whether `auto` mode validates a generated patch by a full
    # Spark run after the sandbox replay, or by sandbox replay alone.
    patch_validation: str | None = None
    # Opt-in stricter Gate 4 (explain regression) for `auto` multi-patch mode.
    # None = inherit from engine default (False).
    block_on_explain_regression: bool | None = None
    # 1.1.0 — sandbox replay fidelity: "sample" (default), "preflight"
    # (full dataset, requires danger.allow_full_preflight), "off" (skip,
    # requires danger.allow_skip_sandbox).
    sandbox_mode: str = "sample"

    def to_dict(self) -> dict[str, Any]:
        """Serialize agent policy fields for the manifest snapshot the LLM sees.

        Adding a new agent field here ensures it reaches the LLM; forgetting
        to add it here means the LLM won't see the field (silent omission).
        """
        return {
            "approval_mode": self.approval_mode,
            "model": self.model,
            "max_patches": self.max_patches,
            "provider": self.provider,
            "base_url": self.base_url,
            "prompt_context": self.prompt_context,
            "sandbox_mode": self.sandbox_mode,
            "allow_defer": self.allow_defer,
            "deep_loop": self.deep_loop,
            "confidence_threshold": self.confidence_threshold,
            "patch_validation": self.patch_validation,
            "block_on_explain_regression": self.block_on_explain_regression,
            "max_heal_attempts_per_hour": self.max_heal_attempts_per_hour,
            "guardrails": {
                "forbidden_ops": list(self.guardrails.forbidden_ops),
                "allowed_paths": list(self.guardrails.allowed_paths),
            },
        }


@dataclass(frozen=True)
class Module:
    id: str
    type: str
    label: str
    config: dict[str, Any]
    description: str = ""
    tags: tuple[str, ...] = ()
    spillway: str | None = None
    depends_on: tuple[str, ...] = ()
    on_failure: dict[str, Any] | None = None
    on_failure_webhook: str | dict[str, Any] | None = None
    checkpoint: bool = False
    # Probe-specific: module this Probe taps
    attach_to: str | None = None
    # Arcade-specific: sub-Blueprint path and context overrides
    ref: str | None = None
    context_override: dict[str, Any] | None = None


@dataclass(frozen=True)
class Edge:
    from_id: str
    to_id: str
    port: str = "main"
    error_types: tuple[str, ...] = ()
    # True when the compiler auto-generated this edge from linear-edge sugar
    # (Blueprint omitted `edges:` entirely). Provenance marker — distinguishes
    # user-declared wiring from compiler-injected decl-order chaining.
    injected: bool = False


@dataclass(frozen=True)
class Blueprint:
    aqueduct_version: str
    id: str
    name: str
    context: ContextRegistry
    modules: tuple[Module, ...]
    edges: tuple[Edge, ...]
    description: str = ""
    spark_config: dict[str, Any] = field(default_factory=dict)
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    agent: AgentConfig = field(default_factory=AgentConfig)
    udf_registry: tuple[dict[str, Any], ...] = ()
    macros: dict[str, str] = field(default_factory=dict)
    required_context: tuple[str, ...] = ()  # Arcade sub-Blueprint: keys the caller must provide
    checkpoint: bool = False
