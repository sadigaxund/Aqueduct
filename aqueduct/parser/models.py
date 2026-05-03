"""Immutable AST dataclasses for the parsed Blueprint.

All types use @dataclass(frozen=True) — prevents accidental mutation in
downstream layers (Compiler, Planner, Executor). Use dataclasses.replace()
to produce modified copies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


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


@dataclass(frozen=True)
class AgentConfig:
    approval_mode: str = "disabled"       # "disabled" | "human" | "auto" | "aggressive"
    on_pending_patches: str = "warn"      # "ignore" | "warn" | "block"
    max_patches_per_run: int = 5
    # Connection fields — None = inherit from aqueduct.yml agent: defaults
    provider: str | None = None
    base_url: str | None = None
    model: str | None = None
    ollama_options: dict | None = None
    # Guardrail policy — deterministically enforced in apply_patch
    guardrails: GuardrailsConfig = field(default_factory=GuardrailsConfig)
    # Dry-run: pre-validate patched Blueprint before writing to disk (aggressive mode)
    validate_patch: bool = False
    # Extra context appended to LLM system prompt for this blueprint only (after engine-level prompt_context)
    prompt_context: str | None = None


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
