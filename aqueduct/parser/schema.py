"""Pydantic v2 schema models for Blueprint YAML validation.

Unknown fields at any level are hard errors (extra="forbid"), matching the
spec requirement that Blueprints are always valid input for LLM patch generation.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator

VALID_MODULE_TYPES = frozenset(
    {"Ingress", "Channel", "Egress", "Junction", "Funnel", "Probe", "Regulator", "Arcade", "Assert"}
)

VALID_PORTS = frozenset({"main", "spillway", "signal"})

# Ports that carry DataFrames (vs control-only ports)
CONTROL_PORTS = frozenset({"signal", "spillway"})


class BackoffSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strategy: Literal["linear", "exponential", "fixed"] = "exponential"
    base_seconds: int = 30
    max_seconds: int = 600
    jitter: bool = True


class RetryPolicySchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_attempts: int = 1
    backoff: BackoffSchema = Field(default_factory=BackoffSchema)
    transient_errors: list[Any] = Field(default_factory=list)
    non_transient_errors: list[str] = Field(default_factory=list)
    on_exhaustion: Literal["trigger_agent", "abort", "alert_only"] = "trigger_agent"
    deadline_seconds: int | None = None


class GuardrailsSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # PatchSpec op names that are blocked from auto-apply (deterministic enforcement in apply_patch)
    forbidden_ops: list[str] = Field(default_factory=list)
    # fnmatch patterns for config `path` values LLM may write; empty = unrestricted
    allowed_paths: list[str] = Field(default_factory=list)
    # Pre-trigger guards: LLM only fires when error_type matches (empty = no restriction)
    heal_on_errors: list[str] = Field(default_factory=list)
    # Pre-trigger guards: LLM never fires when error_type matches (takes priority over heal_on_errors)
    never_heal_errors: list[str] = Field(default_factory=list)


class AgentSchema(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    approval_mode: Literal["disabled", "human", "auto", "aggressive", "ci"] = "disabled"
    on_pending_patches: Literal["ignore", "warn", "block"] = "warn"
    # 1.1.0 — `max_patches` is the canonical name. `aggressive_max_patches` is
    # accepted as a deprecated alias and emits a warning at parse time.
    # Default 1 = single-patch try (former `auto` mode behavior). Set > 1 to
    # opt into the multi-patch reprompt loop; that path also requires
    # `danger.allow_multi_patch: true` (alias: `allow_aggressive_patching`).
    max_patches: int = Field(default=1, validation_alias=AliasChoices("max_patches", "aggressive_max_patches"))
    # Connection fields — None means "inherit from aqueduct.yml agent: defaults"
    provider: Literal["anthropic", "openai_compat"] | None = None
    base_url: str | None = None
    model: str | None = None
    provider_options: dict[str, Any] | None = None
    timeout: float | None = None
    max_reprompts: int | None = None
    # Guardrail policy — deterministically enforced in apply_patch
    guardrails: GuardrailsSchema = Field(default_factory=GuardrailsSchema)
    # Minimum LLM confidence to auto-apply patch (below threshold → escalate to human)
    confidence_threshold: float = 0.7
    # What to do when a patch is generated but fails to fix the pipeline
    on_heal_failure: Literal["stage", "discard", "abort"] = "stage"
    # Extra context appended to LLM system prompt for this blueprint (after engine-level prompt_context)
    prompt_context: str | None = None
    # Spend-cap: max successful LLM healing attempts per rolling 60-minute window for this blueprint.
    # None (default) = unlimited. When exceeded, Surveyor records skip outcome and run ends.
    max_heal_attempts_per_hour: int | None = None
    # Phase 29a — patch validation pyramid. Default `full_run` keeps existing
    # behaviour: a generated patch is sandbox-checked AND then validated by a
    # full Spark run before the Blueprint is written to disk. `sandbox` skips
    # the full-run step — fastest, lowest confidence, lets aggressive mode
    # close patch loops in seconds rather than minutes.
    patch_validation: Literal["full_run", "sandbox"] | None = None
    # Phase 29b — when True (aggressive mode only), the explain gate (post-patch
    # `explain()` regression check) is treated as blocking. Default None
    # inherits engine `agent.block_on_explain_regression` (= False).
    block_on_explain_regression: bool | None = None
    # 1.1.0 — sandbox replay fidelity vs speed trade-off.
    #   sample    : default. Replay every generated patch on the first
    #               1000 rows of each Ingress (no Egress writes). Fast, low
    #               confidence — sufficient when blueprint is small.
    #   preflight : replay on the FULL dataset (no Egress writes). Slow but
    #               conclusive. Requires danger.allow_full_preflight: true.
    #   off       : skip sandbox replay entirely. Patch goes LLM → guardrail
    #               check → apply → next execute() on real data. Most
    #               dangerous; requires danger.allow_skip_sandbox: true.
    sandbox_mode: Literal["sample", "preflight", "off"] = "sample"


class ModuleSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    label: str
    type: str
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    config: dict[str, Any] = Field(default_factory=dict)
    on_failure: dict[str, Any] | None = None
    on_failure_webhook: str | dict[str, Any] | None = None
    spillway: str | None = None
    depends_on: list[str] = Field(default_factory=list)
    checkpoint: bool = False
    # Probe-specific
    attach_to: str | None = None
    # Arcade-specific
    ref: str | None = None
    context_override: dict[str, Any] | None = None

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Module id must not be empty")
        if "__" in v:
            raise ValueError(
                f"Module ID {v!r} contains '__' which is reserved for Arcade expansion. "
                "Use a single underscore or hyphen instead."
            )
        return v

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        if v not in VALID_MODULE_TYPES:
            raise ValueError(
                f"Unknown module type: {v!r}. Must be one of {sorted(VALID_MODULE_TYPES)}"
            )
        return v


class EdgeSchema(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    from_id: str = Field(alias="from")
    to: str
    port: str = "main"
    error_types: list[str] = Field(default_factory=list)

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Port name must not be empty")
        return v


class UdfSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    label: str
    lang: Literal["python", "scala", "java"] = "python"
    return_type: str
    deterministic: bool = True
    path: str | None = None
    entry: str | None = None
    jar: str | None = None
    class_name: str | None = Field(default=None, alias="class")

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class BlueprintSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    aqueduct: str
    id: str
    name: str
    description: str = ""
    context: dict[str, Any] = Field(default_factory=dict)
    context_profiles: dict[str, dict[str, Any]] = Field(default_factory=dict)
    modules: list[ModuleSchema]
    edges: list[EdgeSchema] = Field(default_factory=list)
    spark_config: dict[str, Any] = Field(default_factory=dict)
    retry_policy: RetryPolicySchema = Field(default_factory=RetryPolicySchema)
    agent: AgentSchema = Field(default_factory=AgentSchema)
    udf_registry: list[dict[str, Any]] = Field(default_factory=list)
    macros: dict[str, str] = Field(default_factory=dict)
    required_context: list[str] = Field(default_factory=list)  # Arcade sub-Blueprint
    checkpoint: bool = False

    @field_validator("aqueduct")
    @classmethod
    def validate_version(cls, v: str) -> str:
        supported = {"1.0"}
        if v not in supported:
            raise ValueError(f"Unsupported aqueduct version: {v!r}. Supported: {supported}")
        return v

    @field_validator("id")
    @classmethod
    def validate_blueprint_id(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Blueprint id must not be empty")
        return v

    @model_validator(mode="after")
    def validate_unique_module_ids(self) -> "BlueprintSchema":
        ids = [m.id for m in self.modules]
        seen: set[str] = set()
        dupes = {mid for mid in ids if mid in seen or seen.add(mid)}  # type: ignore[func-returns-value]
        if dupes:
            raise ValueError(f"Duplicate module IDs: {sorted(dupes)}")
        return self
