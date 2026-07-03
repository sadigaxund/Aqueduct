"""Pydantic v2 schema models for Blueprint YAML validation.

Unknown fields at any level are hard errors (extra="forbid"), matching the
spec requirement that Blueprints are always valid input for LLM patch generation.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator

from aqueduct.parser.models import ModuleType

VALID_MODULE_TYPES: frozenset[str] = frozenset(m.value for m in ModuleType)

# Note: edge `port` is NOT constrained to a fixed set at schema level — Junction
# branch ids are valid dynamic port names, so membership is validated downstream
# (graph/compiler), not here.


class BackoffSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strategy: Literal["linear", "exponential", "fixed"] = "exponential"
    base_seconds: int = Field(default=30, ge=1, description="Base backoff delay in seconds (>= 1)")
    max_seconds: int = Field(default=600, ge=1, description="Maximum backoff delay in seconds (>= 1)")
    jitter: bool = True


class RetryPolicySchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_attempts: int = Field(default=1, ge=1, description="Maximum retry attempts (>= 1)")
    backoff: BackoffSchema = Field(default_factory=BackoffSchema)
    transient_errors: list[Any] = Field(default_factory=list)
    non_transient_errors: list[str] = Field(default_factory=list)
    on_exhaustion: Literal["trigger_agent", "abort", "alert_only"] = "trigger_agent"
    deadline_seconds: int | None = Field(default=None, gt=0, description="Retry deadline in seconds (> 0 if set)")


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


class CascadeTierSchema(BaseModel):
    """Phase 44 — Per-tier config in a multi-model healing cascade.

    Every field is optional. Missing fields inherit from the top-level
    ``agent.*`` defaults.
    """
    model_config = ConfigDict(extra="forbid")

    model: str
    provider: Literal["anthropic", "openai_compat"] | None = None
    base_url: str | None = None
    api_key: str | None = None
    provider_options: dict[str, Any] | None = None
    timeout: float | None = Field(default=None, gt=0)
    max_tokens: int | None = Field(default=None, ge=1)
    max_reprompts: int | None = Field(default=None, ge=1)
    max_seconds: int | None = Field(default=None, ge=1)
    deep_loop: bool | None = None
    allow_defer: bool | None = None


class AgentSchema(BaseModel):
    # No populate_by_name: the `approval_mode` attr is settable from YAML ONLY via
    # its `approval` alias — the former `approval_mode` YAML key is rejected (2.0).
    model_config = ConfigDict(extra="forbid")

    api_key: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _model_list_sugar(cls, data: Any) -> Any:
        """Phase 46 — ``agent.model: [cheap, expensive]`` is cascade shorthand.

        A list of model names expands to ``cascade:`` tiers with all-default
        tier settings (provider/base_url/budget inherit normally). Mutually
        exclusive with an explicit ``cascade:`` block. A single-item list
        collapses to a plain ``model:`` string (no cascade overhead).
        """
        if isinstance(data, dict) and isinstance(data.get("model"), list):
            models = data["model"]
            if not models or not all(isinstance(m, str) and m.strip() for m in models):
                raise ValueError("agent.model list must contain non-empty model name strings")
            if data.get("cascade"):
                raise ValueError(
                    "agent.model as a list and agent.cascade are mutually exclusive — "
                    "use the list shorthand OR the explicit cascade block, not both"
                )
            data = dict(data)
            data["model"] = models[0]
            if len(models) > 1:
                data["cascade"] = [{"model": m} for m in models]
        return data

    # `approval` is the YAML key; the Python attribute name stays `approval_mode`
    # (internal — keyed in via the alias).
    approval_mode: Literal["disabled", "human", "auto", "ci"] = Field(
        default="disabled",
        validation_alias=AliasChoices("approval"),
    )
    on_pending_patches: Literal["ignore", "warn", "block"] = "warn"
    # `max_patches` (default 1 = single-patch try). Set > 1 to opt into the
    # multi-patch reprompt loop; that path also requires
    # `danger.allow_multi_patch: true`.
    max_patches: int = Field(default=1, ge=1)
    # Connection fields — None means "inherit from aqueduct.yml agent: defaults"
    provider: Literal["anthropic", "openai_compat"] | None = None
    base_url: str | None = None
    model: str | None = None
    provider_options: dict[str, Any] | None = None
    timeout: float | None = Field(default=None, gt=0)
    max_reprompts: int | None = Field(default=None, ge=1)
    # Guardrail policy — deterministically enforced in apply_patch
    guardrails: GuardrailsSchema = Field(default_factory=GuardrailsSchema)
    # Minimum LLM confidence to auto-apply patch (below threshold → escalate to human)
    confidence_threshold: float = Field(default=0.7, ge=0, le=1)
    # What to do when a patch is generated but fails to fix the pipeline
    on_heal_failure: Literal["stage", "discard", "abort"] = "stage"
    # Phase 41: allow the LLM to emit defer_to_human when the failure is not
    # healable at the Blueprint level. Default False — the LLM must always produce
    # a real patch unless explicitly permitted to defer.
    allow_defer: bool = Field(default=False)
    # Phase 43: run sandbox/lineage/explain gates inside the LLM conversation
    # so the model sees rejection feedback and retries in-context. Default False
    # preserves the current behaviour (gates run post-hoc via apply_callback).
    deep_loop: bool = Field(default=False)
    # Phase 44: multi-model healing cascade — each tier tries a different model,
    # escalating on stuck_signature / exhausted_attempts / deferred.
    cascade: list[CascadeTierSchema] | None = None
    # Extra context appended to LLM system prompt for this blueprint (after engine-level prompt_context)
    prompt_context: str | None = None
    # Spend-cap: max successful LLM healing attempts per rolling 60-minute window for this blueprint.
    # None (default) = unlimited. When exceeded, Surveyor records skip outcome and run ends.
    max_heal_attempts_per_hour: int | None = Field(default=None, ge=1)
    # Patch validation pyramid: default `full_run` keeps existing
    # behaviour: a generated patch is sandbox-checked AND then validated by a
    # full Spark run before the Blueprint is written to disk. `sandbox` skips
    # the full-run step — fastest, lowest confidence, lets multi-patch mode
    # (`auto` + `max_patches > 1`) close patch loops in seconds rather than minutes.
    patch_validation: Literal["full_run", "sandbox"] | None = None
    # When True (`auto` multi-patch mode only), the explain gate (post-patch
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
    # Conditional execution — bool, or an unresolved "${ctx.*}" string at
    # validation time (Tier 0 resolution runs after schema validation); the
    # parser coerces true/false/1/0/yes/no/on/off. A disabled module compiles
    # but is skipped (⏭) at run time, and the disable cascades to every module
    # consuming its output (edges, depends_on, Probe attach_to).
    enabled: bool | str = True
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
    """A UDF registry entry. Matches the executor's registration contract:
    python UDFs import ``module``/``entry``; java/scala UDFs load ``jar``/``class``.
    """
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    id: str
    lang: Literal["python", "java", "scala"] = "python"
    return_type: str | None = None      # executor defaults to "string" when absent
    label: str | None = None
    deterministic: bool = True
    # python: import `entry` (defaults to `id`) from `module`
    module: str | None = None
    entry: str | None = None
    # Optional keyword params. When present on a python UDF, `entry` is treated
    # as a FACTORY — `make_x(**params) -> callable` — so one importable function
    # is reused across blueprints/environments with different settings. Values
    # support ${ctx.*}/${ENV} (Tier 0) and @aq.* incl. @aq.secret() (Tier 1).
    params: dict[str, Any] = Field(default_factory=dict)
    # java / scala: load `class` from `jar` (or `path`)
    jar: str | None = None
    path: str | None = None
    class_name: str | None = Field(default=None, alias="class")

    @field_validator("id")
    @classmethod
    def validate_udf_id(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("UDF id must not be empty")
        return v


class WarningsSchema(BaseModel):
    """Per-Blueprint compile-warning suppression (see `docs/specs.md` §4.2).

    Mirrors the shape of the engine-level ``warnings:`` block in
    ``aqueduct.yml`` (`aqueduct/config.py::WarningsConfig`) but is a distinct
    model — the parser layer does not import `config.py`'s engine models.
    Scope is intentionally narrow: this only affects the compiler's modular
    `aqueduct/compiler/warnings/run_all` rule pass for THIS Blueprint. It does
    NOT touch engine/session warnings, runtime (probe/assert) warnings, or the
    process-global `set_default_suppress` default — those stay engine-wide.
    """
    model_config = ConfigDict(frozen=True, extra="forbid")

    suppress: list[str] = Field(
        default_factory=list,
        description=(
            "List of compile-warning `rule_id` strings to silence for this "
            "Blueprint only. Merged (union) with the engine-level "
            "`warnings.suppress` from aqueduct.yml / `--suppress-warning`. "
            "The single entry `\"*\"` silences every compile warning for "
            "this Blueprint."
        ),
    )


class HookEntrySchema(BaseModel):
    """One lifecycle-hook action — exactly one of `blueprint:` / `webhook:` /
    `command:` (see `docs/specs.md` §Hooks).

    - `blueprint:` chains another Blueprint as a fresh `aqueduct run`
      subprocess (own session/run_id/report — loose coupling by design).
    - `webhook:` fires the same endpoint model as the engine-level
      `webhooks:` block in aqueduct.yml — a bare URL string, or a map with
      url/method/headers/payload (full payload templating included).
    - `command:` runs an arbitrary subprocess (shlex argv, NO shell) —
      requires `danger.allow_command_hooks: true` in aqueduct.yml; entries
      are skipped with a warning otherwise.
    """
    model_config = ConfigDict(extra="forbid")

    blueprint: str | None = None
    webhook: str | dict[str, Any] | None = None
    command: str | None = None
    timeout: int = 300  # seconds — blueprint/command subprocesses

    @model_validator(mode="after")
    def _exactly_one_action(self) -> HookEntrySchema:
        set_keys = [k for k in ("blueprint", "webhook", "command") if getattr(self, k) is not None]
        if len(set_keys) != 1:
            raise ValueError(
                "hook entry must set exactly one of blueprint/webhook/command"
                f" — got {set_keys or 'none'}"
            )
        return self


class HooksSchema(BaseModel):
    """Blueprint lifecycle hooks — run AFTER the pipeline's terminal state.

    Hooks never change the run's exit code; a failing hook is a warning and
    stops the remaining hooks of that event. Distinct from the engine-level
    `webhooks:` block in aqueduct.yml (ops-owned alerting that fires
    regardless of what the Blueprint declares).
    """
    model_config = ConfigDict(extra="forbid")

    on_success: list[HookEntrySchema] = Field(default_factory=list)
    on_failure: list[HookEntrySchema] = Field(default_factory=list)


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
    udf_registry: list[UdfSchema] = Field(default_factory=list)
    macros: dict[str, str] = Field(default_factory=dict)
    required_context: list[str] = Field(default_factory=list)  # Arcade sub-Blueprint
    checkpoint: bool = False
    warnings: WarningsSchema = Field(default_factory=WarningsSchema)
    hooks: HooksSchema = Field(default_factory=HooksSchema)

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
    def validate_unique_module_ids(self) -> BlueprintSchema:
        ids = [m.id for m in self.modules]
        seen: set[str] = set()
        dupes = {mid for mid in ids if mid in seen or seen.add(mid)}  # type: ignore[func-returns-value]
        if dupes:
            raise ValueError(f"Duplicate module IDs: {sorted(dupes)}")
        return self
