"""Pydantic v2 schema models for Blueprint YAML validation.

Unknown fields at any level are hard errors (extra="forbid"), matching the
spec requirement that Blueprints are always valid input for LLM patch generation.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

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


class AgentSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    approval_mode: Literal["disabled", "human", "auto", "aggressive"] = "disabled"
    on_pending_patches: Literal["ignore", "warn", "block"] = "warn"
    max_patches_per_run: int = 5
    # Connection fields — None means "inherit from aqueduct.yml agent: defaults"
    provider: Literal["anthropic", "openai_compat"] | None = None
    base_url: str | None = None
    model: str | None = None
    ollama_options: dict[str, Any] | None = None
    # Guardrail policy — limits what the LLM can autonomously modify
    allowed_paths: list[str] = Field(default_factory=list)
    forbidden_ops: list[str] = Field(default_factory=list)
    # Dry-run: pre-validate patched Blueprint before writing to disk (aggressive mode)
    validate_patch: bool = False


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
