"""Pydantic v2 schema models for Blueprint YAML validation.

Unknown fields at any level are hard errors (extra="forbid"), matching the
spec requirement that Blueprints are always valid input for LLM patch generation.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator

from aqueduct.parser.fs_path import FsPath
from aqueduct.parser.models import ModuleType

# `Handoff` is a compiler-synthesized module kind (Phase 81 step 2) — the
# compiler inserts one at each cross-engine island boundary
# (`aqueduct.compiler.handoff`); it is never legal in authored Blueprint
# YAML. Excluded from the user-facing legal type set here; `validate_type`
# below gives it a dedicated, clearer rejection message than the generic
# "unknown module type" one.
_COMPILER_SYNTHESIZED_TYPES: frozenset[str] = frozenset({ModuleType.Handoff})
VALID_MODULE_TYPES: frozenset[str] = (
    frozenset(m.value for m in ModuleType) - _COMPILER_SYNTHESIZED_TYPES
)

# Note: edge `port` is NOT constrained to a fixed set at schema level — Junction
# branch ids are valid dynamic port names, so membership is validated downstream
# (graph/compiler), not here.


class BackoffSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strategy: Literal["linear", "exponential", "fixed"] = "exponential"
    base_seconds: int = Field(default=30, ge=1, description="Base backoff delay in seconds (>= 1)")
    max_seconds: int = Field(
        default=600, ge=1, description="Maximum backoff delay in seconds (>= 1)"
    )
    jitter: bool = True


class RetryPolicySchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_attempts: int = Field(default=1, ge=1, description="Maximum retry attempts (>= 1)")
    backoff: BackoffSchema = Field(default_factory=BackoffSchema)
    transient_errors: list[Any] = Field(default_factory=list)
    non_transient_errors: list[str] = Field(default_factory=list)
    on_exhaustion: Literal["trigger_agent", "abort", "alert_only"] = "trigger_agent"
    deadline_seconds: int | None = Field(
        default=None, gt=0, description="Retry deadline in seconds (> 0 if set)"
    )


class ModuleRetrySchema(BaseModel):
    """Per-module retry policy override (module-level `retry:` block).

    Every field defaults to ``None`` — a field left unset INHERITS the
    blueprint-level ``retry_policy:`` value for that field. Same per-field
    inheritance shape as agent cascade tiers (``aqueduct/agent/cascade.py``):
    override on the module, inherit from the blueprint when unset.
    ``backoff`` overrides as a whole block (not merged field-by-field) —
    set it entirely or omit it entirely.
    """

    model_config = ConfigDict(extra="forbid")

    max_attempts: int | None = Field(
        default=None,
        ge=1,
        description="Maximum retry attempts (>= 1); inherits blueprint retry_policy when unset",
    )
    backoff: BackoffSchema | None = Field(
        default=None,
        description="Whole-block override; inherits blueprint retry_policy.backoff when unset",
    )
    transient_errors: list[Any] | None = Field(
        default=None, description="Inherits blueprint retry_policy.transient_errors when unset"
    )
    non_transient_errors: list[str] | None = Field(
        default=None, description="Inherits blueprint retry_policy.non_transient_errors when unset"
    )
    on_exhaustion: Literal["trigger_agent", "abort", "alert_only"] | None = Field(
        default=None, description="Inherits blueprint retry_policy.on_exhaustion when unset"
    )
    deadline_seconds: int | None = Field(
        default=None,
        gt=0,
        description="Inherits blueprint retry_policy.deadline_seconds when unset (no module-level way to explicitly clear an inherited deadline)",
    )


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
    # Phase 75 — per-tier tool-use capability override. None inherits the
    # top-level `agent.supports_tools` (default "auto"). "auto" resolves True
    # for `anthropic` without probing; `openai_compat` probes on first call.
    supports_tools: Literal["auto", True, False] | None = None


class AgentPolicySchema(BaseModel):
    """Self-healing POLICY fields shared, by NAME, between the Blueprint's
    ``agent:`` block (``AgentSchema`` below) and the engine-level
    ``aqueduct.yml`` ``agent:`` block (``AgentConnectionConfig`` in
    ``aqueduct/config.py``, which extends this class).

    A field belongs here when it is a RISK DECISION about how aggressively /
    how far to trust a healing attempt (retry budget, patch-validation
    rigor, tool-use limits, chain length) — never an ENDPOINT FACT (which
    provider/URL/credential/model to call). Endpoint facts live only on
    ``AgentConnectionConfig``'s own fields; see that class's docstring and
    ``AgentSchema``'s docstring for the split rationale.

    Every field defaults to ``None`` here — the Blueprint-appropriate
    "unset = inherit the engine-level default" shape. A concrete engine-wide
    default belongs on ``AgentConnectionConfig``, which overrides each field
    it needs a non-``None`` default for. Declaring a policy field ONCE here
    guarantees it is at least NAMEABLE on both ``AgentSchema`` and
    ``AgentConnectionConfig`` (`tests/test_parser/test_agent_policy_split.py`
    asserts this structurally: this class's field set must be a subset of
    both), so a future policy field can't be added to only one side by
    oversight — pydantic subclassing lets each side override the type/
    default it actually needs while keeping the field NAME in lock-step.
    """

    model_config = ConfigDict(extra="forbid")

    max_reprompts: int | None = Field(default=None, ge=1)
    prompt_context: str | None = None
    max_heal_attempts_per_hour: int | None = Field(default=None, ge=1)
    patch_validation: Literal["full_run", "sandbox"] | None = None
    block_on_explain_regression: bool | None = None
    regression_artifact: bool | None = None
    mode: Literal["oneshot", "agentic"] | None = None
    max_tool_calls: int | None = Field(default=None, ge=1)
    supports_tools: Literal["auto", True, False] | None = None
    progressive: bool | None = None
    max_chain: int | None = Field(default=None, ge=1)


class AgentSchema(AgentPolicySchema):
    """Blueprint-level self-healing POLICY block — risk decisions about THIS
    pipeline (approval mode, guardrails, budget/reprompt limits, sandbox
    mode, confidence threshold, ...).

    CONNECTION fields — ``provider``, ``base_url``, ``api_key``, ``model``,
    ``provider_options``, ``timeout``, ``cascade`` — are deliberately NOT
    legal here. They describe WHICH LLM ENDPOINT to call, an
    operator/deployment fact, not a per-pipeline risk decision, and live
    exclusively on ``AgentConnectionConfig`` (``aqueduct/config.py``,
    ``aqueduct.yml``'s ``agent:`` block). This is the same split already
    applied to ``engine:``: ``SparkEngineBlockSchema`` omits ``master_url``
    and ``DuckDBEngineBlockSchema`` omits ``database_path``/``s3_*`` for the
    identical reason (see their docstrings) — a Blueprint does not get to
    decide deployment/connection concerns. Here the stakes are sharper: the
    healing loop ships ``FailureContext`` (pruned manifest, provenance,
    error text, and — in agentic mode — sampled data rows) to whatever
    endpoint is configured, so a Blueprint that could choose its own
    ``base_url`` could redirect production data to an arbitrary host on any
    failure. ``extra="forbid"`` (inherited from ``AgentPolicySchema``) makes
    a connection field written into a Blueprint's ``agent:`` block a named
    pydantic rejection — never a silent inherit-and-ignore.

    Adds no CONNECTION fields — every legal key is declared on
    ``AgentPolicySchema`` above or directly below (the fields with no
    engine-level equivalent: ``approval``, ``on_pending_patches``,
    ``max_patches``, ``guardrails``, ``confidence_threshold``,
    ``on_heal_failure``, ``allow_defer``, ``deep_loop``, ``sandbox_mode``).
    Kept as its own (non-empty) class, rather than an alias for
    ``AgentPolicySchema``, so the name stays stable for patch paths and
    tests that already reference ``aqueduct.parser.schema.AgentSchema``.
    """

    # No populate_by_name: the `approval_mode` attr is settable from YAML ONLY via
    # its `approval` alias — the former `approval_mode` YAML key is rejected (2.0).
    model_config = ConfigDict(extra="forbid")

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


# ═══════════════════════════════════════════════════════════════════════════
# Per-module-type schemas (Pass C, 2026-08 — discriminated union on `type`)
#
# Pre-Pass-C, every one of the 9 module types shared ONE flat `ModuleSchema`
# with a freeform `config: dict[str, Any]`. Two consequences: (a) a
# type-specific field (`materialize`, `attach_to`, ...) had no validator
# tying it to the type it actually applies to; (b) anything typed inside
# `config:` was INVISIBLE to the capability framework by construction —
# leaves are derived by introspecting pydantic models
# (`executor/capability_leaves.py`), so a freeform dict key can never
# become a leaf, and an engine that silently ignores it never has to say so.
# `materialize`/`watermark_column` (2.40) were the first two keys promoted
# out of `config:` for exactly this reason; Pass C generalises the fix: a
# discriminated union of per-type schemas, each declaring only the fields
# legal for THAT type (`extra="forbid"` then makes a misplaced or
# misspelled key a structural rejection naming it, no validator to keep in
# sync), with a TYPED `config:` sub-model per type absorbing that type's
# semantic keys.
#
# `config:` remains a nested YAML block (unlike `materialize`/`attach_to`/
# `ref`, which are genuinely module-top-level concepts) — only its
# freeform-dict SHAPE is gone. The one deliberate exception is `options:`
# (Ingress/Egress) and a couple of genuinely polymorphic fields
# (`schema_hint`, Channel `columns`) — these stay typed as a narrow
# container (`dict`/`Any`) with the polymorphism documented in the field's
# description, per the mandatory design constraint: type the SEMANTIC keys,
# never try to enumerate an engine's open-ended reader/writer option space.
#
# `MODULE_TYPE_SCHEMAS` (bottom of this section) is the single source of
# truth the capability-leaf walker (`executor/capability_leaves.py`) uses to
# derive one `<type_lower>.field.<name>` leaf per type-specific field —
# fields common to every type (below, `ModuleCommonSchema`) keep the pre-Pass-C
# `module.field.<name>` leaf id unchanged; only type-specific fields (the
# already-promoted ones AND every newly-typed former-`config:` key) get the
# new per-type prefix. See that module's docstring for the full scheme.
# ═══════════════════════════════════════════════════════════════════════════


class ModuleCommonSchema(BaseModel):
    """Fields legal on every one of the 9 module types.

    Not itself part of the discriminated union — each concrete
    ``<Type>Schema`` below inherits from this and adds its own ``type``
    discriminator, type-specific top-level fields, and typed ``config:``.
    Kept separate so these ~11 fields keep their pre-Pass-C
    ``module.field.<name>`` capability-leaf ids (they didn't move; only the
    type-specific surface did).
    """

    model_config = ConfigDict(extra="forbid")

    id: str
    label: str
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    # Cross-engine handoff (2.34) — which execution engine runs THIS module.
    # A scalar engine NAME (e.g. "spark", "duckdb"), distinct from the
    # blueprint-level `engine:` BLOCK (EngineBlockSchema, per-engine SETTINGS
    # namespaced by engine name, 2.0). Same word, two levels, deliberately —
    # see EngineBlockSchema's docstring. None (the default) means "inherit
    # from an upstream module, or from `deployment.engine` if this module has
    # no upstream" — resolved by the compiler (`aqueduct/compiler/islands.py`),
    # never validated against a fixed literal set here (only the compiler
    # knows which engines are registered). A Blueprint with no `engine:`
    # field anywhere is fully portable across every registered engine; a
    # pinned `engine:` is a declared engine dependency for that module's
    # island, enforced by the per-island capability gate.
    engine: str | None = None
    on_failure: dict[str, Any] | None = None
    on_failure_webhook: str | dict[str, Any] | None = None
    # Per-module retry policy override — inherits blueprint-level retry_policy
    # for any field left unset. Distinct from `on_failure` (an LLM-patch write
    # target for a full-replacement RetryPolicy; see set_module_on_failure /
    # replace_retry_policy patch ops). `retry:` is the Blueprint-authoring-time
    # surface; when both are present at runtime, `on_failure` takes precedence
    # (heal-time override over authoring-time override).
    retry: ModuleRetrySchema | None = None
    spillway: str | None = None
    depends_on: list[str] = Field(default_factory=list)
    checkpoint: bool = False
    # Conditional execution — bool, or an unresolved "${ctx.*}" string at
    # validation time (Tier 0 resolution runs after schema validation); the
    # parser coerces true/false/1/0/yes/no/on/off. A disabled module compiles
    # but is skipped (⏭) at run time, and the disable cascades to every module
    # consuming its output (edges, depends_on, Probe attach_to).
    enabled: bool | str = True

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


# ── Nested / list-item blocks (own capability-leaf prefix each) ────────────


class IngressTimeTravelSchema(BaseModel):
    """Ingress ``time_travel:`` — pin a Delta/Iceberg historical snapshot.
    Exactly one of ``version``/``timestamp`` (enforced at read time)."""

    model_config = ConfigDict(extra="forbid")

    version: int | None = None
    timestamp: str | None = None


class EgressMaintenanceSchema(BaseModel):
    """Egress ``maintenance:`` — post-write compaction/cleanup ops.
    Field applicability is format-specific (see ``spark/egress.py::build_maintenance_ops``):
    delta uses optimize/zorder_by/vacuum; iceberg uses rewrite_data_files/
    expire_snapshots; hudi uses compaction/clean."""

    model_config = ConfigDict(extra="forbid")

    optimize: bool | None = None
    zorder_by: str | list[str] | None = None
    vacuum: int | float | None = None
    rewrite_data_files: bool | None = None
    expire_snapshots: bool | None = None
    compaction: bool | None = None
    clean: bool | None = None


class JunctionBranchSchema(BaseModel):
    """One Junction fan-out branch. ``condition`` required for `mode:
    conditional` (the sentinel ``"_else_"`` catches unmatched rows);
    ``value`` optional for `mode: partition` (falls back to ``id``)."""

    model_config = ConfigDict(extra="forbid")

    id: str
    condition: str | None = None
    value: str | None = None


class ProbeSignalSchema(BaseModel):
    """One Probe signal. Field applicability is signal-``type``-specific —
    see ``spark/probe.py`` module docstring for the full per-type contract;
    fields are flat and mostly-optional here (same shape as
    ``CascadeTierSchema``) rather than a second-level discriminated union,
    since most fields are shared across several signal types (``fraction``,
    ``columns``) and the type set only grows by hand-curated additions."""

    model_config = ConfigDict(extra="forbid")

    type: str
    # Human-readable signal label — same gallery-wide authoring convention as
    # `AssertRuleSchema.id`; not read by any signal-type handler.
    id: str | None = None
    # row_count_estimate only
    method: Literal["sample", "spark_listener"] | None = None
    # row_count_estimate / null_rates / value_distribution / distinct_count / data_freshness
    fraction: float | None = None
    # null_rates / value_distribution / distinct_count (omit → engine default column set)
    columns: list[str] | None = None
    # sample_rows
    n: int | None = None
    # value_distribution
    percentiles: list[float] | None = None
    # data_freshness (required there)
    column: str | None = None
    # data_freshness
    allow_sample: bool | None = None
    # threshold (required there); custom form 1 (value expression)
    expr: str | None = None
    # custom form 1 — value expression → "estimate"
    sql: str | None = None
    # custom form 1 — boolean expression → "passed"
    passed_when: str | None = None
    # custom form 2 — module pointer (mirrors UDF module/entry contract)
    module: str | None = None
    entry: str | None = None
    # custom form 3 — entry-point plugin (setuptools group 'aqueduct.probe_signals')
    plugin: str | None = None


class AssertOnFailBlockSchema(BaseModel):
    """Assert rule ``on_fail:`` as a block (vs. the bare-string shorthand) —
    only ``action: webhook`` uses ``url``."""

    model_config = ConfigDict(extra="forbid")

    action: Literal["abort", "warn", "webhook", "quarantine", "trigger_agent"]
    url: str | None = None


class AssertRuleType(StrEnum):
    """Assert rule-``type`` vocabulary — the PARSER-layer source of truth for
    the ten rule-type strings (mirrors each engine's own ``AssertRuleType``
    StrEnum — ``executor/spark/assert_.py``, ``executor/duckdb_/assert_.py``
    — member-for-member; those engine copies exist because the Compiler
    layer must not import an engine module, see AGENTS.md's layer-boundary
    rule). ``AssertRuleSchema.type`` derives its ``Literal`` from this enum's
    VALUES (``Literal[*(m.value for m in AssertRuleType)]`` — plain strings,
    not the enum members themselves, so a parsed rule's ``type`` stays a
    bare ``str`` exactly as before; unpacking the members directly would
    make pydantic coerce a matching input into the enum instance, changing
    ``repr()``/message-formatting behaviour downstream) so there is exactly
    one place that lists the ten strings; the Compiler imports this enum
    instead of hand-rolling a raw string like ``"null_rate"`` that would
    silently stop matching if the vocabulary were ever renamed here without
    a corresponding edit at every comparison site
    (`aqueduct/compiler/compiler.py`'s quarantine-validation block, Pass
    G1)."""

    SCHEMA_MATCH = "schema_match"
    MIN_ROWS = "min_rows"
    MAX_ROWS = "max_rows"
    NOT_NULL = "not_null"
    NULL_RATE = "null_rate"
    FRESHNESS = "freshness"
    SQL = "sql"
    SQL_ROW = "sql_row"
    CUSTOM = "custom"
    SPILLWAY_RATE = "spillway_rate"


class AssertRuleSchema(BaseModel):
    """One Assert rule. Field applicability is rule-``type``-specific — see
    ``spark/assert_.py`` module docstring for the full per-type contract.
    Flat and mostly-optional (same rationale as ``ProbeSignalSchema``)."""

    model_config = ConfigDict(extra="forbid")

    type: Literal[*(m.value for m in AssertRuleType)]
    # Human-readable rule label — gallery-wide authoring convention (every
    # shipped Assert snippet sets one). Not read by any rule-type handler in
    # `spark/assert_.py` (no functional effect); kept as a legitimate field
    # rather than rejected, since it is a genuine, harmless labeling
    # convention, not a misspelled synonym of a real key (contrast the
    # `min_count`/`max_count`/`max_rate` typos found and fixed in
    # `24_assert_types_full` during Pass C — those WERE synonyms silently
    # no-oping the rule).
    id: str | None = None
    on_fail: (
        Literal["abort", "warn", "webhook", "quarantine", "trigger_agent"]
        | AssertOnFailBlockSchema
        | None
    ) = None
    # generic across every rule type
    error_type: str | None = None
    # schema_match (required there)
    expected: dict[str, str] | None = None
    # min_rows
    min: int | None = None
    # max_rows / null_rate / spillway_rate — meaning is rule-type-specific
    # (a row-count ceiling for max_rows, a fraction ceiling for the other two)
    max: float | None = None
    # null_rate / freshness / not_null (required on those three)
    column: str | None = None
    # null_rate
    fraction: float | None = None
    # freshness
    max_age_hours: float | None = None
    # sql / sql_row (required on both)
    expr: str | None = None
    # sql_row only
    min_pass_rate: float | None = None
    # custom (required there) — dotted `module.callable`
    fn: str | None = None


# ── Per-type `config:` sub-models ───────────────────────────────────────────


class IngressConfigSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # Any format string the active engine's reader supports (parquet, delta,
    # csv, json, orc, avro, jdbc, kafka, depot, custom, ...) — deliberately
    # NOT a closed Literal; see `executor/capability_leaves.py`'s
    # INGRESS_FORMATS docstring for why only a curated subset gets its own
    # capability leaf.
    format: str | None = None
    # Catalog-addressed read (`spark.read.table()`); mutually exclusive with `path`.
    table: str | None = None
    path: Annotated[str, FsPath()] | None = None
    # `format: custom` — dotted Python DataSource class path (pyspark>=4.0).
    class_: str | None = Field(default=None, alias="class")
    partition_filters: str | None = None
    # Polymorphic passthrough — `{col: type}`, `{mode, columns: [{name,type}]}`,
    # or `[{name, type}]`. Kept untyped-shape (documented here, not modeled)
    # per the narrow-freeform-passthrough carve-out: the 3 accepted shapes
    # are a display convenience, not an engine option space to enumerate.
    schema_hint: dict[str, Any] | list[Any] | None = None
    time_travel: IngressTimeTravelSchema | None = None
    on_new_columns: Literal["allow", "fail", "alert"] | None = None
    known_columns: list[str] | None = None
    # Deliberate freeform passthrough — forwarded verbatim to the engine's
    # DataFrameReader `.option(k, v)`; enumerating every Spark/DuckDB reader
    # option is out of scope (see module docstring's mandatory constraint).
    options: dict[str, Any] = Field(
        default_factory=dict,
        description="Freeform passthrough reader options forwarded verbatim to the engine's reader.",
    )
    header: bool | None = None
    infer_schema: bool | None = None


class ChannelConfigSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # Not a closed Literal — the canonical op set lives in
    # `executor/channel_ops.py::ALL_OPS` (engine-agnostic; parser must not
    # import pyspark, so it isn't imported here) and is already the runtime
    # dispatch guard plus the `channel.op.*` capability-leaf source.
    op: str | None = None
    query: str | None = None
    # op: join
    left: str | None = None
    right: str | None = None
    join_type: Literal["inner", "left", "right", "full", "semi", "anti", "cross"] | None = None
    # op: join (ON clause) / op: filter (alias of `expr`)
    condition: str | None = None
    expr: str | None = None
    broadcast_side: Literal["left", "right"] | None = None
    # op: deduplicate
    key: str | list[str] | None = None
    # op: deduplicate (ranking) / op: sort (alias of `columns`)
    order_by: str | list[str] | None = None
    # Polymorphic per op — select: list[str]; rename: `{old: new}` or
    # `[{from, to}]`/`[{old, new}]`; cast: `{col: type}` or `[{column, type}]`.
    # Kept untyped-shape (see IngressConfigSchema.schema_hint rationale).
    columns: Any = None
    # op: select (alias of `columns`)
    cols: list[str] | None = None
    # op: repartition / coalesce
    num_partitions: int | None = None
    num: int | None = None
    # op: repartition (optional target column)
    column: str | None = None
    col: str | None = None
    # op: cache
    storage_level: (
        Literal[
            "MEMORY_AND_DISK",
            "MEMORY_AND_DISK_SER",
            "MEMORY_ONLY",
            "MEMORY_ONLY_SER",
            "DISK_ONLY",
            "DISK_ONLY_2",
            "OFF_HEAP",
        ]
        | None
    ) = None
    # op: union
    allow_missing_columns: bool | None = None
    # applies to any op — forces a Spark stage-cut for accurate per-module metrics
    metrics_boundary: bool | None = None
    # Read by the executor orchestration layer (not this op's own handler) —
    # rows matching route to the `spillway` port.
    spillway_condition: str | None = None


class EgressConfigSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    format: str | None = None
    mode: (
        Literal[
            "overwrite",
            "append",
            "error",
            "errorifexists",
            "ignore",
            "merge",
            "overwrite_partitions",
        ]
        | None
    ) = None
    table: str | None = None
    path: Annotated[str, FsPath()] | None = None
    partition_by: list[str] | None = None
    # mode: merge (required there)
    merge_key: str | list[str] | None = None
    # format: custom — dotted Python DataSource class path
    class_: str | None = Field(default=None, alias="class")
    # mode: overwrite_partitions
    replace_where: str | None = None
    merge_schema: bool | None = None
    overwrite_schema: bool | None = None
    on_new_columns: Literal["allow", "fail", "alert"] | None = None
    # Deliberate freeform passthrough — see IngressConfigSchema.options.
    options: dict[str, Any] = Field(
        default_factory=dict,
        description="Freeform passthrough writer options forwarded verbatim to the engine's writer.",
    )
    register_as_table: str | None = None
    maintenance: EgressMaintenanceSchema | None = None
    header: bool | None = None
    # format: depot
    key: str | None = None
    value: str | None = None
    value_expr: str | None = None
    # NOT a real repartition/coalesce — read only by two compiler warning
    # heuristics (`file_format_no_repartition`, `perf_delta_append_no_partition`)
    # to decide whether to suppress the "small files" warning; setting this
    # has no effect on the actual write's file count. Kept (it has a real,
    # if weaker-than-the-name-implies, reader) — see Pass C findings.
    repartition: int | bool | None = None
    coalesce: int | bool | None = None


class JunctionConfigSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["conditional", "broadcast", "partition"] | None = None
    branches: list[JunctionBranchSchema] = Field(default_factory=list)
    # mode: partition (required there)
    partition_key: str | None = None


class FunnelConfigSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["union_all", "union", "coalesce", "zip"] | None = None
    inputs: list[str] = Field(default_factory=list)
    # union_all / union only
    schema_check: Literal["strict", "permissive"] | None = None


class ProbeConfigSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    report: Literal["stdout"] | None = None
    signals: list[ProbeSignalSchema] = Field(default_factory=list)


class RegulatorConfigSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    on_block: Literal["skip", "abort", "trigger_agent"] | None = None
    timeout_seconds: float | None = None
    poll_seconds: float | None = None


class ArcadeConfigSchema(BaseModel):
    """Arcade has NO legal `config:` keys — `ref`/`context_override` are
    module-top-level fields (see `ArcadeSchema`), not `config:` entries.
    Zero fields + `extra="forbid"` so a stray `config:` block on an Arcade
    module is a structural rejection, not a silent freeform accept."""

    model_config = ConfigDict(extra="forbid")


class AssertConfigSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rules: list[AssertRuleSchema] = Field(default_factory=list)


# ── Per-type module schemas (the discriminated union members) ──────────────


class IngressSchema(ModuleCommonSchema):
    type: Literal["Ingress"]
    config: IngressConfigSchema = Field(default_factory=IngressConfigSchema)


class ChannelSchema(ModuleCommonSchema):
    type: Literal["Channel"]
    # Incremental watermark processing (2.40) — `op: sql` only; see
    # docs/specs.md §4.4 Channel for the substitution semantics.
    materialize: Literal["incremental"] | None = None
    # Required when `materialize: incremental` — column whose MAX() tracks
    # the incremental high-water mark.
    watermark_column: str | None = None
    config: ChannelConfigSchema = Field(default_factory=ChannelConfigSchema)


class EgressSchema(ModuleCommonSchema):
    type: Literal["Egress"]
    config: EgressConfigSchema = Field(default_factory=EgressConfigSchema)


class JunctionSchema(ModuleCommonSchema):
    type: Literal["Junction"]
    config: JunctionConfigSchema = Field(default_factory=JunctionConfigSchema)


class FunnelSchema(ModuleCommonSchema):
    type: Literal["Funnel"]
    config: FunnelConfigSchema = Field(default_factory=FunnelConfigSchema)


class ProbeSchema(ModuleCommonSchema):
    type: Literal["Probe"]
    # Module this Probe taps. Required at compile time (`compiler/wirer.py`),
    # kept Optional here — same split as pre-Pass-C.
    attach_to: str | None = None
    config: ProbeConfigSchema = Field(default_factory=ProbeConfigSchema)


class RegulatorSchema(ModuleCommonSchema):
    type: Literal["Regulator"]
    config: RegulatorConfigSchema = Field(default_factory=RegulatorConfigSchema)


class ArcadeSchema(ModuleCommonSchema):
    type: Literal["Arcade"]
    # Sub-Blueprint path, required at expansion time (`compiler/expander.py`).
    ref: str | None = None
    context_override: dict[str, Any] | None = None
    config: ArcadeConfigSchema = Field(default_factory=ArcadeConfigSchema)


class AssertSchema(ModuleCommonSchema):
    type: Literal["Assert"]
    config: AssertConfigSchema = Field(default_factory=AssertConfigSchema)


# Type-name → schema class — the single source of truth for tooling that
# needs to walk every module type's own schema (capability-leaf derivation,
# scaffolding, docs generation). Keep in sync with the union below by
# construction — both are built from this same set of classes.
MODULE_TYPE_SCHEMAS: dict[str, type[BaseModel]] = {
    "Ingress": IngressSchema,
    "Channel": ChannelSchema,
    "Egress": EgressSchema,
    "Junction": JunctionSchema,
    "Funnel": FunnelSchema,
    "Probe": ProbeSchema,
    "Regulator": RegulatorSchema,
    "Arcade": ArcadeSchema,
    "Assert": AssertSchema,
}

# Nested/list-item blocks that carry their own capability-leaf prefix — the
# module-type analogue of `capability_leaves.py`'s `_SCHEMA_BLOCKS` (agent,
# guardrails, cascade_tier, ...). Kept here, next to the models, rather than
# hand-duplicated in capability_leaves.py.
MODULE_NESTED_SCHEMA_BLOCKS: tuple[tuple[str, type[BaseModel]], ...] = (
    ("ingress_time_travel", IngressTimeTravelSchema),
    ("egress_maintenance", EgressMaintenanceSchema),
    ("junction_branch", JunctionBranchSchema),
    ("probe_signal", ProbeSignalSchema),
    ("assert_on_fail", AssertOnFailBlockSchema),
    ("assert_rule", AssertRuleSchema),
)

# The discriminated union — BlueprintSchema.modules' element type. Pydantic
# dispatches on `type`; `BlueprintSchema._check_module_types` runs first
# (mode="before") to give the Handoff-rejection and unknown-type messages
# their pre-Pass-C wording instead of pydantic's generic discriminator error.
ModuleSchema = Annotated[
    IngressSchema
    | ChannelSchema
    | EgressSchema
    | JunctionSchema
    | FunnelSchema
    | ProbeSchema
    | RegulatorSchema
    | ArcadeSchema
    | AssertSchema,
    Field(discriminator="type"),
]


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
    return_type: str | None = None  # executor defaults to "string" when absent
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


class SparkEngineBlockSchema(BaseModel):
    """Blueprint-level Spark session configuration (`engine.spark:` block, 2.0).

    Mirrors ``aqueduct.yml``'s ``engine.spark.conf`` inner shape exactly
    (``aqueduct/config.py::SparkEngineConfig``). No ``master_url`` here —
    a Blueprint doesn't pick a cluster to run on, that's a deployment
    concern; only the engine-level `engine.spark:` block has one.
    """

    model_config = ConfigDict(extra="forbid")

    conf: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Per-run Spark session configuration, merged with the engine-"
            "level engine.spark.conf from aqueduct.yml (Blueprint wins). "
            "Replaces the pre-2.0 top-level spark_config block."
        ),
    )


class DuckDBEngineBlockSchema(BaseModel):
    """Blueprint-level DuckDB configuration (`engine.duckdb:` block, 2.0).

    Mirrors ``aqueduct.config.DuckDBEngineConfig``, but only the per-pipeline
    RESOURCE/TUNING knobs — ``memory_limit``/``threads``. A pipeline
    plausibly needs more memory or more threads than the machine default,
    and that is a property of the pipeline, not the deployment, so a
    Blueprint-level override is meaningful.

    Deliberately EXCLUDES the DEPLOYMENT/CONNECTION fields
    (``database_path``, the ``s3_*`` credential/endpoint fields,
    ``extension_repository``): those describe where and how THIS
    installation connects (a persistent database file's location on this
    machine, credentials, an internal extension mirror), not what this
    pipeline needs. Same reasoning ``SparkEngineBlockSchema`` already
    documents for omitting ``master_url`` — a Blueprint overriding a
    credential secret name or an extension-mirror URL is a footgun, not a
    feature.

    Types/defaults mirror ``DuckDBEngineConfig`` exactly (``str | None`` /
    ``int | None``, defaulting ``None``) so an unset field stays unset —
    ``aqueduct.parser.parser._resolve_engine_block_raw``'s
    ``exclude_unset=True`` depends on that to avoid clobbering the
    ``aqueduct.yml``-level value with a spurious ``None``.
    """

    model_config = ConfigDict(extra="forbid")

    memory_limit: str | None = Field(
        default=None,
        description=(
            "DuckDB `SET memory_limit='<value>'` (e.g. '4GB'), merged with "
            "the engine-level engine.duckdb.memory_limit from aqueduct.yml "
            "(Blueprint wins). Unset leaves the aqueduct.yml value (or "
            "DuckDB's own default) untouched."
        ),
    )
    threads: int | None = Field(
        default=None,
        gt=0,
        description=(
            "DuckDB `SET threads=<value>`, merged with the engine-level "
            "engine.duckdb.threads from aqueduct.yml (Blueprint wins). "
            "Unset leaves the aqueduct.yml value (or DuckDB's own default) "
            "untouched."
        ),
    )


class EngineBlockSchema(BaseModel):
    """Per-engine Blueprint configuration, namespaced by engine name
    (`engine:` block, 2.0). Replaces the pre-2.0 top-level `spark_config`
    dict. Mirrors the `engine:` block in `aqueduct.yml` exactly — see
    `aqueduct.config.EngineConfig`.

    Distinct from the per-module `engine:` FIELD (`ModuleSchema.engine`,
    2.34) that selects which engine runs one module — this is the
    per-engine SETTINGS block, keyed by engine name, at the top level of
    the Blueprint. Deliberately the same word at two levels: this block
    configures an engine's session behaviour; the module field picks which
    engine a module runs on. Never confuse the two in error messages.
    """

    model_config = ConfigDict(extra="forbid")

    spark: SparkEngineBlockSchema = Field(default_factory=SparkEngineBlockSchema)
    duckdb: DuckDBEngineBlockSchema = Field(default_factory=DuckDBEngineBlockSchema)


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
            'The single entry `"*"` silences every compile warning for '
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
    # Error-type filter (on_failure / on_patch_pending / on_healed only —
    # HooksSchema rejects it on on_success entries, no failure context there).
    # Matched against FailureContext.error_type / stack-trace exception class,
    # same exact-match semantics as GuardrailsConfig.heal_on_errors. Empty =
    # fires unconditionally (backward-compatible default).
    when_error: list[str] = Field(default_factory=list)
    # blueprint: entries only — parse+compile+execute the target Blueprint
    # in-process, reusing the live SparkSession, instead of spawning an
    # `aqueduct run` subprocess. Falls back to subprocess (with an info
    # message) when the target's engine.spark.conf is non-empty (session
    # config conflict risk).
    in_process: bool = False

    @model_validator(mode="after")
    def _exactly_one_action(self) -> HookEntrySchema:
        set_keys = [k for k in ("blueprint", "webhook", "command") if getattr(self, k) is not None]
        if len(set_keys) != 1:
            raise ValueError(
                "hook entry must set exactly one of blueprint/webhook/command"
                f" — got {set_keys or 'none'}"
            )
        return self

    @model_validator(mode="after")
    def _in_process_only_on_blueprint(self) -> HookEntrySchema:
        if self.in_process and self.blueprint is None:
            raise ValueError("hook entry: in_process: true is only valid on a blueprint: entry")
        return self


class HooksSchema(BaseModel):
    """Blueprint lifecycle hooks. `on_success`/`on_failure` run AFTER the
    pipeline's terminal state; `on_patch_pending`/`on_healed` fire mid-run
    at heal milestones (mirroring the engine-level `webhooks:` vocabulary).

    Hooks never change the run's exit code; a failing hook is a warning and
    stops the remaining hooks of that event. Distinct from the engine-level
    `webhooks:` block in aqueduct.yml (ops-owned alerting that fires
    regardless of what the Blueprint declares).
    """

    model_config = ConfigDict(extra="forbid")

    on_success: list[HookEntrySchema] = Field(default_factory=list)
    on_failure: list[HookEntrySchema] = Field(default_factory=list)
    on_patch_pending: list[HookEntrySchema] = Field(default_factory=list)
    on_healed: list[HookEntrySchema] = Field(default_factory=list)

    @model_validator(mode="after")
    def _when_error_needs_failure_context(self) -> HooksSchema:
        # on_success has no failure context to filter on — reject rather
        # than silently ignore (extra="forbid" strictness philosophy).
        if any(e.when_error for e in self.on_success):
            raise ValueError(
                "hooks.on_success entries cannot set when_error — on_success "
                "has no failure context to match against"
            )
        return self


class HealedByRecordSchema(BaseModel):
    """One self-heal provenance record (heal-patch cross-engine gate).

    MACHINE-WRITTEN ONLY — ``aqueduct patch apply`` appends one of these per
    applied patch; Blueprint authors never hand-write this block. It travels
    with the Blueprint artifact (never a sidecar) so a compiled-for-engine-X
    Blueprint that carries an ``engine_shaped`` patch from engine Y can be
    flagged at compile time before it ships a dialect mismatch to production
    (see ``aqueduct/compiler/capability_check.py::check_cross_engine_heal``
    and ``docs/specs.md`` §8). Purely compiler-consumed metadata — no engine
    reads or executes this block at runtime.
    """

    model_config = ConfigDict(extra="forbid")

    patch_id: str
    # The execution engine the healing LLM call targeted (from the failing
    # run's FailureContext.engine — required, no default; see
    # aqueduct/surveyor/models.py).
    engine: str
    # Best-effort, nullable — the installed engine package version at heal
    # time (e.g. duckdb.__version__), when cheaply available.
    engine_version: str | None = None
    run_id: str | None = None
    # Max over the patch's operations — see aqueduct/patch/provenance.py.
    classification: Literal["dialect_neutral", "engine_shaped"]
    applied_at: str
    # Engines this patch has been GREEN-run-validated on since application.
    # Appended to (never replaces) by the self-clearing stamp on a successful
    # run — see aqueduct/patch/apply.py::stamp_validated_engine.
    validated_on: list[str] = Field(default_factory=list)
    # EFFECTIVE session-config diff this patch produced, per engine:
    # {engine: {key: {before, after}}}. Written by Gate 1's
    # effective-engine-config check (aqueduct/patch/config_delta.py) at apply
    # time, because the resolved config depends on the aqueduct.yml the patch
    # was applied against — not on anything the patch itself carries. Absent
    # for a patch that writes no engine config (the overwhelmingly common
    # pipeline-only heal), never an empty mapping.
    engine_config_delta: dict[str, dict[str, Any]] = Field(default_factory=dict)


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
    engine: EngineBlockSchema = Field(default_factory=EngineBlockSchema)
    retry_policy: RetryPolicySchema = Field(default_factory=RetryPolicySchema)
    agent: AgentSchema = Field(default_factory=AgentSchema)
    udf_registry: list[UdfSchema] = Field(default_factory=list)
    macros: dict[str, str] = Field(default_factory=dict)
    required_context: list[str] = Field(default_factory=list)  # Arcade sub-Blueprint
    checkpoint: bool = False
    warnings: WarningsSchema = Field(default_factory=WarningsSchema)
    hooks: HooksSchema = Field(default_factory=HooksSchema)
    # Self-heal provenance (see HealedByRecordSchema docstring) — machine
    # written by `aqueduct patch apply`, never hand-authored.
    healed_by: list[HealedByRecordSchema] = Field(default_factory=list)

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

    @field_validator("modules", mode="before")
    @classmethod
    def _check_module_types(cls, v: Any) -> Any:
        """Give the Handoff-rejection and unknown-type messages their
        pre-Pass-C wording, before pydantic's own discriminated-union
        dispatch on `modules[i].type` runs (whose generic error is less
        actionable for the Handoff case specifically)."""
        if not isinstance(v, list):
            return v
        for m in v:
            if not isinstance(m, dict):
                continue
            t = m.get("type")
            if t in _COMPILER_SYNTHESIZED_TYPES:
                raise ValueError(
                    f"Module type {t!r} is reserved for the compiler's synthetic "
                    "cross-engine handoff insertion (Phase 81) and cannot be "
                    "declared in a Blueprint — remove it; the compiler inserts "
                    "one automatically at each engine boundary."
                )
            if t not in VALID_MODULE_TYPES:
                raise ValueError(
                    f"Unknown module type: {t!r}. Must be one of {sorted(VALID_MODULE_TYPES)}"
                )
        return v

    @model_validator(mode="after")
    def validate_unique_module_ids(self) -> BlueprintSchema:
        ids = [m.id for m in self.modules]
        seen: set[str] = set()
        dupes = {mid for mid in ids if mid in seen or seen.add(mid)}  # type: ignore[func-returns-value]
        if dupes:
            raise ValueError(f"Duplicate module IDs: {sorted(dupes)}")
        return self
