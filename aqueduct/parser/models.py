"""Immutable AST dataclasses for the parsed Blueprint.

All types use @dataclass(frozen=True) — prevents accidental mutation in
downstream layers (Compiler, Planner, Executor). Use dataclasses.replace()
to produce modified copies.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from enum import StrEnum
from typing import Any


class ModuleType(StrEnum):
    """Canonical module type names for the 9 authorable blueprint module
    kinds, plus one compiler-synthesized kind (`Handoff`, Phase 81 step 2).

    `Handoff` is NEVER legal in authored Blueprint YAML — see
    `aqueduct.parser.schema.ModuleSchema.validate_type`, which rejects it
    explicitly. It only ever appears on a `Module` the compiler inserts at
    a cross-engine island boundary (`aqueduct.compiler.handoff`), always
    with `Module.synthetic=True`.
    """

    Ingress = "Ingress"
    Channel = "Channel"
    Egress = "Egress"
    Junction = "Junction"
    Funnel = "Funnel"
    Probe = "Probe"
    Regulator = "Regulator"
    Arcade = "Arcade"
    Assert = "Assert"
    Handoff = "Handoff"


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
    forbidden_ops: tuple[str, ...] = ()  # PatchSpec op names blocked from auto-apply
    allowed_paths: tuple[
        str, ...
    ] = ()  # fnmatch patterns for config path values; empty = unrestricted
    deny_patterns: tuple[str, ...] = ()
    # fnmatch patterns for config path values, evaluated AFTER allowed_paths;
    # subtract-only — a path allowed_paths permits (or that is unrestricted
    # because allowed_paths is empty) is still refused if it matches here.
    # Applies even when allowed_paths is empty; empty = no additional denial.
    heal_on_errors: tuple[
        str, ...
    ] = ()  # LLM only fires when error_type matches; empty = no restriction
    never_heal_errors: tuple[
        str, ...
    ] = ()  # LLM never fires when error_type matches; takes priority


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
    """Blueprint-level self-healing POLICY, resolved from ``AgentSchema``
    (``aqueduct/parser/schema.py``).

    CONNECTION fields (``provider``/``base_url``/``api_key``/``model``/
    ``provider_options``/``timeout``/``cascade``) are deliberately absent —
    a Blueprint cannot set or override them; see ``AgentSchema``'s
    docstring. The effective connection settings always come from
    ``aqueduct.yml``'s ``agent:`` block (``AgentConnectionConfig`` in
    ``aqueduct/config.py``), resolved by ``aqueduct.cli.resolve_agent_connection``.
    """

    approval_mode: str = "disabled"  # YAML key `approval`: "disabled" | "human" | "auto"
    on_pending_patches: str = "warn"  # "ignore" | "warn" | "block"
    # `max_patches` (default 1). Multi-patch loop opt-in: set > 1 AND
    # `danger.allow_multi_patch: true`.
    max_patches: int = 1
    # Reprompt-budget policy — None = inherit from aqueduct.yml agent: defaults
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
    # Phase 43: run sandbox/lineage gates inside the LLM conversation
    # so the model sees rejection feedback and retries in-context instead of
    # starting a fresh conversation each time. Default False preserves the
    # current behaviour (gates run post-hoc via apply_callback).
    deep_loop: bool = False
    # Extra context appended to LLM system prompt for this blueprint only (after engine-level prompt_context)
    prompt_context: str | None = None
    # Spend-cap: max LLM healing attempts per rolling 60-minute window for this blueprint.
    # None = unlimited. When exceeded, Surveyor blocks the LLM call.
    max_heal_attempts_per_hour: int | None = None
    # "full_run" | "sandbox" | None (= inherit from engine default).
    # Controls whether `auto` mode validates a generated patch by a full
    # Spark run after the sandbox replay, or by sandbox replay alone.
    patch_validation: str | None = None
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
            "max_patches": self.max_patches,
            "prompt_context": self.prompt_context,
            "sandbox_mode": self.sandbox_mode,
            "allow_defer": self.allow_defer,
            "deep_loop": self.deep_loop,
            "confidence_threshold": self.confidence_threshold,
            "patch_validation": self.patch_validation,
            "max_heal_attempts_per_hour": self.max_heal_attempts_per_hour,
            "guardrails": {
                "forbidden_ops": list(self.guardrails.forbidden_ops),
                "allowed_paths": list(self.guardrails.allowed_paths),
                "deny_patterns": list(self.guardrails.deny_patterns),
                "heal_on_errors": list(self.guardrails.heal_on_errors),
                "never_heal_errors": list(self.guardrails.never_heal_errors),
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
    # Cross-engine handoff (2.34) — as authored, `None` means "unresolved":
    # the parser carries this verbatim (an explicit pin, or unset). The
    # compiler (`aqueduct/compiler/islands.py::resolve_module_engines`)
    # replaces this with the FINAL resolved engine name for every enabled
    # module in the compiled Manifest, so `Manifest.modules[i].engine` is
    # always a concrete engine name, never None, once compiled — EXCEPT a
    # synthetic Handoff module (see `synthetic` below), which bridges TWO
    # engines and deliberately carries `engine=None`; its bridged engines
    # live in `config["from_engine"]`/`config["to_engine"]` instead. See
    # `aqueduct.parser.schema.ModuleSchema.engine` for the inheritance rules.
    engine: str | None = None
    spillway: str | None = None
    depends_on: tuple[str, ...] = ()
    on_failure: dict[str, Any] | None = None
    on_failure_webhook: str | dict[str, Any] | None = None
    # Per-module retry policy override, already merged against the
    # blueprint-level RetryPolicy at parse time (None = no per-module
    # override; the blueprint-level `retry_policy` applies as-is). Distinct
    # from `on_failure`, which is an LLM-patch write target for a full
    # RetryPolicy replacement.
    retry: RetryPolicy | None = None
    checkpoint: bool = False
    # Conditional execution (`enabled:` in YAML, resolved from ${ctx.*} at
    # parse time). The compiler cascade-disables downstream consumers and
    # stamps `disabled_reason`; the executor marks disabled modules SKIPPED.
    enabled: bool = True
    disabled_reason: str | None = None
    # Probe-specific: module this Probe taps
    attach_to: str | None = None
    # Arcade-specific: sub-Blueprint path and context overrides
    ref: str | None = None
    context_override: dict[str, Any] | None = None
    # Channel-specific (2.40): incremental watermark processing — see
    # `aqueduct.parser.schema.ModuleSchema.materialize`/`watermark_column`.
    # Promoted out of `config` (a freeform dict, invisible to the capability
    # framework) to a declared field.
    materialize: str | None = None
    watermark_column: str | None = None
    # True ONLY for a compiler-synthesized module (currently: a Handoff —
    # `aqueduct.compiler.handoff.insert_handoff_modules`). Never settable
    # from Blueprint YAML (`ModuleSchema` has no such field), so a module
    # parsed from user YAML always has `synthetic=False` — mirrors
    # `Edge.injected`'s "compiler-inserted vs. user-authored" provenance
    # marker one level up, at the module rather than the edge.
    synthetic: bool = False


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
class HookEntry:
    """One lifecycle-hook action. `kind` ∈ {"blueprint", "webhook", "command"};
    `value` is the path / url-or-endpoint-map / command string verbatim from
    YAML — runtime variables (${run.id}, ${run.status}, ${blueprint.id}) are
    interpolated by the CLI hook runner at fire time, NOT at parse time.

    `when_error`: optional list of error-type names matched against
    `FailureContext.error_type` / the exception class extracted from the
    stack trace — same candidate set and exact-match semantics as
    `GuardrailsConfig.heal_on_errors`. Empty = fires unconditionally
    (backward-compatible default). Only meaningful on events that carry a
    failure context (`on_failure`, `on_patch_pending`, `on_healed`) — the
    schema rejects it on `on_success` entries.

    `in_process`: opt-in for `blueprint:` entries only — parse+compile+
    execute the target Blueprint in the same Python process, reusing the
    live SparkSession, instead of spawning an `aqueduct run` subprocess.
    """

    kind: str
    value: Any
    timeout: int = 300
    when_error: tuple[str, ...] = ()
    in_process: bool = False


@dataclass(frozen=True)
class Hooks:
    """Blueprint lifecycle hooks (`hooks:` block). `on_success`/`on_failure`
    run after the pipeline's terminal state; `on_patch_pending`/`on_healed`
    fire mid-run at heal milestones (mirroring the engine-level `webhooks:`
    vocabulary). Never change the run's exit code. Distinct from the
    engine-level `webhooks:` block in aqueduct.yml (ops-owned alerting)."""

    on_success: tuple[HookEntry, ...] = ()
    on_failure: tuple[HookEntry, ...] = ()
    on_patch_pending: tuple[HookEntry, ...] = ()
    on_healed: tuple[HookEntry, ...] = ()

    def __bool__(self) -> bool:
        return any(getattr(self, name) for name in HOOK_EVENTS)


# The 4 `Hooks` field names, derived from the dataclass itself rather than
# hand-copied — `compiler/models.py::Manifest.to_dict()` (hooks serialization)
# and `cli/hooks.py` (the `hooks.blueprint:` cycle-detection pre-scan) both
# need this exact list and used to hand-write the same 4-string tuple
# independently. A 5th event added to `Hooks` above without also editing both
# of those literals would silently vanish from the exported manifest AND from
# cycle detection — the same include-list-on-a-growing-set shape AGENTS.md's
# bug-family rule names (`compiler/islands.py`'s port set, the Assert
# quarantine gate, ...). Deriving from `fields(Hooks)` makes that structurally
# impossible: a new field is a member here the moment it exists.
HOOK_EVENTS: tuple[str, ...] = tuple(f.name for f in fields(Hooks))


@dataclass(frozen=True)
class HealedByRecord:
    """One self-heal provenance record — parsed mirror of
    ``parser.schema.HealedByRecordSchema``. See that model's docstring."""

    patch_id: str
    engine: str
    classification: str
    applied_at: str
    engine_version: str | None = None
    run_id: str | None = None
    validated_on: tuple[str, ...] = ()
    # Effective session-config diff Gate 1 recorded for this patch, per
    # engine: {engine: {key: {"before": ..., "after": ...}}}. Empty for a
    # patch that writes no engine config.
    engine_config_delta: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Warn-only perf attribution — see HealedByRecordSchema's comments.
    # `perf_baseline` is the pre-patch green run's snapshot; each entry of
    # `perf_observations` is one engine's post-patch note. Both are inert
    # diagnostics: nothing in the compiler or any engine reads them.
    perf_baseline: dict[str, Any] = field(default_factory=dict)
    perf_observations: tuple[dict[str, Any], ...] = ()
    # Set by `aqueduct patch revert` — see HealedByRecordSchema's comment.
    # A reverted record documents a heal that no longer applies: the
    # cross-engine gate skips it and the green-run stamps leave it alone.
    reverted_at: str | None = None


@dataclass(frozen=True)
class Blueprint:
    aqueduct_version: str
    id: str
    name: str
    context: ContextRegistry
    modules: tuple[Module, ...]
    edges: tuple[Edge, ...]
    description: str = ""
    # Per-engine Blueprint-level session config, keyed by engine name (e.g.
    # {"spark": {"spark.sql.shuffle.partitions": "200"}, "duckdb": {...}}) —
    # sourced from the YAML `engine.<name>:` block (was a single top-level
    # `spark_config:` dict pre-2.0, and was still a Spark-only
    # `spark_config` field here through 2.52 even after the YAML surface
    # went generic — see `aqueduct.parser.schema.EngineBlockSchema`). Every
    # registered engine gets an entry (possibly `{}` when the Blueprint sets
    # none of that engine's fields, e.g. `DuckDBEngineBlockSchema`'s
    # `memory_limit`/`threads`, 2.54), so a new engine's Blueprint-level
    # knob needs no change here — `parser/parser.py` derives this dict
    # structurally from `EngineBlockSchema`'s own fields.
    engine_config: dict[str, dict[str, Any]] = field(default_factory=dict)
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    agent: AgentConfig = field(default_factory=AgentConfig)
    udf_registry: tuple[dict[str, Any], ...] = ()
    # Flat list of PEP 508 requirement strings (`dependencies:` block,
    # Phase 88) — see `aqueduct.dependencies` for the parser/checker and
    # `aqueduct/compiler/compiler.py` for the compile-time preflight that
    # consumes this verbatim (no `${ctx.*}` resolution — a requirement
    # string is not a Blueprint value expression).
    dependencies: tuple[str, ...] = ()
    macros: dict[str, str] = field(default_factory=dict)
    required_context: tuple[str, ...] = ()  # Arcade sub-Blueprint: keys the caller must provide
    checkpoint: bool = False
    # Per-Blueprint compile-warning suppress list (`warnings.suppress` in the
    # Blueprint YAML). Compile-time only — unioned with the engine-level
    # suppress set at the `aqueduct/compiler/warnings/run_all` call site in
    # compiler.py. Never touches session/runtime warnings or the
    # process-global `set_default_suppress` default. For an Arcade sub-
    # Blueprint, this field is parsed but ignored — only the top-level
    # (parent) Blueprint's value applies to the whole compilation unit.
    warning_suppress: tuple[str, ...] = ()
    # Lifecycle hooks (`hooks:` block). For an Arcade sub-Blueprint this is
    # parsed but ignored — only the top-level Blueprint's hooks fire.
    hooks: Hooks = field(default_factory=Hooks)
    # Self-heal provenance (`healed_by:` block) — machine-written by
    # `aqueduct patch apply`, read by the compile-time cross-engine-heal gate
    # (`aqueduct/compiler/capability_check.py`). Never hand-authored.
    healed_by: tuple[HealedByRecord, ...] = ()
    # Absolute directory the Blueprint YAML was loaded from ("" when parsed
    # from a dict with no file). Threaded into the Manifest so executor-side
    # user-code imports (custom Assert fn:, Probe module:+entry:, python
    # UDFs, format: custom) resolve a sibling .py file next to the blueprint
    # via infra/module_loading.py — sys.path never has this directory when
    # running the `aqueduct` console script.
    base_dir: str = ""
