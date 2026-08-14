"""Scenario-based LLM benchmark — Phase 22.

A scenario YAML (.aqscenario.yml) defines a simulated failure + expected LLM
response assertions so the healing agent can be regression-tested and compared
across models without running a real engine pipeline.

File format::

  aqueduct_scenario: "1.0"
  id: schema_drift_column_rename
  description: "Column renamed from event_ts to event_time upstream"
  blueprint: ../pipelines/orders.yml    # resolved relative to scenario file
  domains: [pipeline]                   # optional; filter with --domain
  inject_failure:
    module: cast_and_clean              # failed_module
    engine: spark                       # optional; default "spark"
    error_message: "AnalysisException: Column 'event_ts' does not exist"
    stack_trace: |                      # optional
      ...
    structured:                         # optional high-fidelity error fields
      error_class: UNRESOLVED_COLUMN.WITH_SUGGESTION
  expected_patch:
    effect: { ... }                     # see "Expected effect" below
  assertions:
    - patch_is_valid: true             # PatchSpec parses without schema error
    - patch_applies: true              # patch survives Gate 1 + re-parse/compile
    - patch_refused: policy            # the patch was REFUSED, for this reason
    - gate_status: {engine_config: pass}
    - max_attempts: 1                  # must succeed on first LLM call (no reprompts)
    - min_confidence: 0.8              # LLM self-reported confidence above threshold
    - expected_category: format_mismatch  # LLM must classify the failure correctly
    - root_cause_contains: "format"    # root_cause field must contain this keyword
    - allow_defer: true                # accept (or require) defer_to_human

**Unknown keys are rejected**, at every level (top level, ``inject_failure``,
``expected_patch``, ``effect``, and each assertion mapping). The reader used to
read known keys with ``raw.get(...)`` and ignore the rest, so a typo'd
``asertions:`` silently graded the scenario against nothing at all — the
"no silent no-ops" rule violated by the format itself. Rejection is not a
format change: no ``aqueduct_scenario: "1.0"`` file was ever documented as
being allowed to carry an unrecognised key, so a file this now refuses was
already mis-grading. See ``ScenarioError``.

**Version.** The reader still accepts ``"1.0"`` only, and that is deliberate.
Every key added since is OPTIONAL and additive: an existing 1.0 file remains
valid and grades identically. The unknown-key tightening applies to *every*
file regardless of the version it declares — keeping it permissive for "1.0"
would preserve exactly the silent no-op it exists to end — so a ``"1.1"``
reader would be a byte-identical code path under a second name. The version
field earns a bump the day a change is genuinely NOT additive (a key removed
or renamed, as ``ops:``/``forbidden_ops:`` was), which is when a reader has to
branch on it.

Expected effect
---------------

``expected_patch.effect`` grades the POST-PATCH Blueprint. Five keys, at least
one of which must be present — an effect block that states no expectation is a
hard failure, because a scenario that asserts nothing passes for free and that
is the silent no-op this format exists to catch::

  expected_patch:
    effect:
      # 1. a named module's config (pipeline edits, "domain 1")
      module: clean_events
      config_contains:
        query: "event_time"       # SQL-typed key -> sqlglot-normalised substring
        header: true              # bool / number  -> strict typed equality
        path: "data/orders"       # other strings  -> raw substring

      # 2. SOME module matches (the fix inserted a module whose id the
      #    scenario cannot know in advance)
      modules_contain:
        type: Channel
        config_contains: {op: repartition}

      # 3. engine/session config (`set_engine_config`, "domain 2") — the
      #    post-patch value of an engine-config key
      engine_config:
        spark: {spark.sql.shuffle.partitions: 200}
        duckdb: {memory_limit: "4GB"}

      # 4. an engine-config key whose value CHANGED, without pinning to what
      engine_config_changed:
        spark: [spark.sql.shuffle.partitions]

      # 5. at least one of several acceptable fixes
      any_of:
        - engine_config_changed: {spark: [spark.sql.shuffle.partitions]}
        - modules_contain: {type: Channel, config_contains: {op: repartition}}

``module`` is NOT required — a ``set_engine_config`` patch touches no module,
so requiring one gave a domain-2 outcome no legal shape at all. ``module`` and
``modules_contain``/``engine_config``/``engine_config_changed``/``any_of`` are
independent; a block may carry any combination and every one present is
checked.

Engine-config keys are addressed the way ``aqueduct.patch.config_delta.
blueprint_engine_layers`` addresses them — ``{engine: {key: value}}`` — which
normalises Spark's free-form ``engine.spark.conf`` bag and DuckDB's typed
``engine.duckdb.<field>`` block into one shape, so a scenario never has to
know which of the two its target engine uses.

Engine-config VALUES compare by equality on the canonical (string) form, not
by substring the way ``config_contains`` does. Every engine-config value ends
up as a string on the session, so ``200`` and ``"200"`` are the same setting —
but a substring rule would let an actual of ``1200`` satisfy an expected
``200``, which is the exact superstring bug ``config_contains`` already had to
fix for numbers. A module config value is usually a long SQL string or a path
where substring is the only usable check; an engine-config value is a scalar
whose whole point is its exact value.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC
from pathlib import Path
from typing import Any

import yaml

from aqueduct.errors import ScenarioError
from aqueduct.patch.gate_status import GATE_STATUSES, GateStatus

logger = logging.getLogger(__name__)


# ── Format vocabulary ─────────────────────────────────────────────────────────
#
# Every closed set the loader validates against lives here, once. A key the
# reader does not implement is refused by name rather than dropped.

#: Version strings the reader accepts. See the module docstring for why this
#: has not moved.
SUPPORTED_SCENARIO_VERSIONS: tuple[Any, ...] = ("1.0", 1, "1")

#: Healing domains a scenario may declare, and the ONLY legal ``domains:``
#: members. Named rather than numbered: a name says which surface the FIX
#: touches, while a number implies an ordering and a count that do not exist
#: (two of the five planned domains are built). A scenario may declare BOTH —
#: domain is a property of the fix, not of the failure, and some failures are
#: legitimately fixable either way.
SCENARIO_DOMAINS: tuple[str, ...] = (
    "pipeline",  # Blueprint pipeline edits — modules, config, edges
    "engine_config",  # engine/session config via `set_engine_config`
)

_TOP_LEVEL_KEYS = frozenset(
    {
        "aqueduct_scenario",
        "id",
        "description",
        "blueprint",
        "domains",
        "inject_failure",
        "expected_patch",
        "assertions",
    }
)

_INJECT_FAILURE_KEYS = frozenset({"module", "engine", "error_message", "stack_trace", "structured"})

_EXPECTED_PATCH_KEYS = frozenset({"effect"})

_EFFECT_KEYS = frozenset(
    {
        "module",
        "config_contains",
        "modules_contain",
        "engine_config",
        "engine_config_changed",
        "any_of",
    }
)

#: The effect keys that STATE an expectation. An effect block carrying none of
#: them expects nothing, which is a hard failure.
_EFFECT_EXPECTATION_KEYS = frozenset(
    {"module", "modules_contain", "engine_config", "engine_config_changed", "any_of"}
)

_ASSERTION_KEYS = frozenset(
    {
        "patch_is_valid",
        "patch_applies",
        "patch_refused",
        "gate_status",
        "allow_defer",
        "max_attempts",
        "min_confidence",
        "expected_category",
        "root_cause_contains",
    }
)


# ── Refusal vocabulary ────────────────────────────────────────────────────────
#
# Why this is a NEW assertion (`patch_refused:`) rather than a reason bolted
# onto `patch_applies:`
# ---------------------------------------------------------------------------
# `patch_applies: false` means "the patch did not apply", full stop, and it
# conflates four different outcomes: a malformed patch, a Blueprint-guardrail
# violation, a Gate 1 engine-config POLICY refusal, and an INERT config write.
# For a domain-2 suite that distinction is the entire point.
#
# It is a separate key because giving the existing one a reason would change
# the TYPE of an existing key's value (`patch_applies: false` -> a mapping),
# breaking every scenario in the wild for no gain, while still leaving a plain
# `patch_applies: false` asserting nothing about WHY. And silently widening
# `patch_applies`'s meaning — treating `false` as "any of the four" — is the
# thing that made scenario 07 pass while verifying nothing.
#
# `patch_refused: <reason>` states the fact positively, is purely additive, and
# is checked INDEPENDENTLY of `patch_applies`. A scenario may state both; they
# cannot contradict each other silently, because a refused patch never applies
# and a contradiction fails both assertions loudly.

#: Gate 1 refused the write on policy grounds (denied key, unlisted key, wrong
#: type/shape, unregistered engine) — ``patch.apply.EngineConfigPolicyError``.
REFUSAL_POLICY = "policy"
#: Gate 1 refused the write as inert: it changes no effective config —
#: ``patch.apply.EngineConfigInertError``.
REFUSAL_INERT = "inert"
#: The Blueprint's own ``agent.guardrails`` refused the op (``forbidden_ops`` /
#: ``allowed_paths``) — a plain ``PatchError`` from the guardrail branch.
REFUSAL_GUARDRAIL = "guardrail"
#: The patched Blueprint failed to re-parse or re-compile — the patch is
#: malformed against the schema, not against a policy.
REFUSAL_INVALID = "invalid"

#: Every reason ``patch_refused:`` may name. Classified by exception TYPE
#: (``EngineConfigPolicyError`` / ``EngineConfigInertError`` / ``PatchError`` /
#: ``ParseError``+``CompileError``), never by matching an error message.
REFUSAL_REASONS: tuple[str, ...] = (
    REFUSAL_POLICY,
    REFUSAL_INERT,
    REFUSAL_GUARDRAIL,
    REFUSAL_INVALID,
)

#: Gates a scenario run actually evaluates, and therefore the only names
#: ``gate_status:`` may key on. A scenario starts no engine session, so Gates
#: 2/3/4 (lineage, sandbox replay, explain-plan) never run and asserting on
#: them would be asserting on a check that was never performed. Gate 1's
#: engine-config delta gate DOES run, on every apply, via ``_check_guardrails``.
SCENARIO_GATES: tuple[str, ...] = ("engine_config",)


# ── Scenario model ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class AqScenario:
    """Parsed .aqscenario.yml file."""

    id: str
    description: str
    blueprint: str  # path relative to scenario file
    inject_failure: dict[str, Any]
    expected_patch: dict[str, Any]  # {effect: {...}}
    assertions: list[dict[str, Any]]
    source_path: Path  # absolute path of the .aqscenario.yml file
    #: Healing domains this scenario's expected fix touches (`SCENARIO_DOMAINS`).
    #: Empty when the file declares none, which makes the scenario invisible to
    #: `--domain` filtering — the fail-closed direction, and reported as a count
    #: rather than dropped in silence.
    domains: tuple[str, ...] = ()


def _reject_unknown_keys(
    mapping: dict[str, Any],
    known: frozenset[str],
    *,
    path: Path,
    where: str,
) -> None:
    """Raise ``ScenarioError`` naming every key *known* does not contain.

    The whole reason the loader is strict — see the module docstring. The
    message names the offending key AND the legal set, so the fix is a single
    read of the error, with no separate migration guard duplicating what this
    already reports (AGENTS.md: a breaking change ships as documentation).
    """
    unknown = sorted(k for k in mapping if k not in known)
    if unknown:
        raise ScenarioError(
            f"Scenario {path}: unknown key(s) {unknown} in {where}. "
            f"Known keys: {sorted(known)}. A key this reader does not implement "
            "is refused rather than ignored — an ignored key silently grades "
            "the scenario against an expectation nobody wrote."
        )


def load_scenario(path: Path) -> AqScenario:
    """Parse + hard-validate a .aqscenario.yml file into an AqScenario.

    Raises:
        ScenarioError: the file is not a mapping, declares an unsupported
            ``aqueduct_scenario:`` version, omits ``id``/``inject_failure``,
            carries an unknown key at any level, names an assertion nobody
            implements, or declares a ``domains:`` member outside
            ``SCENARIO_DOMAINS``.
    """
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ScenarioError(f"Scenario {path} is not a valid YAML mapping")
    version = raw.get("aqueduct_scenario")
    if version not in SUPPORTED_SCENARIO_VERSIONS:
        raise ScenarioError(
            f"Scenario {path} missing or unsupported aqueduct_scenario version: {version!r}"
        )
    _reject_unknown_keys(raw, _TOP_LEVEL_KEYS, path=path, where="the scenario file")
    if "id" not in raw:
        raise ScenarioError(f"Scenario {path} missing 'id'")
    if "inject_failure" not in raw:
        raise ScenarioError(f"Scenario {path} missing 'inject_failure'")

    inject_failure = raw.get("inject_failure") or {}
    if not isinstance(inject_failure, dict):
        raise ScenarioError(
            f"Scenario {path}: 'inject_failure' must be a mapping, "
            f"got {type(inject_failure).__name__}"
        )
    _reject_unknown_keys(inject_failure, _INJECT_FAILURE_KEYS, path=path, where="inject_failure")

    expected_patch = raw.get("expected_patch") or {}
    if not isinstance(expected_patch, dict):
        raise ScenarioError(
            f"Scenario {path}: 'expected_patch' must be a mapping, "
            f"got {type(expected_patch).__name__}"
        )
    _reject_unknown_keys(expected_patch, _EXPECTED_PATCH_KEYS, path=path, where="expected_patch")
    # Same structural validator the grader runs, so the two cannot disagree
    # about what a legal effect block is — `aqueduct doctor --aqscenario`
    # catches the shape error before an LLM call is ever made.
    shape_errors = validate_expected_patch(expected_patch)
    if shape_errors:
        raise ScenarioError(f"Scenario {path}: " + "; ".join(shape_errors))

    assertions = raw.get("assertions", [{"patch_is_valid": True}])
    if not isinstance(assertions, list):
        raise ScenarioError(
            f"Scenario {path}: 'assertions' must be a list of single-key mappings, "
            f"got {type(assertions).__name__}"
        )
    for entry in assertions:
        if not isinstance(entry, dict):
            raise ScenarioError(
                f"Scenario {path}: every 'assertions' entry must be a mapping, got {entry!r}"
            )
        _reject_unknown_keys(entry, _ASSERTION_KEYS, path=path, where="an assertions entry")
        if "patch_refused" in entry and entry["patch_refused"] not in REFUSAL_REASONS:
            raise ScenarioError(
                f"Scenario {path}: patch_refused={entry['patch_refused']!r} is not a "
                f"known refusal reason. Legal reasons: {list(REFUSAL_REASONS)}."
            )
        if "gate_status" in entry:
            gs = entry["gate_status"]
            if not isinstance(gs, dict) or not gs:
                raise ScenarioError(
                    f"Scenario {path}: gate_status must be a non-empty mapping of "
                    f"{{gate: status}}; got {gs!r}"
                )
            for gate, status in gs.items():
                if gate not in SCENARIO_GATES:
                    raise ScenarioError(
                        f"Scenario {path}: gate_status names gate {gate!r}, which a "
                        f"scenario run never evaluates. Gates a scenario runs: "
                        f"{list(SCENARIO_GATES)} (a scenario starts no engine "
                        "session, so the lineage/sandbox/explain gates never run)."
                    )
                if status not in GATE_STATUSES:
                    raise ScenarioError(
                        f"Scenario {path}: gate_status[{gate!r}]={status!r} is not a "
                        f"gate status. Legal statuses: {list(GATE_STATUSES)}."
                    )

    domains_raw = raw.get("domains", []) or []
    if isinstance(domains_raw, str):
        domains_raw = [domains_raw]
    if not isinstance(domains_raw, list):
        raise ScenarioError(
            f"Scenario {path}: 'domains' must be a list of "
            f"{list(SCENARIO_DOMAINS)}; got {domains_raw!r}"
        )
    unknown_domains = sorted(d for d in domains_raw if d not in SCENARIO_DOMAINS)
    if unknown_domains:
        raise ScenarioError(
            f"Scenario {path}: unknown domains(s) {unknown_domains}. "
            f"Known domains: {list(SCENARIO_DOMAINS)}."
        )

    return AqScenario(
        id=raw["id"],
        description=raw.get("description", ""),
        blueprint=raw.get("blueprint", ""),
        inject_failure=inject_failure,
        expected_patch=expected_patch,
        assertions=assertions,
        source_path=path.resolve(),
        domains=tuple(domains_raw),
    )


#: Engine a scenario simulates when `inject_failure.engine` is absent. Spark,
#: matching `compiler.compile`'s and `executor.get_executor`'s own defaults —
#: an existing scenario file that never mentioned an engine described a Spark
#: failure, and must keep doing so.
DEFAULT_SCENARIO_ENGINE = "spark"


def scenario_engine(scenario: AqScenario) -> str:
    """The engine whose run *scenario* simulates.

    One accessor rather than three `inject_failure.get("engine", …)` reads,
    because the value has to reach three places that must not disagree: the
    compile behind the `FailureContext`, the compile behind the apply check,
    and the `FailureContext.engine` field that selects the engine's prompt
    rules and config allowlist.
    """
    engine = scenario.inject_failure.get("engine") if scenario.inject_failure else None
    return str(engine) if engine else DEFAULT_SCENARIO_ENGINE


# ── Result model ──────────────────────────────────────────────────────────────


@dataclass
class ScenarioResult:
    scenario_id: str
    model: str
    passed: bool
    patch_valid: bool  # PatchSpec parsed without error
    patch_applies: bool  # patch can be applied to blueprint
    failures: list[str]  # GATING failures only (correctness) — these flip passed
    patch: Any  # PatchSpec | None
    duration_seconds: float
    confidence: float | None = None
    attempts_to_parse: int = 0  # LLM calls made (1=first try, >1=reprompts needed, 0=API error)
    reprompt_errors: list[str] = field(default_factory=list)  # validation error per failed attempt
    root_cause_match: bool | None = None  # None = assertion not configured
    category_match: bool | None = None  # None = assertion not configured
    soft_failures: list[str] = field(
        default_factory=list
    )  # quality misses — reported, NEVER flip passed
    diag_score: float | None = (
        None  # fraction of configured diagnosis signals hit; None = none configured
    )
    # Persistence + regression detection
    prompt_version: str | None = (
        None  # agent.PROMPT_VERSION at time of run; carried into benchmark_results
    )
    provider: str | None = None  # LLM provider used (anthropic | openai_compat)
    base_url: str | None = None  # LLM endpoint base_url (may be None for hosted providers)
    # Guardrail compliance chain.
    # None when scenario blueprint declares no agent.guardrails (excluded from
    # guardrail-clean rate); [] when defined-and-clean; non-empty when violated.
    violated_guardrails: list[str] | None = None
    # Benchmark ↔ production parity. ``stop_reason`` records which
    # BudgetConfig axis terminated the heal loop. Same vocabulary production
    # uses (solved, exhausted_attempts, stuck_signature, etc. — see
    # agent.budget.STOP_REASONS). Persisted to benchmark_results so leaderboard
    # consumers can distinguish "model gave up" from "ran out of attempts".
    stop_reason: str | None = None
    escalated: bool = False  # stuck-signature escalation was applied
    tokens_in_total: int = 0
    tokens_out_total: int = 0
    #: Why the apply refused this patch (`REFUSAL_REASONS`), or None when it
    #: applied / no apply was attempted. Distinct from `patch_applies` on
    #: purpose — see the "Refusal vocabulary" comment above.
    refusal: str | None = None
    #: Gate 1's effective-engine-config gate status for this patch
    #: (`GateStatus`), or None when no apply was attempted.
    engine_config_gate: str | None = None

    @property
    def diag_correct(self) -> bool | None:
        """True if ANY diagnostic signal passed (root_cause OR category).

        None when neither assertion was configured in the scenario — excluded
        from diag-only rate calculations.
        """
        signals = [s for s in (self.root_cause_match, self.category_match) if s is not None]
        if not signals:
            return None
        return any(signals)


# ── Failure context builder ───────────────────────────────────────────────────


def _build_failure_ctx(
    scenario: AqScenario,
) -> tuple[Any, Any, Any]:  # (FailureContext, Blueprint, Manifest)
    """Build a synthetic FailureContext + return parsed Blueprint + compiled Manifest.

    Returns the Blueprint alongside the FailureContext so callers can extract
    ``agent.guardrails`` (Phase 33 Part B Scope C step 2 — scenario guardrail
    enforcement) without re-parsing the blueprint a second time. The compiled
    Manifest (Phase 75) lets ``run_scenario`` build a ToolBox for
    ``agent.mode: agentic`` scenario runs without a third parse/compile pass.
    """
    from datetime import datetime

    from aqueduct.surveyor.models import FailureContext

    blueprint_path = (scenario.source_path.parent / scenario.blueprint).resolve()
    if not blueprint_path.exists():
        raise FileNotFoundError(
            f"Scenario {scenario.id!r}: blueprint not found at {blueprint_path}"
        )

    # Parse + compile to get a real manifest (no Spark needed)
    from aqueduct.compiler.compiler import compile as compiler_compile
    from aqueduct.parser.parser import parse

    inj = scenario.inject_failure
    engine = scenario_engine(scenario)

    bp = parse(str(blueprint_path))
    manifest = compiler_compile(bp, blueprint_path=blueprint_path, engine=engine)
    manifest_json = json.dumps(manifest.to_dict())

    now = datetime.now(tz=UTC).isoformat()

    # Optional `structured:` block lets a scenario carry the same
    # high-fidelity error fields that production extracts from
    # PySparkException/Py4JJavaError, so benchmark and production exercise
    # the identical prompt-builder branch. Legacy scenarios with no block
    # fall through and FailureContext stays in legacy stack-trace mode.
    structured = inj.get("structured") or {}
    if not isinstance(structured, dict):
        structured = {}
    sug = structured.get("suggested_columns") or ()
    if isinstance(sug, str):
        sug = (sug,)

    ctx = FailureContext(
        run_id=f"scenario-{scenario.id}",
        blueprint_id=manifest.blueprint_id,
        failed_module=inj.get("module", "_executor"),
        error_message=inj.get("error_message", "Simulated failure"),
        stack_trace=inj.get("stack_trace"),
        manifest_json=manifest_json,
        started_at=now,
        finished_at=now,
        blueprint_source_yaml=blueprint_path.read_text(encoding="utf-8"),
        error_class=structured.get("error_class"),
        root_exception=structured.get("root_exception"),
        sql_state=structured.get("sql_state"),
        suggested_columns=tuple(str(c) for c in sug),
        object_name=structured.get("object_name"),
        # `inject_failure.engine` (default "spark", see `scenario_engine`) —
        # the engine whose run this scenario simulates. It is NOT cosmetic:
        # it selects the engine's `PromptRules` pack and the
        # `engine_config_allowlist.yml` rendered into the healing prompt, so a
        # DuckDB scenario left on the "spark" default would show the model
        # Spark's `spark.*` allowlist and could never produce the DuckDB
        # config write it is testing for.
        engine=engine,
    )
    return ctx, bp, manifest


# ── Effect-based grader (Phase 33 Part B Scope C) ────────────────────────────
#
# Old behavior (deleted): `_check_expected_patch` compared patch OPS by op-name
# equality + substring-on-value. Marked valid alternative ops as FAIL — e.g. a
# `replace_module_config` was rejected when scenario pinned `set_module_config_key`
# even when the resulting blueprint was identical.
#
# New behavior: grade the EFFECT of the patch — does the post-patch blueprint's
# target module have the expected config values? SQL fields normalized via
# sqlglot AST so whitespace / quote / case differences don't trip false fails.

# Keys whose values are SQL strings and should be compared AST-normalized
# rather than as raw text. Extendable when new SQL-typed config keys land.
_SQL_TYPED_KEYS = ("query", "sql")


def _normalize_sql(text: str) -> str:
    """Return an AST-normalized canonical form of a SQL string.

    Uses sqlglot — already a hard dep (see CLAUDE.md: never write a custom SQL
    parser). Whitespace, quoting, alias-case differences collapse to the same
    canonical SQL so substring matches work regardless of formatting.

    Falls back to lowercased whitespace-collapsed text when sqlglot cannot
    parse the input (LLMs occasionally emit dialect-specific oddities) —
    matches the old string-substring behaviour rather than failing the whole
    assertion on a parse error.
    """
    try:
        import sqlglot

        parsed = sqlglot.parse_one(text)
        return parsed.sql()
    except Exception:
        return " ".join(text.lower().split())


def validate_expected_patch(expected: dict[str, Any]) -> list[str]:
    """Structural validation of an ``expected_patch`` block. Pure.

    Returns a list of shape errors (empty = the block is well formed). Called
    from BOTH ``load_scenario`` (which raises ``ScenarioError``, so
    ``aqueduct doctor --aqscenario`` catches a bad shape before any LLM call)
    and ``_check_expected_effect`` (which reports them as gating failures, so
    a programmatically-built expectation is held to the same contract). One
    validator, two call sites: two copies of "what is a legal effect block"
    would drift.

    The rule that matters: **an effect block must state at least one
    expectation.** A block carrying only, say, a ``config_contains`` typo
    grades nothing and therefore passes for free — the silent no-op this
    format exists to catch, and the exact reason a domain-2 scenario could
    pass while its patch touched no module at all.
    """
    errors: list[str] = []
    if not expected:
        return errors
    if not isinstance(expected, dict):
        return [f"expected_patch: must be a mapping, got {type(expected).__name__}"]

    # `ops:`/`forbidden_ops:` were deleted in Phase 33 Part B Scope C. They are
    # unknown keys like any other now, but they get their own message because
    # the fix is a migration, not a spelling correction.
    legacy = sorted(k for k in ("ops", "forbidden_ops") if k in expected)
    if legacy:
        errors.append(
            f"expected_patch: scenario uses the deleted `ops:`/`forbidden_ops:` "
            f"syntax (found {legacy}). Migrate to `expected_patch.effect:`."
        )
    unknown = sorted(k for k in expected if k not in _EXPECTED_PATCH_KEYS and k not in legacy)
    if unknown:
        errors.append(
            f"expected_patch: unknown key(s) {unknown}. "
            f"Known keys: {sorted(_EXPECTED_PATCH_KEYS)}."
        )
    if errors:
        return errors

    effect = expected.get("effect")
    if effect is None:
        return errors
    return _validate_effect(effect, "expected_patch.effect")


def _validate_effect(effect: Any, where: str) -> list[str]:
    """Shape-check one effect block (recursing through ``any_of``)."""
    if not isinstance(effect, dict):
        return [f"{where}: must be a mapping, got {type(effect).__name__}"]

    unknown = sorted(k for k in effect if k not in _EFFECT_KEYS)
    if unknown:
        return [f"{where}: unknown key(s) {unknown}. Known keys: {sorted(_EFFECT_KEYS)}."]

    if "config_contains" in effect and not effect.get("module"):
        return [
            f"{where}: 'module' is required when 'config_contains' is given "
            "(config_contains grades one named module's config)"
        ]

    if not (_EFFECT_EXPECTATION_KEYS & set(effect)):
        return [
            f"{where}: states no expectation. At least one of "
            f"{sorted(_EFFECT_EXPECTATION_KEYS)} is required — an expectation "
            "block that expects nothing passes for free and verifies nothing."
        ]

    errors: list[str] = []
    for key in ("config_contains", "engine_config", "modules_contain"):
        value = effect.get(key)
        if value is not None and not isinstance(value, dict):
            errors.append(f"{where}.{key}: must be a mapping, got {type(value).__name__}")

    changed = effect.get("engine_config_changed")
    if changed is not None:
        if not isinstance(changed, dict):
            errors.append(
                f"{where}.engine_config_changed: must be a mapping of "
                f"{{engine: [key, ...]}}, got {type(changed).__name__}"
            )
        else:
            for engine, keys in changed.items():
                if not isinstance(keys, list) or not keys:
                    errors.append(
                        f"{where}.engine_config_changed[{engine!r}]: must be a "
                        f"non-empty list of config keys, got {keys!r}"
                    )

    any_of = effect.get("any_of")
    if any_of is not None:
        if not isinstance(any_of, list) or not any_of:
            errors.append(
                f"{where}.any_of: must be a non-empty list of effect blocks, got {any_of!r}"
            )
        else:
            for i, alt in enumerate(any_of):
                errors.extend(_validate_effect(alt, f"{where}.any_of[{i}]"))

    return errors


def _compare_config_values(
    where: str,
    expected_config: dict[str, Any],
    actual_config: dict[str, Any],
    *,
    sql_aware: bool,
) -> list[str]:
    """Compare an expected ``{key: value}`` map against an actual config map.

    ``sql_aware`` selects the STRING rule: SQL-normalised substring for
    ``_SQL_TYPED_KEYS`` and raw substring otherwise (module config), versus
    canonical equality (engine config — see the module docstring for why
    substring is wrong there). Booleans and numbers are strict, type-checked
    equality either way.
    """
    failures: list[str] = []
    for key, expected_val in expected_config.items():
        actual_val = actual_config.get(key)
        if actual_val is None:
            failures.append(
                f"{where}[{key!r}]: key not present in patched config "
                f"(keys: {sorted(actual_config.keys())})"
            )
            continue

        if not sql_aware:
            # Engine config: every value reaches the session as a string, so
            # `200` and `"200"` are the same setting — `canonical_config_value`
            # is the SAME normalisation Gate 1 and `patch revert` use, so the
            # three cannot disagree about whether a value changed.
            from aqueduct.patch.config_delta import canonical_config_value

            if canonical_config_value(actual_val) != canonical_config_value(expected_val):
                failures.append(f"{where}[{key!r}]: expected {expected_val!r}, got {actual_val!r}")
            continue

        # Booleans / numbers → strict equality, type-checked. `isinstance(True, int)`
        # is True in Python, so a naive `isinstance(x, (bool, int, float))` plus a
        # loose `!=` lets `True == 1` and `False == 0` silently satisfy the wrong
        # expectation, AND (a distinct, more severe bug fixed in the same pass)
        # `isinstance(x, (bool, int, float)) and isinstance(x, bool) is not False`
        # reduces to `isinstance(x, bool)` — a genuine int/float `expected_val`
        # (the overwhelmingly common case) never entered this branch at all and
        # silently fell through to the substring path below, where e.g.
        # `config_contains: {retries: 1}` passed against an ACTUAL of 11, 21, or
        # 100001 (any value whose string form contains "1"). Booleans and
        # numbers are now handled as two explicit, type-checked branches.
        if isinstance(expected_val, bool):
            if not isinstance(actual_val, bool) or actual_val != expected_val:
                failures.append(
                    f"{where}[{key!r}]: "
                    f"expected bool {expected_val!r}, got {actual_val!r} "
                    f"({type(actual_val).__name__})"
                )
            continue
        if isinstance(expected_val, (int, float)):
            if (
                isinstance(actual_val, bool)
                or not isinstance(actual_val, (int, float))
                or actual_val != expected_val
            ):
                failures.append(
                    f"{where}[{key!r}]: "
                    f"expected {expected_val!r}, got {actual_val!r} "
                    f"({type(actual_val).__name__})"
                )
            continue

        # Strings → SQL-aware substring for SQL-typed keys, raw substring otherwise.
        expected_str = str(expected_val)
        actual_str = str(actual_val)
        if key in _SQL_TYPED_KEYS:
            normalized_actual = _normalize_sql(actual_str)
            normalized_expected = _normalize_sql(expected_str)
            if normalized_expected not in normalized_actual:
                failures.append(
                    f"{where}[{key!r}]: "
                    f"AST-normalized expected substring {expected_str!r} not in "
                    f"normalized actual {actual_str!r}"
                )
        else:
            if expected_str not in actual_str:
                failures.append(
                    f"{where}[{key!r}]: " f"substring {expected_str!r} not in {actual_str!r}"
                )

    return failures


def _module_matches(module: dict, constraints: dict[str, Any]) -> bool:
    """Whether one post-patch module dict satisfies a ``modules_contain`` block."""
    wanted_type = constraints.get("type")
    if wanted_type is not None and str(module.get("type")) != str(wanted_type):
        return False
    config_contains = constraints.get("config_contains") or {}
    if not isinstance(config_contains, dict):
        return False
    return not _compare_config_values(
        "modules_contain.config_contains",
        config_contains,
        module.get("config") or {},
        sql_aware=True,
    )


def _grade_effect(
    effect: dict[str, Any],
    patched_dict: dict,
    blueprint_before: dict,
    where: str,
) -> list[str]:
    """Grade ONE effect block against the post-patch Blueprint.

    ``blueprint_before`` is the pre-patch Blueprint dict, needed only by
    ``engine_config_changed`` (which is a before/after question). Both engine
    blocks are read through ``config_delta.blueprint_engine_layers`` — the same
    normaliser Gate 1 uses — so a scenario addresses Spark's free-form
    ``conf`` bag and DuckDB's typed fields with one shape.
    """
    from aqueduct.patch.config_delta import blueprint_engine_layers

    failures: list[str] = []
    modules = patched_dict.get("modules", []) or []

    module_id = effect.get("module")
    if module_id:
        target = next(
            (m for m in modules if isinstance(m, dict) and m.get("id") == module_id),
            None,
        )
        if target is None:
            failures.append(
                f"{where}.module: {module_id!r} not found in patched blueprint "
                f"(modules present: "
                f"{[m.get('id') for m in modules if isinstance(m, dict)]})"
            )
        else:
            config_contains = effect.get("config_contains") or {}
            failures.extend(
                _compare_config_values(
                    f"{where}.config_contains",
                    config_contains,
                    target.get("config") or {},
                    sql_aware=True,
                )
            )

    modules_contain = effect.get("modules_contain")
    if modules_contain:
        if not any(_module_matches(m, modules_contain) for m in modules if isinstance(m, dict)):
            failures.append(
                f"{where}.modules_contain: no module in the patched blueprint "
                f"matches {modules_contain!r} (modules present: "
                f"{[(m.get('id'), m.get('type')) for m in modules if isinstance(m, dict)]})"
            )

    engine_config = effect.get("engine_config")
    if engine_config:
        after_layers = blueprint_engine_layers(patched_dict)
        for engine, wanted in engine_config.items():
            if not isinstance(wanted, dict):
                failures.append(
                    f"{where}.engine_config[{engine!r}]: must be a mapping of "
                    f"{{key: value}}, got {type(wanted).__name__}"
                )
                continue
            failures.extend(
                _compare_config_values(
                    f"{where}.engine_config[{engine!r}]",
                    wanted,
                    after_layers.get(engine) or {},
                    sql_aware=False,
                )
            )

    changed = effect.get("engine_config_changed")
    if changed:
        from aqueduct.patch.config_delta import ABSENT, canonical_config_value

        before_layers = blueprint_engine_layers(blueprint_before)
        after_layers = blueprint_engine_layers(patched_dict)
        for engine, keys in changed.items():
            before = before_layers.get(engine) or {}
            after = after_layers.get(engine) or {}
            for key in keys:
                b = before.get(key, ABSENT)
                a = after.get(key, ABSENT)
                if canonical_config_value(b) == canonical_config_value(a):
                    failures.append(
                        f"{where}.engine_config_changed[{engine!r}]: {key!r} did not "
                        f"change (before={None if b is ABSENT else b!r}, "
                        f"after={None if a is ABSENT else a!r})"
                    )

    any_of = effect.get("any_of")
    if any_of:
        per_alt = [
            _grade_effect(alt, patched_dict, blueprint_before, f"{where}.any_of[{i}]")
            for i, alt in enumerate(any_of)
        ]
        if all(alt_failures for alt_failures in per_alt):
            joined = "; ".join(
                f"[{i}] " + " / ".join(alt_failures) for i, alt_failures in enumerate(per_alt)
            )
            failures.append(f"{where}.any_of: no alternative held. Each one's failure: {joined}")

    return failures


def _check_expected_effect(
    expected: dict[str, Any],
    patched_dict: dict | None,
    blueprint_before: dict | None = None,
    apply_error: str | None = None,
) -> list[str]:
    """Verify the post-patch blueprint matches the expected effect.

    ``expected`` is the scenario's ``expected_patch`` block; see this module's
    docstring for the full ``effect:`` grammar. Returns a list of failure
    messages (empty = OK).

    ``patched_dict`` is the post-patch blueprint dict produced by
    ``_try_apply_patch`` — None when the patch failed to apply. The per-key
    grading is skipped then (a cascade of "the module has no such key" noise
    on top of a refusal buries the real cause), but the block still fails,
    with ONE line naming the refusal: an effect stated and never graded must
    never read as an effect satisfied. It used to return ``[]`` there, so a
    scenario that stated an effect and did not also assert ``patch_applies:
    true`` scored PASS for a patch Gate 1 had refused outright — the same
    silently-ungraded-expectation family this grammar exists to close, one
    level up. Every shipped scenario with an effect also asserts
    ``patch_applies``, so this is a latent hole, not a live miscount.
    Shape errors in the expectation itself are reported regardless: a
    malformed expectation is the scenario author's bug no matter what the
    model produced.

    ``blueprint_before`` is the pre-patch Blueprint dict, required only by
    ``engine_config_changed``. ``apply_error`` is ``ApplyOutcome.error``,
    used only to name the cause in that single failure line.
    """
    shape_errors = validate_expected_patch(expected)
    if shape_errors:
        return shape_errors

    effect = expected.get("effect")
    if not effect:
        return []
    if patched_dict is None:
        return [
            "expected_patch.effect: stated, but the patch never applied, so no "
            "effect could be graded — "
            f"{apply_error or 'the apply did not succeed (no message)'}"
        ]

    return _grade_effect(effect, patched_dict, blueprint_before or {}, "expected_patch.effect")


@dataclass(frozen=True)
class ApplyOutcome:
    """Everything one apply attempt tells the grader.

    A record rather than a tuple because the four facts it carries are not
    interchangeable and three of them are ``None``-able: ``applied=False``
    with ``refusal=None`` (the apply was never attempted) is a different
    state from ``applied=False`` with ``refusal="policy"`` (Gate 1 said no),
    and a positional tuple makes those two one careless index apart. The
    grader has to tell them apart to answer ``patch_refused:`` at all.
    """

    #: The patch applied AND the patched Blueprint re-parsed + re-compiled.
    applied: bool
    #: Human-readable reason when it did not. Empty on success.
    error: str
    #: ``None`` = the Blueprint declares no ``agent.guardrails`` (guardrail
    #: compliance is N/A); ``[]`` = declared and clean; non-empty = violated.
    violated_guardrails: list[str] | None
    #: Post-patch Blueprint dict; ``None`` whenever the apply did not succeed.
    patched_dict: dict | None
    #: Which refusal reason (`REFUSAL_REASONS`) rejected it, classified by
    #: exception TYPE — never by matching a message. ``None`` on success.
    refusal: str | None = None
    #: Gate 1's effective-engine-config gate status (`GateStatus`) for this
    #: patch, or ``None`` when that gate never ran — which is the honest
    #: answer for a POLICY refusal, since the allowlist check that rejected
    #: the write runs BEFORE the delta gate and the delta was never measured.
    engine_config_gate: str | None = None
    #: The PRE-patch Blueprint dict. Carried here because
    #: ``engine_config_changed`` is a before/after question and the loader
    #: already read the file — re-reading it in the caller is a second
    #: source of truth for the same bytes.
    blueprint_before: dict | None = None


def _try_apply_patch(
    patch: Any,
    blueprint_path: Path,
    *,
    engine: str = DEFAULT_SCENARIO_ENGINE,
) -> ApplyOutcome:
    """Try applying patch to blueprint.

    ``engine`` is the engine whose run the scenario simulates
    (``scenario_engine``). It selects the capability table the post-patch
    re-compile is checked against: a DuckDB scenario left on the Spark
    default would have its patched Blueprint graded against SPARK's
    capability verdicts, so a patch that is legal on DuckDB and unsupported
    on Spark (or the reverse) would be scored against the wrong engine
    entirely.

    ``ApplyOutcome.violated_guardrails`` is:
      - ``None`` when the blueprint defines NO ``agent.guardrails`` (so
        guardrail compliance is N/A for this scenario)
      - ``[]`` when guardrails are defined and the patch satisfies all of them
      - ``[<reason>]`` (single-entry list) when at least one guardrail is
        violated — production would reject the patch here, so we surface that
        as ``success=False`` to keep the benchmark honest

    Phase 33 Part B Scope C step 2: scenarios used to bypass
    ``_check_guardrails`` (only called by ``apply_patch_file``), so benchmark
    over-reported PASS vs production. This helper closes that gap.

    ``patched_dict`` is the post-patch blueprint dict (after a successful
    apply + parse + compile). Returned so the caller can re-use it for the
    new effect-based grader without re-running the apply pipeline.

    **Refusals are classified by exception TYPE.** ``_check_guardrails`` can
    say no for four different reasons whose fixes have nothing in common — a
    denied/unlisted engine-config key (``EngineConfigPolicyError``), a config
    write that provably changes no effective config
    (``EngineConfigInertError``), a Blueprint ``agent.guardrails`` violation
    (a plain ``PatchError``), and a patched Blueprint that no longer parses
    or compiles (``ParseError``/``CompileError``). Collapsing them into one
    ``applied=False`` is what let a domain-2 scenario claim a result it never
    checked. Matching the message text instead of the type is forbidden
    (AGENTS.md), which is why the two narrow ``PatchError`` refinements exist
    in the first place.
    """
    try:
        from aqueduct.compiler.compiler import CompileError
        from aqueduct.compiler.compiler import compile as compiler_compile
        from aqueduct.parser.parser import ParseError, parse_dict
        from aqueduct.patch.apply import (
            EngineConfigInertError,
            EngineConfigPolicyError,
            PatchError,
            _check_guardrails,
            _yaml_load,
            apply_patch_to_dict,
        )

        bp_raw = _yaml_load(blueprint_path)

        # Guardrail check — None when none declared, else list (empty = clean).
        guardrails_block = (bp_raw.get("agent") or {}).get("guardrails") or {}
        has_guardrails = bool(
            guardrails_block.get("forbidden_ops")
            or guardrails_block.get("allowed_paths")
            or guardrails_block.get("heal_on_errors")
            or guardrails_block.get("never_heal_errors")
        )
        violated: list[str] | None = [] if has_guardrails else None
        # Called UNCONDITIONALLY, even for a scenario blueprint that declares
        # no `agent.guardrails`. `_check_guardrails` also enforces
        # `set_engine_config`'s core allowlist and the effective-config delta,
        # neither of which is guardrail-gated — running it only when
        # guardrails exist would let the benchmark score a config patch as
        # PASS that production refuses, which is the exact over-reporting this
        # helper was written to end. `violated` still means *guardrail*
        # violation specifically, so a non-guardrail refusal leaves it as-is.
        from aqueduct.config import load_config as _load_config

        try:
            gate = _check_guardrails(patch, bp_raw, provenance_map=None, cfg=_load_config(None))
        except EngineConfigPolicyError as exc:
            # Ordered before the bare `PatchError` clause: both refinements
            # ARE PatchErrors, so a single broad clause would swallow them
            # and re-conflate the states they exist to separate.
            return ApplyOutcome(
                applied=False,
                error=f"engine-config policy refused the write: {exc}",
                violated_guardrails=violated,
                patched_dict=None,
                refusal=REFUSAL_POLICY,
                # The delta gate runs AFTER the allowlist check, so on a
                # policy refusal it never ran and has no status to report.
                # Reporting `fail` here would claim a measurement nobody took.
                engine_config_gate=None,
                blueprint_before=bp_raw,
            )
        except EngineConfigInertError as exc:
            return ApplyOutcome(
                applied=False,
                error=f"engine-config write is inert: {exc}",
                violated_guardrails=violated,
                patched_dict=None,
                refusal=REFUSAL_INERT,
                # `EngineConfigDeltaResult` deliberately has no `fail` member
                # — the failing state is RAISED so every apply path must
                # refuse it rather than render it. A scenario is the one
                # consumer that has to render it, so the raise is mapped back
                # onto the shared gate vocabulary here, at the boundary.
                engine_config_gate=GateStatus.FAIL,
                blueprint_before=bp_raw,
            )
        except PatchError as exc:
            if has_guardrails:
                violated = [str(exc)]
            return ApplyOutcome(
                applied=False,
                error=f"guardrails violated: {exc}",
                violated_guardrails=violated,
                patched_dict=None,
                refusal=REFUSAL_GUARDRAIL,
                engine_config_gate=None,
                blueprint_before=bp_raw,
            )

        patched = apply_patch_to_dict(bp_raw, patch)

        # Parse the patched scenario dict in-memory with
        # ``base_dir`` set to the scenario blueprint's parent so relative
        # data paths (`../data/...`, `data/...`) resolve against the real
        # fixture directory, not whatever ``/tmp`` location a former
        # NamedTemporaryFile happened to land in.
        base_dir = blueprint_path.parent if blueprint_path.exists() else Path.cwd()
        try:
            bp = parse_dict(patched, base_dir=base_dir)
            compiler_compile(bp, blueprint_path=blueprint_path, engine=engine)
            return ApplyOutcome(
                applied=True,
                error="",
                violated_guardrails=violated,
                patched_dict=patched,
                refusal=None,
                engine_config_gate=gate.status,
                blueprint_before=bp_raw,
            )
        except (ParseError, CompileError) as exc:
            return ApplyOutcome(
                applied=False,
                error=str(exc),
                violated_guardrails=violated,
                patched_dict=None,
                refusal=REFUSAL_INVALID,
                # Gate 1 DID run and DID have an answer here — the patch was
                # permitted and only fell over on the re-parse — so its status
                # is real and reported, unlike the policy-refusal branch.
                engine_config_gate=gate.status,
                blueprint_before=bp_raw,
            )
    except Exception as exc:
        return ApplyOutcome(
            applied=False,
            error=str(exc),
            violated_guardrails=None,
            patched_dict=None,
        )


#: Assertions that can only be answered by actually applying the patch. Used
#: to decide whether ``_check_assertions`` needs an ``ApplyOutcome`` at all —
#: a scenario asserting none of them costs no apply.
_APPLY_DEPENDENT_ASSERTIONS = frozenset({"patch_applies", "patch_refused", "gate_status"})

#: ``gate_status:`` gate name -> where that gate's status lives on an
#: ``ApplyOutcome``. A dict rather than an if/elif so adding a gate is one
#: line here plus one in ``SCENARIO_GATES``, and so the two cannot drift into
#: naming different gate sets (a name in one and not the other is caught by
#: ``tests/test_surveyor/test_scenario.py``).
_GATE_STATUS_READERS = {
    "engine_config": lambda outcome: outcome.engine_config_gate,
}


def _check_assertions(
    assertions: list[dict[str, Any]],
    patch: Any,  # PatchSpec | None
    blueprint_path: Path | None,
    attempts: int = 0,
    apply_outcome: ApplyOutcome | None = None,
) -> tuple[
    list[str], list[str], bool, bool, bool | None, bool | None, list[str] | None, dict | None
]:
    """Evaluate assertion list, split into gating vs scoring.

    Returns (hard_failures, soft_failures, patch_valid, patch_applies,
    root_cause_match, category_match, violated_guardrails, patched_dict).

    `violated_guardrails` is None when the scenario blueprint declares no
    guardrails (excluded from guardrail-clean rate), `[]` when defined-and-
    clean, non-empty when defined-and-violated. `patched_dict` is the post-
    patch blueprint dict — None when apply failed, available when the new
    effect-based grader needs to inspect the result.

    Gating (correctness — flips PASS/FAIL): `patch_is_valid`,
    `patch_applies`, `patch_refused`, `gate_status`. Scoring (quality —
    recorded, NEVER flips PASS/FAIL): `root_cause_contains`,
    `expected_category`, `max_attempts`, `min_confidence`. A correct fix with
    imperfect diagnosis still PASSes; the soft misses are reported and rolled
    into the diagnosis score. root_cause_match / category_match are None when
    not configured.

    ``apply_outcome`` is the ALREADY-COMPUTED result of applying ``patch``
    (``run_scenario`` resolves it once so the effect grader and the refusal
    assertions read the same attempt). When omitted, one is computed here on
    demand — but only if some assertion actually needs it
    (``_APPLY_DEPENDENT_ASSERTIONS``), so a diagnosis-only scenario still
    costs no apply.
    """
    failures: list[str] = []  # gating (correctness)
    soft_failures: list[str] = []  # scoring (quality, non-gating)
    patch_valid = patch is not None
    patch_applies = False
    violated_guardrails: list[str] | None = None  # None = scenario blueprint has no guardrails
    patched_dict: dict | None = None  # post-patch dict reused by the effect grader
    root_cause_match: bool | None = None
    category_match: bool | None = None

    # ── Resolve the apply ONCE ────────────────────────────────────────────
    # Three assertions ask about the same event from different angles, and
    # each used to be free to run its own apply (only `patch_applies` ever
    # did). Two applies of one patch are two chances to disagree about
    # whether it applied.
    outcome = apply_outcome
    if (
        outcome is None
        and patch is not None
        and blueprint_path is not None
        and blueprint_path.exists()
        and any(key in _APPLY_DEPENDENT_ASSERTIONS for a in assertions for key in a)
    ):
        outcome = _try_apply_patch(patch, blueprint_path)
    if outcome is not None:
        patch_applies = outcome.applied
        violated_guardrails = outcome.violated_guardrails
        patched_dict = outcome.patched_dict

    # Phase 41: detect defer_to_human in the patch
    did_defer = patch is not None and any(
        getattr(op, "op", None) == "defer_to_human" for op in (patch.operations or [])
    )
    allow_defer = any(a.get("allow_defer") is True for a in assertions)

    for assertion in assertions:
        if "patch_is_valid" in assertion:
            expected_val = bool(assertion["patch_is_valid"])
            if did_defer:
                # defer_to_human means the model gave up — this is a gating
                # failure unless the scenario explicitly allows deferral.
                if not allow_defer:
                    failures.append(
                        "patch_is_valid: LLM deferred to human "
                        "(add allow_defer: true to accept deferral)"
                    )
                elif not expected_val:
                    failures.append(
                        "patch_is_valid: expected invalid patch but LLM deferred "
                        "(which is valid under allow_defer)"
                    )
            elif expected_val and not patch_valid:
                failures.append(
                    "patch_is_valid: patch is None (LLM failed to produce valid PatchSpec)"
                )
            elif not expected_val and patch_valid:
                failures.append("patch_is_valid: expected invalid patch but got a valid one")

        if "allow_defer" in assertion:
            expected_defer = bool(assertion["allow_defer"])
            if expected_defer and not did_defer:
                failures.append(
                    "allow_defer: expected defer_to_human but LLM produced a regular patch"
                )
            elif not expected_defer and did_defer:
                failures.append("allow_defer: LLM deferred when a fix was expected")

        if "patch_applies" in assertion:
            expected_val = bool(assertion["patch_applies"])
            if patch is None:
                if expected_val:
                    failures.append("patch_applies: cannot check — patch is None")
            elif outcome is None:
                logger.warning("patch_applies assertion: blueprint path not found; skipped")
            elif expected_val and not outcome.applied:
                failures.append(f"patch_applies: patch failed to apply: {outcome.error}")
            elif not expected_val and outcome.applied:
                failures.append("patch_applies: expected patch to fail but it applied successfully")

        if "patch_refused" in assertion:
            # A REASON, not a boolean. `patch_applies: false` says only "it
            # did not apply" and covers four outcomes with four different
            # fixes; this says which one, and is checked independently of
            # `patch_applies` so a scenario stating both cannot have one of
            # them silently satisfied by the other.
            expected_reason = str(assertion["patch_refused"])
            if patch is None:
                failures.append(
                    f"patch_refused: cannot check — patch is None "
                    f"(nothing was submitted for {expected_reason!r} to refuse)"
                )
            elif outcome is None:
                logger.warning("patch_refused assertion: blueprint path not found; skipped")
            elif outcome.refusal is None:
                failures.append(
                    f"patch_refused: expected the patch to be refused "
                    f"({expected_reason!r}) but it applied cleanly"
                )
            elif outcome.refusal != expected_reason:
                failures.append(
                    f"patch_refused: expected refusal {expected_reason!r}, got "
                    f"{outcome.refusal!r} ({outcome.error})"
                )

        if "gate_status" in assertion:
            for gate, expected_status in assertion["gate_status"].items():
                # `load_scenario` already refused an unknown gate/status, so
                # the reader lookup below cannot miss.
                reader = _GATE_STATUS_READERS[gate]
                if patch is None:
                    failures.append(f"gate_status[{gate}]: cannot check — patch is None")
                elif outcome is None:
                    logger.warning("gate_status assertion: blueprint path not found; skipped")
                    continue
                else:
                    actual_status = reader(outcome)
                    if actual_status is None:
                        failures.append(
                            f"gate_status[{gate}]: expected {expected_status!r} but the "
                            f"gate never ran on this patch, so it has no status "
                            f"({outcome.error or 'the patch was applied'}). A gate that "
                            "did not run has no verdict to assert — reporting one would "
                            "be claiming a measurement nobody took."
                        )
                    elif actual_status != expected_status:
                        failures.append(
                            f"gate_status[{gate}]: expected {expected_status!r}, "
                            f"got {actual_status!r}"
                        )

        if "max_attempts" in assertion:
            max_att = int(assertion["max_attempts"])
            if attempts > max_att:
                soft_failures.append(
                    f"max_attempts: took {attempts} LLM call(s), max allowed {max_att} "
                    f"(reprompts needed → LLM needed schema correction)"
                )

        if "min_confidence" in assertion:
            min_conf = float(assertion["min_confidence"])
            actual_conf = patch.confidence if patch else None
            if actual_conf is None:
                soft_failures.append(
                    f"min_confidence: patch has no confidence field (expected >= {min_conf})"
                )
            elif actual_conf < min_conf:
                soft_failures.append(f"min_confidence: {actual_conf:.2f} < {min_conf:.2f}")

        if "expected_category" in assertion:
            expected_cat = str(assertion["expected_category"])
            actual_cat = patch.category if patch else None
            category_match = actual_cat == expected_cat
            if not category_match:
                soft_failures.append(
                    f"expected_category: expected {expected_cat!r}, got {actual_cat!r}"
                )

        if "root_cause_contains" in assertion:
            raw = assertion["root_cause_contains"]
            keywords = [k.lower() for k in raw] if isinstance(raw, list) else [str(raw).lower()]
            actual_rc = (patch.root_cause or "").lower() if patch else ""
            root_cause_match = any(kw in actual_rc for kw in keywords)
            if not root_cause_match:
                soft_failures.append(
                    f"root_cause_contains: none of {keywords!r} found in {actual_rc!r}"
                )

    return (
        failures,
        soft_failures,
        patch_valid,
        patch_applies,
        root_cause_match,
        category_match,
        violated_guardrails,
        patched_dict,
    )


# ── Public API ─────────────────────────────────────────────────────────────────


def run_scenario(
    scenario: AqScenario,
    model: str,
    patches_dir: Path,
    provider: str = "anthropic",
    base_url: str | None = None,
    provider_options: dict[str, Any] | None = None,
    timeout: float = 120.0,
    max_reprompts: int = 3,
    engine_prompt_context: str | None = None,
    budget: Any = None,  # BudgetConfig | None — Phase 34
    mode: str = "oneshot",
    max_tool_calls: int = 8,
    supports_tools: bool | str = "auto",
) -> ScenarioResult:
    """Run one scenario against the LLM and validate the response.

    No Spark session required — builds a FailureContext by compiling the
    referenced blueprint, injects the failure, and calls the LLM.

    Phase 34 (#7 — benchmark = production parity): when ``budget`` is
    supplied, scenario runs use the SAME BudgetConfig + escalation policy
    that production heal uses. When None, falls back to a budget synthesized
    from ``max_reprompts`` (preserves pre-Phase-34 behaviour). The scenario
    also installs an ``apply_callback`` so apply-gate rejections (guardrail
    violation, parse/compile failure on the patched blueprint) feed back
    into the same reprompt loop — closing the leaderboard-cheating path
    where benchmark would silently pass on a patch production would reject.

    Phase 75 (minimal agentic plumbing): ``mode="agentic"`` builds a ToolBox
    from the compiled Manifest so a live A/B (oneshot vs agentic) is
    possible via ``aqueduct benchmark``. No Spark session is ever started
    for a scenario run — session-bound tools (``get_source_schema``,
    ``sample_rows``) report "unavailable", same as `aqueduct heal`.
    """
    from aqueduct.agent import PROMPT_VERSION, generate_agent_patch

    t0 = time.monotonic()

    # Build failure context
    try:
        failure_ctx, bp, scenario_manifest = _build_failure_ctx(scenario)
    except Exception as exc:
        return ScenarioResult(
            scenario_id=scenario.id,
            model=model,
            passed=False,
            patch_valid=False,
            patch_applies=False,
            failures=[f"Failed to build FailureContext: {exc}"],
            patch=None,
            duration_seconds=time.monotonic() - t0,
            prompt_version=PROMPT_VERSION,
            provider=provider,
            base_url=base_url,
        )

    # Step 1 — surface the blueprint's agent.guardrails and allow_defer to
    # the LLM so the model has a chance to satisfy them on the first attempt
    # instead of producing a patch production then post-hoc rejects.
    bp_guardrails = bp.agent.guardrails if (bp and bp.agent) else None
    bp_allow_defer = bp.agent.allow_defer if (bp and bp.agent) else False

    # Resolve blueprint path eagerly so the apply_callback can reuse it.
    blueprint_path: Path | None = None
    if scenario.blueprint:
        bp_candidate = (scenario.source_path.parent / scenario.blueprint).resolve()
        if bp_candidate.exists():
            blueprint_path = bp_candidate

    apply_cb: Any = None
    if blueprint_path is not None:

        def apply_cb(
            patch_spec: Any,
            _bp_path: Path = blueprint_path,
            _engine: str = scenario_engine(scenario),
        ) -> tuple:
            outcome = _try_apply_patch(patch_spec, _bp_path, engine=_engine)
            if outcome.applied:
                return True, None, None, None
            err_class = (
                "guardrail_violation" if outcome.refusal == REFUSAL_GUARDRAIL else "compile_error"
            )
            return False, err_class, outcome.error or "(no message)", None

    _toolbox: Any = None
    if mode == "agentic":
        from aqueduct.agent.toolbox import ToolBox

        _toolbox = ToolBox(
            manifest=scenario_manifest,
            failure_ctx=failure_ctx,
            spark_session=None,  # scenarios never start Spark
            engine=failure_ctx.engine,
        )

    # Call LLM through the unified Phase 34 loop.
    agent_result = generate_agent_patch(
        failure_ctx,
        model=model,
        patches_dir=patches_dir,
        provider=provider,
        base_url=base_url,
        provider_options=provider_options,
        timeout=timeout,
        max_reprompts=max_reprompts,
        engine_prompt_context=engine_prompt_context,
        guardrails=bp_guardrails,
        budget=budget,
        allow_defer=bp_allow_defer,
        deep_loop=False,  # scenarios don't use deep_loop
        model_cascade_position=None,  # scenarios don't use cascade
        apply_callback=apply_cb,
        toolbox=_toolbox,
        mode=mode,
        max_tool_calls=max_tool_calls,
        supports_tools=supports_tools,
    )
    patch = agent_result.patch

    duration = time.monotonic() - t0

    # Apply ONCE, here, for every consumer below. Previously the apply only
    # happened as a side effect of a `patch_applies:` assertion, so a
    # scenario that stated an `expected_patch.effect` but no `patch_applies`
    # graded its effect against `patched_dict=None` — i.e. graded nothing,
    # silently, and passed. Resolving it up front makes the effect grader,
    # the refusal assertions and the gate assertions all read one attempt.
    apply_outcome: ApplyOutcome | None = None
    if patch is not None and blueprint_path is not None:
        apply_outcome = _try_apply_patch(patch, blueprint_path, engine=scenario_engine(scenario))

    # Check assertions — gating (correctness) vs soft (quality)
    # blueprint_path already resolved above; reused for _check_assertions.
    (
        hard_failures,
        soft_failures,
        patch_valid,
        patch_applies,
        root_cause_match,
        category_match,
        violated_guardrails,
        patched_dict,
    ) = _check_assertions(
        scenario.assertions,
        patch,
        blueprint_path,
        attempts=agent_result.attempts,
        apply_outcome=apply_outcome,
    )

    # expected_patch is a correctness/effect check → gating. Effect-based
    # grader inspects the POST-PATCH blueprint (patched_dict) rather than
    # comparing op-name equality on the raw patch — see _check_expected_effect.
    # The PRE-patch dict goes with it: `engine_config_changed` is a
    # before/after question and grading it against an empty "before" would
    # call every present key a change.
    expected_failures: list[str] = []
    if patch is not None and scenario.expected_patch:
        expected_failures = _check_expected_effect(
            scenario.expected_patch,
            patched_dict,
            apply_outcome.blueprint_before if apply_outcome is not None else None,
            apply_error=(apply_outcome.error if apply_outcome is not None else None),
        )

    gating_failures = hard_failures + expected_failures
    passed = len(gating_failures) == 0  # diagnosis quality NEVER flips this

    # Diagnosis score: fraction of configured diagnosis signals that hit
    # (None when the scenario configures neither root_cause nor category).
    diag_signals = [s for s in (root_cause_match, category_match) if s is not None]
    diag_score = (sum(diag_signals) / len(diag_signals)) if diag_signals else None

    return ScenarioResult(
        scenario_id=scenario.id,
        model=model,
        passed=passed,
        patch_valid=patch_valid,
        patch_applies=patch_applies,
        failures=gating_failures,
        soft_failures=soft_failures,
        diag_score=diag_score,
        patch=patch,
        duration_seconds=duration,
        confidence=patch.confidence if patch else None,
        attempts_to_parse=agent_result.attempts,
        reprompt_errors=agent_result.reprompt_errors,
        root_cause_match=root_cause_match,
        category_match=category_match,
        prompt_version=PROMPT_VERSION,
        provider=provider,
        base_url=base_url,
        violated_guardrails=violated_guardrails,
        stop_reason=agent_result.stop_reason,
        escalated=agent_result.escalated,
        tokens_in_total=agent_result.tokens_in_total,
        tokens_out_total=agent_result.tokens_out_total,
        refusal=apply_outcome.refusal if apply_outcome is not None else None,
        engine_config_gate=(
            apply_outcome.engine_config_gate if apply_outcome is not None else None
        ),
    )


@dataclass(frozen=True)
class ScenarioSelection:
    """What ``select_scenarios`` found, and everything it left behind.

    Three "not selected" reasons, kept apart rather than summed into one
    count, because they need three different responses from the user: fix
    the file, widen ``--domain``, or declare ``domains:`` on the scenario.
    A single "12 of 15 scenarios" line hides which.
    """

    #: Scenarios that will actually run.
    scenarios: tuple[AqScenario, ...]
    #: ``"<path>: <error>"`` per file that failed to load. NOT silently
    #: dropped — a malformed scenario that vanishes from a suite makes the
    #: suite look smaller rather than broken.
    load_errors: tuple[str, ...] = ()
    #: IDs of scenarios that declare ``domains:`` none of which was selected.
    #: The ordinary, expected exclusion.
    filtered_out: tuple[str, ...] = ()
    #: IDs of scenarios that declare NO ``domains:`` at all. Excluded by any
    #: ``--domain`` filter, because a scenario that states no domain cannot
    #: truthfully be claimed to be in one — but reported by ID rather than
    #: dropped in silence, since the fix is a one-line edit to the file and
    #: the failure mode otherwise is a suite that quietly shrinks.
    undeclared: tuple[str, ...] = ()


def select_scenarios(
    scenarios_dir: Path,
    domains: Sequence[str] = (),
) -> ScenarioSelection:
    """Load every scenario under *scenarios_dir*, optionally domain-filtered.

    *scenarios_dir* may be a single ``.aqscenario.yml`` file or a directory
    searched recursively — the same two shapes ``aqueduct benchmark`` accepts.

    *domains* empty means no filtering at all: every loadable scenario is
    selected regardless of what it declares, which is what keeps the flag
    purely additive for suites that predate ``domains:``. A non-empty
    *domains* selects a scenario when ANY of its declared domains was asked
    for — a scenario may legitimately declare more than one, because domain
    is a property of the FIX and some failures are fixable more than one way.

    Callers are responsible for validating *domains* against
    ``SCENARIO_DOMAINS``; the CLI does it through ``click.Choice`` so an
    unknown value is a usage error naming the legal set, rather than a filter
    that silently matches nothing.
    """
    if scenarios_dir.is_file():
        scenario_files = [scenarios_dir]
    else:
        scenario_files = sorted(scenarios_dir.glob("**/*.aqscenario.yml"))

    loaded: list[AqScenario] = []
    load_errors: list[str] = []
    for spath in scenario_files:
        try:
            loaded.append(load_scenario(spath))
        except Exception as exc:
            load_errors.append(f"{spath}: {exc}")

    if not domains:
        return ScenarioSelection(scenarios=tuple(loaded), load_errors=tuple(load_errors))

    wanted = set(domains)
    selected: list[AqScenario] = []
    filtered_out: list[str] = []
    undeclared: list[str] = []
    for scenario in loaded:
        if not scenario.domains:
            undeclared.append(scenario.id)
        elif wanted & set(scenario.domains):
            selected.append(scenario)
        else:
            filtered_out.append(scenario.id)
    return ScenarioSelection(
        scenarios=tuple(selected),
        load_errors=tuple(load_errors),
        filtered_out=tuple(filtered_out),
        undeclared=tuple(undeclared),
    )


def run_benchmark(
    scenarios_dir: Path,
    models: list[str],
    patches_dir: Path,
    provider: str = "anthropic",
    base_url: str | None = None,
    provider_options: dict[str, Any] | None = None,
    timeout: float = 120.0,
    max_reprompts: int = 3,
    engine_prompt_context: str | None = None,
    workers: int = 1,
    budget: Any = None,  # BudgetConfig | None — Phase 34 parity
    mode: str = "oneshot",
    max_tool_calls: int = 8,
    supports_tools: bool | str = "auto",
    domains: Sequence[str] = (),
) -> dict[str, dict[str, ScenarioResult]]:
    """Run all scenarios in scenarios_dir against each model.

    Executes (scenario, model) pairs in parallel using a thread pool.
    Each pair is an independent LLM HTTP call — no shared state.

    Args:
        workers: Max concurrent LLM calls. Default 1 (serial). Set >1 to parallelize.
        mode: Phase 75 — ``"oneshot"`` (default) or ``"agentic"``. Plumbed
            straight through to every ``run_scenario`` call so a live A/B
            (oneshot vs agentic, same scenarios/models) is possible.
        domains: Restrict the suite to scenarios declaring at least one of
            these ``domains:`` members (``SCENARIO_DOMAINS``). Empty = no
            filtering. See ``select_scenarios``.

    Returns:
        {scenario_id: {model: ScenarioResult}}
    """
    import concurrent.futures

    selection = select_scenarios(scenarios_dir, domains)
    for message in selection.load_errors:
        logger.error("Failed to load scenario %s", message)
    if selection.undeclared:
        logger.warning(
            "--domain excluded %d scenario(s) that declare no domains: %s",
            len(selection.undeclared),
            ", ".join(selection.undeclared),
        )
    loaded = list(selection.scenarios)
    if not loaded:
        if not selection.load_errors and not selection.filtered_out and not selection.undeclared:
            logger.warning("No .aqscenario.yml files found in %s", scenarios_dir)
        return {}

    # Pre-populate result dict to maintain scenario insertion order
    results: dict[str, dict[str, ScenarioResult]] = {s.id: {} for s in loaded}

    # Iteration order: model-outer, scenario-inner. For serial runs (Ollama
    # / vLLM with workers=1) this drastically reduces weight-swap thrash —
    # the GPU keeps one model loaded across every scenario before switching.
    # Order is independent of the final table layout (driven by `results`
    # dict insertion order, not iteration order).
    pairs = [(s, m) for m in models for s in loaded]
    effective_workers = min(workers, len(pairs))
    # Serial runs get rich visual grouping (separator before, verdict after,
    # model-switch hint). Parallel runs would interleave output, so we keep
    # the legacy single-line log for those.
    serial = effective_workers == 1

    _prev_model: dict[str, str | None] = {"v": None}

    def _emit(line: str) -> None:
        """Stderr-only — separators must never pollute --format json stdout."""
        try:
            import click as _click

            _click.echo(line, err=True)
        except Exception:
            # click not importable from this context (shouldn't happen, but
            # benchmark must never crash on cosmetics); fall back to logger.
            logger.info(line)

    def _run_pair(scenario: Any, model: str) -> tuple[str, str, ScenarioResult]:
        if serial:
            if _prev_model["v"] is not None and _prev_model["v"] != model:
                _emit(
                    f"\n↻ switching models  prev={_prev_model['v']}  next={model}  "
                    f"(local servers may pause to load weights, 30-120s)"
                )
            _prev_model["v"] = model
            header = f"Scenario: {scenario.id}   Model: {model}"
            bar = "═" * max(len(header), 67)
            _emit(f"\n{bar}")
            _emit(header)
            _emit(bar)
        else:
            logger.info("Running scenario %r | model %r", scenario.id, model)

        r = run_scenario(
            scenario,
            model=model,
            patches_dir=patches_dir,
            provider=provider,
            base_url=base_url,
            provider_options=provider_options,
            timeout=timeout,
            max_reprompts=max_reprompts,
            engine_prompt_context=engine_prompt_context,
            budget=budget,
            mode=mode,
            max_tool_calls=max_tool_calls,
            supports_tools=supports_tools,
        )

        if serial:
            status = "PASS" if r.passed else "FAIL"
            conf = f"conf={r.confidence:.2f}" if r.confidence is not None else "conf=—"
            diag = f"diag={r.diag_score:.0%}" if r.diag_score is not None else "diag=—"
            _emit(
                f"└─ {status}  {conf}  {diag}  "
                f"attempts={r.attempts_to_parse}  duration={r.duration_seconds:.1f}s"
            )

        return scenario.id, model, r

    with concurrent.futures.ThreadPoolExecutor(max_workers=effective_workers) as pool:
        futures = [pool.submit(_run_pair, s, m) for s, m in pairs]
        done = 0
        for future in concurrent.futures.as_completed(futures):
            try:
                sid, model, result = future.result()
                results[sid][model] = result
                done += 1
                # Parallel mode can't emit the serial multi-line box (worker
                # threads would interleave it into garbage). Emit ONE atomic
                # progress line per completed pair from this main thread — it
                # is serialised by the loop, so lines never interleave.
                if not serial:
                    status = "PASS" if result.passed else "FAIL"
                    diag = (
                        f"diag={result.diag_score:.0%}"
                        if result.diag_score is not None
                        else "diag=—"
                    )
                    _emit(
                        f"[{done}/{len(pairs)}] {status}  {sid}  ·  {model}  "
                        f"{diag}  {result.duration_seconds:.1f}s"
                    )
            except Exception as exc:
                logger.error("Unexpected error in benchmark worker: %s", exc)

    return results


def format_benchmark_table(
    results: dict[str, dict[str, ScenarioResult]],
    models: list[str],
) -> str:
    """Render benchmark results as a terminal-friendly table."""
    if not results:
        return "(no results)"

    scenario_ids = list(results.keys())

    def _format_cell(r: ScenarioResult | None) -> str:
        if r is None:
            return "—"
        status = "PASS" if r.passed else "FAIL"
        parts: list[str] = [status]
        if r.passed and r.confidence is not None:
            parts.append(f"{r.confidence:.2f}")
        if r.diag_score is not None:
            parts.append(f"{r.diag_score:.0%}")
        parts.append(f"{r.duration_seconds:.0f}s")
        return " · ".join(parts)

    # Compute column widths AFTER pre-rendering every cell so the column
    # exactly fits the widest content (no centred padding mismatch).
    id_col_w = max(len("Scenario"), max(len(sid) for sid in scenario_ids))
    model_col_w_by_model: dict[str, int] = {}
    for m in models:
        widest = len(m)
        for sid in scenario_ids:
            widest = max(widest, len(_format_cell(results[sid].get(m))))
        model_col_w_by_model[m] = max(widest, 10)

    # Row width = id_col + "  │ " (4) + Σ model widths + " │ " (3) between each
    # of the N models = id_col + Σwidths + 3N + 1. The heavy/light rules must
    # match exactly (was +2 → one char overhang).
    total_w = id_col_w + sum(model_col_w_by_model.values()) + 3 * len(models) + 1
    h_heavy = "═" * total_w
    h_light = "─" * total_w

    header = f"{'Scenario':<{id_col_w}}  │ " + " │ ".join(
        f"{m:<{model_col_w_by_model[m]}}" for m in models
    )

    lines: list[str] = [h_heavy, header, h_heavy]

    for sid in scenario_ids:
        cells = [f"{_format_cell(results[sid].get(m)):<{model_col_w_by_model[m]}}" for m in models]
        lines.append(f"{sid:<{id_col_w}}  │ " + " │ ".join(cells))

    lines.append(h_light)

    # Summary rows
    for label, fn in [
        ("Parse rate", lambda rs: f"{sum(1 for r in rs if r.patch_valid) / len(rs):.0%}"),
        ("Apply rate", lambda rs: f"{sum(1 for r in rs if r.patch_applies) / len(rs):.0%}"),
        ("Pass rate", lambda rs: f"{sum(1 for r in rs if r.passed) / len(rs):.0%}"),
        (
            "Avg confidence",
            lambda rs: (
                f"{sum(r.confidence for r in rs if r.confidence is not None) / max(1, sum(1 for r in rs if r.confidence is not None)):.2f}"
                if any(r.confidence is not None for r in rs)
                else "—"
            ),
        ),
        (
            "Avg attempts",
            lambda rs: (
                f"{sum(r.attempts_to_parse for r in rs if r.attempts_to_parse > 0) / max(1, sum(1 for r in rs if r.attempts_to_parse > 0)):.1f}"
                if any(r.attempts_to_parse > 0 for r in rs)
                else "—"
            ),
        ),
        (
            "1-shot rate",
            lambda rs: (
                f"{sum(1 for r in rs if r.attempts_to_parse == 1) / max(1, sum(1 for r in rs if r.attempts_to_parse > 0)):.0%}"
                if any(r.attempts_to_parse > 0 for r in rs)
                else "—"
            ),
        ),
        (
            "Diag-only rate",
            lambda rs: (
                f"{sum(1 for r in rs if r.diag_correct is True and not r.patch_applies) / max(1, sum(1 for r in rs if r.diag_correct is not None)):.0%}"
                if any(r.diag_correct is not None for r in rs)
                else "—"
            ),
        ),
        (
            "Diag score",
            lambda rs: (
                f"{sum(r.diag_score for r in rs if r.diag_score is not None) / max(1, sum(1 for r in rs if r.diag_score is not None)):.0%}"
                if any(r.diag_score is not None for r in rs)
                else "—"
            ),
        ),
        # Guardrail-clean rate. N/A when no scenario in the suite declares
        # guardrails on its blueprint (violated_guardrails is None on every
        # result). Otherwise: fraction of (scenario, model) pairs with
        # violated_guardrails == [] among those where it's non-None.
        (
            "Guardrail-clean",
            lambda rs: (
                f"{sum(1 for r in rs if r.violated_guardrails == []) / max(1, sum(1 for r in rs if r.violated_guardrails is not None)):.0%}"
                if any(getattr(r, "violated_guardrails", None) is not None for r in rs)
                else "—"
            ),
        ),
    ]:
        cells = []
        for model in models:
            model_results_list = [
                results[sid][model] for sid in scenario_ids if model in results[sid]
            ]
            if model_results_list:
                cells.append(f"{fn(model_results_list):<{model_col_w_by_model[model]}}")
            else:
                cells.append(f"{'—':<{model_col_w_by_model[model]}}")
        lines.append(f"{label:<{id_col_w}}  │ " + " │ ".join(cells))

    lines.append(h_heavy)
    return "\n".join(lines)
