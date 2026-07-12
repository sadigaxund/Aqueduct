"""Scenario-based LLM benchmark — Phase 22.

A scenario YAML (.aqscenario.yml) defines a simulated failure + expected LLM
response assertions so the healing agent can be regression-tested and compared
across models without running a real Spark pipeline.

File format:
  aqueduct_scenario: "1.0"
  id: schema_drift_column_rename
  description: "Column renamed from event_ts to event_time upstream"
  blueprint: ../pipelines/orders.yml    # resolved relative to scenario file
  inject_failure:
    module: cast_and_clean              # failed_module
    error_message: "AnalysisException: Column 'event_ts' does not exist"
    stack_trace: |                      # optional
      ...
  expected_patch:
    ops:                                # ALL must match at least one generated op
      - op: set_module_config_key
        module_id: cast_and_clean
        key: query
        value_contains: "event_time"   # substring match on value field
    forbidden_ops:                      # NONE may appear in generated ops
      - replace_module_config
  assertions:
    - patch_is_valid: true             # PatchSpec parses without schema error
    - patch_applies: true              # patch can be applied to blueprint
    - max_attempts: 1                  # must succeed on first LLM call (no reprompts)
    - min_confidence: 0.8              # LLM self-reported confidence above threshold
    - expected_category: format_mismatch  # LLM must classify the failure correctly
    - root_cause_contains: "format"    # root_cause field must contain this keyword
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


# ── Scenario model ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class AqScenario:
    """Parsed .aqscenario.yml file."""
    id: str
    description: str
    blueprint: str                  # path relative to scenario file
    inject_failure: dict[str, Any]
    expected_patch: dict[str, Any]  # {ops: [...], forbidden_ops: [...]}
    assertions: list[dict[str, Any]]
    source_path: Path               # absolute path of the .aqscenario.yml file


def load_scenario(path: Path) -> AqScenario:
    """Parse a .aqscenario.yml file into an AqScenario."""
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Scenario {path} is not a valid YAML mapping")
    version = raw.get("aqueduct_scenario")
    if version not in ("1.0", 1, "1"):
        raise ValueError(
            f"Scenario {path} missing or unsupported aqueduct_scenario version: {version!r}"
        )
    if "id" not in raw:
        raise ValueError(f"Scenario {path} missing 'id'")
    if "inject_failure" not in raw:
        raise ValueError(f"Scenario {path} missing 'inject_failure'")
    return AqScenario(
        id=raw["id"],
        description=raw.get("description", ""),
        blueprint=raw.get("blueprint", ""),
        inject_failure=raw.get("inject_failure", {}),
        expected_patch=raw.get("expected_patch", {}),
        assertions=raw.get("assertions", [{"patch_is_valid": True}]),
        source_path=path.resolve(),
    )


# ── Result model ──────────────────────────────────────────────────────────────

@dataclass
class ScenarioResult:
    scenario_id: str
    model: str
    passed: bool
    patch_valid: bool             # PatchSpec parsed without error
    patch_applies: bool           # patch can be applied to blueprint
    failures: list[str]           # GATING failures only (correctness) — these flip passed
    patch: Any                    # PatchSpec | None
    duration_seconds: float
    confidence: float | None = None
    attempts_to_parse: int = 0    # LLM calls made (1=first try, >1=reprompts needed, 0=API error)
    reprompt_errors: list[str] = field(default_factory=list)  # validation error per failed attempt
    root_cause_match: bool | None = None   # None = assertion not configured
    category_match: bool | None = None     # None = assertion not configured
    soft_failures: list[str] = field(default_factory=list)  # quality misses — reported, NEVER flip passed
    diag_score: float | None = None  # fraction of configured diagnosis signals hit; None = none configured
    # Persistence + regression detection
    prompt_version: str | None = None  # agent.PROMPT_VERSION at time of run; carried into benchmark_results
    provider: str | None = None        # LLM provider used (anthropic | openai_compat)
    base_url: str | None = None        # LLM endpoint base_url (may be None for hosted providers)
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
    escalated: bool = False           # stuck-signature escalation was applied
    tokens_in_total: int = 0
    tokens_out_total: int = 0

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

def _build_failure_ctx(scenario: AqScenario) -> tuple[Any, Any, Any]:  # (FailureContext, Blueprint, Manifest)
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

    bp = parse(str(blueprint_path))
    manifest = compiler_compile(bp, blueprint_path=blueprint_path)
    manifest_json = json.dumps(manifest.to_dict())

    inj = scenario.inject_failure
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


def _check_expected_effect(
    expected: dict[str, Any],
    patched_dict: dict | None,
) -> list[str]:
    """Verify the post-patch blueprint matches the expected effect.

    ``expected`` is the scenario's ``expected_patch`` block in the new
    ``effect:`` shape::

        expected_patch:
          effect:
            module: clean_events
            config_contains:
              query: "event_time"       # SQL-typed → sqlglot-normalized substring
              header: true               # bool / number → equality
              path: "data/orders"        # other strings → raw substring

    Returns a list of failure messages (empty = OK). ``patched_dict`` is the
    post-patch blueprint dict produced by ``_try_apply_patch`` — None when the
    patch failed to apply, in which case effect grading is skipped (the
    patch_applies gate already caught it).

    Old ``ops:`` / ``forbidden_ops:`` syntax was deleted in Phase 33 Part B
    Scope C — scenarios MUST use ``effect:`` after the migration.
    """
    failures: list[str] = []
    if not expected:
        return failures

    effect = expected.get("effect")
    if not effect:
        # Legacy `ops:` block in a scenario that wasn't migrated yet. Surface
        # the migration ask as a single hard failure so the user can't miss it.
        if "ops" in expected or "forbidden_ops" in expected:
            failures.append(
                "expected_patch: scenario uses the deleted `ops:`/`forbidden_ops:` "
                "syntax. Migrate to `expected_patch.effect:` — see Phase 33 Part B "
                "Scope C in CHANGELOG."
            )
        return failures

    if not isinstance(effect, dict):
        failures.append(
            f"expected_patch.effect: must be a mapping, got {type(effect).__name__}"
        )
        return failures

    module_id = effect.get("module")
    if not module_id:
        failures.append("expected_patch.effect.module: required (target module_id)")
        return failures

    if patched_dict is None:
        # Apply failed earlier in the pipeline; effect grader skips so the user
        # sees the apply failure as the root cause, not a noisy follow-up.
        return failures

    modules = patched_dict.get("modules", []) or []
    target = next(
        (m for m in modules if isinstance(m, dict) and m.get("id") == module_id),
        None,
    )
    if target is None:
        failures.append(
            f"expected_patch.effect.module: {module_id!r} not found in patched "
            f"blueprint (modules present: {[m.get('id') for m in modules if isinstance(m, dict)]})"
        )
        return failures

    target_config = target.get("config") or {}
    config_contains = effect.get("config_contains") or {}
    if not isinstance(config_contains, dict):
        failures.append(
            f"expected_patch.effect.config_contains: must be a mapping, "
            f"got {type(config_contains).__name__}"
        )
        return failures

    for key, expected_val in config_contains.items():
        actual_val = target_config.get(key)
        if actual_val is None:
            failures.append(
                f"expected_patch.effect.config_contains[{key!r}]: key not present "
                f"in patched config (keys: {sorted(target_config.keys())})"
            )
            continue

        # Booleans / numbers → strict equality.
        if isinstance(expected_val, (bool, int, float)) and isinstance(expected_val, bool) is not False:
            # `isinstance(True, int)` is True in Python; the redundant check above keeps bools as bools.
            if actual_val != expected_val:
                failures.append(
                    f"expected_patch.effect.config_contains[{key!r}]: "
                    f"expected {expected_val!r}, got {actual_val!r}"
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
                    f"expected_patch.effect.config_contains[{key!r}]: "
                    f"AST-normalized expected substring {expected_str!r} not in "
                    f"normalized actual {actual_str!r}"
                )
        else:
            if expected_str not in actual_str:
                failures.append(
                    f"expected_patch.effect.config_contains[{key!r}]: "
                    f"substring {expected_str!r} not in {actual_str!r}"
                )

    return failures


def _try_apply_patch(patch: Any, blueprint_path: Path) -> tuple[bool, str, list[str] | None, dict | None]:
    """Try applying patch to blueprint.

    Returns (success, error_message, violated_guardrails, patched_dict).

    ``violated_guardrails`` is:
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
    """
    try:
        from aqueduct.compiler.compiler import CompileError
        from aqueduct.compiler.compiler import compile as compiler_compile
        from aqueduct.parser.parser import ParseError, parse_dict
        from aqueduct.patch.apply import (
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
        violated: list[str] | None
        if has_guardrails:
            try:
                _check_guardrails(patch, bp_raw, provenance_map=None)
                violated = []
            except PatchError as exc:
                violated = [str(exc)]
                return False, f"guardrails violated: {exc}", violated, None
        else:
            violated = None

        patched = apply_patch_to_dict(bp_raw, patch)

        # Parse the patched scenario dict in-memory with
        # ``base_dir`` set to the scenario blueprint's parent so relative
        # data paths (`../data/...`, `data/...`) resolve against the real
        # fixture directory, not whatever ``/tmp`` location a former
        # NamedTemporaryFile happened to land in.
        base_dir = blueprint_path.parent if blueprint_path.exists() else Path.cwd()
        try:
            bp = parse_dict(patched, base_dir=base_dir)
            compiler_compile(bp, blueprint_path=blueprint_path)
            return True, "", violated, patched
        except (ParseError, CompileError) as exc:
            return False, str(exc), violated, None
    except Exception as exc:
        return False, str(exc), None, None


def _check_assertions(
    assertions: list[dict[str, Any]],
    patch: Any,  # PatchSpec | None
    blueprint_path: Path | None,
    attempts: int = 0,
) -> tuple[list[str], list[str], bool, bool, bool | None, bool | None, list[str] | None, dict | None]:
    """Evaluate assertion list, split into gating vs scoring.

    Returns (hard_failures, soft_failures, patch_valid, patch_applies,
    root_cause_match, category_match, violated_guardrails, patched_dict).

    `violated_guardrails` is None when the scenario blueprint declares no
    guardrails (excluded from guardrail-clean rate), `[]` when defined-and-
    clean, non-empty when defined-and-violated. `patched_dict` is the post-
    patch blueprint dict — None when apply failed, available when the new
    effect-based grader needs to inspect the result.

    Gating (correctness — flips PASS/FAIL): `patch_is_valid`,
    `patch_applies`. Scoring (quality — recorded, NEVER flips PASS/FAIL):
    `root_cause_contains`, `expected_category`, `max_attempts`,
    `min_confidence`. A correct fix with imperfect diagnosis still PASSes;
    the soft misses are reported and rolled into the diagnosis score.
    root_cause_match / category_match are None when not configured.
    """
    failures: list[str] = []        # gating (correctness)
    soft_failures: list[str] = []   # scoring (quality, non-gating)
    patch_valid = patch is not None
    patch_applies = False
    violated_guardrails: list[str] | None = None  # None = scenario blueprint has no guardrails
    patched_dict: dict | None = None              # post-patch dict reused by the effect grader
    root_cause_match: bool | None = None
    category_match: bool | None = None

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
                failures.append("patch_is_valid: patch is None (LLM failed to produce valid PatchSpec)")
            elif not expected_val and patch_valid:
                failures.append("patch_is_valid: expected invalid patch but got a valid one")

        if "allow_defer" in assertion:
            expected_defer = bool(assertion["allow_defer"])
            if expected_defer and not did_defer:
                failures.append(
                    "allow_defer: expected defer_to_human but LLM produced a regular patch"
                )
            elif not expected_defer and did_defer:
                failures.append(
                    "allow_defer: LLM deferred when a fix was expected"
                )

        if "patch_applies" in assertion:
            expected_val = bool(assertion["patch_applies"])
            if patch is None:
                if expected_val:
                    failures.append("patch_applies: cannot check — patch is None")
            else:
                if blueprint_path and blueprint_path.exists():
                    ok, err, violated, patched = _try_apply_patch(patch, blueprint_path)
                    patch_applies = ok
                    violated_guardrails = violated
                    patched_dict = patched
                    if expected_val and not ok:
                        failures.append(f"patch_applies: patch failed to apply: {err}")
                    elif not expected_val and ok:
                        failures.append("patch_applies: expected patch to fail but it applied successfully")
                else:
                    logger.warning("patch_applies assertion: blueprint path not found; skipped")

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
                soft_failures.append(f"min_confidence: patch has no confidence field (expected >= {min_conf})")
            elif actual_conf < min_conf:
                soft_failures.append(
                    f"min_confidence: {actual_conf:.2f} < {min_conf:.2f}"
                )

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
        failures, soft_failures, patch_valid, patch_applies,
        root_cause_match, category_match, violated_guardrails, patched_dict,
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
        def apply_cb(patch_spec: Any, _bp_path: Path = blueprint_path) -> tuple:
            ok, err, violated, _patched = _try_apply_patch(patch_spec, _bp_path)
            if ok:
                return True, None, None, None
            err_class = "guardrail_violation" if violated else "compile_error"
            return False, err_class, err or "(no message)", None

    _toolbox: Any = None
    if mode == "agentic":
        from aqueduct.agent.toolbox import ToolBox
        _toolbox = ToolBox(
            manifest=scenario_manifest,
            failure_ctx=failure_ctx,
            spark_session=None,  # scenarios never start Spark
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

    # Check assertions — gating (correctness) vs soft (quality)
    # blueprint_path already resolved above; reused for _check_assertions.
    (
        hard_failures, soft_failures, patch_valid, patch_applies,
        root_cause_match, category_match, violated_guardrails, patched_dict,
    ) = _check_assertions(scenario.assertions, patch, blueprint_path, attempts=agent_result.attempts)

    # expected_patch is a correctness/effect check → gating. Effect-based
    # grader inspects the POST-PATCH blueprint (patched_dict) rather than
    # comparing op-name equality on the raw patch — see _check_expected_effect.
    expected_failures: list[str] = []
    if patch is not None and scenario.expected_patch:
        expected_failures = _check_expected_effect(scenario.expected_patch, patched_dict)

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
) -> dict[str, dict[str, ScenarioResult]]:
    """Run all scenarios in scenarios_dir against each model.

    Executes (scenario, model) pairs in parallel using a thread pool.
    Each pair is an independent LLM HTTP call — no shared state.

    Args:
        workers: Max concurrent LLM calls. Default 1 (serial). Set >1 to parallelize.
        mode: Phase 75 — ``"oneshot"`` (default) or ``"agentic"``. Plumbed
            straight through to every ``run_scenario`` call so a live A/B
            (oneshot vs agentic, same scenarios/models) is possible.

    Returns:
        {scenario_id: {model: ScenarioResult}}
    """
    import concurrent.futures

    if scenarios_dir.is_file():
        scenario_files = [scenarios_dir]
    else:
        scenario_files = sorted(scenarios_dir.glob("**/*.aqscenario.yml"))
    if not scenario_files:
        logger.warning("No .aqscenario.yml files found in %s", scenarios_dir)
        return {}

    loaded: list[Any] = []  # AqScenario list
    for spath in scenario_files:
        try:
            loaded.append(load_scenario(spath))
        except Exception as exc:
            logger.error("Failed to load scenario %s: %s", spath, exc)

    if not loaded:
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
                    diag = f"diag={result.diag_score:.0%}" if result.diag_score is not None else "diag=—"
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
        cells = [
            f"{_format_cell(results[sid].get(m)):<{model_col_w_by_model[m]}}"
            for m in models
        ]
        lines.append(f"{sid:<{id_col_w}}  │ " + " │ ".join(cells))

    lines.append(h_light)

    # Summary rows
    for label, fn in [
        ("Parse rate", lambda rs: f"{sum(1 for r in rs if r.patch_valid) / len(rs):.0%}"),
        ("Apply rate", lambda rs: f"{sum(1 for r in rs if r.patch_applies) / len(rs):.0%}"),
        ("Pass rate", lambda rs: f"{sum(1 for r in rs if r.passed) / len(rs):.0%}"),
        ("Avg confidence", lambda rs: (
            f"{sum(r.confidence for r in rs if r.confidence is not None) / max(1, sum(1 for r in rs if r.confidence is not None)):.2f}"
            if any(r.confidence is not None for r in rs) else "—"
        )),
        ("Avg attempts", lambda rs: (
            f"{sum(r.attempts_to_parse for r in rs if r.attempts_to_parse > 0) / max(1, sum(1 for r in rs if r.attempts_to_parse > 0)):.1f}"
            if any(r.attempts_to_parse > 0 for r in rs) else "—"
        )),
        ("1-shot rate", lambda rs: (
            f"{sum(1 for r in rs if r.attempts_to_parse == 1) / max(1, sum(1 for r in rs if r.attempts_to_parse > 0)):.0%}"
            if any(r.attempts_to_parse > 0 for r in rs) else "—"
        )),
        ("Diag-only rate", lambda rs: (
            f"{sum(1 for r in rs if r.diag_correct is True and not r.patch_applies) / max(1, sum(1 for r in rs if r.diag_correct is not None)):.0%}"
            if any(r.diag_correct is not None for r in rs) else "—"
        )),
        ("Diag score", lambda rs: (
            f"{sum(r.diag_score for r in rs if r.diag_score is not None) / max(1, sum(1 for r in rs if r.diag_score is not None)):.0%}"
            if any(r.diag_score is not None for r in rs) else "—"
        )),
        # Guardrail-clean rate. N/A when no scenario in the suite declares
        # guardrails on its blueprint (violated_guardrails is None on every
        # result). Otherwise: fraction of (scenario, model) pairs with
        # violated_guardrails == [] among those where it's non-None.
        ("Guardrail-clean", lambda rs: (
            f"{sum(1 for r in rs if r.violated_guardrails == []) / max(1, sum(1 for r in rs if r.violated_guardrails is not None)):.0%}"
            if any(getattr(r, 'violated_guardrails', None) is not None for r in rs) else "—"
        )),
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
