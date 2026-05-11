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
    failures: list[str]           # descriptions of failed assertions
    patch: Any                    # PatchSpec | None
    duration_seconds: float
    confidence: float | None = None
    attempts_to_parse: int = 0    # LLM calls made (1=first try, >1=reprompts needed, 0=API error)
    reprompt_errors: list[str] = field(default_factory=list)  # validation error per failed attempt
    root_cause_match: bool | None = None   # None = assertion not configured
    category_match: bool | None = None     # None = assertion not configured

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

def _build_failure_ctx(scenario: AqScenario) -> "Any":  # FailureContext
    """Build a synthetic FailureContext by compiling the scenario's blueprint."""
    from datetime import datetime, timezone
    from aqueduct.surveyor.models import FailureContext

    blueprint_path = (scenario.source_path.parent / scenario.blueprint).resolve()
    if not blueprint_path.exists():
        raise FileNotFoundError(
            f"Scenario {scenario.id!r}: blueprint not found at {blueprint_path}"
        )

    # Parse + compile to get a real manifest (no Spark needed)
    from aqueduct.parser.parser import parse
    from aqueduct.compiler.compiler import compile as compiler_compile

    bp = parse(str(blueprint_path))
    manifest = compiler_compile(bp, blueprint_path=blueprint_path)
    manifest_json = json.dumps(manifest.to_dict())

    inj = scenario.inject_failure
    now = datetime.now(tz=timezone.utc).isoformat()

    return FailureContext(
        run_id=f"scenario-{scenario.id}",
        blueprint_id=manifest.blueprint_id,
        failed_module=inj.get("module", "_executor"),
        error_message=inj.get("error_message", "Simulated failure"),
        stack_trace=inj.get("stack_trace"),
        manifest_json=manifest_json,
        started_at=now,
        finished_at=now,
        blueprint_source_yaml=blueprint_path.read_text(encoding="utf-8"),
    )


# ── Op matching ───────────────────────────────────────────────────────────────

def _match_op_spec(spec: dict[str, Any], actual_dict: dict[str, Any]) -> bool:
    """Return True if actual_dict satisfies all fields in spec.

    value_contains is a substring match on the 'value' field; all other
    fields require exact equality.
    """
    for k, v in spec.items():
        if k == "value_contains":
            actual_val = str(actual_dict.get("value", ""))
            if str(v) not in actual_val:
                return False
        else:
            if actual_dict.get(k) != v:
                return False
    return True


def _check_expected_patch(
    patch: "Any",  # PatchSpec
    expected: dict[str, Any],
) -> list[str]:
    """Return list of failure messages from expected_patch assertions."""
    failures: list[str] = []
    if not expected:
        return failures

    actual_ops = [op.model_dump() for op in (patch.operations or [])]
    actual_op_names = {d.get("op") for d in actual_ops}

    # Every expected op must match at least one actual op
    for spec in expected.get("ops", []):
        matched = any(_match_op_spec(spec, actual) for actual in actual_ops)
        if not matched:
            failures.append(
                f"expected_patch.ops: no generated op matches {spec!r}. "
                f"Generated ops: {[d.get('op') for d in actual_ops]}"
            )

    # Forbidden ops must not appear
    for forbidden in expected.get("forbidden_ops", []):
        if forbidden in actual_op_names:
            failures.append(
                f"expected_patch.forbidden_ops: op {forbidden!r} was generated but is forbidden"
            )

    return failures


def _try_apply_patch(patch: "Any", blueprint_path: Path) -> tuple[bool, str]:
    """Try applying patch to blueprint. Returns (success, error_message)."""
    import tempfile
    try:
        from aqueduct.patch.apply import _yaml_dump, _yaml_load, apply_patch_to_dict
        from aqueduct.parser.parser import ParseError, parse
        from aqueduct.compiler.compiler import CompileError, compile as compiler_compile

        bp_raw = _yaml_load(blueprint_path)
        patched = apply_patch_to_dict(bp_raw, patch)

        with tempfile.NamedTemporaryFile(suffix=".yml", delete=False, mode="w") as tmp:
            tmp_path = Path(tmp.name)
        _yaml_dump(patched, tmp_path)
        try:
            bp = parse(str(tmp_path))
            compiler_compile(bp, blueprint_path=tmp_path)
            return True, ""
        except (ParseError, CompileError) as exc:
            return False, str(exc)
        finally:
            tmp_path.unlink(missing_ok=True)
    except Exception as exc:
        return False, str(exc)


def _check_assertions(
    assertions: list[dict[str, Any]],
    patch: "Any",  # PatchSpec | None
    blueprint_path: Path | None,
    attempts: int = 0,
) -> tuple[list[str], bool, bool, bool | None, bool | None]:
    """Evaluate assertion list.

    Returns (failures, patch_valid, patch_applies, root_cause_match, category_match).
    root_cause_match and category_match are None when those assertions are not configured.
    """
    failures: list[str] = []
    patch_valid = patch is not None
    patch_applies = False
    root_cause_match: bool | None = None
    category_match: bool | None = None

    for assertion in assertions:
        if "patch_is_valid" in assertion:
            expected_val = bool(assertion["patch_is_valid"])
            if expected_val and not patch_valid:
                failures.append("patch_is_valid: patch is None (LLM failed to produce valid PatchSpec)")
            elif not expected_val and patch_valid:
                failures.append("patch_is_valid: expected invalid patch but got a valid one")

        if "patch_applies" in assertion:
            expected_val = bool(assertion["patch_applies"])
            if patch is None:
                if expected_val:
                    failures.append("patch_applies: cannot check — patch is None")
            else:
                if blueprint_path and blueprint_path.exists():
                    ok, err = _try_apply_patch(patch, blueprint_path)
                    patch_applies = ok
                    if expected_val and not ok:
                        failures.append(f"patch_applies: patch failed to apply: {err}")
                    elif not expected_val and ok:
                        failures.append("patch_applies: expected patch to fail but it applied successfully")
                else:
                    logger.warning("patch_applies assertion: blueprint path not found; skipped")

        if "max_attempts" in assertion:
            max_att = int(assertion["max_attempts"])
            if attempts > max_att:
                failures.append(
                    f"max_attempts: took {attempts} LLM call(s), max allowed {max_att} "
                    f"(reprompts needed → LLM needed schema correction)"
                )

        if "min_confidence" in assertion:
            min_conf = float(assertion["min_confidence"])
            actual_conf = patch.confidence if patch else None
            if actual_conf is None:
                failures.append(f"min_confidence: patch has no confidence field (expected >= {min_conf})")
            elif actual_conf < min_conf:
                failures.append(
                    f"min_confidence: {actual_conf:.2f} < {min_conf:.2f}"
                )

        if "expected_category" in assertion:
            expected_cat = str(assertion["expected_category"])
            actual_cat = patch.category if patch else None
            category_match = actual_cat == expected_cat
            if not category_match:
                failures.append(
                    f"expected_category: expected {expected_cat!r}, got {actual_cat!r}"
                )

        if "root_cause_contains" in assertion:
            raw = assertion["root_cause_contains"]
            keywords = [k.lower() for k in raw] if isinstance(raw, list) else [str(raw).lower()]
            actual_rc = (patch.root_cause or "").lower() if patch else ""
            root_cause_match = any(kw in actual_rc for kw in keywords)
            if not root_cause_match:
                failures.append(
                    f"root_cause_contains: none of {keywords!r} found in {actual_rc!r}"
                )

    return failures, patch_valid, patch_applies, root_cause_match, category_match


# ── Public API ─────────────────────────────────────────────────────────────────

def run_scenario(
    scenario: AqScenario,
    model: str,
    patches_dir: Path,
    provider: str = "anthropic",
    base_url: str | None = None,
    ollama_options: dict[str, Any] | None = None,
    llm_timeout: float = 120.0,
    llm_max_reprompts: int = 3,
    engine_prompt_context: str | None = None,
) -> ScenarioResult:
    """Run one scenario against the LLM and validate the response.

    No Spark session required — builds a FailureContext by compiling the
    referenced blueprint, injects the failure, and calls the LLM.
    """
    from aqueduct.surveyor.llm import generate_llm_patch

    t0 = time.monotonic()

    # Build failure context
    try:
        failure_ctx = _build_failure_ctx(scenario)
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
        )

    # Call LLM
    llm_result = generate_llm_patch(
        failure_ctx,
        model=model,
        patches_dir=patches_dir,
        provider=provider,
        base_url=base_url,
        ollama_options=ollama_options,
        llm_timeout=llm_timeout,
        llm_max_reprompts=llm_max_reprompts,
        engine_prompt_context=engine_prompt_context,
    )
    patch = llm_result.patch

    duration = time.monotonic() - t0

    # Resolve blueprint path for patch_applies check
    blueprint_path: Path | None = None
    if scenario.blueprint:
        bp_candidate = (scenario.source_path.parent / scenario.blueprint).resolve()
        if bp_candidate.exists():
            blueprint_path = bp_candidate

    # Check assertions
    assertion_failures, patch_valid, patch_applies, root_cause_match, category_match = (
        _check_assertions(scenario.assertions, patch, blueprint_path, attempts=llm_result.attempts)
    )

    # Check expected_patch
    expected_failures: list[str] = []
    if patch is not None and scenario.expected_patch:
        expected_failures = _check_expected_patch(patch, scenario.expected_patch)

    all_failures = assertion_failures + expected_failures
    passed = len(all_failures) == 0

    return ScenarioResult(
        scenario_id=scenario.id,
        model=model,
        passed=passed,
        patch_valid=patch_valid,
        patch_applies=patch_applies,
        failures=all_failures,
        patch=patch,
        duration_seconds=duration,
        confidence=patch.confidence if patch else None,
        attempts_to_parse=llm_result.attempts,
        reprompt_errors=llm_result.reprompt_errors,
        root_cause_match=root_cause_match,
        category_match=category_match,
    )


def run_benchmark(
    scenarios_dir: Path,
    models: list[str],
    patches_dir: Path,
    provider: str = "anthropic",
    base_url: str | None = None,
    ollama_options: dict[str, Any] | None = None,
    llm_timeout: float = 120.0,
    llm_max_reprompts: int = 3,
    engine_prompt_context: str | None = None,
    workers: int = 4,
) -> dict[str, dict[str, ScenarioResult]]:
    """Run all scenarios in scenarios_dir against each model.

    Executes (scenario, model) pairs in parallel using a thread pool.
    Each pair is an independent LLM HTTP call — no shared state.

    Args:
        workers: Max concurrent LLM calls. Default 4. Set to 1 for serial execution.

    Returns:
        {scenario_id: {model: ScenarioResult}}
    """
    import concurrent.futures

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

    def _run_pair(scenario: Any, model: str) -> tuple[str, str, ScenarioResult]:
        logger.info("Running scenario %r | model %r", scenario.id, model)
        r = run_scenario(
            scenario,
            model=model,
            patches_dir=patches_dir,
            provider=provider,
            base_url=base_url,
            ollama_options=ollama_options,
            llm_timeout=llm_timeout,
            llm_max_reprompts=llm_max_reprompts,
            engine_prompt_context=engine_prompt_context,
        )
        return scenario.id, model, r

    pairs = [(s, m) for s in loaded for m in models]
    effective_workers = min(workers, len(pairs))

    with concurrent.futures.ThreadPoolExecutor(max_workers=effective_workers) as pool:
        futures = [pool.submit(_run_pair, s, m) for s, m in pairs]
        for future in concurrent.futures.as_completed(futures):
            try:
                sid, model, result = future.result()
                results[sid][model] = result
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

    # Column widths
    id_col_w = max(len("Scenario"), max(len(sid) for sid in scenario_ids))
    model_col_w = max(len(m) for m in models) + 2
    model_col_w = max(model_col_w, 12)

    sep = "-" * (id_col_w + 2) + "-+-" + "-+-".join("-" * model_col_w for _ in models)
    header = f"{'Scenario':<{id_col_w}}  | " + " | ".join(f"{m:^{model_col_w}}" for m in models)

    lines: list[str] = [header, sep]

    for sid, model_results in results.items():
        cells = []
        for model in models:
            r = model_results.get(model)
            if r is None:
                cells.append(f"{'—':^{model_col_w}}")
            elif r.passed:
                conf = f"{r.confidence:.2f}" if r.confidence is not None else "—"
                t = f"{r.duration_seconds:.1f}s"
                cell = f"PASS {conf} {t}"
                cells.append(f"{cell:^{model_col_w}}")
            else:
                t = f"{r.duration_seconds:.1f}s"
                cell = f"FAIL {t}"
                cells.append(f"{cell:^{model_col_w}}")
        lines.append(f"{sid:<{id_col_w}}  | " + " | ".join(cells))

    lines.append(sep)

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
    ]:
        cells = []
        for model in models:
            model_results_list = [
                results[sid][model] for sid in scenario_ids if model in results[sid]
            ]
            if model_results_list:
                cells.append(f"{fn(model_results_list):^{model_col_w}}")
            else:
                cells.append(f"{'—':^{model_col_w}}")
        lines.append(f"{label:<{id_col_w}}  | " + " | ".join(cells))

    return "\n".join(lines)
