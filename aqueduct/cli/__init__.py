"""Aqueduct CLI.

Commands: init, validate, compile, run, check-config, doctor, runs, report,
          lineage, signal, heal, benchmark, log, rollback,
          patch apply, patch reject, patch commit, patch discard, patch list.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC
from pathlib import Path
from typing import Any

import click

# Click hardcodes `UsageError.exit_code = 2` (a class attribute, not set per
# instance) — every UsageError subclass (BadParameter, MissingParameter,
# NoSuchOption, BadOptionUsage, BadArgumentUsage, the bare "no such command"
# UsageError, ...) inherits it and none override it. That collides with
# `exit_codes.DATA_OR_RUNTIME == 2`, so a wrapper (Airflow operator, shell
# script) cannot tell "typo'd flag" from "pipeline failed at runtime" by exit
# code alone. Repointing the class attribute here — once, at CLI import time
# — makes every Click usage error exit `exit_codes.USAGE_ERROR` (64,
# sysexits EX_USAGE) instead, unifying it with Aqueduct's own
# usage-mistake code. This does not touch Click's message rendering
# (`UsageError.show()` is untouched) or its dispatch — only the exit code
# `BaseCommand.main()` passes to `sys.exit()`. See `aqueduct/exit_codes.py`
# for the full contract.
from aqueduct import exit_codes as _exit_codes

click.exceptions.UsageError.exit_code = _exit_codes.USAGE_ERROR

_PROJECT_ROOT_MAX_DEPTH = 8
_DEFAULT_CONFIG_FILENAME = "aqueduct.yml"

logger = logging.getLogger(__name__)


def _apply_warnings_from_cfg(cfg) -> None:
    """Merge `cfg.warnings` with the CLI flags installed by the root group.

    Idempotent. Call once per command immediately after `load_config()` so the
    engine-level `warnings.suppress` from aqueduct.yml is honoured alongside
    any `--suppress-warning` flags the user passed. Use `*` to silence all.
    """
    from aqueduct.warnings import _DEFAULT_SUPPRESS, set_default_strict, set_default_suppress

    merged = set(_DEFAULT_SUPPRESS) | set(getattr(cfg.warnings, "suppress", []) or [])
    set_default_suppress(suppress=merged)
    set_default_strict(getattr(cfg.warnings, "strict", []) or [])


def _compile_with_warnings(
    compile_fn, *args, _verbose: bool = False, _defer: bool = False, **kwargs
):
    """Call compile_fn, intercept warnings, reprint as clean CLI output.

    Aqueduct's own diagnostics (AqueductWarning category, prefix
    `[aqueduct:rule_id] `) become `AQ-WARN [rule_id] <msg>` lines so the
    rule_id is easy to copy into `warnings.suppress` in aqueduct.yml.
    Non-Aqueduct UserWarnings fall back to the legacy `WARNING:` prefix.

    When ``_defer`` is True, the captured records are RETURNED as
    ``(result, caught)`` instead of emitted here — so the caller can flush them
    at the right point in the output progression (e.g. AFTER the run header,
    where the blueprint these warnings are about is named).
    """
    import warnings as _w

    with _w.catch_warnings(record=True) as caught:
        _w.simplefilter("always")
        result = compile_fn(*args, **kwargs)
    if _defer:
        return result, list(caught)
    from aqueduct.cli.style import emit_warnings

    emit_warnings(caught, verbose=_verbose, err=True, label="compile:")
    return result


def _rule(char: str = "─") -> str:
    """A horizontal rule spanning the terminal width (fallback 64)."""
    import shutil

    return char * shutil.get_terminal_size(fallback=(64, 20)).columns


# ── Self-healing helpers ──────────────────────────────────────────────────────
# Deterministic guardrail enforcement lives in aqueduct.patch.apply._check_guardrails.
# That is the single authoritative implementation; do not reintroduce a CLI-side copy.


def _extract_stack_class(stack_trace: str | None) -> str | None:
    """Extract the exception class name from the last line of a stack trace.

    e.g. 'pyspark.errors.exceptions.SparkException: ...' → 'SparkException'
    """
    if not stack_trace:
        return None
    last_line = stack_trace.strip().splitlines()[-1]
    class_part = last_line.split(":")[0].strip()
    return class_part.split(".")[-1] if class_part else None


def _check_heal_guardrails(failure_ctx: Any, guardrails: Any) -> tuple[bool, str]:
    """Pre-trigger guardrail check.

    Returns (should_heal, reason_if_blocked).
    never_heal_errors takes priority over heal_on_errors.
    Matching uses error_type from FailureContext (Assert label) or the
    exception class name extracted from the stack trace (infra errors).

    Phase 41: never_heal_errors patterns are regex — e.g.
    ``"IllegalStateException.*offsets"`` matches any error class
    containing "IllegalStateException" and "offsets".
    """
    import re

    error_type: str | None = getattr(failure_ctx, "error_type", None)
    stack_class: str | None = _extract_stack_class(getattr(failure_ctx, "stack_trace", None))

    candidates: set[str] = set()
    if error_type:
        candidates.add(error_type)
    if stack_class:
        candidates.add(stack_class)

    never_heal: tuple = tuple(getattr(guardrails, "never_heal_errors", ()))
    heal_on: tuple = tuple(getattr(guardrails, "heal_on_errors", ()))

    for pattern in never_heal:
        for candidate in candidates:
            try:
                if re.search(pattern, candidate):
                    return (
                        False,
                        f"error {candidate!r} matched never_heal_errors pattern {pattern!r}",
                    )
            except re.error:
                # Degrade gracefully on malformed regex: fall back to exact match
                if pattern == candidate:
                    return (
                        False,
                        f"error {candidate!r} matched never_heal_errors pattern {pattern!r}",
                    )

    if heal_on:
        for et in heal_on:
            if et in candidates:
                return True, ""
        matched = f"error_type={error_type!r}" if error_type else f"stack_class={stack_class!r}"
        return False, f"{matched} not in heal_on_errors whitelist"

    return True, ""


def resolve_agent_connection(engine_agent, blueprint_agent=None):
    """Resolve effective agent CONNECTION settings + merge agent POLICY.

    CONNECTION fields (provider/base_url/api_key/model/provider_options/
    timeout/cascade) come from `engine_agent` (aqueduct.yml) ONLY — a
    Blueprint cannot set or override them (`AgentSchema` in
    `aqueduct/parser/schema.py` has no such fields; a Blueprint author who
    could redirect these would redirect the healing loop's FailureContext —
    pruned manifest, provenance, error text, and, in agentic mode, sampled
    data rows — to an arbitrary host on any failure). This function used to
    accept a blueprint override for these; that path was removed as a
    security fix (2.59) — see AgentSchema's docstring / CHANGELOG.

    POLICY fields (max_reprompts, mode, max_tool_calls, supports_tools,
    progressive, max_chain) use the blueprint value when set, falling back
    to the engine default. Returns a simple object with resolved values
    that can be destructured at the call site.

    prompt_context is NOT OR‑merged — the engine and blueprint versions
    are kept separate so the agent loop can concatenate them.
    """

    class _Resolved:
        __slots__ = (
            "provider",
            "base_url",
            "model",
            "api_key",
            "cascade",
            "provider_options",
            "timeout",
            "max_reprompts",
            "engine_prompt_context",
            "blueprint_prompt_context",
            "mode",
            "max_tool_calls",
            "supports_tools",
            "progressive",
            "max_chain",
        )

    bp = blueprint_agent
    eng = engine_agent
    r = _Resolved()
    # Connection fields — engine-only. A Blueprint cannot influence any of these.
    r.provider = eng.provider
    r.base_url = eng.base_url
    r.api_key = eng.api_key
    r.model = eng.model
    r.provider_options = eng.provider_options
    r.timeout = eng.timeout
    from aqueduct.parser.parser import _build_cascade

    r.cascade = _build_cascade(eng.cascade) if eng.cascade else None
    # Policy fields — blueprint overrides when set (None = inherit engine default).
    r.max_reprompts = (bp.max_reprompts or eng.max_reprompts) if bp else eng.max_reprompts
    r.engine_prompt_context = eng.prompt_context
    r.blueprint_prompt_context = bp.prompt_context if bp else None
    # Phase 75 — same `is not None` inheritance shape as patch_validation:
    # these are tri-state (None must mean "inherit"), so `or` merge is wrong
    # (a blueprint explicitly setting supports_tools: false is falsy but valid).
    r.mode = bp.mode if bp and bp.mode is not None else eng.mode
    r.max_tool_calls = (
        bp.max_tool_calls if bp and bp.max_tool_calls is not None else eng.max_tool_calls
    )
    r.supports_tools = (
        bp.supports_tools if bp and bp.supports_tools is not None else eng.supports_tools
    )
    # Progressive (chained) multi-patch healing — same `is not None`
    # inheritance shape as mode/supports_tools above.
    r.progressive = bp.progressive if bp and bp.progressive is not None else eng.progressive
    r.max_chain = bp.max_chain if bp and bp.max_chain is not None else eng.max_chain
    return r


def _resolve_project_root(
    blueprint_path: Path | None = None,
    config_path: Path | None = None,
) -> Path:
    """Walk up from blueprint or config to find the project root.

    Returns the directory containing ``aqueduct.yml`` (the _DEFAULT_CONFIG_FILENAME) when found (walking up
    to _PROJECT_ROOT_MAX_DEPTH levels from the blueprint), or falls back to the file's immediate
    parent directory.  ``config_path``, when given, always wins — its parent
    is the project root.
    """
    from pathlib import Path as _Path

    if config_path is not None:
        return config_path.parent
    if blueprint_path is not None:
        root = blueprint_path.parent
        search = blueprint_path.parent
        for _ in range(_PROJECT_ROOT_MAX_DEPTH):
            if (search / _DEFAULT_CONFIG_FILENAME).exists():
                return search
            if search.parent == search:
                break
            search = search.parent
        return root
    return _Path.cwd()


def _load_config_with_env(
    config_path: Path | None = None,
    *,
    env_file: str | None = None,
    cli_env: tuple[str, ...] | list[str] | None = None,
    quiet: bool = False,
) -> Any:
    """Load engine config after resolving .env / CI-injected env vars.

    Single entry point so ``load_config()`` is never called without first
    populating ``os.environ`` from the project ``.env`` file.  When
    ``config_path`` is ``None`` the project root is discovered by walking up
    from CWD (same ``_resolve_project_root`` logic as every CLI command).

    ``quiet`` suppresses the stderr notice — useful for long-running
    processes that re-load config on every refresh.
    """
    from pathlib import Path as _Path2

    _cfg = _Path2(config_path) if config_path is not None else None
    _anchor = _cfg if _cfg is not None else _resolve_project_root() / _DEFAULT_CONFIG_FILENAME
    if quiet:
        import click as _click

        _real_echo = _click.echo
        _click.echo = lambda *a, **kw: None
        try:
            _resolve_and_load_env(env_file, _anchor, cli_env=cli_env)
        finally:
            _click.echo = _real_echo
    else:
        _resolve_and_load_env(env_file, _anchor, cli_env=cli_env)
    from aqueduct.config import load_config as _load_config

    return _load_config(_cfg)


def _resolve_obs_db(
    cfg,
    store_dir: str | None,
    run_id: str | None = None,
) -> Path | None:
    """Resolve the observability DB file path for a READ command.

    Mirrors the per-pipeline routing the WRITE side (``aqueduct run``) does at
    cli.py:1185-1290: when the user keeps the default
    ``.aqueduct/observability.db``, each blueprint writes to
    ``.aqueduct/observability/<blueprint_id>/observability.db``. READ commands
    (``runs``, ``report``, ``lineage``, ``heal``) need to find the right per-pipeline
    file — historically each command reinvented this with a naive
    ``Path(cfg.stores.observability.path).parent``, which only worked when the
    user explicitly set a non-default path.

    Canonical logic now lives in ``aqueduct.stores.read.resolve_duckdb_obs_path``
    (Phase 69) so every reader shares one resolver; this stays as a thin,
    monkeypatch-friendly wrapper. For backend-aware reads (DuckDB *or* Postgres),
    prefer ``aqueduct.stores.read.open_obs_read``.
    """
    from aqueduct.stores.read import resolve_duckdb_obs_path

    return resolve_duckdb_obs_path(cfg, store_dir, run_id)


def _agent_usable(provider: str, base_url: str | None, api_key: str | None = None) -> bool:
    """Return True if the LLM provider appears reachable without making a network call.

    anthropic:     requires ANTHROPIC_API_KEY in os.environ (or api_key param)
    openai_compat: requires base_url (Ollama/vLLM) OR OPENAI_API_KEY (or api_key param)
    """
    import os as _os

    if provider == "anthropic":
        return bool(api_key or _os.environ.get("ANTHROPIC_API_KEY"))
    if provider == "openai_compat":
        return bool(base_url or api_key or _os.environ.get("OPENAI_API_KEY"))
    return False


def _agent_usable_with_cascade(
    provider: str,
    base_url: str | None,
    api_key: str | None = None,
    cascade_tiers: list | None = None,
) -> bool:
    """Return True if the flat config OR any cascade tier is reachable.

    A cascade tier carries its own base_url/api_key (falling back to the
    flat agent.* defaults).  If ANY tier is usable, healing works even
    when the flat agent.base_url/api_key are unset (ISSUE-045).
    """
    if _agent_usable(provider, base_url, api_key):
        return True
    if cascade_tiers:
        for t in cascade_tiers:
            if _agent_usable(
                provider,
                getattr(t, "base_url", None) or base_url,
                getattr(t, "api_key", None) or api_key,
            ):
                return True
    return False


def _apply_patch_in_memory(
    patch_spec, blueprint_path: Path, depot, profile, cli_overrides: dict
) -> Any:
    """Apply patch operations to Blueprint without touching disk. Returns new Manifest or None."""
    try:
        from aqueduct.compiler.compiler import CompileError
        from aqueduct.compiler.compiler import compile as compiler_compile
        from aqueduct.parser.parser import ParseError, parse_dict
        from aqueduct.patch.apply import _yaml_load, apply_patch_to_dict

        bp_raw = _yaml_load(blueprint_path)
        patched = apply_patch_to_dict(bp_raw, patch_spec)

        # Parse the patched dict directly with
        # ``base_dir`` set to the original Blueprint's parent. Replaces the
        # tempfile dance that broke 1.1.0 path anchoring whenever the
        # tempfile landed in ``/tmp`` and relative module paths resolved
        # against ``/tmp`` instead of the project root.
        base_dir = blueprint_path.parent if blueprint_path.exists() else Path.cwd()
        try:
            bp = parse_dict(
                patched,
                base_dir=base_dir,
                profile=profile,
                cli_overrides=cli_overrides or None,
            )
            return compiler_compile(bp, blueprint_path=blueprint_path, depot=depot)
        except (ParseError, CompileError):
            return None
    except Exception:
        return None


def _write_patch_to_blueprint(
    patch_spec,
    blueprint_path: Path,
    patches_dir: Path,
    failure_ctx,
    mode: str,
    obs_store=None,
    patch_store=None,
    cfg=None,
) -> Any:
    """Write patch permanently to Blueprint, re-parse, re-compile. Returns new Manifest or None.

    ``cfg`` (``AqueductConfig``) is used only to record the EFFECTIVE
    engine-config delta in the ``healed_by:`` provenance record — the same
    delta Gate 1 computed when it let this patch through. Without it the
    auto-apply path would write a provenance record silently missing the one
    field that says what a ``set_engine_config`` patch actually changed,
    while ``aqueduct patch apply`` recorded it. ``None`` falls back to the
    ambient config (``load_config(None)``), never to omitting the field.
    """
    try:
        import os as _os
        from datetime import datetime

        from aqueduct.agent import archive_patch
        from aqueduct.compiler.compiler import CompileError
        from aqueduct.compiler.compiler import compile as compiler_compile
        from aqueduct.parser.parser import ParseError, parse
        from aqueduct.patch.apply import (
            _append_healed_by,
            _yaml_dump,
            _yaml_load,
            apply_patch_to_dict,
        )
        from aqueduct.patch.provenance import build_healed_by_record, detect_engine_version

        bp_raw = _yaml_load(blueprint_path)
        patched = apply_patch_to_dict(bp_raw, patch_spec)

        # Heal-patch provenance (Phase 79) — this is the auto-mode (agent.
        # approval_mode: auto) direct-write path, so there is no on-disk
        # patch JSON with `_aq_meta` to re-read (unlike `apply_patch_file`).
        # Build the equivalent meta straight from the live FailureContext —
        # `.engine` is required on it (see aqueduct/surveyor/models.py).
        _applied_at = datetime.now(tz=UTC).isoformat()
        _meta = {
            "engine": getattr(failure_ctx, "engine", None),
            "engine_version": detect_engine_version(getattr(failure_ctx, "engine", "")),
            "run_id": getattr(failure_ctx, "run_id", None),
        }
        if cfg is None:
            from aqueduct.config import load_config as _load_config

            cfg = _load_config(None)
        from aqueduct.patch.config_delta import run_engine_config_delta_gate

        _delta_res = run_engine_config_delta_gate(
            cfg=cfg,
            blueprint_before=bp_raw,
            patch_spec=patch_spec,
            blueprint_after=patched,
        )
        # Warn-only perf baseline — the last green run before this apply.
        # Same reasoning (and the same best-effort posture) as the
        # `aqueduct patch apply` path in patch/apply.py: without it the
        # auto-apply path would write a provenance record whose only
        # success signal is the binary `validated_on`.
        from aqueduct.patch.perf_attribution import capture_baseline_perf

        _perf_baseline = capture_baseline_perf(
            obs_store, str(bp_raw.get("id") or ""), before=_applied_at
        )
        _healed_by_record = build_healed_by_record(
            patch_id=patch_spec.patch_id,
            operations=patch_spec.operations,
            meta=_meta,
            applied_at=_applied_at,
            fallback_run_id=patch_spec.run_id,
            engine_config_delta=_delta_res.delta,
            perf_baseline=_perf_baseline.to_dict() if _perf_baseline else None,
        )
        patched = _append_healed_by(patched, _healed_by_record)

        # Backup original
        backup_dir = patches_dir / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        import shutil
        from datetime import datetime

        ts = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
        shutil.copy2(
            blueprint_path, backup_dir / f"{patch_spec.patch_id}_{ts}_{blueprint_path.name}"
        )

        # Write atomically
        tmp_out = blueprint_path.with_suffix(".llm_patch.tmp.yml")
        _yaml_dump(patched, tmp_out)
        _os.replace(tmp_out, blueprint_path)

        archive_patch(
            patch_spec,
            patches_dir,
            failure_ctx,
            mode=mode,
            patch_store=patch_store,
            obs_store=obs_store,
        )

        # Re-parse + re-compile from updated file
        bp = parse(str(blueprint_path))
        return compiler_compile(bp, blueprint_path=blueprint_path)
    except (ParseError, CompileError):
        return None
    except Exception:
        return None


def _record_engine_config_simulation(  # noqa: F811
    *,
    patch,  # noqa: F811
    surveyor,
    cfg,
    blueprint_before,
    blueprint_after,
    iteration_run_id: str | None,
    blueprint_id: str | None,
) -> None:
    """Write the engine-config delta gate's verdict to ``patch_simulation``.

    Audit only — never raises, never blocks. The refusal this records as
    ``fail`` is ENFORCED by ``_check_guardrails`` on every apply path; this
    is the row that makes it countable
    (``stores/queries.py::gate_rejection_rates``).

    ``duration_ms`` is measured here rather than carried on
    ``EngineConfigDeltaResult`` because the refusal path has no result
    object at all — an exception cannot carry the field, so the only place
    both outcomes can be timed the same way is the call site.
    """
    import time as _time

    from aqueduct.patch.apply import PatchError
    from aqueduct.patch.config_delta import run_engine_config_delta_gate

    _t0 = _time.monotonic()
    try:
        try:
            _res = run_engine_config_delta_gate(
                cfg=cfg,
                blueprint_before=blueprint_before,
                patch_spec=patch,
                blueprint_after=blueprint_after,
            )
            _status, _detail = _res.status, _res.detail
        except PatchError as exc:
            # Only ONE check ran above, so this is the effective-config
            # no-op refusal by construction — no message matching needed.
            _status, _detail = "fail", str(exc)
        surveyor.record_patch_simulation(
            patch_id=patch.patch_id,
            gate="engine_config",
            status=_status,
            detail=_detail or None,
            duration_ms=int((_time.monotonic() - _t0) * 1000),
            run_id=iteration_run_id,
            blueprint_id=blueprint_id,
        )
    except Exception:
        logger.warning("record_patch_simulation (engine_config) failed", exc_info=True)


def _run_patch_gates_inline(  # noqa: F811
    *,
    patch,  # noqa: F811
    blueprint_path,
    bundle,
    surveyor,
    failed_module,
    iteration_run_id: str,
    blueprint_id: str,
    engine: str,
    cfg,
    sample_rows: int = 1000,
    sandbox_mode: str = "sample",
    sandbox_master_url: str | None = None,
    warnings_suppress=None,
    timezone: str | None = None,
    depot_reads_at_failure: dict[str, str] | None = None,
):
    """Phase 29a — run the lineage and sandbox gates inline.

    Also PERSISTS the engine-config delta gate's verdict (Gate 1's efficacy
    half, ``aqueduct/patch/config_delta.py``) as a fourth
    ``patch_simulation`` row. That gate is ENFORCED elsewhere — inside
    ``_check_guardrails``, on every apply path — and this function does not
    enforce it: ``gates_passed`` is unchanged, nothing here can refuse a
    patch, and the returned tuple keeps its three-gate shape. What was
    missing was the audit trail: `patch_simulation` is the fleet-level
    record of what each gate decided (`stores/queries.py::
    gate_rejection_rates` reads it), and a gate that never writes a row is
    invisible there — it looks like a gate that never rejects anything.
    Evaluating it HERE, rather than passing a result down from the
    enforcing call site, is what makes a `fail` row identifiable without
    matching on message text: this call sees only the delta gate, so any
    ``PatchError`` it raises is by construction the delta refusal, never a
    `forbidden_ops` / `allowed_paths` / allowlist rejection (those are
    `heal_attempts` rows, per `docs/observability_guide.md`).

    Returns (lineage_res, sandbox_res, resolvability_res,
    gates_passed). ``gates_passed`` is decided by TWO predicates, ANDed:
    ``patch/gate_status.py::sandbox_gate_permits_auto_apply`` (Gate 3) and
    ``patch/gate_status.py::resolvability_gate_permits_auto_apply`` (Gate 4,
    Phase 88). Gate 3's sandbox gate must have replayed the patch (`pass`)
    or have been owed no replay at all (`not_applicable`, i.e.
    `agent.sandbox_mode: off`). An ``unavailable`` sandbox result BLOCKS: the
    target engine's dependencies are missing, its session would not start,
    or the Blueprint is polyglot, so nothing about this patch was verified
    and a human decides. That status used to be spelled `skip` and was
    accepted here, which let a never-replayed patch auto-apply as though it
    had been replayed. Gate 4's resolvability gate must find every declared
    dependency already satisfied (`pass`) or owe no check at all
    (`not_applicable` — the patch declares no `declare_dependency` op); a
    `warn` (resolves on PyPI but not installed) or `fail`/`unavailable`
    result BLOCKS auto-apply the same way.

    ``engine`` is REQUIRED — passed straight through to
    ``run_sandbox_gate(engine=...)`` (Phase 79) so the sandbox replay runs
    against the SAME engine the patch's own pipeline targets, never a
    hardcoded Spark session. Every caller already has ``engine`` resolved
    (``aqueduct/cli/run.py``'s ``engine = cfg.deployment.engine``).

    ``cfg`` (``AqueductConfig``) is REQUIRED — forwarded to
    ``run_sandbox_gate(cfg=...)`` (Phase 82 remediation) so the sandbox
    replay's owned session resolves the SAME ``engine.<name>.*`` config a
    real run would use, instead of the sandbox gate seeing no engine config
    at all. Every caller already has ``cfg`` resolved by the time it reaches
    here — none of the three call sites in ``aqueduct/cli/run.py`` lack one.

    ``depot_reads_at_failure`` -- optional; the depot keys/values resolved
    while compiling the Manifest that FAILED (``_CompileResult.depot_reads``
    from ``cli/run_setup.py``). Forwarded to ``run_sandbox_gate`` so Gate 3
    can print a staleness notice when a depot-derived value moved between
    the failure and this recompile. ``None`` (the default, used by
    ``aqueduct/agent/gate_validation.py``'s caller) means no failed run is
    in play here, so no notice is possible.
    """
    from aqueduct.patch.apply import _yaml_load, apply_patch_to_dict
    from aqueduct.patch.preview import run_lineage_gate, run_sandbox_gate

    bp_raw = _yaml_load(blueprint_path)
    try:
        bp_after = apply_patch_to_dict(bp_raw, patch)
    except Exception:
        return None, None, None, False

    _record_engine_config_simulation(
        patch=patch,
        surveyor=surveyor,
        cfg=cfg,
        blueprint_before=bp_raw,
        blueprint_after=bp_after,
        iteration_run_id=iteration_run_id,
        blueprint_id=blueprint_id,
    )

    lineage_res = run_lineage_gate(bp_raw, bp_after, patch)
    try:
        surveyor.record_patch_simulation(
            patch_id=patch.patch_id,
            gate="lineage",
            status=lineage_res.status,
            # Structured findings win when present; otherwise fall back to
            # the gate's own `detail` (populated on `not_applicable` — see
            # `run_lineage_gate` — so the observability row still explains
            # WHY, instead of a bare `not_applicable` with a null detail
            # that looks identical to an unset field).
            detail=(
                "; ".join(w.detail for w in lineage_res.warnings) or lineage_res.detail or None
            ),
            duration_ms=lineage_res.duration_ms,
            run_id=iteration_run_id,
            blueprint_id=blueprint_id,
        )
    except Exception:
        logger.warning("record_patch_simulation (lineage) failed", exc_info=True)

    # 1.1.0 — sandbox_mode controls replay fidelity:
    #   sample   → sample_rows rows per Ingress, no Egress (default)
    #   preflight → full dataset, no Egress (slow, conclusive)
    #   off       → the gate is owed nothing (synthetic `not_applicable`)
    if sandbox_mode == "off":
        from aqueduct.patch.gate_status import GateStatus as _GS
        from aqueduct.patch.preview import SandboxGateResult as _SBR

        # `not_applicable`, NOT `unavailable`: nothing prevented this
        # replay — an operator declared it unowed, behind a danger flag
        # (`agent.sandbox_mode: off` is refused unless
        # `danger.allow_skip_sandbox: true`) that already prints its own
        # startup warning. Blocking here would make that setting refuse
        # every heal it was set to allow. The partition the two words
        # encode is "was a check OWED", not "did a check happen".
        sandbox_res = _SBR(
            status=_GS.NOT_APPLICABLE,
            detail=(
                "no sandbox replay was owed — sandbox_mode=off "
                "(danger.allow_skip_sandbox=true); this patch was NOT replayed"
            ),
            sample_rows=0,
            duration_ms=0,
        )
    else:
        _sample_for_call = 0 if sandbox_mode == "preflight" else int(sample_rows)
        sandbox_res = run_sandbox_gate(
            bp_after,
            blueprint_path=blueprint_path,
            patch_id=patch.patch_id,
            failed_module=failed_module,
            engine=engine,
            cfg=cfg,
            sample_rows=_sample_for_call,
            observability_store=bundle.observability,
            sandbox_master_url=sandbox_master_url,
            warnings_suppress=warnings_suppress,
            timezone=timezone,
            patch_spec=patch,
            depot_reads_at_failure=depot_reads_at_failure,
        )
    try:
        surveyor.record_patch_simulation(
            patch_id=patch.patch_id,
            gate="sandbox",
            status=sandbox_res.status,
            detail=sandbox_res.detail,
            sample_rows=sandbox_res.sample_rows,
            duration_ms=sandbox_res.duration_ms,
            run_id=iteration_run_id,
            blueprint_id=blueprint_id,
        )
    except Exception:
        logger.warning("record_patch_simulation (sandbox) failed", exc_info=True)

    # Resolvability gate (Gate 4, Phase 88) — per-requirement PyPI check for
    # every declare_dependency op in the patch.
    from aqueduct.patch.resolvability_gate import run_resolvability_gate

    resolvability_res = run_resolvability_gate(patch)
    try:
        surveyor.record_patch_simulation(
            patch_id=patch.patch_id,
            gate="resolvability",
            status=resolvability_res.status,
            detail=resolvability_res.detail or None,
            duration_ms=resolvability_res.duration_ms,
            run_id=iteration_run_id,
            blueprint_id=blueprint_id,
        )
    except Exception:
        logger.warning("record_patch_simulation (resolvability) failed", exc_info=True)

    from aqueduct.patch.gate_status import (
        resolvability_gate_permits_auto_apply,
        sandbox_gate_permits_auto_apply,
    )

    gates_passed = sandbox_gate_permits_auto_apply(
        sandbox_res
    ) and resolvability_gate_permits_auto_apply(resolvability_res)
    return lineage_res, sandbox_res, resolvability_res, gates_passed


def _stage_failed_patch(
    on_heal_failure: str,
    patch_spec,
    patches_dir,
    failure_ctx,
    cfg,
    click_mod,
    obs_store=None,
    patch_store=None,
) -> None:
    """Handle on_heal_failure policy for a patch that failed to fix the pipeline."""
    if on_heal_failure == "stage":
        from aqueduct.agent import stage_patch_for_human

        stage_patch_for_human(
            patch_spec,
            patches_dir,
            failure_ctx,
            on_patch_pending_webhook=cfg.webhooks.on_patch_pending,
            patch_store=patch_store,
            obs_store=obs_store,
        )
        _label = patch_store.location_label if patch_store is not None else patches_dir
        click_mod.echo(
            f"  ✎ Failed patch staged for review → {_label}/pending/  (id={patch_spec.patch_id})",
            err=True,
        )
    # discard: do nothing
    # abort: caller handles break


def _load_env_file(env_path: Path) -> int:
    """Load KEY=VALUE pairs from a .env file into os.environ.

    Skips blank lines and comments (#). Existing env vars are NOT overwritten.
    Returns number of variables loaded.
    """
    import os

    loaded = 0
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip("'\"")  # strip optional surrounding quotes
        if key and key not in os.environ:
            os.environ[key] = val
            loaded += 1
    return loaded


# ── Unified env resolution (Phase 30) ─────────────────────────────────────────
# One code path for EVERY config-consuming command. Deterministic, transparent.
#
# Precedence (highest first):
#   1. -e / --env KEY=VAL   (CLI, docker-style, repeatable — overwrites)
#   2. real os.environ      (already exported / injected by orchestrator)
#   3. <anchor-dir>/.env    (project file beside aqueduct.yml / blueprint)
#   4. --env-file PATH       fallback only if no project .env (explicit override)
#   5. ${VAR:-default}       (resolver-level, in parser/config)
#
# cwd is intentionally NOT searched — a stray ./.env silently changing a run
# is the exact footgun we're removing. Disable .env discovery entirely with
# AQ_NO_ENV_FILE=1 (command-independent; CI / prod hermetic). A one-line
# stderr notice is always emitted so the implicit load is never invisible.


def _apply_cli_env(cli_env: tuple[str, ...] | list[str]) -> int:
    """Apply `-e KEY=VAL` overrides into os.environ. Returns count.

    Highest precedence: overwrites real env AND any later .env (the .env
    loader skips keys already present). Docker `-e` semantics.
    """
    import os

    n = 0
    for item in cli_env or ():
        key, sep, val = item.partition("=")
        key = key.strip()
        if not sep or not key:
            raise click.BadParameter(
                f"-e/--env expects KEY=VALUE, got {item!r}",
                param_hint="-e",
            )
        os.environ[key] = val.strip()
        n += 1
    return n


def _resolve_and_load_env(
    explicit: str | None,
    anchor: Path | None,
    cli_env: tuple[str, ...] | list[str] | None = None,
) -> None:
    """Apply -e overrides, then load a single .env file. Emits a stderr notice.

    `anchor` = the input file (aqueduct.yml / blueprint) whose directory holds
    the project .env. cwd is never searched. AQ_NO_ENV_FILE=1 disables .env
    discovery (overrides still applied).
    """
    import os

    from aqueduct.cli.style import ICON
    from aqueduct.cli.style import info as _info

    n_over = _apply_cli_env(cli_env or ())
    over = f"; {n_over} from -e" if n_over else ""
    _env = f"{ICON['info']} env  ·  "

    if os.environ.get("AQ_NO_ENV_FILE"):
        _info(f"{_env}.env discovery disabled — AQ_NO_ENV_FILE{over}", err=True)
        return

    candidates: list[Path] = []
    if anchor is not None:
        candidates.append(Path(anchor).resolve().parent / ".env")
    if explicit:
        candidates.append(Path(explicit).resolve())

    seen: set[Path] = set()
    for cand in candidates:
        if cand in seen or not cand.exists():
            seen.add(cand)
            continue
        n = _load_env_file(cand)
        _info(f"{_env}loaded {n} var(s) from {cand}{over}", err=True)
        return  # first existing file wins — do not stack multiple .env files

    if n_over:
        _info(f"{_env}no .env file found{over}", err=True)


def _env_options(f):
    """Shared decorator: adds `--env-file` + `-e/--env` to a command.

    Phase 30 — every config-consuming command gets identical env handling
    via this single decorator (no per-command copy-paste to forget). The
    `--no-env-file` flag is gone; use the AQ_NO_ENV_FILE=1 env var instead
    (command-independent, CI/prod-settable).
    """
    f = click.option(
        "--env-file",
        "env_file",
        default=None,
        type=click.Path(dir_okay=False),
        help="Fallback .env if no project .env beside the config/blueprint.",
    )(f)
    f = click.option(
        "-e",
        "--env",
        "cli_env",
        multiple=True,
        metavar="KEY=VAL",
        help="Set an env var (repeatable, docker-style). Highest precedence.",
    )(f)
    return f


def _sniff_file_kind(path: Path) -> str | None:
    """Identify an Aqueduct YAML by its version header (no full parse).

    Returns one of: "blueprint", "config", "aqtest", "aqscenario", or None
    when no recognised top-level key is found in the first ~40 lines.

    Header keys:
      aqueduct:           → blueprint
      aqueduct_config:    → engine config (aqueduct.yml)
      aqueduct_test:      → .aqtest.yml
      aqueduct_scenario:  → .aqscenario.yml
    """
    import re as _re

    try:
        head = "\n".join(path.read_text(encoding="utf-8").splitlines()[:40])
    except Exception:
        return None
    for key, kind in (
        (r"^aqueduct_config\s*:", "config"),
        (r"^aqueduct_test\s*:", "aqtest"),
        (r"^aqueduct_scenario\s*:", "aqscenario"),
        (r"^aqueduct\s*:", "blueprint"),
    ):
        if _re.search(key, head, _re.MULTILINE):
            return kind
    return None


from aqueduct import __version__ as _aqueduct_version  # noqa: E402  (intentional mid-file import)


def _install_styled_echo() -> None:
    """Wrap ``click.echo`` so the icon vocabulary is coloured on every status line.

    The systemic styler — installed once at top-level (text mode only), so each
    call site no longer has to colour status lines by hand (the recurring
    "uncoloured `✗ …` / raw line" class of bug). Idempotent; composes as the
    outer wrapper over the redaction hook. JSON/prose/already-styled lines pass
    through untouched (see ``style.colorize_line``)."""
    if getattr(click.echo, "_aq_styled_wrapped", False):
        return
    from aqueduct.cli.style import colorize_line

    _inner_echo = click.echo

    def _styled_echo(message=None, file=None, nl=True, err=False, color=None):
        if isinstance(message, str):
            message = colorize_line(message)
        return _inner_echo(message, file=file, nl=nl, err=err, color=color)

    _styled_echo._aq_styled_wrapped = True  # type: ignore[attr-defined]
    click.echo = _styled_echo  # type: ignore[assignment]


class _RedactingFilter(logging.Filter):
    """Scrub registered @aq.secret() values from a log record.

    Module-level (not nested inside ``_install_secret_redaction_hooks``): the
    ``isinstance`` idempotency checks below compare against THIS class object,
    which only stays stable across repeated calls if the class itself isn't
    redefined on every call — a nested-class version was previously redefined
    each call, so a same-call ``isinstance`` check against a PRIOR call's
    (different) class object always came back ``False``, silently defeating
    its own dedup guard (masked only by the ``click.echo`` early-return that
    used to skip this whole function on the 2nd+ call).
    """

    def filter(self, record: logging.LogRecord) -> bool:
        from aqueduct.redaction import redact as _redact

        try:
            record.msg = _redact(record.getMessage())
            record.args = ()
            # A logged exception's TRACEBACK TEXT is a separate render path:
            # ``logging.Formatter``/``style.StyledLogFormatter`` both call
            # ``formatException(record.exc_info)`` themselves, after this
            # filter has already run — so redacting only `record.msg` above
            # leaves a secret embedded in the exception (e.g. in a caught
            # HTTPError's URL/body) printing raw. Pre-render + redact it here
            # into `record.exc_text`, which ``logging.Formatter`` already
            # treats as a cache (skips re-formatting if set) —
            # `StyledLogFormatter` honours the same cache.
            if record.exc_info:
                import traceback as _tb

                record.exc_text = _redact("".join(_tb.format_exception(*record.exc_info)))
        except Exception:  # noqa: BLE001
            pass  # redaction must never break logging; best-effort sanitisation
        return True


def _install_secret_redaction_hooks() -> None:
    """Wrap click.echo and the logging chain so registered @aq.secret() values
    are scrubbed from every CLI emit path.

    Idempotent — the wrapped click.echo carries an attribute that signals it
    is already wrapped, so re-wrapping is a no-op. Installed eagerly at
    top-level ``cli`` invocation; commands that never resolve a secret incur a
    tiny per-emit no-op cost (empty registry → fast path).

    The logging half is NOT gated behind the same early return as click.echo:
    a filter on the ROOT LOGGER OBJECT (``root.addFilter``) is only consulted
    when a record originates AT the root logger itself (a bare
    ``logging.warning(...)``) — never during propagation from a NAMED logger
    (``logging.getLogger(__name__)``, used throughout the rest of the
    codebase) to root's handler. That path is only covered by a filter on the
    HANDLER, checked regardless of the record's origin logger. `cli()`
    replaces `root.handlers` on every invocation (``--log-format``/
    ``--verbose``), so this re-attaches to whatever handler exists NOW on
    every call rather than bailing out early — a handler created after the
    first call would otherwise never get the filter.
    """
    import logging as _logging

    from aqueduct.redaction import redact as _redact

    if not getattr(click.echo, "_aq_redaction_wrapped", False):
        _orig_echo = click.echo

        def _wrapped_echo(message=None, file=None, nl=True, err=False, color=None):
            if isinstance(message, str):
                message = _redact(message)
            return _orig_echo(message, file=file, nl=nl, err=err, color=color)

        _wrapped_echo._aq_redaction_wrapped = True  # type: ignore[attr-defined]
        click.echo = _wrapped_echo  # type: ignore[assignment]

    root = _logging.getLogger()
    if not any(isinstance(f, _RedactingFilter) for f in root.filters):
        root.addFilter(_RedactingFilter())
    for _handler in root.handlers:
        if not any(isinstance(f, _RedactingFilter) for f in _handler.filters):
            _handler.addFilter(_RedactingFilter())


class _AqueductJsonLogFormatter:
    """Minimal JSON log formatter for `--log-format json`.

    Emits one line of JSON per record with the canonical fields ops teams need
    when shipping to Loki / Splunk / Datadog: timestamp (ISO-8601 UTC), level,
    logger name, message (already %-formatted), plus exc_info when present.

    Implemented as a class with a `format` method (duck-typed to the stdlib
    Formatter interface) rather than subclassing `logging.Formatter` so we
    avoid pulling logging into the CLI import path unnecessarily.
    """

    # Stdlib LogRecord attributes — anything NOT in this set is treated as a
    # caller-supplied `extra=` field and merged into the JSON payload. Keeps
    # the schema open-ended without manually enumerating every domain key.
    _STANDARD_LOGRECORD_ATTRS = frozenset(
        {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "message",
            "asctime",
            "taskName",
        }
    )

    def format(self, record) -> str:  # noqa: D401
        import json as _json
        import logging as _logging
        from datetime import datetime as _dt

        payload = {
            "ts": _dt.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Merge caller-supplied `extra={...}` fields (`run_id`, `blueprint_id`,
        # `module_id`, etc.) into the payload so structured-log consumers can
        # filter on them. Standard LogRecord attributes are skipped.
        for key, value in record.__dict__.items():
            if key in self._STANDARD_LOGRECORD_ATTRS or key in payload:
                continue
            if key.startswith("_"):
                continue
            payload[key] = value
        if record.exc_info:
            payload["exc"] = _logging.Formatter().formatException(record.exc_info)
        return _json.dumps(payload, default=str)


# ── Missing-optional-engine-extra hint ──────────────────────────────────────
# The concrete bug this guards against: `deployment.engine: spark` selected
# without the `[spark]` extra installed raises a bare `ModuleNotFoundError`
# (pyspark) from deep inside a lazy engine wrapper — the SAME failure shape
# as the duckdb-without-pyspark bug this CLI safety net (below) exists for,
# just via the legitimate path (a real engine choice missing its runtime,
# not an accidental cross-engine import). Kept as a small, explicit map
# (rather than trying to derive it from `capabilities.yml`/entry points)
# because it only needs to answer one question — "does this import name
# belong to a KNOWN optional-extra runtime?" — and guessing wrong here would
# produce a misleading hint, which is worse than the generic fallback below.
_MISSING_EXTRA_BY_MODULE: dict[str, str] = {
    "pyspark": "spark",
    "delta": "spark",  # delta-spark's import name
    "py4j": "spark",  # pyspark's own JVM-bridge dependency
}


def _missing_engine_extra(exc: BaseException) -> tuple[str, str] | None:
    """If *exc* is an ``ImportError``/``ModuleNotFoundError`` for a module
    that belongs to a known optional engine extra, return
    ``(module_name, extra_name)``; else ``None``."""
    if not isinstance(exc, ImportError):
        return None
    name = getattr(exc, "name", None) or ""
    top = name.split(".")[0]
    extra = _MISSING_EXTRA_BY_MODULE.get(top)
    return (top, extra) if extra else None


class _AqueductCLIGroup(click.Group):
    """Last-resort safety net around the whole CLI dispatch (AGENTS.md "no
    silent no-ops").

    Click's own ``BaseCommand.main()`` only catches ``ClickException`` /
    ``Abort`` / ``EOFError`` / ``KeyboardInterrupt``. Any OTHER exception
    escaping a subcommand propagates as a raw, unstyled Python traceback in
    a real terminal — and, under Click's own ``CliRunner`` (every test in
    this suite, plus any Airflow/orchestrator caller that drives the CLI
    in-process), is swallowed into exit code 1 with the exception recorded
    on ``result.exception`` but NOTHING written to ``result.output`` —
    genuinely silent from the caller's point of view. That is the exact
    shape of the reported bug: ``aqueduct run`` on a duckdb-engine Blueprint,
    pyspark not installed, crashed with ``exit: 1 | output len: 0``.

    This is a NET, not a substitute for typed handling — every command
    already converts its OWN expected failure modes (``ParseError``,
    ``CompileError``, ``ConfigError``, ``ExecuteError``, ...) into a clean
    message + the right ``exit_codes.*`` constant; this only catches what
    slips through that, so the floor is "always some message, always a
    documented exit code" rather than "always a perfectly specific one".
    """

    def invoke(self, ctx: click.Context) -> Any:
        try:
            return super().invoke(ctx)
        except (
            SystemExit,
            KeyboardInterrupt,
            click.exceptions.Abort,
            click.ClickException,
            # `click.exceptions.Exit` is NOT a SystemExit — its MRO is
            # Exit -> RuntimeError -> Exception — and it is click's normal,
            # SUCCESSFUL exit signal, raised by `ctx.exit(code)` and by the
            # `--help` / eager-param machinery. So a broad `except Exception`
            # in an `invoke` override captures clean exits, not just failures:
            # every subcommand `--help` and the bare `aqueduct` banner reported
            # `✗ unexpected error: 0` and exit 2, where the `0` was
            # `str(Exit(0))` — the code the command ASKED for, rendered as
            # error text. (`aqueduct --help`/`--version` looked fine only
            # because click resolves the ROOT group's eager params before
            # entering `Group.invoke` at all.) Re-raise so the requested code
            # survives — this is a control-flow signal, never an error.
            click.exceptions.Exit,
        ):
            raise
        except Exception as exc:  # noqa: BLE001 — last-resort net, see class docstring
            from aqueduct import exit_codes as _exit_codes

            missing = _missing_engine_extra(exc)
            if missing is not None:
                _module, _extra = missing
                click.echo(
                    f"✗ {_module!r} is not installed — this engine needs the "
                    f"matching extra: pip install aqueduct-core[{_extra}]",
                    err=True,
                )
            else:
                click.echo(f"✗ unexpected error: {exc}", err=True)
            ctx.exit(_exit_codes.DATA_OR_RUNTIME)


@click.group(cls=_AqueductCLIGroup, invoke_without_command=True, no_args_is_help=False)
@click.version_option(
    version=_aqueduct_version,
    prog_name="aqueduct",
    message="%(prog)s %(version)s",
)
@click.option(
    "-v",
    "--verbose",
    "verbosity",
    count=True,
    help=(
        "Increase Aqueduct-side output detail. Repeatable: -v = full narrative "
        "(untruncated errors/warnings, uncollapsed doctor rows, uncapped probe "
        "notes, transcript detail); -vv = also show the raw layer (engine/Spark "
        "startup + log4j output, prompt text, streamed model text). Placed "
        "before OR after the subcommand (`aqueduct -v run bp.yml` / "
        "`aqueduct run -v bp.yml`) — the effective level is the max of both. "
        "Does NOT enable DEBUG logging; use --debug for that."
    ),
)
@click.option(
    "--debug",
    is_flag=True,
    default=False,
    help="Enable Python DEBUG logging (root logger — library/framework internals, "
    "distinct from -v's Aqueduct-side output tiers).",
)
@click.option(
    "--log-format",
    "log_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
    help=(
        "Logging output format. text=human-readable single line (default). "
        "json=one JSON object per record with ts/level/logger/msg fields — "
        "use when shipping logs to Loki / Splunk / Datadog."
    ),
)
@click.option(
    "--suppress-warning",
    "suppress_warnings",
    multiple=True,
    metavar="RULE_ID",
    help=(
        "Silence an Aqueduct warning by rule_id (copy from the `AQ-WARN [...]` "
        "prefix). Repeatable. Use `--suppress-warning '*'` to silence ALL. "
        "Merged with `warnings.suppress` from aqueduct.yml. Goes BEFORE the "
        "subcommand: `aqueduct --suppress-warning '*' run bp.yml`."
    ),
)
@click.pass_context
def cli(
    ctx: click.Context,
    verbosity: int,
    debug: bool,
    log_format: str,
    suppress_warnings: tuple[str, ...],
) -> None:
    """Aqueduct — Intelligent Spark Blueprint Engine."""
    import logging

    from aqueduct.warnings import install_cli_formatter, set_default_suppress

    level = logging.DEBUG if debug else logging.WARNING

    if log_format.lower() == "json":
        handler = logging.StreamHandler()
        handler.setFormatter(_AqueductJsonLogFormatter())  # type: ignore[arg-type]
        # Replace any handlers basicConfig may have installed; idempotent.
        root = logging.getLogger()
        root.handlers.clear()
        root.addHandler(handler)
        root.setLevel(level)
    else:
        from aqueduct.cli.style import StyledLogFormatter

        handler = logging.StreamHandler()
        handler.setFormatter(StyledLogFormatter(verbose=debug))

        class _RuntimeNestedFilter(logging.Filter):
            """Probe/Assert/Retry runtime warnings are displayed nested under
            their module by `run` (the `↳ [rule_id]` lines). Drop their loose
            console line here so they aren't printed twice. They remain in
            the logger for `--log-format json` and pytest's caplog (separate
            handlers).

            Phase 85 F-15 added the four `runtime_retry_*` rule_ids that now
            ALSO go through the per-module collector
            (`executor/models.py::_add_module_warning`) via
            `executor/spark/executor.py` and `executor/duckdb_/executor.py`'s
            `_with_retry` — hence the exact (bracket-closed) matches below,
            not a bare `"[runtime_retry"` prefix. `runtime_retry_exhausted_alert`
            is deliberately EXCLUDED: it fires after that module's
            `ModuleResult` has already been built (see the comment at its
            call site in both executors), so it is never routed through the
            collector and must keep printing here or it would vanish
            entirely — only the four still-collected rule_ids are hidden."""

            _HIDDEN_PREFIXES = ("[runtime_probe", "[runtime_assert")
            _HIDDEN_EXACT = (
                "[runtime_retry_deadline]",
                "[runtime_retry_exhausted]",
                "[runtime_retry_non_retriable]",
                "[runtime_retry_waiting]",
            )

            def filter(self, record: logging.LogRecord) -> bool:
                m = record.getMessage()
                if any(p in m for p in self._HIDDEN_PREFIXES):
                    return False
                if any(e in m for e in self._HIDDEN_EXACT):
                    return False
                return True

        handler.addFilter(_RuntimeNestedFilter())
        root = logging.getLogger()
        root.handlers.clear()
        root.addHandler(handler)
        root.setLevel(level)

    # Install AQ-WARN [rule_id] format + stash CLI suppress overrides.
    # Engine-level `warnings.suppress` from aqueduct.yml is merged later, once a
    # command actually loads config (commands that never read config still
    # honour the CLI flag).
    install_cli_formatter()
    set_default_suppress(suppress=list(suppress_warnings))
    ctx.ensure_object(dict)
    ctx.obj["suppress_warnings_cli"] = list(suppress_warnings)
    ctx.obj["verbosity"] = verbosity

    _install_secret_redaction_hooks()

    # Outer wrapper over redaction — colour the icon vocabulary on every status
    # line (text mode only; JSON output must stay un-styled).
    if log_format.lower() != "json":
        _install_styled_echo()

    # Bare `aqueduct` (no subcommand) → branded banner above the help.
    if ctx.invoked_subcommand is None:
        click.echo(_render_banner(), err=False)
        click.echo(ctx.get_help(), err=False)
        ctx.exit()


def _render_banner() -> str:
    """Small branded wordmark for the bare `aqueduct` command (not per-run)."""
    aq = click.style("aq", fg="red", bold=True)
    ueduct = click.style("ueduct", fg="yellow", bold=True)  # sand
    arches = click.style("∩∩∩", fg="cyan")
    tag = click.style("declarative · self-healing · Apache Spark", dim=True)
    ver = click.style(f"v{_aqueduct_version}", dim=True)
    return f"\n  {arches}  {aq}{ueduct}  {ver}\n  {tag}\n"


# ── patch helpers ────────────────────────────────────────────────────────────


def _uncommitted_applied_patches(
    blueprint_path: Path, patches_root: Path, blueprint_id: str | None = None
) -> list[Path]:
    """Return applied patches with applied_at newer than the last git commit for blueprint_path.

    Falls back to returning all applied patches when not in a git repo or blueprint
    has never been committed. When ``blueprint_id`` is given, only patches OWNED by
    that blueprint are considered — the ``patches/applied/`` dir is shared across a
    project, so without this filter running blueprint B would warn about (and
    mis-suggest committing) blueprint A's patches.
    """
    import subprocess

    applied_dir = patches_root / "applied"
    if not applied_dir.exists():
        return []

    all_applied = sorted(applied_dir.glob("*.json"), key=lambda f: f.stat().st_mtime)
    if not all_applied:
        return []

    # Keep only patches owned by this blueprint (via _aq_meta.blueprint_id).
    # Patches without a recorded blueprint_id are kept (conservative).
    if blueprint_id is not None:
        from aqueduct.patch.grammar import PATCH_META_KEY as _PMK

        owned = []
        for _p in all_applied:
            try:
                _d = json.loads(_p.read_text(encoding="utf-8"))
                _meta = _d.get(_PMK) if isinstance(_d, dict) else None
                _bp = _meta.get("blueprint_id") if isinstance(_meta, dict) else None
            except Exception:
                # Unreadable/malformed applied-patch file: treat like "no recorded
                # blueprint_id" (conservative — kept, per the comment above) rather
                # than crashing this safety scan or silently dropping the patch.
                _bp = None
            if _bp is None or _bp == blueprint_id:
                owned.append(_p)
        all_applied = owned
        if not all_applied:
            return []

    # Get ISO timestamp of last git commit touching this blueprint.
    # Tolerate environments without git (containerized workers, etc.) — the
    # check is informational; falling back to "treat all as uncommitted"
    # preserves the safety semantics without breaking the run.
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%cI", "--", str(blueprint_path)],
            capture_output=True,
            text=True,
        )
        last_commit_ts: str | None = result.stdout.strip() if result.returncode == 0 else None
    except (FileNotFoundError, PermissionError, OSError):
        last_commit_ts = None

    if not last_commit_ts:
        # Not in git or never committed — treat everything as uncommitted
        return all_applied

    uncommitted = []
    from datetime import datetime

    from aqueduct.patch.grammar import PATCH_META_KEY

    for p in all_applied:
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue  # unreadable/malformed patch file — skip rather than abort the scan
        if not isinstance(data, dict):
            continue
        # applied_at may be top-level or inside _aq_meta
        _meta = data.get(PATCH_META_KEY)
        applied_at_str = data.get("applied_at") or (
            _meta.get("applied_at") if isinstance(_meta, dict) else None
        )
        if not applied_at_str:
            continue

        try:
            # Use fromisoformat which handles the Z and offset formats in Python 3.11+
            # For older versions, we might need to replace 'Z' with '+00:00'
            applied_at = datetime.fromisoformat(applied_at_str.replace("Z", "+00:00"))
            last_commit = datetime.fromisoformat(last_commit_ts.replace("Z", "+00:00"))

            if applied_at > last_commit:
                uncommitted.append(p)
        except (ValueError, TypeError):
            # Fallback to string comparison if parsing fails for some reason
            if applied_at_str > last_commit_ts:
                uncommitted.append(p)

    return uncommitted


# ── patch helpers ────────────────────────────────────────────────────────────


def _patches_root_from_blueprint(blueprint_path: Path) -> Path:
    """Return <project_root>/patches by walking up from blueprint to find aqueduct.yml."""
    _search = blueprint_path.parent
    project_root = blueprint_path.parent
    for _ in range(_PROJECT_ROOT_MAX_DEPTH):
        if (_search / _DEFAULT_CONFIG_FILENAME).exists():
            project_root = _search
            break
        if _search.parent == _search:
            break
        _search = _search.parent
    return project_root / "patches"


if __name__ == "__main__":
    cli()

# ── extracted command families (registered + re-exported) ──────────────────────
from .benchmark import benchmark, benchmark_diff_cmd, benchmark_stats_cmd  # noqa: E402,F401
from .compile_cmd import compile  # noqa: E402,F401
from .dev import (  # noqa: E402,F401
    capabilities_check,
    capabilities_docs,
    capabilities_scaffold,
    capabilities_sync,
    dev_capabilities,
    dev_group,
    dev_scaffold,
)
from .diagnostics import doctor, lint_cmd, schema, validate  # noqa: E402,F401
from .drift import drift  # noqa: E402,F401
from .handoff import handoff, handoff_sweep  # noqa: E402,F401
from .heal import heal  # noqa: E402,F401
from .observability import lineage, report, runs, signal  # noqa: E402,F401
from .patch import (  # noqa: E402,F401,F811
    log_cmd,
    patch,
    patch_apply,
    patch_commit,
    patch_discard,
    patch_list,
    patch_policy,
    patch_preview,
    patch_reject,
    rollback_cmd,
)
from .project import completion_cmd, init, test_cmd  # noqa: E402,F401
from .run import run  # noqa: E402,F401
from .stores import stores_group, stores_info, stores_migrate  # noqa: E402,F401
