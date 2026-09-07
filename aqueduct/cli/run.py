"""`run` commands — extracted verbatim from aqueduct/cli/__init__.py.

No behaviour change. The click group + shared helpers come from the package;
commands register onto `cli` when imported at the bottom of __init__.
"""

from __future__ import annotations

import click

from aqueduct.cli import (
    _env_options,
    cli,
)
from aqueduct.cli.run_phases import (
    RunContext,
    acquire_run_lock,
    check_from_to_island_guard,
    check_resume_hash_guard,
    finalize_surveyor_and_depot,
    fire_success_webhook,
    handle_failure_exit,
    print_success_footer_and_hooks,
    render_runtime_warnings,
    run_heal_loop,
    run_sandbox_dryrun,
    stamp_success_provenance,
)
from aqueduct.cli.run_setup import (
    _do_compile,
    _load_engine_config,
    _setup_surveyor,
)
from aqueduct.models import ModuleType

# ── Phase 85 Wave 2 — classified failure label (SCREEN 2/6) ─────────────────
# `mr.error` is free text; the ✗ line wants a SHORT classified label ("SQL
# binder error") with the full message wrapped underneath, not a bare
# 300-char truncated one-liner. `docs/failure_taxonomy.md` catalogs
# recurring dev-facing BUG classes for audits — it has no per-message
# runtime classification to reuse. `FailureContext.error_class` DOES: it is
# the SAME structured field `_extract_structured_error`
# (`aqueduct/surveyor/error_extraction.py` for Spark,
# `aqueduct/executor/duckdb_/error_extraction.py` for DuckDB) already
# populates from the concrete exception class name (DuckDB) or Spark
# condition string. This table maps that existing field to a short label —
# no parallel taxonomy, just a display lookup over data Aqueduct already
# extracts.
_ERROR_CLASS_LABELS: dict[str, str] = {
    "BinderException": "SQL binder error",
    "CatalogException": "missing table or column",
    "ParserException": "SQL syntax error",
    "SyntaxException": "SQL syntax error",
    "ConversionException": "type conversion error",
    "ConstraintException": "constraint violation",
    "IOException": "I/O error",
    "TransactionException": "transaction error",
    "AnalysisException": "analysis error",
}


def _classify_error_label(error_class: str | None) -> str:
    """Short classified label for the ✗ failure line. See the module-level
    note above ``_ERROR_CLASS_LABELS`` for where ``error_class`` comes from
    and why no new taxonomy was invented."""
    ec = (error_class or "").strip()
    if ec in _ERROR_CLASS_LABELS:
        return _ERROR_CLASS_LABELS[ec]
    if ec.startswith("UNRESOLVED_COLUMN"):
        return "unresolved column"
    if ec == "PREDICTED_SCHEMA_DRIFT":
        return "schema drift detected"
    if ec:
        import re as _re

        words = _re.findall(r"[A-Z][a-z0-9]*|[A-Z0-9]+", ec.replace(".", " "))
        if words:
            return " ".join(w.lower() for w in words[:4])
    return "execution error"


# ── F-16 — print validation gates AS THEY RUN (SCREEN 3/7's numbered
# ladder) ────────────────────────────────────────────────────────────────
# Display-only: `_run_patch_gates_inline` (`aqueduct/cli/__init__.py`, W7-
# owned — not edited here) already computes lineage/sandbox/
# resolvability results; this only renders them. Copies `cli/patch.py::_gate_status_line`
# icon-per-`GateStatus` PATTERN (dict-dispatch, not an import from
# patch.py, which Wave 2 does not own) — with an explicit fallback for an
# unrecognised status so a future `GateStatus` member (e.g. the sandbox
# tri-state `NOT_REQUESTED`) never crashes or silently vanishes
# (AGENTS.md "no silent no-ops").
_GATE_ICON: dict[str, tuple[str, str]] = {
    "pass": ("✓", "green"),
    "warn": ("⚠", "yellow"),
    "fail": ("✗", "red"),
    "not_applicable": ("·", "bright_black"),
    "unavailable": ("⊘", "yellow"),
    "observed": ("·", "bright_black"),
    "not_requested": ("·", "bright_black"),
}


def _gate_icon(status: str | None) -> tuple[str, str]:
    if status in _GATE_ICON:
        return _GATE_ICON[status]
    return "⚠", "yellow"  # unknown status — never silently vanish


def _print_gate_ladder(g2, g3, g4, *, verbosity: int, gate1_ok: bool = True) -> None:
    """Print gate 1-4 outcomes for one candidate patch as they complete.

    ``g2``/``g3``/``g4`` are ``lineage_res``/``sandbox_res``/
    ``resolvability_res`` from ``_run_patch_gates_inline``
    (``None`` when that gate did not run). Gate 1 (policy/guardrails) already ran
    in-loop via ``_check_guardrails`` before a candidate ever reaches here,
    so ``gate1_ok`` is a plain bool, not a ``GateStatus``. Default: one
    compact line, collapsing to "gates 1-4 passed" when nothing failed or
    warned. ``-v``: one line per gate.
    """
    entries = [(1, "policy", "pass" if gate1_ok else "fail", None)]
    for num, name, res in (
        (2, "lineage", g2),
        (3, "sandbox", g3),
        (4, "resolvability", g4),
    ):
        if res is None:
            continue
        entries.append((num, name, res.status, getattr(res, "detail", None)))

    all_clean = all(status in ("pass", "not_applicable", "observed") for _, _, status, _ in entries)
    if verbosity < 1:
        if all_clean:
            click.echo(
                click.style(f"  ✓ gates {entries[0][0]}-{entries[-1][0]} passed", fg="green"),
                err=True,
            )
            return
        bits = []
        for num, name, status, _detail in entries:
            icon, color = _gate_icon(status)
            bits.append(click.style(f"{num} {name} {icon}", fg=color))
        click.echo("  · gates: " + "  ".join(bits), err=True)
        return
    for num, name, status, detail in entries:
        icon, color = _gate_icon(status)
        line = click.style(f"  {num} {name}", fg=color) + f"  {click.style(icon, fg=color)}"
        if detail and status not in ("pass", "not_applicable", "observed"):
            line += f"  {detail}"
        click.echo(line, err=True)


# The `compile` command and its four private rendering helpers
# (`_render_compile_show`, `_format_inputs_fingerprint`,
# `_format_provenance_table`, `_format_provenance_rows`) moved to
# `aqueduct/cli/compile_cmd.py` (Phase 85 Wave 5 split) — self-contained,
# never called from `run()`.


# `_LoadConfigResult`, `_load_engine_config`,
# `_CompileResult`, `_do_compile`,
# `_SessionHolder`, `_SurveyorSetupResult`, `_setup_surveyor` moved to
# `aqueduct/cli/run_setup.py` (Phase 85 Wave 5 split) — module-level
# functions with no closure over run()'s locals, imported below.


@cli.command()
@click.argument("blueprint", type=click.Path(exists=True, dir_okay=False))
@click.option("-p", "--profile", default=None, help="Context profile to activate")
@click.option(
    "--ctx",
    multiple=True,
    metavar="KEY=VALUE",
    help="Context override. Repeatable.",
)
@click.option("--run-id", default=None, help="Run identifier (auto-generated UUID if omitted)")
@click.option(
    "--config",
    "config_path",
    default=None,
    type=click.Path(dir_okay=False),
    help="Path to aqueduct.yml (default: aqueduct.yml in CWD)",
)
@click.option(
    "--store-dir",
    default=None,
    help="Store directory (overrides aqueduct.yml; default: .aqueduct",
)
@click.option(
    "--webhook", default=None, help="Webhook URL for failure notifications (overrides aqueduct.yml)"
)
@click.option(
    "--resume", "resume_run_id", default=None, help="Resume from checkpoints of a previous run_id"
)
@click.option(
    "--force",
    "force_resume",
    is_flag=True,
    default=False,
    help="With --resume: proceed even when the checkpointed run's manifest hash differs "
    "from this run's compiled Manifest (fail-closed by default — see --resume). "
    "Invalid without --resume.",
)
@click.option(
    "--from",
    "from_module",
    default=None,
    metavar="MODULE_ID",
    help="Start execution at this module (skip all preceding modules)",
)
@click.option(
    "--to",
    "to_module",
    default=None,
    metavar="MODULE_ID",
    help="Stop execution after this module (skip all subsequent modules)",
)
@click.option(
    "--execution-date",
    "execution_date_str",
    default=None,
    metavar="YYYY-MM-DD",
    help="Logical execution date for @aq.date.* functions — enables idempotent backfills",
)
@click.option(
    "--allow-multi-patch",
    "allow_multi_patch_flag",
    is_flag=True,
    default=False,
    help="Allow `max_patches > 1` for this run (overrides danger.allow_multi_patch=false).",
)
@_env_options
@click.option(
    "--parallel",
    is_flag=True,
    default=False,
    help="Execute independent DAG branches concurrently (one thread per connected component). "
    "Only beneficial when the Blueprint has multiple fully-independent source trees.",
)
@click.option(
    "-v",
    "--verbose",
    "verbose",
    count=True,
    help="Increase output detail (repeatable: -v, -vv). Also honoured when "
    "given on the root group instead (`aqueduct -v run ...`) — the effective "
    "level is the max of both. -v = full Aqueduct-side story (untruncated "
    "errors/warnings, uncapped probe notes, transcript detail); -vv = also "
    "show the raw layer (full Spark/JVM startup banner — incubator notice, "
    "log4j init, NativeCodeLoader — plus prompt text and streamed model "
    "text). See `aqueduct --help` for the full tier description.",
)
@click.option(
    "--sandbox",
    is_flag=True,
    default=False,
    help="Dev dry-run: compile + execute against sampled inputs with every Egress "
    "skipped (no writes). No self-healing, no observability persistence. Fast "
    "feedback loop for iterating on transforms.",
)
@click.option(
    "--sample",
    default=1000,
    show_default=True,
    type=int,
    help="Row cap per Ingress in --sandbox mode (0 = no limit). Ignored without --sandbox.",
)
@click.option(
    "-s",
    "--set",
    "set_items",
    multiple=True,
    metavar="PATH=VALUE",
    help="Override a config or blueprint value for this run only (repeatable, "
    "in-memory, never persisted). Dotted path — e.g. "
    "--set agent.approval=auto --set engine.spark.master_url=spark://h:7077. "
    "Values coerce to bool/int/float/null else string; use PATH:=JSON for "
    "structured values. Highest precedence (beats blueprint + aqueduct.yml).",
)
@click.option(
    "--wait-for-lock",
    is_flag=True,
    default=False,
    help="Queue behind a concurrent run of the same Blueprint instead of "
    "refusing. Without it, a second run exits immediately naming the holder.",
)
def run(
    blueprint: str,
    profile: str | None,
    ctx: tuple[str, ...],
    run_id: str | None,
    config_path: str | None,
    store_dir: str | None,
    webhook: str | None,
    resume_run_id: str | None,
    from_module: str | None,
    to_module: str | None,
    execution_date_str: str | None,
    verbose: int = 0,
    allow_multi_patch_flag: bool = False,
    env_file: str | None = None,
    cli_env: tuple[str, ...] = (),
    parallel: bool = False,
    sandbox: bool = False,
    sample: int = 1000,
    set_items: tuple[str, ...] = (),
    force_resume: bool = False,
    wait_for_lock: bool = False,
) -> None:
    """Compile and execute a Blueprint on a SparkSession."""
    import contextlib
    import os
    from pathlib import Path

    # `--force` is only meaningful alongside `--resume` (it overrides the
    # fail-closed manifest-hash check below) — a bare `--force` is a usage
    # mistake, not a config error, so this is Click's own `UsageError`
    # (exit code exit_codes.USAGE_ERROR == 64, same taxonomy as an unknown
    # flag — see the exit-code patch in `aqueduct/cli/__init__.py`), raised
    # before any real work (chdir, config load, compile) happens.
    if force_resume and not resume_run_id:
        raise click.UsageError("--force is only valid together with --resume")

    from aqueduct.cli.verbosity import resolve_verbosity
    from aqueduct.executor.models import ExecutionStatus

    # Effective verbosity = max(root `-v` count, this command's own `-v`
    # count) — see aqueduct/cli/verbosity.py for the tier semantics. `verbose`
    # (the local count, kept for Click's postfix `run -v` support) is not
    # used again below this point; every consumer reads `verbosity`.
    verbosity = resolve_verbosity(local=verbose)

    # Phase 85 Wave 2 — total wall-clock time on the closing footer (SCREEN
    # 1/2's "one added line vs today"). Wall-clock deliberately, not a sum of
    # per-module `duration_ms` — those are best-effort obs-store reads (empty
    # on DuckDB today, see `_render_module_summary`'s docstring) and never
    # include heal/gate time between iterations anyway.
    import time as _time85

    _run_started_at = _time85.monotonic()

    # ── Anchor CWD to project root ────────────────────────────────────────────
    # Resolve all CLI-supplied paths to absolute BEFORE chdir so that relative
    # flags like --config ../shared/aqueduct.yml keep their original meaning.
    #
    # Project root = the directory containing aqueduct.yml.  We find it by:
    #   1. If --config is given, use that file's parent dir.
    #   2. Otherwise walk up from the blueprint file until aqueduct.yml is found
    #      (up to 8 levels), falling back to the blueprint's own directory.
    #
    # After chdir, relative paths in Blueprint YAML (e.g. "data/input/*.parquet")
    # resolve from the project root regardless of where the CLI was invoked.
    blueprint_abs = Path(blueprint).resolve()
    config_path_abs = Path(config_path).resolve() if config_path else None
    store_dir_abs = Path(store_dir).resolve() if store_dir else None

    if config_path_abs:
        _project_root = config_path_abs.parent
    else:
        from aqueduct.cli import _resolve_project_root

        _project_root = _resolve_project_root(blueprint_path=blueprint_abs)

    _original_cwd = os.getcwd()
    os.chdir(_project_root)
    # Everything entered here is released in the outer `finally`, on every
    # exit path: normal return, exception, or `sys.exit` (SystemExit is a
    # BaseException, which `finally` still honours).
    _run_stack = contextlib.ExitStack()
    try:
        _lcr = _load_engine_config(
            blueprint_abs=blueprint_abs,
            config_path_abs=config_path_abs,
            store_dir_abs=store_dir_abs,
            webhook=webhook,
            set_items=set_items,
            env_file=env_file,
            cli_env=cli_env,
            _project_root=_project_root,
        )
        blueprint = _lcr.blueprint_str
        cfg = _lcr.cfg
        resolved_store_dir = _lcr.resolved_store_dir
        resolved_webhook = _lcr.resolved_webhook
        engine = _lcr.engine
        master_url = _lcr.master_url
        probe_sampling = _lcr.probe_sampling
        blueprint_set_nested = _lcr.blueprint_set_nested
        _using_default_obs_path = _lcr._using_default_obs_path
        _obs_routing_base = _lcr._obs_routing_base
        execute = _lcr.execute

        # `checkpoint_root` (aqueduct.yml) overrides the derived
        # `<store_dir>/checkpoints/` location. Config-load already rejected
        # remote URI schemes; resolve a relative local path against the
        # project root (CWD, post-chdir above) for consistency with other
        # config-file path handling.
        checkpoint_root_abs = Path(cfg.checkpoint_root).resolve() if cfg.checkpoint_root else None

        # `handoff.root` — same anchoring concern as `checkpoint_root`
        # above, and the same "Path anchoring" bug family (AGENTS.md): a
        # RELATIVE `handoff.root` (the default, `.aqueduct/handoff`) must
        # resolve against the project root (CWD, post-chdir), never against
        # whatever directory the engine session happens to have been
        # constructed in. Left unresolved, a Spark session created before
        # this process's chdir (a long-lived/shared session — the exact
        # shape a real cluster driver or a reused session takes) writes a
        # relative path against ITS OWN JVM `user.dir`, while a
        # freshly-`os.chdir()`-aware Python-side reader (DuckDB) resolves
        # the SAME relative string against the CURRENT cwd — two different
        # absolute locations for what is supposed to be one shared spill
        # directory, silently. A remote URI (s3://, gs://, …) is passed
        # through untouched — there is no "CWD" to anchor a URI against.
        from aqueduct.executor.spill import is_remote_uri as _is_remote_uri

        _handoff_root_abs = (
            cfg.handoff.root
            if _is_remote_uri(cfg.handoff.root)
            else str(Path(cfg.handoff.root).resolve())
        )

        # ── Resolution preamble — surface the non-default inputs shaping this run
        # (dim info lines next to the `· env ·` notice). Keys only for --set:
        # values may embed secrets that were never registered for redaction.
        from aqueduct.cli.render.style import info as _preamble_info

        _over_parts = []
        if set_items:
            _set_keys = ", ".join(i.partition("=")[0].strip() for i in set_items)
            _over_parts.append(f"--set {_set_keys}")
        if ctx:
            _over_parts.append(f"--ctx {len(ctx)} key(s)")
        if profile:
            _over_parts.append(f"profile: {profile}")
        if _over_parts:
            _preamble_info("· overrides  ·  " + "  ·  ".join(_over_parts), err=True)
        if cfg.secrets.provider != "env":
            _preamble_info(f"· secrets  ·  provider: {cfg.secrets.provider}", err=True)
        if _lcr.danger_pairs:
            from aqueduct.cli.render.style import emit_warning_pairs

            emit_warning_pairs(list(_lcr.danger_pairs), label="danger:", err=True)

        _cr = _do_compile(
            blueprint=blueprint,
            profile=profile,
            ctx=ctx,
            execution_date_str=execution_date_str,
            store_dir_abs=store_dir_abs,
            cfg=cfg,
            verbosity=verbosity,
            blueprint_set_nested=blueprint_set_nested,
        )
        manifest = _cr.manifest
        bundle = _cr.bundle
        depot = _cr.depot
        execution_date = _cr.execution_date
        cli_overrides = _cr.cli_overrides

        check_resume_hash_guard(
            resume_run_id=resume_run_id,
            force_resume=force_resume,
            manifest=manifest,
            checkpoint_root_abs=checkpoint_root_abs,
            resolved_store_dir=resolved_store_dir,
            handoff_root_abs=_handoff_root_abs,
        )

        check_from_to_island_guard(
            manifest=manifest,
            from_module=from_module,
            to_module=to_module,
        )

        # ── Sandbox dry-run (short-circuit) ──────────────────────────────────────
        # Dev loop: run the compiled pipeline against sampled inputs with every
        # Egress skipped — no writes, no Surveyor, no self-healing, no
        # observability persistence. Reuses the patch-validation sandbox
        # transform so behaviour matches Gate 3.
        #
        # Engine-agnostic (Phase 89 add-on): built THROUGH THE PROTOCOL
        # REGISTRY — `get_protocol(engine).session_factory()` +
        # `resolve_session_engine_config` — the same seam
        # `aqueduct.patch.preview.run_sandbox_gate` (Gate 3's own sandbox
        # replay) and the main run path's `_execute_target` already use,
        # rather than a hardcoded `make_spark_session()` that made
        # `--sandbox` reachable only for `engine=spark` regardless of which
        # engines were actually registered.
        if sandbox:
            run_sandbox_dryrun(
                manifest=manifest,
                engine=engine,
                run_id=run_id,
                sample=sample,
                master_url=master_url,
                verbosity=verbosity,
                cfg=cfg,
                depot=depot,
                from_module=from_module,
                to_module=to_module,
                parallel=parallel,
                probe_sampling=probe_sampling,
                execute=execute,
            )

        # ── Per-blueprint run lock ────────────────────────────────────────────
        # Two concurrent runs of one Blueprint share an observability store
        # and a depot. DuckDB serialises their statements so neither crashes,
        # but they still interleave logically (two run_records rows growing
        # at once, two heal loops writing the same depot keys). Take the lock
        # BEFORE the Surveyor exists, since Surveyor setup is itself the
        # first writer, and hold it for the rest of the run: `_run_stack`
        # closes in the outer `finally` below, so an exception or a
        # `sys.exit` releases it too.
        resolved_store_dir = acquire_run_lock(
            resolved_store_dir=resolved_store_dir,
            obs_routing_base=_obs_routing_base,
            manifest=manifest,
            bundle=bundle,
            wait_for_lock=wait_for_lock,
            run_stack=_run_stack,
        )

        _ssr = _setup_surveyor(
            resolved_store_dir=resolved_store_dir,
            manifest=manifest,
            cfg=cfg,
            _obs_routing_base=_obs_routing_base,
            _using_default_obs_path=_using_default_obs_path,
            verbosity=verbosity,
            allow_multi_patch_flag=allow_multi_patch_flag,
            _project_root=_project_root,
            blueprint_str=blueprint,
            run_id=run_id,
            from_module=from_module,
            to_module=to_module,
            execution_date=execution_date,
            engine=engine,
            master_url=master_url,
            resolved_webhook=resolved_webhook,
            bundle=bundle,
            depot=depot,
            compile_warnings=_cr.compile_warnings,
        )
        resolved_store_dir = _ssr.resolved_store_dir
        patches_dir = _ssr.patches_dir
        run_id = _ssr.run_id
        approval_mode = _ssr.approval_mode
        max_patches = _ssr.max_patches
        _is_multi_patch = _ssr._is_multi_patch
        resolved_agent_provider = _ssr.resolved_agent_provider
        resolved_agent_base_url = _ssr.resolved_agent_base_url
        resolved_agent_model = _ssr.resolved_agent_model
        resolved_agent_provider_options = _ssr.resolved_agent_provider_options
        resolved_agent_timeout = _ssr.resolved_agent_timeout
        resolved_agent_max_reprompts = _ssr.resolved_agent_max_reprompts
        resolved_agent_api_key = _ssr.resolved_agent_api_key
        resolved_agent_engine_prompt_context = _ssr.resolved_agent_engine_prompt_context
        resolved_agent_blueprint_prompt_context = _ssr.resolved_agent_blueprint_prompt_context
        resolved_agent_cascade = _ssr.resolved_agent_cascade
        resolved_sandbox_master_url = _ssr.resolved_sandbox_master_url
        surveyor = _ssr.surveyor
        _obs_store = _ssr._obs_store
        _patch_store = _ssr._patch_store
        # Every consumer below reads the CURRENT session off this one
        # holder (never a plain `session` local) — `_execute_target` (below)
        # rebuilds the session in place, on a config-fingerprint mismatch,
        # before EVERY single-engine execution it performs (baseline
        # re-executions as well as patch retries), and every reader must
        # observe that rebuild, not a value captured once at setup time
        # (cross-engine remediation, generalizing the Phase 82 fix).
        _session_holder = _ssr.session_holder
        bundle = _ssr.bundle
        depot = _ssr.depot

        # `RunContext` carries the state the phases AFTER this point need —
        # the heal loop below (still inline, unchanged) and the helper
        # functions it calls, extracted to `run_phases.py` (Phase 91
        # decomposition). `run()` keeps its own plain locals too; `ctx` is
        # only the argument object those extracted functions read/mutate.
        # The `--ctx` CLI option (the `ctx` parameter) is not read again
        # past this point, so reusing the name here does not shadow it.
        ctx = RunContext(
            manifest=manifest,
            cfg=cfg,
            blueprint=blueprint,
            engine=engine,
            master_url=master_url,
            verbosity=verbosity,
            execute=execute,
            resolved_store_dir=resolved_store_dir,
            checkpoint_root_abs=checkpoint_root_abs,
            handoff_root_abs=_handoff_root_abs,
            surveyor=surveyor,
            depot=depot,
            bundle=bundle,
            session_holder=_session_holder,
            store_dir_param=store_dir,
            run_id=run_id,
            obs_store=_obs_store,
            run_started_at=_run_started_at,
            resume_run_id=resume_run_id,
            from_module=from_module,
            to_module=to_module,
            parallel=parallel,
            profile=profile,
            probe_sampling=probe_sampling,
            cli_overrides=cli_overrides,
            depot_reads=_cr.depot_reads,
            project_root=_project_root,
            blueprint_abs=blueprint_abs,
            patches_dir=patches_dir,
            approval_mode=approval_mode,
            max_patches=max_patches,
            patch_store=_patch_store,
            resolved_agent_provider=resolved_agent_provider,
            resolved_agent_base_url=resolved_agent_base_url,
            resolved_agent_model=resolved_agent_model,
            resolved_agent_provider_options=resolved_agent_provider_options,
            resolved_agent_timeout=resolved_agent_timeout,
            resolved_agent_max_reprompts=resolved_agent_max_reprompts,
            resolved_agent_api_key=resolved_agent_api_key,
            resolved_agent_engine_prompt_context=resolved_agent_engine_prompt_context,
            resolved_agent_blueprint_prompt_context=resolved_agent_blueprint_prompt_context,
            resolved_agent_cascade=resolved_agent_cascade,
            resolved_sandbox_master_url=resolved_sandbox_master_url,
        )

        # Per-module resolved engine (islands.py stamps the fully-resolved
        # engine onto every enabled Module at compile time — see
        # `compiler.py`'s `dataclasses.replace(m, engine=_resolved_engine[m.id])`).
        # Only built/shown for a polyglot run — a single-engine run never
        # gains this column, preserving the compat bar byte-for-byte.
        _is_polyglot = len(manifest.islands) > 1
        _module_engine: dict[str, str] = (
            {m.id: m.engine for m in manifest.modules if m.engine} if _is_polyglot else {}
        )
        # Synthetic Handoff modules (§4.3/§10.9) — id -> {from_module,
        # to_module, from_engine, to_engine}. Rendered as a first-class step
        # (distinct marker, engine pair, bytes/duration), never folded into
        # the Arcade tree-nesting below despite a handoff id containing
        # "__" (`<from_id>__handoff__<to_id>`) the same way an Arcade
        # child's namespaced id does.
        ctx.handoff_info = {
            m.id: m.config for m in manifest.modules if m.type == ModuleType.Handoff
        }

        _heal = run_heal_loop(ctx)
        result = _heal.result
        failure_ctx = _heal.failure_ctx
        patch_count = _heal.patch_count
        patch_staged_for_review = _heal.patch_staged_for_review
        patch_rejected_by_gate = _heal.patch_rejected_by_gate

        finalize_surveyor_and_depot(ctx)
        render_runtime_warnings(ctx, result)

        if result.status not in (ExecutionStatus.SUCCESS, ExecutionStatus.PATCHED):
            handle_failure_exit(
                ctx,
                result,
                failure_ctx,
                patch_staged_for_review=patch_staged_for_review,
                patch_rejected_by_gate=patch_rejected_by_gate,
            )

        stamp_success_provenance(ctx, result)
        fire_success_webhook(ctx, result)
        print_success_footer_and_hooks(ctx, result, patch_count)
    finally:
        _run_stack.close()
        os.chdir(_original_cwd)
