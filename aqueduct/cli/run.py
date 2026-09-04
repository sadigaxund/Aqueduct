"""`run` commands — extracted verbatim from aqueduct/cli/__init__.py.

No behaviour change. The click group + shared helpers come from the package;
commands register onto `cli` when imported at the bottom of __init__.
"""

from __future__ import annotations

import sys
from typing import Any

import click

import aqueduct.cli as _aqcli  # noqa: E402  (monkeypatch-able helpers)
from aqueduct import exit_codes
from aqueduct.cli import (
    _check_heal_guardrails,
    _env_options,
    _rule,
    cli,
)
from aqueduct.cli.render.funnel import emit
from aqueduct.cli.run_setup import (
    _do_compile,
    _load_engine_config,
    _setup_surveyor,
)
from aqueduct.executor.models import concise_error
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
) -> None:
    """Compile and execute a Blueprint on a SparkSession."""
    import os
    import uuid
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
    from aqueduct.executor import ExecuteError
    from aqueduct.executor.models import ExecutionResult, ExecutionStatus, ModuleResult

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

        # ── P4: --resume fails closed on a manifest-hash mismatch ─────────────
        # `--resume <run_id>` reuses checkpoints from a PRIOR run. Two
        # independent checkpoint mechanisms exist, both keyed off
        # `aqueduct.executor.models.manifest_hash(manifest)` (a content hash
        # of the WHOLE compiled Manifest — any Blueprint edit changes it):
        #
        #   1. Module checkpoints (`checkpoint_root`/`store_dir/checkpoints`)
        #      — `<base>/<run_id>/_manifest_hash` stores the hash the ORIGINAL
        #      run compiled. Both engines' `execute()` already read this back
        #      and compare it (`spark/executor.py`, `duckdb_/executor.py`) —
        #      but only ever WARN (`runtime_resume_hash_changed`) and proceed;
        #      that permissive behaviour is a deliberate, separately-tested
        #      contract at the engine layer (see
        #      `test_resume_mismatched_manifest_warns_and_continues`) and is
        #      left untouched. The hard refusal below happens one layer up, at
        #      the CLI, BEFORE any engine session is even built.
        #
        #   2. Handoff spill (`aqueduct/executor/spill.py`, polyglot Blueprints
        #      only) — laid out as `<handoff.root>/<manifest_hash>/<run_id>/`,
        #      keyed STRICTLY by the CURRENT hash. A mismatch here is not an
        #      observable "wrong hash" condition inside the orchestrator at
        #      all — it just finds nothing under the new hash and silently
        #      starts that island fresh. `find_run_under_other_hash` is the
        #      detector: it tells "genuinely first run of this run_id" apart
        #      from "run_id exists, but under a stale hash" by scanning every
        #      OTHER hash directory.
        #
        # Either mechanism finding a stale hash is refused identically here,
        # unless `--force` (validated above) opts back into today's
        # behaviour (module checkpoints keep warning-and-proceeding; handoff
        # spill keeps silently re-executing that island).
        if resume_run_id and not force_resume:
            from aqueduct.executor.models import manifest_hash as _manifest_hash_fn
            from aqueduct.executor.spill import find_run_under_other_hash as _find_other_hash

            _current_hash = _manifest_hash_fn(manifest)
            _stale_hash: str | None = None

            _checkpoints_base = (
                checkpoint_root_abs
                if checkpoint_root_abs
                else (resolved_store_dir / "checkpoints" if resolved_store_dir else None)
            )
            if _checkpoints_base is not None:
                _stored_hash_path = Path(_checkpoints_base) / resume_run_id / "_manifest_hash"
                if _stored_hash_path.exists():
                    _stored_hash = _stored_hash_path.read_text(encoding="utf-8").strip()
                    if _stored_hash != _current_hash:
                        _stale_hash = _stored_hash

            if _stale_hash is None and len(manifest.islands) > 1:
                _stale_hash = _find_other_hash(_handoff_root_abs, resume_run_id, _current_hash)

            if _stale_hash is not None:
                click.echo(
                    f"✗ --resume {resume_run_id!r} refused: checkpoint manifest hash "
                    f"{_stale_hash!r} does not match this run's compiled Manifest hash "
                    f"{_current_hash!r} — the Blueprint (or its context/profile) has "
                    "changed since that run's checkpoints were written. Pass --force to "
                    "reuse them anyway, or drop --resume to start fresh.",
                    err=True,
                )
                sys.exit(exit_codes.CONFIG_ERROR)

        # ── --from / --to are not yet island-aware ────────────────────────────
        # Module-range selection assumes ONE execution graph; which island(s)
        # a `--from`/`--to` pair spans, and how a sub-manifest gets built per
        # island for a partial range, is real cross-island work this batch
        # does not attempt. Refusing loudly (CONFIG_ERROR) beats silently
        # running the whole polyglot graph while looking like it honoured the
        # flag — the same "loud, not silent" choice `--sandbox` already makes
        # for a polyglot Manifest below.
        if len(manifest.islands) > 1 and (from_module or to_module):
            click.echo(
                "✗ --from/--to do not yet support a polyglot Blueprint "
                f"({len(manifest.islands)} islands) — module-range selection "
                "across engine islands is not implemented in this release",
                err=True,
            )
            sys.exit(exit_codes.CONFIG_ERROR)

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
            import atexit

            from aqueduct.executor.capabilities import Support, get_capabilities
            from aqueduct.patch.preview import build_sandbox_manifest

            if len(manifest.islands) > 1:
                _island_engines = ", ".join(sorted({isl.engine for isl in manifest.islands}))
                click.echo(
                    f"✗ --sandbox does not support a polyglot Blueprint "
                    f"({len(manifest.islands)} islands: {_island_engines}) — a single-session "
                    "dry-run cannot replay a multi-engine Manifest in this release",
                    err=True,
                )
                sys.exit(exit_codes.CONFIG_ERROR)

            _sandbox_leaf = get_capabilities(engine).verdict("tooling.sandbox_dry_run")
            if _sandbox_leaf.support != Support.SUPPORTED:
                click.echo(
                    f"✗ --sandbox does not support engine {engine!r}: "
                    f"{_sandbox_leaf.hint or 'tooling.sandbox_dry_run is unsupported for this engine'}",
                    err=True,
                )
                sys.exit(exit_codes.CONFIG_ERROR)

            from aqueduct.executor.protocol import (
                SessionSpec,
                filter_execute_kwargs,
                get_protocol,
            )
            from aqueduct.executor.session_config import (
                resolve_session_engine_config,
                session_secrets_options,
            )

            sandboxed_manifest, egress_targets = build_sandbox_manifest(manifest, sample)
            sandbox_run_id = (
                f"sandbox-{run_id or uuid.uuid4().hex}"  # full uuid — queryable, no collisions
            )

            _limit_desc = f"≤{sample} row(s)/Ingress" if sample and sample > 0 else "no row limit"
            click.echo(
                f"⊙ sandbox dry-run — {_limit_desc}, {len(egress_targets)} Egress "
                "module(s) skipped (no writes, no healing, no persistence)",
                err=True,
            )

            _protocol = get_protocol(engine)
            session = _protocol.session_factory()(
                SessionSpec(
                    blueprint_id=manifest.blueprint_id,
                    engine_config=resolve_session_engine_config(cfg, engine, manifest),
                    master_url=master_url,
                    quiet_startup=(verbosity < 2),
                    timezone=cfg.timezone,
                    engine_options=session_secrets_options(cfg, manifest),
                )
            )
            atexit.register(lambda: _protocol.session_closer()(session))

            try:
                _sandbox_kwargs = filter_execute_kwargs(
                    engine,
                    dict(
                        run_id=sandbox_run_id,
                        store_dir=None,
                        surveyor=None,
                        depot=depot,
                        from_module=from_module,
                        to_module=to_module,
                        block_full_actions=not cfg.danger.allow_full_probe_actions,
                        parallel=parallel,
                        sampling=probe_sampling,
                    ),
                    suppress=cfg.warnings.suppress,
                )
                result = execute(sandboxed_manifest, session, **_sandbox_kwargs)
            except ExecuteError as exc:
                click.echo(f"✗ sandbox run failed: {exc}", err=True)
                sys.exit(exit_codes.DATA_OR_RUNTIME)

            if result.status != ExecutionStatus.SUCCESS:
                failing = next(
                    (r for r in result.module_results if r.status == ExecutionStatus.ERROR), None
                )
                detail = (
                    f" — first error in {failing.module_id!r}: {failing.error}" if failing else ""
                )
                from aqueduct.cli.render.style import error as _style_error

                _style_error(f"sandbox run status={result.status}{detail}", err=False)
                sys.exit(exit_codes.DATA_OR_RUNTIME)

            _ran = sum(1 for r in result.module_results if r.status == ExecutionStatus.SUCCESS)
            from aqueduct.cli.render.style import success as _style_success

            _style_success(
                f"sandbox run succeeded — {_ran} module(s) executed, "
                f"{len(egress_targets)} Egress skipped",
                err=False,
            )
            for tgt in egress_targets:
                click.echo(
                    f"    · skipped Egress {tgt['id']!r} → "
                    f"{tgt.get('format')} {tgt.get('path')}",
                    err=True,
                )
            sys.exit(exit_codes.SUCCESS)

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

        # ── Self-healing run loop ─────────────────────────────────────────────────
        patch_count = 0
        failure_ctx = None
        result = None
        patch_staged_for_review = False  # set when human/ci mode writes a patch to patches/pending/
        patch_rejected_by_gate = False  # set when a validation gate rejects a patch in auto (non-interactive) mode → VALIDATION_GATE(4)
        last_apply_error: str | None = None  # fed back to LLM on next multi-patch iteration

        # One-shot flag for the polyglot sandbox-unavailable notice — the
        # same patch/candidate can pass through the gate pyramid several
        # times in one run (deep_loop's in-context validate_cb, the final
        # multi-patch commit check); the underlying reason (this Blueprint
        # has >1 island) never changes mid-run, so only the first
        # occurrence needs to say so.
        _polyglot_sandbox_unavailable_warned = False

        def _fire_heal_hook(event: str, *, iter_run_id: str, hook_status: str, ctx) -> None:
            """Fire `hooks.on_patch_pending` / `hooks.on_healed` — mid-run
            heal-milestone hooks, mirroring the engine-level `webhooks:`
            `on_patch_pending` vocabulary at the Blueprint.
            Best-effort, never blocks the heal loop; never changes the exit
            code (same contract as the terminal on_success/on_failure hooks).
            """
            entries = (
                manifest.hooks.on_patch_pending
                if event == "on_patch_pending"
                else manifest.hooks.on_healed
            )
            if not entries:
                return
            from aqueduct.cli.hooks import run_hooks as _run_heal_hooks

            _run_heal_hooks(
                entries,
                event,
                run_id=iter_run_id,
                status=hook_status,
                blueprint_id=manifest.blueprint_id,
                blueprint_path=blueprint,
                allow_command_hooks=cfg.danger.allow_command_hooks,
                failure_ctx=ctx,
                session=_session_holder.session,
                engine=engine,
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
        _handoff_info: dict[str, dict] = {
            m.id: m.config for m in manifest.modules if m.type == ModuleType.Handoff
        }

        def _render_module_summary(
            _result, failure_ctx=None, *, healed_module=None, healed_patch_num=None
        ) -> None:
            """Print the per-module ✓/✗ status block for one execution result.

            ``failure_ctx`` (optional) is the ``FailureContext`` the surveyor
            just recorded for THIS result — carries ``error_class`` and
            ``suggested_columns`` for the classified ✗ failure line (SCREEN
            2/6). Only its ``failed_module`` row uses it; other rows fall
            back to the generic label.

            Called once per heal iteration right after the result is recorded, so
            module outcomes print BEFORE that iteration's agent/heal output —
            chronological order (execute → result → heal → next attempt). Metrics
            are a best-effort post-execute read from the obs store (short-lived
            connections, so the store is free by now)."""
            _metrics: dict[str, dict] = {}
            try:
                from aqueduct.stores.queries import run_detail as _run_detail
                from aqueduct.stores.read import open_obs_read

                _rs = open_obs_read(
                    cfg,
                    store_dir=store_dir,
                    run_id=_result.run_id,
                    blueprint_id=manifest.blueprint_id,
                )
                if _rs is not None:
                    _det = _run_detail(_rs, _result.run_id)
                    if _det:
                        # `run_detail` already merges a module_id's multiple
                        # `module_metrics` rows into one `ProfileRow` (a
                        # synthetic Handoff module gets a write-side row and
                        # a read-side row under the SAME module_id — see
                        # `stores/queries.py::run_detail`) — one entry per
                        # module_id here, never overwritten.
                        for _p in _det.profile:
                            _metrics[_p.module_id] = {
                                "records_written": _p.records_written,
                                "duration_ms": _p.duration_ms,
                                "bytes_written": getattr(_p, "bytes_written", None),
                                "bytes_read": getattr(_p, "bytes_read", None),
                            }
            except Exception:
                pass  # per-module profile read is best-effort; never fail for a missing metric

            def _fmt_dur(ms):
                return None if ms is None else (f"{ms} ms" if ms < 1000 else f"{ms / 1000:.1f} s")

            # ⏭ reason column for `enabled: false` modules (compiler-stamped).
            _disabled_reason = {
                m.id: m.disabled_reason
                for m in manifest.modules
                if getattr(m, "disabled_reason", None)
            }

            # Phase 85 Wave 2 — egress rows show their destination instead of
            # rows/time metadata (SCREEN 1). `m.config` is dict-like (same
            # access pattern `_handoff_info` above already relies on).
            _egress_dest: dict[str, str] = {}
            for _m in manifest.modules:
                if _m.type == ModuleType.Egress:
                    _dest = _m.config.get("path") or _m.config.get("table") or _m.config.get("key")
                    if _dest:
                        _egress_dest[_m.id] = str(_dest)

            click.echo(err=False)

            def _icon(mr):
                if mr.status == ExecutionStatus.SUCCESS:
                    return click.style("✓", fg="green")
                if mr.status == ExecutionStatus.SKIPPED:
                    return click.style("⏭", fg="cyan")
                return click.style("✗", fg="red", bold=True)

            # Tree view — Arcade-expanded children (`{arcade}__{child}`, the
            # expander's namespacing convention; `__` is rejected in user ids)
            # nest under a synthetic parent row. A synthetic Handoff module's
            # id ALSO contains "__" (`<from_id>__handoff__<to_id>`) but is
            # checked first and routed to its own row kind instead — folding
            # it into the Arcade-child branch would misparse it as a child
            # of whichever module happens to share its `from_id` prefix.
            # Only THIS summary block nests/specializes: runtime logs,
            # observability rows, and the failed_module footer keep the
            # full flattened id so error correlation stays joinable.
            _rows: list[tuple[str, object]] = []
            _arc_children: dict[str, list] = {}
            for mr in _result.module_results:
                if mr.module_id in _handoff_info:
                    _rows.append(("handoff", mr))
                elif "__" in mr.module_id:
                    _arc = mr.module_id.split("__", 1)[0]
                    if _arc not in _arc_children:
                        _arc_children[_arc] = []
                        _rows.append(("arcade", _arc))
                    _arc_children[_arc].append(mr)
                else:
                    _rows.append(("module", mr))

            _CHILD_PAD = 5  # child names start 5 columns deeper than top-level names
            _w = max(
                [len(mr.module_id) for kind, mr in _rows if kind == "module"]
                + [len(mr.module_id) for kind, mr in _rows if kind == "handoff"]
                + [len(a) for kind, a in _rows if kind == "arcade"]
                + [
                    len(c.module_id.split("__", 1)[1]) + _CHILD_PAD
                    for cs in _arc_children.values()
                    for c in cs
                ],
                default=0,
            )

            # Phase 85 Wave 2 — right-aligned metadata column (SCREEN 1).
            # Two passes: first compute each row's (left, tail) unpadded,
            # find the widest `left + 2sp + tail` among rows that HAVE a
            # tail, then pad every such row's left side out to that width
            # so every tail starts (or, since tails vary in length, ENDS)
            # at the same column. Degrades to the old left-packed
            # `name.ljust(pad)` layout when the terminal is too narrow for
            # that column to fit without negative padding.
            from aqueduct.cli.render.width import display_width as _dw
            from aqueduct.cli.render.width import terminal_width as _term_width

            _pending_rows: list[dict] = []  # collected before any printing

            def _queue_row(
                mr, left_plain, left_styled, tail_plain, tail_styled, warn_prefix, kind="metric"
            ):
                # `kind` separates the right-aligned METRIC column (rows ·
                # time, bytes · duration — short, comparable) from a "path"
                # tail (an egress destination — long, variable, free text).
                # Audit-fixed 2026-08-23: an egress row used to join the
                # SAME right-alignment group as metric rows, so one long
                # absolute path inflated the natural width and dragged
                # every metric row's gap out to match it (`raw_orders` with
                # 60+ spaces before a bare "14 ms").
                _pending_rows.append(
                    {
                        "mr": mr,
                        "left_plain": left_plain,
                        "left_styled": left_styled,
                        "tail_plain": tail_plain,
                        "tail_styled": tail_styled,
                        "warn_prefix": warn_prefix,
                        "kind": kind,
                    }
                )

            def _flush_rows():
                _tw = _term_width()
                _metric_rows = [
                    r for r in _pending_rows if r["tail_plain"] and r["kind"] == "metric"
                ]
                _natural = (
                    max(_dw(r["left_plain"]) + 2 + _dw(r["tail_plain"]) for r in _metric_rows)
                    if _metric_rows
                    else 0
                )
                _fits = bool(_metric_rows) and _natural <= _tw
                # Path rows align among THEMSELVES (and with the metric
                # rows' name column, for a tidy shared left edge) — never
                # against the metric column's own width.
                _name_width = max((_dw(r["left_plain"]) for r in _pending_rows), default=0)
                for r in _pending_rows:
                    mr, warn_prefix = r["mr"], r["warn_prefix"]
                    if not r["tail_plain"]:
                        line = r["left_styled"]
                    elif r["kind"] == "path":
                        pad = max(2, _name_width - _dw(r["left_plain"]) + 2)
                        line = r["left_styled"] + (" " * pad) + r["tail_styled"]
                    elif _fits:
                        pad = max(2, _natural - _dw(r["left_plain"]) - _dw(r["tail_plain"]))
                        line = r["left_styled"] + (" " * pad) + r["tail_styled"]
                    else:
                        # Narrow terminal — fall back to a simple 2-space gap
                        # rather than compute a negative/degenerate pad.
                        line = f"{r['left_styled']}  {r['tail_styled']}"
                    click.echo(line, err=False)
                    for rule_id, msg in mr.warnings:
                        from aqueduct.cli.render.funnel import warn as _output_warn

                        _output_warn(rule_id, msg, prefix=warn_prefix, err=False)
                    _notes = tuple(getattr(mr, "notes", ()) or ())
                    _cap = len(_notes) if verbosity >= 1 else 10
                    from aqueduct.cli.render.style import dim as _dim2

                    for note in _notes[:_cap]:
                        click.echo(_dim2(f"{warn_prefix}{note}"), err=False)
                    if len(_notes) > _cap:
                        click.echo(
                            _dim2(
                                f"{warn_prefix}· {len(_notes) - _cap} more  ·  -v for full output"
                            ),
                            err=False,
                        )
                _pending_rows.clear()

            def _print_failure_block(mr, name_or_boundary, lead):
                """Classified label + wrapped detail + candidates + hint
                (SCREEN 2/6). TTY: multi-line, structured. Piped/CI: ONE
                merged logical record (grep-safe) — built explicitly here
                rather than via `wrap_line`'s newline-splitting, since the
                piped shape (label — detail — candidates all on one line)
                differs from the TTY shape (candidates always its own
                line) and `wrap_line` alone can't express that difference."""
                from aqueduct.cli.render.width import is_tty as _is_tty
                from aqueduct.cli.render.wrap import wrap_line as _wrap_line

                _is_failed_module = (
                    failure_ctx is not None and mr.module_id == failure_ctx.failed_module
                )
                _ec = failure_ctx.error_class if _is_failed_module else None
                _label = _classify_error_label(_ec)
                _candidates = (
                    list(failure_ctx.suggested_columns)
                    if _is_failed_module and getattr(failure_ctx, "suggested_columns", None)
                    else []
                )
                _detail = concise_error(mr.error, limit=100_000) if mr.error else ""
                _cand_text = f"candidates: {', '.join(_candidates)}" if _candidates else ""

                if _is_tty(err=False):
                    click.echo(
                        f"{lead}{_icon(mr)} {name_or_boundary}  " + click.style(_label, fg="red"),
                        err=False,
                    )
                    for line in _wrap_line(
                        _detail,
                        gutter="      ",
                        err=False,
                        verbose=verbosity >= 1,
                        max_lines=None if verbosity >= 1 else 3,
                        hint="full error text",
                    ):
                        click.echo(line, err=False)
                    if _cand_text:
                        for line in _wrap_line(
                            _cand_text, gutter="      ", err=False, verbose=True
                        ):
                            click.echo(line, err=False)
                else:
                    _parts = [_label]
                    if _detail:
                        _parts.append(_detail)
                    if _cand_text:
                        _parts.append(_cand_text)
                    _combined = " — ".join(_parts)
                    for line in _wrap_line(_combined, gutter="", err=False, verbose=True):
                        click.echo(f"{lead}{_icon(mr)} {name_or_boundary}  {line}", err=False)

            def _mr_line(mr, name, pad, lead, warn_prefix):
                from aqueduct.cli.render.style import dim as _dim

                if mr.status == ExecutionStatus.ERROR and mr.error:
                    _flush_rows()  # preserve chronological order vs queued rows
                    _print_failure_block(mr, name, lead)
                    return
                _m = _metrics.get(mr.module_id, {})
                rows, dur = _m.get("records_written"), _m.get("duration_ms")
                if mr.module_id in _egress_dest:
                    tail_plain = f"→ {_egress_dest[mr.module_id]}"
                    tail_styled = tail_plain
                else:
                    meta = []
                    if mr.status == ExecutionStatus.SKIPPED and mr.module_id in _disabled_reason:
                        meta.append(_disabled_reason[mr.module_id])
                    if rows is not None:
                        meta.append(f"{rows:,} rows")
                    if _fmt_dur(dur):
                        meta.append(_fmt_dur(dur))
                    if healed_module is not None and mr.module_id == healed_module:
                        meta.append(
                            f"healed patch #{healed_patch_num}"
                            if healed_patch_num is not None
                            else "healed"
                        )
                    tail_plain = "  ·  ".join(meta)
                    tail_styled = _dim(tail_plain) if tail_plain else ""
                left_styled = f"{lead}{_icon(mr)} {name}"
                _kind = "path" if mr.module_id in _egress_dest else "metric"
                _queue_row(
                    mr,
                    f"{lead}{name}",
                    left_styled,
                    tail_plain,
                    tail_styled,
                    warn_prefix,
                    kind=_kind,
                )

            def _handoff_line(mr, pad, lead, warn_prefix):
                """First-class rendering for a synthetic Handoff module's
                result — a dedicated engine-boundary line (SCREEN 1 notes:
                "engine appears ONLY at a polyglot handover boundary"),
                distinct from an ordinary module row."""
                from aqueduct.cli.render.funnel import format_bytes as _format_bytes
                from aqueduct.cli.render.style import dim as _dim

                _cfg = _handoff_info[mr.module_id]
                _boundary = f"handoff · {_cfg.get('from_engine')} → {_cfg.get('to_engine')}"
                if mr.status == ExecutionStatus.ERROR and mr.error:
                    _flush_rows()
                    _print_failure_block(mr, f"⇄ {_boundary}", lead)
                    return
                _m = _metrics.get(mr.module_id, {})
                meta = []
                _bw, _br = _m.get("bytes_written"), _m.get("bytes_read")
                _fmt = _cfg.get("format")
                if _fmt:
                    meta.append(str(_fmt))
                if _bw is not None:
                    meta.append(f"{_format_bytes(_bw)} written")
                if _br is not None:
                    meta.append(f"{_format_bytes(_br)} read")
                if _fmt_dur(_m.get("duration_ms")):
                    meta.append(_fmt_dur(_m.get("duration_ms")))
                tail_plain = "  ·  ".join(meta)
                tail_styled = _dim(tail_plain) if tail_plain else ""
                left_styled = f"{lead}{_icon(mr)} ⇄ {_boundary}"
                _queue_row(
                    mr, f"{lead}⇄ {_boundary}", left_styled, tail_plain, tail_styled, warn_prefix
                )

            for kind, item in _rows:
                if kind == "module":
                    _mr_line(item, item.module_id, _w, "  ", "   ↳ ")
                    continue
                if kind == "handoff":
                    _handoff_line(item, _w, "  ", "   ↳ ")
                    continue
                _kids = _arc_children[item]
                # Parent row = worst child: any ✗ → ✗, else any ✓ → ✓, else ⏭.
                if any(m.status == ExecutionStatus.ERROR for m in _kids):
                    _p_icon = click.style("✗", fg="red", bold=True)
                elif any(m.status == ExecutionStatus.SUCCESS for m in _kids):
                    _p_icon = click.style("✓", fg="green")
                else:
                    _p_icon = click.style("⏭", fg="cyan")
                _flush_rows()
                click.echo(f"  {_p_icon} {item}", err=False)
                for _i, _kid in enumerate(_kids):
                    _glyph = "└─" if _i == len(_kids) - 1 else "├─"
                    _lead = "    " + click.style(_glyph, fg="bright_black") + " "
                    _mr_line(
                        _kid, _kid.module_id.split("__", 1)[1], _w - _CHILD_PAD, _lead, "       ↳ "
                    )
                _flush_rows()

            _flush_rows()  # trailing queued metric rows

        def _announce_polyglot_sandbox_unavailable(_gate_result) -> None:
            """Gate 3 could not replay a patch against this polyglot
            Blueprint (it replays through ONE engine's session and would
            leave every other island unchecked — see
            ``patch/preview.py::run_sandbox_gate``). Printed at the moment
            it happens, not only recorded to `patch_simulation`, because a
            user who has internalised "patches are sandbox-replayed before
            they touch my Blueprint" needs to be told the guarantee did not
            hold. Single-engine runs never reach this (only ever
            `manifest.islands` == 1).

            The status this reacts to used to be `skip` and was treated as
            acceptance: the patch applied anyway, and this notice was the
            only trace. It is now `unavailable` and BLOCKS auto-apply, so
            the line below announces a patch that stopped, not one that
            went through — the caller prints the stop itself. One-shot per
            run: see `_polyglot_sandbox_unavailable_warned` above.
            """
            nonlocal _polyglot_sandbox_unavailable_warned
            from aqueduct.patch.gate_status import GateStatus as _GateStatus

            if (
                not _polyglot_sandbox_unavailable_warned
                and len(manifest.islands) > 1
                and _gate_result is not None
                and _gate_result.status == _GateStatus.UNAVAILABLE
            ):
                _polyglot_sandbox_unavailable_warned = True
                from aqueduct.cli.render.style import warn as _style_warn

                _style_warn(_gate_result.detail, err=True)

        def _execute_target(
            target_manifest, *, run_id: str, resume_run_id: str | None = None, **kw
        ):
            """Execute *target_manifest* — the single-engine ``execute()``
            call for a Manifest with exactly one island (byte-for-byte the
            same call this code made before polyglot routing existed: same
            kwargs dict, same ``filter_execute_kwargs`` call, same
            ``ExecuteError`` handling), or ``run_polyglot()`` for one with
            more than one.

            ``kw`` carries whatever the specific call site already builds
            for ``execute()`` (``store_dir``, ``checkpoint_root``,
            ``surveyor``, ``depot``, ``from_module``, ``to_module``,
            ``block_full_actions``, ``parallel``, ``use_observe``,
            ``observability_store``, ``sampling``) — the three call sites in
            this function pass different subsets (the main heal loop passes
            the full set; the retry-execute calls after a patch pass a
            narrower one), preserved exactly as each already did.

            Returns ``(result, execute_exc)``. ``execute_exc`` is the raw
            ``ExecuteError`` on the single-engine path only (kept so callers
            can still feed it to ``surveyor.record(exc=...)`` for
            stack_trace enrichment, exactly as today) — a polyglot
            structural failure is already converted to a synthetic
            ``ModuleResult`` inside ``run_polyglot()`` itself (see its
            ``AqueductError`` wrap), so ``execute_exc`` is always ``None``
            on that path.

            **Session-fingerprint guard (cross-engine remediation).** Before
            the single-engine branch executes, it compares the session
            fingerprint *target_manifest* would resolve
            (``session_config_fingerprint``, in
            ``aqueduct/executor/session_config.py``) against the one
            ``_session_holder.session`` was actually built from, rebuilding
            only on mismatch. This is the ONE funnel every single-engine
            execution in this run passes through — the outer heal loop's
            baseline re-execution at the top of ``while True:`` AND every
            patch retry — so it catches both directions of the invariant
            "never execute a Manifest on a session built from a DIFFERENT
            Manifest": a patch retry whose ``set_engine_config`` op the
            pre-patch session hasn't picked up, AND (the bug this check adds
            over the earlier Phase 82 fix) the next baseline re-execution of
            the ORIGINAL manifest running on whatever session a FAILED
            patch's retry left behind. A mismatch-free call (nothing
            session-relevant changed) costs one fingerprint recompute and no
            rebuild — a Spark JVM is never torn down for a patch that never
            touched engine config. This subsumes the removed
            ``_rebuild_session_for_patch`` — a Manifest change is now always
            observed exactly once, at the point of execution, instead of at
            two separate explicit call sites that could disagree.
            """
            if len(target_manifest.islands) <= 1:
                from aqueduct.executor.protocol import (
                    SessionSpec,
                    filter_execute_kwargs,
                    get_protocol,
                )
                from aqueduct.executor.session_config import (
                    resolve_session_engine_config,
                    session_config_fingerprint,
                    session_secrets_options,
                )

                _target_fingerprint = session_config_fingerprint(cfg, engine, target_manifest)
                if (
                    _session_holder.session is not None
                    and _session_holder.engine_config_fingerprint != _target_fingerprint
                ):
                    _protocol = get_protocol(engine)
                    # Stop the STALE session before building the new one — a
                    # `getOrCreate()`-style reuse without a genuine teardown
                    # first would silently hand back the same live session
                    # (the exact no-op-that-looks-like-a-fix this rebuild
                    # exists to avoid). See `make_spark_session` — most
                    # engine config (definitely anything `set_engine_config`
                    # changes) has no effect on an already-running session.
                    _protocol.session_closer()(_session_holder.session)
                    _session_holder.session = _protocol.session_factory()(
                        SessionSpec(
                            blueprint_id=target_manifest.blueprint_id,
                            engine_config=resolve_session_engine_config(
                                cfg, engine, target_manifest
                            ),
                            master_url=master_url,
                            quiet_startup=(verbosity < 2),
                            timezone=cfg.timezone,
                            engine_options=session_secrets_options(cfg, target_manifest),
                        )
                    )
                    _session_holder.engine_config_fingerprint = _target_fingerprint

                try:
                    _filtered = filter_execute_kwargs(
                        engine,
                        dict(kw, run_id=run_id, resume_run_id=resume_run_id),
                        suppress=cfg.warnings.suppress,
                    )
                    return execute(target_manifest, _session_holder.session, **_filtered), None
                except ExecuteError as exc:
                    return (
                        ExecutionResult(
                            blueprint_id=target_manifest.blueprint_id,
                            run_id=run_id,
                            status=ExecutionStatus.ERROR,
                            module_results=(
                                ModuleResult(
                                    module_id="_executor",
                                    status=ExecutionStatus.ERROR,
                                    error=str(exc),
                                ),
                            ),
                        ),
                        exc,
                    )

            # ── Polyglot ──────────────────────────────────────────────────
            from aqueduct.executor.orchestrator import run_polyglot
            from aqueduct.executor.session_config import (
                resolve_session_engine_config,
                session_secrets_options,
            )

            _engine_configs: dict[str, dict] = {
                _isl.engine: resolve_session_engine_config(cfg, _isl.engine, target_manifest)
                for _isl in target_manifest.islands
            }

            polyglot_result = run_polyglot(
                target_manifest,
                run_id=run_id,
                handoff_root=_handoff_root_abs,
                keep_on_failure=cfg.handoff.keep_on_failure,
                resume_run_id=resume_run_id,
                store_dir=kw.get("store_dir", resolved_store_dir),
                checkpoint_root=kw.get("checkpoint_root", checkpoint_root_abs),
                surveyor=kw.get("surveyor", surveyor),
                depot=kw.get("depot", depot),
                observability_store=kw.get("observability_store", bundle.observability),
                warnings_suppress=cfg.warnings.suppress,
                engine_configs=_engine_configs,
                master_url=master_url,
                quiet_startup=(verbosity < 2),
                timezone=cfg.timezone,
                secrets_config=session_secrets_options(cfg, target_manifest)["secrets"],
                block_full_actions=kw.get("block_full_actions", False),
                parallel=kw.get("parallel", False),
                use_observe=kw.get("use_observe", False),
                sampling=kw.get("sampling"),
                record_result=False,
                session_keep_alive=cfg.execution.session_keep_alive,
                share_island_state=cfg.execution.share_island_state,
                prune_eagerly=cfg.handoff.prune_eagerly,
            )
            # Phase 89 item 1 — one quiet `-v` narrative line per boundary
            # where a session was kept alive instead of rebuilt, same
            # funnel/style convention as the `⇄ handoff` boundary rendering
            # above. `session_reused` is empty whenever keep-alive found no
            # same-engine adjacency (or `execution.session_keep_alive` is
            # off), so this is silent in the common case.
            if verbosity >= 1 and polyglot_result.session_reused:
                from aqueduct.cli.render.funnel import info as _funnel_info

                for _reused_engine in polyglot_result.session_reused:
                    _funnel_info(
                        f"session kept alive · {_reused_engine}",
                        gutter="  ",
                        err=True,
                    )
            # Phase 89 item 3 — same, but for eager spill pruning, one quiet
            # `-vv` narrative line per boundary whose spill was deleted the
            # moment its reader island succeeded rather than at run end.
            # Gated at -vv (not -v, unlike the reuse line above): a pruned
            # edge is routine per-boundary housekeeping, one level quieter
            # than "a session build was skipped" is.
            if verbosity >= 2 and polyglot_result.pruned_spills:
                from aqueduct.cli.render.funnel import info as _funnel_info

                for _pruned_edge in polyglot_result.pruned_spills:
                    _funnel_info(
                        f"spill pruned · {_pruned_edge}",
                        gutter="  ",
                        err=True,
                    )
            return polyglot_result, None

        # ── Pending-patch short-circuit — snapshot taken ONCE ────────────
        # A blueprint that already had an unreviewed patch sitting in
        # patches/pending/ BEFORE this run started never gets an LLM call —
        # re-healing it would just burn tokens on a duplicate nobody has
        # looked at yet. Snapshotted ONCE here, before the heal loop's first
        # iteration, rather than re-queried every iteration: the loop's own
        # multi-patch retry path (`agent.on_heal_failure: stage`, the
        # default) stages a rejected patch and keeps trying up to
        # `max_patches` in THIS SAME run — a live per-iteration query would
        # make that patch block its own very next retry. A one-time
        # snapshot means only a patch that existed before this run started
        # can trigger the short-circuit. `list_by_status` is the same
        # backend-blind lookup `aqueduct patch list` uses.
        _pending_before_run: list[dict] = []
        if _obs_store is not None and manifest.blueprint_id:
            try:
                with _obs_store.connect() as _pending_cur:
                    from aqueduct.patch.index import list_by_status as _list_pending

                    _pending_before_run = _list_pending(
                        _pending_cur,
                        status="pending",
                        blueprint_id=manifest.blueprint_id,
                        limit=1,
                    )
            except Exception:
                pass  # lookup failure must never block the run — falls through to a normal heal

        while True:
            # `iteration_run_id` is the per-iteration uuid used as `run_id`
            # for execute() and persisted on `run_records`. The user-visible
            # outer `run_id` is captured separately as `parent_run_id` on
            # `healing_outcomes` so cross-iteration aggregations remain
            # joinable to the original heal call.
            iteration_run_id = run_id if patch_count == 0 else str(uuid.uuid4())
            patch_rejected_by_gate = (
                False  # reset per iteration — only the terminal reason drives the exit code
            )
            if iteration_run_id != run_id:
                # 1.1.0 fix — register parent linkage so record() stamps the
                # outer run_id into run_records.parent_run_id for this
                # iteration's row (INSERT-or-UPDATE in surveyor.record()).
                try:
                    surveyor.register_iteration(
                        run_id=iteration_run_id,
                        parent_run_id=run_id,
                    )
                except Exception:
                    pass  # iteration registration is best-effort; never let persistence block execution
            # `_execute_target` is the single-engine `execute()` call
            # (byte-for-byte the same kwargs dict + filter_execute_kwargs +
            # ExecuteError handling this code has always used) when
            # `manifest.islands` is exactly one, or `run_polyglot()` for a
            # >1-island Manifest — see its docstring above the loop.
            result, execute_exc = _execute_target(
                manifest,
                run_id=iteration_run_id,
                resume_run_id=resume_run_id if patch_count == 0 else None,
                store_dir=resolved_store_dir,
                checkpoint_root=checkpoint_root_abs,
                surveyor=surveyor,
                depot=depot,
                from_module=from_module,
                to_module=to_module,
                block_full_actions=not cfg.danger.allow_full_probe_actions,
                parallel=parallel,
                use_observe=cfg.metrics.use_observe,
                observability_store=bundle.observability,
                sampling=probe_sampling,
            )

            failure_ctx = surveyor.record(result, exc=execute_exc, engine=result.failed_engine)

            # Chronological output: render THIS iteration's module outcomes now,
            # before any agent/heal block below. Replaces the old single post-loop
            # summary so a heal attempt always reads after the result it heals.
            _render_module_summary(result, failure_ctx)

            if result.status == ExecutionStatus.SUCCESS:
                break

            # trigger_agent flag overrides approval_mode=disabled — escalate to human staging at minimum
            effective_mode = approval_mode
            if result.trigger_agent and effective_mode == "disabled":
                effective_mode = "human"
                if _aqcli._agent_usable(
                    resolved_agent_provider, resolved_agent_base_url, resolved_agent_api_key
                ):
                    click.echo(
                        "  ↻ Agent triggered by module rule (overriding approval_mode=disabled → staging patch for review)",
                        err=True,
                    )

            if effective_mode == "disabled" or failure_ctx is None:
                break

            if not _aqcli._agent_usable_with_cascade(
                resolved_agent_provider,
                resolved_agent_base_url,
                resolved_agent_api_key,
                resolved_agent_cascade,
            ):
                break  # already warned at startup (line 730)

            if patch_count >= max_patches:
                click.echo(
                    f"⚠  Agent: max_patches={max_patches} reached, stopping self-healing loop",
                    err=True,
                )
                break

            # ── Pre-trigger guardrail check ────────────────────────────────────────
            _should_heal, _no_heal_reason = _check_heal_guardrails(
                failure_ctx, manifest.agent.guardrails
            )
            if not _should_heal:
                click.echo(
                    f"  ⊘  Agent guardrail blocked healing: {_no_heal_reason}",
                    err=True,
                )
                break

            # ── Spend-cap: max_heal_attempts_per_hour (blueprint override > engine default) ─
            _heal_cap = manifest.agent.max_heal_attempts_per_hour
            if _heal_cap is None:
                _heal_cap = getattr(cfg.agent, "max_heal_attempts_per_hour", None)
            if _heal_cap is not None and _heal_cap >= 0:
                _recent = surveyor.count_recent_heal_attempts(within_minutes=60)
                if _recent >= _heal_cap:
                    click.echo(
                        f"  ⊘  Agent rate-limit reached: {_recent} healing attempt(s) "
                        f"in the last 60 minutes (max_heal_attempts_per_hour={_heal_cap}). "
                        "Run ends without further Agent calls. Inspect healing_outcomes in observability.db.",
                        err=True,
                    )
                    break

            # ── Failure signature ──────────────────────────────────────────────
            # Still computed every iteration — it stamps
            # healing_outcomes.failure_signature[_coarse] / patch_index rows
            # below, and feeds the budget loop's stuck-signature /
            # progress-stalled axes (aqueduct/agent/budget.py). It no longer
            # keys any lookup — the pending-patch guard below matches on
            # blueprint_id instead.
            from aqueduct.agent.signature import from_failure_context as _from_failure_ctx

            _sig_exact, _sig_coarse = _from_failure_ctx(failure_ctx)
            # Only value now that signature-keyed pending-reuse and exact
            # replay are gone — kept as named constants (not inlined) so the
            # many downstream stage_patch_for_human(source=…) /
            # healing_outcomes.resolution call sites below don't each need
            # a literal edited by hand.
            _patch_source = "llm"
            _resolution = "llm"

            # ── Pending-patch short-circuit ──────────────────────────────────
            # Uses the ONE-TIME snapshot taken before the loop's first
            # iteration (`_pending_before_run`, see its comment above the
            # loop) — not a fresh per-iteration query — so a patch THIS
            # run's own multi-patch retry loop just staged never blocks its
            # own next retry, while a patch that was already pending before
            # this run started still stops it before any Agent call.
            if _pending_before_run:
                _pending_row = _pending_before_run[0]
                from aqueduct.cli.render.style import success as _style_success

                _style_success(
                    f"patch {_pending_row.get('patch_id')} is already staged for review on "
                    "this blueprint — skipping Agent\n"
                    f"    Review: aqueduct patch pull {_pending_row.get('patch_id')}  "
                    "(or: aqueduct patch list)",
                    err=True,
                )
                # No heal_attempts row: no LLM attempt happened, and none of
                # the vocabulary's stop_reason values ("solved", "budget",
                # "api_error", ...) describe a short-circuit before any
                # attempt runs. The failure itself is already recorded — the
                # unconditional `surveyor.record(...)` call earlier this
                # iteration wrote it before this guard ever ran.
                patch_staged_for_review = True
                break

            # ── Generate patch ────────────────────────────────────────────────────
            from aqueduct.agent import AgentRunConfig, generate_agent_patch, stage_patch_for_human
            from aqueduct.agent.transcript import TranscriptWriter

            _attempt_display = (
                f"{patch_count + 1}/{max_patches}" if max_patches > 1 else f"{patch_count + 1}"
            )
            from aqueduct.cli.render.style import colorize_line as _style_heal_line

            # Live SSE streaming is interactive-TTY-only (piped/CI keep the
            # non-streaming POST path). The ENTIRE heal block — including this
            # stream — is narrative (stderr), so the TTY check is against
            # stderr, not stdout.
            _use_stream = sys.stderr.isatty()
            _transcript = TranscriptWriter(
                verbose=verbosity >= 1,
                write=lambda s: emit(_style_heal_line(s), err=True),
                streamed=_use_stream,
            )

            # Phase 85 Wave 2 — one ◆ header line replaces the old
            # "⚠ … failed → agent self-healing" line PLUS the separate
            # "│  ◆ …" ceremony line (SCREENS 2-5). ORANGE/yellow per
            # the owner ruling; `tier N/M · model` for a cascade names
            # the STARTING tier (known at heal start regardless of
            # outcome — `_open_tier_if_new` in transcript.py only
            # prints a `├─` node on a LATER escalation, since this line
            # already announced tier 1).
            if resolved_agent_cascade:
                _n_tiers = len(resolved_agent_cascade)
                _tier0_model = resolved_agent_cascade[0].model
                _agent_info = f"cascade · tier 1/{_n_tiers} · {_tier0_model}"
            else:
                _agent_info = (
                    f"{resolved_agent_model} · {resolved_agent_provider} "
                    f"· ≤{resolved_agent_max_reprompts} reprompts"
                )
            _header_text = f"◆ self-healing {failure_ctx.failed_module} · {_agent_info}"
            if max_patches > 1:
                _header_text += f" (patch {_attempt_display})"
            click.echo(click.style(_header_text, fg="yellow"), err=True)
            _transcript.header(
                patch_count + 1 if max_patches > 1 else 1,
                resolved_agent_max_reprompts,
                resolve=_resolution,
            )
            # Immediate cue — the stream meter only appears once the FIRST
            # token arrives, and a reasoning model can digest a big prompt for
            # a while first, so without this the open branch looks hung.
            # Routed through the funnel's `echo()` (wrap_line-backed), not
            # `emit()` — `emit()` is the structured-result entry point and
            # does not wrap; a bare f-string handed to it is exactly the
            # heal-block-overflows-80-columns defect this fixes.
            from aqueduct.cli.render.funnel import echo as _funnel_echo

            _cue_text = (
                "waiting for first token… (reasoning models digest the prompt before replying)"
                if _use_stream
                else "contacting agent… (first response can be slow — big prompt / local cold-start)"
            )
            _funnel_echo(_cue_text, gutter="│   · ", err=True, verbose=verbosity >= 1)

            # Run blueprint doctor checks against the compiled Manifest (all modules resolved,
            # arcades expanded — no need to re-parse or recurse into sub-blueprints).
            try:
                from dataclasses import replace as _dc_replace

                from aqueduct.doctor import check_blueprint_sources_from_manifest

                _dr = check_blueprint_sources_from_manifest(
                    manifest,
                    deployment_env=cfg.deployment.env,
                    duckdb_engine_config=cfg.engine.duckdb.model_dump(),
                )
                _hints = tuple(
                    f"{r.name} — {r.detail}" for r in _dr if r.status in ("warn", "fail")
                )
                if _hints:
                    failure_ctx = _dc_replace(failure_ctx, doctor_hints=_hints)
            except Exception:
                pass  # doctor errors must never block self-healing

            # Persist per-attempt log via the unified reprompt loop's
            # on_attempt hook. Stop reason is recorded against the FINAL row
            # after the loop returns (each row carries it for joinability).
            _heal_run_id = run_id
            from aqueduct.agent import resolve_budget as _resolve_budget

            _budget = _resolve_budget(
                getattr(cfg.agent, "budget", None),
                max_reprompts=resolved_agent_max_reprompts,
            )

            # ── Live token streaming display ──────────────────────────────────
            # _on_token(kind, text) is fired by the provider per SSE delta.
            # default: a compact in-place meter (· thinking… N chars);
            # -v: streams the actual thinking/answer text under a ┆ gutter.
            # Markers stay in the serious geometric vocabulary: · = internal
            # (reasoning, recedes), ▸ = output (the answer, points forward).
            _stream_state = {"chars": 0, "kind": None, "active": False}

            def _on_token(kind: str, text: str) -> None:
                # Heal-block narrative — stderr, like everything else in this
                # block (see the stream-routing note on `_use_stream` above).
                _stream_state["active"] = True
                if verbosity >= 1:
                    if kind != _stream_state["kind"]:
                        head = "· thinking" if kind == "thinking" else "▸ answer"
                        sys.stderr.write(f"\n│   {head}:\n│   ┆ ")
                        _stream_state["kind"] = kind
                    sys.stderr.write(text.replace("\n", "\n│   ┆ "))
                else:
                    _stream_state["chars"] += len(text)
                    label = "thinking" if kind == "thinking" else "writing"
                    sys.stderr.write(f"\r│   · {label}… {_stream_state['chars']} chars")
                sys.stderr.flush()

            def _close_stream() -> None:
                if _stream_state["active"]:
                    sys.stderr.write("\n")
                    sys.stderr.flush()
                    _stream_state.update(chars=0, kind=None, active=False)

            def _on_attempt(rec):
                _close_stream()  # finish the live line before the turn renders
                try:
                    surveyor.record_heal_attempt(run_id=_heal_run_id, attempt_record=rec)
                except Exception:
                    pass  # never let persistence block the loop
                try:
                    _tier_model = getattr(rec, "_aq_tier_model", None) or resolved_agent_model
                    _transcript.write(
                        rec,
                        None,
                        model=_tier_model,
                        cascade_position=rec.model_cascade_position,
                    )
                except Exception:
                    pass  # transcript is best-effort

            # Apply-gate guardrail check wired INTO the unified reprompt loop.
            # Deterministic + fast (no Spark) — runs `_check_guardrails` on the
            # generated PatchSpec against the current Blueprint and feeds any
            # rejection back as a reprompt instead of letting the loop exit
            # 'solved' and then having the outer code silently stage. Slower
            # gates (lineage / sandbox / resolvability) stay OUTSIDE the loop — they
            # run once per patch in multi-patch mode.
            _bp_path_for_cb = Path(blueprint)

            def _apply_cb(patch_spec: Any, _bp=_bp_path_for_cb, _cfg=cfg) -> tuple:
                try:
                    from aqueduct.patch.apply import (
                        PatchError,
                        _check_guardrails,
                        _yaml_load,
                        apply_patch_to_dict,
                    )

                    bp_raw = _yaml_load(_bp)
                    # 1.1.0 — compile-sanity check. Catches patches that drop
                    # discriminator fields (e.g. `replace_module_config` on a
                    # Channel that omits `op`) before sandbox replay burns
                    # 30+ seconds proving the same thing. Errors feed back to
                    # the LLM as concrete reprompt context.
                    try:
                        bp_after = apply_patch_to_dict(bp_raw, patch_spec)
                        for _m in bp_after.get("modules") or []:
                            if not isinstance(_m, dict):
                                continue
                            _mt = _m.get("type")
                            _cfg = _m.get("config") or {}
                            if _mt == ModuleType.Channel and "op" not in _cfg:
                                return (
                                    False,
                                    "schema_drift",
                                    (
                                        f"Patch leaves Channel module {_m.get('id')!r} without "
                                        f"required 'op' key in config. Use set_module_config_key "
                                        f"to update one key instead of replace_module_config."
                                    ),
                                    None,
                                )
                            if (
                                _mt in (ModuleType.Ingress, ModuleType.Egress)
                                and "format" not in _cfg
                            ):
                                return (
                                    False,
                                    "schema_drift",
                                    (
                                        f"Patch leaves {_mt} module {_m.get('id')!r} without "
                                        f"required 'format' key in config."
                                    ),
                                    None,
                                )
                    except Exception as exc:
                        return False, "apply_error", (f"Patch failed to apply cleanly: {exc}"), None
                    # Called UNCONDITIONALLY. It used to short-circuit when the
                    # Blueprint declared no `agent.guardrails`, which silently
                    # skipped the two checks that are not guardrail-gated at
                    # all — `set_engine_config`'s core allowlist and the
                    # effective-config delta — on every Blueprint that never
                    # opted into guardrails, i.e. most of them. Those checks
                    # still fired at the later real-apply site, so nothing
                    # unsafe applied; what was lost is the reprompt: the model
                    # never saw the rejection and could not correct it in the
                    # same heal.
                    try:
                        _check_guardrails(patch_spec, bp_raw, provenance_map=None, cfg=_cfg)
                        return True, None, None, None
                    except PatchError as exc:
                        return False, "guardrail_violation", str(exc), None
                except Exception as exc:
                    # Fail-open: don't let an apply-callback bug block healing.
                    return False, "apply_error", str(exc), None

            # Phase 43: when deep_loop is enabled, build a validate_callback
            # that runs sandbox/lineage/resolvability gates inside the LLM conversation.
            # The model sees rejection feedback and retries in-context.
            # Cascade tiers can opt into deep_loop individually, so the
            # callback must exist whenever ANY tier (or the top level) wants it.
            _deep_loop = manifest.agent.deep_loop if manifest.agent else False
            _cascade_tiers = resolved_agent_cascade
            _any_deep_loop = _deep_loop or any(bool(t.deep_loop) for t in (_cascade_tiers or []))
            _validate_cb = None
            if _any_deep_loop:
                # Phase 85 F-17 — was an inline closure capturing ~12 locals;
                # extracted to aqueduct/agent/gate_validation.py (the agent
                # boundary: this exists only to feed the deep-loop
                # validate_callback protocol). The captured locals are now
                # explicit `partial` keyword args instead of closure state.
                from functools import partial

                from aqueduct.agent.gate_validation import validate_patch_via_gates

                _validate_cb = partial(
                    validate_patch_via_gates,
                    blueprint_path=Path(blueprint),
                    bundle=bundle,
                    surveyor=surveyor,
                    failed_module=failure_ctx.failed_module,
                    iteration_run_id=iteration_run_id,
                    blueprint_id=manifest.blueprint_id,
                    engine=(failure_ctx.engine or engine),
                    cfg=cfg,
                    sandbox_mode=(manifest.agent.sandbox_mode if manifest.agent else "sample"),
                    sandbox_master_url=resolved_sandbox_master_url,
                    warnings_suppress=cfg.warnings.suppress,
                    timezone=cfg.timezone,
                    announce_unavailable=_announce_polyglot_sandbox_unavailable,
                )

            # ── Chained (progressive) multi-patch healing state ──────────────────
            # Phase 92 — chaining is now the ONLY heal-loop behavior for the
            # single-model (non-cascade) path below; there is no more opt-in
            # flag to check here. `accumulated_patches` holds the validated
            # links folded so far (empty until the first candidate advances
            # past its own module); `current_failure` is the failure the NEXT
            # diagnosis targets, starting at this outer iteration's failure
            # and moving forward only when a candidate proves itself at a
            # DIFFERENT module. See the merged loop below (`else:` branch)
            # for the full rule.
            accumulated_patches: list = []
            current_failure = failure_ctx

            # Phase 44: multi-model cascade takes priority over single-model loop.
            if _cascade_tiers:
                from aqueduct.agent.cascade import generate_cascade_patch

                agent_result = generate_cascade_patch(
                    tiers=list(_cascade_tiers),
                    failure_ctx=failure_ctx,
                    patches_dir=patches_dir,
                    # The FAILING island's engine (Surveyor.record()'s
                    # per-island override, §10.9) drives the healing
                    # prompt's persona/rules — never the run's nominal
                    # default. Identical to `engine` for a single-engine run
                    # (`failure_ctx.engine` is always that same value there).
                    engine=(failure_ctx.engine or engine),
                    provider=resolved_agent_provider,
                    base_url=resolved_agent_base_url,
                    api_key=resolved_agent_api_key,
                    provider_options=resolved_agent_provider_options,
                    timeout=resolved_agent_timeout,
                    max_tokens=4096,
                    max_reprompts=resolved_agent_max_reprompts,
                    engine_prompt_context=resolved_agent_engine_prompt_context,
                    blueprint_prompt_context=resolved_agent_blueprint_prompt_context,
                    last_apply_error=last_apply_error,
                    guardrails=manifest.agent.guardrails if manifest.agent else None,
                    budget=_budget,
                    allow_defer=manifest.agent.allow_defer if manifest.agent else False,
                    deep_loop=_deep_loop,
                    apply_callback=_apply_cb,
                    validate_callback=_validate_cb,
                    on_attempt=_on_attempt,
                    on_token=_on_token if _use_stream else None,
                    memory_coaching=True,
                    retry_max_retries=cfg.agent.retry.max_retries,
                    retry_backoff_seconds=cfg.agent.retry.backoff_seconds,
                    obs_store=_obs_store,
                )
            else:
                # ── Chained (progressive) multi-patch healing — the ONLY ────
                # ── loop behavior for the single-model (non-cascade) path ──
                # Phase 92 fold: `max_patches` is the ONE counter for the
                # whole chain — every LLM diagnosis call below spends one
                # unit of it, whether the resulting candidate advances the
                # chain, is discarded as a same-module retry, or is rejected
                # by a gate/guardrail. A candidate that validates in memory
                # but leaves the pipeline failing at a DIFFERENT module means
                # the patch was right: it folds into `accumulated_patches`
                # and the chain advances to diagnose the new failure. The
                # SAME module failing again means the patch was wrong: only
                # THAT candidate is discarded (the already-proven
                # accumulated patches are kept) and the same failure is
                # retried. Nothing is written to the Blueprint file until the
                # full accumulated patch (accumulated_patches + the winning
                # candidate) passes the pipeline end-to-end — exactly one
                # combined PatchSpec is ever staged/applied for this heal.
                # Cascade (above) is explicitly excluded from chaining — it
                # keeps its existing one-tier-patch-per-outer-iteration model.
                from aqueduct.agent import merge_patch_specs

                _chain_last_rejected = None  # last discarded same-module/gate-rejected candidate
                while patch_count < max_patches:
                    patch_rejected_by_gate = (
                        False  # reset per attempt — only the terminal reason drives the exit code
                    )
                    if len(accumulated_patches) == 0:
                        click.echo(
                            click.style(
                                f"◆ self-healing {current_failure.failed_module} · "
                                f"{resolved_agent_model} · {resolved_agent_provider} "
                                f"(patch {patch_count + 1}/{max_patches})",
                                fg="yellow",
                            ),
                            err=True,
                        )
                    else:
                        click.echo(
                            click.style(
                                f"  · chain advanced → diagnosing {current_failure.failed_module} "
                                f"(link {len(accumulated_patches) + 1}, patch {patch_count + 1}/{max_patches})",
                                fg="yellow",
                            ),
                            err=True,
                        )

                    _link_budget = _resolve_budget(
                        getattr(cfg.agent, "budget", None),
                        max_reprompts=resolved_agent_max_reprompts,
                    )
                    _link_validate_cb = None
                    if _any_deep_loop:
                        from functools import partial

                        from aqueduct.agent.gate_validation import validate_patch_via_gates

                        _link_validate_cb = partial(
                            validate_patch_via_gates,
                            blueprint_path=Path(blueprint),
                            bundle=bundle,
                            surveyor=surveyor,
                            failed_module=current_failure.failed_module,
                            iteration_run_id=iteration_run_id,
                            blueprint_id=manifest.blueprint_id,
                            engine=(current_failure.engine or engine),
                            cfg=cfg,
                            sandbox_mode=(
                                manifest.agent.sandbox_mode if manifest.agent else "sample"
                            ),
                            sandbox_master_url=resolved_sandbox_master_url,
                            warnings_suppress=cfg.warnings.suppress,
                            timezone=cfg.timezone,
                            announce_unavailable=_announce_polyglot_sandbox_unavailable,
                        )

                    def _chain_on_attempt(rec, _link_index=len(accumulated_patches) + 1):
                        rec.chain_link = _link_index
                        _on_attempt(rec)

                    agent_result = generate_agent_patch(
                        agent_cfg=AgentRunConfig(
                            failure_ctx=current_failure,
                            model=resolved_agent_model,
                            patches_dir=patches_dir,
                            # This chain link's own failing island, not the
                            # run's nominal default — same reasoning as the
                            # cascade call site above.
                            engine=(current_failure.engine or engine),
                            provider=resolved_agent_provider,
                            base_url=resolved_agent_base_url,
                            api_key=resolved_agent_api_key,
                            provider_options=resolved_agent_provider_options,
                            timeout=resolved_agent_timeout,
                            max_reprompts=resolved_agent_max_reprompts,
                            engine_prompt_context=resolved_agent_engine_prompt_context,
                            blueprint_prompt_context=resolved_agent_blueprint_prompt_context,
                            last_apply_error=last_apply_error,
                            guardrails=manifest.agent.guardrails if manifest.agent else None,
                            budget=_link_budget,
                            allow_defer=manifest.agent.allow_defer if manifest.agent else False,
                            deep_loop=_deep_loop,
                            validate_callback=_link_validate_cb,
                            on_attempt=_chain_on_attempt,
                            on_token=_on_token if _use_stream else None,
                            apply_callback=_apply_cb,
                            memory_coaching=True,
                            retry_max_retries=cfg.agent.retry.max_retries,
                            retry_backoff_seconds=cfg.agent.retry.backoff_seconds,
                            obs_store=_obs_store,
                        ),
                    )
                    # Every diagnosis call spends one unit of max_patches —
                    # ALWAYS, win or lose (the rule this fold implements).
                    patch_count += 1

                    patch = agent_result.patch
                    # Phase 46 — record the model that actually produced this
                    # result (cascade doesn't reach this branch, so this is
                    # always the top-level agent.model).
                    _outcome_model = agent_result.model or resolved_agent_model
                    _cascade_pos = agent_result.model_cascade_position
                    # Update the last persisted row with stop_reason so
                    # downstream joins can answer "which axis terminated this
                    # heal".
                    if agent_result.attempt_records and agent_result.stop_reason:
                        try:
                            _defer_op = next(
                                (
                                    op
                                    for op in (patch.operations if patch is not None else [])
                                    if op.op == "defer_to_human"
                                ),
                                None,
                            )
                            surveyor.update_heal_attempt_stop_reason(
                                run_id=_heal_run_id,
                                attempt_num=agent_result.attempt_records[-1].attempt_num,
                                stop_reason=agent_result.stop_reason,
                                defer_reason=(
                                    getattr(_defer_op, "defer_reason", None)
                                    if _defer_op is not None
                                    else None
                                ),
                            )
                        except Exception:
                            pass  # updating stop_reason is best-effort; never let persistence block the loop
                    _summary_model = agent_result.model or agent_result.__dict__.get("model")
                    _transcript.summary(
                        agent_result.stop_reason,
                        agent_result.attempts,
                        agent_result.tokens_in_total,
                        agent_result.tokens_out_total,
                        model=_summary_model or resolved_agent_model,
                    )

                    if patch is None:
                        # Unchanged today's no-patch handling — terminal,
                        # regardless of any progress already accumulated.
                        if agent_result.stop_reason == "api_error":
                            from aqueduct.cli.render.funnel import echo as _funnel_echo

                            _rel_bp = os.path.relpath(str(blueprint))
                            _bp_display = str(blueprint) if _rel_bp.startswith("..") else _rel_bp
                            _funnel_echo(
                                "failure context saved · retry once the agent is reachable:",
                                gutter="   ↳ ",
                                err=True,
                                verbose=verbosity >= 1,
                                style={"fg": "bright_black"},
                            )
                            _funnel_echo(
                                f"aqueduct heal {_bp_display}",
                                gutter="     ",
                                err=True,
                                wrap=False,
                                style={"fg": "bright_black"},
                            )
                        on_hf = manifest.agent.on_heal_failure if manifest.agent else "stage"
                        if on_hf == "stage" and agent_result.stop_reason != "api_error":
                            click.echo(
                                click.style(
                                    "   ↑ no patch to stage — failure context saved to the observability store",
                                    fg="bright_black",
                                ),
                                err=True,
                            )
                        try:
                            for _rec in agent_result.attempt_records or ():
                                _fail_cat = (
                                    _rec.signature.error_class
                                    if getattr(_rec, "signature", None) is not None
                                    else None
                                )
                                surveyor.record_healing_outcome(
                                    run_id=iteration_run_id,
                                    parent_run_id=run_id,
                                    failed_module=current_failure.failed_module,
                                    failure_category=_fail_cat,
                                    model=_outcome_model,
                                    patch_id=None,
                                    confidence=None,
                                    patch_applied=False,
                                    run_success_after_patch=False,
                                    failure_signature=_sig_exact.hash,
                                    failure_signature_coarse=_sig_coarse.hash,
                                    resolution="llm",
                                    model_cascade_position=getattr(
                                        _rec, "model_cascade_position", None
                                    ),
                                )
                        except Exception:
                            pass  # never let persistence block the loop exit
                        break

                    # ── Confidence escalation — low-confidence patches go to human ──
                    _conf_threshold = manifest.agent.confidence_threshold
                    if (
                        patch.confidence is not None
                        and patch.confidence < _conf_threshold
                        and effective_mode not in ("human", "disabled")
                    ):
                        click.echo(
                            f"  ↑ Agent patch confidence {patch.confidence:.0%} < {_conf_threshold:.0%} — escalating to human review",
                            err=True,
                        )
                        effective_mode = "human"

                    _BENIGN_RECOVERIES = {
                        "stripped_code_fence",
                        "stripped_think_block",
                        "stripped_orphan_think_close",
                        "stripped_leading_prose",
                    }
                    _risky_recovery = [
                        r for r in agent_result.recovery_applied if r not in _BENIGN_RECOVERIES
                    ]
                    if _risky_recovery and effective_mode == "auto":
                        click.echo(
                            f"  ↑ Agent response needed mechanical recovery "
                            f"({', '.join(_risky_recovery)}) — "
                            f"downgrading to human review for safety",
                            err=True,
                        )
                        effective_mode = "human"

                    # The candidate this attempt validates/applies/writes is
                    # ALWAYS the full accumulated chain — earlier links plus
                    # this new one — never the candidate alone. When there
                    # are no accumulated links yet, merge_patch_specs returns
                    # the candidate unchanged.
                    _combined_candidate = (
                        merge_patch_specs(accumulated_patches + [patch])
                        if accumulated_patches
                        else patch
                    )

                    # ── Guardrail check (pre-staging) ───────────────────────
                    try:
                        import yaml as _yaml

                        from aqueduct.patch.apply import PatchError as _PatchError
                        from aqueduct.patch.apply import (
                            _check_guardrails as _apply_check_guardrails,
                        )

                        _bp_raw = _yaml.safe_load(blueprint_abs.read_text(encoding="utf-8")) or {}
                        _apply_check_guardrails(
                            _combined_candidate,
                            _bp_raw,
                            provenance_map=manifest.provenance_map,
                            cfg=cfg,
                        )
                        guardrail_err = None
                    except _PatchError as _ge:
                        guardrail_err = str(_ge)
                    except Exception as _gx:
                        guardrail_err = f"Unexpected guardrail error: {_gx}"
                    if guardrail_err:
                        last_apply_error = f"Patch {_combined_candidate.patch_id!r} was blocked by agent guardrail: {guardrail_err}"
                        click.echo(
                            f"  ✗ Agent patch blocked by guardrail: {guardrail_err}", err=True
                        )
                        stage_patch_for_human(
                            _combined_candidate,
                            patches_dir,
                            current_failure,
                            on_patch_pending_webhook=cfg.webhooks.on_patch_pending,
                            source=_patch_source,
                            patch_store=_patch_store,
                            obs_store=_obs_store,
                            on_defer_webhook=cfg.webhooks.on_defer,
                        )
                        _fire_heal_hook(
                            "on_patch_pending",
                            iter_run_id=iteration_run_id,
                            hook_status="pending",
                            ctx=current_failure,
                        )
                        click.echo(
                            f"  ▸ Patch staged for human review → "
                            f"{_patch_store.location_label if _patch_store is not None else patches_dir}/pending/  "
                            f"(id={_combined_candidate.patch_id})",
                            err=True,
                        )
                        surveyor.record_healing_outcome(
                            run_id=iteration_run_id,
                            failed_module=current_failure.failed_module,
                            parent_run_id=run_id,
                            failure_category=_combined_candidate.category,
                            model=_outcome_model,
                            patch_id=_combined_candidate.patch_id,
                            confidence=_combined_candidate.confidence,
                            patch_applied=False,
                            run_success_after_patch=False,
                            failure_signature=_sig_exact.hash,
                            failure_signature_coarse=_sig_coarse.hash,
                            resolution=_resolution,
                            model_cascade_position=_cascade_pos,
                        )
                        break

                    if effective_mode == "human":
                        stage_patch_for_human(
                            _combined_candidate,
                            patches_dir,
                            current_failure,
                            on_patch_pending_webhook=cfg.webhooks.on_patch_pending,
                            source=_patch_source,
                            patch_store=_patch_store,
                            obs_store=_obs_store,
                            on_defer_webhook=cfg.webhooks.on_defer,
                        )
                        _fire_heal_hook(
                            "on_patch_pending",
                            iter_run_id=iteration_run_id,
                            hook_status="pending",
                            ctx=current_failure,
                        )
                        patch_staged_for_review = True
                        rel_bp = (
                            Path(blueprint).relative_to(_project_root)
                            if Path(blueprint).is_relative_to(_project_root)
                            else Path(blueprint)
                        )
                        click.echo(
                            f"  ▸ Agent patch staged → "
                            f"{_patch_store.location_label if _patch_store is not None else patches_dir}/pending/  "
                            f"(id={_combined_candidate.patch_id})\n"
                            f"    Review: aqueduct patch apply {_combined_candidate.patch_id} --blueprint {rel_bp}",
                            err=True,
                        )
                        surveyor.record_healing_outcome(
                            run_id=iteration_run_id,
                            failed_module=current_failure.failed_module,
                            parent_run_id=run_id,
                            failure_category=_combined_candidate.category,
                            model=_outcome_model,
                            patch_id=_combined_candidate.patch_id,
                            confidence=_combined_candidate.confidence,
                            patch_applied=False,
                            run_success_after_patch=False,
                            failure_signature=_sig_exact.hash,
                            failure_signature_coarse=_sig_coarse.hash,
                            resolution=_resolution,
                            model_cascade_position=_cascade_pos,
                        )
                        break

                    elif effective_mode == "auto":
                        has_defer = any(op.op == "defer_to_human" for op in patch.operations)
                        _defer_only = has_defer and all(
                            op.op == "defer_to_human" for op in patch.operations
                        )
                        if _defer_only:
                            click.echo(
                                "  ▸ defer-only patch — skipping sandbox/gate/apply, "
                                "staging for human review",
                                err=True,
                            )
                            stage_patch_for_human(
                                _combined_candidate,
                                patches_dir,
                                current_failure,
                                on_patch_pending_webhook=cfg.webhooks.on_patch_pending,
                                source=_patch_source,
                                patch_store=_patch_store,
                                obs_store=_obs_store,
                                on_defer_webhook=cfg.webhooks.on_defer,
                            )
                            _fire_heal_hook(
                                "on_patch_pending",
                                iter_run_id=iteration_run_id,
                                hook_status="pending",
                                ctx=current_failure,
                            )
                            patch_staged_for_review = True
                            _rel_bp_defer = (
                                Path(blueprint).relative_to(_project_root)
                                if Path(blueprint).is_relative_to(_project_root)
                                else Path(blueprint)
                            )
                            click.echo(
                                f"  ▸ Agent patch staged → "
                                f"{_patch_store.location_label if _patch_store is not None else patches_dir}/pending/  "
                                f"(id={_combined_candidate.patch_id})\n"
                                f"    Review: aqueduct patch apply {_combined_candidate.patch_id} --blueprint {_rel_bp_defer}",
                                err=True,
                            )
                            surveyor.record_healing_outcome(
                                run_id=iteration_run_id,
                                failed_module=current_failure.failed_module,
                                parent_run_id=run_id,
                                failure_category=_combined_candidate.category,
                                model=_outcome_model,
                                patch_id=_combined_candidate.patch_id,
                                confidence=_combined_candidate.confidence,
                                patch_applied=False,
                                run_success_after_patch=False,
                                failure_signature=_sig_exact.hash,
                                failure_signature_coarse=_sig_coarse.hash,
                                resolution=_resolution,
                                model_cascade_position=_cascade_pos,
                            )
                            break

                        # Multi-patch gate validation: sandbox + resolvability
                        # replay check before writing to the blueprint —
                        # against the FULL combined candidate, so a chain's
                        # earlier links are honored by the gate too.
                        _g2, _g3, _g4, _g3_passed = _aqcli._run_patch_gates_inline(
                            patch=_combined_candidate,
                            blueprint_path=Path(blueprint),
                            bundle=bundle,
                            surveyor=surveyor,
                            failed_module=current_failure.failed_module,
                            iteration_run_id=iteration_run_id,
                            blueprint_id=manifest.blueprint_id,
                            engine=(current_failure.engine or engine),
                            cfg=cfg,
                            sandbox_mode=(
                                manifest.agent.sandbox_mode if manifest.agent else "sample"
                            ),
                            sandbox_master_url=resolved_sandbox_master_url,
                            warnings_suppress=cfg.warnings.suppress,
                            timezone=cfg.timezone,
                            depot_reads_at_failure=_cr.depot_reads,
                        )
                        _announce_polyglot_sandbox_unavailable(_g3)
                        _print_gate_ladder(_g2, _g3, _g4, verbosity=verbosity)
                        if _g3 is not None and not _g3_passed:
                            click.echo(
                                f"  ✗ multi-patch: sandbox rejected patch — {_g3.detail}",
                                err=True,
                            )
                            last_apply_error = f"Patch {_combined_candidate.patch_id!r} rejected by sandbox: {_g3.detail}"
                            surveyor.record_healing_outcome(
                                run_id=iteration_run_id,
                                failed_module=current_failure.failed_module,
                                parent_run_id=run_id,
                                failure_category=_combined_candidate.category,
                                model=_outcome_model,
                                patch_id=_combined_candidate.patch_id,
                                confidence=_combined_candidate.confidence,
                                patch_applied=False,
                                run_success_after_patch=False,
                                failure_signature=_sig_exact.hash,
                                failure_signature_coarse=_sig_coarse.hash,
                                resolution=_resolution,
                                model_cascade_position=_cascade_pos,
                            )
                            patch_rejected_by_gate = (
                                True  # sandbox gate → VALIDATION_GATE(4) if loop exhausts
                            )
                            # Gate-rejects it: discard just this candidate
                            # (accumulated_patches are already proven; kept),
                            # retry the SAME current_failure.
                            _chain_last_rejected = patch
                            if manifest.agent.on_heal_failure == "abort":
                                _final = (
                                    merge_patch_specs(accumulated_patches + [patch])
                                    if accumulated_patches
                                    else patch
                                )
                                _aqcli._stage_failed_patch(
                                    manifest.agent.on_heal_failure,
                                    _final,
                                    patches_dir,
                                    current_failure,
                                    cfg,
                                    click,
                                    obs_store=_obs_store,
                                    patch_store=_patch_store,
                                )
                                break
                            continue  # retry current_failure — try next patch

                        _patch_validation = (
                            manifest.agent.patch_validation or cfg.agent.patch_validation
                        )

                        if (
                            _patch_validation == "sandbox"
                            and _g3 is not None
                            and _g3.status == "pass"
                        ):
                            _aqcli._write_patch_to_blueprint(
                                _combined_candidate,
                                Path(blueprint),
                                patches_dir,
                                current_failure,
                                mode="auto",
                                obs_store=_obs_store,
                                patch_store=_patch_store,
                                cfg=cfg,
                            )
                            click.echo(
                                f"  ✓ multi-patch: sandbox-only validated ({_g3.sample_rows or '∞'} rows) → {blueprint}",
                                err=True,
                            )
                            surveyor.record_healing_outcome(
                                run_id=iteration_run_id,
                                failed_module=current_failure.failed_module,
                                parent_run_id=run_id,
                                failure_category=_combined_candidate.category,
                                model=_outcome_model,
                                patch_id=_combined_candidate.patch_id,
                                confidence=_combined_candidate.confidence,
                                patch_applied=True,
                                run_success_after_patch=True,
                                failure_signature=_sig_exact.hash,
                                failure_signature_coarse=_sig_coarse.hash,
                                resolution=_resolution,
                                model_cascade_position=_cascade_pos,
                            )
                            failure_ctx = current_failure
                            break

                        import warnings as _wsup

                        from aqueduct.warnings import AqueductWarning as _AqWarn

                        with _wsup.catch_warnings():
                            _wsup.simplefilter("ignore", _AqWarn)
                            new_manifest = _aqcli._apply_patch_in_memory(
                                _combined_candidate,
                                Path(blueprint),
                                depot,
                                profile,
                                cli_overrides or {},
                            )
                        if new_manifest is None:
                            click.echo(
                                "  ✗ Agent patch produces invalid Blueprint, discarding", err=True
                            )
                            last_apply_error = (
                                f"Patch {_combined_candidate.patch_id!r} produced invalid Blueprint"
                            )
                            surveyor.record_healing_outcome(
                                run_id=iteration_run_id,
                                failed_module=current_failure.failed_module,
                                parent_run_id=run_id,
                                failure_category=_combined_candidate.category,
                                model=_outcome_model,
                                patch_id=_combined_candidate.patch_id,
                                confidence=_combined_candidate.confidence,
                                patch_applied=False,
                                run_success_after_patch=False,
                                failure_signature=_sig_exact.hash,
                                failure_signature_coarse=_sig_coarse.hash,
                                resolution=_resolution,
                                model_cascade_position=_cascade_pos,
                            )
                            # Unusable outright — discard just this candidate,
                            # retry the SAME current_failure.
                            _chain_last_rejected = patch
                            if manifest.agent.on_heal_failure == "abort":
                                _final = (
                                    merge_patch_specs(accumulated_patches + [patch])
                                    if accumulated_patches
                                    else patch
                                )
                                _aqcli._stage_failed_patch(
                                    manifest.agent.on_heal_failure,
                                    _final,
                                    patches_dir,
                                    current_failure,
                                    cfg,
                                    click,
                                    obs_store=_obs_store,
                                    patch_store=_patch_store,
                                )
                                break
                            continue

                        result2, _exc2 = _execute_target(
                            new_manifest,
                            run_id=str(uuid.uuid4()),
                            store_dir=resolved_store_dir,
                            checkpoint_root=checkpoint_root_abs,
                            surveyor=surveyor,
                            depot=depot,
                        )
                        patch_success = result2.status == ExecutionStatus.SUCCESS
                        failure_ctx2 = surveyor.record(
                            result2, patched=patch_success, engine=result2.failed_engine
                        )
                        surveyor.record_healing_outcome(
                            run_id=iteration_run_id,
                            failed_module=current_failure.failed_module,
                            parent_run_id=run_id,
                            failure_category=_combined_candidate.category,
                            model=_outcome_model,
                            patch_id=_combined_candidate.patch_id,
                            confidence=_combined_candidate.confidence,
                            patch_applied=True,
                            run_success_after_patch=patch_success,
                            failure_signature=_sig_exact.hash,
                            failure_signature_coarse=_sig_coarse.hash,
                            resolution=_resolution,
                            model_cascade_position=_cascade_pos,
                        )
                        result = result2
                        failure_ctx = failure_ctx2
                        if patch_success:
                            _aqcli._write_patch_to_blueprint(
                                _combined_candidate,
                                Path(blueprint),
                                patches_dir,
                                current_failure,
                                mode="auto",
                                obs_store=_obs_store,
                                patch_store=_patch_store,
                                cfg=cfg,
                            )
                            click.echo(
                                f"  {click.style('✓', fg='green', bold=True)} Agent patch validated and applied ({patch_count}/{max_patches}) → {blueprint}",
                                err=True,
                            )
                            _fire_heal_hook(
                                "on_healed",
                                iter_run_id=iteration_run_id,
                                hook_status="healed",
                                ctx=current_failure,
                            )
                            _healed_attempt_num = (
                                agent_result.attempt_records[-1].attempt_num
                                if agent_result.attempt_records
                                else agent_result.attempts
                            )
                            _render_module_summary(
                                result2,
                                failure_ctx2,
                                healed_module=current_failure.failed_module,
                                healed_patch_num=_healed_attempt_num,
                            )
                            break
                        else:
                            _next_module = getattr(failure_ctx2, "failed_module", None) or "<root>"
                            _this_module = current_failure.failed_module or "<root>"
                            if failure_ctx2 is not None and _next_module != _this_module:
                                # DIFFERENT module — the candidate was right:
                                # fold it into the accumulated chain and
                                # advance to diagnose the new failure.
                                click.echo(
                                    f"  ↳ {_this_module} fixed, new failure at "
                                    f"{_next_module} — chaining ({patch_count}/{max_patches})",
                                    err=True,
                                )
                                accumulated_patches.append(patch)
                                current_failure = failure_ctx2
                                _chain_last_rejected = None
                                last_apply_error = None
                                continue

                            # SAME module (or no new failure context) — wrong
                            # patch: discard just this candidate (accumulated
                            # patches, already proven, are kept) and retry
                            # the SAME current_failure.
                            last_apply_error = (
                                f"Patch {_combined_candidate.patch_id!r} applied in-memory but re-run still failed: "
                                + (
                                    result2.module_results[-1].error or "unknown"
                                    if result2.module_results
                                    else "unknown"
                                )
                            )
                            click.echo(
                                f"  {click.style('✗', fg='red', bold=True)} Agent patch did not fix the issue ({patch_count}/{max_patches})",
                                err=True,
                            )
                            _chain_last_rejected = patch
                            if manifest.agent.on_heal_failure == "abort":
                                _final = merge_patch_specs(accumulated_patches + [patch])
                                _aqcli._stage_failed_patch(
                                    manifest.agent.on_heal_failure,
                                    _final,
                                    patches_dir,
                                    current_failure,
                                    cfg,
                                    click,
                                    obs_store=_obs_store,
                                    patch_store=_patch_store,
                                )
                                break
                            # discard/stage: chain continues → retry same failure
                            continue

                else:
                    # Loop exited via `while patch_count < max_patches`
                    # becoming false (budget exhausted) rather than an
                    # explicit `break` above — the last thing that happened
                    # was either a successful advance (nothing pending) or
                    # the loop never ran a first attempt. Stage whatever was
                    # accumulated (+ the last rejected candidate, if any), per
                    # the same "exactly one combined patch ever staged" rule.
                    click.echo(
                        f"⚠  Agent: max_patches={max_patches} reached, stopping self-healing loop",
                        err=True,
                    )
                    _final_patches = list(accumulated_patches)
                    if _chain_last_rejected is not None:
                        _final_patches.append(_chain_last_rejected)
                    if _final_patches:
                        _final = merge_patch_specs(_final_patches)
                        _aqcli._stage_failed_patch(
                            manifest.agent.on_heal_failure,
                            _final,
                            patches_dir,
                            current_failure,
                            cfg,
                            click,
                            obs_store=_obs_store,
                            patch_store=_patch_store,
                        )

                # The entire chain (all retries, all links) is driven by the
                # inner `while` above — there is no more outer-loop-driven
                # retry for the non-cascade path (that was the exact bug this
                # fold fixes: an outer `continue` used to throw away every
                # accumulated link and restart a fresh chain). Always end the
                # run's outer loop here, regardless of how the chain ended.
                break

        # ── Surveyor stop ─────────────────────────────────────────────────────────
        surveyor.stop()

        # ── Depot — persist run_id for @aq.run.prev_id() ─────────────────
        try:
            depot.put("_last_run_id", run_id)
        except Exception:
            pass  # depot write is best-effort; prev_run_id unavailability is a soft degradation, not a failure
        depot.close()

        # ── Report ────────────────────────────────────────────────────────────────
        # The per-module ✓/✗ summary already printed inline (per heal iteration)
        # via `_render_module_summary` right after each execute — so the heal
        # block reads chronologically after the result it heals. Only the framed
        # terminal footer remains here.

        # T26 — end-of-run runtime-warning roll-up: a single collapsed tally of
        # everything that warned this run (additive to the inline `↳` lines
        # under each module — locality there, "don't miss it" tally here). Reuses
        # the compile-block shape; empty → nothing.
        _runtime_pairs = [
            (rid, f"{mr.module_id}: {msg}")
            for mr in result.module_results
            for rid, msg in mr.warnings
        ]
        if _runtime_pairs:
            from aqueduct.cli.render.style import emit_warning_pairs

            emit_warning_pairs(_runtime_pairs, label="runtime:", verbose=verbosity >= 1, err=True)

        if result.status not in (ExecutionStatus.SUCCESS, ExecutionStatus.PATCHED):
            # Print the outer (user-visible) run_id — that's the join key for
            # heal_attempts and `healing_outcomes.parent_run_id`. In multi-patch
            # mode `result.run_id` would be the LAST iteration's per-iteration
            # uuid, which can't be used to retrieve the full heal history.
            from aqueduct.cli.render.style import dim as _dim
            from aqueduct.cli.render.style import error as _style_error

            # Stdout, explicitly: the closing divider + verdict are part of
            # the SAME framed result block as the header/tree above (must
            # survive `> run.log` piped alone) — `style.error` defaults to
            # stderr, so the destination is overridden here rather than
            # fought around.
            click.echo(_dim(_rule()), err=False)
            if failure_ctx:
                _style_error(
                    f"blueprint failed  run_id={run_id}"
                    f"  failed_module={failure_ctx.failed_module}",
                    err=False,
                )
            else:
                _style_error(f"blueprint failed  run_id={run_id}", err=False)
            # on_failure hooks — after the verdict line, before the exit code.
            # Hook outcomes never alter the exit code below.
            from aqueduct.cli.hooks import run_hooks as _run_hooks

            _run_hooks(
                manifest.hooks.on_failure,
                "on_failure",
                run_id=run_id,
                status="failure",
                blueprint_id=manifest.blueprint_id,
                blueprint_path=blueprint,
                allow_command_hooks=cfg.danger.allow_command_hooks,
                failure_ctx=failure_ctx,
                session=_session_holder.session,
                engine=engine,
            )
            # Distinguish the three non-success terminal states for downstream
            # orchestrators (Airflow operator, CI runners):
            #   HEAL_PENDING(3)   — a patch was staged for human/ci review
            #   VALIDATION_GATE(4)— auto-mode patch rejected by a validation gate
            #   DATA_OR_RUNTIME(2)— hard runtime failure, no actionable patch
            if patch_staged_for_review:
                sys.exit(exit_codes.HEAL_PENDING)
            if patch_rejected_by_gate:
                sys.exit(exit_codes.VALIDATION_GATE)
            sys.exit(exit_codes.DATA_OR_RUNTIME)

        # ── Self-heal provenance: green run stamps validated_on (Phase 79) ────────
        # Best-effort — `stamp_validated_engine` never raises (it logs its own
        # failures internally) and never affects the run's outcome or exit
        # code. No-op when the Blueprint carries no `healed_by:` block at all.
        try:
            from aqueduct.patch.apply import stamp_perf_observation, stamp_validated_engine

            stamp_validated_engine(Path(blueprint), engine)
            # Warn-only perf attribution: `validated_on` above says the run
            # was green, which a patch that tripled the runtime also says.
            # This says what it cost. Reports, never blocks, never judges —
            # Aqueduct sets no regression threshold, so the ratio is printed
            # and a human decides.
            for _obs in stamp_perf_observation(
                Path(blueprint), engine, obs_store=_obs_store, run_id=result.run_id
            ):
                if _obs.get("status") != "observed":
                    continue
                from aqueduct.cli.render.funnel import emit_info as _emit_info

                _emit_info(f"perf vs pre-patch baseline: {_obs['detail']}", err=True)
                for _caveat in _obs.get("caveats") or []:
                    _emit_info(f"  {_caveat}", err=True)
        except Exception:
            pass  # provenance stamping must never affect a successful run

        # ── on_success webhook ────────────────────────────────────────────────────
        if cfg.webhooks.on_success:
            from aqueduct.surveyor.webhook import fire_webhook

            success_payload = {
                "run_id": run_id,
                "blueprint_id": manifest.blueprint_id,
                "blueprint_name": manifest.name,
                "module_count": str(len(result.module_results)),
            }
            fire_webhook(
                cfg.webhooks.on_success,
                full_payload=success_payload,
                template_vars=success_payload,
                event="on_success",
            )

        status_label = "patched" if result.status == ExecutionStatus.PATCHED else "complete"
        from aqueduct.cli.render.style import dim as _dim
        from aqueduct.cli.render.style import success as _style_success

        # Phase 85 Wave 2 — total wall-clock time (one added line vs today,
        # SCREEN 1 notes) + healed-module count + pending-review hint when
        # this run auto-applied a patch (SCREEN 2). `patch_count` is the
        # number of patches actually applied this run (multi-patch loop).
        _elapsed_s = _time85.monotonic() - _run_started_at
        _footer_text = f"blueprint {status_label} · {_elapsed_s:.1f}s"
        if status_label == "patched" and patch_count:
            _footer_text += (
                f" · {patch_count} module{'s' if patch_count != 1 else ''} healed"
                " · pending review"
            )
        click.echo(_dim(_rule()), err=False)
        _style_success(_footer_text, err=False)
        if status_label == "patched" and patch_count:
            # Q5 ruling: ONE line, not a multi-line patch-id + command block.
            click.echo(
                click.style(f"  ⓘ review: aqueduct patch pull {run_id}", fg="cyan"), err=False
            )

        # on_success hooks — chained blueprints / webhooks / gated commands.
        # A hooks section closes with its own `run complete` footer.
        from aqueduct.cli.hooks import run_hooks as _run_hooks

        if _run_hooks(
            manifest.hooks.on_success,
            "on_success",
            run_id=run_id,
            status=result.status,
            blueprint_id=manifest.blueprint_id,
            blueprint_path=blueprint,
            allow_command_hooks=cfg.danger.allow_command_hooks,
            session=_session_holder.session,
            engine=engine,
        ):
            _style_success("run complete", err=False)
    finally:
        os.chdir(_original_cwd)
