"""Aqueduct CLI.

Commands: init, validate, compile, run, check-config, doctor, runs, report,
          lineage, signal, heal, log, rollback,
          patch apply, patch reject, patch commit, patch discard, patch list.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

import click


# ── Guardrail validation ──────────────────────────────────────────────────────

def _check_guardrails(patch: Any, agent: Any) -> str | None:
    """Return error message if patch violates agent guardrail policy, else None."""
    import fnmatch

    for op_dict in getattr(patch, "operations", []):
        op_name = op_dict.get("op", "") if isinstance(op_dict, dict) else ""

        if agent.guardrails.forbidden_ops and op_name in agent.guardrails.forbidden_ops:
            return f"Operation {op_name!r} blocked by agent.guardrails.forbidden_ops"

        if agent.guardrails.allowed_paths:
            config = (op_dict.get("config") or {}) if isinstance(op_dict, dict) else {}
            path_val = config.get("path") if isinstance(config, dict) else None
            # Skip check for unresolved context refs — they're validated at runtime
            if path_val and not str(path_val).startswith("${ctx.") and not any(
                fnmatch.fnmatch(str(path_val), pat) for pat in agent.guardrails.allowed_paths
            ):
                return f"Path {path_val!r} not in agent.guardrails.allowed_paths whitelist"

    return None


# ── Self-healing helpers ──────────────────────────────────────────────────────

def _apply_patch_in_memory(patch, blueprint_path: Path, depot, profile, cli_overrides: dict) -> Any:
    """Apply patch operations to Blueprint without touching disk. Returns new Manifest or None."""
    try:
        from aqueduct.patch.apply import _yaml_dump, _yaml_load, apply_patch_to_dict
        from aqueduct.parser.parser import ParseError, parse
        from aqueduct.compiler.compiler import compile as compiler_compile, CompileError

        bp_raw = _yaml_load(blueprint_path)
        patched = apply_patch_to_dict(bp_raw, patch)

        with tempfile.NamedTemporaryFile(suffix=".yml", delete=False, mode="w") as tmp:
            tmp_path = Path(tmp.name)
        _yaml_dump(patched, tmp_path)
        try:
            bp = parse(str(tmp_path), profile=profile, cli_overrides=cli_overrides or None)
            return compiler_compile(bp, blueprint_path=tmp_path, depot=depot)
        except (ParseError, CompileError):
            return None
        finally:
            tmp_path.unlink(missing_ok=True)
    except Exception:
        return None


def _write_patch_to_blueprint(patch, blueprint_path: Path, patches_dir: Path, failure_ctx, mode: str) -> Any:
    """Write patch permanently to Blueprint, re-parse, re-compile. Returns new Manifest or None."""
    try:
        import os as _os
        from aqueduct.patch.apply import _yaml_dump, _yaml_load, apply_patch_to_dict
        from aqueduct.parser.parser import ParseError, parse
        from aqueduct.compiler.compiler import compile as compiler_compile, CompileError
        from aqueduct.surveyor.llm import archive_patch

        bp_raw = _yaml_load(blueprint_path)
        patched = apply_patch_to_dict(bp_raw, patch)

        # Backup original
        backup_dir = patches_dir / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        import shutil
        from datetime import datetime, timezone
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        shutil.copy2(blueprint_path, backup_dir / f"{patch.patch_id}_{ts}_{blueprint_path.name}")

        # Write atomically
        tmp_out = blueprint_path.with_suffix(".llm_patch.tmp.yml")
        _yaml_dump(patched, tmp_out)
        _os.replace(tmp_out, blueprint_path)

        archive_patch(patch, patches_dir, failure_ctx, mode=mode)

        # Re-parse + re-compile from updated file
        bp = parse(str(blueprint_path))
        return compiler_compile(bp, blueprint_path=blueprint_path)
    except (ParseError, CompileError):
        return None
    except Exception:
        return None


@click.group()
@click.version_option(package_name="aqueduct")
def cli() -> None:
    """Aqueduct — Intelligent Spark Blueprint Engine."""


@cli.command()
@click.argument("blueprint", type=click.Path(exists=True, dir_okay=False))
def validate(blueprint: str) -> None:
    """Parse and validate a Blueprint. Exit 0 = valid, 1 = invalid."""
    from aqueduct.parser.parser import ParseError, parse

    try:
        bp = parse(blueprint)
        click.echo(
            f"✓ {bp.id}  ({len(bp.modules)} modules, {len(bp.edges)} edges)"
        )
    except ParseError as exc:
        click.echo(f"✗ {exc}", err=True)
        sys.exit(1)


@cli.command("check-config")
@click.option(
    "--config",
    "config_path",
    default=None,
    type=click.Path(dir_okay=False),
    help="Path to aqueduct.yml (default: aqueduct.yml in CWD)",
)
def check_config(config_path: str | None) -> None:
    """Validate aqueduct.yml without running a blueprint. Exit 0 = valid, 1 = invalid."""
    import json
    from pathlib import Path

    from aqueduct.config import ConfigError, load_config

    try:
        cfg = load_config(Path(config_path) if config_path else None)
    except ConfigError as exc:
        click.echo(f"✗ {exc}", err=True)
        sys.exit(1)

    source = config_path or "aqueduct.yml (CWD) or defaults"
    click.echo(f"✓ config valid  [{source}]")
    click.echo(f"  engine:  {cfg.deployment.engine}  target={cfg.deployment.target}  master={cfg.deployment.master_url}")
    click.echo(f"  stores:  obs={cfg.stores.obs.path}  lineage={cfg.stores.lineage.path}  depot={cfg.stores.depot.path}")
    click.echo(f"  secrets: provider={cfg.secrets.provider}")
    wh_lines = []
    if cfg.webhooks.on_failure:
        wh = cfg.webhooks.on_failure
        wh_lines.append(f"on_failure={wh.method} {wh.url}")
    if cfg.webhooks.on_success:
        wh = cfg.webhooks.on_success
        wh_lines.append(f"on_success={wh.method} {wh.url}")
    click.echo(f"  webhooks: {', '.join(wh_lines) if wh_lines else '(not configured)'}")
    if cfg.spark_config:
        click.echo(f"  spark_config: {json.dumps(cfg.spark_config)}")


@cli.command()
@click.option(
    "--config",
    "config_path",
    default=None,
    type=click.Path(dir_okay=False),
    help="Path to aqueduct.yml (default: aqueduct.yml in CWD)",
)
@click.option(
    "--skip-spark",
    is_flag=True,
    default=False,
    help="Skip Spark connectivity check (fast mode — avoids JVM startup).",
)
@click.option(
    "--blueprint",
    "blueprint_path",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Also probe all Ingress/Egress paths and JDBC endpoints declared in this Blueprint.",
)
def doctor(config_path: str | None, skip_spark: bool, blueprint_path: str | None) -> None:
    """Probe all configured resources: config, stores, Spark, webhook, secrets, storage.

    Each check is independent. Spark check requires pyspark and may take 10-15s
    for JVM startup. Use --skip-spark to skip it in fast CI contexts.

    With --blueprint: additionally checks all Ingress/Egress sources — local path
    existence, cloud auth probes, and TCP connectivity for JDBC endpoints.

    Exit codes: 0 = all ok/warn/skip, 1 = any check failed.
    """
    from pathlib import Path
    from aqueduct.doctor import run_doctor

    _STATUS_ICON = {"ok": "✓", "fail": "✗", "warn": "⚠", "skip": "-"}
    _STATUS_COLOR = {"ok": "green", "fail": "red", "warn": "yellow", "skip": None}

    if not skip_spark:
        click.echo("Running connectivity checks (Spark may take 10–15s for JVM startup)...")
    else:
        click.echo("Running connectivity checks (--skip-spark: Spark check skipped)...")

    results = run_doctor(
        config_path=Path(config_path) if config_path else None,
        skip_spark=skip_spark,
        blueprint_path=Path(blueprint_path) if blueprint_path else None,
    )

    col_w = max(len(r.name) for r in results) + 2
    any_fail = False
    for r in results:
        icon = _STATUS_ICON[r.status]
        color = _STATUS_COLOR[r.status]
        label = r.name.ljust(col_w)
        elapsed = f"  [{r.elapsed_ms}ms]" if r.elapsed_ms > 0 else ""
        line = f"  {icon} {label}{r.detail}{elapsed}"
        click.echo(click.style(line, fg=color) if color else line)
        if r.status == "fail":
            any_fail = True

    click.echo()
    if any_fail:
        click.echo(click.style("✗ one or more checks failed", fg="red"), err=True)
        sys.exit(1)
    else:
        click.echo(click.style("✓ all checks passed", fg="green"))


@cli.command()
@click.argument("blueprint", type=click.Path(exists=True, dir_okay=False))
@click.option("-o", "--output", default="-", show_default=True, help="Output path (- = stdout)")
@click.option("-p", "--profile", default=None, help="Context profile to activate")
@click.option(
    "--ctx",
    multiple=True,
    metavar="KEY=VALUE",
    help="Context override. Repeatable.",
)
@click.option(
    "--execution-date",
    "execution_date_str",
    default=None,
    metavar="YYYY-MM-DD",
    help="Logical execution date for @aq.date.* functions",
)
def compile(blueprint: str, output: str, profile: str | None, ctx: tuple[str, ...], execution_date_str: str | None) -> None:
    """Parse and compile a Blueprint to a fully-resolved Manifest JSON."""
    from pathlib import Path

    from aqueduct.compiler.compiler import CompileError
    from aqueduct.compiler.compiler import compile as compiler_compile
    from aqueduct.parser.parser import ParseError, parse

    cli_overrides: dict[str, str] = {}
    for item in ctx:
        if "=" not in item:
            click.echo(f"--ctx flag must be KEY=VALUE, got: {item!r}", err=True)
            sys.exit(1)
        k, _, v = item.partition("=")
        cli_overrides[k.strip()] = v

    execution_date = None
    if execution_date_str:
        from datetime import date as _date
        try:
            execution_date = _date.fromisoformat(execution_date_str)
        except ValueError:
            click.echo(f"✗ --execution-date must be YYYY-MM-DD, got: {execution_date_str!r}", err=True)
            sys.exit(1)

    try:
        bp = parse(blueprint, profile=profile, cli_overrides=cli_overrides or None)
    except ParseError as exc:
        click.echo(f"✗ {exc}", err=True)
        sys.exit(1)

    try:
        manifest = compiler_compile(bp, blueprint_path=Path(blueprint), execution_date=execution_date)
    except CompileError as exc:
        click.echo(f"✗ {exc}", err=True)
        sys.exit(1)

    manifest_json = json.dumps(manifest.to_dict(), indent=2)

    if output == "-":
        click.echo(manifest_json)
    else:
        Path(output).write_text(manifest_json, encoding="utf-8")
        click.echo(f"Manifest written → {output}")

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
    help="Store directory (overrides aqueduct.yml; default: .aqueduct)",
)
@click.option("--webhook", default=None, help="Webhook URL for failure notifications (overrides aqueduct.yml)")
@click.option("--resume", "resume_run_id", default=None, help="Resume from checkpoints of a previous run_id")
@click.option("--from", "from_module", default=None, metavar="MODULE_ID", help="Start execution at this module (skip all preceding modules)")
@click.option("--to", "to_module", default=None, metavar="MODULE_ID", help="Stop execution after this module (skip all subsequent modules)")
@click.option(
    "--execution-date",
    "execution_date_str",
    default=None,
    metavar="YYYY-MM-DD",
    help="Logical execution date for @aq.date.* functions — enables idempotent backfills",
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
) -> None:
    """Compile and execute a Blueprint on a SparkSession."""
    import os
    import uuid
    from pathlib import Path

    from aqueduct.compiler.compiler import CompileError
    from aqueduct.compiler.compiler import compile as compiler_compile
    from aqueduct.config import ConfigError, WebhookEndpointConfig, load_config
    from aqueduct.depot.depot import DepotStore
    from aqueduct.executor import ExecuteError, get_executor
    from aqueduct.executor.models import ExecutionResult, ModuleResult
    from aqueduct.parser.parser import ParseError, parse
    from aqueduct.surveyor.surveyor import Surveyor

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
        _project_root = blueprint_abs.parent
        _search = blueprint_abs.parent
        for _ in range(8):
            if (_search / "aqueduct.yml").exists():
                _project_root = _search
                break
            if _search.parent == _search:
                break
            _search = _search.parent

    os.chdir(_project_root)
    # Rebind blueprint to absolute so all downstream code is CWD-agnostic.
    blueprint = str(blueprint_abs)

    # ── Load engine config ─────────────────────────────────────────────────────
    try:
        cfg = load_config(config_path_abs)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(1)

    # CLI flags override config file; config file overrides built-in defaults
    resolved_store_dir = store_dir_abs if store_dir_abs else Path(cfg.stores.obs.path).parent
    # --webhook CLI flag (plain URL) overrides aqueduct.yml; config may be full WebhookEndpointConfig
    resolved_webhook = WebhookEndpointConfig(url=webhook) if webhook else cfg.webhooks.on_failure
    engine = cfg.deployment.engine
    master_url = cfg.deployment.master_url

    # Resolve executor early so an unsupported engine exits before any Spark work
    try:
        execute = get_executor(engine)
    except (NotImplementedError, ValueError) as exc:
        click.echo(f"✗ engine error: {exc}", err=True)
        sys.exit(1)

    cli_overrides: dict[str, str] = {}
    for item in ctx:
        if "=" not in item:
            click.echo(f"--ctx flag must be KEY=VALUE, got: {item!r}", err=True)
            sys.exit(1)
        k, _, v = item.partition("=")
        cli_overrides[k.strip()] = v

    # ── Parse --execution-date ─────────────────────────────────────────────────
    execution_date = None
    if execution_date_str:
        from datetime import date as _date
        try:
            execution_date = _date.fromisoformat(execution_date_str)
        except ValueError:
            click.echo(f"✗ --execution-date must be YYYY-MM-DD, got: {execution_date_str!r}", err=True)
            sys.exit(1)

    # ── Depot — open before compile so @aq.depot.get() resolves ──────────────
    depot_path = Path(cfg.stores.depot.path)
    depot = DepotStore(depot_path)

    # ── Parse ──────────────────────────────────────────────────────────────────
    try:
        bp = parse(blueprint, profile=profile, cli_overrides=cli_overrides or None)
    except ParseError as exc:
        click.echo(f"✗ parse error: {exc}", err=True)
        sys.exit(1)

    # ── Compile ────────────────────────────────────────────────────────────────
    try:
        manifest = compiler_compile(
            bp,
            blueprint_path=Path(blueprint),
            depot=depot,
            execution_date=execution_date,
        )
    except CompileError as exc:
        click.echo(f"✗ compile error: {exc}", err=True)
        sys.exit(1)

    # ── Pending patch check ────────────────────────────────────────────────────
    patches_dir = _project_root / "patches"
    pending_dir = patches_dir / "pending"
    pending_patches = list(pending_dir.glob("*.json")) if pending_dir.exists() else []
    if pending_patches:
        policy = manifest.agent.on_pending_patches
        names = ", ".join(p.stem for p in pending_patches)
        msg = (
            f"⚠ {len(pending_patches)} pending patch(es) unreviewed: {names}\n"
            f"  Review with: aqueduct patch apply <file> --blueprint {blueprint}\n"
            f"  Reject with: aqueduct patch reject <patch_id> --reason '...'"
        )
        if policy == "block":
            click.echo(f"✗ blocked — {msg}", err=True)
            sys.exit(1)
        elif policy == "warn":
            click.echo(msg, err=True)

    # ── Uncommitted applied patch warning ──────────────────────────────────────
    uncommitted_applied = _uncommitted_applied_patches(Path(blueprint), patches_dir)
    if uncommitted_applied:
        n_uc = len(uncommitted_applied)
        click.echo(
            f"⚠ {n_uc} applied patch(es) not yet committed to git — "
            f"run 'aqueduct patch commit --blueprint {blueprint}'",
            err=True,
        )

    run_id = run_id or str(uuid.uuid4())
    selector_note = ""
    if from_module or to_module:
        parts = []
        if from_module:
            parts.append(f"from={from_module}")
        if to_module:
            parts.append(f"to={to_module}")
        selector_note = "  [" + ", ".join(parts) + "]"
    exec_date_note = f"  exec_date={execution_date}" if execution_date else ""
    click.echo(
        f"▶ {manifest.blueprint_id}  ({len(manifest.modules)} modules)"
        f"  run={run_id}  engine={engine}  master={master_url}"
        f"{selector_note}{exec_date_note}"
    )

    # ── Resolve agent connection (engine defaults ← blueprint overrides) ─────
    eng = cfg.agent
    bp_agent = manifest.agent
    resolved_agent_provider = bp_agent.provider or eng.provider
    resolved_agent_base_url = bp_agent.base_url or eng.base_url
    resolved_agent_model = bp_agent.model or eng.model
    resolved_agent_ollama_options = bp_agent.ollama_options or eng.ollama_options
    resolved_agent_llm_timeout = eng.llm_timeout
    resolved_agent_llm_max_reprompts = eng.llm_max_reprompts
    resolved_agent_engine_prompt_context = eng.prompt_context
    resolved_agent_blueprint_prompt_context = bp_agent.prompt_context

    # ── Aggressive mode disclaimer ────────────────────────────────────────────
    approval_mode = manifest.agent.approval_mode
    max_patches = manifest.agent.max_patches_per_run
    if approval_mode == "aggressive":
        click.echo(
            f"⚠  approval_mode=aggressive — LLM may modify this Blueprint up to "
            f"{max_patches} time(s) autonomously. All changes are permanent. "
            f"Review patches/applied/ after the run.",
            err=True,
        )

    # ── Surveyor — start ───────────────────────────────────────────────────────
    surveyor = Surveyor(
        manifest,
        store_dir=resolved_store_dir,
        webhook_config=resolved_webhook,
        blueprint_path=Path(blueprint),
        patches_dir=patches_dir,
    )
    surveyor.start(run_id)

    # ── Engine session ────────────────────────────────────────────────────────
    merged_spark_config = {**cfg.spark_config, **manifest.spark_config}
    if engine == "spark":
        from aqueduct.executor.spark.session import make_spark_session
        session = make_spark_session(manifest.blueprint_id, merged_spark_config, master_url=master_url)
    else:
        raise NotImplementedError(f"Session creation for engine {engine!r} not implemented")

    import atexit
    atexit.register(session.stop)

    # ── Self-healing run loop ─────────────────────────────────────────────────
    patch_count = 0
    failure_ctx = None
    result = None
    last_apply_error: str | None = None  # fed back to LLM on next aggressive iteration

    while True:
        current_run_id = run_id if patch_count == 0 else str(uuid.uuid4())
        execute_exc: ExecuteError | None = None
        try:
            result = execute(
                manifest, session,
                run_id=current_run_id,
                store_dir=resolved_store_dir,
                surveyor=surveyor,
                depot=depot,
                resume_run_id=resume_run_id if patch_count == 0 else None,
                from_module=from_module,
                to_module=to_module,
                block_full_actions=cfg.probes.block_full_actions_in_prod,
            )
        except ExecuteError as exc:
            execute_exc = exc
            result = ExecutionResult(
                blueprint_id=manifest.blueprint_id,
                run_id=current_run_id,
                status="error",
                module_results=(
                    ModuleResult(module_id="_executor", status="error", error=str(exc)),
                ),
            )

        failure_ctx = surveyor.record(result, exc=execute_exc)

        if result.status == "success":
            break

        # trigger_agent flag overrides approval_mode=disabled — escalate to human staging at minimum
        effective_mode = approval_mode
        if result.trigger_agent and effective_mode == "disabled":
            effective_mode = "human"
            click.echo(
                "  ↻ LLM triggered by module rule (overriding approval_mode=disabled → staging patch for review)",
                err=True,
            )

        if effective_mode == "disabled" or failure_ctx is None:
            break

        if patch_count >= max_patches:
            click.echo(
                f"⚠  LLM: max_patches_per_run={max_patches} reached, stopping self-healing loop",
                err=True,
            )
            break

        # ── Generate patch ────────────────────────────────────────────────────
        from aqueduct.surveyor.llm import archive_patch, generate_llm_patch, stage_patch_for_human
        click.echo(
            f"  ↻ LLM self-healing ({patch_count + 1}/{max_patches})  "
            f"failed_module={failure_ctx.failed_module}",
            err=True,
        )

        # Run blueprint doctor checks against the compiled Manifest (all modules resolved,
        # arcades expanded — no need to re-parse or recurse into sub-blueprints).
        try:
            from aqueduct.doctor import check_blueprint_sources_from_manifest
            from dataclasses import replace as _dc_replace
            _dr = check_blueprint_sources_from_manifest(manifest)
            _hints = tuple(
                f"{r.name} — {r.detail}"
                for r in _dr if r.status in ("warn", "fail")
            )
            if _hints:
                failure_ctx = _dc_replace(failure_ctx, doctor_hints=_hints)
        except Exception:
            pass  # doctor errors must never block self-healing

        patch = generate_llm_patch(
            failure_ctx,
            model=resolved_agent_model,
            patches_dir=patches_dir,
            provider=resolved_agent_provider,
            base_url=resolved_agent_base_url,
            ollama_options=resolved_agent_ollama_options,
            llm_timeout=resolved_agent_llm_timeout,
            llm_max_reprompts=resolved_agent_llm_max_reprompts,
            engine_prompt_context=resolved_agent_engine_prompt_context,
            blueprint_prompt_context=resolved_agent_blueprint_prompt_context,
            last_apply_error=last_apply_error,
        )
        if patch is None:
            click.echo("  ✗ LLM: failed to generate valid patch, stopping", err=True)
            break

        # ── Guardrail check (pre-staging) ─────────────────────────────────────
        # Use apply.py's guardrail check — resolves ${ctx.*} path values via provenance_map
        try:
            from aqueduct.patch.apply import PatchError as _PatchError, _check_guardrails as _apply_check_guardrails
            import yaml as _yaml
            _bp_raw = _yaml.safe_load(blueprint_abs.read_text(encoding="utf-8")) or {}
            _apply_check_guardrails(patch, _bp_raw, provenance_map=manifest.provenance_map)
            guardrail_err = None
        except _PatchError as _ge:
            guardrail_err = str(_ge)
        except Exception:
            guardrail_err = None  # don't block on unexpected errors
        if guardrail_err:
            last_apply_error = f"Patch {patch.patch_id!r} was blocked by agent guardrail: {guardrail_err}"
            click.echo(f"  ✗ LLM patch blocked by guardrail: {guardrail_err}", err=True)
            stage_patch_for_human(patch, patches_dir, failure_ctx)
            click.echo(
                f"  ✎ Patch staged for human review → patches/pending/{patch.patch_id}.json",
                err=True,
            )
            break

        patch_count += 1

        if effective_mode == "human":
            stage_patch_for_human(patch, patches_dir, failure_ctx)
            pending_file = next(patches_dir.glob(f"pending/*_{patch.patch_id}.json"), None) \
                or patches_dir / "pending" / f"{patch.patch_id}.json"
            rel_patch = pending_file.relative_to(_project_root) if pending_file.is_relative_to(_project_root) else pending_file
            rel_bp = Path(blueprint).relative_to(_project_root) if Path(blueprint).is_relative_to(_project_root) else Path(blueprint)
            click.echo(
                f"  ✎ LLM patch staged → {rel_patch}\n"
                f"    Review: aqueduct patch apply {rel_patch} --blueprint {rel_bp}",
                err=True,
            )
            break  # human reviews; no re-run

        elif effective_mode == "auto":
            # Apply in-memory → re-run → write permanently only if success
            new_manifest = _apply_patch_in_memory(patch, Path(blueprint), depot, profile, cli_overrides or {})
            if new_manifest is None:
                click.echo("  ✗ LLM patch produces invalid Blueprint, discarding", err=True)
                break
            try:
                result2 = execute(
                    new_manifest, session,
                    run_id=str(uuid.uuid4()),
                    store_dir=resolved_store_dir,
                    surveyor=surveyor,
                    depot=depot,
                )
            except ExecuteError as exc:
                result2 = ExecutionResult(
                    blueprint_id=manifest.blueprint_id,
                    run_id=str(uuid.uuid4()),
                    status="error",
                    module_results=(ModuleResult(module_id="_executor", status="error", error=str(exc)),),
                )
            failure_ctx2 = surveyor.record(result2)
            if result2.status == "success":
                _write_patch_to_blueprint(patch, Path(blueprint), patches_dir, failure_ctx, mode="auto")
                click.echo(
                    f"  ✓ LLM patch validated and applied → {blueprint}",
                    err=True,
                )
                result = result2
                failure_ctx = failure_ctx2
            else:
                click.echo("  ✗ LLM patch did not fix the issue, Blueprint unchanged", err=True)
                result = result2
                failure_ctx = failure_ctx2
            break  # auto: one patch attempt only

        elif effective_mode == "aggressive":
            # Dry-run: pre-validate patched Blueprint before writing to disk
            if manifest.agent.validate_patch:
                pre_manifest = _apply_patch_in_memory(
                    patch, Path(blueprint), depot, profile, cli_overrides or {}
                )
                if pre_manifest is None:
                    click.echo(
                        f"  ✗ LLM patch produces invalid Blueprint (validate_patch=true); "
                        f"staging for human review.",
                        err=True,
                    )
                    stage_patch_for_human(patch, patches_dir, failure_ctx)
                    click.echo(
                        f"  ✎ Patch staged → patches/pending/{patch.patch_id}.json",
                        err=True,
                    )
                    break

            # Write to Blueprint permanently, re-compile, continue loop
            new_manifest = _write_patch_to_blueprint(
                patch, Path(blueprint), patches_dir, failure_ctx, mode="aggressive"
            )
            if new_manifest is None:
                last_apply_error = f"Patch {patch.patch_id!r} produced an invalid Blueprint and was not applied."
                click.echo("  ✗ LLM patch failed to apply, stopping", err=True)
                break
            last_apply_error = None  # patch applied — reset for next iteration
            manifest = new_manifest
            click.echo(
                f"  ✓ LLM patch applied ({patch_count}/{max_patches}) → {blueprint}",
                err=True,
            )
            # Continue loop with updated manifest

    # ── Surveyor stop ─────────────────────────────────────────────────────────
    surveyor.stop()

    # ── Depot — persist run_id for @aq.runtime.prev_run_id() ─────────────────
    try:
        depot.put("_last_run_id", result.run_id)
    except Exception:
        pass
    depot.close()

    # ── Report ────────────────────────────────────────────────────────────────
    for mr in result.module_results:
        icon = "✓" if mr.status == "success" else "✗"
        line = f"  {icon} {mr.module_id}"
        if mr.error:
            line += f"  — {mr.error}"
        click.echo(line)

    if result.status != "success":
        if failure_ctx:
            click.echo(
                f"\n✗ blueprint failed  run_id={result.run_id}"
                f"  failed_module={failure_ctx.failed_module}",
                err=True,
            )
        else:
            click.echo(f"\n✗ blueprint failed  run_id={result.run_id}", err=True)
        sys.exit(1)

    # ── on_success webhook ────────────────────────────────────────────────────
    if cfg.webhooks.on_success:
        from aqueduct.surveyor.webhook import fire_webhook
        success_payload = {
            "run_id": result.run_id,
            "blueprint_id": manifest.blueprint_id,
            "blueprint_name": manifest.name,
            "module_count": str(len(result.module_results)),
        }
        fire_webhook(
            cfg.webhooks.on_success,
            full_payload=success_payload,
            template_vars=success_payload,
        )

    click.echo(f"\n✓ blueprint complete  run_id={result.run_id}")


# ── patch helpers ────────────────────────────────────────────────────────────

def _uncommitted_applied_patches(blueprint_path: Path, patches_root: Path) -> list[Path]:
    """Return applied patches with applied_at newer than the last git commit for blueprint_path.

    Falls back to returning all applied patches when not in a git repo or blueprint
    has never been committed.
    """
    import subprocess

    applied_dir = patches_root / "applied"
    if not applied_dir.exists():
        return []

    all_applied = sorted(applied_dir.glob("*.json"), key=lambda f: f.stat().st_mtime)
    if not all_applied:
        return []

    # Get ISO timestamp of last git commit touching this blueprint
    result = subprocess.run(
        ["git", "log", "-1", "--format=%cI", "--", str(blueprint_path)],
        capture_output=True, text=True,
    )
    last_commit_ts: str | None = result.stdout.strip() if result.returncode == 0 else None

    if not last_commit_ts:
        # Not in git or never committed — treat everything as uncommitted
        return all_applied

    uncommitted = []
    from datetime import datetime
    for p in all_applied:
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        # applied_at may be top-level or inside _aq_meta
        applied_at_str = data.get("applied_at") or (data.get("_aq_meta") or {}).get("applied_at")
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
    for _ in range(8):
        if (_search / "aqueduct.yml").exists():
            project_root = _search
            break
        if _search.parent == _search:
            break
        _search = _search.parent
    return project_root / "patches"


# ── patch command group ───────────────────────────────────────────────────────

@cli.group()
def patch() -> None:
    """Manage Blueprint patches."""


@patch.command("apply")
@click.argument("patch_file", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--blueprint",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML file to patch",
)
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: <blueprint-dir>/patches)",
)
def patch_apply(patch_file: str, blueprint: str, patches_dir: str | None) -> None:
    """Validate and apply a PatchSpec JSON file to a Blueprint YAML.

    Backs up the original Blueprint, applies all operations atomically,
    verifies the result parses cleanly, then archives the patch.
    """
    from pathlib import Path

    from aqueduct.patch.apply import PatchError, apply_patch_file

    blueprint_path = Path(blueprint)
    patches_root = Path(patches_dir) if patches_dir else _patches_root_from_blueprint(blueprint_path)

    try:
        result = apply_patch_file(
            blueprint_path=blueprint_path,
            patch_path=Path(patch_file),
            patches_dir=patches_root,
        )
    except PatchError as exc:
        click.echo(f"✗ patch failed: {exc}", err=True)
        sys.exit(1)

    click.echo(f"✓ patch applied  id={result.patch_id}")
    click.echo(f"  blueprint  → {result.blueprint_path}")
    click.echo(f"  archived   → {result.archive_path}")
    click.echo(f"  operations   {result.operations_applied} applied")
    click.echo(f"  commit with: aqueduct patch commit --blueprint {blueprint}")


@patch.command("reject")
@click.argument("patch_ref")
@click.option("--reason", required=True, help="Rejection reason (recorded in patch file)")
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: derived from patch file path or CWD/patches)",
)
def patch_reject(patch_ref: str, reason: str, patches_dir: str | None) -> None:
    """Reject a pending patch and record the reason.

    PATCH_REF can be a file path (patches/pending/00001_*.json) or a bare patch_id slug.

    Moves patches/pending/<file> → patches/rejected/<file> with a rejection_reason annotation.
    """
    from pathlib import Path

    from aqueduct.patch.apply import PatchError, reject_patch

    # Accept either a file path or a bare patch_id slug.
    ref_path = Path(patch_ref)
    if ref_path.suffix == ".json" and ref_path.exists():
        # Full path given — derive patches_dir from grandparent (pending/ → patches/)
        resolved_patches_dir = ref_path.parent.parent
        patch_id = ref_path.stem
    elif ref_path.suffix == ".json" and not ref_path.exists() and ref_path.parent.name == "pending":
        # Path given but file not found via CWD — try same derivation
        resolved_patches_dir = ref_path.parent.parent
        patch_id = ref_path.stem
    else:
        resolved_patches_dir = Path(patches_dir) if patches_dir else Path("patches")
        patch_id = patch_ref

    try:
        rejected_path = reject_patch(
            patch_id=patch_id,
            reason=reason,
            patches_dir=resolved_patches_dir,
        )
    except PatchError as exc:
        click.echo(f"✗ reject failed: {exc}", err=True)
        sys.exit(1)

    click.echo(f"✓ patch rejected  id={patch_id}")
    click.echo(f"  archived → {rejected_path}")
    click.echo(f"  reason: {reason}")


@patch.command("commit")
@click.option(
    "--blueprint",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML file to commit",
)
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: <blueprint-dir>/patches)",
)
def patch_commit(blueprint: str, patches_dir: str | None) -> None:
    """Commit applied patches to git with a structured commit message.

    Finds applied patches newer than the last git commit for this Blueprint,
    then runs: git add <blueprint> && git commit.
    """
    import subprocess
    from pathlib import Path

    blueprint_path = Path(blueprint)
    patches_root = Path(patches_dir) if patches_dir else _patches_root_from_blueprint(blueprint_path)

    uncommitted = _uncommitted_applied_patches(blueprint_path, patches_root)
    if not uncommitted:
        click.echo("Nothing to commit — no applied patches since last git commit.")
        return

    # Parse blueprint_id
    try:
        from aqueduct.parser.parser import parse as _parse
        bp = _parse(blueprint)
        blueprint_id = bp.id
    except Exception:
        blueprint_id = blueprint_path.stem

    # Build commit message
    patch_lines: list[str] = []
    all_ops: list[str] = []
    run_id: str | None = None
    rationales: list[str] = []

    for p in uncommitted:
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            data = {}
        rat = data.get("rationale", "")
        if rat:
            rationales.append(rat)
        ops = [op.get("op", "?") for op in data.get("operations", [])]
        all_ops.extend(ops)
        meta = data.get("_aq_meta", {})
        if not run_id:
            run_id = meta.get("run_id") or data.get("run_id")
        patch_lines.append(f"  - {p.stem}: {rat or '(no rationale)'}")

    n = len(uncommitted)
    summary = rationales[0] if n == 1 and rationales else f"{n} patches applied"
    combined_rationale = "\n".join(rationales) if rationales else ""
    ops_str = ", ".join(dict.fromkeys(all_ops))  # deduplicated, ordered

    aqueduct_block = "---aqueduct---\npatches:\n" + "\n".join(patch_lines)
    if run_id:
        aqueduct_block += f"\nrun_id: {run_id}"
    if ops_str:
        aqueduct_block += f"\nops: {ops_str}"
    aqueduct_block += "\n---"

    commit_msg = f"fix(aqueduct/{blueprint_id}): {summary}"
    if combined_rationale:
        commit_msg += f"\n\n{combined_rationale}"
    commit_msg += f"\n\n{aqueduct_block}"

    add = subprocess.run(["git", "add", blueprint_path.name], capture_output=True, cwd=blueprint_path.parent or None)
    if add.returncode != 0:
        click.echo(f"✗ git add failed: {add.stderr.decode().strip()}", err=True)
        sys.exit(1)

    commit = subprocess.run(
        ["git", "commit", "-m", commit_msg],
        capture_output=True, text=True, cwd=blueprint_path.parent or None,
    )
    if commit.returncode != 0:
        click.echo(f"✗ git commit failed: {commit.stderr.strip()}", err=True)
        sys.exit(1)

    short_hash = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        capture_output=True, text=True, cwd=blueprint_path.parent or None,
    ).stdout.strip()

    click.echo(f"✓ committed {n} patch(es)  [{short_hash}]  {blueprint_id}")
    for p in uncommitted:
        click.echo(f"  {p.name}")


@patch.command("discard")
@click.option(
    "--blueprint",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML file to restore from git HEAD",
)
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: <blueprint-dir>/patches)",
)
def patch_discard(blueprint: str, patches_dir: str | None) -> None:
    """Discard applied patches — restore Blueprint to last git commit.

    Runs: git checkout HEAD -- <blueprint>
    Moves uncommitted applied patches back to patches/pending/.
    """
    import subprocess
    from pathlib import Path

    blueprint_path = Path(blueprint)
    patches_root = Path(patches_dir) if patches_dir else _patches_root_from_blueprint(blueprint_path)

    uncommitted = _uncommitted_applied_patches(blueprint_path, patches_root)

    restore = subprocess.run(
        ["git", "checkout", "HEAD", "--", blueprint_path.name],
        capture_output=True, text=True, cwd=blueprint_path.parent or None,
    )
    if restore.returncode != 0:
        click.echo(f"✗ git checkout failed: {restore.stderr.strip()}", err=True)
        sys.exit(1)

    click.echo(f"✓ blueprint restored to HEAD: {blueprint_path}")

    pending_dir = patches_root / "pending"
    pending_dir.mkdir(parents=True, exist_ok=True)
    moved = 0
    for patch_file in uncommitted:
        dest = pending_dir / patch_file.name
        try:
            patch_file.rename(dest)
            moved += 1
        except OSError:
            pass

    if moved:
        click.echo(f"  moved {moved} applied patch(es) back to patches/pending/")
        click.echo(f"  re-apply with: aqueduct patch apply patches/pending/<file> --blueprint {blueprint}")


@patch.command("list")
@click.option(
    "--blueprint",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML file (used to locate patches/ dir)",
)
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: <blueprint-dir>/patches or CWD/patches)",
)
@click.option(
    "--status",
    "filter_status",
    default="pending",
    type=click.Choice(["pending", "applied", "rejected", "all"]),
    show_default=True,
    help="Which lifecycle directory to list",
)
def patch_list(blueprint: str | None, patches_dir: str | None, filter_status: str) -> None:
    """List patches, showing metadata for each.

    Defaults to showing pending patches. Use --status=applied/rejected/all for other dirs.
    """
    from pathlib import Path

    if patches_dir:
        patches_root = Path(patches_dir)
    elif blueprint:
        patches_root = _patches_root_from_blueprint(Path(blueprint))
    else:
        patches_root = _patches_root_from_blueprint(Path.cwd() / "_sentinel")

    dirs_to_show: list[tuple[str, Path]] = []
    if filter_status == "all":
        for sub in ("pending", "applied", "rejected"):
            d = patches_root / sub
            if d.exists():
                dirs_to_show.append((sub, d))
    else:
        d = patches_root / filter_status
        if d.exists():
            dirs_to_show.append((filter_status, d))

    total = 0
    for status_label, d in dirs_to_show:
        files = sorted(d.glob("*.json"), key=lambda f: f.name)
        if not files:
            continue

        click.echo(f"\n  [{status_label}]  {d}")
        click.echo(f"  {'file':<55} {'patch_id':<36} {'rationale'}")
        click.echo(f"  {'-'*55} {'-'*36} {'-'*40}")

        for f in files:
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
            except Exception:
                data = {}
            pid = data.get("patch_id", f.stem)
            rationale = (data.get("rationale") or "").replace("\n", " ")[:60]
            click.echo(f"  {f.name:<55} {pid:<36} {rationale}")
            total += 1

    if total == 0:
        click.echo(f"No {filter_status} patches found in {patches_root}")
        return

    if filter_status == "pending":
        click.echo(f"\n  Apply: aqueduct patch apply patches/pending/<file> --blueprint <blueprint.yml>")
        click.echo(f"  Reject: aqueduct patch reject patches/pending/<file> --reason '<reason>'")


# ── aqueduct test ────────────────────────────────────────────────────────────

@cli.command("test")
@click.argument("test_file", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--blueprint",
    "blueprint_path",
    default=None,
    type=click.Path(dir_okay=False),
    help="Override the blueprint path declared in the test file",
)
@click.option(
    "--config",
    "config_path",
    default=None,
    help="Path to aqueduct.yml",
)
@click.option(
    "--quiet",
    is_flag=True,
    default=False,
    help="Suppress Spark progress output",
)
def test_cmd(
    test_file: str,
    blueprint_path: str | None,
    config_path: str | None,
    quiet: bool,
) -> None:
    """Run isolated module tests from a test YAML file.

    Tests execute Channel, Junction, Funnel, and Assert modules against
    inline data — no Ingress or Egress, no external sources.

    \b
    Example test file (blueprint.aqtest.yml):

      aqueduct_test: "1.0"
      blueprint: blueprint.yml

      tests:
        - id: test_filter_nulls
          module: clean_orders
          inputs:
            raw_orders:
              schema: {order_id: long, amount: double}
              rows:
                - [1, 10.0]
                - [2, null]
          assertions:
            - type: row_count
              expected: 1
            - type: sql
              expr: "SELECT count(*) = 1 FROM __output__"
    """
    from pathlib import Path

    from aqueduct.config import ConfigError, load_config
    from aqueduct.executor.spark.session import make_spark_session
    from aqueduct.executor.spark.test_runner import TestError, run_test_file

    try:
        cfg = load_config(Path(config_path) if config_path else None)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(1)

    merged_spark_config = dict(cfg.spark_config)
    master_url = cfg.deployment.master_url

    spark = make_spark_session(
        "aqueduct_test",
        merged_spark_config,
        master_url=master_url,
        quiet=quiet,
    )

    try:
        suite = run_test_file(
            test_file=Path(test_file),
            spark=spark,
            blueprint_path_override=Path(blueprint_path) if blueprint_path else None,
        )
    except TestError as exc:
        click.echo(f"✗ test file error: {exc}", err=True)
        sys.exit(1)
    finally:
        spark.stop()

    # ── Print results ─────────────────────────────────────────────────────────
    click.echo(f"\nTest suite: {test_file}")
    click.echo(f"  {suite.total} tests  |  {suite.passed} passed  |  {suite.failed} failed\n")

    for result in suite.results:
        icon = "✓" if result.passed else "✗"
        click.echo(f"  {icon} {result.test_id}")
        if result.error:
            click.echo(f"      error: {result.error}")
        for ar in result.assertion_results:
            a_icon = "  ✓" if ar.passed else "  ✗"
            click.echo(f"      {a_icon} [{ar.assertion_type}] {ar.message}")

    click.echo()
    if suite.failed > 0:
        click.echo(f"✗ {suite.failed} test(s) failed", err=True)
        sys.exit(1)
    else:
        click.echo(f"✓ all {suite.passed} test(s) passed")


# ── aqueduct report ───────────────────────────────────────────────────────────

@cli.command()
@click.argument("run_id")
@click.option(
    "--store-dir",
    default=None,
    help="Store directory (default: aqueduct.yml or .aqueduct)",
)
@click.option(
    "--config",
    "config_path",
    default=None,
    help="Path to aqueduct.yml",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["table", "json", "csv"]),
    default="table",
    show_default=True,
)
def report(run_id: str, store_dir: str | None, config_path: str | None, fmt: str) -> None:
    """Print the Flow Report for a completed run."""
    import csv as _csv
    import io

    import duckdb as _duckdb

    from aqueduct.config import ConfigError, load_config

    try:
        cfg = load_config(Path(config_path) if config_path else None)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(1)

    resolved = Path(store_dir) if store_dir else Path(cfg.stores.obs.path).parent
    obs_db = resolved / "obs.db"
    if not obs_db.exists():
        click.echo(f"✗ obs.db not found at {obs_db}", err=True)
        sys.exit(1)

    conn = _duckdb.connect(str(obs_db), read_only=True)
    try:
        row = conn.execute(
            """
            SELECT run_id, blueprint_id, status,
                   CAST(started_at AS VARCHAR),
                   CAST(finished_at AS VARCHAR),
                   module_results
            FROM run_records WHERE run_id = ?
            """,
            [run_id],
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        click.echo(f"✗ run {run_id!r} not found in {obs_db}", err=True)
        sys.exit(1)

    run_id_val, blueprint_id, status, started_at, finished_at, module_results_raw = row
    module_results = json.loads(module_results_raw) if isinstance(module_results_raw, str) else (module_results_raw or [])

    if fmt == "json":
        out = {
            "run_id": run_id_val,
            "blueprint_id": blueprint_id,
            "status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "module_results": module_results,
        }
        click.echo(json.dumps(out, indent=2))
        return

    if fmt == "csv":
        buf = io.StringIO()
        writer = _csv.writer(buf)
        writer.writerow(["run_id", "blueprint_id", "status", "started_at", "finished_at"])
        writer.writerow([run_id_val, blueprint_id, status, started_at, finished_at])
        click.echo(buf.getvalue(), nl=False)
        buf2 = io.StringIO()
        writer2 = _csv.writer(buf2)
        writer2.writerow(["module_id", "status", "error"])
        for mr in module_results:
            writer2.writerow([mr.get("module_id", ""), mr.get("status", ""), mr.get("error", "")])
        click.echo(buf2.getvalue(), nl=False)
        return

    # table format
    status_icon = "✓" if status == "success" else "✗"
    click.echo(f"{status_icon} run_id={run_id_val}  blueprint={blueprint_id}  status={status}")
    click.echo(f"  started:  {started_at}")
    click.echo(f"  finished: {finished_at or '(running)'}")
    click.echo("")
    click.echo(f"  {'Module':<30} {'Status':<10} Error")
    click.echo(f"  {'-'*30} {'-'*10} {'-'*40}")
    for mr in module_results:
        icon = "✓" if mr.get("status") == "success" else ("⏭" if mr.get("status") == "skipped" else "✗")
        err = mr.get("error") or ""
        if len(err) > 60:
            err = err[:57] + "..."
        click.echo(f"  {icon} {mr.get('module_id', ''):<28} {mr.get('status', ''):<10} {err}")


# ── aqueduct runs ─────────────────────────────────────────────────────────────

@cli.command("runs")
@click.option("--blueprint", default=None, metavar="PATH_OR_ID", help="Filter by blueprint file path or blueprint ID")
@click.option("--failed", is_flag=True, default=False, help="Show only failed runs")
@click.option("--last", "limit", default=20, show_default=True, help="Max rows to show")
@click.option("--store-dir", default=None, help="Store directory (default: .aqueduct)")
@click.option("--config", "config_path", default=None, metavar="PATH", help="Path to aqueduct.yml")
def runs(blueprint: str | None, failed: bool, limit: int, store_dir: str | None, config_path: str | None) -> None:
    """List recent blueprint runs."""
    import duckdb as _duckdb
    from aqueduct.config import load_config

    cfg = load_config(Path(config_path) if config_path else None)
    resolved = Path(store_dir) if store_dir else Path(cfg.stores.obs.path).parent
    obs_db = resolved / "obs.db"
    if not obs_db.exists():
        click.echo(f"No runs found (obs.db not at {obs_db})")
        return

    blueprint_id: str | None = None
    if blueprint:
        arg_path = Path(blueprint)
        if arg_path.suffix in (".yml", ".yaml") and arg_path.exists():
            from aqueduct.parser.parser import ParseError, parse
            try:
                bp = parse(str(arg_path))
                blueprint_id = bp.id
            except ParseError:
                blueprint_id = blueprint
        else:
            blueprint_id = blueprint

    conn = _duckdb.connect(str(obs_db), read_only=True)
    try:
        where_parts = []
        params: list = []
        if blueprint_id:
            where_parts.append("blueprint_id = ?")
            params.append(blueprint_id)
        if failed:
            where_parts.append("status = 'error'")
        where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
        params.append(limit)

        rows = conn.execute(
            f"""
            SELECT run_id, blueprint_id, status, started_at, finished_at,
                   json_extract_string(module_results, '$[0].module_id') AS first_failed
            FROM run_records
            {where}
            ORDER BY started_at DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        click.echo("No runs found.")
        return

    click.echo(f"  {'run_id':<38} {'blueprint':<30} {'status':<10} {'started':<22} {'failed_module'}")
    click.echo(f"  {'-'*38} {'-'*30} {'-'*10} {'-'*22} {'-'*20}")
    for run_id_val, bp_id, status, started_at, finished_at, first_failed in rows:
        icon = "✓" if status == "success" else ("↻" if status == "running" else "✗")
        failed_col = (first_failed or "") if status == "error" else ""
        click.echo(f"  {icon} {run_id_val:<37} {bp_id:<30} {status:<10} {str(started_at)[:19]:<22} {failed_col}")


# ── aqueduct lineage ──────────────────────────────────────────────────────────

@cli.command()
@click.argument("blueprint_id_or_blueprint")
@click.option(
    "--store-dir",
    default=None,
    help="Observability store directory",
)
@click.option(
    "--config",
    "config_path",
    default=None,
    help="Path to aqueduct.yml",
)
@click.option(
    "--from",
    "from_table",
    default=None,
    help="Filter: only show lineage originating from this source table",
)
@click.option(
    "--column",
    "column_filter",
    default=None,
    help="Filter: only show lineage for this output column name",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["table", "json"]),
    default="table",
    show_default=True,
)
def lineage(
    blueprint_id_or_blueprint: str,
    store_dir: str | None,
    config_path: str | None,
    from_table: str | None,
    column_filter: str | None,
    fmt: str,
) -> None:
    """Print column-level lineage graph for a blueprint.

    PIPELINE_ID_OR_BLUEPRINT: blueprint id (e.g. nyc_taxi_demo) or path to
    the blueprint YAML file (e.g. blueprint.yml — id is extracted automatically).
    """
    import duckdb as _duckdb

    from aqueduct.config import ConfigError, load_config

    try:
        cfg = load_config(Path(config_path) if config_path else None)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(1)

    # Accept blueprint file path — extract blueprint id from it
    arg_path = Path(blueprint_id_or_blueprint)
    if arg_path.suffix in (".yml", ".yaml") and arg_path.exists():
        try:
            from aqueduct.parser.parser import parse
            bp = parse(str(arg_path))
            blueprint_id = bp.id
        except Exception as exc:
            click.echo(f"✗ could not read blueprint id from {blueprint_id_or_blueprint!r}: {exc}", err=True)
            sys.exit(1)
    else:
        blueprint_id = blueprint_id_or_blueprint

    resolved = Path(store_dir) if store_dir else Path(cfg.stores.obs.path).parent
    lineage_db = resolved / "lineage.db"
    if not lineage_db.exists():
        click.echo(f"✗ lineage.db not found at {lineage_db}", err=True)
        sys.exit(1)

    params: list[Any] = [blueprint_id]
    where_parts = ["blueprint_id = ?"]
    if from_table:
        where_parts.append("source_table = ?")
        params.append(from_table)
    if column_filter:
        where_parts.append("output_column = ?")
        params.append(column_filter)

    where = " AND ".join(where_parts)
    query = f"""
        SELECT DISTINCT channel_id, output_column, source_table, source_column
        FROM column_lineage
        WHERE {where}
        ORDER BY channel_id, output_column
    """

    conn = _duckdb.connect(str(lineage_db), read_only=True)
    try:
        rows = conn.execute(query, params).fetchall()
    finally:
        conn.close()

    if not rows:
        click.echo(f"No lineage records found for blueprint {blueprint_id!r}.")
        return

    if fmt == "json":
        out = [
            {
                "channel_id": r[0],
                "output_column": r[1],
                "source_table": r[2],
                "source_column": r[3],
            }
            for r in rows
        ]
        click.echo(json.dumps(out, indent=2))
        return

    click.echo(f"Column lineage — blueprint: {blueprint_id}")
    click.echo(f"  {'Channel':<25} {'Output Column':<25} {'Source Table':<25} Source Column")
    click.echo(f"  {'-'*25} {'-'*25} {'-'*25} {'-'*25}")
    for channel_id, output_column, source_table, source_column in rows:
        click.echo(f"  {channel_id:<25} {output_column:<25} {source_table:<25} {source_column or ''}")


# ── aqueduct signal ───────────────────────────────────────────────────────────

@cli.command()
@click.argument("signal_id")
@click.option(
    "--value",
    "value_str",
    type=click.Choice(["true", "false"]),
    default=None,
    help="Set override: 'false' closes gate (blocks all future runs), 'true' clears override",
)
@click.option(
    "--error",
    "error_msg",
    default=None,
    help="Attach a reason message (implies --value false)",
)
@click.option(
    "--store-dir",
    default=None,
    help="Observability store directory",
)
@click.option(
    "--config",
    "config_path",
    default=None,
    help="Path to aqueduct.yml",
)
def signal(
    signal_id: str,
    value_str: str | None,
    error_msg: str | None,
    store_dir: str | None,
    config_path: str | None,
) -> None:
    """Set or clear a persistent gate override for a Probe signal.

    \b
    Close gate (block all future runs):
      aqueduct signal my_probe --value false
      aqueduct signal my_probe --error "Source data is stale"

    Clear override (resume normal evaluation):
      aqueduct signal my_probe --value true
    """
    from datetime import datetime, timezone

    import duckdb as _duckdb

    from aqueduct.config import ConfigError, load_config
    from aqueduct.surveyor.surveyor import _SIGNAL_OVERRIDES_DDL

    if value_str is None and error_msg is None:
        # Show current override status
        try:
            cfg = load_config(Path(config_path) if config_path else None)
        except ConfigError as exc:
            click.echo(f"✗ config error: {exc}", err=True)
            sys.exit(1)
        resolved = Path(store_dir) if store_dir else Path(cfg.stores.obs.path).parent
        obs_db = resolved / "obs.db"
        conn = _duckdb.connect(str(obs_db))
        try:
            conn.execute(_SIGNAL_OVERRIDES_DDL)
            row = conn.execute(
                "SELECT passed, error_message, CAST(set_at AS VARCHAR) FROM signal_overrides WHERE signal_id = ?",
                [signal_id],
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            click.echo(f"  {signal_id}: no persistent override (evaluates from Probe data)")
        else:
            state = "open" if row[0] else "closed (blocked)"
            click.echo(f"  {signal_id}: gate={state}  set_at={row[2]}")
            if row[1]:
                click.echo(f"  reason: {row[1]}")
        return

    if error_msg is not None and value_str == "true":
        click.echo("✗ --error implies gate closed; cannot combine with --value true", err=True)
        sys.exit(1)

    # Resolve passed value
    if value_str == "true":
        passed = True
    else:
        passed = False  # explicit false OR error_msg provided

    try:
        cfg = load_config(Path(config_path) if config_path else None)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(1)

    resolved = Path(store_dir) if store_dir else Path(cfg.stores.obs.path).parent
    resolved.mkdir(parents=True, exist_ok=True)
    obs_db = resolved / "obs.db"

    now = datetime.now(tz=timezone.utc).isoformat()
    conn = _duckdb.connect(str(obs_db))
    try:
        conn.execute(_SIGNAL_OVERRIDES_DDL)
        if passed:
            # Clear override — delete row entirely
            conn.execute("DELETE FROM signal_overrides WHERE signal_id = ?", [signal_id])
            click.echo(f"✓ signal {signal_id!r}: override cleared — gate resumes normal Probe evaluation")
        else:
            conn.execute(
                """
                INSERT INTO signal_overrides (signal_id, passed, error_message, set_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (signal_id) DO UPDATE SET
                    passed = excluded.passed,
                    error_message = excluded.error_message,
                    set_at = excluded.set_at
                """,
                [signal_id, False, error_msg, now],
            )
            msg_note = f"  reason: {error_msg}" if error_msg else ""
            click.echo(f"✓ signal {signal_id!r}: gate CLOSED — all future runs blocked at this Regulator")
            if msg_note:
                click.echo(msg_note)
    finally:
        conn.close()


# ── aqueduct heal ─────────────────────────────────────────────────────────────

@cli.command()
@click.argument("run_id")
@click.option(
    "--module",
    "module_id",
    default=None,
    help="Scope healing to a specific module (default: use failed_module from run record)",
)
@click.option(
    "--store-dir",
    default=None,
    help="Observability store directory",
)
@click.option(
    "--config",
    "config_path",
    default=None,
    help="Path to aqueduct.yml",
)
@click.option(
    "--patches-dir",
    default="patches",
    show_default=True,
    help="Root directory for patch lifecycle subdirs",
)
def heal(
    run_id: str,
    module_id: str | None,
    store_dir: str | None,
    config_path: str | None,
    patches_dir: str,
) -> None:
    """Manually trigger LLM self-healing for a failed run.

    Reads the FailureContext from the observability store, calls the LLM
    agent, and stages the resulting patch for human review.
    """
    import duckdb as _duckdb

    from aqueduct.config import ConfigError, load_config
    from aqueduct.surveyor.llm import generate_llm_patch, stage_patch_for_human
    from aqueduct.surveyor.models import FailureContext

    try:
        cfg = load_config(Path(config_path) if config_path else None)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(1)

    resolved = Path(store_dir) if store_dir else Path(cfg.stores.obs.path).parent
    obs_db = resolved / "obs.db"
    if not obs_db.exists():
        click.echo(f"✗ obs.db not found at {obs_db}", err=True)
        sys.exit(1)

    conn = _duckdb.connect(str(obs_db), read_only=True)
    try:
        fc_row = conn.execute(
            """
            SELECT run_id, blueprint_id, failed_module, error_message,
                   stack_trace, manifest_json,
                   CAST(started_at AS VARCHAR), CAST(finished_at AS VARCHAR)
            FROM failure_contexts WHERE run_id = ?
            """,
            [run_id],
        ).fetchone()
    finally:
        conn.close()

    if fc_row is None:
        click.echo(
            f"✗ no failure record for run {run_id!r}\n"
            "  (Only failed runs have a FailureContext stored.)",
            err=True,
        )
        sys.exit(1)

    (
        fc_run_id, blueprint_id, failed_module, error_message,
        stack_trace, manifest_json_raw, started_at, finished_at,
    ) = fc_row

    # Allow --module to override the recorded failed_module for scoped healing
    target_module = module_id or failed_module

    failure_ctx = FailureContext(
        run_id=fc_run_id,
        blueprint_id=blueprint_id,
        failed_module=target_module,
        error_message=error_message,
        stack_trace=stack_trace,
        manifest_json=manifest_json_raw if isinstance(manifest_json_raw, str) else json.dumps(manifest_json_raw),
        started_at=started_at,
        finished_at=finished_at,
    )

    # Resolve agent config (same priority as `aqueduct run`)
    eng = cfg.agent
    resolved_provider = eng.provider
    resolved_base_url = eng.base_url
    resolved_model = eng.model
    resolved_ollama_options = eng.ollama_options
    resolved_llm_timeout = eng.llm_timeout
    resolved_llm_max_reprompts = eng.llm_max_reprompts
    resolved_engine_prompt_context = eng.prompt_context

    if resolved_model is None:
        click.echo(
            "✗ no LLM agent configured — set agent.model in aqueduct.yml",
            err=True,
        )
        sys.exit(1)

    click.echo(
        f"↻ heal  run={run_id}  module={target_module}  "
        f"provider={resolved_provider}  model={resolved_model}"
    )

    patches_path = Path(patches_dir)
    patch = generate_llm_patch(
        failure_ctx,
        model=resolved_model,
        patches_dir=patches_path,
        provider=resolved_provider or "anthropic",
        base_url=resolved_base_url,
        ollama_options=resolved_ollama_options,
        llm_timeout=resolved_llm_timeout,
        llm_max_reprompts=resolved_llm_max_reprompts,
        engine_prompt_context=resolved_engine_prompt_context,
    )

    if patch is None:
        click.echo("✗ LLM failed to produce a valid patch", err=True)
        sys.exit(1)

    stage_patch_for_human(patch, patches_path, failure_ctx)
    click.echo(f"✓ patch staged → {patches_path}/pending/{patch.patch_id}.json")
    click.echo(f"  apply with: aqueduct patch apply patches/pending/{patch.patch_id}.json --blueprint <path>")


# ── aqueduct log ─────────────────────────────────────────────────────────────

@cli.command("log")
@click.argument("blueprint", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["table", "json"]),
    default="table",
    show_default=True,
)
def log_cmd(blueprint: str, fmt: str) -> None:
    """Show git commit history for a Blueprint with Aqueduct patch metadata.

    Parses ---aqueduct--- blocks from commit messages.  Manual commits (no
    block) are shown as '(manual change)'.
    """
    import re
    import subprocess

    blueprint_path = Path(blueprint)

    result = subprocess.run(
        ["git", "log", "--follow", "--format=%H\x1f%ci\x1f%s\x1f%B\x1eENDCOMMIT", "--", str(blueprint_path)],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        click.echo(f"✗ git log failed: {result.stderr.strip()}", err=True)
        sys.exit(1)

    raw = result.stdout.strip()
    if not raw:
        click.echo("No git history for this blueprint.")
        return

    _AQ_BLOCK_RE = re.compile(r"---aqueduct---(.*?)---", re.DOTALL)
    _PATCH_LINE_RE = re.compile(r"^\s*-\s+(\S+):\s*(.*)", re.MULTILINE)

    entries = []
    for commit_raw in raw.split("\x1eENDCOMMIT"):
        commit_raw = commit_raw.strip()
        if not commit_raw:
            continue
        # First line is the \x1f-separated header
        header_line, _, body = commit_raw.partition("\n")
        parts = header_line.split("\x1f")
        if len(parts) < 3:
            continue
        commit_hash = parts[0].strip()
        commit_date = parts[1].strip()
        subject = parts[2].strip()

        aq_match = _AQ_BLOCK_RE.search(body)
        if aq_match:
            block = aq_match.group(1)
            patch_ids = [m.group(1) for m in _PATCH_LINE_RE.finditer(block)]
            ops_match = re.search(r"^ops:\s*(.+)", block, re.MULTILINE)
            ops = ops_match.group(1).strip() if ops_match else ""
            run_match = re.search(r"^run_id:\s*(\S+)", block, re.MULTILINE)
            run_id = run_match.group(1) if run_match else ""
        else:
            patch_ids = []
            ops = ""
            run_id = ""

        entries.append({
            "hash": commit_hash[:8],
            "date": commit_date[:19],
            "subject": subject,
            "patches": ", ".join(patch_ids) if patch_ids else "(manual change)",
            "ops": ops,
            "run_id": run_id,
        })

    if fmt == "json":
        click.echo(json.dumps(entries, indent=2))
        return

    if not entries:
        click.echo("No commits found.")
        return

    click.echo(f"  {'hash':<10} {'date':<20} {'patches':<40} {'ops'}")
    click.echo(f"  {'-'*10} {'-'*20} {'-'*40} {'-'*30}")
    for e in entries:
        patches_col = e["patches"][:38] + ".." if len(e["patches"]) > 40 else e["patches"]
        click.echo(f"  {e['hash']:<10} {e['date']:<20} {patches_col:<40} {e['ops']}")


# ── aqueduct rollback ─────────────────────────────────────────────────────────

@cli.command("rollback")
@click.argument("blueprint", type=click.Path(exists=True, dir_okay=False))
@click.option("--to", "patch_id", required=True, help="Revert the git commit containing this patch_id")
@click.option(
    "--hard",
    is_flag=True,
    default=False,
    help="Destructive: git reset --hard to the commit BEFORE the patch (requires confirmation)",
)
def rollback_cmd(blueprint: str, patch_id: str, hard: bool) -> None:
    """Revert a Blueprint to before a specific patch was applied.

    Safe mode (default): runs git revert <commit> — adds a new revert commit.
    Hard mode (--hard):  runs git reset --hard <commit~1> — rewrites history,
    requires explicit confirmation.
    """
    import re
    import subprocess

    blueprint_path = Path(blueprint)

    # Walk git log to find the commit containing patch_id in ---aqueduct--- block
    result = subprocess.run(
        ["git", "log", "--follow", "--format=%H\x1f%B\x1eENDCOMMIT", "--", str(blueprint_path)],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        click.echo(f"✗ git log failed: {result.stderr.strip()}", err=True)
        sys.exit(1)

    target_hash: str | None = None
    for commit_raw in result.stdout.split("\x1eENDCOMMIT"):
        commit_raw = commit_raw.strip()
        if not commit_raw:
            continue
        header_line, _, body = commit_raw.partition("\n")
        commit_hash = header_line.split("\x1f")[0].strip()
        if patch_id in body:
            target_hash = commit_hash
            break

    if not target_hash:
        click.echo(
            f"✗ patch_id {patch_id!r} not found in git history for {blueprint}\n"
            "  Use 'aqueduct log <blueprint>' to list available patch_ids.",
            err=True,
        )
        sys.exit(1)

    if hard:
        click.echo(
            f"WARNING: --hard will reset HEAD to the commit before {target_hash[:8]}.\n"
            f"This rewrites git history and cannot be undone without a reflog.\n"
            f"Blueprint: {blueprint}\n"
        )
        confirm = click.prompt("Type 'yes' to confirm", default="no")
        if confirm.lower() != "yes":
            click.echo("Aborted.")
            return

        parent = subprocess.run(
            ["git", "rev-parse", f"{target_hash}~1"],
            capture_output=True, text=True,
        )
        if parent.returncode != 0:
            click.echo(f"✗ could not resolve parent commit: {parent.stderr.strip()}", err=True)
            sys.exit(1)
        parent_hash = parent.stdout.strip()

        reset = subprocess.run(
            ["git", "reset", "--hard", parent_hash],
            capture_output=True, text=True,
        )
        if reset.returncode != 0:
            click.echo(f"✗ git reset --hard failed: {reset.stderr.strip()}", err=True)
            sys.exit(1)
        click.echo(f"✓ hard reset to {parent_hash[:8]}  (before patch {patch_id!r})")
    else:
        revert = subprocess.run(
            ["git", "revert", "--no-edit", target_hash],
            capture_output=True, text=True,
        )
        if revert.returncode != 0:
            click.echo(f"✗ git revert failed: {revert.stderr.strip()}", err=True)
            sys.exit(1)
        short = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True,
        ).stdout.strip()
        click.echo(f"✓ reverted patch {patch_id!r}  [{short}]")
        click.echo(f"  Blueprint restored to state before commit {target_hash[:8]}")


# ── aqueduct init ─────────────────────────────────────────────────────────────

_GITIGNORE = """\
# Aqueduct runtime stores
.aqueduct/

# Applied patches (tracked by git commit message; backups unnecessary)
patches/applied/
patches/backups/

# Python
__pycache__/
*.pyc
*.pyo
*.egg-info/
dist/
.env
"""

_AQUEDUCT_YML = """\
aqueduct_config: "1.0"

deployment:
  engine: spark
  target: local
  master_url: "local[*]"

stores:
  obs:
    path: ".aqueduct/obs.db"
  lineage:
    path: ".aqueduct/lineage.db"
  depot:
    path: ".aqueduct/depot.db"

agent:
  provider: "anthropic"
  model: "claude-sonnet-4-6"
  # ANTHROPIC_API_KEY env var required for LLM self-healing
"""

_EXAMPLE_BLUEPRINT = """\
id: {blueprint_id}
name: {name}

context:
  input_path: "data/input"
  output_path: "data/output"

modules:
  - id: ingest
    type: Ingress
    config:
      format: parquet
      path: "${{ctx.input_path}}"

  - id: transform
    type: Channel
    config:
      op: sql
      query: "SELECT * FROM ingest"

  - id: output
    type: Egress
    config:
      format: parquet
      path: "${{ctx.output_path}}"
      mode: overwrite

edges:
  - from: ingest
    to: transform
  - from: transform
    to: output
"""


@cli.command("init")
@click.option("--name", default=None, help="Project name (defaults to current directory name)")
def init(name: str | None) -> None:
    """Scaffold a new Aqueduct project in the current directory."""
    import subprocess

    cwd = Path.cwd()
    project_name = name or cwd.name
    blueprint_id = project_name.lower().replace(" ", ".").replace("-", ".").replace("_", ".")

    created: list[str] = []
    skipped: list[str] = []

    def _write(path: Path, content: str) -> None:
        if path.exists():
            skipped.append(str(path.relative_to(cwd)))
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
            created.append(str(path.relative_to(cwd)))

    def _mkdir(path: Path) -> None:
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            (path / ".gitkeep").write_text("", encoding="utf-8")
            created.append(str(path.relative_to(cwd)) + "/")

    # Directories
    _mkdir(cwd / "arcades")
    _mkdir(cwd / "tests")
    _mkdir(cwd / "patches" / "pending")
    _mkdir(cwd / "patches" / "rejected")

    # Files
    _write(
        cwd / "blueprints" / "example.yml",
        _EXAMPLE_BLUEPRINT.format(blueprint_id=blueprint_id, name=project_name),
    )
    _write(cwd / "aqueduct.yml", _AQUEDUCT_YML)
    _write(cwd / ".gitignore", _GITIGNORE)

    for f in created:
        click.echo(f"  create  {f}")
    for f in skipped:
        click.echo(f"  skip    {f}  (already exists)")

    # Git
    in_git = subprocess.run(
        ["git", "rev-parse", "--git-dir"],
        capture_output=True, cwd=cwd,
    ).returncode == 0

    if not in_git:
        r = subprocess.run(["git", "init"], capture_output=True, text=True, cwd=cwd)
        if r.returncode == 0:
            click.echo("  git init")
        else:
            click.echo(f"  ⚠ git init failed: {r.stderr.strip()}")

    # Initial commit
    add = subprocess.run(["git", "add", "."], capture_output=True, cwd=cwd)
    if add.returncode == 0:
        commit = subprocess.run(
            ["git", "commit", "-m", f"init: aqueduct project ({project_name})"],
            capture_output=True, text=True, cwd=cwd,
        )
        if commit.returncode == 0:
            click.echo("  git commit  init: aqueduct project")
        elif "nothing to commit" in commit.stdout + commit.stderr:
            pass  # already clean
        else:
            click.echo(f"  ⚠ git commit failed: {commit.stderr.strip()}")

    click.echo(f"\n✓ {project_name} ready")
    click.echo(f"\nNext steps:")
    click.echo(f"  1. Edit blueprints/example.yml to define your pipeline")
    click.echo(f"  2. aqueduct validate blueprints/example.yml")
    click.echo(f"  3. aqueduct run blueprints/example.yml")


if __name__ == "__main__":
    cli()