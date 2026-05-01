"""Aqueduct CLI.

Active commands: validate, compile, run, check-config, doctor,
                 report, lineage, signal, heal,
                 patch apply, patch reject, patch rollback.
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

        if agent.forbidden_ops and op_name in agent.forbidden_ops:
            return f"Operation {op_name!r} blocked by agent.forbidden_ops"

        if agent.allowed_paths:
            config = (op_dict.get("config") or {}) if isinstance(op_dict, dict) else {}
            path_val = config.get("path") if isinstance(config, dict) else None
            if path_val and not any(
                fnmatch.fnmatch(str(path_val), pat) for pat in agent.allowed_paths
            ):
                return f"Path {path_val!r} not in agent.allowed_paths whitelist"

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
    """Aqueduct — Intelligent Spark Pipeline Engine."""


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
    """Validate aqueduct.yml without running a pipeline. Exit 0 = valid, 1 = invalid."""
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
    click.echo(f"  stores:  observability={cfg.stores.observability.path}  depot={cfg.stores.depot.path}")
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
def doctor(config_path: str | None, skip_spark: bool) -> None:
    """Probe all configured resources: config, stores, Spark, webhook, secrets, storage.

    Each check is independent. Spark check requires pyspark and may take 10-15s
    for JVM startup. Use --skip-spark to skip it in fast CI contexts.

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
    help="Observability store directory (overrides aqueduct.yml; default: .aqueduct/signals)",
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

    # ── Load engine config ─────────────────────────────────────────────────────
    try:
        cfg = load_config(Path(config_path) if config_path else None)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(1)

    # CLI flags override config file; config file overrides built-in defaults
    resolved_store_dir = Path(store_dir) if store_dir else Path(cfg.stores.observability.path)
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
    patches_dir = Path(blueprint).parent / "patches"
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
        f"▶ {manifest.pipeline_id}  ({len(manifest.modules)} modules)"
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
        session = make_spark_session(manifest.pipeline_id, merged_spark_config, master_url=master_url)
    else:
        raise NotImplementedError(f"Session creation for engine {engine!r} not implemented")

    import atexit
    atexit.register(session.stop)

    # ── Self-healing run loop ─────────────────────────────────────────────────
    patch_count = 0
    failure_ctx = None
    result = None

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
                pipeline_id=manifest.pipeline_id,
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
        patch = generate_llm_patch(
            failure_ctx,
            model=resolved_agent_model,
            patches_dir=patches_dir,
            provider=resolved_agent_provider,
            base_url=resolved_agent_base_url,
            ollama_options=resolved_agent_ollama_options,
        )
        if patch is None:
            click.echo("  ✗ LLM: failed to generate valid patch, stopping", err=True)
            break

        # ── Guardrail check ───────────────────────────────────────────────────
        guardrail_err = _check_guardrails(patch, manifest.agent)
        if guardrail_err:
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
            click.echo(
                f"  ✎ LLM patch staged → patches/pending/{patch.patch_id}.json\n"
                f"    Review: aqueduct patch apply patches/pending/{patch.patch_id}.json "
                f"--blueprint {blueprint}",
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
                    pipeline_id=manifest.pipeline_id,
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
                click.echo("  ✗ LLM patch failed to apply, stopping", err=True)
                break
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
                f"\n✗ pipeline failed  run_id={result.run_id}"
                f"  failed_module={failure_ctx.failed_module}",
                err=True,
            )
        else:
            click.echo(f"\n✗ pipeline failed  run_id={result.run_id}", err=True)
        sys.exit(1)

    # ── on_success webhook ────────────────────────────────────────────────────
    if cfg.webhooks.on_success:
        from aqueduct.surveyor.webhook import fire_webhook
        success_payload = {
            "run_id": result.run_id,
            "pipeline_id": manifest.pipeline_id,
            "pipeline_name": manifest.pipeline_name,
            "module_count": str(len(result.module_results)),
        }
        fire_webhook(
            cfg.webhooks.on_success,
            full_payload=success_payload,
            template_vars=success_payload,
        )

    click.echo(f"\n✓ pipeline complete  run_id={result.run_id}")


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
    default="patches",
    show_default=True,
    help="Root directory for patch lifecycle subdirs (backups/, applied/)",
)
def patch_apply(patch_file: str, blueprint: str, patches_dir: str) -> None:
    """Validate and apply a PatchSpec JSON file to a Blueprint YAML.

    Backs up the original Blueprint, applies all operations atomically,
    verifies the result parses cleanly, then archives the patch.
    """
    from pathlib import Path

    from aqueduct.patch.apply import PatchError, apply_patch_file

    try:
        result = apply_patch_file(
            blueprint_path=Path(blueprint),
            patch_path=Path(patch_file),
            patches_dir=Path(patches_dir),
        )
    except PatchError as exc:
        click.echo(f"✗ patch failed: {exc}", err=True)
        sys.exit(1)

    click.echo(f"✓ patch applied  id={result.patch_id}")
    click.echo(f"  blueprint  → {result.blueprint_path}")
    click.echo(f"  backup     → {result.backup_path}")
    click.echo(f"  archived   → {result.archive_path}")
    click.echo(f"  operations   {result.operations_applied} applied")


@patch.command("reject")
@click.argument("patch_id")
@click.option("--reason", required=True, help="Rejection reason (recorded in patch file)")
@click.option(
    "--patches-dir",
    default="patches",
    show_default=True,
    help="Root directory for patch lifecycle subdirs (pending/, rejected/)",
)
def patch_reject(patch_id: str, reason: str, patches_dir: str) -> None:
    """Reject a pending patch and record the reason.

    Moves patches/pending/<patch_id>.json → patches/rejected/<patch_id>.json
    with a rejection_reason annotation.
    """
    from pathlib import Path

    from aqueduct.patch.apply import PatchError, reject_patch

    try:
        rejected_path = reject_patch(
            patch_id=patch_id,
            reason=reason,
            patches_dir=Path(patches_dir),
        )
    except PatchError as exc:
        click.echo(f"✗ reject failed: {exc}", err=True)
        sys.exit(1)

    click.echo(f"✓ patch rejected  id={patch_id}")
    click.echo(f"  archived → {rejected_path}")
    click.echo(f"  reason: {reason}")


@patch.command("rollback")
@click.argument("patch_id")
@click.option(
    "--blueprint",
    required=True,
    type=click.Path(dir_okay=False),
    help="Blueprint YAML file to restore",
)
@click.option(
    "--patches-dir",
    default="patches",
    show_default=True,
    help="Root directory for patch lifecycle subdirs",
)
def patch_rollback(patch_id: str, blueprint: str, patches_dir: str) -> None:
    """Roll back an applied patch — restores the pre-patch Blueprint backup.

    Finds the backup in patches/backups/, restores it atomically to the Blueprint,
    and moves the patch record to patches/rolled_back/.
    """
    import json
    import os
    import shutil
    from datetime import datetime, timezone
    from pathlib import Path

    patches_root = Path(patches_dir)
    blueprint_path = Path(blueprint)

    # Locate backup — name format: <patch_id>_<timestamp>_<blueprint_name>
    backup_dir = patches_root / "backups"
    matches = list(backup_dir.glob(f"{patch_id}_*")) if backup_dir.exists() else []
    if not matches:
        click.echo(
            f"✗ no backup found for patch {patch_id!r} in {backup_dir}\n"
            "  (Only patches applied by Aqueduct have automatic backups.)",
            err=True,
        )
        sys.exit(1)

    backup_path = sorted(matches)[-1]  # most recent backup if multiple

    # Restore backup → blueprint (atomic via temp file)
    tmp = blueprint_path.with_suffix(".rollback.tmp.yml")
    try:
        shutil.copy2(backup_path, tmp)
        os.replace(tmp, blueprint_path)
    except OSError as exc:
        tmp.unlink(missing_ok=True)
        click.echo(f"✗ rollback failed: {exc}", err=True)
        sys.exit(1)

    # Move applied → rolled_back
    applied_path = patches_root / "applied" / f"{patch_id}.json"
    rolled_back_dir = patches_root / "rolled_back"
    rolled_back_dir.mkdir(parents=True, exist_ok=True)
    rolled_back_path = rolled_back_dir / f"{patch_id}.json"

    if applied_path.exists():
        try:
            data = json.loads(applied_path.read_text(encoding="utf-8"))
            meta = data.get("_aq_meta", {})
            meta["rolled_back_at"] = datetime.now(tz=timezone.utc).isoformat()
            data["_aq_meta"] = meta
            rolled_back_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            applied_path.unlink()
        except OSError:
            pass  # non-fatal — blueprint already restored

    click.echo(f"✓ patch rolled back  id={patch_id}")
    click.echo(f"  blueprint  ← {backup_path}")
    if rolled_back_path.exists():
        click.echo(f"  archived   → {rolled_back_path}")


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
    Example test file (pipeline.aqtest.yml):

      aqueduct_test: "1.0"
      blueprint: pipeline.yml

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
    help="Observability store directory (default: aqueduct.yml or .aqueduct/signals)",
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

    resolved = Path(store_dir) if store_dir else Path(cfg.stores.observability.path)
    runs_db = resolved / "runs.db"
    if not runs_db.exists():
        click.echo(f"✗ runs.db not found at {runs_db}", err=True)
        sys.exit(1)

    conn = _duckdb.connect(str(runs_db), read_only=True)
    try:
        row = conn.execute(
            """
            SELECT run_id, pipeline_id, status,
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
        click.echo(f"✗ run {run_id!r} not found in {runs_db}", err=True)
        sys.exit(1)

    run_id_val, pipeline_id, status, started_at, finished_at, module_results_raw = row
    module_results = json.loads(module_results_raw) if isinstance(module_results_raw, str) else (module_results_raw or [])

    if fmt == "json":
        out = {
            "run_id": run_id_val,
            "pipeline_id": pipeline_id,
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
        writer.writerow(["run_id", "pipeline_id", "status", "started_at", "finished_at"])
        writer.writerow([run_id_val, pipeline_id, status, started_at, finished_at])
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
    click.echo(f"{status_icon} run_id={run_id_val}  pipeline={pipeline_id}  status={status}")
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


# ── aqueduct lineage ──────────────────────────────────────────────────────────

@cli.command()
@click.argument("pipeline_id_or_blueprint")
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
    pipeline_id_or_blueprint: str,
    store_dir: str | None,
    config_path: str | None,
    from_table: str | None,
    column_filter: str | None,
    fmt: str,
) -> None:
    """Print column-level lineage graph for a pipeline.

    PIPELINE_ID_OR_BLUEPRINT: pipeline id (e.g. nyc_taxi_demo) or path to
    the blueprint YAML file (e.g. blueprint.yml — id is extracted automatically).
    """
    import duckdb as _duckdb

    from aqueduct.config import ConfigError, load_config

    try:
        cfg = load_config(Path(config_path) if config_path else None)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(1)

    # Accept blueprint file path — extract pipeline id from it
    arg_path = Path(pipeline_id_or_blueprint)
    if arg_path.suffix in (".yml", ".yaml") and arg_path.exists():
        try:
            from aqueduct.parser.parser import parse
            bp = parse(str(arg_path))
            pipeline_id = bp.id
        except Exception as exc:
            click.echo(f"✗ could not read pipeline id from {pipeline_id_or_blueprint!r}: {exc}", err=True)
            sys.exit(1)
    else:
        pipeline_id = pipeline_id_or_blueprint

    resolved = Path(store_dir) if store_dir else Path(cfg.stores.observability.path)
    lineage_db = resolved / "lineage.db"
    if not lineage_db.exists():
        click.echo(f"✗ lineage.db not found at {lineage_db}", err=True)
        sys.exit(1)

    params: list[Any] = [pipeline_id]
    where_parts = ["pipeline_id = ?"]
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
        click.echo(f"No lineage records found for pipeline {pipeline_id!r}.")
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

    click.echo(f"Column lineage — pipeline: {pipeline_id}")
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
        resolved = Path(store_dir) if store_dir else Path(cfg.stores.observability.path)
        signals_db = resolved / "signals.db"
        conn = _duckdb.connect(str(signals_db))
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

    resolved = Path(store_dir) if store_dir else Path(cfg.stores.observability.path)
    resolved.mkdir(parents=True, exist_ok=True)
    signals_db = resolved / "signals.db"

    now = datetime.now(tz=timezone.utc).isoformat()
    conn = _duckdb.connect(str(signals_db))
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

    resolved = Path(store_dir) if store_dir else Path(cfg.stores.observability.path)
    runs_db = resolved / "runs.db"
    if not runs_db.exists():
        click.echo(f"✗ runs.db not found at {runs_db}", err=True)
        sys.exit(1)

    conn = _duckdb.connect(str(runs_db), read_only=True)
    try:
        fc_row = conn.execute(
            """
            SELECT run_id, pipeline_id, failed_module, error_message,
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
        fc_run_id, pipeline_id, failed_module, error_message,
        stack_trace, manifest_json_raw, started_at, finished_at,
    ) = fc_row

    # Allow --module to override the recorded failed_module for scoped healing
    target_module = module_id or failed_module

    failure_ctx = FailureContext(
        run_id=fc_run_id,
        pipeline_id=pipeline_id,
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
    )

    if patch is None:
        click.echo("✗ LLM failed to produce a valid patch", err=True)
        sys.exit(1)

    stage_patch_for_human(patch, patches_path, failure_ctx)
    click.echo(f"✓ patch staged → {patches_path}/pending/{patch.patch_id}.json")
    click.echo(f"  apply with: aqueduct patch apply patches/pending/{patch.patch_id}.json --blueprint <path>")


if __name__ == "__main__":
    cli()