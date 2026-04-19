"""Aqueduct CLI.

Active commands: validate, compile, run, patch apply, patch reject.
"""

from __future__ import annotations

import json
import sys

import click


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
def compile(blueprint: str, output: str, profile: str | None, ctx: tuple[str, ...]) -> None:
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

    try:
        bp = parse(blueprint, profile=profile, cli_overrides=cli_overrides or None)
    except ParseError as exc:
        click.echo(f"✗ {exc}", err=True)
        sys.exit(1)

    try:
        manifest = compiler_compile(bp, blueprint_path=Path(blueprint))
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
def run(
    blueprint: str,
    profile: str | None,
    ctx: tuple[str, ...],
    run_id: str | None,
    config_path: str | None,
    store_dir: str | None,
    webhook: str | None,
) -> None:
    """Compile and execute a Blueprint on a SparkSession."""
    import uuid
    from pathlib import Path

    from aqueduct.compiler.compiler import CompileError
    from aqueduct.compiler.compiler import compile as compiler_compile
    from aqueduct.config import ConfigError, load_config
    from aqueduct.depot.depot import DepotStore
    from aqueduct.executor.executor import ExecuteError, execute
    from aqueduct.executor.models import ExecutionResult, ModuleResult
    from aqueduct.executor.session import make_spark_session
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
    resolved_webhook = webhook or cfg.webhooks.on_failure
    master_url = cfg.deployment.master_url

    cli_overrides: dict[str, str] = {}
    for item in ctx:
        if "=" not in item:
            click.echo(f"--ctx flag must be KEY=VALUE, got: {item!r}", err=True)
            sys.exit(1)
        k, _, v = item.partition("=")
        cli_overrides[k.strip()] = v

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
        manifest = compiler_compile(bp, blueprint_path=Path(blueprint), depot=depot)
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
    click.echo(
        f"▶ {manifest.pipeline_id}  ({len(manifest.modules)} modules)"
        f"  run={run_id}  master={master_url}"
    )

    # ── Surveyor — start ───────────────────────────────────────────────────────
    surveyor = Surveyor(
        manifest,
        store_dir=resolved_store_dir,
        webhook_url=resolved_webhook,
        blueprint_path=Path(blueprint),
        patches_dir=patches_dir,
    )
    surveyor.start(run_id)

    # ── Spark — merge engine spark_config under Blueprint (Blueprint wins) ─────
    merged_spark_config = {**cfg.spark_config, **manifest.spark_config}
    spark = make_spark_session(manifest.pipeline_id, merged_spark_config, master_url=master_url)

    # ── Execute ────────────────────────────────────────────────────────────────
    execute_exc: ExecuteError | None = None
    try:
        result = execute(manifest, spark, run_id=run_id, store_dir=resolved_store_dir, surveyor=surveyor, depot=depot)
    except ExecuteError as exc:
        execute_exc = exc
        # Wrap into a synthetic ExecutionResult so Surveyor can persist it
        result = ExecutionResult(
            pipeline_id=manifest.pipeline_id,
            run_id=run_id,
            status="error",
            module_results=(
                ModuleResult(module_id="_executor", status="error", error=str(exc)),
            ),
        )

    # ── Surveyor — record ──────────────────────────────────────────────────────
    failure_ctx = surveyor.record(result, exc=execute_exc)
    surveyor.stop()

    # ── Depot — persist run_id for @aq.runtime.prev_run_id() ─────────────────
    try:
        depot.put("_last_run_id", result.run_id)
    except Exception:
        pass  # non-fatal
    depot.close()

    # ── Report ─────────────────────────────────────────────────────────────────
    for mr in result.module_results:
        icon = "✓" if mr.status == "success" else "✗"
        line = f"  {icon} {mr.module_id}"
        if mr.error:
            line += f"  — {mr.error}"
        click.echo(line)

    # Spark cleanup: register stop on atexit so it runs on process exit, not
    # here. Calling spark.stop() directly kills the global JVM context and
    # breaks any caller that reuses this process (tests, REPL, embedded use).
    import atexit
    atexit.register(spark.stop)

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


if __name__ == "__main__":
    cli()