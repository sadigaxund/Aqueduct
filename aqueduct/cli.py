"""Aqueduct CLI.

Active commands: validate, compile, run.
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
@click.option("--store-dir", default=".aqueduct", show_default=True, help="Observability store directory")
@click.option("--webhook", default=None, help="Webhook URL for failure notifications")
def run(
    blueprint: str,
    profile: str | None,
    ctx: tuple[str, ...],
    run_id: str | None,
    store_dir: str,
    webhook: str | None,
) -> None:
    """Compile and execute a Blueprint on a local SparkSession."""
    import uuid
    from pathlib import Path

    from aqueduct.compiler.compiler import CompileError
    from aqueduct.compiler.compiler import compile as compiler_compile
    from aqueduct.executor.executor import ExecuteError, execute
    from aqueduct.executor.models import ExecutionResult, ModuleResult
    from aqueduct.executor.session import make_spark_session
    from aqueduct.parser.parser import ParseError, parse
    from aqueduct.surveyor.surveyor import Surveyor

    cli_overrides: dict[str, str] = {}
    for item in ctx:
        if "=" not in item:
            click.echo(f"--ctx flag must be KEY=VALUE, got: {item!r}", err=True)
            sys.exit(1)
        k, _, v = item.partition("=")
        cli_overrides[k.strip()] = v

    # ── Parse ──────────────────────────────────────────────────────────────────
    try:
        bp = parse(blueprint, profile=profile, cli_overrides=cli_overrides or None)
    except ParseError as exc:
        click.echo(f"✗ parse error: {exc}", err=True)
        sys.exit(1)

    # ── Compile ────────────────────────────────────────────────────────────────
    try:
        manifest = compiler_compile(bp, blueprint_path=Path(blueprint))
    except CompileError as exc:
        click.echo(f"✗ compile error: {exc}", err=True)
        sys.exit(1)

    run_id = run_id or str(uuid.uuid4())
    click.echo(f"▶ {manifest.pipeline_id}  ({len(manifest.modules)} modules)  run={run_id}")

    # ── Surveyor — start ───────────────────────────────────────────────────────
    surveyor = Surveyor(manifest, store_dir=Path(store_dir), webhook_url=webhook)
    surveyor.start(run_id)

    # ── Spark ──────────────────────────────────────────────────────────────────
    spark = make_spark_session(manifest.pipeline_id, manifest.spark_config)

    # ── Execute ────────────────────────────────────────────────────────────────
    execute_exc: ExecuteError | None = None
    try:
        result = execute(manifest, spark, run_id=run_id)
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

    # ── Report ─────────────────────────────────────────────────────────────────
    for mr in result.module_results:
        icon = "✓" if mr.status == "success" else "✗"
        line = f"  {icon} {mr.module_id}"
        if mr.error:
            line += f"  — {mr.error}"
        click.echo(line)

    spark.stop()

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


if __name__ == "__main__":
    cli()