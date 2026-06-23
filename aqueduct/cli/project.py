"""`project` commands — extracted verbatim from aqueduct/cli/__init__.py.

No behaviour change. The click group + shared helpers come from the package;
commands register onto `cli` when imported at the bottom of __init__.
"""
from __future__ import annotations

import sys
from pathlib import Path

import click

from aqueduct import exit_codes
from aqueduct.cli import (
    cli,
    _apply_warnings_from_cfg,
    _resolve_and_load_env,
    _env_options,)


@cli.command("completion")
@click.argument("shell", type=click.Choice(["bash", "zsh", "fish"]))
def completion_cmd(shell: str) -> None:
    """Print a shell-completion script for bash, zsh, or fish.

    \b
    Install — bash:
        aqueduct completion bash > /etc/bash_completion.d/aqueduct.sh
    Install — zsh:
        aqueduct completion zsh > /usr/local/share/zsh/site-functions/_aqueduct
    Install — fish:
        aqueduct completion fish > ~/.config/fish/completions/aqueduct.fish

    The completion script is auto-generated from the click command tree —
    new subcommands and flags are picked up automatically; rerun this
    command after upgrading Aqueduct to refresh the script.
    """
    from click.shell_completion import get_completion_class
    comp_cls = get_completion_class(shell)
    if comp_cls is None:
        raise click.ClickException(f"Unsupported shell: {shell!r}")
    comp = comp_cls(cli, {}, "aqueduct", "_AQUEDUCT_COMPLETE")
    click.echo(comp.source())



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
@click.option(
    "--master",
    default=None,
    help="Spark master for the test session. Default: local[*] (unit tests "
    "ignore deployment.master_url). Set only for cluster-runtime-dependent modules.",
)
@_env_options
def test_cmd(
    test_file: str,
    blueprint_path: str | None,
    config_path: str | None,
    quiet: bool,
    master: str | None,
    env_file: str | None,
    cli_env: tuple[str, ...],
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
    from aqueduct.cli.style import error as _error
    from aqueduct.executor.spark.session import make_spark_session, stop_spark_session
    from aqueduct.executor.spark.test_runner import TestSchemaError, run_test_file

    try:
        _resolve_and_load_env(
            env_file,
            Path(config_path) if config_path
            else Path(blueprint_path) if blueprint_path
            else Path(test_file),
            cli_env=cli_env,
        )
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        _error(f"config error: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)

    merged_spark_config = dict(cfg.spark_config)

    # aqtests are isolated unit tests over inline data — they run local by
    # default and deliberately ignore deployment.master_url so a cluster-
    # pointed config never drags unit tests onto the cluster. --master is
    # the escape hatch for modules whose correctness needs cluster runtime.
    if master:
        master_url = master
    else:
        master_url = "local[*]"
        config_master = cfg.deployment.master_url
        if config_master and not config_master.startswith("local"):
            click.echo(
                f"(test: ignoring deployment.master_url={config_master!r}; "
                f"running on {master_url} — pass --master to override)",
                err=True,
            )

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
    except TestSchemaError as exc:
        click.echo(f"✗ test file error: {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)
    finally:
        stop_spark_session(spark)

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
        sys.exit(exit_codes.DATA_OR_RUNTIME)
    else:
        click.echo(f"✓ all {suite.passed} test(s) passed")



# ── aqueduct init ─────────────────────────────────────────────────────────────


@cli.command("init")
def init() -> None:
    """Scaffold a new Aqueduct project in the current directory."""
    import importlib.resources
    import subprocess

    cwd = Path.cwd()
    project_name = cwd.name

    created: list[str] = []
    skipped: list[str] = []

    def _copy_template(src_subpath: str, dest: Path) -> None:
        if dest.exists():
            skipped.append(str(dest.relative_to(cwd)))
            return
        dest.parent.mkdir(parents=True, exist_ok=True)
        ref = importlib.resources.files("aqueduct.templates.default") / src_subpath
        dest.write_bytes(ref.read_bytes())
        created.append(str(dest.relative_to(cwd)))

    def _mkdir(path: Path) -> None:
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            (path / ".gitkeep").write_text("", encoding="utf-8")
            created.append(str(path.relative_to(cwd)) + "/")

    # Directories
    _mkdir(cwd / "arcades")
    _mkdir(cwd / "blueprints")
    _mkdir(cwd / "aqtests")
    _mkdir(cwd / "aqscenarios")
    _mkdir(cwd / "patches" / "pending")
    _mkdir(cwd / "patches" / "rejected")

    # Templates
    _copy_template("gitignore.template", cwd / ".gitignore")
    _copy_template("aqueduct.yml.template", cwd / "aqueduct.yml.template")
    _copy_template(
        "blueprints/blueprint.yml.template", cwd / "blueprints" / "blueprint.yml.template"
    )
    _copy_template("aqtests/aqtest.yml.template", cwd / "aqtests" / "aqtest.yml.template")
    _copy_template(
        "aqscenarios/aqscenario.yml.template", cwd / "aqscenarios" / "aqscenario.yml.template"
    )

    for f in created:
        click.echo(f"  create  {f}")
    for f in skipped:
        click.echo(f"  skip    {f}  (already exists)")

    # Git
    try:
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
    except FileNotFoundError:
        click.echo("  ⚠ git not found — skipping version control setup")

    click.echo(f"\n✓ {project_name} ready")
    click.echo("\nNext steps:")
    click.echo("  1. Create blueprints/<name>.yml  (see blueprint.template.yml for reference)")
    click.echo("  2. aqueduct validate blueprints/<name>.yml")
    click.echo("  3. aqueduct run blueprints/<name>.yml")

