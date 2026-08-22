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
    _apply_warnings_from_cfg,
    _env_options,
    _resolve_and_load_env,
    cli,
)


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
    # Structured result the user redirects straight into a shell completion
    # file (`aqueduct completion bash > ...`) — never wrapped/truncated, so
    # this stays raw click.echo (explicit err=False) rather than funnel.echo.
    click.echo(comp.source(), err=False)


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
    "ignore engine.spark.master_url). Set only for cluster-runtime-dependent modules.",
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

    import yaml

    from aqueduct.cli.style import error as _error
    from aqueduct.cli.style import success as _success
    from aqueduct.config import ConfigError, load_config
    from aqueduct.executor.spark.session import make_spark_session, stop_spark_session
    from aqueduct.executor.spark.test_runner import TestSchemaError, run_test_file

    try:
        _resolve_and_load_env(
            env_file,
            (
                Path(config_path)
                if config_path
                else Path(blueprint_path) if blueprint_path else Path(test_file)
            ),
            cli_env=cli_env,
        )
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        _error(f"config error: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)

    merged_spark_config = dict(cfg.engine.spark.conf)

    # aqtests are isolated unit tests over inline data — they run local by
    # default and deliberately ignore engine.spark.master_url so a cluster-
    # pointed config never drags unit tests onto the cluster. --master is
    # the escape hatch for modules whose correctness needs cluster runtime.
    if master:
        master_url = master
    else:
        master_url = "local[*]"
        config_master = cfg.engine.spark.master_url
        if config_master and not config_master.startswith("local"):
            from aqueduct.cli.render.funnel import echo as _funnel_echo

            _funnel_echo(
                f"(test: ignoring engine.spark.master_url={config_master!r}; "
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
        _error(f"test file error: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)
    except yaml.YAMLError as exc:
        # `run_test_file` parses the test YAML with `yaml.safe_load` and has
        # no schema validation ahead of it — a malformed file (not merely a
        # SCHEMA problem, which is `TestSchemaError` above) raised this raw
        # and uncaught before this handler existed, silently defaulting to
        # whatever exit code Click/CliRunner happens to assign an unhandled
        # exception rather than the documented CONFIG_ERROR contract (a
        # malformed test file is the same class of problem as a malformed
        # aqueduct.yml/Blueprint — exit_codes.py's own docstring).
        _error(f"test file is not valid YAML: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)
    finally:
        stop_spark_session(spark)

    # ── Print results ─────────────────────────────────────────────────────────
    from aqueduct.cli.render.funnel import echo as _funnel_echo

    _funnel_echo(f"\nTest suite: {test_file}", err=False)
    _funnel_echo(
        f"  {suite.total} tests  |  {suite.passed} passed  |  {suite.failed} failed\n", err=False
    )

    for result in suite.results:
        icon = "✓" if result.passed else "✗"
        _funnel_echo(f"  {icon} {result.test_id}", err=False)
        if result.error:
            _funnel_echo(f"      error: {result.error}", err=False)
        for ar in result.assertion_results:
            a_icon = "  ✓" if ar.passed else "  ✗"
            _funnel_echo(f"      {a_icon} [{ar.assertion_type}] {ar.message}", err=False)

    _funnel_echo("", err=False)
    if suite.failed > 0:
        _error(f"{suite.failed} test(s) failed")
        sys.exit(exit_codes.DATA_OR_RUNTIME)
    else:
        _success(f"all {suite.passed} test(s) passed")


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

    from aqueduct.cli.render.funnel import echo as _funnel_echo
    from aqueduct.cli.render.funnel import success as _funnel_success
    from aqueduct.cli.render.funnel import warn_line as _funnel_warn_line

    for f in created:
        _funnel_echo(f"  create  {f}", err=False)
    for f in skipped:
        _funnel_echo(f"  skip    {f}  (already exists)", err=False)

    # Git
    try:
        in_git = (
            subprocess.run(
                ["git", "rev-parse", "--git-dir"],
                capture_output=True,
                cwd=cwd,
            ).returncode
            == 0
        )

        if not in_git:
            r = subprocess.run(["git", "init"], capture_output=True, text=True, cwd=cwd)
            if r.returncode == 0:
                _funnel_echo("  git init", err=False)
            else:
                _funnel_warn_line(f"git init failed: {r.stderr.strip()}", err=False)

        # Initial commit
        add = subprocess.run(["git", "add", "."], capture_output=True, cwd=cwd)
        if add.returncode == 0:
            commit = subprocess.run(
                ["git", "commit", "-m", f"init: aqueduct project ({project_name})"],
                capture_output=True,
                text=True,
                cwd=cwd,
            )
            if commit.returncode == 0:
                _funnel_echo("  git commit  init: aqueduct project", err=False)
            elif "nothing to commit" in commit.stdout + commit.stderr:
                pass  # already clean
            else:
                _funnel_warn_line(f"git commit failed: {commit.stderr.strip()}", err=False)
    except FileNotFoundError:
        _funnel_warn_line("git not found — skipping version control setup", err=False)

    _funnel_success(f"{project_name} ready", err=False)
    _funnel_echo("\nNext steps:", err=False)
    _funnel_echo(
        "  1. Create blueprints/<name>.yml  (see blueprint.template.yml for reference)", err=False
    )
    _funnel_echo("  2. aqueduct validate blueprints/<name>.yml", err=False)
    _funnel_echo("  3. aqueduct run blueprints/<name>.yml", err=False)
