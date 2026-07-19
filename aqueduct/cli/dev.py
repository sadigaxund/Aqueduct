"""`dev` command group — engine/extension authoring tools that SHIP.

`aqueduct dev capabilities scaffold|sync|check|docs` is the capability-declaration
tooling (`aqueduct/executor/capability_tooling.py`). It used to live in
`scripts/capabilities.py`, which is not in the wheel — so a third-party engine
author who `pip install`ed aqueduct could not generate the 261-row capability
table their engine cannot register without. The alternatives were hand-writing it
or copying Spark's, and copying Spark's hands the new engine 261 `supported` rows:
a silent claim to implement the whole grammar, which is the exact bug the
capability framework was just fixed to prevent. So the tool ships.

`dev` is a dev-tooling surface, not a runtime one — it reads and writes source
files in a checkout of an engine package. It follows the same group conventions
as `patch`, `stores`, `mcp` and `blueprint`: a `@cli.group`, commands registered
onto it, re-exported from the bottom of `aqueduct/cli/__init__.py`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import click

from aqueduct import exit_codes
from aqueduct.cli import cli, style
from aqueduct.dev import scaffolds
from aqueduct.executor import capability_tooling as tooling


@cli.group("dev")
def dev_group() -> None:
    """Engine + extension authoring tools (scaffolds, capability declarations)."""


@dev_group.group("capabilities")
def dev_capabilities() -> None:
    """Generate, sync, and verify engine capability declarations."""


def _fmt_path(p: Path) -> str:
    """Show a path relative to CWD when it is under it, else absolute."""
    try:
        return str(p.relative_to(Path.cwd()))
    except ValueError:
        return str(p)


@dev_capabilities.command("check")
def capabilities_check() -> None:
    """Report declaration drift without writing. Exit 1 if any engine is incomplete.

    Wired into CI: a new grammar/config leaf that no engine has given a verdict
    for fails the build here, before any engine can register with a hole in it.
    """
    reports = tooling.check()
    if not reports:
        style.warn("no engine capability declarations found")
        sys.exit(exit_codes.CONFIG_ERROR)

    bad = False
    for r in reports:
        if r.ok:
            style.success(f"{_fmt_path(r.path)} — {r.total} leaves, all declared")
        else:
            bad = True
            style.error(_fmt_path(r.path))
            for leaf in r.missing:
                click.echo(f"    MISSING     {leaf}  (no verdict declared)")
            for leaf in r.undeclared:
                click.echo(f"    UNDECLARED  {leaf}  (needs a real verdict)")
            for leaf in r.orphaned:
                click.echo(f"    ORPHANED    {leaf}  (not a real leaf — renamed/removed?)")

        # Verdict-test links (Phase 79) — reported, but NOT part of `.ok`/exit
        # code: the build-breaking gate for these lives in the pytest closure
        # test (tests/test_capabilities/test_verdict_test_links.py), which can
        # legitimately stay red on a genuinely unbacked verdict without this
        # dev-tooling command (used for the leaf-completeness CI gate) also
        # going red for the same reason.
        if r.missing_test_links or r.dangling_test_links:
            style.warn(f"{_fmt_path(r.path)} — verdict-test links incomplete:")
            for leaf in r.missing_test_links:
                click.echo(f"    NO TEST     {leaf}  (supported execution leaf has no tests: id)")
            for leaf, test_id, reason in r.dangling_test_links:
                click.echo(f"    DANGLING    {leaf} -> {test_id}  ({reason})")

    if bad:
        click.echo(
            "\nRun `aqueduct dev capabilities sync`, then replace each `undeclared` "
            "with a real verdict."
        )
        sys.exit(exit_codes.CONFIG_ERROR)


@dev_capabilities.command("sync")
def capabilities_sync() -> None:
    """Append every newly-derived leaf to each engine's table as `undeclared`.

    Never invents a verdict and never deletes a row: a human decides what an
    engine does with a new leaf, and an orphaned row is reported for review
    rather than silently dropped. The build stays red until each `undeclared`
    becomes a real verdict — that is the point.
    """
    reports = tooling.sync()
    if not reports:
        style.warn("no engine capability declarations found")
        sys.exit(exit_codes.CONFIG_ERROR)

    for r in reports:
        rel = _fmt_path(r.path)
        if not r.missing:
            style.success(f"{rel} — already complete ({r.total} leaves)")
        else:
            style.info(f"{rel} — appended {len(r.missing)} leaf/leaves as `undeclared`:")
            for leaf in r.missing:
                click.echo(f"    {leaf}")
        if r.orphaned:
            style.warn(
                f"{rel} — {len(r.orphaned)} orphaned row(s) NOT removed "
                "(review + delete by hand if the leaf really is gone):"
            )
            for leaf in r.orphaned:
                click.echo(f"    {leaf}")

    click.echo(
        "\nNow replace each `undeclared` with a real verdict "
        "(supported | unsupported | ignored_with_warning)."
    )


@dev_capabilities.command("scaffold")
@click.option("--engine", required=True, help="Engine name, e.g. duckdb.")
@click.option(
    "--out",
    "out",
    default=None,
    type=click.Path(dir_okay=False),
    help="Output path (default: the engine's package dir under aqueduct/executor/).",
)
@click.option("--force", is_flag=True, help="Overwrite an existing declaration.")
def capabilities_scaffold(engine: str, out: str | None, force: bool) -> None:
    """Write a COMPLETE capabilities.yml for a NEW engine — every leaf `undeclared`.

    Start a new engine here. The table is generated from the live grammar and
    config walkers, so it cannot go stale the way a checked-in template would,
    and it is deliberately not a copy of Spark's declaration: that would hand the
    new engine hundreds of `supported` rows it never decided on. The engine
    cannot register until every row is a real verdict.
    """
    try:
        result = tooling.scaffold(engine, out=out, force=force)
    except FileExistsError as exc:
        style.error(
            f"{_fmt_path(Path(str(exc)))} already exists — refusing to overwrite. "
            "Use --force, or `aqueduct dev capabilities sync` to top it up."
        )
        sys.exit(exit_codes.CONFIG_ERROR)

    style.success(f"wrote {_fmt_path(result.path)}")
    click.echo(
        f"  {result.leaves} leaves, ALL `undeclared` "
        f"({result.grammar_leaves} grammar + {result.config_leaves} config)"
    )
    click.echo("")
    click.echo(f"  The build will REFUSE to register engine {engine!r} until every row is a")
    click.echo("  real verdict (supported | unsupported | ignored_with_warning).")
    click.echo("  Spark's capabilities.yml is a reference to read, not a file to copy.")


@dev_capabilities.command("docs")
@click.option(
    "--out",
    "doc",
    default="docs/compatibility.md",
    show_default=True,
    type=click.Path(dir_okay=False),
    help="Markdown file carrying the ENGINE_MATRIX_START/END markers.",
)
def capabilities_docs(doc: str) -> None:
    """Regenerate the engine capability matrix from the declarations."""
    path = Path(doc)
    if not path.is_file():
        style.error(f"{doc} not found")
        sys.exit(exit_codes.CONFIG_ERROR)
    try:
        changed = tooling.write_matrix(path)
    except ValueError as exc:
        style.error(str(exc))
        sys.exit(exit_codes.CONFIG_ERROR)
    if changed:
        style.success(f"{doc} — engine matrix regenerated")
    else:
        style.success(f"{doc} — engine matrix already up to date")


@dev_group.command("scaffold")
@click.argument("kind", type=click.Choice(list(scaffolds.KINDS)))
@click.option("--name", default=None, help="Name of the generated callable/class.")
@click.option(
    "--module",
    default=None,
    help="Python module (file stem) the stub lands in — the name the config points at.",
)
@click.option(
    "--out",
    "out_dir",
    default=".",
    show_default=True,
    type=click.Path(file_okay=False),
    help="Directory to write the stub into (normally the blueprint's directory).",
)
@click.option("--force", is_flag=True, help="Overwrite an existing file.")
def dev_scaffold(
    kind: str, name: str | None, module: str | None, out_dir: str, force: bool
) -> None:
    """Generate an extension stub for a seam: probe, assert, udf, datasource, secrets.

    The stub is generated from the live contracts (the pydantic schema models, the
    Assert enums, the installed pyspark DataSource's abstract methods, the secrets
    resolver's annotated type), not from a template that rots when a contract
    moves. The config snippet that points at the stub is printed, not written: it
    has to be merged into your blueprint or aqueduct.yml by hand.

    A whole ENGINE starts elsewhere: `aqueduct dev capabilities scaffold --engine <name>`.
    """
    try:
        scaffold = scaffolds.render(kind, name=name, module=module)
    except ModuleNotFoundError as exc:  # datasource needs pyspark installed
        style.error(
            f"scaffolding a {kind} needs its runtime installed: {exc}. "
            "Try: pip install aqueduct-core[spark]"
        )
        sys.exit(exit_codes.CONFIG_ERROR)

    try:
        path = scaffolds.write(scaffold, out_dir=out_dir, force=force)
    except FileExistsError as exc:
        style.error(f"{_fmt_path(Path(str(exc)))} already exists — use --force to overwrite.")
        sys.exit(exit_codes.CONFIG_ERROR)

    style.success(f"wrote {_fmt_path(path)}  ({scaffold.kind} → {scaffold.name})")
    click.echo("")
    click.echo(f"Add this to your {scaffold.config_target}:")
    click.echo("")
    click.echo(scaffold.config_snippet)
    for step in scaffold.next_steps:
        style.info(step)
