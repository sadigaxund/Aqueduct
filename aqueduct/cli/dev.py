"""`dev` command group — engine/extension authoring tools that SHIP.

`aqueduct dev capabilities scaffold|sync|check|docs` is the capability-declaration
tooling (`aqueduct/executor/capability_tooling.py`). It used to live in
`scripts/capabilities.py`, which is not in the wheel — so a third-party engine
author who `pip install`ed aqueduct could not generate the ~206-row capability
table their engine cannot register without (Spark's own checklist today; a new
engine's is smaller still — no core `config.*` leaves, no other engine's
`engine.<name>.*` leaves, see Q4 step 2 / `docs/specs.md` §10.9). The alternatives
were hand-writing it or copying Spark's, and copying Spark's hands the new engine
~206 `supported` rows: a silent claim to implement the whole grammar, which is
the exact bug the capability framework was just fixed to prevent. So the tool ships.

`dev` is a dev-tooling surface, not a runtime one — it reads and writes source
files in a checkout of an engine package. It follows the same group conventions
as `patch`, `stores` and `blueprint`: a `@cli.group`, commands registered
onto it, re-exported from the bottom of `aqueduct/cli/__init__.py`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import click

from aqueduct import exit_codes
from aqueduct.cli import cli, style
from aqueduct.cli.render.funnel import echo as _funnel_echo
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
                _funnel_echo(f"    MISSING     {leaf}  (no verdict declared)", err=False)
            for leaf in r.undeclared:
                _funnel_echo(f"    UNDECLARED  {leaf}  (needs a real verdict)", err=False)
            for leaf in r.orphaned:
                _funnel_echo(
                    f"    ORPHANED    {leaf}  (not a real leaf — renamed/removed?)", err=False
                )

        # Verdict-test links (Phase 79) — reported, but NOT part of `.ok`/exit
        # code: the build-breaking gate for these lives in the pytest closure
        # test (tests/test_capabilities/test_verdict_test_links.py), which can
        # legitimately stay red on a genuinely unbacked verdict without this
        # dev-tooling command (used for the leaf-completeness CI gate) also
        # going red for the same reason.
        if r.missing_test_links or r.dangling_test_links:
            style.warn(f"{_fmt_path(r.path)} — verdict-test links incomplete:")
            for leaf in r.missing_test_links:
                _funnel_echo(
                    f"    NO TEST     {leaf}  (supported execution leaf has no tests: id)",
                    err=False,
                )
            for leaf, test_id, reason in r.dangling_test_links:
                _funnel_echo(f"    DANGLING    {leaf} -> {test_id}  ({reason})", err=False)

    if bad:
        _funnel_echo(
            "\nRun `aqueduct dev capabilities sync`, then replace each `undeclared` "
            "with a real verdict.",
            err=False,
        )
        sys.exit(exit_codes.CONFIG_ERROR)


@dev_capabilities.command("sync")
@click.option(
    "--no-prune",
    "no_prune",
    is_flag=True,
    default=False,
    help="Report orphaned rows instead of deleting them (the pre-Q4-step-2 behaviour).",
)
def capabilities_sync(no_prune: bool) -> None:
    """Append every newly-derived leaf to each engine's table as `undeclared`;
    prune orphaned rows.

    Never invents a verdict: a human decides what an engine does with a new
    leaf, so a new leaf is always parked at `undeclared`. An orphaned row (a
    declared leaf id that is no longer real — renamed, removed from the
    schema, reclassified core, or now positionally owned by a different
    engine) is DELETED by default — it already makes the table invalid
    (`CapabilityDeclarationError` at registration), so this command's job is
    to leave the table valid, not just report the problem. The deletion is a
    reviewable git diff on a data file, same as any other `sync` edit. Pass
    `--no-prune` to fall back to report-only. The build stays red until each
    `undeclared` becomes a real verdict — that is the point.
    """
    reports = tooling.sync(prune_orphans=not no_prune)
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
                _funnel_echo(f"    {leaf}", err=False)
        if r.orphaned:
            verb = "reported (NOT removed — --no-prune)" if no_prune else "removed"
            style.warn(f"{rel} — {len(r.orphaned)} orphaned row(s) {verb}:")
            for leaf in r.orphaned:
                _funnel_echo(f"    {leaf}", err=False)

    _funnel_echo(
        "\nNow replace each `undeclared` with a real verdict "
        "(supported | unsupported | ignored_with_warning).",
        err=False,
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
    _funnel_echo(
        f"  {result.leaves} leaves, ALL `undeclared` "
        f"({result.grammar_leaves} grammar + {result.config_leaves} config)",
        err=False,
    )
    _funnel_echo("", err=False)
    _funnel_echo(
        f"  The build will REFUSE to register engine {engine!r} until every row is a", err=False
    )
    _funnel_echo("  real verdict (supported | unsupported | ignored_with_warning).", err=False)
    _funnel_echo(
        "  Spark's capabilities.yml is a reference to read, not a file to copy.", err=False
    )


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
    _funnel_echo("", err=False)
    _funnel_echo(f"Add this to your {scaffold.config_target}:", err=False)
    _funnel_echo("", err=False)
    # Structured result the user copies verbatim into a config file — never
    # wrapped/truncated, so it stays raw click.echo (explicit err=False)
    # rather than funnel.echo.
    click.echo(scaffold.config_snippet, err=False)
    for step in scaffold.next_steps:
        style.info(step)
