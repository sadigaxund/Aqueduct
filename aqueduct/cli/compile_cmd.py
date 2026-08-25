"""`compile` command — extracted verbatim from aqueduct/cli/run.py (Phase 85
Wave 5, F-17 follow-on split).

Self-contained: the `compile` command and its four private rendering helpers
share no state with `run()` — they were never called from inside `run()`
(only from this command itself), so this is a zero-risk extraction. No
behaviour change; registers onto `cli` the same way `run.py` does, by being
imported at the bottom of `aqueduct/cli/__init__.py`.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

import click

from aqueduct import exit_codes
from aqueduct.cli import _apply_warnings_from_cfg, _compile_with_warnings, cli


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
@click.option(
    "--show",
    "show",
    type=click.Choice(["manifest", "provenance", "inputs", "all"], case_sensitive=False),
    default="manifest",
    show_default=True,
    help=(
        "Which section of the compiled artefact to emit. "
        "manifest=full Manifest JSON (current default); provenance=just the "
        "ProvenanceMap as a readable table; inputs=just the inputs_fingerprint; "
        "all=the full Manifest plus the rendered provenance + inputs tables."
    ),
)
def compile(
    blueprint: str,
    output: str,
    profile: str | None,
    ctx: tuple[str, ...],
    execution_date_str: str | None,
    show: str,
) -> None:
    """Parse and compile a Blueprint to a fully-resolved Manifest JSON.

    Use --show provenance to inspect where every config value came from
    (literal vs ${ctx.*} vs @aq.* vs arcade context_override) — useful when
    debugging which Blueprint expression resolved to which runtime value.
    """
    from pathlib import Path

    from aqueduct.cli import _load_config_with_env
    from aqueduct.compiler.compiler import CompileError
    from aqueduct.compiler.compiler import compile as compiler_compile
    from aqueduct.config import ConfigError
    from aqueduct.parser.parser import ParseError, parse

    try:
        # Auto-discover aqueduct.yml (CWD walk-up) like every other command.
        _cfg = _load_config_with_env(None, quiet=True)
        _apply_warnings_from_cfg(_cfg)
    except ConfigError:
        _cfg = None  # missing/invalid aqueduct.yml is OK for `aqueduct compile`

    cli_overrides: dict[str, str] = {}
    for item in ctx:
        if "=" not in item:
            click.echo(f"--ctx flag must be KEY=VALUE, got: {item!r}", err=True)
            sys.exit(exit_codes.USAGE_ERROR)
        k, _, v = item.partition("=")
        cli_overrides[k.strip()] = v

    execution_date = None
    if execution_date_str:
        from datetime import date as _date

        try:
            execution_date = _date.fromisoformat(execution_date_str)
        except ValueError:
            click.echo(
                f"✗ --execution-date must be YYYY-MM-DD, got: {execution_date_str!r}", err=True
            )
            sys.exit(exit_codes.USAGE_ERROR)

    try:
        bp = parse(blueprint, profile=profile, cli_overrides=cli_overrides or None)
    except ParseError as exc:
        click.echo(f"✗ {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)

    depot = None
    depots = None
    if _cfg is not None:
        try:
            from aqueduct.depot.depot import preview_depots

            depot, depots = preview_depots(_cfg, bp.id)
        except Exception as exc:  # pragma: no cover — depot build must never crash preview
            logging.getLogger(__name__).warning(
                "aqueduct compile: could not build preview depot (%s) — "
                "@aq.depot.*/@aq.run.prev_id will hard-fail if this Blueprint uses them",
                exc,
            )
            depot, depots = None, None

    try:
        _dep = getattr(_cfg, "deployment", None) if _cfg is not None else None
        manifest = _compile_with_warnings(
            compiler_compile,
            bp,
            blueprint_path=Path(blueprint),
            depot=depot,
            depots=depots,
            execution_date=execution_date,
            deployment_env=getattr(_dep, "env", None),
            deployment_target=getattr(_dep, "target", None),
            engine=getattr(_dep, "engine", None) or "spark",
        )
    except CompileError as exc:
        click.echo(f"✗ {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)

    rendered = _render_compile_show(manifest, show.lower())

    if output == "-":
        click.echo(rendered, err=False)
    else:
        Path(output).write_text(rendered, encoding="utf-8")
        click.echo(f"Compile artefact written → {output}  (--show={show})", err=False)


def _render_compile_show(manifest: Any, show: str) -> str:
    """Render the compile output for the chosen --show selector."""
    manifest_dict = manifest.to_dict()

    if show == "manifest":
        return json.dumps(manifest_dict, indent=2, ensure_ascii=False)

    if show == "inputs":
        return _format_inputs_fingerprint(manifest_dict.get("inputs_fingerprint") or {})

    if show == "provenance":
        return _format_provenance_table(manifest_dict.get("provenance_map") or {})

    # "all" — full manifest + readable tables appended
    return "\n".join(
        [
            json.dumps(manifest_dict, indent=2, ensure_ascii=False),
            "",
            "── Provenance ────────────────────────────────────────────────────────",
            _format_provenance_table(manifest_dict.get("provenance_map") or {}),
            "",
            "── Inputs fingerprint ────────────────────────────────────────────────",
            _format_inputs_fingerprint(manifest_dict.get("inputs_fingerprint") or {}),
        ]
    )


def _format_inputs_fingerprint(fingerprint: dict) -> str:
    """Render inputs_fingerprint as a per-module table."""
    if not fingerprint:
        return "(no Ingress modules; inputs_fingerprint is empty)"
    rows: list[tuple[str, str, str, str]] = []
    for module_id, entry in fingerprint.items():
        path = str(entry.get("path") or "")
        size_b = entry.get("size_bytes")
        mtime = entry.get("last_modified") or "—"
        size = f"{size_b:,} B" if isinstance(size_b, int) else "—"
        rows.append((module_id, path, size, str(mtime)))
    widths = [
        max(len(r[c]) for r in rows + [("module_id", "path", "size", "last_modified")])
        for c in range(4)
    ]
    header = (
        "module_id".ljust(widths[0])
        + "  "
        + "path".ljust(widths[1])
        + "  "
        + "size".ljust(widths[2])
        + "  "
        + "last_modified"
    )
    sep = "  ".join("-" * w for w in widths)
    body = "\n".join(
        r[0].ljust(widths[0])
        + "  "
        + r[1].ljust(widths[1])
        + "  "
        + r[2].ljust(widths[2])
        + "  "
        + r[3]
        for r in rows
    )
    return "\n".join([header, sep, body])


def _format_provenance_table(provenance_map: dict) -> str:
    """Render ProvenanceMap as a readable per-module / per-context table."""
    out: list[str] = []

    context_section = provenance_map.get("context") or {}
    if context_section:
        out.append("# Context")
        out.append(_format_provenance_rows((key, prov) for key, prov in context_section.items()))
        out.append("")

    modules_section = provenance_map.get("modules") or {}
    for module_id, module_prov in modules_section.items():
        out.append(f"# Module: {module_id}")
        cfg_prov = (module_prov or {}).get("config") or {}
        if not cfg_prov:
            out.append("  (no config entries — module had empty config block)")
            out.append("")
            continue
        out.append(_format_provenance_rows((key, prov) for key, prov in cfg_prov.items()))
        out.append("")
    if not out:
        return "(provenance_map is empty — compile from source first)"
    return "\n".join(out).rstrip()


def _format_provenance_rows(pairs) -> str:
    """Helper: render an iterable of (key, ValueProvenance-dict) into aligned rows."""
    rows: list[tuple[str, str, str, str]] = []
    for key, prov in pairs:
        src_type = str((prov or {}).get("source_type") or "?")
        original = str((prov or {}).get("original_expression") or "")
        resolved = (prov or {}).get("resolved_value")
        rows.append((str(key), src_type, original, "" if resolved is None else str(resolved)))
    if not rows:
        return "  (empty)"
    headers = ("key", "source_type", "original_expression", "resolved_value")
    widths = [max(len(r[c]) for r in [headers] + rows) for c in range(4)]
    header = "  " + "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    sep = "  " + "  ".join("-" * w for w in widths)
    body = "\n".join("  " + "  ".join(r[i].ljust(widths[i]) for i in range(4)) for r in rows)
    return "\n".join([header, sep, body])
