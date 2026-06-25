"""`aqueduct validate`, `lint`, `schema`, `doctor` — static checks + diagnostics.

Extracted verbatim from aqueduct/cli/__init__.py — no behaviour change. The
click group + shared helpers come from the package; commands register onto
`cli` when this module is imported at the bottom of __init__.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import click

from aqueduct import exit_codes
from aqueduct.cli import (
    cli,
    _apply_warnings_from_cfg,
    _DEFAULT_CONFIG_FILENAME,
    _env_options,
    _resolve_and_load_env,
    _rule,
    _sniff_file_kind,
)

@cli.command()
@click.argument("files", nargs=-1, type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="Output format. `json` emits a stable, versioned per-file document.",
)
@_env_options
def validate(
    files: tuple[str, ...], fmt: str, env_file: str | None, cli_env: tuple[str, ...]
) -> None:
    """Static validation — parse + schema check, no side effects.

    File type is detected by its version header, no flag needed:
      `aqueduct: "1.0"`         → Blueprint
      `aqueduct_config: "1.0"`  → engine config (aqueduct.yml)

    Pass any number of files. With no argument, validates `aqueduct.yml`
    in the current directory if present. Exit 0 = all valid, 1 = any invalid.
    """
    from pathlib import Path
    from aqueduct.parser.parser import ParseError, parse
    from aqueduct.config import ConfigError, load_config

    _VALIDATE_JSON_SCHEMA_VERSION = "1.0"

    targets = [Path(f) for f in files]
    if not targets:
        default_cfg = Path.cwd() / _DEFAULT_CONFIG_FILENAME
        if default_cfg.exists():
            if fmt == "text":
                click.echo(f"(no file given → validating {default_cfg.name})", err=True)
            targets = [default_cfg]
        else:
            if fmt == "json":
                click.echo(json.dumps({
                    "schema_version": _VALIDATE_JSON_SCHEMA_VERSION,
                    "summary": {"total": 0, "valid": 0, "invalid": 0, "passed": False},
                    "files": [],
                    "error": "no file given and no aqueduct.yml in CWD",
                }, indent=2))
            else:
                click.echo("✗ no file given and no aqueduct.yml in CWD", err=True)
            sys.exit(exit_codes.CONFIG_ERROR)

    text = fmt == "text"
    any_fail = False
    file_results: list[dict] = []
    from aqueduct.cli import _resolve_project_root
    if targets:
        _root = _resolve_project_root(blueprint_path=targets[0])
        _resolve_and_load_env(env_file, _root / targets[0].name, cli_env=cli_env)
    for path in targets:
        kind = _sniff_file_kind(path)

        if kind == "config":
            try:
                cfg = load_config(path)
                _apply_warnings_from_cfg(cfg)
            except ConfigError as exc:
                if text:
                    click.echo(f"✗ {path}: {exc}", err=True)
                file_results.append({"path": str(path), "kind": "config", "valid": False, "error": str(exc)})
                any_fail = True
                continue
            wh = {}
            if cfg.webhooks.on_failure:
                wh["on_failure"] = f"{cfg.webhooks.on_failure.method} {cfg.webhooks.on_failure.url}"
            if cfg.webhooks.on_success:
                wh["on_success"] = f"{cfg.webhooks.on_success.method} {cfg.webhooks.on_success.url}"
            file_results.append({
                "path": str(path), "kind": "config", "valid": True,
                "engine": cfg.deployment.engine, "target": cfg.deployment.target,
                "master_url": cfg.deployment.master_url,
                "stores": {
                    "observability": cfg.stores.observability.path or "(default)",
                    "depot": cfg.stores.depot.path,
                },
                "secrets_provider": cfg.secrets.provider,
                "webhooks": wh,
                "spark_config": cfg.spark_config or {},
            })
            if text:
                click.echo(f"✓ {path}  [engine config]")
                click.echo(f"  engine:  {cfg.deployment.engine}  target={cfg.deployment.target}  master={cfg.deployment.master_url}")
                click.echo(f"  stores:  observability={cfg.stores.observability.path or '(default)'}  depot={cfg.stores.depot.path}")
                click.echo(f"  secrets: provider={cfg.secrets.provider}")
                click.echo(f"  webhooks: {', '.join(f'{k}={v}' for k, v in wh.items()) if wh else '(not configured)'}")
                if cfg.spark_config:
                    click.echo(f"  spark_config: {json.dumps(cfg.spark_config)}")

        elif kind == "blueprint" or kind is None:
            # Unknown header → attempt blueprint parse (most common case);
            # the parser emits a precise error if it is not a blueprint.
            try:
                bp = parse(str(path))
                file_results.append({
                    "path": str(path), "kind": "blueprint", "valid": True,
                    "id": bp.id, "modules": len(bp.modules), "edges": len(bp.edges),
                })
                if text:
                    click.echo(f"✓ {path}  [blueprint: {bp.id}  {len(bp.modules)} modules, {len(bp.edges)} edges]")
            except ParseError as exc:
                file_results.append({"path": str(path), "kind": "blueprint", "valid": False, "error": str(exc)})
                any_fail = True
                if text:
                    click.echo(f"✗ {path}: {exc}", err=True)

        else:  # aqtest / aqscenario — schema pre-flight lives in `doctor`
            file_results.append({
                "path": str(path), "kind": kind, "valid": None,
                "note": f"use `aqueduct doctor --{kind} {path}` for schema pre-flight",
            })
            if text:
                click.echo(
                    f"- {path}: {kind} file — use `aqueduct doctor --{kind} {path}` "
                    "for schema pre-flight",
                    err=True,
                )

    if fmt == "json":
        _checked = [r for r in file_results if r["valid"] is not None]
        click.echo(json.dumps({
            "schema_version": _VALIDATE_JSON_SCHEMA_VERSION,
            "summary": {
                "total": len(file_results),
                "valid": sum(1 for r in _checked if r["valid"]),
                "invalid": sum(1 for r in _checked if not r["valid"]),
                "passed": not any_fail,
            },
            "files": file_results,
        }, indent=2))

    sys.exit(exit_codes.CONFIG_ERROR if any_fail else exit_codes.SUCCESS)


@cli.command("lint")
@click.argument("blueprint", type=click.Path(exists=True, dir_okay=False))
@click.option("-p", "--profile", default=None, help="Context profile to activate")
@click.option(
    "--strict",
    is_flag=True,
    default=False,
    help="Promote every finding to error severity — exit non-zero on any "
    "finding. Use to gate CI on a lint-clean Blueprint.",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="Output format. `json` emits a stable, versioned document.",
)
@_env_options
def lint_cmd(
    blueprint: str,
    profile: str | None,
    strict: bool,
    fmt: str,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Static style + correctness checks on a Blueprint (AQ-LINT rules).

    Goes beyond `validate` (which only parses + schema-checks): lints the
    parsed module/edge graph and Channel SQL for smells the schema permits —
    orphan modules, duplicate edges, non-descriptive labels, cartesian joins,
    `SELECT *` into an Egress, aggregate/GROUP BY mismatches, un-aliased
    self-joins. Each finding has a stable `AQ-LINT<NNN>` id.

    All initial rules are advisory (`warn`); a clean Blueprint and a
    warn-only result both exit 0. `--strict` promotes findings to errors so a
    non-empty result exits non-zero — wire that into CI to enforce a clean tree.
    """
    from pathlib import Path

    from aqueduct.lint import LINT_SCHEMA_VERSION, run_lint
    from aqueduct.parser.parser import ParseError, parse

    from aqueduct.cli import _resolve_project_root
    _resolve_and_load_env(env_file, _resolve_project_root(blueprint_path=Path(blueprint)) / Path(blueprint).name, cli_env=cli_env)
    try:
        bp = parse(blueprint, profile=profile)
    except ParseError as exc:
        if fmt == "json":
            click.echo(json.dumps({
                "schema_version": LINT_SCHEMA_VERSION,
                "blueprint": str(blueprint),
                "error": f"parse error: {exc}",
                "findings": [],
            }, indent=2))
        else:
            click.echo(f"✗ {blueprint}: parse error — {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)

    findings = run_lint(bp)

    def _sev(f) -> str:
        return "error" if strict else f.severity

    n_error = sum(1 for f in findings if _sev(f) == "error")
    n_warn = len(findings) - n_error
    has_blocking = n_error > 0

    if fmt == "json":
        click.echo(json.dumps({
            "schema_version": LINT_SCHEMA_VERSION,
            "blueprint": bp.id,
            "strict": strict,
            "summary": {
                "total": len(findings),
                "error": n_error,
                "warn": n_warn,
                "passed": not has_blocking,
            },
            "findings": [
                {
                    "rule_id": f.rule_id,
                    "severity": _sev(f),
                    "module_id": f.module_id,
                    "message": f.message,
                }
                for f in findings
            ],
        }, indent=2))
    else:
        if not findings:
            click.echo(click.style(f"✓ {blueprint}: no lint findings", fg="green"))
        else:
            for f in findings:
                sev = _sev(f)
                color = "red" if sev == "error" else "yellow"
                loc = f" [{f.module_id}]" if f.module_id else ""
                click.echo(click.style(
                    f"  {sev.upper():<5} {f.rule_id}{loc}: {f.message}", fg=color,
                ))
            click.echo()
            click.echo(f"{len(findings)} finding(s): {n_error} error, {n_warn} warn")

    sys.exit(exit_codes.CONFIG_ERROR if has_blocking else exit_codes.SUCCESS)


@cli.command("schema")
@click.option(
    "--target",
    type=click.Choice(["blueprint", "config", "patch"], case_sensitive=False),
    default="blueprint",
    show_default=True,
    help="Which Pydantic model to emit the JSON Schema for.",
)
@click.option(
    "-o", "--output",
    type=click.Path(dir_okay=False, writable=True),
    default="-",
    show_default=True,
    help="Output file path. `-` writes to stdout.",
)
def schema(target: str, output: str) -> None:
    """Emit Pydantic-derived JSON Schema (Phase 30b — v1.0 stability contract).

    Targets:
      blueprint  Blueprint YAML schema (enables IDE autocomplete + CI gate)
      config     aqueduct.yml engine config
      patch      PatchSpec grammar (LLM-generated patches)
    """
    import json as _json

    target = target.lower()
    if target == "blueprint":
        from aqueduct.parser.schema import BlueprintSchema
        model = BlueprintSchema
    elif target == "config":
        from aqueduct.config import AqueductConfig
        model = AqueductConfig
    elif target == "patch":
        from aqueduct.patch.grammar import PatchSpec
        model = PatchSpec
    else:  # pragma: no cover — Click already validates
        click.echo(f"✗ unknown target: {target}", err=True)
        sys.exit(exit_codes.USAGE_ERROR)

    try:
        js = model.model_json_schema()
    except Exception as exc:
        click.echo(f"✗ schema generation failed: {exc}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    text = _json.dumps(js, indent=2, sort_keys=True)
    if output == "-":
        click.echo(text)
    else:
        Path(output).write_text(text + "\n", encoding="utf-8")
        click.echo(f"✓ wrote {target} schema → {output}", err=True)



@cli.command()
@click.argument("target", required=False, type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--skip-spark",
    is_flag=True,
    default=False,
    help="Skip the Spark check entirely.",
)
@click.option(
    "--preflight",
    "preflight",
    is_flag=True,
    default=False,
    help="Full Spark session test with real spark_config (builds a session, runs a task). "
         "Unbounded — waits out cluster cold-start / jar shipping (Ctrl-C to abort). "
         "Default is a fast bounded TCP reachability probe, no session.",
)
@click.option(
    "--aqtest",
    "aqtest_path",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Schema pre-flight on a .aqtest.yml file (verifies blueprint ref + module IDs).",
)
@click.option(
    "--aqscenario",
    "aqscenario_path",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Schema pre-flight on a .aqscenario.yml file (verifies blueprint ref + inject_failure.module).",
)
@click.option(
    "-v", "--verbose",
    "verbose",
    is_flag=True,
    default=False,
    help="Show skipped checks too (not-applicable / not-configured), not just the collapsed summary.",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="Output format. `json` emits a stable, versioned document of every "
    "check (implies --verbose: no rows are collapsed).",
)
@_env_options
def doctor(
    target: str | None,
    skip_spark: bool,
    preflight: bool,
    aqtest_path: str | None,
    aqscenario_path: str | None,
    verbose: bool,
    fmt: str,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Live connectivity checks: config, stores, Spark, webhook, secrets, storage.

    Pass a file — its type is detected by the version header, no flag needed:
      `aqueduct_config: "1.0"`  → engine config (also the default when no arg)
      `aqueduct: "1.0"`         → Blueprint (also probes Ingress/Egress/JDBC)

    With no argument, checks `aqueduct.yml` in the current directory.

    `--aqtest` / `--aqscenario` add a schema pre-flight on those file kinds
    (additive — combine with a config/blueprint target).

    Each check is independent. Spark check requires pyspark and may take
    10-15s for JVM startup; use --skip-spark in fast CI contexts.
    Exit codes: 0 = all ok/warn/skip, 1 = any check failed.
    """
    from pathlib import Path
    from aqueduct.doctor import run_doctor
    from aqueduct.cli.style import ICON as _ICON, COLOR as _COLOR, info as _info, error as _error, success as _success, emit_warnings as _emit_warnings

    config_path: Path | None = None
    blueprint_path: Path | None = None

    if target is not None:
        tpath = Path(target)
        kind = _sniff_file_kind(tpath)
        if kind == "config":
            config_path = tpath
        elif kind == "blueprint":
            blueprint_path = tpath
        elif kind == "aqtest":
            aqtest_path = aqtest_path or str(tpath)
        elif kind == "aqscenario":
            aqscenario_path = aqscenario_path or str(tpath)
        else:
            click.echo(
                f"✗ {tpath}: unrecognised Aqueduct file "
                "(no aqueduct / aqueduct_config / aqueduct_test / "
                "aqueduct_scenario header)",
                err=True,
            )
            sys.exit(exit_codes.CONFIG_ERROR)
    else:
        default_cfg = Path.cwd() / _DEFAULT_CONFIG_FILENAME
        if default_cfg.exists():
            _info(f"(no file given \u2192 checking {default_cfg.name})", err=True)
            config_path = default_cfg

    # Anchor .env discovery to the project root (walk up to aqueduct.yml),
    # not the blueprint's immediate directory.  Matches run.py behaviour so
    # `${HOST_IP}` / `${DRIVER_HOST}` resolve for --preflight.
    from aqueduct.cli import _resolve_project_root
    _anchor = _resolve_project_root(blueprint_path=blueprint_path, config_path=config_path)
    _resolve_and_load_env(env_file, _anchor / (blueprint_path or config_path or Path(_DEFAULT_CONFIG_FILENAME)).name, cli_env=cli_env)

    _STATUS_ICON = _ICON
    _STATUS_COLOR = _COLOR

    # Group assignment for sectioned display + JSON. Most checks already carry
    # a `group`; leaf checks default to "general" — stamp those by name here so
    # the renderer can section them without per-call-site churn in doctor/.
    _GROUP_FOR_NAME = {
        "config": "config",
        "observability": "stores", "lineage": "stores", "depot": "stores",
        "store-backend": "stores", "cluster-stores": "stores",
        "secrets": "secrets",
        "agent": "agent",
        "webhook": "network",
        "spark": "spark", "storage": "spark", "cloudpickle": "spark",
        "aqtest": "validation", "aqscenario": "validation", "blueprint": "validation",
    }
    _GROUP_ORDER = ["config", "stores", "spark", "io", "agent", "secrets", "network", "validation", "general"]
    _GROUP_LABEL = {
        "config": "Config", "stores": "Stores", "spark": "Spark",
        "io": "Blueprint sources", "agent": "Agent / LLM", "secrets": "Secrets",
        "network": "Network", "validation": "Blueprint files", "general": "General",
    }

    def _group_of(r) -> str:
        if r.group and r.group != "general":
            return r.group
        if r.name.startswith(("ingress:", "egress:")):
            return "io"
        return _GROUP_FOR_NAME.get(r.name, "general")

    if fmt == "text":
        pass  # header rendered below — no preamble needed

    import warnings as _w
    with _w.catch_warnings(record=True) as _caught:
        _w.simplefilter("always")
        results = run_doctor(
            config_path=config_path,
            skip_spark=skip_spark,
            blueprint_path=blueprint_path,
            aqtest_path=Path(aqtest_path) if aqtest_path else None,
            aqscenario_path=Path(aqscenario_path) if aqscenario_path else None,
            preflight=preflight,
        )

    any_fail = any(r.status == "fail" for r in results)

    # ── Emit any warnings caught during config-load / run_doctor before the grid ──
    if fmt == "text":
        _emit_warnings(_caught, verbose=verbose)

    # ── JSON output (no row collapsing — every check is emitted) ──────────────
    if fmt == "json":
        import json as _json
        _DOCTOR_JSON_SCHEMA_VERSION = "1.0"
        counts = {"ok": 0, "fail": 0, "warn": 0, "skip": 0}
        for r in results:
            counts[r.status] = counts.get(r.status, 0) + 1
        click.echo(_json.dumps({
            "schema_version": _DOCTOR_JSON_SCHEMA_VERSION,
            "summary": {**counts, "total": len(results), "passed": not any_fail},
            "checks": [
                {
                    "name": r.name,
                    "status": r.status,
                    "group": _group_of(r),
                    "detail": r.detail,
                    "elapsed_ms": r.elapsed_ms,
                }
                for r in results
            ],
        }, indent=2))
        sys.exit(exit_codes.CONFIG_ERROR if any_fail else exit_codes.SUCCESS)

    # ── Framed header (text mode only, matches aqueduct run style) ─────────────
    _file_label = (
        str(blueprint_path or config_path or _DEFAULT_CONFIG_FILENAME)
        if target else _DEFAULT_CONFIG_FILENAME
    )
    _r = click.style(_rule(), dim=True)
    click.echo(_r)
    click.echo(
        f"{click.style(_ICON['header'], fg=_COLOR['header'], bold=True)} "
        f"{click.style('doctor', bold=True)}  \u00b7  "
        f"{_file_label}  \u00b7  "
        f"{len(results)} checks"
    )
    click.echo(_r)

    # ── Text output (grouped sections) ────────────────────────────────────────
    # Default view = actionable rows only. Hidden (collapsed into one aligned
    # line): `skip` rows (not-applicable / not-configured) and green
    # low-signal rows (quiet_when_ok, e.g. cloudpickle). Policy is data on
    # CheckResult — the renderer stays generic, no per-name branches.
    def _hidden(r) -> bool:
        return r.status == "skip" or (r.status == "ok" and r.quiet_when_ok)

    shown = results if verbose else [r for r in results if not _hidden(r)]
    hidden = [] if verbose else [r for r in results if _hidden(r)]

    col_w = max((len(r.name) for r in shown), default=0)
    col_w = max(col_w, len("more")) + 2

    by_group: dict[str, list] = {}
    for r in shown:
        by_group.setdefault(_group_of(r), []).append(r)

    for grp in _GROUP_ORDER:
        rows = by_group.get(grp)
        if not rows:
            continue
        click.echo(click.style(f"  {_GROUP_LABEL.get(grp, grp.title())}", fg=_COLOR['header'], bold=True))
        for r in rows:
            icon = _STATUS_ICON[r.status]
            color = _STATUS_COLOR[r.status]
            label = r.name.ljust(col_w)
            elapsed = f"  [{r.elapsed_ms}ms]" if r.elapsed_ms > 0 else ""
            line = f"    {icon} {label}{r.detail}{elapsed}"
            click.echo(click.style(line, fg=color) if color else line)

    if hidden:
        names = ", ".join(r.name for r in hidden)
        # Same aligned `{glyph} {name.ljust(col_w)}{detail}` shape as the rows above.
        click.echo(click.style(
            f"  · {'more'.ljust(col_w)}{names}  (ok / not applicable / not configured — --verbose)",
            fg="bright_black",
        ))

    click.echo(click.style(_rule(), dim=True))
    if any_fail:
        _error("one or more checks failed")
        sys.exit(exit_codes.CONFIG_ERROR)
    else:
        _success("all checks passed")

