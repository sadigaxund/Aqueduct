"""`run` commands — extracted verbatim from aqueduct/cli/__init__.py.

No behaviour change. The click group + shared helpers come from the package;
commands register onto `cli` when imported at the bottom of __init__.
"""
from __future__ import annotations

import json
from aqueduct.parser.models import ModuleType
import sys
from typing import Any

import click

from aqueduct import exit_codes
from aqueduct.cli import (
    cli,
    _apply_warnings_from_cfg,
    _compile_with_warnings,
    _check_heal_guardrails,
    _resolve_and_load_env,
    _env_options,
    _rule,
    _uncommitted_applied_patches,)
import aqueduct.cli as _aqcli  # noqa: E402  (monkeypatch-able helpers)


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

    from aqueduct.compiler.compiler import CompileError
    from aqueduct.compiler.compiler import compile as compiler_compile
    from aqueduct.parser.parser import ParseError, parse
    from aqueduct.config import load_config, ConfigError
    try:
        _cfg = load_config(None)
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
            click.echo(f"✗ --execution-date must be YYYY-MM-DD, got: {execution_date_str!r}", err=True)
            sys.exit(exit_codes.USAGE_ERROR)

    try:
        bp = parse(blueprint, profile=profile, cli_overrides=cli_overrides or None)
    except ParseError as exc:
        click.echo(f"✗ {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)

    try:
        _dep = getattr(_cfg, "deployment", None) if _cfg is not None else None
        manifest = _compile_with_warnings(
            compiler_compile, bp, blueprint_path=Path(blueprint), execution_date=execution_date,
            deployment_env=getattr(_dep, "env", None),
            deployment_target=getattr(_dep, "target", None),
        )
    except CompileError as exc:
        click.echo(f"✗ {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)

    rendered = _render_compile_show(manifest, show.lower())

    if output == "-":
        click.echo(rendered)
    else:
        Path(output).write_text(rendered, encoding="utf-8")
        click.echo(f"Compile artefact written → {output}  (--show={show})")


def _render_compile_show(manifest: Any, show: str) -> str:
    """Render the compile output for the chosen --show selector."""
    manifest_dict = manifest.to_dict()

    if show == "manifest":
        return json.dumps(manifest_dict, indent=2)

    if show == "inputs":
        return _format_inputs_fingerprint(manifest_dict.get("inputs_fingerprint") or {})

    if show == "provenance":
        return _format_provenance_table(manifest_dict.get("provenance_map") or {})

    # "all" — full manifest + readable tables appended
    return "\n".join([
        json.dumps(manifest_dict, indent=2),
        "",
        "── Provenance ────────────────────────────────────────────────────────",
        _format_provenance_table(manifest_dict.get("provenance_map") or {}),
        "",
        "── Inputs fingerprint ────────────────────────────────────────────────",
        _format_inputs_fingerprint(manifest_dict.get("inputs_fingerprint") or {}),
    ])


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
    widths = [max(len(r[c]) for r in rows + [("module_id", "path", "size", "last_modified")]) for c in range(4)]
    header = (
        "module_id".ljust(widths[0]) + "  "
        + "path".ljust(widths[1]) + "  "
        + "size".ljust(widths[2]) + "  "
        + "last_modified"
    )
    sep = "  ".join("-" * w for w in widths)
    body = "\n".join(
        r[0].ljust(widths[0]) + "  "
        + r[1].ljust(widths[1]) + "  "
        + r[2].ljust(widths[2]) + "  "
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
        out.append(_format_provenance_rows(
            (key, prov) for key, prov in context_section.items()
        ))
        out.append("")

    modules_section = provenance_map.get("modules") or {}
    for module_id, module_prov in modules_section.items():
        out.append(f"# Module: {module_id}")
        cfg_prov = (module_prov or {}).get("config") or {}
        if not cfg_prov:
            out.append("  (no config entries — module had empty config block)")
            out.append("")
            continue
        out.append(_format_provenance_rows(
            (key, prov) for key, prov in cfg_prov.items()
        ))
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
    body = "\n".join(
        "  " + "  ".join(r[i].ljust(widths[i]) for i in range(4))
        for r in rows
    )
    return "\n".join([header, sep, body])


def _zero_token_attempt(sig_exact):
    """Return a synthetic ``SimpleNamespace`` attempt record for a zero‑token resolution.

    Used by the pending‑cache‑hit and exact‑replay paths so a single factory
    produces the record rather than two duplicated inline constructors.
    """
    from types import SimpleNamespace
    return SimpleNamespace(attempt_num=0, signature=sig_exact,
                           tokens_in=0, tokens_out=0, latency_ms=0,
                           gate_that_rejected=None, escalated=False)


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
    help="Store directory (overrides aqueduct.yml; default: .aqueduct",
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
@click.option(
    "--allow-multi-patch",
    "allow_multi_patch_flag",
    is_flag=True,
    default=False,
    help="Allow `max_patches > 1` for this run (overrides danger.allow_multi_patch=false).",
)
@_env_options
@click.option(
    "--parallel",
    is_flag=True,
    default=False,
    help="Execute independent DAG branches concurrently (one thread per connected component). "
         "Only beneficial when the Blueprint has multiple fully-independent source trees.",
)
@click.option(
    "-v", "--verbose",
    is_flag=True,
    default=False,
    help="Show the full Spark/JVM startup banner (incubator notice, log4j init, "
         "NativeCodeLoader). Suppressed by default for cleaner output; runtime Spark "
         "warnings always print.",
)
@click.option(
    "--sandbox",
    is_flag=True,
    default=False,
    help="Dev dry-run: compile + execute against sampled inputs with every Egress "
         "skipped (no writes). No self-healing, no observability persistence. Fast "
         "feedback loop for iterating on transforms.",
)
@click.option(
    "--sample",
    default=1000,
    show_default=True,
    type=int,
    help="Row cap per Ingress in --sandbox mode (0 = no limit). Ignored without --sandbox.",
)
@click.option(
    "-s", "--set", "set_items",
    multiple=True,
    metavar="PATH=VALUE",
    help="Override a config or blueprint value for this run only (repeatable, "
         "in-memory, never persisted). Dotted path — e.g. "
         "--set agent.approval_mode=auto --set deployment.master_url=spark://h:7077. "
         "Values coerce to bool/int/float/null else string; use PATH:=JSON for "
         "structured values. Highest precedence (beats blueprint + aqueduct.yml).",
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
    verbose: bool = False,
    allow_multi_patch_flag: bool = False,
    env_file: str | None = None,
    cli_env: tuple[str, ...] = (),
    parallel: bool = False,
    sandbox: bool = False,
    sample: int = 1000,
    set_items: tuple[str, ...] = (),
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
    from aqueduct.cli.style import error as _error

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
        from aqueduct.cli import _resolve_project_root
        _project_root = _resolve_project_root(blueprint_path=blueprint_abs)

    _original_cwd = os.getcwd()
    os.chdir(_project_root)
    try:
        # ── .env loading (after project root, before config so vars resolve) ──
        # Anchor on _project_root so the helper loads <project_root>/.env
        # (config dir, else blueprint dir walked up). cwd discovery was
        # dropped Phase 30 — this keeps the project .env working without it.
        _resolve_and_load_env(
            env_file, _project_root / blueprint_abs.name, cli_env=cli_env
        )
        # Rebind blueprint to absolute so all downstream code is CWD-agnostic.
        blueprint = str(blueprint_abs)

        # ── Load engine config ─────────────────────────────────────────────────────
        try:
            cfg = load_config(config_path_abs)
            _apply_warnings_from_cfg(cfg)
        except ConfigError as exc:
            _error(f"config error: {exc}")
            sys.exit(exit_codes.CONFIG_ERROR)

        # ── -s/--set overrides (top precedence, in-memory) ──────────────────────────
        # Applied BEFORE engine/master/danger are read below; blueprint-targeted
        # overrides (agent.*) are overlaid on the raw Blueprint dict at parse time.
        blueprint_set_nested: dict = {}
        if set_items:
            from aqueduct.overrides import OverrideError, apply_to_model, route_overrides
            try:
                _config_set_nested, blueprint_set_nested = route_overrides(
                    set_items, allow_blueprint=True
                )
                cfg = apply_to_model(cfg, _config_set_nested)
            except OverrideError as exc:
                click.echo(f"✗ {exc}", err=True)
                sys.exit(exit_codes.CONFIG_ERROR)
            if _config_set_nested.get("danger"):
                click.echo(click.style(
                    f"⚠  --set DANGER override(s) (single-run, NOT persisted): "
                    f"{_config_set_nested['danger']}", fg="red", bold=True,
                ), err=True)

        # CLI flags override config file; config file overrides built-in defaults
        # Per-pipeline store paths: default .aqueduct/observability/<blueprint_id>.db instead of shared observability.db
        # _using_default_obs_path: only the default path gets relocated to a
        # per-pipeline .aqueduct/observability/<blueprint_id>/ dir below. A
        # user-set observability.path / lineage.path is already honoured
        # verbatim by get_stores() and must NOT be clobbered (ISSUE-024).
        _using_default_obs_path = False
        _obs_routing_base = ".aqueduct/observability"  # per-blueprint routing root
        if store_dir_abs:
            resolved_store_dir = store_dir_abs
        else:
            _observability_path = cfg.stores.observability.path
            if _observability_path is None:
                _using_default_obs_path = True
            if cfg.stores.observability.backend != "duckdb":
                # Non-DuckDB (postgres/redis): `path` is a DSN, NOT a
                # filesystem path — never Path()/mkdir it (would create a
                # bogus `postgresql:/user:pass@host:port` dir). The DSN store
                # persists itself; use the default per-pipeline local scratch
                # dir (.aqueduct/observability/<blueprint_id>) for any
                # residual local artifacts only.
                resolved_store_dir = None  # set below after manifest
            elif _observability_path is None:
                _using_default_obs_path = True
                # Defer to after manifest is parsed (need blueprint_id) — placeholder for now
                resolved_store_dir = None  # set below after manifest
            elif not Path(_observability_path).suffix:
                # Location-only: a custom path with NO file suffix is a BASE
                # DIRECTORY. Route per-blueprint files under it
                # (<dir>/<blueprint_id>/observability.db) — parallel-safe, like
                # the default — instead of one shared file. (specs §3.2)
                _using_default_obs_path = True
                _obs_routing_base = _observability_path
                resolved_store_dir = None  # set below after manifest
            else:
                # Explicit single file (e.g. .../obs.db): ONE DuckDB file for every
                # blueprint using this config → single-writer, so those blueprints
                # must not run in parallel. Use a suffix-less base dir for
                # per-blueprint routing if you need concurrency on DuckDB.
                resolved_store_dir = Path(_observability_path).parent
        # --webhook CLI flag (plain URL) overrides aqueduct.yml; config may be full WebhookEndpointConfig
        resolved_webhook = WebhookEndpointConfig(url=webhook) if webhook else cfg.webhooks.on_failure
        engine = cfg.deployment.engine
        master_url = cfg.deployment.master_url

        # ── Danger settings startup warning ──────────────────────────────────────
        danger_active = []
        if cfg.danger.allow_full_probe_actions:
            danger_active.append("allow_full_probe_actions=true")
        if cfg.danger.allow_multi_patch:
            danger_active.append("allow_multi_patch=true")
        if danger_active:
            click.echo(
                f"⚠  DANGER settings active: {', '.join(danger_active)}",
                err=True,
            )

        # Resolve executor early so an unsupported engine exits before any Spark work
        try:
            execute = get_executor(engine)
        except (NotImplementedError, ValueError) as exc:
            _error(f"engine error: {exc}")
            sys.exit(exit_codes.CONFIG_ERROR)

        # ── Phase 63 / 64 — remote-submit targets branch ──────────────────────────
        _REMOTE_TARGETS = frozenset({"databricks", "emr", "dataproc"})
        if cfg.deployment.target in _REMOTE_TARGETS:
            from aqueduct.deploy import get_submitter
            _submitter = get_submitter(cfg.deployment.target, cfg)
            click.echo(
                "⚠ self-healing is disabled for remote targets — "
                "failures must be handled by the orchestrator",
                err=True,
            )
            try:
                _bp_raw = parse(blueprint, profile=profile)
                _bp_agent = getattr(_bp_raw, "agent", None)
                _approval_mode = getattr(_bp_agent, "approval_mode", None) if _bp_agent else None
                if _approval_mode and _approval_mode not in ("disabled", None):
                    click.echo(
                        f"  ⊘ agent.approval_mode={_approval_mode!r} is ignored for "
                        "remote-submit targets",
                        err=True,
                    )
            except ParseError:
                pass
            try:
                _packaged = _submitter.package(blueprint, cfg)
            except Exception as exc:
                click.echo(f"✗ remote package failed: {exc}", err=True)
                sys.exit(exit_codes.DATA_OR_RUNTIME)

            try:
                _job_id = _submitter.submit(_packaged, cfg)
                click.echo(f"  → submitted remote job  id={_job_id}", err=True)
            except Exception as exc:
                click.echo(f"✗ remote submit failed: {exc}", err=True)
                sys.exit(exit_codes.DATA_OR_RUNTIME)

            _remote_result = _submitter.poll(_job_id, cfg)
            _logs = _submitter.fetch_logs(_job_id, cfg) if _remote_result.status == "error" else ""

            if _remote_result.status == "success":
                for mr in _remote_result.module_results:
                    icon = "✓" if mr.status == "success" else "✗"
                    line = f"  {icon} {mr.module_id}"
                    if mr.error:
                        line += f"  — {mr.error}"
                    click.echo(line)
                click.echo(click.style(_rule(), dim=True))
                click.echo(f"{click.style('✓', fg='green', bold=True)} blueprint complete")
                sys.exit(exit_codes.SUCCESS)
            else:
                if _logs:
                    click.echo(f"\n── remote logs ──\n{_logs}\n──", err=True)
                click.echo(f"\n✗ remote job failed  run_id={run_id}", err=True)
                sys.exit(exit_codes.DATA_OR_RUNTIME)

        cli_overrides: dict[str, str] = {}
        for item in ctx:
            if "=" not in item:
                click.echo(f"--ctx flag must be KEY=VALUE, got: {item!r}", err=True)
                sys.exit(exit_codes.USAGE_ERROR)
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
                sys.exit(exit_codes.USAGE_ERROR)

        # ── Parse ──────────────────────────────────────────────────────────────────
        # Parse BEFORE building stores so the depot mounts can be key-isolated by
        # blueprint_id (per-blueprint depot isolation lives in the backend wiring,
        # not in @aq — see specs §5.3.1 / §6).
        try:
            if blueprint_set_nested:
                # Overlay blueprint-targeted --set values (e.g. agent.approval_mode)
                # on the raw Blueprint dict, then parse so schema validation +
                # extra="forbid" still apply to the overridden values.
                import yaml as _yaml
                from aqueduct.overrides import deep_merge as _deep_merge
                from aqueduct.parser.parser import parse_dict
                _raw_bp = _yaml.safe_load(Path(blueprint).read_text(encoding="utf-8")) or {}
                _raw_bp = _deep_merge(_raw_bp, blueprint_set_nested)
                bp = parse_dict(
                    _raw_bp, base_dir=Path(blueprint).parent,
                    profile=profile, cli_overrides=cli_overrides or None,
                )
            else:
                bp = parse(blueprint, profile=profile, cli_overrides=cli_overrides or None)
        except ParseError as exc:
            _error(f"parse error: {exc}")
            sys.exit(exit_codes.CONFIG_ERROR)

        # ── Build per-run store bundle (Phase 28 — DuckDB / Postgres / Redis dispatch) ─
        # Depot must be ready before compile() so @aq.depot.* in the Blueprint can
        # resolve. Pass blueprint_id so default + non-shared mounts isolate their
        # keys per blueprint.
        from aqueduct.stores import get_stores
        bundle = get_stores(cfg, store_dir_override=store_dir_abs, blueprint_id=bp.id)
        depot = DepotStore(backend=bundle.depot)
        # get/put-interface wrappers for @aq.depot.* resolution (keys already
        # per-blueprint-isolated at the raw layer for non-shared mounts).
        depots_wrapped = {n: DepotStore(backend=s) for n, s in bundle.depots.items()}

        # ── Compile ────────────────────────────────────────────────────────────────
        try:
            manifest = _compile_with_warnings(
                compiler_compile,
                bp,
                blueprint_path=Path(blueprint),
                depot=depot,
                depots=depots_wrapped,
                execution_date=execution_date,
                secrets_provider=cfg.secrets.provider,
                secrets_region=cfg.secrets.region,
                secrets_resolver=cfg.secrets.resolver,
                deployment_env=getattr(cfg.deployment, "env", None),
                deployment_target=getattr(cfg.deployment, "target", None),
                _verbose=verbose,
            )
        except CompileError as exc:
            _error(f"compile error: {exc}")
            sys.exit(exit_codes.CONFIG_ERROR)

        # ── Sandbox dry-run (short-circuit) ──────────────────────────────────────
        # Dev loop: run the compiled pipeline against sampled inputs with every
        # Egress skipped — no writes, no Surveyor, no self-healing, no
        # observability persistence. Reuses the patch-validation sandbox
        # transform so behaviour matches Gate 3.
        if sandbox:
            import atexit
            from aqueduct.patch.preview import build_sandbox_manifest

            if engine != "spark":
                click.echo(f"✗ --sandbox requires engine=spark (got {engine!r})", err=True)
                sys.exit(exit_codes.CONFIG_ERROR)

            sandboxed_manifest, egress_targets = build_sandbox_manifest(manifest, sample)
            merged_spark_config = {**cfg.spark_config, **manifest.spark_config}
            sandbox_run_id = f"sandbox-{run_id or uuid.uuid4().hex[:8]}"

            _limit_desc = f"≤{sample} row(s)/Ingress" if sample and sample > 0 else "no row limit"
            click.echo(
                f"⊙ sandbox dry-run — {_limit_desc}, {len(egress_targets)} Egress "
                "module(s) skipped (no writes, no healing, no persistence)",
                err=True,
            )

            from aqueduct.executor.spark.session import make_spark_session
            session = make_spark_session(manifest.blueprint_id, merged_spark_config, master_url=master_url, quiet_startup=not verbose)
            atexit.register(session.stop)

            try:
                result = execute(
                    sandboxed_manifest, session,
                    run_id=sandbox_run_id,
                    store_dir=None,
                    surveyor=None,
                    depot=depot,
                    from_module=from_module,
                    to_module=to_module,
                    block_full_actions=not cfg.danger.allow_full_probe_actions,
                    parallel=parallel,
                )
            except ExecuteError as exc:
                click.echo(f"✗ sandbox run failed: {exc}", err=True)
                sys.exit(exit_codes.DATA_OR_RUNTIME)

            if result.status != "success":
                failing = next((r for r in result.module_results if r.status == "error"), None)
                detail = f" — first error in {failing.module_id!r}: {failing.error}" if failing else ""
                click.echo(click.style(f"✗ sandbox run status={result.status}{detail}", fg="red"), err=True)
                sys.exit(exit_codes.DATA_OR_RUNTIME)

            _ran = sum(1 for r in result.module_results if r.status == "success")
            click.echo(click.style(
                f"✓ sandbox run succeeded — {_ran} module(s) executed, "
                f"{len(egress_targets)} Egress skipped", fg="green",
            ))
            for tgt in egress_targets:
                click.echo(
                    f"    · skipped Egress {tgt['id']!r} → "
                    f"{tgt.get('format')} {tgt.get('path')}",
                    err=True,
                )
            sys.exit(exit_codes.SUCCESS)

        import warnings as _w
        with _w.catch_warnings(record=True) as _setup_caught:
            _w.simplefilter("always")

            # ── Resolve per-pipeline store dir (needs blueprint_id from manifest) ────────
            if resolved_store_dir is None:
                resolved_store_dir = Path(_obs_routing_base) / manifest.blueprint_id
                resolved_store_dir.mkdir(parents=True, exist_ok=True)

            # ── Cluster-mode store path warning ───────────────────────────────────────
            # Standardized AQ-WARN rule (suppressible). Same rule_id + condition as
            # doctor's `cluster-stores` check — single source of truth, one wording.
            if (
                cfg.deployment.env in ("cluster", "cloud")
                and cfg.stores.observability.backend == "duckdb"
                and not resolved_store_dir.is_absolute()
            ):
                from aqueduct.warnings import emit as _emit_warning
                _emit_warning(
                    "cluster_store_path_relative",
                    f"relative store dir {str(resolved_store_dir)!r} on env="
                    f"{cfg.deployment.env!r} — lost on driver restart (ephemeral CWD on "
                    "YARN/K8s). Set stores.observability.path to an absolute shared-FS path.",
                )

            # ── Multi-patch danger gate ───────────────────────────────────────────────
            # 1.1.0 — gate now keys off `max_patches > 1`, not the legacy
            # `approval_mode: aggressive` string. The legacy `aggressive` string
            # is preserved on the Manifest so downstream branching that still
            # checks for it keeps working; deprecation warning is emitted at
            # parse time.
            _max_patches = manifest.agent.max_patches if manifest.agent else 1
            _mode = manifest.agent.approval_mode if manifest.agent else "disabled"
            # Only auto / aggressive actually drive the multi-patch loop; for
            # human / ci / disabled the `max_patches` value is inert, so don't
            # fail closed on the danger gate just because the field is set high.
            _is_multi_patch = (
                _mode in ("auto", "aggressive")
                and (_max_patches > 1 or _mode == "aggressive")
            )
            if _is_multi_patch and not allow_multi_patch_flag:
                if not cfg.danger.allow_multi_patch:
                    click.echo(
                        f"✗ max_patches={_max_patches} (>1) requires danger.allow_multi_patch: true "
                        "in aqueduct.yml, or pass --allow-multi-patch for this run.",
                        err=True,
                    )
                    sys.exit(exit_codes.CONFIG_ERROR)

            # ── Sandbox-mode danger gates ─────────────────────────────────────────────
            _sandbox_mode = manifest.agent.sandbox_mode if manifest.agent else "sample"
            if _sandbox_mode == "preflight" and not cfg.danger.allow_full_preflight:
                click.echo(
                    "✗ agent.sandbox_mode: preflight requires danger.allow_full_preflight: true "
                    "in aqueduct.yml (full-dataset sandbox replay).",
                    err=True,
                )
                sys.exit(exit_codes.CONFIG_ERROR)
            if _sandbox_mode == "off" and not cfg.danger.allow_skip_sandbox:
                click.echo(
                    "✗ agent.sandbox_mode: off requires danger.allow_skip_sandbox: true "
                    "in aqueduct.yml (skips pre-apply validation; patches hit real data).",
                    err=True,
                )
                sys.exit(exit_codes.CONFIG_ERROR)
            if _sandbox_mode == "preflight":
                click.echo(
                    "⚠ sandbox mode: preflight (full-dataset replay, no Egress) — slow but conclusive",
                    err=True,
                )
            elif _sandbox_mode == "off":
                click.echo(
                    "⚠ DANGER: sandbox mode = off (skipping pre-apply replay; patches apply to real data)",
                    err=True,
                )
            # Double-danger combo — sandbox off + auto multi-patch loop
            if _sandbox_mode == "off" and _is_multi_patch:
                click.echo(
                    "⚠ DANGER COMBO: sandbox_mode=off + max_patches > 1 — every LLM patch "
                    f"applies to real data without pre-validation, up to max_patches="
                    f"{_max_patches} times per failure. Use only when you "
                    "fully trust the model and blueprint scope is tiny.",
                    err=True,
                )

            # ── Pending patch check ────────────────────────────────────────────────────
            patches_dir = _project_root / "patches"
            pending_dir = patches_dir / "pending"
            pending_patches = list(pending_dir.glob("*.json")) if pending_dir.exists() else []
            if pending_patches:
                policy = manifest.agent.on_pending_patches
                _np = len(pending_patches)
                _noun = "patch" if _np == 1 else "patches"
                if policy == "block":
                    names = ", ".join(p.stem for p in pending_patches)
                    click.echo(
                        f"✗ blocked — {_np} pending {_noun} unreviewed: {names}\n"
                        f"  Review: aqueduct patch apply <file> --blueprint {blueprint}\n"
                        f"  Reject: aqueduct patch reject <patch_id> --reason '...'",
                        err=True,
                    )
                    sys.exit(exit_codes.CONFIG_ERROR)
                elif policy == "warn":
                    click.echo(
                        click.style(f"⚠ {_np} pending {_noun} unreviewed", fg="yellow", bold=True)
                        + click.style("  ·  aqueduct patch list", dim=True),
                        err=True,
                    )
                    if verbose:
                        for p in pending_patches:
                            click.echo(f"  · {p.stem}", err=True)

            # ── Uncommitted applied patch warning ──────────────────────────────────────
            uncommitted_applied = _uncommitted_applied_patches(
                Path(blueprint), patches_dir, blueprint_id=manifest.blueprint_id
            )
            if uncommitted_applied:
                n_uc = len(uncommitted_applied)
                _noun = "patch" if n_uc == 1 else "patches"
                click.echo(
                    click.style(f"⚠ {n_uc} applied {_noun} uncommitted", fg="yellow", bold=True)
                    + click.style(
                        f"  ·  aqueduct patch commit --blueprint {Path(blueprint).name}", dim=True
                    ),
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
            _r = click.style(_rule(), dim=True)
        from aqueduct.cli.style import emit_warnings as _emit_warnings
        _emit_warnings(_setup_caught, verbose=verbose, label="session:")


        click.echo(_r)
        click.echo(
            f"{click.style('▶', fg='cyan', bold=True)} "
            f"{click.style(manifest.blueprint_id, bold=True)}  ·  "
            f"{len(manifest.modules)} modules  ·  run {run_id}  ·  {engine} {master_url}"
            f"{selector_note}{exec_date_note}"
        )
        click.echo(_r)

        # ── Resolve agent connection (engine defaults ← blueprint overrides) ─────
        from aqueduct.cli import resolve_agent_connection
        _rac = resolve_agent_connection(cfg.agent, manifest.agent)
        resolved_agent_provider = _rac.provider
        resolved_agent_base_url = _rac.base_url
        resolved_agent_model = _rac.model
        resolved_agent_provider_options = _rac.provider_options
        resolved_agent_timeout = _rac.timeout
        resolved_agent_max_reprompts = _rac.max_reprompts
        resolved_agent_api_key = _rac.api_key
        resolved_agent_engine_prompt_context = _rac.engine_prompt_context
        resolved_agent_blueprint_prompt_context = _rac.blueprint_prompt_context
        resolved_sandbox_master_url = cfg.agent.sandbox_master_url

        # ── Register agent API key for redaction ─────────────────────────────────
        if resolved_agent_api_key:
            from aqueduct.redaction import register as _register_secret
            _register_secret(resolved_agent_api_key, key_hint="agent.api_key")

        # ── Multi-patch disclaimer ────────────────────────────────────────────────
        approval_mode = manifest.agent.approval_mode
        max_patches = manifest.agent.max_patches
        if (approval_mode == "auto" and max_patches > 1) or approval_mode == "aggressive":
            click.echo(
                f"⚠  multi-patch mode — LLM will attempt up to {max_patches} patch(es). "
                f"Each patch is validated in-memory before being written to Blueprint. "
                f"Review patches/applied/ after the run.",
                err=True,
            )

        # ── Surveyor — start ───────────────────────────────────────────────────────
        # For DuckDB *defaults only* the bundle's obs store points at the shared
        # `.aqueduct/observability.db`; rebuild it under the per-pipeline
        # resolved_store_dir (`.aqueduct/observability/<blueprint_id>/`) like
        # before Phase 28. A user-customised observability.path / lineage.path
        # is already honoured verbatim by get_stores() — do NOT rebuild it,
        # that would silently ignore the configured filename (ISSUE-024).
        if _using_default_obs_path and cfg.stores.observability.backend == "duckdb":
            from aqueduct.stores.duckdb_ import (
                DuckDBObservabilityStore,
            )
            from aqueduct.stores import StoreBundle
            bundle = StoreBundle(
                observability=DuckDBObservabilityStore(resolved_store_dir / "observability.db"),
                lineage=None,  # Phase 38: lineage merged into observability
                depot=bundle.depot,
            )
            depot = DepotStore(backend=bundle.depot)
        surveyor = Surveyor(
            manifest,
            store_dir=resolved_store_dir,
            webhook_config=resolved_webhook,
            blueprint_path=Path(blueprint),
            patches_dir=patches_dir,
            stores=bundle,
            blob_config=(cfg.stores.blob.backend, cfg.stores.blob.path),
            lineage_config=(cfg.lineage.openlineage_url, cfg.lineage.openlineage_namespace)
            if cfg.lineage.openlineage_url else None,
        )
        surveyor.start(run_id)
        # Phase 53 — object-store patch lifecycle + patch_index heal cache.
        # Built once: the obs store backs the index, the patch store the bodies.
        _obs_store = surveyor.observability
        _patch_store = surveyor.patch_store()

        # ── Engine session ────────────────────────────────────────────────────────
        merged_spark_config = {**cfg.spark_config, **manifest.spark_config}
        if engine == "spark":
            from aqueduct.executor.spark.session import make_spark_session
            session = make_spark_session(manifest.blueprint_id, merged_spark_config, master_url=master_url, quiet_startup=not verbose)
        else:
            raise NotImplementedError(f"Session creation for engine {engine!r} not implemented")

        import atexit
        atexit.register(session.stop)

        # ── Self-healing run loop ─────────────────────────────────────────────────
        patch_count = 0
        failure_ctx = None
        result = None
        patch_staged_for_review = False  # set when human/ci mode writes a patch to patches/pending/
        patch_rejected_by_gate = False  # set when a validation gate rejects a patch in auto (non-interactive) mode → VALIDATION_GATE(4)
        last_apply_error: str | None = None  # fed back to LLM on next multi-patch iteration

        _replay_tried: set[str] = set()  # patch_ids already replayed this run — multi-patch loop guard

        while True:
            # `iteration_run_id` is the per-iteration uuid used as `run_id`
            # for execute() and persisted on `run_records`. The user-visible
            # outer `run_id` is captured separately as `parent_run_id` on
            # `healing_outcomes` so cross-iteration aggregations remain
            # joinable to the original heal call.
            iteration_run_id = run_id if patch_count == 0 else str(uuid.uuid4())
            if iteration_run_id != run_id:
                # 1.1.0 fix — register parent linkage so record() stamps the
                # outer run_id into run_records.parent_run_id for this
                # iteration's row (INSERT-or-UPDATE in surveyor.record()).
                try:
                    surveyor.register_iteration(
                        run_id=iteration_run_id, parent_run_id=run_id,
                    )
                except Exception:
                    pass
            execute_exc: ExecuteError | None = None
            try:
                result = execute(
                    manifest, session,
                    run_id=iteration_run_id,
                    store_dir=resolved_store_dir,
                    surveyor=surveyor,
                    depot=depot,
                    resume_run_id=resume_run_id if patch_count == 0 else None,
                    from_module=from_module,
                    to_module=to_module,
                    block_full_actions=not cfg.danger.allow_full_probe_actions,
                    parallel=parallel,
                    use_observe=cfg.metrics.use_observe,
                    observability_store=bundle.observability,
                )
            except ExecuteError as exc:
                execute_exc = exc
                result = ExecutionResult(
                    blueprint_id=manifest.blueprint_id,
                    run_id=iteration_run_id,
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
                if _aqcli._agent_usable(resolved_agent_provider, resolved_agent_base_url, resolved_agent_api_key):
                    click.echo(
                        "  ↻ LLM triggered by module rule (overriding approval_mode=disabled → staging patch for review)",
                        err=True,
                    )

            if effective_mode == "disabled" or failure_ctx is None:
                break

            if not _aqcli._agent_usable(resolved_agent_provider, resolved_agent_base_url, resolved_agent_api_key):
                click.echo(
                    f"  ⚠  LLM not reachable (provider={resolved_agent_provider}, no API key or base_url) — "
                    "skipping self-healing. Configure agent in aqueduct.yml or set the API key env var.",
                    err=True,
                )
                break

            if patch_count >= max_patches:
                click.echo(
                    f"⚠  LLM: max_patches={max_patches} reached, stopping self-healing loop",
                    err=True,
                )
                break

            # ── Pre-trigger guardrail check ────────────────────────────────────────
            _should_heal, _no_heal_reason = _check_heal_guardrails(
                failure_ctx, manifest.agent.guardrails
            )
            if not _should_heal:
                click.echo(
                    f"  ⊘  LLM guardrail blocked healing: {_no_heal_reason}",
                    err=True,
                )
                break

            # ── Spend-cap: max_heal_attempts_per_hour (blueprint override > engine default) ─
            _heal_cap = manifest.agent.max_heal_attempts_per_hour
            if _heal_cap is None:
                _heal_cap = getattr(cfg.agent, "max_heal_attempts_per_hour", None)
            if _heal_cap is not None and _heal_cap >= 0:
                _recent = surveyor.count_recent_heal_attempts(within_minutes=60)
                if _recent >= _heal_cap:
                    click.echo(
                        f"  ⊘  LLM rate-limit reached: {_recent} healing attempt(s) "
                        f"in the last 60 minutes (max_heal_attempts_per_hour={_heal_cap}). "
                        "Run ends without further LLM calls. Inspect healing_outcomes in observability.db.",
                        err=True,
                    )
                    break

            # ── Phase 45 signature memory — zero-token paths before the LLM ───────
            # The failure signature hash is computed every iteration (it also
            # stamps healing_outcomes.failure_signature for LLM resolutions).
            from aqueduct.agent.signature import from_failure_context as _from_failure_ctx
            _sig_exact, _sig_coarse = _from_failure_ctx(failure_ctx)
            _patch_source = "llm"     # → stage_patch_for_human(source=...) + healing_outcomes.resolution
            _replay_result = None     # synthetic AgentPatchResult substituting the LLM call
            _replay_gates_done = False  # gates already ran on the replay candidate pre-substitution

            _memory_cfg = getattr(cfg.agent, "memory", None)
            if _memory_cfg is None or _memory_cfg.replay:
                from aqueduct.agent import memory as _heal_memory

                # 1) Pending-patch reuse — same failure already has a patch
                #    awaiting review; re-healing it would burn tokens on a
                #    duplicate. Surface the existing patch and stop.
                _pending_hit = _heal_memory.find_pending(_obs_store, _sig_exact.hash)
                if _pending_hit is not None:
                    _rel_pending = f"{_patch_store.location_label}/{_pending_hit.object_key}"
                    click.echo(
                        f"  ✓ heal cache: pending patch {_pending_hit.patch_id} already covers "
                        f"this failure signature ({_sig_exact.hash}) — skipping LLM (0 tokens)\n"
                        f"    Review: aqueduct patch pull {_pending_hit.patch_id}  "
                        f"(body: {_rel_pending})",
                        err=True,
                    )
                    patch_staged_for_review = True
                    try:
                        surveyor.record_heal_attempt(
                            run_id=run_id,
                            attempt_record=_zero_token_attempt(_sig_exact),
                            stop_reason="cached",
                        )
                        surveyor.record_healing_outcome(
                            run_id=iteration_run_id, failed_module=failure_ctx.failed_module,
                            parent_run_id=run_id,
                            failure_category=failure_ctx.error_class, model=None,
                            patch_id=_pending_hit.patch_id, confidence=None,
                            patch_applied=False, run_success_after_patch=False,
                            failure_signature=_sig_exact.hash, failure_signature_coarse=_sig_coarse.hash, resolution="cached",
                        )
                    except Exception:
                        pass  # persistence must never block the cache hit
                    break

                # 2) Exact replay — an archived patch already fixed this
                #    signature (confirmed via healing_outcomes). Re-validate it
                #    through the normal pipeline with zero LLM tokens; any
                #    failure falls through to the LLM in this same iteration.
                _candidate = _heal_memory.find_replay_candidate(
                    _obs_store, _patch_store, _sig_exact.hash, surveyor.successful_patch_ids(),
                )
                if _candidate is not None and _candidate.patch_id not in _replay_tried:
                    _replay_tried.add(_candidate.patch_id)
                    try:
                        from aqueduct.patch.grammar import PATCH_META_KEY, PatchSpec as _PatchSpec
                        _payload = {k: v for k, v in _candidate.payload.items() if k != PATCH_META_KEY}
                        _replay_patch = _PatchSpec.model_validate(_payload)
                    except Exception as _re_exc:
                        _replay_patch = None
                        click.echo(
                            f"  ⚠ heal cache: archived patch {_candidate.patch_id} no longer "
                            f"parses ({_re_exc}) — falling through to LLM",
                            err=True,
                        )
                    if _replay_patch is not None:
                        _replay_ok = True
                        if effective_mode in ("auto", "aggressive"):
                            # Run the gate pyramid on the candidate NOW so a stale
                            # patch costs one sandbox pass, not a production write.
                            _rg2, _rg3, _rg4, _rg3_passed = _aqcli._run_patch_gates_inline(
                                patch=_replay_patch,
                                blueprint_path=Path(blueprint),
                                bundle=bundle,
                                surveyor=surveyor,
                                failed_module=failure_ctx.failed_module,
                                iteration_run_id=iteration_run_id,
                                blueprint_id=manifest.blueprint_id,
                                sandbox_mode=manifest.agent.sandbox_mode if manifest.agent else "sample",
                                sandbox_master_url=resolved_sandbox_master_url,
                            )
                            if _rg3 is not None and not _rg3_passed:
                                _replay_ok = False
                                click.echo(
                                    f"  ⚠ heal cache: replay candidate {_candidate.patch_id} failed "
                                    f"sandbox replay ({_rg3.detail}) — falling through to LLM",
                                    err=True,
                                )
                            else:
                                _replay_gates_done = True
                        if _replay_ok:
                            from aqueduct.agent import AgentPatchResult as _AgentPatchResult
                            _replay_result = _AgentPatchResult(
                                patch=_replay_patch, attempts=0, stop_reason="replayed",
                            )
                            _patch_source = "replay"
                            click.echo(
                                f"  ✓ heal cache: replaying archived patch {_candidate.patch_id} "
                                f"(signature {_sig_exact.hash}, 0 tokens)",
                                err=True,
                            )
                            try:
                                surveyor.record_heal_attempt(
                                    run_id=run_id,
                                    attempt_record=_zero_token_attempt(_sig_exact),
                                    stop_reason="replayed",
                                )
                            except Exception:
                                pass

            _resolution = "replayed" if _patch_source == "replay" else "llm"

            # ── Generate patch ────────────────────────────────────────────────────
            from aqueduct.agent import generate_agent_patch, stage_patch_for_human
            _attempt_display = (
                f"{patch_count + 1}/{max_patches}"
                if max_patches > 1
                else f"{patch_count + 1}"
            )
            click.echo(
                f"  ↻ LLM self-healing ({_attempt_display})  "
                f"failed_module={failure_ctx.failed_module}",
                err=True,
            )

            # Run blueprint doctor checks against the compiled Manifest (all modules resolved,
            # arcades expanded — no need to re-parse or recurse into sub-blueprints).
            try:
                from aqueduct.doctor import check_blueprint_sources_from_manifest
                from dataclasses import replace as _dc_replace
                _dr = check_blueprint_sources_from_manifest(manifest, deployment_env=cfg.deployment.env)
                _hints = tuple(
                    f"{r.name} — {r.detail}"
                    for r in _dr if r.status in ("warn", "fail")
                )
                if _hints:
                    failure_ctx = _dc_replace(failure_ctx, doctor_hints=_hints)
            except Exception:
                pass  # doctor errors must never block self-healing

            # Persist per-attempt log via the unified reprompt loop's
            # on_attempt hook. Stop reason is recorded against the FINAL row
            # after the loop returns (each row carries it for joinability).
            _heal_run_id = run_id
            from aqueduct.agent import resolve_budget as _resolve_budget
            _budget = _resolve_budget(
                getattr(cfg.agent, "budget", None),
                max_reprompts=resolved_agent_max_reprompts,
            )

            def _persist_attempt(rec, _rid=_heal_run_id, _srv=surveyor):
                try:
                    _srv.record_heal_attempt(run_id=_rid, attempt_record=rec)
                except Exception:
                    pass  # never let persistence block the loop

            # Apply-gate guardrail check wired INTO the unified reprompt loop.
            # Deterministic + fast (no Spark) — runs `_check_guardrails` on the
            # generated PatchSpec against the current Blueprint and feeds any
            # rejection back as a reprompt instead of letting the loop exit
            # 'solved' and then having the outer code silently stage. Slower
            # gates (lineage / sandbox / explain) stay OUTSIDE the loop — they
            # run once per patch in multi-patch mode.
            _bp_path_for_cb = Path(blueprint)
            def _apply_cb(patch_spec: Any, _bp=_bp_path_for_cb) -> tuple:
                try:
                    from aqueduct.patch.apply import (
                        _check_guardrails, _yaml_load, apply_patch_to_dict,
                        PatchError,
                    )
                    bp_raw = _yaml_load(_bp)
                    # 1.1.0 — compile-sanity check. Catches patches that drop
                    # discriminator fields (e.g. `replace_module_config` on a
                    # Channel that omits `op`) before sandbox replay burns
                    # 30+ seconds proving the same thing. Errors feed back to
                    # the LLM as concrete reprompt context.
                    try:
                        bp_after = apply_patch_to_dict(bp_raw, patch_spec)
                        for _m in (bp_after.get("modules") or []):
                            if not isinstance(_m, dict):
                                continue
                            _mt = _m.get("type")
                            _cfg = _m.get("config") or {}
                            if _mt == ModuleType.Channel and "op" not in _cfg:
                                return False, "schema_drift", (
                                    f"Patch leaves Channel module {_m.get('id')!r} without "
                                    f"required 'op' key in config. Use set_module_config_key "
                                    f"to update one key instead of replace_module_config."
                                ), None
                            if _mt in (ModuleType.Ingress, ModuleType.Egress) and "format" not in _cfg:
                                return False, "schema_drift", (
                                    f"Patch leaves {_mt} module {_m.get('id')!r} without "
                                    f"required 'format' key in config."
                                ), None
                    except Exception as exc:
                        return False, "apply_error", (
                            f"Patch failed to apply cleanly: {exc}"
                        ), None
                    gb = (bp_raw.get("agent") or {}).get("guardrails") or {}
                    if not (gb.get("forbidden_ops") or gb.get("allowed_paths")
                            or gb.get("heal_on_errors") or gb.get("never_heal_errors")):
                        return True, None, None, None
                    try:
                        _check_guardrails(patch_spec, bp_raw, provenance_map=None)
                        return True, None, None, None
                    except PatchError as exc:
                        return False, "guardrail_violation", str(exc), None
                except Exception as exc:
                    # Fail-open: don't let an apply-callback bug block healing.
                    return False, "apply_error", str(exc), None

            # Phase 43: when deep_loop is enabled, build a validate_callback
            # that runs sandbox/lineage/explain gates inside the LLM conversation.
            # The model sees rejection feedback and retries in-context.
            # Cascade tiers can opt into deep_loop individually, so the
            # callback must exist whenever ANY tier (or the top level) wants it.
            _deep_loop = manifest.agent.deep_loop if manifest.agent else False
            _cascade_tiers = _rac.cascade
            _any_deep_loop = _deep_loop or any(
                bool(t.deep_loop) for t in (_cascade_tiers or [])
            )
            _validate_cb = None
            if _any_deep_loop:
                _bp_path_for_vc = Path(blueprint)
                _vc_bundle = bundle
                _vc_surveyor = surveyor
                _vc_failed_module = failure_ctx.failed_module
                _vc_rid = iteration_run_id
                _vc_bid = manifest.blueprint_id
                _vc_sandbox_mode = manifest.agent.sandbox_mode if manifest.agent else "sample"

                def _validate_cb(patch_spec: Any) -> tuple:
                    try:
                        _g2, _g3, _g4, _g3_passed = _aqcli._run_patch_gates_inline(
                            patch=patch_spec,
                            blueprint_path=_bp_path_for_vc,
                            bundle=_vc_bundle,
                            surveyor=_vc_surveyor,
                            failed_module=_vc_failed_module,
                            iteration_run_id=_vc_rid,
                            blueprint_id=_vc_bid,
                            sandbox_mode=_vc_sandbox_mode,
                            sandbox_master_url=resolved_sandbox_master_url,
                        )
                        failures: list[str] = []
                        if _g2 is not None and _g2.status == "fail":
                            failures.append(
                                f"Lineage gate: {_g2.detail or 'column impact detected'}"
                            )
                        if _g3 is not None and _g3.status == "fail":
                            failures.append(
                                f"Sandbox gate: {_g3.detail}"
                            )
                        if _g4 is not None and _g4.status == "fail":
                            failures.append(
                                f"Explain gate: {_g4.detail or 'plan regression detected'}"
                            )
                        if failures:
                            return False, " | ".join(failures)
                        return True, ""
                    except Exception as exc:
                        return False, f"Validation error: {exc}"

            # Phase 45: a validated replay candidate substitutes the LLM call
            # entirely — downstream staging/validation/archival treats it like
            # any agent patch (with source="replay" + resolution="replayed").
            if _replay_result is not None:
                agent_result = _replay_result
            # Phase 44: multi-model cascade takes priority over single-model loop.
            elif _cascade_tiers:
                from aqueduct.agent.cascade import generate_cascade_patch
                agent_result = generate_cascade_patch(
                    tiers=list(_cascade_tiers),
                    failure_ctx=failure_ctx,
                    patches_dir=patches_dir,
                    provider=resolved_agent_provider,
                    base_url=resolved_agent_base_url,
                    api_key=resolved_agent_api_key,
                    provider_options=resolved_agent_provider_options,
                    timeout=resolved_agent_timeout,
                    max_tokens=4096,
                    max_reprompts=resolved_agent_max_reprompts,
                    engine_prompt_context=resolved_agent_engine_prompt_context,
                    blueprint_prompt_context=resolved_agent_blueprint_prompt_context,
                    last_apply_error=last_apply_error,
                    guardrails=manifest.agent.guardrails if manifest.agent else None,
                    budget=_budget,
                    allow_defer=manifest.agent.allow_defer if manifest.agent else False,
                    deep_loop=_deep_loop,
                    apply_callback=_apply_cb,
                    validate_callback=_validate_cb,
                    on_attempt=_persist_attempt,
                    memory_coaching=_memory_cfg.coaching if _memory_cfg is not None else True,
                    retry_max_retries=cfg.agent.retry.max_retries,
                    retry_backoff_seconds=cfg.agent.retry.backoff_seconds,
                    obs_store=_obs_store,
                )
            else:
                agent_result = generate_agent_patch(
                    failure_ctx,
                    model=resolved_agent_model,
                    patches_dir=patches_dir,
                    provider=resolved_agent_provider,
                    base_url=resolved_agent_base_url,
                    api_key=resolved_agent_api_key,
                    provider_options=resolved_agent_provider_options,
                    timeout=resolved_agent_timeout,
                    max_reprompts=resolved_agent_max_reprompts,
                    engine_prompt_context=resolved_agent_engine_prompt_context,
                    blueprint_prompt_context=resolved_agent_blueprint_prompt_context,
                    last_apply_error=last_apply_error,
                    guardrails=manifest.agent.guardrails if manifest.agent else None,
                    budget=_budget,
                    allow_defer=manifest.agent.allow_defer if manifest.agent else False,
                    deep_loop=_deep_loop,
                    validate_callback=_validate_cb,
                    on_attempt=_persist_attempt,
                    apply_callback=_apply_cb,
                    memory_coaching=_memory_cfg.coaching if _memory_cfg is not None else True,
                    retry_max_retries=cfg.agent.retry.max_retries,
                    retry_backoff_seconds=cfg.agent.retry.backoff_seconds,
                    obs_store=_obs_store,
                )
            patch = agent_result.patch
            # Phase 46 — record the model that actually produced this result
            # (under cascade the producing tier's model, not the top-level
            # agent.model) and its tier index. None on replay (no LLM ran).
            _outcome_model = agent_result.model or (
                None if _patch_source == "replay" else resolved_agent_model
            )
            _cascade_pos = agent_result.model_cascade_position
            # Update the last persisted row with stop_reason so downstream
            # joins can answer "which axis terminated this heal".
            if agent_result.attempt_records and agent_result.stop_reason:
                try:
                    surveyor.update_heal_attempt_stop_reason(
                        run_id=_heal_run_id,
                        attempt_num=agent_result.attempt_records[-1].attempt_num,
                        stop_reason=agent_result.stop_reason,
                    )
                except Exception:
                    pass
            if patch is None:
                click.echo("  ✗ LLM: failed to generate valid patch, stopping", err=True)
                on_hf = manifest.agent.on_heal_failure if manifest.agent else "stage"
                if on_hf == "stage":
                    click.echo(
                        "  ↑ on_heal_failure=stage: no valid patch to stage — failure context logged in observability.db.",
                        err=True,
                    )
                # Synthesise one healing_outcomes row per rejected
                # attempt so the patch_applied=false trail is observable. Without
                # this, in-loop apply_callback rejections and budget-exhausted
                # heals leave healing_outcomes empty even though heal_attempts
                # logged the per-attempt detail.
                try:
                    for _rec in (agent_result.attempt_records or ()):
                        _fail_cat = (
                            _rec.signature.error_class
                            if getattr(_rec, "signature", None) is not None
                            else None
                        )
                        surveyor.record_healing_outcome(
                            run_id=iteration_run_id,
                            parent_run_id=run_id,
                            failed_module=failure_ctx.failed_module,
                            failure_category=_fail_cat,
                            model=_outcome_model,
                            patch_id=None,
                            confidence=None,
                            patch_applied=False,
                            run_success_after_patch=False,
                            failure_signature=_sig_exact.hash, failure_signature_coarse=_sig_coarse.hash, resolution="llm",
                            model_cascade_position=getattr(_rec, "model_cascade_position", None),
                        )
                except Exception:
                    pass  # never let persistence block the loop exit
                break

            # ── Confidence escalation — low-confidence patches go to human ─────────
            _conf_threshold = manifest.agent.confidence_threshold
            if patch.confidence is not None and patch.confidence < _conf_threshold and effective_mode not in ("human", "disabled"):
                click.echo(
                    f"  ↑ LLM patch confidence {patch.confidence:.0%} < {_conf_threshold:.0%} — escalating to human review",
                    err=True,
                )
                effective_mode = "human"

            # Recovered patches never silently land. If the parser had to apply
            # any mechanical recovery (think-block strip, json_repair fallback,
            # etc.) we downgrade auto/aggressive → human so a reviewer sees
            # exactly what we rescued. Trust boundary stays at the human, not
            # the regex.
            if (
                agent_result.recovery_applied
                and effective_mode in ("auto", "aggressive")
            ):
                click.echo(
                    f"  ↑ LLM response needed mechanical recovery "
                    f"({', '.join(agent_result.recovery_applied)}) — "
                    f"downgrading to human review for safety",
                    err=True,
                )
                effective_mode = "human"

            # ── Guardrail check (pre-staging) ─────────────────────────────────────
            try:
                from aqueduct.patch.apply import PatchError as _PatchError, _check_guardrails as _apply_check_guardrails
                import yaml as _yaml
                _bp_raw = _yaml.safe_load(blueprint_abs.read_text(encoding="utf-8")) or {}
                _apply_check_guardrails(patch, _bp_raw, provenance_map=manifest.provenance_map)
                guardrail_err = None
            except _PatchError as _ge:
                guardrail_err = str(_ge)
            except Exception as _gx:
                guardrail_err = f"Unexpected guardrail error: {_gx}"
            if guardrail_err:
                last_apply_error = f"Patch {patch.patch_id!r} was blocked by agent guardrail: {guardrail_err}"
                click.echo(f"  ✗ LLM patch blocked by guardrail: {guardrail_err}", err=True)
                stage_patch_for_human(patch, patches_dir, failure_ctx,
                                      on_patch_pending_webhook=cfg.webhooks.on_patch_pending,
                                      source=_patch_source,
                                      patch_store=_patch_store, obs_store=_obs_store)
                click.echo(
                    f"  ✎ Patch staged for human review → patches/pending/{patch.patch_id}.json",
                    err=True,
                )
                surveyor.record_healing_outcome(
                    run_id=iteration_run_id, failed_module=failure_ctx.failed_module,
                    parent_run_id=run_id,
                    failure_category=patch.category, model=_outcome_model,
                    patch_id=patch.patch_id, confidence=patch.confidence,
                    patch_applied=False, run_success_after_patch=False,
                    failure_signature=_sig_exact.hash, failure_signature_coarse=_sig_coarse.hash, resolution=_resolution,
                    model_cascade_position=_cascade_pos,
                )
                break

            patch_count += 1

            if effective_mode == "human":
                stage_patch_for_human(patch, patches_dir, failure_ctx,
                                      on_patch_pending_webhook=cfg.webhooks.on_patch_pending,
                                      source=_patch_source,
                                      patch_store=_patch_store, obs_store=_obs_store)
                patch_staged_for_review = True
                pending_file = next(patches_dir.glob(f"pending/*_{patch.patch_id}.json"), None) \
                    or patches_dir / "pending" / f"{patch.patch_id}.json"
                rel_patch = pending_file.relative_to(_project_root) if pending_file.is_relative_to(_project_root) else pending_file
                rel_bp = Path(blueprint).relative_to(_project_root) if Path(blueprint).is_relative_to(_project_root) else Path(blueprint)
                click.echo(
                    f"  ✎ LLM patch staged → {rel_patch}\n"
                    f"    Review: aqueduct patch apply {rel_patch} --blueprint {rel_bp}",
                    err=True,
                )
                surveyor.record_healing_outcome(
                    run_id=iteration_run_id, failed_module=failure_ctx.failed_module,
                    parent_run_id=run_id,
                    failure_category=patch.category, model=_outcome_model,
                    patch_id=patch.patch_id, confidence=patch.confidence,
                    patch_applied=False, run_success_after_patch=False,
                    failure_signature=_sig_exact.hash, failure_signature_coarse=_sig_coarse.hash, resolution=_resolution,
                    model_cascade_position=_cascade_pos,
                )
                break

            elif effective_mode == "ci":
                _ci_url = resolved_agent_base_url or cfg.agent.ci_webhook_url
                if _ci_url:
                    try:
                        import httpx as _httpx
                        _httpx.post(_ci_url, json={
                            "patch": patch.model_dump(),
                            "run_id": iteration_run_id,
                            "blueprint_id": manifest.blueprint_id,
                            "failed_module": failure_ctx.failed_module,
                        }, timeout=10)
                    except Exception as _ce:
                        click.echo(f"  ⚠ ci webhook failed: {_ce}", err=True)
                stage_patch_for_human(patch, patches_dir, failure_ctx,
                                      on_patch_pending_webhook=cfg.webhooks.on_ci_patch,
                                      source=_patch_source,
                                      webhook_event="on_ci_patch",
                                      patch_store=_patch_store, obs_store=_obs_store)
                patch_staged_for_review = True
                click.echo(
                    f"  ✎ CI patch staged → patches/pending/{patch.patch_id}.json",
                    err=True,
                )
                surveyor.record_healing_outcome(
                    run_id=iteration_run_id, failed_module=failure_ctx.failed_module,
                    parent_run_id=run_id,
                    failure_category=patch.category, model=_outcome_model,
                    patch_id=patch.patch_id, confidence=patch.confidence,
                    patch_applied=False, run_success_after_patch=False,
                    failure_signature=_sig_exact.hash, failure_signature_coarse=_sig_coarse.hash, resolution=_resolution,
                    model_cascade_position=_cascade_pos,
                )
                break

            elif effective_mode == "auto":
                # Patch validation pyramid Gates 2 (lineage), 3 (sandbox), 4 (explain) pre-filter.
                # Phase 43: when deep_loop is enabled, these gates already ran
                # inside the LLM conversation — skip the redundant post-hoc run.
                # Phase 45: same skip when the gates already validated a replay
                # candidate at the heal-cache check.
                if _deep_loop or _replay_gates_done:
                    _g3_passed = True
                    _g2, _g3, _g4 = None, None, None
                else:
                    _g2, _g3, _g4, _g3_passed = _aqcli._run_patch_gates_inline(
                        patch=patch,
                        blueprint_path=Path(blueprint),
                        bundle=bundle,
                        surveyor=surveyor,
                        failed_module=failure_ctx.failed_module,
                        iteration_run_id=iteration_run_id,
                        blueprint_id=manifest.blueprint_id,
                        sandbox_mode=manifest.agent.sandbox_mode if manifest.agent else "sample",
                        sandbox_master_url=resolved_sandbox_master_url,
                    )
                if _g4 is not None and _g4.status == "warn":
                    for _r in _g4.regressions:
                        click.echo(f"  ⚠ explain-gate regression: {_r.detail}", err=True)
                if _g3 is not None and not _g3_passed:
                    # Non-interactive (auto) gate rejection → exit VALIDATION_GATE(4).
                    patch_rejected_by_gate = True
                    click.echo(
                        f"  ✗ LLM patch failed sandbox replay: {_g3.detail}",
                        err=True,
                    )
                    surveyor.record_healing_outcome(
                        run_id=iteration_run_id, failed_module=failure_ctx.failed_module,
                        parent_run_id=run_id,
                        failure_category=patch.category, model=_outcome_model,
                        patch_id=patch.patch_id, confidence=patch.confidence,
                        patch_applied=False, run_success_after_patch=False,
                        failure_signature=_sig_exact.hash, failure_signature_coarse=_sig_coarse.hash, resolution=_resolution,
                        model_cascade_position=_cascade_pos,
                    )
                    _aqcli._stage_failed_patch(
                        manifest.agent.on_heal_failure, patch, patches_dir, failure_ctx, cfg, click,
                        obs_store=_obs_store, patch_store=_patch_store,
                    )
                    break

                # Resolve patch_validation (blueprint override → engine default)
                _patch_validation = manifest.agent.patch_validation or cfg.agent.patch_validation

                if _patch_validation == "sandbox" and _g3 is not None and _g3.status == "pass":
                    # Sandbox-only validation: write the patched Blueprint without
                    # running the full pipeline. The next regular `aqueduct run`
                    # will execute it against real data and real Egress sinks.
                    _aqcli._write_patch_to_blueprint(patch, Path(blueprint), patches_dir, failure_ctx, mode="auto",
                                              obs_store=_obs_store, patch_store=_patch_store)
                    click.echo(
                        f"  ✓ LLM patch validated via sandbox-only ({_g3.sample_rows or '∞'} rows) "
                        f"→ {blueprint}",
                        err=True,
                    )
                    surveyor.record_healing_outcome(
                        run_id=iteration_run_id, failed_module=failure_ctx.failed_module,
                        parent_run_id=run_id,
                        failure_category=patch.category, model=_outcome_model,
                        patch_id=patch.patch_id, confidence=patch.confidence,
                        patch_applied=True, run_success_after_patch=True,
                        failure_signature=_sig_exact.hash, failure_signature_coarse=_sig_coarse.hash, resolution=_resolution,
                        model_cascade_position=_cascade_pos,
                    )
                    break

                new_manifest = _aqcli._apply_patch_in_memory(patch, Path(blueprint), depot, profile, cli_overrides or {})
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
                patch_success = result2.status == "success"
                failure_ctx2 = surveyor.record(result2, patched=patch_success)
                surveyor.record_healing_outcome(
                    run_id=iteration_run_id, failed_module=failure_ctx.failed_module,
                    parent_run_id=run_id,
                    failure_category=patch.category, model=_outcome_model,
                    patch_id=patch.patch_id, confidence=patch.confidence,
                    patch_applied=True, run_success_after_patch=patch_success,
                    failure_signature=_sig_exact.hash, failure_signature_coarse=_sig_coarse.hash, resolution=_resolution,
                    model_cascade_position=_cascade_pos,
                )
                if patch_success:
                    _aqcli._write_patch_to_blueprint(patch, Path(blueprint), patches_dir, failure_ctx, mode="auto",
                                              obs_store=_obs_store, patch_store=_patch_store)
                    click.echo(f"  ✓ LLM patch validated and applied → {blueprint}", err=True)
                    result = result2
                    failure_ctx = failure_ctx2
                else:
                    click.echo("  ✗ LLM patch did not fix the issue, Blueprint unchanged", err=True)
                    _aqcli._stage_failed_patch(
                        manifest.agent.on_heal_failure, patch, patches_dir, failure_ctx, cfg, click,
                        obs_store=_obs_store, patch_store=_patch_store,
                    )
                    result = result2
                    failure_ctx = failure_ctx2
                break

            elif effective_mode == "aggressive":
                # Patch validation pyramid Gates 2, 3, 4 pre-filter for the legacy
                # `aggressive` mode (deprecated alias for `auto` + `max_patches > 1`).
                # Phase 45: gates already ran on a replay candidate at the
                # heal-cache check — skip the redundant rerun.
                if _replay_gates_done:
                    _g3_passed = True
                    _g2, _g3, _g4 = None, None, None
                else:
                    _g2, _g3, _g4, _g3_passed = _aqcli._run_patch_gates_inline(
                        patch=patch,
                        blueprint_path=Path(blueprint),
                        bundle=bundle,
                        surveyor=surveyor,
                        failed_module=failure_ctx.failed_module,
                        iteration_run_id=iteration_run_id,
                        blueprint_id=manifest.blueprint_id,
                        sandbox_mode=manifest.agent.sandbox_mode if manifest.agent else "sample",
                        sandbox_master_url=resolved_sandbox_master_url,
                    )
                _block_on_g4 = (
                    manifest.agent.block_on_explain_regression
                    if manifest.agent.block_on_explain_regression is not None
                    else cfg.agent.block_on_explain_regression
                )
                if _g4 is not None and _g4.status == "warn":
                    for _r in _g4.regressions:
                        click.echo(f"  ⚠ explain-gate regression: {_r.detail}", err=True)
                if _block_on_g4 and _g4 is not None and _g4.status == "warn":
                    last_apply_error = (
                        f"Patch {patch.patch_id!r} rejected by the explain gate: "
                        + "; ".join(r.detail for r in _g4.regressions)
                    )
                    click.echo(f"  ✗ multi-patch: explain gate blocked — {last_apply_error}", err=True)
                    surveyor.record_healing_outcome(
                        run_id=iteration_run_id, failed_module=failure_ctx.failed_module,
                        parent_run_id=run_id,
                        failure_category=patch.category, model=_outcome_model,
                        patch_id=patch.patch_id, confidence=patch.confidence,
                        patch_applied=False, run_success_after_patch=False,
                        failure_signature=_sig_exact.hash, failure_signature_coarse=_sig_coarse.hash, resolution=_resolution,
                        model_cascade_position=_cascade_pos,
                    )
                    continue
                if _g3 is not None and not _g3_passed:
                    click.echo(
                        f"  ✗ multi-patch: sandbox rejected patch — {_g3.detail}",
                        err=True,
                    )
                    last_apply_error = f"Patch {patch.patch_id!r} rejected by sandbox: {_g3.detail}"
                    surveyor.record_healing_outcome(
                        run_id=iteration_run_id, failed_module=failure_ctx.failed_module,
                        parent_run_id=run_id,
                        failure_category=patch.category, model=_outcome_model,
                        patch_id=patch.patch_id, confidence=patch.confidence,
                        patch_applied=False, run_success_after_patch=False,
                        failure_signature=_sig_exact.hash, failure_signature_coarse=_sig_coarse.hash, resolution=_resolution,
                        model_cascade_position=_cascade_pos,
                    )
                    continue  # try next patch iteration

                _patch_validation = manifest.agent.patch_validation or cfg.agent.patch_validation

                if _patch_validation == "sandbox" and _g3 is not None and _g3.status == "pass":
                    _aqcli._write_patch_to_blueprint(patch, Path(blueprint), patches_dir, failure_ctx, mode="aggressive",
                                              obs_store=_obs_store, patch_store=_patch_store)
                    click.echo(
                        f"  ✓ multi-patch: sandbox-only validated → {blueprint}",
                        err=True,
                    )
                    surveyor.record_healing_outcome(
                        run_id=iteration_run_id, failed_module=failure_ctx.failed_module,
                        parent_run_id=run_id,
                        failure_category=patch.category, model=_outcome_model,
                        patch_id=patch.patch_id, confidence=patch.confidence,
                        patch_applied=True, run_success_after_patch=True,
                        failure_signature=_sig_exact.hash, failure_signature_coarse=_sig_coarse.hash, resolution=_resolution,
                        model_cascade_position=_cascade_pos,
                    )
                    break

                new_manifest = _aqcli._apply_patch_in_memory(patch, Path(blueprint), depot, profile, cli_overrides or {})
                if new_manifest is None:
                    click.echo("  ✗ LLM patch produces invalid Blueprint, discarding", err=True)
                    last_apply_error = f"Patch {patch.patch_id!r} produced invalid Blueprint"
                    surveyor.record_healing_outcome(
                        run_id=iteration_run_id, failed_module=failure_ctx.failed_module,
                        parent_run_id=run_id,
                        failure_category=patch.category, model=_outcome_model,
                        patch_id=patch.patch_id, confidence=patch.confidence,
                        patch_applied=False, run_success_after_patch=False,
                        failure_signature=_sig_exact.hash, failure_signature_coarse=_sig_coarse.hash, resolution=_resolution,
                        model_cascade_position=_cascade_pos,
                    )
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
                patch_success = result2.status == "success"
                failure_ctx2 = surveyor.record(result2, patched=patch_success)
                surveyor.record_healing_outcome(
                    run_id=iteration_run_id, failed_module=failure_ctx.failed_module,
                    parent_run_id=run_id,
                    failure_category=patch.category, model=_outcome_model,
                    patch_id=patch.patch_id, confidence=patch.confidence,
                    patch_applied=True, run_success_after_patch=patch_success,
                    failure_signature=_sig_exact.hash, failure_signature_coarse=_sig_coarse.hash, resolution=_resolution,
                    model_cascade_position=_cascade_pos,
                )
                if patch_success:
                    _aqcli._write_patch_to_blueprint(patch, Path(blueprint), patches_dir, failure_ctx, mode="aggressive",
                                              obs_store=_obs_store, patch_store=_patch_store)
                    click.echo(
                        f"  ✓ LLM patch validated and applied ({patch_count}/{max_patches}) → {blueprint}",
                        err=True,
                    )
                    result = result2
                    failure_ctx = failure_ctx2
                    break
                else:
                    last_apply_error = (
                        f"Patch {patch.patch_id!r} applied in-memory but re-run still failed: "
                        + (result2.module_results[-1].error or "unknown" if result2.module_results else "unknown")
                    )
                    click.echo(
                        f"  ✗ LLM patch did not fix the issue ({patch_count}/{max_patches})", err=True,
                    )
                    _aqcli._stage_failed_patch(
                        manifest.agent.on_heal_failure, patch, patches_dir, failure_ctx, cfg, click,
                        obs_store=_obs_store, patch_store=_patch_store,
                    )
                    result = result2
                    failure_ctx = failure_ctx2
                    if manifest.agent.on_heal_failure == "abort":
                        break
                    # discard/stage: loop continues → try next patch

        # ── Surveyor stop ─────────────────────────────────────────────────────────
        surveyor.stop()

        # ── Depot — persist run_id for @aq.run.prev_id() ─────────────────
        try:
            depot.put("_last_run_id", run_id)
        except Exception:
            pass
        depot.close()

        # ── Report ────────────────────────────────────────────────────────────────
        # Per-module metrics for the inline summary (best-effort post-run read;
        # surveyor uses short-lived connections, so the store is free by now).
        _metrics: dict[str, tuple] = {}
        try:
            from aqueduct.stores.read import open_obs_read
            from aqueduct.stores.queries import run_detail as _run_detail
            _rs = open_obs_read(cfg, store_dir=store_dir, run_id=run_id,
                                blueprint_id=manifest.blueprint_id)
            if _rs is not None:
                _det = _run_detail(_rs, run_id)
                if _det:
                    for _p in _det.profile:
                        _metrics[_p.module_id] = (_p.records_written, _p.duration_ms)
        except Exception:
            pass

        def _fmt_dur(ms):
            return None if ms is None else (f"{ms} ms" if ms < 1000 else f"{ms / 1000:.1f} s")

        _w = max((len(mr.module_id) for mr in result.module_results), default=0)
        for mr in result.module_results:
            if mr.status == "success":
                icon = click.style("✓", fg="green")
            elif mr.status == "skipped":
                icon = click.style("⏭", fg="cyan")
            else:
                icon = click.style("✗", fg="red", bold=True)
            if mr.status == "error" and mr.error:
                line = f"  {icon} {mr.module_id}  {click.style('— ' + mr.error, fg='red')}"
            else:
                rows, dur = _metrics.get(mr.module_id, (None, None))
                meta = []
                if rows is not None:
                    meta.append(f"{rows:,} rows")
                if _fmt_dur(dur):
                    meta.append(_fmt_dur(dur))
                tail = click.style("  ·  ".join(meta), dim=True) if meta else ""
                line = f"  {icon} {mr.module_id.ljust(_w)}   {tail}".rstrip()
            click.echo(line)

        if result.status not in ("success", "patched"):
            # Print the outer (user-visible) run_id — that's the join key for
            # heal_attempts and `healing_outcomes.parent_run_id`. In multi-patch
            # mode `result.run_id` would be the LAST iteration's per-iteration
            # uuid, which can't be used to retrieve the full heal history.
            if failure_ctx:
                click.echo(
                    f"\n✗ blueprint failed  run_id={run_id}"
                    f"  failed_module={failure_ctx.failed_module}",
                    err=True,
                )
            else:
                click.echo(f"\n✗ blueprint failed  run_id={run_id}", err=True)
            # Distinguish the three non-success terminal states for downstream
            # orchestrators (Airflow operator, CI runners):
            #   HEAL_PENDING(3)   — a patch was staged for human/ci review
            #   VALIDATION_GATE(4)— auto-mode patch rejected by a validation gate
            #   DATA_OR_RUNTIME(2)— hard runtime failure, no actionable patch
            if patch_staged_for_review:
                sys.exit(exit_codes.HEAL_PENDING)
            if patch_rejected_by_gate:
                sys.exit(exit_codes.VALIDATION_GATE)
            sys.exit(exit_codes.DATA_OR_RUNTIME)

        # ── on_success webhook ────────────────────────────────────────────────────
        if cfg.webhooks.on_success:
            from aqueduct.surveyor.webhook import fire_webhook
            success_payload = {
                "run_id": run_id,
                "blueprint_id": manifest.blueprint_id,
                "blueprint_name": manifest.name,
                "module_count": str(len(result.module_results)),
            }
            fire_webhook(
                cfg.webhooks.on_success,
                full_payload=success_payload,
                template_vars=success_payload,
                event="on_success",
            )

        status_label = "patched" if result.status == "patched" else "complete"
        click.echo(click.style(_rule(), dim=True))
        click.echo(f"{click.style('✓', fg='green', bold=True)} blueprint {status_label}")
    finally:
        os.chdir(_original_cwd)

