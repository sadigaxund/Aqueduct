"""Aqueduct CLI.

Commands: init, validate, compile, run, check-config, doctor, runs, report,
          lineage, signal, heal, benchmark, log, rollback,
          patch apply, patch reject, patch commit, patch discard, patch list.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

import warnings

import click


def _compile_with_warnings(compile_fn, *args, **kwargs):
    """Call compile_fn, intercept UserWarnings, reprint as clean CLI output."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = compile_fn(*args, **kwargs)
    for w in caught:
        if issubclass(w.category, UserWarning):
            click.echo(f"WARNING: {w.message}", err=True)
        else:
            warnings.warn_explicit(w.message, w.category, w.filename, w.lineno)
    return result


# ── Self-healing helpers ──────────────────────────────────────────────────────
# Deterministic guardrail enforcement lives in aqueduct.patch.apply._check_guardrails.
# That is the single authoritative implementation; do not reintroduce a CLI-side copy.

def _extract_stack_class(stack_trace: str | None) -> str | None:
    """Extract the exception class name from the last line of a stack trace.

    e.g. 'pyspark.errors.exceptions.SparkException: ...' → 'SparkException'
    """
    if not stack_trace:
        return None
    last_line = stack_trace.strip().splitlines()[-1]
    class_part = last_line.split(":")[0].strip()
    return class_part.split(".")[-1] if class_part else None


def _check_heal_guardrails(failure_ctx: Any, guardrails: Any) -> tuple[bool, str]:
    """Pre-trigger guardrail check.

    Returns (should_heal, reason_if_blocked).
    never_heal_errors takes priority over heal_on_errors.
    Matching uses error_type from FailureContext (Assert label) or the
    exception class name extracted from the stack trace (infra errors).
    """
    error_type: str | None = getattr(failure_ctx, "error_type", None)
    stack_class: str | None = _extract_stack_class(getattr(failure_ctx, "stack_trace", None))

    candidates: set[str] = set()
    if error_type:
        candidates.add(error_type)
    if stack_class:
        candidates.add(stack_class)

    never_heal: tuple = tuple(getattr(guardrails, "never_heal_errors", ()))
    heal_on: tuple = tuple(getattr(guardrails, "heal_on_errors", ()))

    for et in never_heal:
        if et in candidates:
            return False, f"error_type {et!r} matched never_heal_errors"

    if heal_on:
        for et in heal_on:
            if et in candidates:
                return True, ""
        matched = f"error_type={error_type!r}" if error_type else f"stack_class={stack_class!r}"
        return False, f"{matched} not in heal_on_errors whitelist"

    return True, ""


def _llm_usable(provider: str, base_url: str | None) -> bool:
    """Return True if the LLM provider appears reachable without making a network call.

    anthropic:     requires ANTHROPIC_API_KEY in os.environ
    openai_compat: requires base_url (Ollama/vLLM) OR OPENAI_API_KEY
    """
    import os as _os
    if provider == "anthropic":
        return bool(_os.environ.get("ANTHROPIC_API_KEY"))
    if provider == "openai_compat":
        return bool(base_url or _os.environ.get("OPENAI_API_KEY"))
    return False


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


def _stage_failed_patch(on_heal_failure: str, patch, patches_dir, failure_ctx, cfg, click_mod) -> None:
    """Handle on_heal_failure policy for a patch that failed to fix the pipeline."""
    if on_heal_failure == "stage":
        from aqueduct.surveyor.llm import stage_patch_for_human
        stage_patch_for_human(patch, patches_dir, failure_ctx,
                              on_patch_pending_webhook=cfg.webhooks.on_patch_pending)
        click_mod.echo(
            f"  ✎ Failed patch staged for review → patches/pending/{patch.patch_id}.json",
            err=True,
        )
    # discard: do nothing
    # abort: caller handles break


def _load_env_file(env_path: "Path") -> int:
    """Load KEY=VALUE pairs from a .env file into os.environ.

    Skips blank lines and comments (#). Existing env vars are NOT overwritten.
    Returns number of variables loaded.
    """
    import os
    loaded = 0
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip("'\"")  # strip optional surrounding quotes
        if key and key not in os.environ:
            os.environ[key] = val
            loaded += 1
    return loaded


from aqueduct import __version__ as _aqueduct_version


@click.group()
@click.version_option(
    version=_aqueduct_version,
    prog_name="aqueduct",
    message="%(prog)s %(version)s",
)
@click.option("-v", "--verbose", is_flag=True, default=False, help="Enable DEBUG logging.")
@click.pass_context
def cli(ctx: click.Context, verbose: bool) -> None:
    """Aqueduct — Intelligent Spark Blueprint Engine."""
    import logging
    if verbose:
        logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(name)s: %(message)s")
    else:
        logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")


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
    """Validate aqueduct.yml without running a blueprint. Exit 0 = valid, 1 = invalid."""
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
    click.echo(f"  stores:  obs={cfg.stores.obs.path}  lineage={cfg.stores.lineage.path}  depot={cfg.stores.depot.path}")
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
@click.option(
    "--blueprint",
    "blueprint_path",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Also probe all Ingress/Egress paths and JDBC endpoints declared in this Blueprint.",
)
def doctor(config_path: str | None, skip_spark: bool, blueprint_path: str | None) -> None:
    """Probe all configured resources: config, stores, Spark, webhook, secrets, storage.

    Each check is independent. Spark check requires pyspark and may take 10-15s
    for JVM startup. Use --skip-spark to skip it in fast CI contexts.

    With --blueprint: additionally checks all Ingress/Egress sources — local path
    existence, cloud auth probes, and TCP connectivity for JDBC endpoints.

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
        blueprint_path=Path(blueprint_path) if blueprint_path else None,
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
        manifest = _compile_with_warnings(
            compiler_compile, bp, blueprint_path=Path(blueprint), execution_date=execution_date
        )
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
    help="Store directory (overrides aqueduct.yml; default: .aqueduct)",
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
    "--allow-aggressive",
    is_flag=True,
    default=False,
    help="Allow approval_mode: aggressive for this run (overrides danger.allow_aggressive_patching=false)",
)
@click.option(
    "--env-file",
    "env_file",
    default=None,
    type=click.Path(dir_okay=False),
    help="Load environment variables from a .env file before running (auto-discovers .env next to blueprint if omitted).",
)
@click.option(
    "--no-env-file",
    "no_env_file",
    is_flag=True,
    default=False,
    help="Disable automatic .env discovery.",
)
@click.option(
    "--parallel",
    is_flag=True,
    default=False,
    help="Execute independent DAG branches concurrently (one thread per connected component). "
         "Only beneficial when the Blueprint has multiple fully-independent source trees.",
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
    allow_aggressive: bool = False,
    env_file: str | None = None,
    no_env_file: bool = False,
    parallel: bool = False,
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
        _project_root = blueprint_abs.parent
        _search = blueprint_abs.parent
        for _ in range(8):
            if (_search / "aqueduct.yml").exists():
                _project_root = _search
                break
            if _search.parent == _search:
                break
            _search = _search.parent

    _original_cwd = os.getcwd()
    os.chdir(_project_root)
    try:
        # ── .env loading (after project root, before config so vars are available) ─
        if not no_env_file:
            _env_path = Path(env_file).resolve() if env_file else _project_root / ".env"
            if _env_path.exists():
                _n = _load_env_file(_env_path)
                if _n:
                    click.echo(f"  Loaded {_n} variable(s) from {_env_path}", err=True)
        # Rebind blueprint to absolute so all downstream code is CWD-agnostic.
        blueprint = str(blueprint_abs)

        # ── Load engine config ─────────────────────────────────────────────────────
        try:
            cfg = load_config(config_path_abs)
        except ConfigError as exc:
            click.echo(f"✗ config error: {exc}", err=True)
            sys.exit(1)

        # CLI flags override config file; config file overrides built-in defaults
        # Per-pipeline store paths: default .aqueduct/obs/<blueprint_id>.db instead of shared obs.db
        if store_dir_abs:
            resolved_store_dir = store_dir_abs
        else:
            _obs_path = cfg.stores.obs.path
            _default_obs = ".aqueduct/obs.db"
            if _obs_path == _default_obs:
                # Defer to after manifest is parsed (need blueprint_id) — placeholder for now
                resolved_store_dir = None  # set below after manifest
            else:
                resolved_store_dir = Path(_obs_path).parent
        # --webhook CLI flag (plain URL) overrides aqueduct.yml; config may be full WebhookEndpointConfig
        resolved_webhook = WebhookEndpointConfig(url=webhook) if webhook else cfg.webhooks.on_failure
        engine = cfg.deployment.engine
        master_url = cfg.deployment.master_url

        # ── Danger settings startup warning ──────────────────────────────────────
        danger_active = []
        if cfg.danger.allow_full_probe_actions:
            danger_active.append("allow_full_probe_actions=true")
        if cfg.danger.allow_aggressive_patching:
            danger_active.append("allow_aggressive_patching=true")
        if danger_active:
            click.echo(
                f"⚠  DANGER settings active: {', '.join(danger_active)}",
                err=True,
            )

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
            manifest = _compile_with_warnings(
                compiler_compile,
                bp,
                blueprint_path=Path(blueprint),
                depot=depot,
                execution_date=execution_date,
                secrets_provider=cfg.secrets.provider,
                secrets_region=cfg.secrets.region,
                secrets_resolver=cfg.secrets.resolver,
            )
        except CompileError as exc:
            click.echo(f"✗ compile error: {exc}", err=True)
            sys.exit(1)

        # ── Resolve per-pipeline store dir (needs blueprint_id from manifest) ────────
        if resolved_store_dir is None:
            resolved_store_dir = Path(f".aqueduct/obs/{manifest.blueprint_id}")
            resolved_store_dir.mkdir(parents=True, exist_ok=True)

        # ── Cluster-mode store path warning ───────────────────────────────────────
        if cfg.deployment.env in ("cluster", "cloud"):
            _abs = resolved_store_dir.is_absolute()
            if not _abs:
                click.echo(
                    f"WARNING: deployment.env={cfg.deployment.env!r} but store dir "
                    f"{str(resolved_store_dir)!r} is not an absolute path. "
                    "On YARN/K8s the driver CWD is ephemeral — obs.db, watermarks, "
                    "and checkpoints will be lost on driver restart. "
                    "Set stores.obs.path to an absolute shared FS path in aqueduct.yml.",
                    err=True,
                )

        # ── Aggressive mode danger gate ────────────────────────────────────────────
        if manifest.agent.approval_mode == "aggressive" and not allow_aggressive:
            if not cfg.danger.allow_aggressive_patching:
                click.echo(
                    "✗ approval_mode: aggressive requires danger.allow_aggressive_patching: true "
                    "in aqueduct.yml, or pass --allow-aggressive for this run.",
                    err=True,
                )
                sys.exit(1)

        # ── Pending patch check ────────────────────────────────────────────────────
        patches_dir = _project_root / "patches"
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

        # ── Uncommitted applied patch warning ──────────────────────────────────────
        uncommitted_applied = _uncommitted_applied_patches(Path(blueprint), patches_dir)
        if uncommitted_applied:
            n_uc = len(uncommitted_applied)
            click.echo(
                f"⚠ {n_uc} applied patch(es) not yet committed to git — "
                f"run 'aqueduct patch commit --blueprint {blueprint}'",
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
        click.echo(
            f"▶ {manifest.blueprint_id}  ({len(manifest.modules)} modules)"
            f"  run={run_id}  engine={engine}  master={master_url}"
            f"{selector_note}{exec_date_note}"
        )

        # ── Resolve agent connection (engine defaults ← blueprint overrides) ─────
        eng = cfg.agent
        bp_agent = manifest.agent
        resolved_agent_provider = bp_agent.provider or eng.provider
        resolved_agent_base_url = bp_agent.base_url or eng.base_url
        resolved_agent_model = bp_agent.model or eng.model
        resolved_agent_provider_options = bp_agent.provider_options or eng.provider_options
        resolved_agent_llm_timeout = bp_agent.llm_timeout or eng.llm_timeout
        resolved_agent_llm_max_reprompts = bp_agent.llm_max_reprompts or eng.llm_max_reprompts
        resolved_agent_engine_prompt_context = eng.prompt_context
        resolved_agent_blueprint_prompt_context = bp_agent.prompt_context

        # ── Aggressive mode disclaimer ────────────────────────────────────────────
        approval_mode = manifest.agent.approval_mode
        max_patches = manifest.agent.aggressive_max_patches
        if approval_mode == "aggressive":
            click.echo(
                f"⚠  approval_mode=aggressive — LLM will attempt up to {max_patches} patch(es). "
                f"Each patch is validated in-memory before being written to Blueprint. "
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
            session = make_spark_session(manifest.blueprint_id, merged_spark_config, master_url=master_url)
        else:
            raise NotImplementedError(f"Session creation for engine {engine!r} not implemented")

        import atexit
        atexit.register(session.stop)

        # ── Self-healing run loop ─────────────────────────────────────────────────
        patch_count = 0
        failure_ctx = None
        result = None
        last_apply_error: str | None = None  # fed back to LLM on next aggressive iteration

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
                    block_full_actions=not cfg.danger.allow_full_probe_actions,
                    parallel=parallel,
                    use_observe=cfg.metrics.use_observe,
                )
            except ExecuteError as exc:
                execute_exc = exc
                result = ExecutionResult(
                    blueprint_id=manifest.blueprint_id,
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
                if _llm_usable(resolved_agent_provider, resolved_agent_base_url):
                    click.echo(
                        "  ↻ LLM triggered by module rule (overriding approval_mode=disabled → staging patch for review)",
                        err=True,
                    )

            if effective_mode == "disabled" or failure_ctx is None:
                break

            if not _llm_usable(resolved_agent_provider, resolved_agent_base_url):
                click.echo(
                    f"  ⚠  LLM not reachable (provider={resolved_agent_provider}, no API key or base_url) — "
                    "skipping self-healing. Configure agent in aqueduct.yml or set the API key env var.",
                    err=True,
                )
                break

            if patch_count >= max_patches:
                click.echo(
                    f"⚠  LLM: aggressive_max_patches={max_patches} reached, stopping self-healing loop",
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

            # ── Generate patch ────────────────────────────────────────────────────
            from aqueduct.surveyor.llm import archive_patch, generate_llm_patch, stage_patch_for_human
            _attempt_display = (
                f"{patch_count + 1}/{max_patches}"
                if approval_mode == "aggressive"
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

            llm_result = generate_llm_patch(
                failure_ctx,
                model=resolved_agent_model,
                patches_dir=patches_dir,
                provider=resolved_agent_provider,
                base_url=resolved_agent_base_url,
                provider_options=resolved_agent_provider_options,
                llm_timeout=resolved_agent_llm_timeout,
                llm_max_reprompts=resolved_agent_llm_max_reprompts,
                engine_prompt_context=resolved_agent_engine_prompt_context,
                blueprint_prompt_context=resolved_agent_blueprint_prompt_context,
                last_apply_error=last_apply_error,
            )
            patch = llm_result.patch
            if patch is None:
                click.echo("  ✗ LLM: failed to generate valid patch, stopping", err=True)
                on_hf = manifest.agent.on_heal_failure if manifest.agent else "stage"
                if on_hf == "stage":
                    click.echo(
                        "  ↑ on_heal_failure=stage: no valid patch to stage — failure context logged in obs.db.",
                        err=True,
                    )
                break

            # ── Confidence escalation — low-confidence patches go to human ─────────
            _conf_threshold = manifest.agent.confidence_threshold
            if patch.confidence is not None and patch.confidence < _conf_threshold and effective_mode not in ("human", "disabled"):
                click.echo(
                    f"  ↑ LLM patch confidence {patch.confidence:.0%} < {_conf_threshold:.0%} — escalating to human review",
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
            except Exception:
                guardrail_err = None
            if guardrail_err:
                last_apply_error = f"Patch {patch.patch_id!r} was blocked by agent guardrail: {guardrail_err}"
                click.echo(f"  ✗ LLM patch blocked by guardrail: {guardrail_err}", err=True)
                stage_patch_for_human(patch, patches_dir, failure_ctx,
                                      on_patch_pending_webhook=cfg.webhooks.on_patch_pending)
                click.echo(
                    f"  ✎ Patch staged for human review → patches/pending/{patch.patch_id}.json",
                    err=True,
                )
                surveyor.record_healing_outcome(
                    run_id=current_run_id, failed_module=failure_ctx.failed_module,
                    failure_category=patch.category, model=resolved_agent_model,
                    patch_id=patch.patch_id, confidence=patch.confidence,
                    patch_applied=False, run_success_after_patch=False,
                )
                break

            patch_count += 1

            if effective_mode == "human":
                stage_patch_for_human(patch, patches_dir, failure_ctx,
                                      on_patch_pending_webhook=cfg.webhooks.on_patch_pending)
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
                    run_id=current_run_id, failed_module=failure_ctx.failed_module,
                    failure_category=patch.category, model=resolved_agent_model,
                    patch_id=patch.patch_id, confidence=patch.confidence,
                    patch_applied=False, run_success_after_patch=False,
                )
                break

            elif effective_mode == "ci":
                _ci_url = resolved_agent_base_url or (cfg.agent.ci_webhook_url if hasattr(cfg.agent, "ci_webhook_url") else None)
                if _ci_url:
                    try:
                        import json as _json
                        import httpx as _httpx
                        _httpx.post(_ci_url, json={
                            "patch": patch.model_dump(),
                            "run_id": current_run_id,
                            "blueprint_id": manifest.blueprint_id,
                            "failed_module": failure_ctx.failed_module,
                        }, timeout=10)
                    except Exception as _ce:
                        click.echo(f"  ⚠ ci webhook failed: {_ce}", err=True)
                stage_patch_for_human(patch, patches_dir, failure_ctx,
                                      on_patch_pending_webhook=cfg.webhooks.on_ci_patch)
                click.echo(
                    f"  ✎ CI patch staged → patches/pending/{patch.patch_id}.json",
                    err=True,
                )
                surveyor.record_healing_outcome(
                    run_id=current_run_id, failed_module=failure_ctx.failed_module,
                    failure_category=patch.category, model=resolved_agent_model,
                    patch_id=patch.patch_id, confidence=patch.confidence,
                    patch_applied=False, run_success_after_patch=False,
                )
                break

            elif effective_mode == "auto":
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
                        blueprint_id=manifest.blueprint_id,
                        run_id=str(uuid.uuid4()),
                        status="error",
                        module_results=(ModuleResult(module_id="_executor", status="error", error=str(exc)),),
                    )
                patch_success = result2.status == "success"
                failure_ctx2 = surveyor.record(result2, patched=patch_success)
                surveyor.record_healing_outcome(
                    run_id=current_run_id, failed_module=failure_ctx.failed_module,
                    failure_category=patch.category, model=resolved_agent_model,
                    patch_id=patch.patch_id, confidence=patch.confidence,
                    patch_applied=True, run_success_after_patch=patch_success,
                )
                if patch_success:
                    _write_patch_to_blueprint(patch, Path(blueprint), patches_dir, failure_ctx, mode="auto")
                    click.echo(f"  ✓ LLM patch validated and applied → {blueprint}", err=True)
                    result = result2
                    failure_ctx = failure_ctx2
                else:
                    click.echo("  ✗ LLM patch did not fix the issue, Blueprint unchanged", err=True)
                    _stage_failed_patch(
                        manifest.agent.on_heal_failure, patch, patches_dir, failure_ctx, cfg, click,
                    )
                    result = result2
                    failure_ctx = failure_ctx2
                break

            elif effective_mode == "aggressive":
                new_manifest = _apply_patch_in_memory(patch, Path(blueprint), depot, profile, cli_overrides or {})
                if new_manifest is None:
                    click.echo("  ✗ LLM patch produces invalid Blueprint, discarding", err=True)
                    last_apply_error = f"Patch {patch.patch_id!r} produced invalid Blueprint"
                    surveyor.record_healing_outcome(
                        run_id=current_run_id, failed_module=failure_ctx.failed_module,
                        failure_category=patch.category, model=resolved_agent_model,
                        patch_id=patch.patch_id, confidence=patch.confidence,
                        patch_applied=False, run_success_after_patch=False,
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
                    run_id=current_run_id, failed_module=failure_ctx.failed_module,
                    failure_category=patch.category, model=resolved_agent_model,
                    patch_id=patch.patch_id, confidence=patch.confidence,
                    patch_applied=True, run_success_after_patch=patch_success,
                )
                if patch_success:
                    _write_patch_to_blueprint(patch, Path(blueprint), patches_dir, failure_ctx, mode="aggressive")
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
                    _stage_failed_patch(
                        manifest.agent.on_heal_failure, patch, patches_dir, failure_ctx, cfg, click,
                    )
                    result = result2
                    failure_ctx = failure_ctx2
                    if manifest.agent.on_heal_failure == "abort":
                        break
                    # discard/stage: loop continues → try next patch

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

        if result.status not in ("success", "patched"):
            if failure_ctx:
                click.echo(
                    f"\n✗ blueprint failed  run_id={result.run_id}"
                    f"  failed_module={failure_ctx.failed_module}",
                    err=True,
                )
            else:
                click.echo(f"\n✗ blueprint failed  run_id={result.run_id}", err=True)
            sys.exit(1)

        # ── on_success webhook ────────────────────────────────────────────────────
        if cfg.webhooks.on_success:
            from aqueduct.surveyor.webhook import fire_webhook
            success_payload = {
                "run_id": result.run_id,
                "blueprint_id": manifest.blueprint_id,
                "blueprint_name": manifest.name,
                "module_count": str(len(result.module_results)),
            }
            fire_webhook(
                cfg.webhooks.on_success,
                full_payload=success_payload,
                template_vars=success_payload,
            )

        status_label = "patched" if result.status == "patched" else "complete"
        click.echo(f"\n✓ blueprint {status_label}  run_id={result.run_id}")
    finally:
        os.chdir(_original_cwd)


# ── patch helpers ────────────────────────────────────────────────────────────

def _uncommitted_applied_patches(blueprint_path: Path, patches_root: Path) -> list[Path]:
    """Return applied patches with applied_at newer than the last git commit for blueprint_path.

    Falls back to returning all applied patches when not in a git repo or blueprint
    has never been committed.
    """
    import subprocess

    applied_dir = patches_root / "applied"
    if not applied_dir.exists():
        return []

    all_applied = sorted(applied_dir.glob("*.json"), key=lambda f: f.stat().st_mtime)
    if not all_applied:
        return []

    # Get ISO timestamp of last git commit touching this blueprint
    result = subprocess.run(
        ["git", "log", "-1", "--format=%cI", "--", str(blueprint_path)],
        capture_output=True, text=True,
    )
    last_commit_ts: str | None = result.stdout.strip() if result.returncode == 0 else None

    if not last_commit_ts:
        # Not in git or never committed — treat everything as uncommitted
        return all_applied

    uncommitted = []
    from datetime import datetime
    for p in all_applied:
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        # applied_at may be top-level or inside _aq_meta
        applied_at_str = data.get("applied_at") or (data.get("_aq_meta") or {}).get("applied_at")
        if not applied_at_str:
            continue

        try:
            # Use fromisoformat which handles the Z and offset formats in Python 3.11+
            # For older versions, we might need to replace 'Z' with '+00:00'
            applied_at = datetime.fromisoformat(applied_at_str.replace("Z", "+00:00"))
            last_commit = datetime.fromisoformat(last_commit_ts.replace("Z", "+00:00"))

            if applied_at > last_commit:
                uncommitted.append(p)
        except (ValueError, TypeError):
            # Fallback to string comparison if parsing fails for some reason
            if applied_at_str > last_commit_ts:
                uncommitted.append(p)

    return uncommitted


# ── patch helpers ────────────────────────────────────────────────────────────

def _patches_root_from_blueprint(blueprint_path: Path) -> Path:
    """Return <project_root>/patches by walking up from blueprint to find aqueduct.yml."""
    _search = blueprint_path.parent
    project_root = blueprint_path.parent
    for _ in range(8):
        if (_search / "aqueduct.yml").exists():
            project_root = _search
            break
        if _search.parent == _search:
            break
        _search = _search.parent
    return project_root / "patches"


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
    default=None,
    help="Root directory for patch lifecycle subdirs (default: <blueprint-dir>/patches)",
)
def patch_apply(patch_file: str, blueprint: str, patches_dir: str | None) -> None:
    """Validate and apply a PatchSpec JSON file to a Blueprint YAML.

    Backs up the original Blueprint, applies all operations atomically,
    verifies the result parses cleanly, then archives the patch.
    """
    from pathlib import Path

    from aqueduct.patch.apply import PatchError, apply_patch_file

    blueprint_path = Path(blueprint)
    patches_root = Path(patches_dir) if patches_dir else _patches_root_from_blueprint(blueprint_path)

    try:
        result = apply_patch_file(
            blueprint_path=blueprint_path,
            patch_path=Path(patch_file),
            patches_dir=patches_root,
        )
    except PatchError as exc:
        click.echo(f"✗ patch failed: {exc}", err=True)
        sys.exit(1)

    click.echo(f"✓ patch applied  id={result.patch_id}")
    click.echo(f"  blueprint  → {result.blueprint_path}")
    click.echo(f"  archived   → {result.archive_path}")
    click.echo(f"  operations   {result.operations_applied} applied")
    click.echo(f"  commit with: aqueduct patch commit --blueprint {blueprint}")


@patch.command("reject")
@click.argument("patch_ref")
@click.option("--reason", required=True, help="Rejection reason (recorded in patch file)")
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: derived from patch file path or CWD/patches)",
)
def patch_reject(patch_ref: str, reason: str, patches_dir: str | None) -> None:
    """Reject a pending patch and record the reason.

    PATCH_REF can be a file path (patches/pending/00001_*.json) or a bare patch_id slug.

    Moves patches/pending/<file> → patches/rejected/<file> with a rejection_reason annotation.
    """
    from pathlib import Path

    from aqueduct.patch.apply import PatchError, reject_patch

    # Accept either a file path or a bare patch_id slug.
    ref_path = Path(patch_ref)
    if ref_path.suffix == ".json" and ref_path.exists():
        # Full path given — derive patches_dir from grandparent (pending/ → patches/)
        resolved_patches_dir = ref_path.parent.parent
        patch_id = ref_path.stem
    elif ref_path.suffix == ".json" and not ref_path.exists() and ref_path.parent.name == "pending":
        # Path given but file not found via CWD — try same derivation
        resolved_patches_dir = ref_path.parent.parent
        patch_id = ref_path.stem
    else:
        resolved_patches_dir = Path(patches_dir) if patches_dir else _patches_root_from_blueprint(Path.cwd() / "_sentinel")
        patch_id = patch_ref

    try:
        rejected_path = reject_patch(
            patch_id=patch_id,
            reason=reason,
            patches_dir=resolved_patches_dir,
        )
    except PatchError as exc:
        click.echo(f"✗ reject failed: {exc}", err=True)
        sys.exit(1)

    click.echo(f"✓ patch rejected  id={patch_id}")
    click.echo(f"  archived → {rejected_path}")
    click.echo(f"  reason: {reason}")


@patch.command("commit")
@click.option(
    "--blueprint",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML file to commit",
)
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: <blueprint-dir>/patches)",
)
def patch_commit(blueprint: str, patches_dir: str | None) -> None:
    """Commit applied patches to git with a structured commit message.

    Finds applied patches newer than the last git commit for this Blueprint,
    then runs: git add <blueprint> && git commit.
    """
    import subprocess
    from pathlib import Path

    blueprint_path = Path(blueprint)
    patches_root = Path(patches_dir) if patches_dir else _patches_root_from_blueprint(blueprint_path)

    uncommitted = _uncommitted_applied_patches(blueprint_path, patches_root)
    if not uncommitted:
        click.echo("Nothing to commit — no applied patches since last git commit.")
        return

    # Parse blueprint_id
    try:
        from aqueduct.parser.parser import parse as _parse
        bp = _parse(blueprint)
        blueprint_id = bp.id
    except Exception:
        blueprint_id = blueprint_path.stem

    # Build commit message
    patch_lines: list[str] = []
    all_ops: list[str] = []
    run_id: str | None = None
    rationales: list[str] = []

    for p in uncommitted:
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            data = {}
        rat = data.get("rationale", "")
        if rat:
            rationales.append(rat)
        ops = [op.get("op", "?") for op in data.get("operations", [])]
        all_ops.extend(ops)
        meta = data.get("_aq_meta", {})
        if not run_id:
            run_id = meta.get("run_id") or data.get("run_id")
        patch_lines.append(f"  - {p.stem}: {rat or '(no rationale)'}")

    n = len(uncommitted)
    summary = rationales[0] if n == 1 and rationales else f"{n} patches applied"
    combined_rationale = "\n".join(rationales) if rationales else ""
    ops_str = ", ".join(dict.fromkeys(all_ops))  # deduplicated, ordered

    aqueduct_block = "---aqueduct---\npatches:\n" + "\n".join(patch_lines)
    if run_id:
        aqueduct_block += f"\nrun_id: {run_id}"
    if ops_str:
        aqueduct_block += f"\nops: {ops_str}"
    aqueduct_block += "\n---"

    commit_msg = f"fix(aqueduct/{blueprint_id}): {summary}"
    if combined_rationale:
        commit_msg += f"\n\n{combined_rationale}"
    commit_msg += f"\n\n{aqueduct_block}"

    add = subprocess.run(["git", "add", blueprint_path.name], capture_output=True, cwd=blueprint_path.parent or None)
    if add.returncode != 0:
        click.echo(f"✗ git add failed: {add.stderr.decode().strip()}", err=True)
        sys.exit(1)

    commit = subprocess.run(
        ["git", "commit", "-m", commit_msg],
        capture_output=True, text=True, cwd=blueprint_path.parent or None,
    )
    if commit.returncode != 0:
        click.echo(f"✗ git commit failed: {commit.stderr.strip()}", err=True)
        sys.exit(1)

    short_hash = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        capture_output=True, text=True, cwd=blueprint_path.parent or None,
    ).stdout.strip()

    click.echo(f"✓ committed {n} patch(es)  [{short_hash}]  {blueprint_id}")
    for p in uncommitted:
        click.echo(f"  {p.name}")


@patch.command("discard")
@click.option(
    "--blueprint",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML file to restore from git HEAD",
)
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: <blueprint-dir>/patches)",
)
def patch_discard(blueprint: str, patches_dir: str | None) -> None:
    """Discard applied patches — restore Blueprint to last git commit.

    Runs: git checkout HEAD -- <blueprint>
    Moves uncommitted applied patches back to patches/pending/.
    """
    import subprocess
    from pathlib import Path

    blueprint_path = Path(blueprint)
    patches_root = Path(patches_dir) if patches_dir else _patches_root_from_blueprint(blueprint_path)

    uncommitted = _uncommitted_applied_patches(blueprint_path, patches_root)

    restore = subprocess.run(
        ["git", "checkout", "HEAD", "--", blueprint_path.name],
        capture_output=True, text=True, cwd=blueprint_path.parent or None,
    )
    if restore.returncode != 0:
        click.echo(f"✗ git checkout failed: {restore.stderr.strip()}", err=True)
        sys.exit(1)

    click.echo(f"✓ blueprint restored to HEAD: {blueprint_path}")

    pending_dir = patches_root / "pending"
    pending_dir.mkdir(parents=True, exist_ok=True)
    moved = 0
    for patch_file in uncommitted:
        dest = pending_dir / patch_file.name
        try:
            patch_file.rename(dest)
            moved += 1
        except OSError:
            pass

    if moved:
        click.echo(f"  moved {moved} applied patch(es) back to patches/pending/")
        click.echo(f"  re-apply with: aqueduct patch apply patches/pending/<file> --blueprint {blueprint}")


@patch.command("list")
@click.option(
    "--blueprint",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML file (used to locate patches/ dir)",
)
@click.option(
    "--patches-dir",
    default=None,
    help="Root directory for patch lifecycle subdirs (default: <blueprint-dir>/patches or CWD/patches)",
)
@click.option(
    "--status",
    "filter_status",
    default="pending",
    type=click.Choice(["pending", "applied", "rejected", "all"]),
    show_default=True,
    help="Which lifecycle directory to list",
)
def patch_list(blueprint: str | None, patches_dir: str | None, filter_status: str) -> None:
    """List patches, showing metadata for each.

    Defaults to showing pending patches. Use --status=applied/rejected/all for other dirs.
    """
    from pathlib import Path

    if patches_dir:
        patches_root = Path(patches_dir)
    elif blueprint:
        patches_root = _patches_root_from_blueprint(Path(blueprint))
    else:
        patches_root = _patches_root_from_blueprint(Path.cwd() / "_sentinel")

    dirs_to_show: list[tuple[str, Path]] = []
    if filter_status == "all":
        for sub in ("pending", "applied", "rejected"):
            d = patches_root / sub
            if d.exists():
                dirs_to_show.append((sub, d))
    else:
        d = patches_root / filter_status
        if d.exists():
            dirs_to_show.append((filter_status, d))

    total = 0
    for status_label, d in dirs_to_show:
        files = sorted(d.glob("*.json"), key=lambda f: f.name)
        if not files:
            continue

        click.echo(f"\n  [{status_label}]  {d}")
        click.echo(f"  {'file':<55} {'patch_id':<36} {'rationale'}")
        click.echo(f"  {'-'*55} {'-'*36} {'-'*40}")

        for f in files:
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
            except Exception:
                data = {}
            pid = data.get("patch_id", f.stem)
            rationale = (data.get("rationale") or "").replace("\n", " ")[:60]
            click.echo(f"  {f.name:<55} {pid:<36} {rationale}")
            total += 1

    if total == 0:
        click.echo(f"No {filter_status} patches found in {patches_root}")
        return

    if filter_status == "pending":
        click.echo(f"\n  Apply: aqueduct patch apply patches/pending/<file> --blueprint <blueprint.yml>")
        click.echo(f"  Reject: aqueduct patch reject patches/pending/<file> --reason '<reason>'")


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
    from aqueduct.executor.spark.session import make_spark_session
    from aqueduct.executor.spark.test_runner import TestSchemaError, run_test_file

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
    except TestSchemaError as exc:
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
    help="Store directory (default: aqueduct.yml or .aqueduct)",
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

    resolved = Path(store_dir) if store_dir else Path(cfg.stores.obs.path).parent
    obs_db = resolved / "obs.db"
    if not obs_db.exists():
        click.echo(f"✗ obs.db not found at {obs_db}", err=True)
        sys.exit(1)

    conn = _duckdb.connect(str(obs_db), read_only=True)
    try:
        row = conn.execute(
            """
            SELECT run_id, blueprint_id, status,
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
        click.echo(f"✗ run {run_id!r} not found in {obs_db}", err=True)
        sys.exit(1)

    run_id_val, blueprint_id, status, started_at, finished_at, module_results_raw = row
    module_results = json.loads(module_results_raw) if isinstance(module_results_raw, str) else (module_results_raw or [])

    if fmt == "json":
        out = {
            "run_id": run_id_val,
            "blueprint_id": blueprint_id,
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
        writer.writerow(["run_id", "blueprint_id", "status", "started_at", "finished_at"])
        writer.writerow([run_id_val, blueprint_id, status, started_at, finished_at])
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
    click.echo(f"{status_icon} run_id={run_id_val}  blueprint={blueprint_id}  status={status}")
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


# ── aqueduct runs ─────────────────────────────────────────────────────────────

@cli.command("runs")
@click.option("--blueprint", default=None, metavar="PATH_OR_ID", help="Filter by blueprint file path or blueprint ID")
@click.option("--failed", is_flag=True, default=False, help="Show only failed runs")
@click.option("--last", "limit", default=20, show_default=True, help="Max rows to show")
@click.option("--store-dir", default=None, help="Store directory (default: .aqueduct)")
@click.option("--config", "config_path", default=None, metavar="PATH", help="Path to aqueduct.yml")
def runs(blueprint: str | None, failed: bool, limit: int, store_dir: str | None, config_path: str | None) -> None:
    """List recent blueprint runs."""
    import duckdb as _duckdb
    from aqueduct.config import load_config

    cfg = load_config(Path(config_path) if config_path else None)
    resolved = Path(store_dir) if store_dir else Path(cfg.stores.obs.path).parent
    obs_db = resolved / "obs.db"
    if not obs_db.exists():
        click.echo(f"No runs found (obs.db not at {obs_db})")
        return

    blueprint_id: str | None = None
    if blueprint:
        arg_path = Path(blueprint)
        if arg_path.suffix in (".yml", ".yaml") and arg_path.exists():
            from aqueduct.parser.parser import ParseError, parse
            try:
                bp = parse(str(arg_path))
                blueprint_id = bp.id
            except ParseError:
                blueprint_id = blueprint
        else:
            blueprint_id = blueprint

    conn = _duckdb.connect(str(obs_db), read_only=True)
    try:
        where_parts = []
        params: list = []
        if blueprint_id:
            where_parts.append("blueprint_id = ?")
            params.append(blueprint_id)
        if failed:
            where_parts.append("status = 'error'")
        where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
        params.append(limit)

        rows = conn.execute(
            f"""
            SELECT run_id, blueprint_id, status, started_at, finished_at,
                   json_extract_string(module_results, '$[0].module_id') AS first_failed
            FROM run_records
            {where}
            ORDER BY started_at DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        click.echo("No runs found.")
        return

    click.echo(f"  {'run_id':<38} {'blueprint':<30} {'status':<10} {'started':<22} {'failed_module'}")
    click.echo(f"  {'-'*38} {'-'*30} {'-'*10} {'-'*22} {'-'*20}")
    for run_id_val, bp_id, status, started_at, finished_at, first_failed in rows:
        icon = "✓" if status == "success" else ("↻" if status == "running" else "✗")
        failed_col = (first_failed or "") if status == "error" else ""
        click.echo(f"  {icon} {run_id_val:<37} {bp_id:<30} {status:<10} {str(started_at)[:19]:<22} {failed_col}")


# ── aqueduct lineage ──────────────────────────────────────────────────────────

@cli.command()
@click.argument("blueprint_id_or_blueprint")
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
    blueprint_id_or_blueprint: str,
    store_dir: str | None,
    config_path: str | None,
    from_table: str | None,
    column_filter: str | None,
    fmt: str,
) -> None:
    """Print column-level lineage graph for a blueprint.

    PIPELINE_ID_OR_BLUEPRINT: blueprint id (e.g. nyc_taxi_demo) or path to
    the blueprint YAML file (e.g. blueprint.yml — id is extracted automatically).
    """
    import duckdb as _duckdb

    from aqueduct.config import ConfigError, load_config

    try:
        cfg = load_config(Path(config_path) if config_path else None)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(1)

    # Accept blueprint file path — extract blueprint id from it
    arg_path = Path(blueprint_id_or_blueprint)
    if arg_path.suffix in (".yml", ".yaml") and arg_path.exists():
        try:
            from aqueduct.parser.parser import parse
            bp = parse(str(arg_path))
            blueprint_id = bp.id
        except Exception as exc:
            click.echo(f"✗ could not read blueprint id from {blueprint_id_or_blueprint!r}: {exc}", err=True)
            sys.exit(1)
    else:
        blueprint_id = blueprint_id_or_blueprint

    resolved = Path(store_dir) if store_dir else Path(cfg.stores.obs.path).parent
    lineage_db = resolved / "lineage.db"
    if not lineage_db.exists():
        click.echo(f"✗ lineage.db not found at {lineage_db}", err=True)
        sys.exit(1)

    params: list[Any] = [blueprint_id]
    where_parts = ["blueprint_id = ?"]
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
        click.echo(f"No lineage records found for blueprint {blueprint_id!r}.")
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

    click.echo(f"Column lineage — blueprint: {blueprint_id}")
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
        resolved = Path(store_dir) if store_dir else Path(cfg.stores.obs.path).parent
        obs_db = resolved / "obs.db"
        conn = _duckdb.connect(str(obs_db))
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

    resolved = Path(store_dir) if store_dir else Path(cfg.stores.obs.path).parent
    resolved.mkdir(parents=True, exist_ok=True)
    obs_db = resolved / "obs.db"

    now = datetime.now(tz=timezone.utc).isoformat()
    conn = _duckdb.connect(str(obs_db))
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

def _print_prompt(prompt: dict, fmt: str) -> None:
    """Print system+user prompt to stdout in the requested format."""
    if fmt == "json":
        click.echo(json.dumps(prompt, indent=2))
    else:
        sep = "─" * 72
        click.echo(f"## SYSTEM PROMPT\n{sep}")
        click.echo(prompt["system"])
        click.echo(f"\n## USER PROMPT\n{sep}")
        click.echo(prompt["user"])


@cli.command()
@click.argument("run_id", required=False, default=None)
@click.option(
    "--scenario",
    "scenario_path",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Run scenario-based healing from a .aqscenario.yml file instead of a live run_id",
)
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
@click.option(
    "--print-prompt",
    "print_prompt",
    is_flag=True,
    default=False,
    help="Print the LLM prompt that would be sent and exit without calling the model.",
)
@click.option(
    "--print-prompt-format",
    "print_prompt_format",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="Output format for --print-prompt.",
)
def heal(
    run_id: str | None,
    scenario_path: str | None,
    module_id: str | None,
    store_dir: str | None,
    config_path: str | None,
    patches_dir: str,
    print_prompt: bool,
    print_prompt_format: str,
) -> None:
    """Manually trigger LLM self-healing for a failed run or scenario.

    Two modes:

    \b
    Live run:      aqueduct heal <run_id>
    Scenario test: aqueduct heal --scenario path/to/scenario.aqscenario.yml

    In live mode, reads the FailureContext from the observability store.
    In scenario mode, builds a synthetic FailureContext from the scenario file
    (no Spark required) and validates the LLM response against expected assertions.
    """
    from aqueduct.config import ConfigError, load_config
    from aqueduct.surveyor.llm import build_prompt, generate_llm_patch, stage_patch_for_human

    if not run_id and not scenario_path:
        click.echo(
            "✗ provide either a run_id argument or --scenario <path>",
            err=True,
        )
        sys.exit(1)

    try:
        cfg = load_config(Path(config_path) if config_path else None)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(1)

    eng = cfg.agent
    resolved_provider = eng.provider
    resolved_base_url = eng.base_url
    resolved_model = eng.model
    resolved_provider_options = eng.provider_options
    resolved_llm_timeout = eng.llm_timeout
    resolved_llm_max_reprompts = eng.llm_max_reprompts
    resolved_engine_prompt_context = eng.prompt_context

    if resolved_model is None and not print_prompt:
        click.echo(
            "✗ no LLM agent configured — set agent.model in aqueduct.yml",
            err=True,
        )
        sys.exit(1)

    patches_path = Path(patches_dir)

    # ── Scenario mode ─────────────────────────────────────────────────────────
    if scenario_path:
        from aqueduct.surveyor.scenario import load_scenario, run_scenario, _build_failure_ctx

        try:
            scenario = load_scenario(Path(scenario_path))
        except Exception as exc:
            click.echo(f"✗ failed to load scenario: {exc}", err=True)
            sys.exit(1)

        if print_prompt:
            try:
                failure_ctx = _build_failure_ctx(scenario)
            except Exception as exc:
                click.echo(f"✗ failed to build failure context from scenario: {exc}", err=True)
                sys.exit(1)
            prompt = build_prompt(failure_ctx, patches_path, resolved_engine_prompt_context)
            _print_prompt(prompt, print_prompt_format)
            return

        click.echo(
            f"↻ heal scenario  id={scenario.id}  "
            f"provider={resolved_provider}  model={resolved_model}"
        )

        result = run_scenario(
            scenario,
            model=resolved_model,
            patches_dir=patches_path,
            provider=resolved_provider or "anthropic",
            base_url=resolved_base_url,
            provider_options=resolved_provider_options,
            llm_timeout=resolved_llm_timeout,
            llm_max_reprompts=resolved_llm_max_reprompts,
            engine_prompt_context=resolved_engine_prompt_context,
        )

        status = "PASS" if result.passed else "FAIL"
        conf = f"  confidence={result.confidence:.2f}" if result.confidence is not None else ""
        att = f"  attempts={result.attempts_to_parse}" if result.attempts_to_parse else ""
        click.echo(f"{status}  scenario={scenario.id}  {result.duration_seconds:.1f}s{conf}{att}")

        if result.reprompt_errors:
            for i, err in enumerate(result.reprompt_errors, 1):
                click.echo(f"  reprompt {i}: {err}", err=True)

        if result.failures:
            for f in result.failures:
                click.echo(f"  ✗ {f}", err=True)
            sys.exit(1)

        click.echo("  All assertions passed.")
        return

    # ── Live run mode ─────────────────────────────────────────────────────────
    import duckdb as _duckdb
    from aqueduct.surveyor.models import FailureContext

    resolved = Path(store_dir) if store_dir else Path(cfg.stores.obs.path).parent
    obs_db = resolved / "obs.db"
    if not obs_db.exists():
        click.echo(f"✗ obs.db not found at {obs_db}", err=True)
        sys.exit(1)

    conn = _duckdb.connect(str(obs_db), read_only=True)
    try:
        fc_row = conn.execute(
            """
            SELECT run_id, blueprint_id, failed_module, error_message,
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
        fc_run_id, blueprint_id, failed_module, error_message,
        stack_trace, manifest_json_raw, started_at, finished_at,
    ) = fc_row

    target_module = module_id or failed_module

    failure_ctx = FailureContext(
        run_id=fc_run_id,
        blueprint_id=blueprint_id,
        failed_module=target_module,
        error_message=error_message,
        stack_trace=stack_trace,
        manifest_json=manifest_json_raw if isinstance(manifest_json_raw, str) else json.dumps(manifest_json_raw),
        started_at=started_at,
        finished_at=finished_at,
    )

    if print_prompt:
        prompt = build_prompt(failure_ctx, patches_path, resolved_engine_prompt_context)
        _print_prompt(prompt, print_prompt_format)
        return

    click.echo(
        f"↻ heal  run={run_id}  module={target_module}  "
        f"provider={resolved_provider}  model={resolved_model}"
    )

    llm_result = generate_llm_patch(
        failure_ctx,
        model=resolved_model,
        patches_dir=patches_path,
        provider=resolved_provider or "anthropic",
        base_url=resolved_base_url,
        provider_options=resolved_provider_options,
        llm_timeout=resolved_llm_timeout,
        llm_max_reprompts=resolved_llm_max_reprompts,
        engine_prompt_context=resolved_engine_prompt_context,
    )
    patch = llm_result.patch

    if patch is None:
        click.echo(
            f"✗ LLM failed to produce a valid patch after {llm_result.attempts} attempt(s)",
            err=True,
        )
        for err in llm_result.reprompt_errors:
            click.echo(f"  · {err}", err=True)
        sys.exit(1)

    stage_patch_for_human(patch, patches_path, failure_ctx)
    click.echo(f"✓ patch staged → {patches_path}/pending/{patch.patch_id}.json")
    click.echo(f"  apply with: aqueduct patch apply patches/pending/{patch.patch_id}.json --blueprint <path>")


# ── aqueduct benchmark ────────────────────────────────────────────────────────

@cli.command()
@click.option(
    "--scenarios",
    "scenarios_dir",
    required=True,
    type=click.Path(exists=True, file_okay=False),
    help="Directory containing .aqscenario.yml files (searched recursively)",
)
@click.option(
    "--model",
    "models",
    multiple=True,
    default=None,
    help="Model to benchmark (repeatable: --model A --model B). Defaults to agent.model in aqueduct.yml",
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
    help="Root directory for patch lifecycle (for previous-patch history)",
)
@click.option(
    "--output",
    "fmt",
    default="table",
    type=click.Choice(["table", "json"]),
    show_default=True,
    help="Output format",
)
@click.option(
    "--workers",
    default=4,
    show_default=True,
    help="Max concurrent LLM calls (set to 1 for serial execution)",
)
def benchmark(
    scenarios_dir: str,
    models: tuple[str, ...],
    config_path: str | None,
    patches_dir: str,
    fmt: str,
    workers: int,
) -> None:
    """Run scenario suite against one or more LLM models and compare results.

    Example:
      aqueduct benchmark --scenarios scenarios/ --model claude-opus-4-7 --model llama3

    Discovers all .aqscenario.yml files recursively in the given directory,
    runs each against every specified model, and prints a comparison table.
    No Spark required — scenarios inject failures synthetically.
    """
    from aqueduct.config import ConfigError, load_config
    from aqueduct.surveyor.scenario import format_benchmark_table, run_benchmark

    try:
        cfg = load_config(Path(config_path) if config_path else None)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(1)

    eng = cfg.agent
    resolved_provider = eng.provider
    resolved_base_url = eng.base_url
    resolved_model = eng.model
    resolved_provider_options = eng.provider_options
    resolved_llm_timeout = eng.llm_timeout
    resolved_llm_max_reprompts = eng.llm_max_reprompts
    resolved_engine_prompt_context = eng.prompt_context

    model_list = list(models) if models else ([resolved_model] if resolved_model else None)
    if not model_list:
        click.echo(
            "✗ no models specified — use --model <model> or set agent.model in aqueduct.yml",
            err=True,
        )
        sys.exit(1)

    click.echo(
        f"↻ benchmark  scenarios={scenarios_dir}  "
        f"models={model_list}  provider={resolved_provider}"
    )

    results = run_benchmark(
        scenarios_dir=Path(scenarios_dir),
        models=model_list,
        patches_dir=Path(patches_dir),
        provider=resolved_provider or "anthropic",
        base_url=resolved_base_url,
        provider_options=resolved_provider_options,
        llm_timeout=resolved_llm_timeout,
        llm_max_reprompts=resolved_llm_max_reprompts,
        engine_prompt_context=resolved_engine_prompt_context,
        workers=workers,
    )

    if fmt == "json":
        output: dict = {}
        for sid, model_results in results.items():
            output[sid] = {}
            for model, r in model_results.items():
                output[sid][model] = {
                    "passed": r.passed,
                    "patch_valid": r.patch_valid,
                    "patch_applies": r.patch_applies,
                    "confidence": r.confidence,
                    "duration_seconds": r.duration_seconds,
                    "attempts_to_parse": r.attempts_to_parse,
                    "reprompt_errors": r.reprompt_errors,
                    "root_cause_match": r.root_cause_match,
                    "category_match": r.category_match,
                    "failures": r.failures,
                }
        click.echo(json.dumps(output, indent=2))
    else:
        click.echo(format_benchmark_table(results, model_list))

    total = sum(
        1 for model_results in results.values()
        for r in model_results.values()
    )
    passed = sum(
        1 for model_results in results.values()
        for r in model_results.values()
        if r.passed
    )
    failed = total - passed
    if failed:
        sys.exit(1)


# ── aqueduct log ─────────────────────────────────────────────────────────────

@cli.command("log")
@click.argument("blueprint", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["table", "json"]),
    default="table",
    show_default=True,
)
def log_cmd(blueprint: str, fmt: str) -> None:
    """Show git commit history for a Blueprint with Aqueduct patch metadata.

    Parses ---aqueduct--- blocks from commit messages.  Manual commits (no
    block) are shown as '(manual change)'.
    """
    import re
    import subprocess

    blueprint_path = Path(blueprint)

    result = subprocess.run(
        ["git", "log", "--follow", "--format=%H\x1f%ci\x1f%s\x1f%B\x1eENDCOMMIT", "--", str(blueprint_path)],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        click.echo(f"✗ git log failed: {result.stderr.strip()}", err=True)
        sys.exit(1)

    raw = result.stdout.strip()
    if not raw:
        click.echo("No git history for this blueprint.")
        return

    _AQ_BLOCK_RE = re.compile(r"---aqueduct---(.*?)---", re.DOTALL)
    _PATCH_LINE_RE = re.compile(r"^\s*-\s+(\S+):\s*(.*)", re.MULTILINE)

    entries = []
    for commit_raw in raw.split("\x1eENDCOMMIT"):
        commit_raw = commit_raw.strip()
        if not commit_raw:
            continue
        # First line is the \x1f-separated header
        header_line, _, body = commit_raw.partition("\n")
        parts = header_line.split("\x1f")
        if len(parts) < 3:
            continue
        commit_hash = parts[0].strip()
        commit_date = parts[1].strip()
        subject = parts[2].strip()

        aq_match = _AQ_BLOCK_RE.search(body)
        if aq_match:
            block = aq_match.group(1)
            patch_ids = [m.group(1) for m in _PATCH_LINE_RE.finditer(block)]
            ops_match = re.search(r"^ops:\s*(.+)", block, re.MULTILINE)
            ops = ops_match.group(1).strip() if ops_match else ""
            run_match = re.search(r"^run_id:\s*(\S+)", block, re.MULTILINE)
            run_id = run_match.group(1) if run_match else ""
        else:
            patch_ids = []
            ops = ""
            run_id = ""

        entries.append({
            "hash": commit_hash[:8],
            "date": commit_date[:19],
            "subject": subject,
            "patches": ", ".join(patch_ids) if patch_ids else "(manual change)",
            "ops": ops,
            "run_id": run_id,
        })

    if fmt == "json":
        click.echo(json.dumps(entries, indent=2))
        return

    if not entries:
        click.echo("No commits found.")
        return

    click.echo(f"  {'hash':<10} {'date':<20} {'patches':<40} {'ops'}")
    click.echo(f"  {'-'*10} {'-'*20} {'-'*40} {'-'*30}")
    for e in entries:
        patches_col = e["patches"][:38] + ".." if len(e["patches"]) > 40 else e["patches"]
        click.echo(f"  {e['hash']:<10} {e['date']:<20} {patches_col:<40} {e['ops']}")


# ── aqueduct rollback ─────────────────────────────────────────────────────────

@cli.command("rollback")
@click.argument("blueprint", type=click.Path(exists=True, dir_okay=False))
@click.option("--to", "patch_id", required=True, help="Revert the git commit containing this patch_id")
def rollback_cmd(blueprint: str, patch_id: str) -> None:
    """Revert a Blueprint file to its state before a specific patch was applied.

    Restores only the blueprint file (and any arcade blueprints touched in the
    same commit) by checking out the pre-patch file content from git history,
    then creates a new forward commit. Never rewrites history or touches other
    files in the repository.
    """
    import subprocess

    blueprint_path = Path(blueprint)
    cwd = blueprint_path.parent or Path.cwd()

    # Walk git log scoped to this blueprint file to find the target commit
    result = subprocess.run(
        ["git", "log", "--follow", "--format=%H\x1f%B\x1eENDCOMMIT", "--", str(blueprint_path)],
        capture_output=True, text=True, cwd=cwd,
    )
    if result.returncode != 0:
        click.echo(f"✗ git log failed: {result.stderr.strip()}", err=True)
        sys.exit(1)

    target_hash: str | None = None
    for commit_raw in result.stdout.split("\x1eENDCOMMIT"):
        commit_raw = commit_raw.strip()
        if not commit_raw:
            continue
        header_line, _, body = commit_raw.partition("\n")
        commit_hash = header_line.split("\x1f")[0].strip()
        if patch_id in body:
            target_hash = commit_hash
            break

    if not target_hash:
        click.echo(
            f"✗ patch_id {patch_id!r} not found in git history for {blueprint}\n"
            "  Use 'aqueduct log <blueprint>' to list available patch_ids.",
            err=True,
        )
        sys.exit(1)

    # Resolve the commit immediately before the patch
    parent = subprocess.run(
        ["git", "rev-parse", f"{target_hash}~1"],
        capture_output=True, text=True, cwd=cwd,
    )
    if parent.returncode != 0:
        click.echo(f"✗ could not resolve parent commit: {parent.stderr.strip()}", err=True)
        sys.exit(1)
    parent_hash = parent.stdout.strip()

    # Discover all blueprint files touched by the patch commit (handles arcades)
    diff_files = subprocess.run(
        ["git", "diff-tree", "--no-commit-id", "-r", "--name-only", target_hash],
        capture_output=True, text=True, cwd=cwd,
    )
    if diff_files.returncode != 0:
        click.echo(f"✗ could not list files in commit: {diff_files.stderr.strip()}", err=True)
        sys.exit(1)

    touched_files = [f.strip() for f in diff_files.stdout.splitlines() if f.strip()]
    if not touched_files:
        click.echo(f"✗ commit {target_hash[:8]} has no file changes", err=True)
        sys.exit(1)

    # Restore each file to its pre-patch state (file-scoped, non-destructive)
    for rel_path in touched_files:
        restore = subprocess.run(
            ["git", "checkout", parent_hash, "--", rel_path],
            capture_output=True, text=True, cwd=cwd,
        )
        if restore.returncode != 0:
            click.echo(f"✗ git checkout {rel_path} failed: {restore.stderr.strip()}", err=True)
            sys.exit(1)

    # Stage restored files and create a forward revert commit
    add = subprocess.run(
        ["git", "add", "--"] + touched_files,
        capture_output=True, text=True, cwd=cwd,
    )
    if add.returncode != 0:
        click.echo(f"✗ git add failed: {add.stderr.strip()}", err=True)
        sys.exit(1)

    commit_msg = (
        f"revert(aqueduct): roll back patch {patch_id!r}\n\n"
        f"Restores {', '.join(touched_files)} to state before commit {target_hash[:8]}."
    )
    commit = subprocess.run(
        ["git", "commit", "-m", commit_msg],
        capture_output=True, text=True, cwd=cwd,
    )
    if commit.returncode != 0:
        click.echo(f"✗ git commit failed: {commit.stderr.strip()}", err=True)
        sys.exit(1)

    short = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        capture_output=True, text=True, cwd=cwd,
    ).stdout.strip()

    click.echo(f"✓ rolled back patch {patch_id!r}  [{short}]")
    for f in touched_files:
        click.echo(f"  restored  {f}  (from {parent_hash[:8]})")


# ── aqueduct init ─────────────────────────────────────────────────────────────


@cli.command("init")
@click.option("--name", default=None, help="Project name (defaults to current directory name)")
def init(name: str | None) -> None:
    """Scaffold a new Aqueduct project in the current directory."""
    import importlib.resources
    import subprocess

    cwd = Path.cwd()
    project_name = name or cwd.name

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
    _mkdir(cwd / "tests")
    _mkdir(cwd / "benchmarks")
    _mkdir(cwd / "patches" / "pending")
    _mkdir(cwd / "patches" / "rejected")

    # Templates
    _copy_template("aqueduct.yml.template", cwd / "aqueduct.yml.template")
    _copy_template(
        "blueprints/blueprint.yml.template", cwd / "blueprints" / "blueprint.yml.template"
    )
    _copy_template("tests/aqtest.yml.template", cwd / "tests" / "aqtest.yml.template")
    _copy_template(
        "benchmarks/aqscenario.yml.template", cwd / "benchmarks" / "aqscenario.yml.template"
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
    click.echo("  1. Create blueprints/<name>.yml  (see aqueduct.template.yml for reference)")
    click.echo("  2. aqueduct validate blueprints/<name>.yml")
    click.echo("  3. aqueduct run blueprints/<name>.yml")


if __name__ == "__main__":
    cli()