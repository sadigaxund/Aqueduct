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


def _apply_warnings_from_cfg(cfg) -> None:
    """Merge `cfg.warnings` with the CLI flags installed by the root group.

    Idempotent. Call once per command immediately after `load_config()` so the
    engine-level `warnings.suppress` from aqueduct.yml is honoured alongside
    any `--suppress-warning` flags the user passed. Use `*` to silence all.
    """
    from aqueduct.warnings import _DEFAULT_SUPPRESS, set_default_suppress
    merged = set(_DEFAULT_SUPPRESS) | set(getattr(cfg.warnings, "suppress", []) or [])
    set_default_suppress(suppress=merged)


def _compile_with_warnings(compile_fn, *args, **kwargs):
    """Call compile_fn, intercept warnings, reprint as clean CLI output.

    Aqueduct's own diagnostics (AqueductWarning category, prefix
    `[aqueduct:rule_id] `) become `AQ-WARN [rule_id] <msg>` lines so the
    rule_id is easy to copy into `warnings.suppress` in aqueduct.yml.
    Non-Aqueduct UserWarnings fall back to the legacy `WARNING:` prefix.
    """
    from aqueduct.warnings import AqueductWarning
    _AQ_PREFIX = "[aqueduct:"
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = compile_fn(*args, **kwargs)
    for w in caught:
        msg = str(w.message)
        if issubclass(w.category, AqueductWarning) and msg.startswith(_AQ_PREFIX):
            body = msg[len(_AQ_PREFIX):]
            try:
                rid, rest = body.split("] ", 1)
                click.echo(f"AQ-WARN [{rid}] {rest}", err=True)
            except ValueError:
                click.echo(f"AQ-WARN {body}", err=True)
        elif issubclass(w.category, UserWarning):
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


_DEFAULT_OBS_PATH = ".aqueduct/observability.db"


def _resolve_obs_db(
    cfg,
    store_dir: str | None,
    run_id: str | None = None,
) -> Path | None:
    """Resolve the observability DB file path for a READ command.

    Mirrors the per-pipeline routing the WRITE side (`aqueduct run`) does at
    cli.py:1185-1290: when the user keeps the default
    `.aqueduct/observability.db`, each blueprint writes to
    `.aqueduct/observability/<blueprint_id>/observability.db`. READ commands
    (`runs`, `report`, `lineage`, `heal`) need to find the right per-pipeline
    file — historically each command reinvented this with a naive
    `Path(cfg.stores.observability.path).parent`, which only worked when the
    user explicitly set a non-default path.

    Resolution order:
      1. `--store-dir <path>` flag (explicit user override) → `<path>/observability.db`
      2. User set a non-default `stores.observability.path` in aqueduct.yml → use it verbatim
      3. Default sentinel AND `run_id` provided → glob the per-pipeline dirs
         and return the first DB containing a row for that run_id
      4. Default sentinel AND no run_id → return the legacy shared path
         `.aqueduct/observability.db` (may be missing; caller decides what to do)

    Returns None when no candidate file exists, so the caller can render a
    friendlier error than `FileNotFoundError`.
    """
    if store_dir:
        candidate = Path(store_dir) / "observability.db"
        return candidate if candidate.exists() else None

    obs_path = cfg.stores.observability.path
    if obs_path != _DEFAULT_OBS_PATH:
        explicit = Path(obs_path)
        if explicit.is_dir():
            explicit = explicit / "observability.db"
        return explicit if explicit.exists() else None

    if run_id:
        import duckdb as _duckdb
        for candidate in sorted(Path(".aqueduct/observability").glob("*/observability.db")):
            try:
                conn = _duckdb.connect(str(candidate), read_only=True)
                try:
                    hit = conn.execute(
                        "SELECT 1 FROM run_records WHERE run_id = ? LIMIT 1",
                        [run_id],
                    ).fetchone()
                finally:
                    conn.close()
                if hit:
                    return candidate
            except Exception:
                continue

    legacy = Path(_DEFAULT_OBS_PATH)
    return legacy if legacy.exists() else None


def _agent_usable(provider: str, base_url: str | None) -> bool:
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

        # 1.1.0 — write the tempfile NEXT TO the original blueprint so the
        # 1.1.0 path-anchoring rule (relative module paths resolve to the
        # blueprint's parent dir) still finds the real data files. Using /tmp/
        # would resolve `../data/...` against /tmp/ and produce `/data/...`.
        _anchor_dir = blueprint_path.parent if blueprint_path.exists() else None
        with tempfile.NamedTemporaryFile(
            suffix=".aq-apply.yml",
            delete=False,
            mode="w",
            dir=str(_anchor_dir) if _anchor_dir else None,
        ) as tmp:
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
        from aqueduct.agent import archive_patch

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


def _run_patch_gates_inline(
    *,
    patch,
    blueprint_path,
    bundle,
    surveyor,
    failed_module,
    iteration_run_id: str,
    blueprint_id: str,
    sample_rows: int = 1000,
    sandbox_mode: str = "sample",
):
    """Phase 29a/b — run the lineage, sandbox, and explain gates inline.

    Returns (lineage_res, sandbox_res, explain_res, gates_passed).
    gates_passed is True when the sandbox gate passes (or is skipped —
    Spark unavailable / no patch impact) AND the explain gate does not
    hard-block (explain is warn-only by default; only blocks when
    `agent.block_on_explain_regression` is True).
    """
    from aqueduct.patch.apply import _yaml_load, apply_patch_to_dict
    from aqueduct.patch.preview import run_lineage_gate, run_sandbox_gate
    from aqueduct.patch.explain_gate import run_explain_gate

    bp_raw = _yaml_load(blueprint_path)
    try:
        bp_after = apply_patch_to_dict(bp_raw, patch)
    except Exception:
        return None, None, None, False

    lineage_res = run_lineage_gate(bp_raw, bp_after, patch)
    try:
        surveyor.record_patch_simulation(
            patch_id=patch.patch_id,
            gate="lineage",
            status=lineage_res.status,
            detail="; ".join(w.detail for w in lineage_res.warnings) or None,
            duration_ms=lineage_res.duration_ms,
            run_id=iteration_run_id,
            blueprint_id=blueprint_id,
        )
    except Exception:
        pass

    explain_after: dict[str, dict] = {}
    # 1.1.0 — sandbox_mode controls replay fidelity:
    #   sample   → sample_rows rows per Ingress, no Egress (default)
    #   preflight → full dataset, no Egress (slow, conclusive)
    #   off       → skip the gate entirely (synthetic pass)
    if sandbox_mode == "off":
        from aqueduct.patch.preview import SandboxGateResult as _SBR
        sandbox_res = _SBR(
            status="skip",
            detail="sandbox_mode=off (danger.allow_skip_sandbox=true)",
            sample_rows=0,
            duration_ms=0,
        )
    else:
        _sample_for_call = 0 if sandbox_mode == "preflight" else int(sample_rows)
        sandbox_res = run_sandbox_gate(
            bp_after,
            blueprint_path=blueprint_path,
            patch_id=patch.patch_id,
            failed_module=failed_module,
            sample_rows=_sample_for_call,
            observability_store=bundle.observability,
            lineage_store=bundle.lineage,
            explain_capture=explain_after,
        )
    try:
        surveyor.record_patch_simulation(
            patch_id=patch.patch_id,
            gate="sandbox",
            status=sandbox_res.status,
            detail=sandbox_res.detail,
            sample_rows=sandbox_res.sample_rows,
            duration_ms=sandbox_res.duration_ms,
            run_id=iteration_run_id,
            blueprint_id=blueprint_id,
        )
    except Exception:
        pass

    # explain gate — per-module plan-count diff vs baseline in
    # observability.explain_snapshot
    explain_res = None
    try:
        baseline = surveyor.latest_explain_snapshots(blueprint_id=blueprint_id) if surveyor else {}
        explain_res = run_explain_gate(baseline, explain_after, touched_modules=lineage_res.touched_modules)
        surveyor.record_patch_simulation(
            patch_id=patch.patch_id,
            gate="explain",
            status=explain_res.status,
            detail=explain_res.detail or "; ".join(r.detail for r in explain_res.regressions) or None,
            duration_ms=explain_res.duration_ms,
            run_id=iteration_run_id,
            blueprint_id=blueprint_id,
        )
    except Exception:
        pass

    gates_passed = sandbox_res.status in ("pass", "skip")
    return lineage_res, sandbox_res, explain_res, gates_passed


def _stage_failed_patch(on_heal_failure: str, patch, patches_dir, failure_ctx, cfg, click_mod) -> None:
    """Handle on_heal_failure policy for a patch that failed to fix the pipeline."""
    if on_heal_failure == "stage":
        from aqueduct.agent import stage_patch_for_human
        stage_patch_for_human(patch, patches_dir, failure_ctx,
                              on_patch_pending_webhook=cfg.webhooks.on_patch_pending)
        # Reflect the actual on-disk filename (timestamp prefix added by
        # `_patch_filename`) instead of the bare patch_id.
        pending = patches_dir / "pending"
        _file = next(pending.glob(f"*_{patch.patch_id}.json"), None)
        _shown = _file.name if _file else f"{patch.patch_id}.json"
        click_mod.echo(
            f"  ✎ Failed patch staged for review → patches/pending/{_shown}",
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


# ── Unified env resolution (Phase 30) ─────────────────────────────────────────
# One code path for EVERY config-consuming command. Deterministic, transparent.
#
# Precedence (highest first):
#   1. -e / --env KEY=VAL   (CLI, docker-style, repeatable — overwrites)
#   2. real os.environ      (already exported / injected by orchestrator)
#   3. <anchor-dir>/.env    (project file beside aqueduct.yml / blueprint)
#   4. --env-file PATH       fallback only if no project .env (explicit override)
#   5. ${VAR:-default}       (resolver-level, in parser/config)
#
# cwd is intentionally NOT searched — a stray ./.env silently changing a run
# is the exact footgun we're removing. Disable .env discovery entirely with
# AQ_NO_ENV_FILE=1 (command-independent; CI / prod hermetic). A one-line
# stderr notice is always emitted so the implicit load is never invisible.


def _apply_cli_env(cli_env: "tuple[str, ...] | list[str]") -> int:
    """Apply `-e KEY=VAL` overrides into os.environ. Returns count.

    Highest precedence: overwrites real env AND any later .env (the .env
    loader skips keys already present). Docker `-e` semantics.
    """
    import os
    n = 0
    for item in cli_env or ():
        key, sep, val = item.partition("=")
        key = key.strip()
        if not sep or not key:
            raise click.BadParameter(
                f"-e/--env expects KEY=VALUE, got {item!r}", param_hint="-e",
            )
        os.environ[key] = val.strip()
        n += 1
    return n


def _resolve_and_load_env(
    explicit: "str | None",
    anchor: "Path | None",
    cli_env: "tuple[str, ...] | list[str] | None" = None,
) -> None:
    """Apply -e overrides, then load a single .env file. Emits a stderr notice.

    `anchor` = the input file (aqueduct.yml / blueprint) whose directory holds
    the project .env. cwd is never searched. AQ_NO_ENV_FILE=1 disables .env
    discovery (overrides still applied).
    """
    import os
    n_over = _apply_cli_env(cli_env or ())
    over = f"; {n_over} from -e" if n_over else ""

    if os.environ.get("AQ_NO_ENV_FILE"):
        click.echo(
            f"(env: .env discovery disabled — AQ_NO_ENV_FILE{over})", err=True
        )
        return

    candidates: list[Path] = []
    if anchor is not None:
        candidates.append(Path(anchor).resolve().parent / ".env")
    if explicit:
        candidates.append(Path(explicit).resolve())

    seen: set[Path] = set()
    for cand in candidates:
        if cand in seen or not cand.exists():
            seen.add(cand)
            continue
        n = _load_env_file(cand)
        click.echo(f"(env: loaded {n} var(s) from {cand}{over})", err=True)
        return  # first existing file wins — do not stack multiple .env files

    if n_over:
        click.echo(f"(env: no .env file found{over})", err=True)


def _env_options(f):
    """Shared decorator: adds `--env-file` + `-e/--env` to a command.

    Phase 30 — every config-consuming command gets identical env handling
    via this single decorator (no per-command copy-paste to forget). The
    `--no-env-file` flag is gone; use the AQ_NO_ENV_FILE=1 env var instead
    (command-independent, CI/prod-settable).
    """
    f = click.option(
        "--env-file", "env_file", default=None, type=click.Path(dir_okay=False),
        help="Fallback .env if no project .env beside the config/blueprint.",
    )(f)
    f = click.option(
        "-e", "--env", "cli_env", multiple=True, metavar="KEY=VAL",
        help="Set an env var (repeatable, docker-style). Highest precedence.",
    )(f)
    return f


def _sniff_file_kind(path: "Path") -> "str | None":
    """Identify an Aqueduct YAML by its version header (no full parse).

    Returns one of: "blueprint", "config", "aqtest", "aqscenario", or None
    when no recognised top-level key is found in the first ~40 lines.

    Header keys:
      aqueduct:           → blueprint
      aqueduct_config:    → engine config (aqueduct.yml)
      aqueduct_test:      → .aqtest.yml
      aqueduct_scenario:  → .aqscenario.yml
    """
    import re as _re
    try:
        head = "\n".join(path.read_text(encoding="utf-8").splitlines()[:40])
    except Exception:
        return None
    for key, kind in (
        (r"^aqueduct_config\s*:", "config"),
        (r"^aqueduct_test\s*:", "aqtest"),
        (r"^aqueduct_scenario\s*:", "aqscenario"),
        (r"^aqueduct\s*:", "blueprint"),
    ):
        if _re.search(key, head, _re.MULTILINE):
            return kind
    return None


from aqueduct import __version__ as _aqueduct_version
from aqueduct import exit_codes


def _install_secret_redaction_hooks() -> None:
    """Wrap click.echo and the logging chain so registered @aq.secret() values
    are scrubbed from every CLI emit path.

    Idempotent — the wrapped functions carry an attribute that signals they are
    already wrapped, so re-invoking from nested commands is a no-op. Installed
    eagerly at top-level ``cli`` invocation; commands that never resolve a
    secret incur a tiny per-emit no-op cost (empty registry → fast path).
    """
    from aqueduct.redaction import redact as _redact
    import logging as _logging

    if getattr(click.echo, "_aq_redaction_wrapped", False):
        return

    _orig_echo = click.echo

    def _wrapped_echo(message=None, file=None, nl=True, err=False, color=None):
        if isinstance(message, str):
            message = _redact(message)
        return _orig_echo(message, file=file, nl=nl, err=err, color=color)

    _wrapped_echo._aq_redaction_wrapped = True  # type: ignore[attr-defined]
    click.echo = _wrapped_echo  # type: ignore[assignment]

    class _RedactingFilter(_logging.Filter):
        def filter(self, record: _logging.LogRecord) -> bool:
            try:
                record.msg = _redact(record.getMessage())
                record.args = ()
            except Exception:  # noqa: BLE001
                pass
            return True

    root = _logging.getLogger()
    if not any(isinstance(f, _RedactingFilter) for f in root.filters):
        root.addFilter(_RedactingFilter())


class _AqueductJsonLogFormatter:
    """Minimal JSON log formatter for `--log-format json`.

    Emits one line of JSON per record with the canonical fields ops teams need
    when shipping to Loki / Splunk / Datadog: timestamp (ISO-8601 UTC), level,
    logger name, message (already %-formatted), plus exc_info when present.

    Implemented as a class with a `format` method (duck-typed to the stdlib
    Formatter interface) rather than subclassing `logging.Formatter` so we
    avoid pulling logging into the CLI import path unnecessarily.
    """

    # Stdlib LogRecord attributes — anything NOT in this set is treated as a
    # caller-supplied `extra=` field and merged into the JSON payload. Keeps
    # the schema open-ended without manually enumerating every domain key.
    _STANDARD_LOGRECORD_ATTRS = frozenset({
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "message", "asctime", "taskName",
    })

    def format(self, record) -> str:  # noqa: D401
        import json as _json
        import logging as _logging
        from datetime import datetime as _dt, timezone as _tz

        payload = {
            "ts": _dt.fromtimestamp(record.created, tz=_tz.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Merge caller-supplied `extra={...}` fields (`run_id`, `blueprint_id`,
        # `module_id`, etc.) into the payload so structured-log consumers can
        # filter on them. Standard LogRecord attributes are skipped.
        for key, value in record.__dict__.items():
            if key in self._STANDARD_LOGRECORD_ATTRS or key in payload:
                continue
            if key.startswith("_"):
                continue
            payload[key] = value
        if record.exc_info:
            payload["exc"] = _logging.Formatter().formatException(record.exc_info)
        return _json.dumps(payload, default=str)


@click.group()
@click.version_option(
    version=_aqueduct_version,
    prog_name="aqueduct",
    message="%(prog)s %(version)s",
)
@click.option("-v", "--verbose", is_flag=True, default=False, help="Enable DEBUG logging.")
@click.option(
    "--log-format",
    "log_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
    help=(
        "Logging output format. text=human-readable single line (default). "
        "json=one JSON object per record with ts/level/logger/msg fields — "
        "use when shipping logs to Loki / Splunk / Datadog."
    ),
)
@click.option(
    "--suppress-warning",
    "suppress_warnings",
    multiple=True,
    metavar="RULE_ID",
    help=(
        "Silence an Aqueduct warning by rule_id (copy from the `AQ-WARN [...]` "
        "prefix). Repeatable. Use `--suppress-warning '*'` to silence ALL. "
        "Merged with `warnings.suppress` from aqueduct.yml. Goes BEFORE the "
        "subcommand: `aqueduct --suppress-warning '*' run bp.yml`."
    ),
)
@click.pass_context
def cli(
    ctx: click.Context,
    verbose: bool,
    log_format: str,
    suppress_warnings: tuple[str, ...],
) -> None:
    """Aqueduct — Intelligent Spark Blueprint Engine."""
    import logging
    from aqueduct.warnings import install_cli_formatter, set_default_suppress
    level = logging.DEBUG if verbose else logging.WARNING

    if log_format.lower() == "json":
        handler = logging.StreamHandler()
        handler.setFormatter(_AqueductJsonLogFormatter())  # type: ignore[arg-type]
        # Replace any handlers basicConfig may have installed; idempotent.
        root = logging.getLogger()
        root.handlers.clear()
        root.addHandler(handler)
        root.setLevel(level)
    else:
        fmt = "%(levelname)s %(name)s: %(message)s" if verbose else "%(levelname)s: %(message)s"
        logging.basicConfig(level=level, format=fmt)

    # Install AQ-WARN [rule_id] format + stash CLI suppress overrides.
    # Engine-level `warnings.suppress` from aqueduct.yml is merged later, once a
    # command actually loads config (commands that never read config still
    # honour the CLI flag).
    install_cli_formatter()
    set_default_suppress(suppress=list(suppress_warnings))
    ctx.ensure_object(dict)
    ctx.obj["suppress_warnings_cli"] = list(suppress_warnings)

    _install_secret_redaction_hooks()


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


@cli.command()
@click.argument("files", nargs=-1, type=click.Path(exists=True, dir_okay=False))
@_env_options
def validate(
    files: tuple[str, ...], env_file: str | None, cli_env: tuple[str, ...]
) -> None:
    """Static validation — parse + schema check, no side effects.

    File type is detected by its version header, no flag needed:
      `aqueduct: "1.0"`         → Blueprint
      `aqueduct_config: "1.0"`  → engine config (aqueduct.yml)

    Pass any number of files. With no argument, validates `aqueduct.yml`
    in the current directory if present. Exit 0 = all valid, 1 = any invalid.
    """
    import json
    from pathlib import Path
    from aqueduct.parser.parser import ParseError, parse
    from aqueduct.config import ConfigError, load_config

    targets = [Path(f) for f in files]
    if not targets:
        default_cfg = Path.cwd() / "aqueduct.yml"
        if default_cfg.exists():
            click.echo(f"(no file given → validating {default_cfg.name})", err=True)
            targets = [default_cfg]
        else:
            click.echo("✗ no file given and no aqueduct.yml in CWD", err=True)
            sys.exit(1)

    any_fail = False
    for path in targets:
        _resolve_and_load_env(env_file, path, cli_env=cli_env)
        kind = _sniff_file_kind(path)

        if kind == "config":
            try:
                cfg = load_config(path)
                _apply_warnings_from_cfg(cfg)
            except ConfigError as exc:
                click.echo(f"✗ {path}: {exc}", err=True)
                any_fail = True
                continue
            click.echo(f"✓ {path}  [engine config]")
            click.echo(f"  engine:  {cfg.deployment.engine}  target={cfg.deployment.target}  master={cfg.deployment.master_url}")
            click.echo(f"  stores:  observability={cfg.stores.observability.path}  lineage={cfg.stores.lineage.path}  depot={cfg.stores.depot.path}")
            click.echo(f"  secrets: provider={cfg.secrets.provider}")
            wh_lines = []
            if cfg.webhooks.on_failure:
                wh_lines.append(f"on_failure={cfg.webhooks.on_failure.method} {cfg.webhooks.on_failure.url}")
            if cfg.webhooks.on_success:
                wh_lines.append(f"on_success={cfg.webhooks.on_success.method} {cfg.webhooks.on_success.url}")
            click.echo(f"  webhooks: {', '.join(wh_lines) if wh_lines else '(not configured)'}")
            if cfg.spark_config:
                click.echo(f"  spark_config: {json.dumps(cfg.spark_config)}")

        elif kind == "blueprint" or kind is None:
            # Unknown header → attempt blueprint parse (most common case);
            # the parser emits a precise error if it is not a blueprint.
            try:
                bp = parse(str(path))
                click.echo(f"✓ {path}  [blueprint: {bp.id}  {len(bp.modules)} modules, {len(bp.edges)} edges]")
            except ParseError as exc:
                click.echo(f"✗ {path}: {exc}", err=True)
                any_fail = True

        else:  # aqtest / aqscenario — schema pre-flight lives in `doctor`
            click.echo(
                f"- {path}: {kind} file — use `aqueduct doctor --{kind} {path}` "
                "for schema pre-flight",
                err=True,
            )

    sys.exit(1 if any_fail else 0)


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
        sys.exit(5)

    try:
        js = model.model_json_schema()
    except Exception as exc:
        click.echo(f"✗ schema generation failed: {exc}", err=True)
        sys.exit(2)

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
    "--verbose",
    "verbose",
    is_flag=True,
    default=False,
    help="Show skipped checks too (not-applicable / not-configured), not just the collapsed summary.",
)
@_env_options
def doctor(
    target: str | None,
    skip_spark: bool,
    preflight: bool,
    aqtest_path: str | None,
    aqscenario_path: str | None,
    verbose: bool,
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
            sys.exit(1)
    else:
        default_cfg = Path.cwd() / "aqueduct.yml"
        if default_cfg.exists():
            click.echo(f"(no file given → checking {default_cfg.name})", err=True)
            config_path = default_cfg

    # Anchor .env discovery to the resolved input file's directory.
    _anchor = config_path or blueprint_path
    _resolve_and_load_env(env_file, _anchor, cli_env=cli_env)

    _STATUS_ICON = {"ok": "✓", "fail": "✗", "warn": "⚠", "skip": "-"}
    _STATUS_COLOR = {"ok": "green", "fail": "red", "warn": "yellow", "skip": None}

    if skip_spark:
        click.echo("Running connectivity checks (--skip-spark: Spark check skipped)...")
    elif preflight:
        click.echo("Running connectivity checks (--preflight: full Spark session, unbounded — Ctrl-C to abort)...")
    else:
        click.echo("Running connectivity checks (Spark = fast TCP reachability; --preflight for full session)...")

    results = run_doctor(
        config_path=config_path,
        skip_spark=skip_spark,
        blueprint_path=blueprint_path,
        aqtest_path=Path(aqtest_path) if aqtest_path else None,
        aqscenario_path=Path(aqscenario_path) if aqscenario_path else None,
        preflight=preflight,
    )

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
    any_fail = False
    for r in shown:
        icon = _STATUS_ICON[r.status]
        color = _STATUS_COLOR[r.status]
        label = r.name.ljust(col_w)
        elapsed = f"  [{r.elapsed_ms}ms]" if r.elapsed_ms > 0 else ""
        line = f"  {icon} {label}{r.detail}{elapsed}"
        click.echo(click.style(line, fg=color) if color else line)
        if r.status == "fail":
            any_fail = True

    if hidden:
        names = ", ".join(r.name for r in hidden)
        # Same `  {glyph} {name.ljust(col_w)}{detail}` shape as every row above.
        click.echo(click.style(
            f"  · {'more'.ljust(col_w)}{names}  (ok / not applicable / not configured — --verbose)",
            fg="bright_black",
        ))

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
        pass  # missing/invalid aqueduct.yml is OK for `aqueduct compile`

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
    "--allow-multi-patch", "--allow-aggressive",
    "allow_aggressive",
    is_flag=True,
    default=False,
    help="Allow `max_patches > 1` for this run (overrides danger.allow_multi_patch=false). `--allow-aggressive` is a deprecated alias.",
)
@_env_options
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
    cli_env: tuple[str, ...] = (),
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
            click.echo(f"✗ config error: {exc}", err=True)
            sys.exit(1)

        # CLI flags override config file; config file overrides built-in defaults
        # Per-pipeline store paths: default .aqueduct/observability/<blueprint_id>.db instead of shared observability.db
        # _using_default_obs_path: only the default path gets relocated to a
        # per-pipeline .aqueduct/observability/<blueprint_id>/ dir below. A
        # user-set observability.path / lineage.path is already honoured
        # verbatim by get_stores() and must NOT be clobbered (ISSUE-024).
        _using_default_obs_path = False
        if store_dir_abs:
            resolved_store_dir = store_dir_abs
        else:
            _observability_path = cfg.stores.observability.path
            _default_obs = ".aqueduct/observability.db"
            if cfg.stores.observability.backend != "duckdb":
                # Non-DuckDB (postgres/redis): `path` is a DSN, NOT a
                # filesystem path — never Path()/mkdir it (would create a
                # bogus `postgresql:/user:pass@host:port` dir). The DSN store
                # persists itself; use the default per-pipeline local scratch
                # dir (.aqueduct/observability/<blueprint_id>) for any
                # residual local artifacts only.
                resolved_store_dir = None  # set below after manifest
            elif _observability_path == _default_obs:
                _using_default_obs_path = True
                # Defer to after manifest is parsed (need blueprint_id) — placeholder for now
                resolved_store_dir = None  # set below after manifest
            else:
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

        # ── Build per-run store bundle (Phase 28 — DuckDB / Postgres / Redis dispatch) ─
        # Depot must be ready before compile() so @aq.depot.get() in the
        # Blueprint can resolve.
        from aqueduct.stores import get_stores
        bundle = get_stores(cfg, store_dir_override=store_dir_abs)
        depot = DepotStore(backend=bundle.depot)

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
            resolved_store_dir = Path(f".aqueduct/observability/{manifest.blueprint_id}")
            resolved_store_dir.mkdir(parents=True, exist_ok=True)

        # ── Cluster-mode store path warning ───────────────────────────────────────
        # Standardized AQ-WARN rule (suppressible). Same rule_id + condition as
        # doctor's `cluster-stores` check — single source of truth, one wording.
        if cfg.deployment.env in ("cluster", "cloud") and not resolved_store_dir.is_absolute():
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
        if _is_multi_patch and not allow_aggressive:
            if not cfg.danger.allow_multi_patch:
                click.echo(
                    f"✗ max_patches={_max_patches} (>1) requires danger.allow_multi_patch: true "
                    "in aqueduct.yml, or pass --allow-multi-patch for this run "
                    "(legacy alias: --allow-aggressive).",
                    err=True,
                )
                sys.exit(1)

        # ── Sandbox-mode danger gates ─────────────────────────────────────────────
        _sandbox_mode = manifest.agent.sandbox_mode if manifest.agent else "sample"
        if _sandbox_mode == "preflight" and not cfg.danger.allow_full_preflight:
            click.echo(
                "✗ agent.sandbox_mode: preflight requires danger.allow_full_preflight: true "
                "in aqueduct.yml (full-dataset sandbox replay).",
                err=True,
            )
            sys.exit(1)
        if _sandbox_mode == "off" and not cfg.danger.allow_skip_sandbox:
            click.echo(
                "✗ agent.sandbox_mode: off requires danger.allow_skip_sandbox: true "
                "in aqueduct.yml (skips pre-apply validation; patches hit real data).",
                err=True,
            )
            sys.exit(1)
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
        resolved_agent_timeout = bp_agent.timeout or eng.timeout
        resolved_agent_max_reprompts = bp_agent.max_reprompts or eng.max_reprompts
        resolved_agent_engine_prompt_context = eng.prompt_context
        resolved_agent_blueprint_prompt_context = bp_agent.prompt_context

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
                DuckDBDepotStore,
                DuckDBLineageStore,
                DuckDBObservabilityStore,
            )
            from aqueduct.stores import StoreBundle
            bundle = StoreBundle(
                observability=DuckDBObservabilityStore(resolved_store_dir / "observability.db"),
                lineage=DuckDBLineageStore(resolved_store_dir / "lineage.db"),
                depot=bundle.depot,  # depot path stays user-configured
            )
            depot = DepotStore(backend=bundle.depot)
        surveyor = Surveyor(
            manifest,
            store_dir=resolved_store_dir,
            webhook_config=resolved_webhook,
            blueprint_path=Path(blueprint),
            patches_dir=patches_dir,
            stores=bundle,
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
        patch_staged_for_review = False  # set when human/ci mode writes a patch to patches/pending/
        last_apply_error: str | None = None  # fed back to LLM on next multi-patch iteration

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
                    lineage_store=bundle.lineage,
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
                if _agent_usable(resolved_agent_provider, resolved_agent_base_url):
                    click.echo(
                        "  ↻ LLM triggered by module rule (overriding approval_mode=disabled → staging patch for review)",
                        err=True,
                    )

            if effective_mode == "disabled" or failure_ctx is None:
                break

            if not _agent_usable(resolved_agent_provider, resolved_agent_base_url):
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

            # ── Generate patch ────────────────────────────────────────────────────
            from aqueduct.agent import archive_patch, generate_agent_patch, stage_patch_for_human
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
                            if _mt == "Channel" and "op" not in _cfg:
                                return False, "schema_drift", (
                                    f"Patch leaves Channel module {_m.get('id')!r} without "
                                    f"required 'op' key in config. Use set_module_config_key "
                                    f"to update one key instead of replace_module_config."
                                ), None
                            if _mt in ("Ingress", "Egress") and "format" not in _cfg:
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

            agent_result = generate_agent_patch(
                failure_ctx,
                model=resolved_agent_model,
                patches_dir=patches_dir,
                provider=resolved_agent_provider,
                base_url=resolved_agent_base_url,
                provider_options=resolved_agent_provider_options,
                timeout=resolved_agent_timeout,
                max_reprompts=resolved_agent_max_reprompts,
                engine_prompt_context=resolved_agent_engine_prompt_context,
                blueprint_prompt_context=resolved_agent_blueprint_prompt_context,
                last_apply_error=last_apply_error,
                guardrails=manifest.agent.guardrails if manifest.agent else None,
                budget=_budget,
                on_attempt=_persist_attempt,
                apply_callback=_apply_cb,
            )
            patch = agent_result.patch
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
                            model=resolved_agent_model,
                            patch_id=None,
                            confidence=None,
                            patch_applied=False,
                            run_success_after_patch=False,
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
                    run_id=iteration_run_id, failed_module=failure_ctx.failed_module,
                    parent_run_id=run_id,
                    failure_category=patch.category, model=resolved_agent_model,
                    patch_id=patch.patch_id, confidence=patch.confidence,
                    patch_applied=False, run_success_after_patch=False,
                )
                break

            patch_count += 1

            if effective_mode == "human":
                stage_patch_for_human(patch, patches_dir, failure_ctx,
                                      on_patch_pending_webhook=cfg.webhooks.on_patch_pending)
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
                            "run_id": iteration_run_id,
                            "blueprint_id": manifest.blueprint_id,
                            "failed_module": failure_ctx.failed_module,
                        }, timeout=10)
                    except Exception as _ce:
                        click.echo(f"  ⚠ ci webhook failed: {_ce}", err=True)
                stage_patch_for_human(patch, patches_dir, failure_ctx,
                                      on_patch_pending_webhook=cfg.webhooks.on_ci_patch)
                patch_staged_for_review = True
                click.echo(
                    f"  ✎ CI patch staged → patches/pending/{patch.patch_id}.json",
                    err=True,
                )
                surveyor.record_healing_outcome(
                    run_id=iteration_run_id, failed_module=failure_ctx.failed_module,
                    parent_run_id=run_id,
                    failure_category=patch.category, model=resolved_agent_model,
                    patch_id=patch.patch_id, confidence=patch.confidence,
                    patch_applied=False, run_success_after_patch=False,
                )
                break

            elif effective_mode == "auto":
                # Patch validation pyramid Gates 2 (lineage), 3 (sandbox), 4 (explain) pre-filter.
                _g2, _g3, _g4, _g3_passed = _run_patch_gates_inline(
                    patch=patch,
                    blueprint_path=Path(blueprint),
                    bundle=bundle,
                    surveyor=surveyor,
                    failed_module=failure_ctx.failed_module,
                    iteration_run_id=iteration_run_id,
                    blueprint_id=manifest.blueprint_id,
                    sandbox_mode=manifest.agent.sandbox_mode if manifest.agent else "sample",
                )
                if _g4 is not None and _g4.status == "warn":
                    for _r in _g4.regressions:
                        click.echo(f"  ⚠ explain-gate regression: {_r.detail}", err=True)
                if _g3 is not None and not _g3_passed:
                    click.echo(
                        f"  ✗ LLM patch failed sandbox replay: {_g3.detail}",
                        err=True,
                    )
                    surveyor.record_healing_outcome(
                        run_id=iteration_run_id, failed_module=failure_ctx.failed_module,
                        parent_run_id=run_id,
                        failure_category=patch.category, model=resolved_agent_model,
                        patch_id=patch.patch_id, confidence=patch.confidence,
                        patch_applied=False, run_success_after_patch=False,
                    )
                    _stage_failed_patch(
                        manifest.agent.on_heal_failure, patch, patches_dir, failure_ctx, cfg, click,
                    )
                    break

                # Resolve patch_validation (blueprint override → engine default)
                _patch_validation = manifest.agent.patch_validation or cfg.agent.patch_validation

                if _patch_validation == "sandbox" and _g3 is not None and _g3.status == "pass":
                    # Sandbox-only validation: write the patched Blueprint without
                    # running the full pipeline. The next regular `aqueduct run`
                    # will execute it against real data and real Egress sinks.
                    _write_patch_to_blueprint(patch, Path(blueprint), patches_dir, failure_ctx, mode="auto")
                    click.echo(
                        f"  ✓ LLM patch validated via sandbox-only ({_g3.sample_rows or '∞'} rows) "
                        f"→ {blueprint}",
                        err=True,
                    )
                    surveyor.record_healing_outcome(
                        run_id=iteration_run_id, failed_module=failure_ctx.failed_module,
                        parent_run_id=run_id,
                        failure_category=patch.category, model=resolved_agent_model,
                        patch_id=patch.patch_id, confidence=patch.confidence,
                        patch_applied=True, run_success_after_patch=True,
                    )
                    break

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
                    run_id=iteration_run_id, failed_module=failure_ctx.failed_module,
                    parent_run_id=run_id,
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
                # Patch validation pyramid Gates 2, 3, 4 pre-filter for the legacy
                # `aggressive` mode (deprecated alias for `auto` + `max_patches > 1`).
                _g2, _g3, _g4, _g3_passed = _run_patch_gates_inline(
                    patch=patch,
                    blueprint_path=Path(blueprint),
                    bundle=bundle,
                    surveyor=surveyor,
                    failed_module=failure_ctx.failed_module,
                    iteration_run_id=iteration_run_id,
                    blueprint_id=manifest.blueprint_id,
                    sandbox_mode=manifest.agent.sandbox_mode if manifest.agent else "sample",
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
                        failure_category=patch.category, model=resolved_agent_model,
                        patch_id=patch.patch_id, confidence=patch.confidence,
                        patch_applied=False, run_success_after_patch=False,
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
                        failure_category=patch.category, model=resolved_agent_model,
                        patch_id=patch.patch_id, confidence=patch.confidence,
                        patch_applied=False, run_success_after_patch=False,
                    )
                    continue  # try next patch iteration

                _patch_validation = manifest.agent.patch_validation or cfg.agent.patch_validation

                if _patch_validation == "sandbox" and _g3 is not None and _g3.status == "pass":
                    _write_patch_to_blueprint(patch, Path(blueprint), patches_dir, failure_ctx, mode="aggressive")
                    click.echo(
                        f"  ✓ multi-patch: sandbox-only validated → {blueprint}",
                        err=True,
                    )
                    surveyor.record_healing_outcome(
                        run_id=iteration_run_id, failed_module=failure_ctx.failed_module,
                        parent_run_id=run_id,
                        failure_category=patch.category, model=resolved_agent_model,
                        patch_id=patch.patch_id, confidence=patch.confidence,
                        patch_applied=True, run_success_after_patch=True,
                    )
                    break

                new_manifest = _apply_patch_in_memory(patch, Path(blueprint), depot, profile, cli_overrides or {})
                if new_manifest is None:
                    click.echo("  ✗ LLM patch produces invalid Blueprint, discarding", err=True)
                    last_apply_error = f"Patch {patch.patch_id!r} produced invalid Blueprint"
                    surveyor.record_healing_outcome(
                        run_id=iteration_run_id, failed_module=failure_ctx.failed_module,
                        parent_run_id=run_id,
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
                    run_id=iteration_run_id, failed_module=failure_ctx.failed_module,
                    parent_run_id=run_id,
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
            depot.put("_last_run_id", run_id)
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
            # Exit 3 (HEAL_PENDING) when a patch was staged for human review — lets
            # downstream orchestrators (Airflow operator, CI runners) distinguish
            # "needs human approval" from a hard runtime failure (exit 2).
            sys.exit(
                exit_codes.HEAL_PENDING if patch_staged_for_review else exit_codes.DATA_OR_RUNTIME
            )

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
            )

        status_label = "patched" if result.status == "patched" else "complete"
        click.echo(f"\n✓ blueprint {status_label}  run_id={run_id}")
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

    # Get ISO timestamp of last git commit touching this blueprint.
    # Tolerate environments without git (containerized workers, etc.) — the
    # check is informational; falling back to "treat all as uncommitted"
    # preserves the safety semantics without breaking the run.
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%cI", "--", str(blueprint_path)],
            capture_output=True, text=True,
        )
        last_commit_ts: str | None = result.stdout.strip() if result.returncode == 0 else None
    except (FileNotFoundError, PermissionError, OSError):
        last_commit_ts = None

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


@patch.command("preview")
@click.argument("patch_file", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--blueprint",
    "blueprint_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Blueprint YAML the patch will be applied to.",
)
@click.option(
    "--sandbox",
    is_flag=True,
    default=False,
    help="Also run the sandbox gate — replay the patched Blueprint on a sampled DataFrame.",
)
@click.option(
    "--sample",
    "sample_rows",
    type=int,
    default=1000,
    show_default=True,
    help="Per-Ingress row limit during the sandbox gate. 0 = unbounded (full data).",
)
@click.option(
    "--config",
    "config_path",
    default=None,
    type=click.Path(dir_okay=False),
    help="Path to aqueduct.yml",
)
@click.option(
    "--format",
    "out_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format. `text` (default) renders diff + gate findings. `json` emits a machine-readable report.",
)
@_env_options
def patch_preview(
    patch_file: str,
    blueprint_path: str,
    sandbox: bool,
    sample_rows: int,
    config_path: str | None,
    out_format: str,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Validation pyramid preview for a pending patch.

    Always runs the guardrails gate (schema + post-apply Parser re-check)
    and the lineage gate (live lineage impact). With `--sandbox`, also
    runs the sandbox gate (replay the patched Blueprint on a per-Ingress
    LIMIT N, Egress modules skipped and listed in the report) and the
    explain gate (post-patch `explain()` regression
    check against the most recent baseline in `observability.explain_snapshot`).
    """
    import json as _json
    from pathlib import Path as _Path
    from aqueduct.config import ConfigError, load_config
    from aqueduct.patch.apply import (
        PatchError,
        _check_guardrails,
        _yaml_load,
        apply_patch_to_dict,
        load_patch_spec,
    )
    from aqueduct.patch.preview import (
        render_unified_diff,
        run_lineage_gate,
        run_sandbox_gate,
    )
    from aqueduct.patch.explain_gate import run_explain_gate

    bp_raw = _yaml_load(_Path(blueprint_path))
    try:
        spec = load_patch_spec(_Path(patch_file))
    except PatchError as exc:
        click.echo(f"✗ patch schema error: {exc}", err=True)
        sys.exit(2)

    # Guardrails gate — deterministic. Identical enforcement used by
    # `patch apply`; surfaced here so reviewers see violations up front.
    try:
        _check_guardrails(spec, bp_raw, provenance_map=None)
    except PatchError as exc:
        click.echo(f"✗ Guardrails gate blocked: {exc}", err=True)
        sys.exit(2)

    try:
        bp_after = apply_patch_to_dict(bp_raw, spec)
    except PatchError as exc:
        click.echo(f"✗ patch could not be applied in memory: {exc}", err=True)
        sys.exit(2)

    diff = render_unified_diff(bp_raw, bp_after)
    lineage_res = run_lineage_gate(bp_raw, bp_after, spec)

    sandbox_res = None
    explain_res = None
    if sandbox:
        cfg = None
        try:
            _resolve_and_load_env(
                env_file,
                _Path(config_path) if config_path else _Path(blueprint_path),
                cli_env=cli_env,
            )
            cfg = load_config(_Path(config_path) if config_path else None)
            _apply_warnings_from_cfg(cfg)
        except ConfigError as exc:
            click.echo(f"✗ config error (needed for sandbox): {exc}", err=True)
            sys.exit(1)
        from aqueduct.stores import get_stores
        bundle = get_stores(cfg)
        failed_module = None
        explain_after: dict[str, dict] = {}
        # patch_id is used both for run-tagging and tempfile naming
        sandbox_res = run_sandbox_gate(
            bp_after,
            blueprint_path=_Path(blueprint_path),
            patch_id=spec.patch_id,
            failed_module=failed_module,
            sample_rows=int(sample_rows),
            observability_store=bundle.observability,
            lineage_store=bundle.lineage,
            explain_capture=explain_after,
        )
        # Explain gate — baseline read directly from the observability store.
        try:
            from aqueduct.stores import get_stores as _gs  # noqa
            from aqueduct.surveyor.surveyor import Surveyor
            # Compile to retrieve blueprint_id without full run.
            from aqueduct.parser.parser import parse as _parse
            from aqueduct.compiler.compiler import compile as _compile
            _bp = _parse(blueprint_path)
            _mf = _compile(_bp, blueprint_path=_Path(blueprint_path))
            _surv = Surveyor(manifest=_mf, store_dir=cfg.store_dir, stores=bundle)
            _baseline = _surv.latest_explain_snapshots(blueprint_id=_mf.blueprint_id)
        except Exception:
            _baseline = {}
        explain_res = run_explain_gate(_baseline, explain_after, touched_modules=lineage_res.touched_modules)

    if out_format.lower() == "json":
        report = {
            "patch_id": spec.patch_id,
            "blueprint_path": str(blueprint_path),
            "diff": diff,
            "lineage": {
                "status": lineage_res.status,
                "touched_modules": lineage_res.touched_modules,
                "warnings": [w.__dict__ for w in lineage_res.warnings],
                "duration_ms": lineage_res.duration_ms,
            },
        }
        if sandbox_res is not None:
            report["sandbox"] = {
                "status": sandbox_res.status,
                "detail": sandbox_res.detail,
                "sample_rows": sandbox_res.sample_rows,
                "duration_ms": sandbox_res.duration_ms,
                "egress_targets": sandbox_res.egress_targets,
            }
        if explain_res is not None:
            report["explain"] = {
                "status": explain_res.status,
                "detail": explain_res.detail,
                "duration_ms": explain_res.duration_ms,
                "baseline_run_id": explain_res.baseline_run_id,
                "regressions": [r.__dict__ for r in explain_res.regressions],
            }
        click.echo(_json.dumps(report, indent=2))
        sys.exit(0 if lineage_res.status != "fail" and (sandbox_res is None or sandbox_res.status == "pass" or sandbox_res.status == "skip") else 2)

    # Text report
    click.echo(f"Patch {spec.patch_id}")
    click.echo(f"  rationale: {spec.rationale}")
    if spec.confidence is not None:
        click.echo(f"  confidence: {spec.confidence:.0%}")
    click.echo()
    click.echo("── Blueprint diff ────────────────────────────────────────────")
    click.echo(diff if diff.strip() else "  (no textual change)")

    click.echo()
    click.echo("── Lineage gate (live sqlglot) ───────────────────────────────")
    click.echo(f"  status:          {lineage_res.status}")
    click.echo(f"  touched modules: {', '.join(lineage_res.touched_modules) or '(none)'}")
    if lineage_res.warnings:
        for w in lineage_res.warnings:
            click.echo(f"  ⚠ {w.detail}")
    else:
        click.echo("  no downstream column-consumption regressions detected")
    click.echo(f"  duration:        {lineage_res.duration_ms} ms")

    if sandbox_res is not None:
        click.echo()
        click.echo("── Sandbox gate (replay) ─────────────────────────────────────")
        click.echo(f"  status:      {sandbox_res.status}")
        click.echo(f"  detail:      {sandbox_res.detail}")
        if sandbox_res.sample_rows is not None:
            click.echo(f"  sample_rows: {sandbox_res.sample_rows}")
        click.echo(f"  duration:    {sandbox_res.duration_ms} ms")
        if sandbox_res.egress_targets:
            click.echo("  Egress operations (sandbox skipped):")
            for t in sandbox_res.egress_targets:
                click.echo(
                    f"    {t.get('id')}  → {t.get('format')}  {t.get('path')}"
                    + (f"  (mode={t.get('mode')})" if t.get("mode") else "")
                )

    if explain_res is not None:
        click.echo()
        click.echo("── Explain gate (plan regression) ────────────────────────────")
        click.echo(f"  status:   {explain_res.status}")
        click.echo(f"  detail:   {explain_res.detail}")
        if explain_res.baseline_run_id:
            click.echo(f"  baseline: run {explain_res.baseline_run_id}")
        if explain_res.regressions:
            for r in explain_res.regressions:
                click.echo(f"  ⚠ {r.detail}")
        click.echo(f"  duration: {explain_res.duration_ms} ms")

    exit_code = 0
    if lineage_res.status == "fail":
        exit_code = 2
    if sandbox_res is not None and sandbox_res.status == "fail":
        exit_code = 2
    sys.exit(exit_code)


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
@click.option(
    "--format", "out_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text", show_default=True,
    help="Output format. `json` for machine-readable consumption (Phase 30b).",
)
def patch_list(blueprint: str | None, patches_dir: str | None, filter_status: str, out_format: str) -> None:
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

    if out_format.lower() == "json":
        payload: list[dict] = []
        for status_label, d in dirs_to_show:
            for f in sorted(d.glob("*.json"), key=lambda x: x.name):
                try:
                    data = json.loads(f.read_text(encoding="utf-8"))
                except Exception:
                    data = {}
                meta = data.get("_aq_meta") or {}
                payload.append({
                    "status": status_label,
                    "file": str(f),
                    "patch_id": data.get("patch_id", f.stem),
                    "rationale": data.get("rationale"),
                    "confidence": data.get("confidence"),
                    "category": data.get("category"),
                    "run_id": meta.get("run_id"),
                    "blueprint_id": meta.get("blueprint_id"),
                    "failed_module": meta.get("failed_module"),
                })
        click.echo(json.dumps(payload, indent=2))
        return

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
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(1)

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
        sys.exit(1)
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
@_env_options
def report(
    run_id: str, store_dir: str | None, config_path: str | None, fmt: str,
    env_file: str | None, cli_env: tuple[str, ...],
) -> None:
    """Print the Flow Report for a completed run."""
    import csv as _csv
    import io

    import duckdb as _duckdb

    from aqueduct.config import ConfigError, load_config

    try:
        _resolve_and_load_env(
            env_file, Path(config_path) if config_path else None, cli_env=cli_env
        )
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(1)

    observability_db = _resolve_obs_db(cfg, store_dir, run_id=run_id)
    if observability_db is None or not observability_db.exists():
        click.echo(
            f"✗ observability.db not found for run_id={run_id!r} "
            f"(searched: --store-dir, cfg.stores.observability.path, "
            f"and .aqueduct/observability/*/observability.db)",
            err=True,
        )
        sys.exit(1)

    conn = _duckdb.connect(str(observability_db), read_only=True)
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
        click.echo(f"✗ run {run_id!r} not found in {observability_db}", err=True)
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
@click.option(
    "--format", "out_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text", show_default=True,
    help="Output format. `json` for machine-readable consumption (Phase 30b).",
)
@_env_options
def runs(
    blueprint: str | None,
    failed: bool,
    limit: int,
    store_dir: str | None,
    config_path: str | None,
    out_format: str,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """List recent blueprint runs."""
    import duckdb as _duckdb
    from aqueduct.config import load_config

    _resolve_and_load_env(
        env_file, Path(config_path) if config_path else None, cli_env=cli_env
    )
    cfg = load_config(Path(config_path) if config_path else None)
    _apply_warnings_from_cfg(cfg)

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

    # Collect candidate DBs across per-pipeline dirs + legacy shared path.
    # When the user set an explicit obs path, honour it verbatim. When
    # `--blueprint` is given, prefer that per-pipeline dir.
    candidates: list[Path] = []
    if store_dir:
        c = Path(store_dir) / "observability.db"
        if c.exists():
            candidates.append(c)
    elif cfg.stores.observability.path != _DEFAULT_OBS_PATH:
        c = Path(cfg.stores.observability.path)
        if c.is_dir():
            c = c / "observability.db"
        if c.exists():
            candidates.append(c)
    else:
        if blueprint_id:
            c = Path(".aqueduct/observability") / blueprint_id / "observability.db"
            if c.exists():
                candidates.append(c)
        if not candidates:
            candidates = sorted(Path(".aqueduct/observability").glob("*/observability.db"))
        legacy = Path(_DEFAULT_OBS_PATH)
        if legacy.exists():
            candidates.append(legacy)

    if not candidates:
        click.echo("No runs found (no observability.db files discovered)")
        return

    where_parts = []
    params_base: list = []
    if blueprint_id:
        where_parts.append("blueprint_id = ?")
        params_base.append(blueprint_id)
    if failed:
        where_parts.append("status = 'error'")
    where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    rows: list = []
    for db in candidates:
        try:
            conn = _duckdb.connect(str(db), read_only=True)
            try:
                rows.extend(conn.execute(
                    f"""
                    SELECT run_id, blueprint_id, status, started_at, finished_at,
                           json_extract_string(module_results, '$[0].module_id') AS first_failed
                    FROM run_records
                    {where}
                    """,
                    params_base,
                ).fetchall())
            finally:
                conn.close()
        except Exception:
            continue
    # Sort merged result by started_at DESC (col index 3), then limit
    rows.sort(key=lambda r: (r[3] is None, r[3]), reverse=True)
    rows = rows[:limit]

    if out_format.lower() == "json":
        import json as _json
        payload = [
            {
                "run_id": rv,
                "blueprint_id": bp,
                "status": st,
                "started_at": str(sa) if sa else None,
                "finished_at": str(fa) if fa else None,
                "first_failed_module": ff,
            }
            for rv, bp, st, sa, fa, ff in rows
        ]
        click.echo(_json.dumps(payload, indent=2))
        return

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
@_env_options
def lineage(
    blueprint_id_or_blueprint: str,
    store_dir: str | None,
    config_path: str | None,
    from_table: str | None,
    column_filter: str | None,
    fmt: str,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Print column-level lineage graph for a blueprint.

    PIPELINE_ID_OR_BLUEPRINT: blueprint id (e.g. nyc_taxi_demo) or path to
    the blueprint YAML file (e.g. blueprint.yml — id is extracted automatically).
    """
    import duckdb as _duckdb

    from aqueduct.config import ConfigError, load_config

    try:
        _resolve_and_load_env(
            env_file, Path(config_path) if config_path else None, cli_env=cli_env
        )
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
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

    # Per-pipeline routing: when on the default obs path, lineage.db lives
    # at `.aqueduct/observability/<blueprint_id>/lineage.db` alongside its
    # observability.db sibling.
    if store_dir:
        lineage_db = Path(store_dir) / "lineage.db"
    elif cfg.stores.observability.path != _DEFAULT_OBS_PATH:
        lineage_db = Path(cfg.stores.observability.path).parent / "lineage.db"
    else:
        lineage_db = Path(".aqueduct/observability") / blueprint_id / "lineage.db"
        if not lineage_db.exists():
            # Legacy fallback for pre-per-pipeline-routing installs
            lineage_db = Path(".aqueduct/lineage.db")
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
@_env_options
def signal(
    signal_id: str,
    value_str: str | None,
    error_msg: str | None,
    store_dir: str | None,
    config_path: str | None,
    env_file: str | None,
    cli_env: tuple[str, ...],
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

    from aqueduct.config import ConfigError, load_config
    from aqueduct.stores import get_stores
    from aqueduct.surveyor.surveyor import _SIGNAL_OVERRIDES_DDL

    try:
        _resolve_and_load_env(
            env_file, Path(config_path) if config_path else None, cli_env=cli_env
        )
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(1)

    bundle = get_stores(cfg, store_dir_override=Path(store_dir) if store_dir else None)

    if value_str is None and error_msg is None:
        # Show current override status
        with bundle.observability.connect() as cur:
            cur.execute(_SIGNAL_OVERRIDES_DDL)
            row = cur.execute(
                "SELECT passed, error_message, CAST(set_at AS VARCHAR) FROM signal_overrides WHERE signal_id = ?",
                [signal_id],
            ).fetchone()
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
    passed = value_str == "true"

    now = datetime.now(tz=timezone.utc).isoformat()
    with bundle.observability.connect() as cur:
        cur.execute(_SIGNAL_OVERRIDES_DDL)
        if passed:
            # Clear override — delete row entirely
            cur.execute("DELETE FROM signal_overrides WHERE signal_id = ?", [signal_id])
            click.echo(f"✓ signal {signal_id!r}: override cleared — gate resumes normal Probe evaluation")
        else:
            cur.execute(
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
    is_flag=False,
    flag_value="text",
    default=None,
    type=click.Choice(["text", "json"]),
    help="Print the LLM prompt that would be sent and exit without calling "
    "the model. Bare = text; `--print-prompt json` for JSON.",
)
@_env_options
def heal(
    run_id: str | None,
    module_id: str | None,
    store_dir: str | None,
    config_path: str | None,
    patches_dir: str,
    print_prompt: str | None,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Manually trigger LLM self-healing for a failed run.

    \b
    aqueduct heal <run_id>

    Reads the FailureContext for that run from the observability store,
    asks the agent for a patch, and stages it into the patch lifecycle.
    Scenario/aqscenario evaluation is a separate concern — use
    `aqueduct benchmark <file-or-dir>`.
    """
    from aqueduct.config import ConfigError, load_config
    from aqueduct.agent import build_prompt, generate_agent_patch, stage_patch_for_human

    if not run_id:
        click.echo("✗ provide a run_id argument", err=True)
        sys.exit(1)

    try:
        _resolve_and_load_env(
            env_file,
            Path(config_path) if config_path else None,
            cli_env=cli_env,
        )
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(1)

    eng = cfg.agent
    resolved_provider = eng.provider
    resolved_base_url = eng.base_url
    resolved_model = eng.model
    resolved_provider_options = eng.provider_options
    resolved_timeout = eng.timeout
    resolved_max_reprompts = eng.max_reprompts
    resolved_engine_prompt_context = eng.prompt_context

    if resolved_model is None and not print_prompt:
        click.echo(
            "✗ no LLM agent configured — set agent.model in aqueduct.yml",
            err=True,
        )
        sys.exit(1)

    patches_path = Path(patches_dir)

    # ── Live run mode ─────────────────────────────────────────────────────────
    import duckdb as _duckdb
    from aqueduct.surveyor.models import FailureContext

    observability_db = _resolve_obs_db(cfg, store_dir, run_id=run_id)
    if observability_db is None or not observability_db.exists():
        click.echo(
            f"✗ observability.db not found for run_id={run_id!r} "
            f"(searched: --store-dir, cfg.stores.observability.path, "
            f"and .aqueduct/observability/*/observability.db)",
            err=True,
        )
        sys.exit(1)

    conn = _duckdb.connect(str(observability_db), read_only=True)
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

    # Extract guardrails from the persisted manifest so heal-from-store paths
    # surface the same constraints the live run would have used.
    _guardrails_for_prompt: Any = None
    try:
        _mdict = json.loads(manifest_json_raw) if isinstance(manifest_json_raw, str) else manifest_json_raw
        if isinstance(_mdict, dict):
            _agent_block = _mdict.get("agent") or {}
            _guardrails_for_prompt = _agent_block.get("guardrails") or None
    except Exception:
        _guardrails_for_prompt = None

    if print_prompt:
        prompt = build_prompt(failure_ctx, patches_path, resolved_engine_prompt_context, guardrails=_guardrails_for_prompt)
        _print_prompt(prompt, print_prompt)
        return

    click.echo(
        f"↻ heal  run={run_id}  module={target_module}  "
        f"provider={resolved_provider}  model={resolved_model}"
    )

    from aqueduct.agent import resolve_budget as _resolve_budget
    _budget = _resolve_budget(
        getattr(cfg.agent, "budget", None),
        max_reprompts=resolved_max_reprompts,
    )

    # Wire deterministic apply-gate guardrail check INTO the loop so
    # rejections feed back as reprompts (same as `aqueduct run` self-heal). No
    # live blueprint path here — heal-from-store reconstructs the minimal dict
    # `_check_guardrails` needs from the manifest_json carried in the obs DB.
    def _apply_cb(patch_spec: Any, _gb=_guardrails_for_prompt) -> tuple:
        if not _gb:
            return True, None, None, None
        try:
            from aqueduct.patch.apply import _check_guardrails, PatchError
            bp_raw = {"agent": {"guardrails": _gb}}
            try:
                _check_guardrails(patch_spec, bp_raw, provenance_map=None)
                return True, None, None, None
            except PatchError as exc:
                return False, "guardrail_violation", str(exc), None
        except Exception as exc:
            return False, "apply_error", str(exc), None

    agent_result = generate_agent_patch(
        failure_ctx,
        model=resolved_model,
        patches_dir=patches_path,
        provider=resolved_provider or "anthropic",
        base_url=resolved_base_url,
        provider_options=resolved_provider_options,
        timeout=resolved_timeout,
        max_reprompts=resolved_max_reprompts,
        engine_prompt_context=resolved_engine_prompt_context,
        guardrails=_guardrails_for_prompt,
        budget=_budget,
        apply_callback=_apply_cb,
    )
    patch = agent_result.patch

    if patch is None:
        click.echo(
            f"✗ LLM failed to produce a valid patch after {agent_result.attempts} attempt(s) "
            f"(stop_reason={agent_result.stop_reason})",
            err=True,
        )
        for err in agent_result.reprompt_errors:
            click.echo(f"  · {err}", err=True)
        sys.exit(1)

    stage_patch_for_human(patch, patches_path, failure_ctx)
    click.echo(f"✓ patch staged → {patches_path}/pending/{patch.patch_id}.json")
    click.echo(f"  apply with: aqueduct patch apply patches/pending/{patch.patch_id}.json --blueprint <path>")


# ── aqueduct benchmark ────────────────────────────────────────────────────────

@cli.command()
@click.argument(
    "scenarios_pos",
    required=False,
    default=None,
    type=click.Path(exists=True),
)
@click.option(
    "--scenarios",
    "scenarios_dir",
    default=None,
    type=click.Path(exists=True),
    help="A .aqscenario.yml file or a directory of them (searched recursively). "
    "May also be given as a positional argument.",
)
@click.option(
    "--model",
    "models",
    multiple=True,
    default=None,
    help="Model to benchmark (repeatable: --model A --model B). Defaults to agent.model in aqueduct.yml",
)
@click.option(
    "--provider",
    "provider_override",
    default=None,
    type=click.Choice(["anthropic", "openai_compat"]),
    help="Override agent.provider for this run (e.g. openai_compat for Ollama/vLLM).",
)
@click.option(
    "--base-url",
    "base_url_override",
    default=None,
    help="Override agent.base_url for this run (e.g. http://host:11434/v1).",
)
@click.option(
    "--timeout",
    "timeout_override",
    default=None,
    type=float,
    help="Override agent.timeout (seconds) for this run. Raise for slow/cold "
    "local models (default 120; e.g. 600). Use 0 for no limit (unbounded "
    "read; connect still fails fast).",
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
    "--format",
    "fmt",
    default="table",
    type=click.Choice(["table", "json"]),
    show_default=True,
    help="Output data shape (table | json). (-o/--output is reserved for "
    "file destinations on other commands; this is the data format.)",
)
@click.option(
    "--workers",
    default=1,
    show_default=True,
    help="Max concurrent LLM calls. Default 1 (serial); set >1 to parallelize scenario×model pairs.",
)
@click.option(
    "--no-persist",
    "no_persist",
    is_flag=True,
    default=False,
    help="Skip writing results to the benchmark store (Phase 33 Part A). "
    "Default is to persist each (scenario, model) row into "
    "<scenarios_dir>/.aqueduct/benchmark.duckdb for future regression diffs.",
)
@click.option(
    "--store-path",
    "store_path_override",
    default=None,
    type=click.Path(dir_okay=False),
    help="Override the benchmark store path. Default: "
    "<scenarios_dir>/.aqueduct/benchmark.duckdb",
)
@click.option(
    "--gate-on-regression",
    "gate_on_regression",
    is_flag=True,
    default=False,
    help="After persisting, diff each (scenario, model) vs the most recent "
    "prior row in the store. Exit non-zero if any regression is detected "
    "(passed True→False, patch_applies True→False, diag_score or confidence "
    "drop > 5pp). Implies persistence; ignored with --no-persist.",
)
@_env_options
def benchmark(
    scenarios_pos: str | None,
    scenarios_dir: str | None,
    models: tuple[str, ...],
    provider_override: str | None,
    base_url_override: str | None,
    timeout_override: float | None,
    config_path: str | None,
    patches_dir: str,
    fmt: str,
    workers: int,
    no_persist: bool,
    store_path_override: str | None,
    gate_on_regression: bool,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Run one or many scenarios against one or more LLM models and compare.

    \b
    Example:
      aqueduct benchmark aqscenarios/ --model claude-opus-4-7 --model llama3
      aqueduct benchmark one.aqscenario.yml          # single scenario

    Takes a .aqscenario.yml file or a directory (recursively globbed),
    runs each against every specified model, prints a comparison table.
    No Spark required — scenarios inject failures synthetically.
    """
    target = scenarios_pos or scenarios_dir
    if not target:
        click.echo(
            "✗ provide a scenario file or directory (positional, or --scenarios)",
            err=True,
        )
        sys.exit(1)
    scenarios_dir = target
    from aqueduct.config import ConfigError, load_config
    from aqueduct.surveyor.scenario import format_benchmark_table, run_benchmark

    try:
        _resolve_and_load_env(
            env_file,
            Path(config_path) if config_path else Path(scenarios_dir),
            cli_env=cli_env,
        )
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(1)

    eng = cfg.agent
    # Precedence: CLI flag > cfg.agent > built-in default. Connection identity
    # only — provider_options / guardrails stay config (aqueduct.yml).
    resolved_provider = provider_override or eng.provider
    resolved_base_url = base_url_override or eng.base_url
    resolved_model = eng.model
    resolved_provider_options = eng.provider_options
    resolved_timeout = timeout_override if timeout_override is not None else eng.timeout
    # 0 = sentinel for "no limit" → None (httpx: unbounded read; connect
    # still bounded so an unreachable host fails fast).
    if resolved_timeout == 0:
        resolved_timeout = None
    resolved_max_reprompts = eng.max_reprompts
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
        f"models={model_list}  provider={resolved_provider}",
        err=True,
    )

    # Count scenarios up-front for the banner. Cheap glob — load_scenario
    # runs again inside run_benchmark, but we only need the count here.
    _scn_count = (
        1 if Path(scenarios_dir).is_file()
        else len(list(Path(scenarios_dir).glob("**/*.aqscenario.yml")))
    )
    _pair_count = _scn_count * len(model_list)
    click.echo(
        f"[benchmark] {_scn_count} scenarios × {len(model_list)} models = "
        f"{_pair_count} pairs",
        err=True,
    )

    # Benchmark MUST use the same BudgetConfig as production. Reading from
    # the same engine config block enforces parity — divergence would
    # silently invalidate the leaderboard.
    from aqueduct.agent import resolve_budget as _resolve_budget
    _budget = _resolve_budget(
        getattr(cfg.agent, "budget", None),
        max_reprompts=resolved_max_reprompts,
    )
    try:
        results = run_benchmark(
            scenarios_dir=Path(scenarios_dir),
            models=model_list,
            patches_dir=Path(patches_dir),
            provider=resolved_provider or "anthropic",
            base_url=resolved_base_url,
            provider_options=resolved_provider_options,
            timeout=resolved_timeout,
            max_reprompts=resolved_max_reprompts,
            engine_prompt_context=resolved_engine_prompt_context,
            workers=workers,
            budget=_budget,
        )
    except KeyboardInterrupt:
        # Per-pair results persist to benchmark.duckdb inside run_scenario
        # via Surveyor.record_benchmark_result, so completed pairs survive
        # the interrupt. Queued pairs are dropped when the executor's
        # __exit__ propagates the KeyboardInterrupt; in-flight HTTP calls
        # close their socket via the `with httpx.Client():` context
        # manager, signalling the LLM server to abort generation.
        click.echo(
            "\n↑ interrupted — completed pairs persisted to benchmark store",
            err=True,
        )
        sys.exit(130)  # SIGINT convention

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
                    "diag_score": r.diag_score,
                    "violated_guardrails": r.violated_guardrails,
                    "failures": r.failures,
                    "soft_failures": r.soft_failures,
                    "patch": (
                        r.patch.model_dump(mode="json")
                        if r.patch is not None else None
                    ),
                }
        click.echo(json.dumps(output, indent=2))
    else:
        _table = format_benchmark_table(results, model_list)
        click.echo(_table)
        # Mirror to stderr when stdout is redirected/piped so the user sees
        # the table in the terminal AND captures it in `> file`. Skip when
        # stdout is a TTY (avoid duplicate output in interactive runs).
        if not sys.stdout.isatty():
            click.echo(_table, err=True)

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
    if failed and fmt != "json":
        click.echo(
            f"({failed} failed — rerun with --format json for failure "
            f"detail + the generated patch)",
            err=True,
        )

    # ── Phase 33 Part A — persist + optional regression gate ──────────────────
    regression_exit = False
    if not no_persist:
        from aqueduct.surveyor.benchmark_store import (
            default_store_path, diff_latest, format_diff_table,
            has_regressions, persist_results,
        )
        store_path = (
            Path(store_path_override) if store_path_override
            else default_store_path(Path(scenarios_dir))
        )
        written = persist_results(results, store_path)
        if written and fmt != "json":
            click.echo(f"↳ persisted {written} benchmark row(s) → {store_path}")
        if gate_on_regression:
            diff_entries = diff_latest(results, store_path)
            if fmt == "json":
                click.echo(json.dumps({
                    "diff": [
                        {
                            "scenario_id": e.scenario_id,
                            "model": e.model,
                            "baseline_prompt_mismatch": e.baseline_prompt_mismatch,
                            "baseline_recorded_at": e.baseline.recorded_at if e.baseline else None,
                            "regressions": list(e.regressions),
                            "improvements": list(e.improvements),
                        }
                        for e in diff_entries
                    ],
                }, indent=2))
            else:
                click.echo("")
                click.echo("Regression diff vs baseline:")
                click.echo(format_diff_table(diff_entries))
            if has_regressions(diff_entries):
                regression_exit = True
                if fmt != "json":
                    click.echo(
                        "✗ regression(s) detected vs baseline — failing the gate",
                        err=True,
                    )
    elif gate_on_regression and fmt != "json":
        click.echo(
            "(--gate-on-regression ignored: --no-persist set)",
            err=True,
        )

    if failed or regression_exit:
        sys.exit(1)


# ── aqueduct benchmark-diff ──────────────────────────────────────────────────


@cli.command("benchmark-diff")
@click.option(
    "--store-path",
    "store_path_override",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the benchmark store. Default: ./.aqueduct/benchmark.duckdb",
)
@click.option(
    "--scenario",
    "scenario_filter",
    default=None,
    help="Restrict the diff to a single scenario_id.",
)
@click.option(
    "--model",
    "model_filter",
    default=None,
    help="Restrict the diff to a single model.",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["table", "json"]),
    default="table",
    show_default=True,
)
def benchmark_diff_cmd(
    store_path_override: str | None,
    scenario_filter: str | None,
    model_filter: str | None,
    fmt: str,
) -> None:
    """Diff the two most recent benchmark runs per (scenario, model) pair.

    Reads from the benchmark store written by ``aqueduct benchmark``. Does
    not re-run any scenarios — purely a store inspection. Exits non-zero if
    any pair shows a regression (passed True→False, patch_applies True→False,
    diag_score or confidence drop > 5pp).
    """
    from aqueduct.surveyor.benchmark_store import (
        _connect, _fetch_baseline, _row_from_record, _SELECT_COLS,
        DiffEntry, format_diff_table, has_regressions,
    )

    store_path = Path(store_path_override) if store_path_override else Path(".aqueduct/benchmark.duckdb")
    if not store_path.exists():
        click.echo(f"✗ benchmark store not found: {store_path}", err=True)
        sys.exit(1)

    try:
        con = _connect(store_path)
    except Exception as exc:  # noqa: BLE001
        click.echo(f"✗ cannot open benchmark store {store_path}: {exc}", err=True)
        sys.exit(1)

    where_parts: list[str] = []
    params: list = []
    if scenario_filter:
        where_parts.append("scenario_id = ?")
        params.append(scenario_filter)
    if model_filter:
        where_parts.append("model = ?")
        params.append(model_filter)
    where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    try:
        pairs = con.execute(
            f"SELECT DISTINCT scenario_id, model FROM benchmark_results {where}",
            params,
        ).fetchall()
        entries: list[DiffEntry] = []
        for sid, model in pairs:
            rec = con.execute(
                f"SELECT {_SELECT_COLS} FROM benchmark_results "
                "WHERE scenario_id = ? AND model = ? "
                "ORDER BY recorded_at DESC LIMIT 1",
                [sid, model],
            ).fetchone()
            if rec is None:
                continue
            current = _row_from_record(rec)
            baseline, prompt_mismatch = _fetch_baseline(
                con, sid, model, current.prompt_version, current.recorded_at,
            )
            if baseline is None:
                entries.append(DiffEntry(sid, model, None, current, False, (), ()))
                continue
            from aqueduct.surveyor.benchmark_store import _compare
            regs, imps = _compare(baseline, current)
            entries.append(DiffEntry(sid, model, baseline, current, prompt_mismatch, tuple(regs), tuple(imps)))
    finally:
        con.close()

    if fmt == "json":
        click.echo(json.dumps({
            "diff": [
                {
                    "scenario_id": e.scenario_id,
                    "model": e.model,
                    "baseline_prompt_mismatch": e.baseline_prompt_mismatch,
                    "baseline_recorded_at": e.baseline.recorded_at if e.baseline else None,
                    "regressions": list(e.regressions),
                    "improvements": list(e.improvements),
                }
                for e in entries
            ],
        }, indent=2))
    else:
        click.echo(format_diff_table(entries))

    if has_regressions(entries):
        if fmt != "json":
            click.echo("✗ regression(s) detected", err=True)
        sys.exit(1)


# ── aqueduct log ─────────────────────────────────────────────────────────────

@patch.command("log")
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

@patch.command("rollback")
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


# ── aqueduct stores ──────────────────────────────────────────────────────────


@cli.group("stores")
def stores_group() -> None:
    """Inspect and migrate the configured store backends (Phase 28)."""


@stores_group.command("info")
@click.option(
    "--config",
    "config_path",
    default=None,
    type=click.Path(dir_okay=False),
    help="Path to aqueduct.yml",
)
@_env_options
def stores_info(
    config_path: str | None, env_file: str | None, cli_env: tuple[str, ...]
) -> None:
    """Print each store's resolved backend + location label."""
    from aqueduct.config import ConfigError, load_config
    from aqueduct.stores import get_stores

    try:
        _resolve_and_load_env(
            env_file, Path(config_path) if config_path else None, cli_env=cli_env
        )
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(1)

    bundle = get_stores(cfg)
    rows = [
        ("observability", bundle.observability.backend, bundle.observability.location_label),
        ("lineage",       bundle.lineage.backend,       bundle.lineage.location_label),
        ("depot",         bundle.depot.backend,         bundle.depot.location_label),
    ]
    w0 = max(len(r[0]) for r in rows)
    w1 = max(len(r[1]) for r in rows)
    click.echo(f"  {'store'.ljust(w0)}  {'backend'.ljust(w1)}  location")
    click.echo(f"  {'-' * w0}  {'-' * w1}  --------")
    for store, backend, loc in rows:
        click.echo(f"  {store.ljust(w0)}  {backend.ljust(w1)}  {loc}")


@stores_group.command("migrate")
@click.option(
    "--config",
    "config_path",
    default=None,
    type=click.Path(dir_okay=False),
    help="Path to aqueduct.yml (the TARGET config — backend must already be set to postgres/redis)",
)
@click.option(
    "--from-duckdb",
    "from_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the source DuckDB file (typically `.aqueduct/depot.db`)",
)
@click.option(
    "--store",
    type=click.Choice(["depot"], case_sensitive=False),
    default="depot",
    show_default=True,
    help=(
        "Which store to migrate. v1 ships depot migration only; observability/lineage "
        "migration requires schema-aware row copying and is tracked in TODOs.md "
        "for a follow-up phase. Document the manual route: COPY each DuckDB "
        "table to Parquet, then `\\copy observability.<table> FROM 'file.parquet'` on PG."
    ),
)
@_env_options
def stores_migrate(
    config_path: str | None, from_path: str, store: str,
    env_file: str | None, cli_env: tuple[str, ...],
) -> None:
    """Copy KV rows from a DuckDB file into the configured target backend.

    Useful when promoting an existing DuckDB-backed project to Postgres or Redis
    without losing depot watermarks and counters. Idempotent: re-running upserts
    the same keys with the same values.
    """
    from aqueduct.config import ConfigError, load_config
    from aqueduct.stores import get_stores

    if store.lower() != "depot":
        click.echo(f"✗ unsupported --store: {store}", err=True)
        sys.exit(1)

    try:
        _resolve_and_load_env(
            env_file, Path(config_path) if config_path else None, cli_env=cli_env
        )
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(1)

    bundle = get_stores(cfg)
    target_label = f"{bundle.depot.backend}:{bundle.depot.location_label}"
    if bundle.depot.backend == "duckdb" and Path(bundle.depot.location_label) == Path(from_path).resolve():
        click.echo("✗ source and target depot are the same DuckDB file; nothing to migrate", err=True)
        sys.exit(1)

    try:
        import duckdb as _duckdb
    except ImportError as exc:
        click.echo(f"✗ duckdb not installed: {exc}", err=True)
        sys.exit(1)

    conn = _duckdb.connect(str(Path(from_path).resolve()), read_only=True)
    try:
        rows = conn.execute("SELECT key, value FROM depot_kv").fetchall()
    except Exception as exc:
        click.echo(f"✗ could not read depot_kv from {from_path}: {exc}", err=True)
        sys.exit(1)
    finally:
        conn.close()

    if not rows:
        click.echo(f"  source depot has 0 rows — nothing to copy ({from_path})")
        return

    for key, value in rows:
        bundle.depot.kv_put(str(key), str(value))

    click.echo(f"✓ migrated {len(rows)} depot key(s)  source={from_path}  target={target_label}")


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


if __name__ == "__main__":
    cli()