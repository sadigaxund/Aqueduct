"""Aqueduct CLI.

Commands: init, validate, compile, run, check-config, doctor, runs, report,
          lineage, signal, heal, benchmark, log, rollback,
          patch apply, patch reject, patch commit, patch discard, patch list.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import warnings
from pathlib import Path
from typing import Any

import click

_PROJECT_ROOT_MAX_DEPTH = 8
_DEFAULT_CONFIG_FILENAME = "aqueduct.yml"

logger = logging.getLogger(__name__)


def _apply_warnings_from_cfg(cfg) -> None:
    """Merge `cfg.warnings` with the CLI flags installed by the root group.

    Idempotent. Call once per command immediately after `load_config()` so the
    engine-level `warnings.suppress` from aqueduct.yml is honoured alongside
    any `--suppress-warning` flags the user passed. Use `*` to silence all.
    """
    from aqueduct.warnings import _DEFAULT_SUPPRESS, set_default_suppress
    merged = set(_DEFAULT_SUPPRESS) | set(getattr(cfg.warnings, "suppress", []) or [])
    set_default_suppress(suppress=merged)



def _compile_with_warnings(compile_fn, *args, _verbose: bool = False, **kwargs):
    """Call compile_fn, intercept warnings, reprint as clean CLI output.

    Aqueduct's own diagnostics (AqueductWarning category, prefix
    `[aqueduct:rule_id] `) become `AQ-WARN [rule_id] <msg>` lines so the
    rule_id is easy to copy into `warnings.suppress` in aqueduct.yml.
    Non-Aqueduct UserWarnings fall back to the legacy `WARNING:` prefix.
    """
    import warnings as _w
    with _w.catch_warnings(record=True) as caught:
        _w.simplefilter("always")
        result = compile_fn(*args, **kwargs)
    from aqueduct.cli.style import emit_warnings
    emit_warnings(caught, verbose=_verbose)
    return result


def _rule(char: str = "─") -> str:
    """A horizontal rule spanning the terminal width (fallback 64)."""
    import shutil
    return char * shutil.get_terminal_size(fallback=(64, 20)).columns


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

    Phase 41: never_heal_errors patterns are regex — e.g.
    ``"IllegalStateException.*offsets"`` matches any error class
    containing "IllegalStateException" and "offsets".
    """
    import re

    error_type: str | None = getattr(failure_ctx, "error_type", None)
    stack_class: str | None = _extract_stack_class(getattr(failure_ctx, "stack_trace", None))

    candidates: set[str] = set()
    if error_type:
        candidates.add(error_type)
    if stack_class:
        candidates.add(stack_class)

    never_heal: tuple = tuple(getattr(guardrails, "never_heal_errors", ()))
    heal_on: tuple = tuple(getattr(guardrails, "heal_on_errors", ()))

    for pattern in never_heal:
        for candidate in candidates:
            try:
                if re.search(pattern, candidate):
                    return False, f"error {candidate!r} matched never_heal_errors pattern {pattern!r}"
            except re.error:
                # Degrade gracefully on malformed regex: fall back to exact match
                if pattern == candidate:
                    return False, f"error {candidate!r} matched never_heal_errors pattern {pattern!r}"

    if heal_on:
        for et in heal_on:
            if et in candidates:
                return True, ""
        matched = f"error_type={error_type!r}" if error_type else f"stack_class={stack_class!r}"
        return False, f"{matched} not in heal_on_errors whitelist"

    return True, ""


def resolve_agent_connection(engine_agent, blueprint_agent=None):
    """Merge blueprint agent connection overrides into engine defaults.

    Each connection field uses the blueprint value when set (truthy),
    falling back to the engine default.  Returns a simple object with
    resolved values that can be destructured at the call site.

    prompt_context is NOT OR‑merged — the engine and blueprint versions
    are kept separate so the agent loop can concatenate them.
    """
    class _Resolved:
        __slots__ = ("provider", "base_url", "model", "api_key", "cascade",
                      "provider_options",
                      "timeout", "max_reprompts", "engine_prompt_context",
                      "blueprint_prompt_context")

    bp = blueprint_agent
    eng = engine_agent
    r = _Resolved()
    r.provider = (bp.provider or eng.provider) if bp else eng.provider
    r.base_url = (bp.base_url or eng.base_url) if bp else eng.base_url
    r.api_key = (bp.api_key or eng.api_key) if bp else eng.api_key
    r.model = (bp.model or eng.model) if bp else eng.model
    r.provider_options = (bp.provider_options or eng.provider_options) if bp else eng.provider_options
    r.timeout = (bp.timeout or eng.timeout) if bp else eng.timeout
    r.max_reprompts = (bp.max_reprompts or eng.max_reprompts) if bp else eng.max_reprompts
    # Cascade: blueprint wins when present; fall back to engine cascade default
    from aqueduct.parser.parser import _build_cascade
    _bp_cascade = bp.cascade if bp else None
    _eng_cascade = _build_cascade(eng.cascade) if eng.cascade else None
    r.cascade = _bp_cascade if _bp_cascade else _eng_cascade
    r.engine_prompt_context = eng.prompt_context
    r.blueprint_prompt_context = bp.prompt_context if bp else None
    return r


def _resolve_project_root(
    blueprint_path: "Path | None" = None,
    config_path: "Path | None" = None,
) -> "Path":
    """Walk up from blueprint or config to find the project root.

    Returns the directory containing ``aqueduct.yml`` (the _DEFAULT_CONFIG_FILENAME) when found (walking up
    to _PROJECT_ROOT_MAX_DEPTH levels from the blueprint), or falls back to the file's immediate
    parent directory.  ``config_path``, when given, always wins — its parent
    is the project root.
    """
    from pathlib import Path as _Path
    if config_path is not None:
        return config_path.parent
    if blueprint_path is not None:
        root = blueprint_path.parent
        search = blueprint_path.parent
        for _ in range(_PROJECT_ROOT_MAX_DEPTH):
            if (search / _DEFAULT_CONFIG_FILENAME).exists():
                return search
            if search.parent == search:
                break
            search = search.parent
        return root
    return _Path.cwd()


_DEFAULT_OBS_PATH = ".aqueduct/observability.db"


def _resolve_obs_db(
    cfg,
    store_dir: str | None,
    run_id: str | None = None,
) -> Path | None:
    """Resolve the observability DB file path for a READ command.

    Mirrors the per-pipeline routing the WRITE side (``aqueduct run``) does at
    cli.py:1185-1290: when the user keeps the default
    ``.aqueduct/observability.db``, each blueprint writes to
    ``.aqueduct/observability/<blueprint_id>/observability.db``. READ commands
    (``runs``, ``report``, ``lineage``, ``heal``) need to find the right per-pipeline
    file — historically each command reinvented this with a naive
    ``Path(cfg.stores.observability.path).parent``, which only worked when the
    user explicitly set a non-default path.

    Canonical logic now lives in ``aqueduct.stores.read.resolve_duckdb_obs_path``
    (Phase 69) so every reader shares one resolver; this stays as a thin,
    monkeypatch-friendly wrapper. For backend-aware reads (DuckDB *or* Postgres),
    prefer ``aqueduct.stores.read.open_obs_read``.
    """
    from aqueduct.stores.read import resolve_duckdb_obs_path

    return resolve_duckdb_obs_path(cfg, store_dir, run_id)


def _agent_usable(provider: str, base_url: str | None, api_key: str | None = None) -> bool:
    """Return True if the LLM provider appears reachable without making a network call.

    anthropic:     requires ANTHROPIC_API_KEY in os.environ (or api_key param)
    openai_compat: requires base_url (Ollama/vLLM) OR OPENAI_API_KEY (or api_key param)
    """
    import os as _os
    if provider == "anthropic":
        return bool(api_key or _os.environ.get("ANTHROPIC_API_KEY"))
    if provider == "openai_compat":
        return bool(base_url or api_key or _os.environ.get("OPENAI_API_KEY"))
    return False


def _apply_patch_in_memory(patch, blueprint_path: Path, depot, profile, cli_overrides: dict) -> Any:
    """Apply patch operations to Blueprint without touching disk. Returns new Manifest or None."""
    try:
        from aqueduct.patch.apply import _yaml_load, apply_patch_to_dict
        from aqueduct.parser.parser import ParseError, parse_dict
        from aqueduct.compiler.compiler import compile as compiler_compile, CompileError

        bp_raw = _yaml_load(blueprint_path)
        patched = apply_patch_to_dict(bp_raw, patch)

        # Parse the patched dict directly with
        # ``base_dir`` set to the original Blueprint's parent. Replaces the
        # tempfile dance that broke 1.1.0 path anchoring whenever the
        # tempfile landed in ``/tmp`` and relative module paths resolved
        # against ``/tmp`` instead of the project root.
        base_dir = blueprint_path.parent if blueprint_path.exists() else Path.cwd()
        try:
            bp = parse_dict(
                patched,
                base_dir=base_dir,
                profile=profile,
                cli_overrides=cli_overrides or None,
            )
            return compiler_compile(bp, blueprint_path=blueprint_path, depot=depot)
        except (ParseError, CompileError):
            return None
    except Exception:
        return None


def _write_patch_to_blueprint(patch, blueprint_path: Path, patches_dir: Path, failure_ctx, mode: str,
                              obs_store=None, patch_store=None) -> Any:
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

        archive_patch(patch, patches_dir, failure_ctx, mode=mode,
                      patch_store=patch_store, obs_store=obs_store)

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
    sandbox_master_url: str | None = None,
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
        logger.warning("record_patch_simulation (lineage) failed", exc_info=True)

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
            explain_capture=explain_after,
            sandbox_master_url=sandbox_master_url,
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
        logger.warning("record_patch_simulation (sandbox) failed", exc_info=True)

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
        logger.warning("record_patch_simulation (explain) failed", exc_info=True)

    gates_passed = sandbox_res.status in ("pass", "skip")
    return lineage_res, sandbox_res, explain_res, gates_passed


def _stage_failed_patch(on_heal_failure: str, patch, patches_dir, failure_ctx, cfg, click_mod,
                        obs_store=None, patch_store=None) -> None:
    """Handle on_heal_failure policy for a patch that failed to fix the pipeline."""
    if on_heal_failure == "stage":
        from aqueduct.agent import stage_patch_for_human
        stage_patch_for_human(patch, patches_dir, failure_ctx,
                              on_patch_pending_webhook=cfg.webhooks.on_patch_pending,
                              patch_store=patch_store, obs_store=obs_store)
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


@click.group(invoke_without_command=True, no_args_is_help=False)
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

    # Bare `aqueduct` (no subcommand) → branded banner above the help.
    if ctx.invoked_subcommand is None:
        click.echo(_render_banner())
        click.echo(ctx.get_help())
        ctx.exit()


def _render_banner() -> str:
    """Small branded wordmark for the bare `aqueduct` command (not per-run)."""
    aq = click.style("aq", fg="red", bold=True)
    ueduct = click.style("ueduct", fg="yellow", bold=True)   # sand
    arches = click.style("∩∩∩", fg="cyan")
    tag = click.style("declarative · self-healing · Apache Spark", dim=True)
    ver = click.style(f"v{_aqueduct_version}", dim=True)
    return f"\n  {arches}  {aq}{ueduct}  {ver}\n  {tag}\n"





# ── patch helpers ────────────────────────────────────────────────────────────

def _uncommitted_applied_patches(
    blueprint_path: Path, patches_root: Path, blueprint_id: str | None = None
) -> list[Path]:
    """Return applied patches with applied_at newer than the last git commit for blueprint_path.

    Falls back to returning all applied patches when not in a git repo or blueprint
    has never been committed. When ``blueprint_id`` is given, only patches OWNED by
    that blueprint are considered — the ``patches/applied/`` dir is shared across a
    project, so without this filter running blueprint B would warn about (and
    mis-suggest committing) blueprint A's patches.
    """
    import subprocess

    applied_dir = patches_root / "applied"
    if not applied_dir.exists():
        return []

    all_applied = sorted(applied_dir.glob("*.json"), key=lambda f: f.stat().st_mtime)
    if not all_applied:
        return []

    # Keep only patches owned by this blueprint (via _aq_meta.blueprint_id).
    # Patches without a recorded blueprint_id are kept (conservative).
    if blueprint_id is not None:
        from aqueduct.patch.grammar import PATCH_META_KEY as _PMK
        owned = []
        for _p in all_applied:
            try:
                _d = json.loads(_p.read_text(encoding="utf-8"))
            except Exception:
                continue
            _bp = (_d.get(_PMK) or {}).get("blueprint_id")
            if _bp is None or _bp == blueprint_id:
                owned.append(_p)
        all_applied = owned
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
    from aqueduct.patch.grammar import PATCH_META_KEY
    for p in all_applied:
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        # applied_at may be top-level or inside _aq_meta
        applied_at_str = data.get("applied_at") or (data.get(PATCH_META_KEY) or {}).get("applied_at")
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
    for _ in range(_PROJECT_ROOT_MAX_DEPTH):
        if (_search / _DEFAULT_CONFIG_FILENAME).exists():
            project_root = _search
            break
        if _search.parent == _search:
            break
        _search = _search.parent
    return project_root / "patches"










if __name__ == "__main__":
    cli()

# ── extracted command families (registered + re-exported) ──────────────────────
from .benchmark import benchmark, benchmark_diff_cmd, benchmark_stats_cmd  # noqa: E402,F401
from .diagnostics import validate, lint_cmd, schema, doctor  # noqa: E402,F401
from .stores import stores_group, stores_info, stores_migrate  # noqa: E402,F401

from .run import compile, run  # noqa: E402,F401
from .patch import patch, patch_preview, patch_apply, patch_reject, patch_commit, patch_discard, patch_list, log_cmd, rollback_cmd  # noqa: E402,F401
from .observability import report, runs, lineage, signal  # noqa: E402,F401
from .heal import heal  # noqa: E402,F401
from .drift import drift  # noqa: E402,F401
from .project import completion_cmd, test_cmd, init  # noqa: E402,F401
