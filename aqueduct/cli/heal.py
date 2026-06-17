"""`heal` commands — extracted verbatim from aqueduct/cli/__init__.py.

No behaviour change. The click group + shared helpers come from the package;
commands register onto `cli` when imported at the bottom of __init__.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import click

from aqueduct import exit_codes
from aqueduct.cli import (
    cli,
    _apply_warnings_from_cfg,
    _resolve_and_load_env,
    _env_options,)
import aqueduct.cli as _aqcli  # noqa: E402  (monkeypatch-able helpers)


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
@click.option(
    "-s", "--set", "set_items",
    multiple=True,
    metavar="PATH=VALUE",
    help="Override an aqueduct.yml value for this run only (repeatable, "
         "in-memory). Dotted path — e.g. --set agent.model=claude-opus-4-8 "
         "--set agent.timeout=600.",
)
@_env_options
def heal(
    run_id: str | None,
    module_id: str | None,
    store_dir: str | None,
    config_path: str | None,
    patches_dir: str,
    print_prompt: str | None,
    set_items: tuple[str, ...],
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
        sys.exit(exit_codes.USAGE_ERROR)

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
        sys.exit(exit_codes.CONFIG_ERROR)

    # ── -s/--set overrides (config-only) ───────────────────────────────────────
    if set_items:
        from aqueduct.overrides import OverrideError, apply_to_model, route_overrides
        try:
            _cfg_set_nested, _ = route_overrides(set_items, allow_blueprint=False)
            cfg = apply_to_model(cfg, _cfg_set_nested)
        except OverrideError as exc:
            click.echo(f"✗ {exc}", err=True)
            sys.exit(exit_codes.CONFIG_ERROR)

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
        sys.exit(exit_codes.CONFIG_ERROR)

    patches_path = Path(patches_dir)

    # ── Live run mode ─────────────────────────────────────────────────────────
    import duckdb as _duckdb
    from aqueduct.surveyor.models import FailureContext

    observability_db = _aqcli._resolve_obs_db(cfg, store_dir, run_id=run_id)
    if observability_db is None or not observability_db.exists():
        click.echo(
            f"✗ observability.db not found for run_id={run_id!r} "
            f"(searched: --store-dir, cfg.stores.observability.path, "
            f"and .aqueduct/observability/*/observability.db)",
            err=True,
        )
        sys.exit(exit_codes.DATA_OR_RUNTIME)

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
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    (
        fc_run_id, blueprint_id, failed_module, error_message,
        stack_trace, manifest_json_raw, started_at, finished_at,
    ) = fc_row

    # Phase 39/53 — materialize blob-externalised columns transparently.
    # If the DB row stores a blob marker ("blobs/<run_id>/manifest.json.zst"),
    # load and decompress via the configured object store; otherwise inline text.
    from aqueduct.stores.object_store import make_blob_store
    _blob = make_blob_store(cfg.stores.blob.backend, cfg.stores.blob.path, observability_db.parent)
    _manifest_str = _blob.materialize(manifest_json_raw if isinstance(manifest_json_raw, str) else "")
    _stack_str = _blob.materialize(stack_trace or "")

    target_module = module_id or failed_module

    failure_ctx = FailureContext(
        run_id=fc_run_id,
        blueprint_id=blueprint_id,
        failed_module=target_module,
        error_message=error_message,
        stack_trace=_stack_str,
        manifest_json=_manifest_str,
        started_at=started_at,
        finished_at=finished_at,
    )

    # Extract guardrails and allow_defer from the persisted manifest so
    # heal-from-store paths surface the same constraints the live run would
    # have used.
    _guardrails_for_prompt: Any = None
    _allow_defer: bool = False
    try:
        _mdict = json.loads(_manifest_str) if _manifest_str else {}
        if isinstance(_mdict, dict):
            _agent_block = _mdict.get("agent") or {}
            _guardrails_for_prompt = _agent_block.get("guardrails") or None
            _allow_defer = bool(_agent_block.get("allow_defer", False))
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
        allow_defer=_allow_defer,
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
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    stage_patch_for_human(patch, patches_path, failure_ctx)
    click.echo(f"✓ patch staged → {patches_path}/pending/{patch.patch_id}.json")
    click.echo(f"  apply with: aqueduct patch apply patches/pending/{patch.patch_id}.json --blueprint <path>")

