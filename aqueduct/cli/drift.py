"""`aqueduct drift` — proactive schema-drift detection.

Standalone, schedulable command; `aqueduct run` is untouched. Where `run`'s
self-heal is the **reactive arm** (fix after a failure), `drift` is the
**proactive arm**: schedule it ahead of the batch, and an upstream schema change
is caught and healed *before* the pipeline ever fails.

Flow per Ingress:
  1. Read the live source schema metadata-only (zero Spark actions).
  2. Diff against the self-owned baseline (last-seen schema in `drift_checks`).
     No baseline yet → store it, report `baseline_set`, no heal.
  3. Classify: dropped/type-changed = breaking; added = benign.
  4. On a breaking change, build an in-memory synthetic FailureContext and run
     the normal agent + apply-gate, staging a patch (or firing the ci webhook).

Exit codes: 0 = no drift / baseline established; HEAL_PENDING = a patch was
staged; DATA_OR_RUNTIME = a source could not be read/diffed.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import click

from aqueduct import exit_codes
from aqueduct.cli import (
    _apply_warnings_from_cfg,
    _env_options,
    _resolve_and_load_env,
    cli,
)
from aqueduct.parser.models import ModuleType


@cli.command()
@click.argument("blueprint", type=click.Path(exists=True, dir_okay=False))
@click.option("--config", "config_path", default=None, help="Path to aqueduct.yml")
@click.option("--store-dir", default=None, help="Observability store directory")
@click.option("--patches-dir", default="patches", show_default=True, help="Patch lifecycle root")
@click.option(
    "--module", "only_module", default=None,
    help="Limit the check to a single Ingress module id (default: all Ingress).",
)
@click.option(
    "--format", "fmt", type=click.Choice(["text", "json"]), default="text", show_default=True,
)
@_env_options
def drift(
    blueprint: str,
    config_path: str | None,
    store_dir: str | None,
    patches_dir: str,
    only_module: str | None,
    fmt: str,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Detect upstream schema drift and pre-emptively heal it.

    \b
    aqueduct drift blueprints/orders.yml      # check every Ingress
    aqueduct drift blueprints/orders.yml --module raw_orders
    """
    import json as _json

    from aqueduct.compiler.compiler import CompileError
    from aqueduct.compiler.compiler import compile as compiler_compile
    from aqueduct.config import ConfigError, load_config
    from aqueduct.drift import store as drift_store
    from aqueduct.drift.classifier import diff_schemas
    from aqueduct.parser.parser import ParseError, parse
    from aqueduct.stores.read import open_obs_write

    try:
        _resolve_and_load_env(env_file, Path(config_path) if config_path else None, cli_env=cli_env)
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)

    try:
        bp = parse(blueprint)
        manifest = compiler_compile(
            bp, blueprint_path=Path(blueprint),
            deployment_env=getattr(cfg.deployment, "env", None),
            deployment_target=getattr(cfg.deployment, "target", None),
        )
    except (ParseError, CompileError) as exc:
        click.echo(f"✗ could not compile {blueprint!r}: {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)

    ingress = [m for m in manifest.modules if m.type == ModuleType.Ingress]
    if only_module:
        ingress = [m for m in ingress if m.id == only_module]
    if not ingress:
        click.echo("✗ no Ingress modules to check" + (f" (module {only_module!r} not found)" if only_module else ""), err=True)
        sys.exit(exit_codes.USAGE_ERROR)

    # Per-blueprint write store (mirrors `run` — must not open the routing
    # directory as a file; see resolve_obs_store_dir).
    obs = open_obs_write(cfg, manifest.blueprint_id, store_dir)
    drift_store.ensure_schema(obs)

    manifest_json = _json.dumps(manifest.to_dict())

    # ── Spark session (metadata-only reads — no actions) ───────────────────────
    from aqueduct.executor.spark.ingress import read_source_schema
    from aqueduct.executor.spark.session import make_spark_session

    merged_spark_config = {**cfg.spark_config, **manifest.spark_config}
    session = make_spark_session(manifest.blueprint_id, merged_spark_config, master_url=cfg.deployment.master_url)

    results: list[dict[str, Any]] = []
    undiffable = False
    staged_any = False

    try:
        for mod in ingress:
            try:
                live = read_source_schema(mod, session)
            except Exception as exc:
                undiffable = True
                results.append({"module": mod.id, "status": "undiffable", "error": str(exc)})
                click.echo(f"✗ {mod.id}: could not read source schema — {exc}", err=True)
                continue

            baseline = drift_store.get_baseline(obs, manifest.blueprint_id, mod.id)
            if baseline is None:
                drift_store.record_check(
                    obs, blueprint_id=manifest.blueprint_id, module_id=mod.id,
                    baseline_schema=None, live_schema=live, status="baseline_set",
                )
                results.append({"module": mod.id, "status": "baseline_set", "columns": len(live)})
                click.echo(f"◆ {mod.id}: baseline established ({len(live)} columns) — no prior schema to diff")
                continue

            result = diff_schemas(baseline, live)
            patch_id = None

            if result.has_breaking:
                try:
                    _src_yaml = Path(blueprint).read_text(encoding="utf-8")
                except Exception:
                    _src_yaml = None
                patch_id = _heal_drift(
                    cfg, manifest.blueprint_id, mod.id, result, manifest_json, Path(patches_dir),
                    blueprint_source_yaml=_src_yaml,
                )
                if patch_id:
                    staged_any = True

            drift_store.record_check(
                obs, blueprint_id=manifest.blueprint_id, module_id=mod.id,
                baseline_schema=baseline, live_schema=live, status=result.status,
                breaking_changes=[_change_dict(c) for c in result.breaking] or None,
                benign_changes=[_change_dict(c) for c in result.benign] or None,
                patch_id=patch_id,
            )
            results.append({
                "module": mod.id,
                "status": result.status,
                "breaking": [c.describe() for c in result.breaking],
                "benign": [c.describe() for c in result.benign],
                "patch_id": patch_id,
            })
            _echo_result(mod.id, result, patch_id)
    finally:
        try:
            session.stop()
        except Exception:
            pass

    if fmt == "json":
        click.echo(_json.dumps({"blueprint_id": manifest.blueprint_id, "checks": results}, indent=2))

    if undiffable:
        sys.exit(exit_codes.DATA_OR_RUNTIME)
    if staged_any:
        sys.exit(exit_codes.HEAL_PENDING)
    sys.exit(0)


def _change_dict(c: Any) -> dict[str, Any]:
    return {"column": c.column, "kind": c.kind, "baseline_type": c.baseline_type, "live_type": c.live_type}


def _echo_result(module_id: str, result: Any, patch_id: str | None) -> None:
    if not result.has_drift:
        click.echo(f"✓ {module_id}: no drift")
        return
    if result.has_breaking:
        click.echo(f"⚠ {module_id}: breaking drift")
        for c in result.breaking:
            click.echo(f"    · {c.describe()}")
        if patch_id:
            click.echo(f"  → patch staged: {patch_id}")
        else:
            click.echo("  → no patch (agent disabled or failed to produce one)")
    for c in result.benign:
        click.echo(f"  ◦ {module_id}: benign — {c.describe()} (no heal)")


def _heal_drift(
    cfg: Any,
    blueprint_id: str,
    module_id: str,
    result: Any,
    manifest_json: str,
    patches_path: Path,
    blueprint_source_yaml: str | None = None,
) -> str | None:
    """Run the agent on a synthetic drift FailureContext; stage a patch. Returns patch_id."""
    from aqueduct.agent import generate_agent_patch, resolve_budget, stage_patch_for_human
    from aqueduct.drift.context import build_synthetic_failure_context

    eng = cfg.agent
    if eng.model is None:
        click.echo(f"  (agent disabled — set agent.model to auto-heal {module_id!r})", err=True)
        return None

    failure_ctx = build_synthetic_failure_context(
        blueprint_id, module_id, result, manifest_json,
        blueprint_source_yaml=blueprint_source_yaml,
    )
    budget = resolve_budget(getattr(eng, "budget", None), max_reprompts=eng.max_reprompts)

    agent_result = generate_agent_patch(
        failure_ctx,
        model=eng.model,
        patches_dir=patches_path,
        provider=eng.provider or "anthropic",
        base_url=eng.base_url,
        provider_options=eng.provider_options,
        timeout=eng.timeout,
        max_reprompts=eng.max_reprompts,
        engine_prompt_context=eng.prompt_context,
        budget=budget,
        allow_defer=False,
    )
    if agent_result.patch is None:
        return None
    stage_patch_for_human(agent_result.patch, patches_path, failure_ctx)
    return agent_result.patch.patch_id
