"""`aqueduct drift` — schema-drift detection (report-only).

Standalone, schedulable command; `aqueduct run` is untouched. Drift detection
is an early warning you can schedule ahead of the batch — the run itself
still heals reactively, after it actually fails.

Flow per Ingress:
  1. Read the live source schema metadata-only (zero actions), through each
     module's OWN resolved engine — a polyglot Blueprint's DuckDB-resolved
     Ingress modules are read via DuckDB, its Spark-resolved ones via Spark;
     never a single hardcoded engine for the whole run.
  2. Diff against the self-owned baseline (last-seen schema in `drift_checks`).
     No baseline yet → store it, report `baseline_set`.
  3. Classify: dropped/type-changed = breaking; added = benign. Both are
     recorded and printed; neither triggers a heal.

Exit codes: 0 = no drift / baseline established / only benign drift;
DATA_OR_RUNTIME = a breaking change was found, or a source could not be
read/diffed.
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
from aqueduct.cli.output import emit
from aqueduct.models import ModuleType


@cli.command()
@click.argument("blueprint", type=click.Path(exists=True, dir_okay=False))
@click.option("--config", "config_path", default=None, help="Path to aqueduct.yml")
@click.option("--store-dir", default=None, help="Observability store directory")
@click.option(
    "--module",
    "only_module",
    default=None,
    help="Limit the check to a single Ingress module id (default: all Ingress).",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
)
@_env_options
def drift(
    blueprint: str,
    config_path: str | None,
    store_dir: str | None,
    only_module: str | None,
    fmt: str,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Detect upstream schema drift and report it.

    \b
    aqueduct drift blueprints/orders.yml      # check every Ingress
    aqueduct drift blueprints/orders.yml --module raw_orders
    """
    from aqueduct.cli.render.funnel import echo as _funnel_echo
    from aqueduct.cli.render.funnel import error as _funnel_error
    from aqueduct.cli.style import error as _error
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
        _error(f"config error: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)

    try:
        bp = parse(blueprint)

        depot = None
        depots = None
        try:
            from aqueduct.depot.depot import preview_depots

            depot, depots = preview_depots(cfg, bp.id)
        except Exception as exc:  # pragma: no cover — depot build must never crash drift
            import logging as _logging

            _logging.getLogger(__name__).warning(
                "aqueduct drift: could not build preview depot (%s) — "
                "@aq.depot.*/@aq.run.prev_id will hard-fail if this Blueprint uses them",
                exc,
            )
            depot, depots = None, None

        manifest = compiler_compile(
            bp,
            blueprint_path=Path(blueprint),
            depot=depot,
            depots=depots,
            deployment_env=getattr(cfg.deployment, "env", None),
            deployment_target=getattr(cfg.deployment, "target", None),
            # `compile()` defaults `engine="spark"` when not given — omitting
            # this meant EVERY module resolved to Spark regardless of
            # `cfg.deployment.engine`, silently defeating the per-module
            # engine routing below no matter how it read `mod.engine`. Same
            # root cause as the audit's finding #2 (tmp/phase85/
            # engine_parity_audit.md, category (c)), one layer up: the
            # hardcoded engine was here, at compile time, not just in the
            # session-building code that consumed the (always-"spark")
            # result.
            engine=cfg.deployment.engine,
        )
    except (ParseError, CompileError) as exc:
        _funnel_error(f"could not compile {blueprint!r}: {exc}")
        sys.exit(exit_codes.CONFIG_ERROR)

    ingress = [m for m in manifest.modules if m.type == ModuleType.Ingress]
    if only_module:
        ingress = [m for m in ingress if m.id == only_module]
    if not ingress:
        _funnel_error(
            "no Ingress modules to check"
            + (f" (module {only_module!r} not found)" if only_module else "")
        )
        sys.exit(exit_codes.USAGE_ERROR)

    # Per-blueprint write store (mirrors `run` — must not open the routing
    # directory as a file; see resolve_obs_store_dir).
    obs = open_obs_write(cfg, manifest.blueprint_id, store_dir)
    drift_store.ensure_schema(obs)

    # ── Per-module engine session (metadata-only reads — no actions) ───────────
    # Route through EACH module's own resolved engine (`mod.engine`, set by
    # `resolve_module_engines` at compile time), not a hardcoded "spark" —
    # a DuckDB-deployed pipeline used to read schemas via a Spark session
    # regardless of what actually runs it (tmp/phase85/engine_parity_audit.md,
    # category (c) finding #2). `read_source_schema` is an `ExecutorProtocol`
    # seam built for exactly this (`aqueduct/executor/protocol.py`), and both
    # shipped engines declare `tooling.drift_schema_read: supported`
    # (capabilities.yml) — this is a routing fix, not a capability gate.
    # Sessions are built lazily, one per DISTINCT engine among the checked
    # Ingress modules (a single-engine Blueprint never pays for more than
    # one), and all closed in the top-level `finally` below.
    from aqueduct.executor.protocol import SessionSpec, get_protocol
    from aqueduct.executor.session_config import (
        resolve_session_engine_config,
        session_secrets_options,
    )

    _protocols: dict[str, Any] = {}
    _sessions: dict[str, Any] = {}

    def _session_for(engine: str) -> tuple[Any, Any]:
        if engine not in _sessions:
            protocol = get_protocol(engine)
            _protocols[engine] = protocol
            _sessions[engine] = protocol.session_factory()(
                SessionSpec(
                    blueprint_id=manifest.blueprint_id,
                    engine_config=resolve_session_engine_config(cfg, engine, manifest),
                    master_url=cfg.engine.spark.master_url if engine == "spark" else "",
                    timezone=cfg.timezone,
                    engine_options=session_secrets_options(cfg, manifest),
                )
            )
        return _protocols[engine], _sessions[engine]

    results: list[dict[str, Any]] = []
    undiffable = False
    breaking_any = False

    try:
        for mod in ingress:
            mod_engine = mod.engine or cfg.deployment.engine
            try:
                protocol, session = _session_for(mod_engine)
                if protocol.read_source_schema is None:
                    raise RuntimeError(
                        f"engine {mod_engine!r} does not support reading a live source schema "
                        "(ExecutorProtocol.read_source_schema is None for this engine)"
                    )
                live = protocol.read_source_schema(mod, session)
            except Exception as exc:
                undiffable = True
                results.append({"module": mod.id, "status": "undiffable", "error": str(exc)})
                _funnel_error(f"{mod.id}: could not read source schema — {exc}")
                continue

            baseline = drift_store.get_baseline(obs, manifest.blueprint_id, mod.id)
            if baseline is None:
                drift_store.record_check(
                    obs,
                    blueprint_id=manifest.blueprint_id,
                    module_id=mod.id,
                    baseline_schema=None,
                    live_schema=live,
                    status="baseline_set",
                )
                results.append({"module": mod.id, "status": "baseline_set", "columns": len(live)})
                # Text-format result row (--format text is the default report),
                # so this stays on stdout like the rest of the per-module rows.
                _funnel_echo(
                    f"◆ {mod.id}: baseline established ({len(live)} columns) — no prior schema to diff",
                    err=False,
                )
                continue

            result = diff_schemas(baseline, live)

            if result.has_breaking:
                breaking_any = True

            drift_store.record_check(
                obs,
                blueprint_id=manifest.blueprint_id,
                module_id=mod.id,
                baseline_schema=baseline,
                live_schema=live,
                status=result.status,
                breaking_changes=[_change_dict(c) for c in result.breaking] or None,
                benign_changes=[_change_dict(c) for c in result.benign] or None,
            )
            results.append(
                {
                    "module": mod.id,
                    "status": result.status,
                    "breaking": [c.describe() for c in result.breaking],
                    "benign": [c.describe() for c in result.benign],
                }
            )
            _echo_result(mod.id, result)
    finally:
        for _engine, _session in _sessions.items():
            try:
                _protocols[_engine].session_closer()(_session)
            except Exception:
                pass  # best-effort cleanup in a finally; the process is about to exit

    if fmt == "json":
        emit({"blueprint_id": manifest.blueprint_id, "checks": results}, fmt="json")

    if undiffable or breaking_any:
        sys.exit(exit_codes.DATA_OR_RUNTIME)
    sys.exit(exit_codes.SUCCESS)


def _change_dict(c: Any) -> dict[str, Any]:
    return {
        "column": c.column,
        "kind": c.kind,
        "baseline_type": c.baseline_type,
        "live_type": c.live_type,
    }


def _echo_result(module_id: str, result: Any) -> None:
    # Text-format result rows (--format text is the default report) — stdout,
    # routed through the funnel so a long `c.describe()` wraps on a TTY and
    # stays one full record when piped.
    from aqueduct.cli.render.funnel import echo as _funnel_echo

    if not result.has_drift:
        _funnel_echo(f"✓ {module_id}: no drift", err=False)
        return
    if result.has_breaking:
        _funnel_echo(f"⚠ {module_id}: breaking drift", err=False)
        for c in result.breaking:
            _funnel_echo(f"    · {c.describe()}", err=False)
    for c in result.benign:
        _funnel_echo(f"  ◦ {module_id}: benign — {c.describe()}", err=False)
