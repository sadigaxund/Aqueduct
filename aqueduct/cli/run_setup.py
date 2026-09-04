"""`run` command setup phases — extracted verbatim from aqueduct/cli/run.py
(Phase 85 Wave 5 split).

Config load (`_load_engine_config`) → parse/compile (`_do_compile`) →
surveyor/session bootstrap (`_setup_surveyor`) is the `run()` command's
sequential setup pipeline, already broken into three module-level functions
(T18) with explicit parameter lists and dataclass return bundles — none of
them close over `run()`'s locals (module-level functions structurally
cannot), so moving them here is a pure relocation, no behaviour change.

`_SessionHolder` (the mutable single-slot box `_setup_surveyor` builds and
`run()` threads through every session consumer — see its own docstring for
the invariant it exists to hold) moves with `_setup_surveyor` since it is
only ever instantiated there. `run()` still receives it via
`_SurveyorSetupResult.session_holder` and must keep reading `.session` off
that ONE shared instance, never a captured local — this module does not
change that contract, only relocates the class definition.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass as _dc_frozen
from typing import TYPE_CHECKING as _t

import click

from aqueduct import exit_codes

if _t:
    from aqueduct.config import AqueductConfig, WebhookEndpointConfig
    from aqueduct.executor.probe_sampling import ProbeSampling as _PS


def require_sandbox_for_chained_healing(max_patches: int, sandbox_mode: str) -> None:
    """Refuse to chain multi-patch healing with sandbox validation disabled.

    Phase 92 — chained (progressive) multi-patch healing is now the ONLY
    heal-loop behavior; a single-attempt heal (``agent.max_patches`` left at
    its default of 1) never needs to validate a candidate mid-chain, so
    ``agent.sandbox_mode: off`` stays legal there (already gated behind
    ``danger.allow_skip_sandbox`` separately). Chaining is what actually
    needs per-link validation — each link's advancement test IS the
    in-memory-apply + sandbox gate, and without a sandbox a chain link has
    no way to validate a candidate before folding it into the accumulated
    patch. So the refusal is scoped to ``max_patches > 1``, not unconditional.
    Raises ``ConfigError`` rather than silently chaining unsafely.
    """
    from aqueduct.errors import ConfigError

    if max_patches > 1 and sandbox_mode == "off":
        raise ConfigError(
            f"agent.max_patches={max_patches} (>1) enables chained multi-patch "
            "healing, which requires per-link sandbox validation, but "
            "agent.sandbox_mode: off disables sandbox replay entirely. Set "
            "sandbox_mode to 'sample' (default) or 'preflight', or leave "
            "max_patches at 1 for a single-attempt heal."
        )


@_dc_frozen(frozen=True)
class _LoadConfigResult:
    """Return-type bundle for ``_load_engine_config`` — all values derived from
    config/env/CLI resolution, before parse/compile/execute."""

    cfg: AqueductConfig
    resolved_store_dir: str | None
    resolved_webhook: WebhookEndpointConfig | None
    engine: str
    master_url: str
    probe_sampling: _PS
    blueprint_set_nested: dict
    _using_default_obs_path: bool
    _obs_routing_base: str
    execute: object  # get_executor callable — deferred type
    blueprint_str: str  # = str(blueprint_abs)
    danger_pairs: tuple = ()  # (rule_id, message) — emitted by the caller after
    # the `· env/overrides/secrets ·` preamble so info lines stay grouped


def _load_engine_config(
    blueprint_abs,
    config_path_abs,
    store_dir_abs,
    webhook,
    set_items,
    env_file,
    cli_env,
    _project_root,
):
    """Phase 1 — config load + env + --set overrides → ``_LoadConfigResult``.

    Extracted from the ``run()`` god-function (T18).  No behaviour change.
    """
    import sys as _sys

    from aqueduct.cli import _resolve_and_load_env as _renv
    from aqueduct.cli.render.style import error as _err
    from aqueduct.cli.render.style import warn as _warn

    # ── .env loading ───────────────────────────────────────────────────────────
    _renv(env_file, _project_root / blueprint_abs.name, cli_env=cli_env)
    blueprint_str = str(blueprint_abs)

    # ── Load engine config ─────────────────────────────────────────────────────
    try:
        from aqueduct.config import ConfigError, WebhookEndpointConfig
        from aqueduct.config import load_config as _load_cfg

        cfg = _load_cfg(config_path_abs)
        from aqueduct.cli import _apply_warnings_from_cfg

        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        _err(f"config error: {exc}")
        _sys.exit(exit_codes.CONFIG_ERROR)

    # ── -s/--set overrides (top precedence, in-memory) ──────────────────────────
    blueprint_set_nested: dict = {}
    if set_items:
        from aqueduct.overrides import OverrideError, apply_to_model, route_overrides

        try:
            _config_set_nested, blueprint_set_nested = route_overrides(
                set_items, allow_blueprint=True
            )
            cfg = apply_to_model(cfg, _config_set_nested)
            if _config_set_nested:
                # `--set` overlays AFTER `load_config()` already ran its own
                # governance gates, so it bypasses both: re-run them here so
                # `--set stores.observability.backend=postgres` without the
                # extra installed gets the same ConfigError + install hint the
                # file-based path gets (not a bare ImportError at first
                # store use), and an engine-ignored `--set engine.<x>.*` key
                # still emits the suppressible `engine_key_ignored` warning.
                from aqueduct.config import _validate_store_backends, _warn_ignored_config_keys

                _validate_store_backends(cfg.stores)
                _warn_ignored_config_keys(cfg)
        except OverrideError as exc:
            _err(str(exc))
            _sys.exit(exit_codes.CONFIG_ERROR)
        except ConfigError as exc:
            _err(f"config error: {exc}")
            _sys.exit(exit_codes.CONFIG_ERROR)
        if _config_set_nested.get("danger"):
            _warn(
                f"--set DANGER override(s) (single-run, NOT persisted): "
                f"{_config_set_nested['danger']}",
                err=True,
            )

    # ── Store dir resolution ───────────────────────────────────────────────────
    # 2.0: the duckdb observability path is a routing DIRECTORY only (config
    # load rejects `.db`-suffixed paths) — per-blueprint files always live at
    # `<base>/<blueprint_id>/observability.db`. `--store-dir` bypasses routing.
    from aqueduct.config import DEFAULT_OBS_ROUTING_ROOT

    _using_default_obs_path = False
    _obs_routing_base = DEFAULT_OBS_ROUTING_ROOT
    if store_dir_abs:
        resolved_store_dir = store_dir_abs
    else:
        resolved_store_dir = None
        _observability_path = cfg.stores.observability.path
        if cfg.stores.observability.backend == "duckdb":
            _using_default_obs_path = True
            if _observability_path is not None:
                _obs_routing_base = _observability_path

    resolved_webhook = WebhookEndpointConfig(url=webhook) if webhook else cfg.webhooks.on_failure
    engine = cfg.deployment.engine
    master_url = cfg.engine.spark.master_url

    # ── Danger settings startup warning ──────────────────────────────────────
    danger_pairs = []
    if cfg.danger.allow_full_probe_actions:
        danger_pairs.append(
            (
                "danger-full-probe-actions",
                "allow_full_probe_actions=true — full Spark actions in Probes enabled",
            )
        )
    if cfg.danger.allow_multi_patch:
        danger_pairs.append(
            (
                "danger-multi-patch",
                "allow_multi_patch=true — successive LLM patches without human review",
            )
        )
    if cfg.danger.allow_full_preflight:
        danger_pairs.append(
            (
                "danger-full-preflight",
                "allow_full_preflight=true — full-dataset sandbox replay (no Egress writes)",
            )
        )
    if cfg.danger.allow_skip_sandbox:
        danger_pairs.append(
            (
                "danger-skip-sandbox",
                "allow_skip_sandbox=true — patches go straight to production, no sandbox",
            )
        )
    if cfg.danger.allow_command_hooks:
        danger_pairs.append(
            (
                "danger-command-hooks",
                "allow_command_hooks=true — blueprint `command:` hooks run arbitrary subprocesses",
            )
        )
    # Emission deferred to the caller (after the info-line preamble) so the
    # `⚠ danger` block doesn't interleave the dim `· env/overrides/secrets ·` lines.

    # ── Executor resolve ──────────────────────────────────────────────────────
    # Phase 78 Step 2: get_executor() resolves through the aqueduct.engines
    # registry (ExecutorProtocol) and raises UnknownEngineError (an
    # AqueductError) for an unregistered engine — kept alongside
    # NotImplementedError/ValueError so any pre-registration-seam caller that
    # still raises those (or a future engine that does) reports the same
    # clean CONFIG_ERROR exit instead of an unhandled crash.
    try:
        from aqueduct.errors import AqueductError
        from aqueduct.executor import get_executor

        execute = get_executor(engine)
    except (NotImplementedError, ValueError, AqueductError) as exc:
        _err(f"engine error: {exc}")
        _sys.exit(exit_codes.CONFIG_ERROR)

    # ── Probe sampling ────────────────────────────────────────────────────────
    from aqueduct.executor.probe_sampling import ProbeSampling

    probes_cfg = cfg.probes
    probe_sampling = ProbeSampling(
        max_sample_rows=probes_cfg.max_sample_rows,
        default_sample_fraction=probes_cfg.default_sample_fraction,
    )

    return _LoadConfigResult(
        cfg=cfg,
        resolved_store_dir=resolved_store_dir,
        resolved_webhook=resolved_webhook,
        engine=engine,
        master_url=master_url,
        probe_sampling=probe_sampling,
        blueprint_set_nested=blueprint_set_nested,
        _using_default_obs_path=_using_default_obs_path,
        _obs_routing_base=_obs_routing_base,
        execute=execute,
        blueprint_str=blueprint_str,
        danger_pairs=tuple(danger_pairs),
    )


@_dc_frozen(frozen=True)
class _CompileResult:
    """Return-type bundle for ``_do_compile`` — parse + compile → manifest + store wiring."""

    manifest: object  # Manifest
    bundle: object  # StoreBundle
    depot: object  # DepotStore
    depots_wrapped: dict
    execution_date: object
    cli_overrides: dict
    compile_warnings: list  # captured AQ-WARN records, emitted after the run header
    depot_reads: dict  # depot keys resolved during this compile's Tier 1 (Gate 3 staleness notice)


def _do_compile(
    blueprint,
    profile,
    ctx,
    execution_date_str,
    store_dir_abs,
    cfg,
    verbosity,
    blueprint_set_nested,
):
    """Phase 2 — parse blueprint + build stores + compile → ``_CompileResult``."""
    try:
        import sys as _sys
        from pathlib import Path as _P

        from aqueduct.cli import _compile_with_warnings
        from aqueduct.cli.render.style import error as _err
        from aqueduct.compiler.compiler import CompileError
        from aqueduct.compiler.compiler import compile as compiler_compile
        from aqueduct.depot.depot import DepotStore as _DS
        from aqueduct.parser.parser import ParseError
        from aqueduct.parser.parser import parse as _parse
    except ImportError as exc:
        raise RuntimeError(f"compile dependencies missing: {exc}") from exc

    cli_overrides: dict[str, str] = {}
    for item in ctx:
        if "=" not in item:
            click.echo(f"--ctx flag must be KEY=VALUE, got: {item!r}", err=True)
            _sys.exit(exit_codes.USAGE_ERROR)
        k, _, v = item.partition("=")
        cli_overrides[k.strip()] = v

    # ── Parse --execution-date ─────────────────────────────────────────────────
    execution_date = None
    if execution_date_str:
        from datetime import date as _date

        try:
            execution_date = _date.fromisoformat(execution_date_str)
        except ValueError:
            click.echo(
                f"\u2717 --execution-date must be YYYY-MM-DD, got: {execution_date_str!r}", err=True
            )
            _sys.exit(exit_codes.USAGE_ERROR)

    # ── Parse ──────────────────────────────────────────────────────────────────
    try:
        if blueprint_set_nested:
            import yaml as _yaml

            from aqueduct.overrides import deep_merge as _deep_merge
            from aqueduct.parser.parser import parse_dict

            _raw_bp = _yaml.safe_load(_P(blueprint).read_text(encoding="utf-8")) or {}
            _raw_bp = _deep_merge(_raw_bp, blueprint_set_nested)
            bp = parse_dict(
                _raw_bp,
                base_dir=_P(blueprint).parent,
                profile=profile,
                cli_overrides=cli_overrides or None,
            )
        else:
            bp = _parse(blueprint, profile=profile, cli_overrides=cli_overrides or None)
    except ParseError as exc:
        _err(f"parse error: {exc}")
        _sys.exit(exit_codes.CONFIG_ERROR)

    # ── Build per-run store bundle ─────────────────────────────────────────────
    from aqueduct.stores import get_stores

    bundle = get_stores(cfg, store_dir_override=store_dir_abs, blueprint_id=bp.id)
    depot = _DS(backend=bundle.depot)
    depots_wrapped = {n: _DS(backend=s) for n, s in bundle.depots.items()}

    # ── Compile ────────────────────────────────────────────────────────────────
    depot_reads: dict[str, str] = {}
    try:
        manifest, compile_warnings = _compile_with_warnings(
            compiler_compile,
            bp,
            blueprint_path=_P(blueprint),
            depot=depot,
            depots=depots_wrapped,
            execution_date=execution_date,
            secrets_provider=cfg.secrets.provider,
            secrets_region=cfg.secrets.region,
            secrets_resolver=cfg.secrets.resolver,
            deployment_env=getattr(cfg.deployment, "env", None),
            deployment_target=getattr(cfg.deployment, "target", None),
            engine=getattr(cfg.deployment, "engine", "spark"),
            depot_reads_out=depot_reads,
            _verbose=verbosity >= 1,
            _defer=True,  # emit after the run header (tier-2 blueprint warnings)
        )
    except CompileError as exc:
        _err(f"compile error: {exc}")
        _sys.exit(exit_codes.CONFIG_ERROR)

    return _CompileResult(
        manifest=manifest,
        bundle=bundle,
        depot=depot,
        depots_wrapped=depots_wrapped,
        execution_date=execution_date,
        cli_overrides=cli_overrides,
        compile_warnings=compile_warnings,
        depot_reads=depot_reads,
    )


class _SessionHolder:
    """Mutable single-slot box for the run's CURRENT single-engine session,
    plus the fingerprint of the config it was built from.

    ``_execute_target`` (below) rebuilds the session whenever the manifest it
    is about to execute would resolve a DIFFERENT ``engine_config`` than the
    one the LIVE session carries — never execute a Manifest on a session
    built from a different Manifest (cross-engine remediation; see
    ``session_config_fingerprint`` in ``aqueduct/executor/session_config.py``
    for what goes into that comparison and why). This subsumes the earlier
    Phase 82 fix (``_rebuild_session_for_patch``, since removed), which only
    rebuilt before a PATCHED retry — it never caught the matching bug in the
    other direction: the outer heal loop's baseline re-execution of the
    ORIGINAL manifest, at the top of ``while True:``, running on whatever
    session a FAILED patch left behind. Two independent consumers need to
    observe whichever session is CURRENT at the moment they run, never the
    one that existed when they were defined:

      - the ``atexit`` closer registered in ``_setup_surveyor`` — a plain
        ``atexit.register(lambda: close(session))`` closing over a local
        variable would freeze on whatever session existed when
        ``_setup_surveyor`` returned (that function's own ``session`` name
        is a dead alias by the time a heal retry rebuilds one), so the
        pre-patch session would leak (never closed) and the rebuilt one
        would double-close nothing;
      - every session consumer inside the ``run`` command itself
        (``_execute_target``, the terminal ``on_success``/``on_failure``
        hooks, the agentic ``ToolBox``) — reading a bare local ``session``
        variable would work for these (ordinary late-binding closure
        semantics inside the SAME function), but mixing "read the holder"
        and "read the local var" is the kind of inconsistency this class
        exists to rule out; every consumer reads ``.session`` off ONE
        shared instance instead.

    A frozen dataclass field (``_SurveyorSetupResult.session_holder``) can
    hold a reference to this mutable object without violating that
    dataclass's own immutability — only the REFERENCE is frozen, not what
    it points at.
    """

    __slots__ = ("session", "engine_config_fingerprint")

    def __init__(
        self, session: object = None, engine_config_fingerprint: str | None = None
    ) -> None:
        self.session = session
        # The fingerprint the CURRENT `.session` was built from — `None`
        # until a single-engine session is actually built (never set for a
        # polyglot run, which never builds one through this holder at all).
        self.engine_config_fingerprint = engine_config_fingerprint


@_dc_frozen(frozen=True)
class _SurveyorSetupResult:
    """Return-type bundle for ``_setup_surveyor`` — surveyor, session, agent config, etc."""

    resolved_store_dir: object
    patches_dir: object
    run_id: str
    approval_mode: str
    max_patches: int
    _is_multi_patch: bool
    resolved_agent_provider: str | None
    resolved_agent_base_url: str | None
    resolved_agent_model: str | None
    resolved_agent_provider_options: object | None
    resolved_agent_timeout: int | None
    resolved_agent_max_reprompts: int | None
    resolved_agent_api_key: str | None
    resolved_agent_engine_prompt_context: str | None
    resolved_agent_blueprint_prompt_context: str | None
    resolved_agent_cascade: object | None
    resolved_agent_mode: str | None
    resolved_agent_max_tool_calls: int | None
    resolved_agent_supports_tools: object | None
    resolved_sandbox_master_url: str | None
    surveyor: object
    _obs_store: object
    _patch_store: object
    session_holder: object  # _SessionHolder — see class docstring above
    bundle: object
    depot: object
    _r: object  # click.style rule for banner


# `resolve_session_engine_config`/`session_secrets_options` moved to
# `aqueduct/executor/session_config.py` (Phase 82 remediation) so the patch
# preview sandbox gate (`aqueduct/patch/preview.py::run_sandbox_gate`) can
# build a ``SessionSpec`` through the SAME resolver this module uses, instead
# of a second engine-config resolution path that only ever saw Spark's
# merged conf. Imported at each use site below (`_setup_surveyor`,
# `_execute_target`) per this file's existing lazy-import convention.


def _setup_surveyor(
    resolved_store_dir,
    manifest,
    cfg,
    _obs_routing_base,
    _using_default_obs_path,
    verbosity,
    allow_multi_patch_flag,
    _project_root,
    blueprint_str,
    run_id,
    from_module,
    to_module,
    execution_date,
    engine,
    master_url,
    resolved_webhook,
    bundle,
    depot,
    compile_warnings,
):
    """Phase 3 — warnings, gates, surveyor creation, engine session → ``_SurveyorSetupResult``."""
    import sys as _sys
    import uuid as _uuid
    import warnings as _w
    from pathlib import Path as _P

    with _w.catch_warnings(record=True) as _setup_caught:
        _w.simplefilter("always")

        # ── Resolve per-pipeline store dir (needs blueprint_id from manifest) ──
        if resolved_store_dir is None:
            resolved_store_dir = _P(_obs_routing_base) / manifest.blueprint_id
            resolved_store_dir.mkdir(parents=True, exist_ok=True)

        # ── Cluster-mode store path warning ───────────────────────────────────────
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
        _max_patches = manifest.agent.max_patches if manifest.agent else 1
        _mode = manifest.agent.approval_mode if manifest.agent else "disabled"
        _is_multi_patch = _mode == "auto" and _max_patches > 1
        if _is_multi_patch and not allow_multi_patch_flag:
            if not cfg.danger.allow_multi_patch:
                click.echo(
                    f"\u2717 max_patches={_max_patches} (>1) requires danger.allow_multi_patch: true "
                    "in aqueduct.yml, or pass --allow-multi-patch for this run.",
                    err=True,
                )
                _sys.exit(exit_codes.CONFIG_ERROR)

        # ── Sandbox-mode danger gates ─────────────────────────────────────────────
        _sandbox_mode = manifest.agent.sandbox_mode if manifest.agent else "sample"
        if _sandbox_mode == "preflight" and not cfg.danger.allow_full_preflight:
            click.echo(
                "\u2717 agent.sandbox_mode: preflight requires danger.allow_full_preflight: true "
                "in aqueduct.yml (full-dataset sandbox replay).",
                err=True,
            )
            _sys.exit(exit_codes.CONFIG_ERROR)
        if _sandbox_mode == "off" and not cfg.danger.allow_skip_sandbox:
            click.echo(
                "\u2717 agent.sandbox_mode: off requires danger.allow_skip_sandbox: true "
                "in aqueduct.yml (skips pre-apply validation; patches hit real data).",
                err=True,
            )
            _sys.exit(exit_codes.CONFIG_ERROR)
        if _sandbox_mode == "preflight":
            click.echo(
                "\u26a0 sandbox mode: preflight (full-dataset replay, no Egress) \u2014 slow but conclusive",
                err=True,
            )
        elif _sandbox_mode == "off":
            click.echo(
                "\u26a0 DANGER: sandbox mode = off (skipping pre-apply replay; patches apply to real data)",
                err=True,
            )
        if _sandbox_mode == "off" and _is_multi_patch:
            click.echo(
                "\u26a0 DANGER COMBO: sandbox_mode=off + max_patches > 1 \u2014 every Agent patch "
                f"applies to real data without pre-validation, up to max_patches="
                f"{_max_patches} times per failure. Use only when you "
                "fully trust the model and blueprint scope is tiny.",
                err=True,
            )

        # \u2500\u2500 Chained healing requires per-link sandbox validation \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
        from aqueduct.errors import ConfigError as _ConfigError

        try:
            require_sandbox_for_chained_healing(_max_patches, _sandbox_mode)
        except _ConfigError as _chain_sandbox_exc:
            click.echo(f"\u2717 {_chain_sandbox_exc}", err=True)
            _sys.exit(exit_codes.CONFIG_ERROR)

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
                    f"\u2717 blocked \u2014 {_np} pending {_noun} unreviewed: {names}\n"
                    f"  Review: aqueduct patch apply <file> --blueprint {blueprint_str}\n"
                    f"  Reject: aqueduct patch reject <patch_id> --reason '...'",
                    err=True,
                )
                _sys.exit(exit_codes.CONFIG_ERROR)
            elif policy == "warn":
                click.echo(
                    click.style(f"\u26a0 {_np} pending {_noun} unreviewed", fg="yellow", bold=True)
                    + click.style("  \u00b7  aqueduct patch list", dim=True),
                    err=True,
                )
                if verbosity >= 1:
                    for p in pending_patches:
                        click.echo(f"  \u00b7 {p.stem}", err=True)

        # ── Uncommitted applied patch warning ──────────────────────────────────────
        from aqueduct.cli import _uncommitted_applied_patches

        uncommitted_applied = _uncommitted_applied_patches(
            _P(blueprint_str), patches_dir, blueprint_id=manifest.blueprint_id
        )
        if uncommitted_applied:
            n_uc = len(uncommitted_applied)
            _noun = "patch" if n_uc == 1 else "patches"
            click.echo(
                click.style(f"\u26a0 {n_uc} applied {_noun} uncommitted", fg="yellow", bold=True)
                + click.style(
                    f"  \u00b7  aqueduct patch commit --blueprint {_P(blueprint_str).name}",
                    dim=True,
                ),
                err=True,
            )

        run_id = run_id or str(_uuid.uuid4())
        selector_note = ""
        if from_module or to_module:
            parts = []
            if from_module:
                parts.append(f"from={from_module}")
            if to_module:
                parts.append(f"to={to_module}")
            selector_note = "  [" + ", ".join(parts) + "]"
        exec_date_note = f"  exec_date={execution_date}" if execution_date else ""
        from aqueduct.cli import _rule
        from aqueduct.cli.render.style import dim as _dim

        _r = _dim(_rule())
    from aqueduct.cli.render.style import emit_warnings as _emit_warnings

    # \u2500\u2500 Header \u2014 the divider between engine/setup context (above) and this run \u2500\u2500
    # Explicitly stdout: the header + tree + closing divider + verdict form
    # ONE coherent "final result" block that must survive `> run.log` intact.
    click.echo(_r, err=False)
    _arrow = click.style("\u25b6", fg="cyan", bold=True)
    _bp_label = click.style(manifest.blueprint_id, bold=True)
    # A polyglot Manifest (>1 island) names every engine actually involved,
    # not the single `deployment.engine` default \u2014 `master_url` is a
    # single-session concept (each island's own session may ignore it, e.g.
    # DuckDB) so it's dropped from this line for a polyglot run rather than
    # implying it applies uniformly. Single-engine (the common case) is
    # untouched: same `{engine} {master_url}` this line has always shown.
    if len(manifest.islands) > 1:
        _island_engines_hdr = "+".join(sorted({isl.engine for isl in manifest.islands}))
        _engine_desc = f"{_island_engines_hdr}  ({len(manifest.islands)} islands)"
    else:
        _engine_desc = f"{engine} {master_url}"
    click.echo(
        f"{_arrow} "
        f"{_bp_label}  \u00b7  "
        f"{len(manifest.modules)} modules  \u00b7  run {run_id}  \u00b7  {_engine_desc}"
        f"{selector_note}{exec_date_note}",
        err=False,
    )
    click.echo(_r, err=False)

    # Tier 2 \u2014 blueprint + session warnings AFTER the header (the header names the
    # blueprint they are about). Engine/config-level warnings already printed
    # above the header; runtime probe/assert warnings come later, during execution.
    _emit_warnings(compile_warnings, verbose=verbosity >= 1, err=True, label="compile:")
    _emit_warnings(_setup_caught, verbose=verbosity >= 1, err=True, label="session:")

    # The blueprint's compile warnings are now shown once (grouped). The run
    # re-parses/re-compiles the SAME blueprint several times after this point —
    # heal re-runs, sandbox gates — each of which would
    # otherwise re-emit those identical warnings through the raw `AQ-WARN [...]`
    # fallback formatter (they escape the initial catch_warnings block). Suppress
    # AqueductWarning for the rest of this run so they never leak mid-execution.
    # Runtime probe/assert warnings use logger.warning (not AqueductWarning) and
    # are unaffected.
    #
    # KNOWN SCOPING GAP (audit 2026-08-01): this is a category-wide, process-global
    # filter with no matching restore, so it also swallows any AqueductWarning a
    # LATER phase raises for a genuinely new reason (not a re-emission of the
    # already-shown compile warnings) — e.g. a patch that introduces a new compiler
    # warning during a heal re-compile. A correct fix scopes this to "already-shown
    # message text only" or wraps the remainder of `run()` in
    # `warnings.catch_warnings()` so the filter reverts when the command ends; both
    # require either re-indenting the rest of this (very long) function or changing
    # how heal/gates/sandbox (`aqueduct/agent/`, `aqueduct/patch/preview.py` — both
    # outside this batch's surface) capture their own recompile warnings, so it is
    # not done here. `filterwarnings` (used below) is at least non-destructive to
    # OTHER pre-existing filters, unlike `simplefilter`, which clears the entire
    # filter list first.
    import warnings as _wmod

    from aqueduct.warnings import AqueductWarning as _AqWarning

    _wmod.filterwarnings("ignore", category=_AqWarning)

    # ── Resolve agent connection (engine defaults \u2190 blueprint overrides) ────
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
    resolved_agent_cascade = _rac.cascade
    # Phase 75 — agentic mode + tool-use capability, same engine←blueprint
    # inheritance shape as every other resolved_agent_* field above.
    resolved_agent_mode = _rac.mode
    resolved_agent_max_tool_calls = _rac.max_tool_calls
    resolved_agent_supports_tools = _rac.supports_tools
    resolved_sandbox_master_url = cfg.agent.sandbox_master_url

    # ── Self-healing reachability pre-check (upfront) ────────────────────────────
    # Surface a misconfigured agent at startup rather than only at heal time. Gated
    # on the blueprint actually OPTING INTO healing — `agent.approval` is set to a
    # non-disabled mode (human/auto/ci). The default is `disabled` (healing off),
    # so a blueprint with no `agent:` block — or one that only configures budget/
    # memory/connection without `approval:` — never triggers this. `agent.model`
    # always has a default value, so it is NOT a signal of intent.
    _heal_mode = manifest.agent.approval_mode if manifest.agent else "disabled"
    import aqueduct.cli as _aqcli

    # Cascade connectivity counts: a cascade tier carries its own base_url/api_key
    # (falling back to the flat agent.* defaults). If ANY tier is reachable, healing
    # works even when the flat agent.base_url/api_key are unset (ISSUE-045).
    _agent_reachable = _aqcli._agent_usable(
        resolved_agent_provider, resolved_agent_base_url, resolved_agent_api_key
    ) or _aqcli._agent_usable_with_cascade(
        resolved_agent_provider,
        resolved_agent_base_url,
        resolved_agent_api_key,
        resolved_agent_cascade,
    )
    if _heal_mode != "disabled" and not _agent_reachable:
        from aqueduct.cli.render.style import warn as _style_warn

        _style_warn(
            f"self-healing is enabled (agent.approval={_heal_mode}) but the agent is not "
            f"reachable (provider={resolved_agent_provider}, no API key / base_url, and no "
            "usable cascade tier) — failures will NOT be auto-healed. Set the API key env "
            "var, agent.base_url, or a cascade tier base_url.",
            err=True,
        )

    # ── Register agent API key for redaction ─────────────────────────────────────
    if resolved_agent_api_key:
        from aqueduct.redaction import register as _register_secret

        _register_secret(resolved_agent_api_key, key_hint="agent.api_key")

    # ── Multi-patch disclaimer ────────────────────────────────────────────────────
    approval_mode = manifest.agent.approval_mode
    max_patches = manifest.agent.max_patches
    if approval_mode == "auto" and max_patches > 1:
        click.echo(
            f"\u26a0  multi-patch mode \u2014 Agent will attempt up to {max_patches} patch(es). "
            "Each patch is validated in-memory before being written to Blueprint. "
            "Review patches/applied/ after the run.",
            err=True,
        )

    # ── Surveyor \u2014 start ───────────────────────────────────────────────────────
    from aqueduct.depot.depot import DepotStore as _DS
    from aqueduct.surveyor.surveyor import Surveyor as _Surveyor

    if _using_default_obs_path and cfg.stores.observability.backend == "duckdb":
        from aqueduct.stores import StoreBundle
        from aqueduct.stores.duckdb_ import DuckDBObservabilityStore

        bundle = StoreBundle(
            observability=DuckDBObservabilityStore(resolved_store_dir / "observability.db"),
            depot=bundle.depot,
        )
        depot = _DS(backend=bundle.depot)
    surveyor = _Surveyor(
        manifest,
        store_dir=resolved_store_dir,
        engine=engine,
        webhook_config=resolved_webhook,
        blueprint_path=_P(blueprint_str),
        patches_dir=patches_dir,
        stores=bundle,
        blob_config=(cfg.stores.blob.backend, cfg.stores.blob.path),
    )
    surveyor.start(run_id)
    _obs_store = surveyor.observability
    _patch_store = surveyor.patch_store()

    # ── Engine session ────────────────────────────────────────────────────────────
    # Built THROUGH THE PROTOCOL REGISTRY, not a per-engine branch. The old
    # `if engine == "spark": make_spark_session() else: raise NotImplementedError`
    # meant the CLI could never reach any engine but Spark regardless of which
    # handlers existed; `get_protocol(engine).make_session(SessionSpec(...))`
    # dispatches by contract. An engine registered without a session factory
    # raises a clean EnginePluginError (naming the engine) via session_factory(),
    # the AqueductError replacement for the bare NotImplementedError.
    #
    # A polyglot Manifest (>1 island) does NOT build this eager session at
    # all — `run_polyglot()` opens one session PER ISLAND, lazily, in
    # topological order, and closes each immediately after its island
    # finishes (see `aqueduct/executor/orchestrator.py`). Building one more
    # session here for `deployment.engine` (the run's nominal default, not
    # necessarily any island's actual engine) would be wasted work at best
    # and a stray, never-closed-until-atexit session at worst.
    # `_session_holder.session` stays None for the rest of this run — every
    # downstream consumer (hooks' in-process fallback, the agentic ToolBox's
    # `spark_session`) already treats a missing session as "no live session
    # to reuse", the same fallback a single-engine run never exercises.
    _session_holder = _SessionHolder(None)
    if len(manifest.islands) <= 1:
        from aqueduct.executor.protocol import SessionSpec, get_protocol
        from aqueduct.executor.session_config import (
            resolve_session_engine_config,
            session_config_fingerprint,
            session_secrets_options,
        )

        _protocol = get_protocol(engine)
        _session_holder.engine_config_fingerprint = session_config_fingerprint(
            cfg, engine, manifest
        )
        _session_holder.session = _protocol.session_factory()(
            SessionSpec(
                blueprint_id=manifest.blueprint_id,
                engine_config=resolve_session_engine_config(cfg, engine, manifest),
                master_url=master_url,
                quiet_startup=(verbosity < 2),
                timezone=cfg.timezone,
                engine_options=session_secrets_options(cfg, manifest),
            )
        )

        import atexit

        _close_session = _protocol.session_closer()
        # Closes over `_session_holder`, not `session` — a later
        # `_execute_target` rebuild (see its docstring below) mutates the
        # holder's `.session`/`.engine_config_fingerprint` attributes from a
        # DIFFERENT function's scope (this function has already returned by
        # then), so reading `_session_holder.session` here at exit time is
        # the only way this closer ever sees a rebuilt session instead of
        # the original one.
        atexit.register(lambda: _close_session(_session_holder.session))

    return _SurveyorSetupResult(
        resolved_store_dir=resolved_store_dir,
        patches_dir=patches_dir,
        run_id=run_id,
        approval_mode=approval_mode,
        max_patches=max_patches,
        _is_multi_patch=_is_multi_patch,
        resolved_agent_provider=resolved_agent_provider,
        resolved_agent_base_url=resolved_agent_base_url,
        resolved_agent_model=resolved_agent_model,
        resolved_agent_provider_options=resolved_agent_provider_options,
        resolved_agent_timeout=resolved_agent_timeout,
        resolved_agent_max_reprompts=resolved_agent_max_reprompts,
        resolved_agent_api_key=resolved_agent_api_key,
        resolved_agent_engine_prompt_context=resolved_agent_engine_prompt_context,
        resolved_agent_blueprint_prompt_context=resolved_agent_blueprint_prompt_context,
        resolved_agent_cascade=resolved_agent_cascade,
        resolved_agent_mode=resolved_agent_mode,
        resolved_agent_max_tool_calls=resolved_agent_max_tool_calls,
        resolved_agent_supports_tools=resolved_agent_supports_tools,
        resolved_sandbox_master_url=resolved_sandbox_master_url,
        surveyor=surveyor,
        _obs_store=_obs_store,
        _patch_store=_patch_store,
        session_holder=_session_holder,
        bundle=bundle,
        depot=depot,
        _r=_r,
    )
