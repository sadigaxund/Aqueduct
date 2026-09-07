"""`run` command phases — extracted verbatim from aqueduct/cli/run.py
(Phase 91 `run()` decomposition).

`run()`'s ~2500-line body is a sequence of phases (see
`run-decomposition-map.md` for the full phase table); this module collects
the ones that are pure moves — module-level functions with an explicit
keyword-only parameter list, no closure over `run()`'s other locals. Each
function is copied verbatim from its original line range in `run.py` (only
indentation and the `_handoff_root_abs` → `handoff_root_abs` local→param
rename are adjusted); no behaviour change.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING as _t

import click

from aqueduct import exit_codes

if _t:
    from aqueduct.compiler.models import Manifest
    from aqueduct.config import AqueductConfig


@dataclass
class RunContext:
    """Mutable state shared by the phases that run AFTER Surveyor/session
    setup (the decomposition map's phase J) — the heal loop itself (which
    stays inline in `run()`, unchanged) and the helper functions it calls
    (`execute_target`, `render_module_summary`, `fire_heal_hook`,
    `announce_polyglot_sandbox_unavailable`).

    Built once in `run()` immediately after `_setup_surveyor`'s result is
    unpacked. `run()` keeps its own plain locals too — this is only the
    argument object the extracted functions read and mutate, not a
    replacement for every local in `run()`'s body.
    """

    manifest: Manifest
    cfg: AqueductConfig
    blueprint: str
    engine: str
    master_url: str
    verbosity: int
    execute: object  # Callable — heavy import (engine-specific), left untyped
    resolved_store_dir: object
    checkpoint_root_abs: Path | None
    handoff_root_abs: str
    surveyor: object
    depot: object
    bundle: object
    session_holder: object  # `_SessionHolder` (run_setup.py)
    # RAW `--store-dir` CLI param (possibly None) — deliberately distinct
    # from `resolved_store_dir` above. `render_module_summary` reads THIS
    # field, not `resolved_store_dir`; see its docstring.
    store_dir_param: str | None
    handoff_info: dict = field(default_factory=dict)
    polyglot_sandbox_unavailable_warned: bool = False


def check_resume_hash_guard(
    *,
    resume_run_id: str | None,
    force_resume: bool,
    manifest: Manifest,
    checkpoint_root_abs: Path | None,
    resolved_store_dir,
    handoff_root_abs: str,
) -> None:
    """`--resume` fails closed on a manifest-hash mismatch.

    `--resume <run_id>` reuses checkpoints from a PRIOR run. Two
    independent checkpoint mechanisms exist, both keyed off
    `aqueduct.executor.models.manifest_hash(manifest)` (a content hash
    of the WHOLE compiled Manifest — any Blueprint edit changes it):

      1. Module checkpoints (`checkpoint_root`/`store_dir/checkpoints`)
         — `<base>/<run_id>/_manifest_hash` stores the hash the ORIGINAL
         run compiled. Both engines' `execute()` already read this back
         and compare it (`spark/executor.py`, `duckdb_/executor.py`) —
         but only ever WARN (`runtime_resume_hash_changed`) and proceed;
         that permissive behaviour is a deliberate, separately-tested
         contract at the engine layer (see
         `test_resume_mismatched_manifest_warns_and_continues`) and is
         left untouched. The hard refusal below happens one layer up, at
         the CLI, BEFORE any engine session is even built.

      2. Handoff spill (`aqueduct/executor/spill.py`, polyglot Blueprints
         only) — laid out as `<handoff.root>/<manifest_hash>/<run_id>/`,
         keyed STRICTLY by the CURRENT hash. A mismatch here is not an
         observable "wrong hash" condition inside the orchestrator at
         all — it just finds nothing under the new hash and silently
         starts that island fresh. `find_run_under_other_hash` is the
         detector: it tells "genuinely first run of this run_id" apart
         from "run_id exists, but under a stale hash" by scanning every
         OTHER hash directory.

    Either mechanism finding a stale hash is refused identically here,
    unless `--force` (validated above) opts back into today's
    behaviour (module checkpoints keep warning-and-proceeding; handoff
    spill keeps silently re-executing that island).
    """
    if resume_run_id and not force_resume:
        from aqueduct.executor.models import manifest_hash as _manifest_hash_fn
        from aqueduct.executor.spill import find_run_under_other_hash as _find_other_hash

        _current_hash = _manifest_hash_fn(manifest)
        _stale_hash: str | None = None

        _checkpoints_base = (
            checkpoint_root_abs
            if checkpoint_root_abs
            else (resolved_store_dir / "checkpoints" if resolved_store_dir else None)
        )
        if _checkpoints_base is not None:
            _stored_hash_path = Path(_checkpoints_base) / resume_run_id / "_manifest_hash"
            if _stored_hash_path.exists():
                _stored_hash = _stored_hash_path.read_text(encoding="utf-8").strip()
                if _stored_hash != _current_hash:
                    _stale_hash = _stored_hash

        if _stale_hash is None and len(manifest.islands) > 1:
            _stale_hash = _find_other_hash(handoff_root_abs, resume_run_id, _current_hash)

        if _stale_hash is not None:
            click.echo(
                f"✗ --resume {resume_run_id!r} refused: checkpoint manifest hash "
                f"{_stale_hash!r} does not match this run's compiled Manifest hash "
                f"{_current_hash!r} — the Blueprint (or its context/profile) has "
                "changed since that run's checkpoints were written. Pass --force to "
                "reuse them anyway, or drop --resume to start fresh.",
                err=True,
            )
            sys.exit(exit_codes.CONFIG_ERROR)


def run_sandbox_dryrun(
    *,
    manifest: Manifest,
    engine: str,
    run_id: str | None,
    sample: int,
    master_url: str,
    verbosity: int,
    cfg,
    depot,
    from_module: str | None,
    to_module: str | None,
    parallel: bool,
    probe_sampling,
    execute,
) -> None:
    """Sandbox dry-run (short-circuit).

    Dev loop: run the compiled pipeline against sampled inputs with every
    Egress skipped — no writes, no Surveyor, no self-healing, no
    observability persistence. Reuses the patch-validation sandbox
    transform so behaviour matches Gate 3.

    Engine-agnostic (Phase 89 add-on): built THROUGH THE PROTOCOL
    REGISTRY — `get_protocol(engine).session_factory()` +
    `resolve_session_engine_config` — the same seam
    `aqueduct.patch.preview.run_sandbox_gate` (Gate 3's own sandbox
    replay) and the main run path's `_execute_target` already use,
    rather than a hardcoded `make_spark_session()` that made
    `--sandbox` reachable only for `engine=spark` regardless of which
    engines were actually registered.
    """
    import atexit
    import uuid

    from aqueduct.executor import ExecuteError
    from aqueduct.executor.capabilities import Support, get_capabilities
    from aqueduct.executor.models import ExecutionStatus
    from aqueduct.patch.preview import build_sandbox_manifest

    if len(manifest.islands) > 1:
        _island_engines = ", ".join(sorted({isl.engine for isl in manifest.islands}))
        click.echo(
            f"✗ --sandbox does not support a polyglot Blueprint "
            f"({len(manifest.islands)} islands: {_island_engines}) — a single-session "
            "dry-run cannot replay a multi-engine Manifest in this release",
            err=True,
        )
        sys.exit(exit_codes.CONFIG_ERROR)

    _sandbox_leaf = get_capabilities(engine).verdict("tooling.sandbox_dry_run")
    if _sandbox_leaf.support != Support.SUPPORTED:
        click.echo(
            f"✗ --sandbox does not support engine {engine!r}: "
            f"{_sandbox_leaf.hint or 'tooling.sandbox_dry_run is unsupported for this engine'}",
            err=True,
        )
        sys.exit(exit_codes.CONFIG_ERROR)

    from aqueduct.executor.protocol import (
        SessionSpec,
        filter_execute_kwargs,
        get_protocol,
    )
    from aqueduct.executor.session_config import (
        resolve_session_engine_config,
        session_secrets_options,
    )

    sandboxed_manifest, egress_targets = build_sandbox_manifest(manifest, sample)
    sandbox_run_id = f"sandbox-{run_id or uuid.uuid4().hex}"  # full uuid — queryable, no collisions

    _limit_desc = f"≤{sample} row(s)/Ingress" if sample and sample > 0 else "no row limit"
    click.echo(
        f"⊙ sandbox dry-run — {_limit_desc}, {len(egress_targets)} Egress "
        "module(s) skipped (no writes, no healing, no persistence)",
        err=True,
    )

    _protocol = get_protocol(engine)
    session = _protocol.session_factory()(
        SessionSpec(
            blueprint_id=manifest.blueprint_id,
            engine_config=resolve_session_engine_config(cfg, engine, manifest),
            master_url=master_url,
            quiet_startup=(verbosity < 2),
            timezone=cfg.timezone,
            engine_options=session_secrets_options(cfg, manifest),
        )
    )
    atexit.register(lambda: _protocol.session_closer()(session))

    try:
        _sandbox_kwargs = filter_execute_kwargs(
            engine,
            dict(
                run_id=sandbox_run_id,
                store_dir=None,
                surveyor=None,
                depot=depot,
                from_module=from_module,
                to_module=to_module,
                block_full_actions=not cfg.danger.allow_full_probe_actions,
                parallel=parallel,
                sampling=probe_sampling,
            ),
            suppress=cfg.warnings.suppress,
        )
        result = execute(sandboxed_manifest, session, **_sandbox_kwargs)
    except ExecuteError as exc:
        click.echo(f"✗ sandbox run failed: {exc}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    if result.status != ExecutionStatus.SUCCESS:
        failing = next(
            (r for r in result.module_results if r.status == ExecutionStatus.ERROR), None
        )
        detail = f" — first error in {failing.module_id!r}: {failing.error}" if failing else ""
        from aqueduct.cli.render.style import error as _style_error

        _style_error(f"sandbox run status={result.status}{detail}", err=False)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    _ran = sum(1 for r in result.module_results if r.status == ExecutionStatus.SUCCESS)
    from aqueduct.cli.render.style import success as _style_success

    _style_success(
        f"sandbox run succeeded — {_ran} module(s) executed, "
        f"{len(egress_targets)} Egress skipped",
        err=False,
    )
    for tgt in egress_targets:
        click.echo(
            f"    · skipped Egress {tgt['id']!r} → " f"{tgt.get('format')} {tgt.get('path')}",
            err=True,
        )
    sys.exit(exit_codes.SUCCESS)


def acquire_run_lock(
    *,
    resolved_store_dir,
    obs_routing_base: str,
    manifest: Manifest,
    bundle,
    wait_for_lock: bool,
    run_stack,
) -> str:
    """Per-blueprint run lock.

    Two concurrent runs of one Blueprint share an observability store
    and a depot. DuckDB serialises their statements so neither crashes,
    but they still interleave logically (two run_records rows growing
    at once, two heal loops writing the same depot keys). Take the lock
    BEFORE the Surveyor exists, since Surveyor setup is itself the
    first writer, and hold it for the rest of the run: `_run_stack`
    closes in the outer `finally` below, so an exception or a
    `sys.exit` releases it too.
    """
    from aqueduct.cli.run_setup import resolve_blueprint_store_dir as _resolve_bp_dir
    from aqueduct.stores.run_lock import RunLockedError as _RunLockedError
    from aqueduct.stores.run_lock import blueprint_run_lock as _blueprint_run_lock

    _lock_dir = _resolve_bp_dir(resolved_store_dir, obs_routing_base, manifest.blueprint_id)
    resolved_store_dir = _lock_dir
    try:
        run_stack.enter_context(
            _blueprint_run_lock(
                _lock_dir,
                manifest.blueprint_id,
                obs_store=bundle.observability if bundle is not None else None,
                wait=wait_for_lock,
            )
        )
    except _RunLockedError as exc:
        from aqueduct.cli.render.style import error as _style_error

        _style_error(str(exc))
        sys.exit(exit_codes.CONFIG_ERROR)

    return resolved_store_dir


def check_from_to_island_guard(
    *,
    manifest: Manifest,
    from_module: str | None,
    to_module: str | None,
) -> None:
    """`--from` / `--to` are not yet island-aware.

    Module-range selection assumes ONE execution graph; which island(s)
    a `--from`/`--to` pair spans, and how a sub-manifest gets built per
    island for a partial range, is real cross-island work this batch
    does not attempt. Refusing loudly (CONFIG_ERROR) beats silently
    running the whole polyglot graph while looking like it honoured the
    flag — the same "loud, not silent" choice `--sandbox` already makes
    for a polyglot Manifest below.
    """
    if len(manifest.islands) > 1 and (from_module or to_module):
        click.echo(
            "✗ --from/--to do not yet support a polyglot Blueprint "
            f"({len(manifest.islands)} islands) — module-range selection "
            "across engine islands is not implemented in this release",
            err=True,
        )
        sys.exit(exit_codes.CONFIG_ERROR)


def execute_target(
    ctx: RunContext,
    target_manifest,
    *,
    run_id: str,
    resume_run_id: str | None = None,
    **kw,
):
    """Execute *target_manifest* — the single-engine ``execute()``
    call for a Manifest with exactly one island (byte-for-byte the
    same call this code made before polyglot routing existed: same
    kwargs dict, same ``filter_execute_kwargs`` call, same
    ``ExecuteError`` handling), or ``run_polyglot()`` for one with
    more than one.

    ``kw`` carries whatever the specific call site already builds
    for ``execute()`` (``store_dir``, ``checkpoint_root``,
    ``surveyor``, ``depot``, ``from_module``, ``to_module``,
    ``block_full_actions``, ``parallel``, ``use_observe``,
    ``observability_store``, ``sampling``) — the three call sites in
    this function pass different subsets (the main heal loop passes
    the full set; the retry-execute calls after a patch pass a
    narrower one), preserved exactly as each already did.

    Returns ``(result, execute_exc)``. ``execute_exc`` is the raw
    ``ExecuteError`` on the single-engine path only (kept so callers
    can still feed it to ``surveyor.record(exc=...)`` for
    stack_trace enrichment, exactly as today) — a polyglot
    structural failure is already converted to a synthetic
    ``ModuleResult`` inside ``run_polyglot()`` itself (see its
    ``AqueductError`` wrap), so ``execute_exc`` is always ``None``
    on that path.

    **Session-fingerprint guard (cross-engine remediation).** Before
    the single-engine branch executes, it compares the session
    fingerprint *target_manifest* would resolve
    (``session_config_fingerprint``, in
    ``aqueduct/executor/session_config.py``) against the one
    ``_session_holder.session`` was actually built from, rebuilding
    only on mismatch. This is the ONE funnel every single-engine
    execution in this run passes through — the outer heal loop's
    baseline re-execution at the top of ``while True:`` AND every
    patch retry — so it catches both directions of the invariant
    "never execute a Manifest on a session built from a DIFFERENT
    Manifest": a patch retry whose ``set_engine_config`` op the
    pre-patch session hasn't picked up, AND (the bug this check adds
    over the earlier Phase 82 fix) the next baseline re-execution of
    the ORIGINAL manifest running on whatever session a FAILED
    patch's retry left behind. A mismatch-free call (nothing
    session-relevant changed) costs one fingerprint recompute and no
    rebuild — a Spark JVM is never torn down for a patch that never
    touched engine config. This subsumes the removed
    ``_rebuild_session_for_patch`` — a Manifest change is now always
    observed exactly once, at the point of execution, instead of at
    two separate explicit call sites that could disagree.
    """
    from aqueduct.executor import ExecuteError
    from aqueduct.executor.models import ExecutionResult, ExecutionStatus, ModuleResult

    if len(target_manifest.islands) <= 1:
        from aqueduct.executor.protocol import (
            SessionSpec,
            filter_execute_kwargs,
            get_protocol,
        )
        from aqueduct.executor.session_config import (
            resolve_session_engine_config,
            session_config_fingerprint,
            session_secrets_options,
        )

        _target_fingerprint = session_config_fingerprint(ctx.cfg, ctx.engine, target_manifest)
        if (
            ctx.session_holder.session is not None
            and ctx.session_holder.engine_config_fingerprint != _target_fingerprint
        ):
            _protocol = get_protocol(ctx.engine)
            # Stop the STALE session before building the new one — a
            # `getOrCreate()`-style reuse without a genuine teardown
            # first would silently hand back the same live session
            # (the exact no-op-that-looks-like-a-fix this rebuild
            # exists to avoid). See `make_spark_session` — most
            # engine config (definitely anything `set_engine_config`
            # changes) has no effect on an already-running session.
            _protocol.session_closer()(ctx.session_holder.session)
            ctx.session_holder.session = _protocol.session_factory()(
                SessionSpec(
                    blueprint_id=target_manifest.blueprint_id,
                    engine_config=resolve_session_engine_config(
                        ctx.cfg, ctx.engine, target_manifest
                    ),
                    master_url=ctx.master_url,
                    quiet_startup=(ctx.verbosity < 2),
                    timezone=ctx.cfg.timezone,
                    engine_options=session_secrets_options(ctx.cfg, target_manifest),
                )
            )
            ctx.session_holder.engine_config_fingerprint = _target_fingerprint

        try:
            _filtered = filter_execute_kwargs(
                ctx.engine,
                dict(kw, run_id=run_id, resume_run_id=resume_run_id),
                suppress=ctx.cfg.warnings.suppress,
            )
            return ctx.execute(target_manifest, ctx.session_holder.session, **_filtered), None
        except ExecuteError as exc:
            return (
                ExecutionResult(
                    blueprint_id=target_manifest.blueprint_id,
                    run_id=run_id,
                    status=ExecutionStatus.ERROR,
                    module_results=(
                        ModuleResult(
                            module_id="_executor",
                            status=ExecutionStatus.ERROR,
                            error=str(exc),
                        ),
                    ),
                ),
                exc,
            )

    # ── Polyglot ──────────────────────────────────────────────────
    from aqueduct.executor.orchestrator import run_polyglot
    from aqueduct.executor.session_config import (
        resolve_session_engine_config,
        session_secrets_options,
    )

    _engine_configs: dict[str, dict] = {
        _isl.engine: resolve_session_engine_config(ctx.cfg, _isl.engine, target_manifest)
        for _isl in target_manifest.islands
    }

    polyglot_result = run_polyglot(
        target_manifest,
        run_id=run_id,
        handoff_root=ctx.handoff_root_abs,
        keep_on_failure=ctx.cfg.handoff.keep_on_failure,
        resume_run_id=resume_run_id,
        store_dir=kw.get("store_dir", ctx.resolved_store_dir),
        checkpoint_root=kw.get("checkpoint_root", ctx.checkpoint_root_abs),
        surveyor=kw.get("surveyor", ctx.surveyor),
        depot=kw.get("depot", ctx.depot),
        observability_store=kw.get("observability_store", ctx.bundle.observability),
        warnings_suppress=ctx.cfg.warnings.suppress,
        engine_configs=_engine_configs,
        master_url=ctx.master_url,
        quiet_startup=(ctx.verbosity < 2),
        timezone=ctx.cfg.timezone,
        secrets_config=session_secrets_options(ctx.cfg, target_manifest)["secrets"],
        block_full_actions=kw.get("block_full_actions", False),
        parallel=kw.get("parallel", False),
        use_observe=kw.get("use_observe", False),
        sampling=kw.get("sampling"),
        record_result=False,
        session_keep_alive=ctx.cfg.execution.session_keep_alive,
        share_island_state=ctx.cfg.execution.share_island_state,
        prune_eagerly=ctx.cfg.handoff.prune_eagerly,
    )
    # Phase 89 item 1 — one quiet `-v` narrative line per boundary
    # where a session was kept alive instead of rebuilt, same
    # funnel/style convention as the `⇄ handoff` boundary rendering
    # above. `session_reused` is empty whenever keep-alive found no
    # same-engine adjacency (or `execution.session_keep_alive` is
    # off), so this is silent in the common case.
    if ctx.verbosity >= 1 and polyglot_result.session_reused:
        from aqueduct.cli.render.funnel import info as _funnel_info

        for _reused_engine in polyglot_result.session_reused:
            _funnel_info(
                f"session kept alive · {_reused_engine}",
                gutter="  ",
                err=True,
            )
    # Phase 89 item 3 — same, but for eager spill pruning, one quiet
    # `-vv` narrative line per boundary whose spill was deleted the
    # moment its reader island succeeded rather than at run end.
    # Gated at -vv (not -v, unlike the reuse line above): a pruned
    # edge is routine per-boundary housekeeping, one level quieter
    # than "a session build was skipped" is.
    if ctx.verbosity >= 2 and polyglot_result.pruned_spills:
        from aqueduct.cli.render.funnel import info as _funnel_info

        for _pruned_edge in polyglot_result.pruned_spills:
            _funnel_info(
                f"spill pruned · {_pruned_edge}",
                gutter="  ",
                err=True,
            )
    return polyglot_result, None


def render_module_summary(
    ctx: RunContext,
    _result,
    failure_ctx=None,
    *,
    healed_module=None,
    healed_patch_num=None,
) -> None:
    """Print the per-module ✓/✗ status block for one execution result.

    ``failure_ctx`` (optional) is the ``FailureContext`` the surveyor
    just recorded for THIS result — carries ``error_class`` and
    ``suggested_columns`` for the classified ✗ failure line (SCREEN
    2/6). Only its ``failed_module`` row uses it; other rows fall
    back to the generic label.

    Called once per heal iteration right after the result is recorded, so
    module outcomes print BEFORE that iteration's agent/heal output —
    chronological order (execute → result → heal → next attempt). Metrics
    are a best-effort post-execute read from the obs store (short-lived
    connections, so the store is free by now)."""
    from aqueduct.cli.run import _classify_error_label
    from aqueduct.executor.models import ExecutionStatus, concise_error
    from aqueduct.models import ModuleType

    # Local aliases so the body below (including its nested sub-closures,
    # copied verbatim from `run.py`) reads exactly as it did as a nested
    # closure inside `run()` — only the names read via `ctx` change.
    cfg = ctx.cfg
    manifest = ctx.manifest
    verbosity = ctx.verbosity
    _handoff_info = ctx.handoff_info
    # `store_dir` here is DELIBERATELY the RAW `--store-dir` CLI param
    # (`ctx.store_dir_param`, possibly `None` if `--store-dir` wasn't
    # passed), NOT `ctx.resolved_store_dir` — preserved exactly as this
    # closure read it in `run.py` before this extraction (flagged by the
    # Phase 91 decomposition map as a pre-existing divergence; reported,
    # not silently "fixed" here).
    store_dir = ctx.store_dir_param

    _metrics: dict[str, dict] = {}
    try:
        from aqueduct.stores.queries import run_detail as _run_detail
        from aqueduct.stores.read import open_obs_read

        _rs = open_obs_read(
            cfg,
            store_dir=store_dir,
            run_id=_result.run_id,
            blueprint_id=manifest.blueprint_id,
        )
        if _rs is not None:
            _det = _run_detail(_rs, _result.run_id)
            if _det:
                # `run_detail` already merges a module_id's multiple
                # `module_metrics` rows into one `ProfileRow` (a
                # synthetic Handoff module gets a write-side row and
                # a read-side row under the SAME module_id — see
                # `stores/queries.py::run_detail`) — one entry per
                # module_id here, never overwritten.
                for _p in _det.profile:
                    _metrics[_p.module_id] = {
                        "records_written": _p.records_written,
                        "duration_ms": _p.duration_ms,
                        "bytes_written": getattr(_p, "bytes_written", None),
                        "bytes_read": getattr(_p, "bytes_read", None),
                    }
    except Exception:
        pass  # per-module profile read is best-effort; never fail for a missing metric

    def _fmt_dur(ms):
        return None if ms is None else (f"{ms} ms" if ms < 1000 else f"{ms / 1000:.1f} s")

    # ⏭ reason column for `enabled: false` modules (compiler-stamped).
    _disabled_reason = {
        m.id: m.disabled_reason for m in manifest.modules if getattr(m, "disabled_reason", None)
    }

    # Phase 85 Wave 2 — egress rows show their destination instead of
    # rows/time metadata (SCREEN 1). `m.config` is dict-like (same
    # access pattern `_handoff_info` above already relies on).
    _egress_dest: dict[str, str] = {}
    for _m in manifest.modules:
        if _m.type == ModuleType.Egress:
            _dest = _m.config.get("path") or _m.config.get("table") or _m.config.get("key")
            if _dest:
                _egress_dest[_m.id] = str(_dest)

    click.echo(err=False)

    def _icon(mr):
        if mr.status == ExecutionStatus.SUCCESS:
            return click.style("✓", fg="green")
        if mr.status == ExecutionStatus.SKIPPED:
            return click.style("⏭", fg="cyan")
        return click.style("✗", fg="red", bold=True)

    # Tree view — Arcade-expanded children (`{arcade}__{child}`, the
    # expander's namespacing convention; `__` is rejected in user ids)
    # nest under a synthetic parent row. A synthetic Handoff module's
    # id ALSO contains "__" (`<from_id>__handoff__<to_id>`) but is
    # checked first and routed to its own row kind instead — folding
    # it into the Arcade-child branch would misparse it as a child
    # of whichever module happens to share its `from_id` prefix.
    # Only THIS summary block nests/specializes: runtime logs,
    # observability rows, and the failed_module footer keep the
    # full flattened id so error correlation stays joinable.
    _rows: list[tuple[str, object]] = []
    _arc_children: dict[str, list] = {}
    for mr in _result.module_results:
        if mr.module_id in _handoff_info:
            _rows.append(("handoff", mr))
        elif "__" in mr.module_id:
            _arc = mr.module_id.split("__", 1)[0]
            if _arc not in _arc_children:
                _arc_children[_arc] = []
                _rows.append(("arcade", _arc))
            _arc_children[_arc].append(mr)
        else:
            _rows.append(("module", mr))

    _CHILD_PAD = 5  # child names start 5 columns deeper than top-level names
    _w = max(
        [len(mr.module_id) for kind, mr in _rows if kind == "module"]
        + [len(mr.module_id) for kind, mr in _rows if kind == "handoff"]
        + [len(a) for kind, a in _rows if kind == "arcade"]
        + [
            len(c.module_id.split("__", 1)[1]) + _CHILD_PAD
            for cs in _arc_children.values()
            for c in cs
        ],
        default=0,
    )

    # Phase 85 Wave 2 — right-aligned metadata column (SCREEN 1).
    # Two passes: first compute each row's (left, tail) unpadded,
    # find the widest `left + 2sp + tail` among rows that HAVE a
    # tail, then pad every such row's left side out to that width
    # so every tail starts (or, since tails vary in length, ENDS)
    # at the same column. Degrades to the old left-packed
    # `name.ljust(pad)` layout when the terminal is too narrow for
    # that column to fit without negative padding.
    from aqueduct.cli.render.width import display_width as _dw
    from aqueduct.cli.render.width import terminal_width as _term_width

    _pending_rows: list[dict] = []  # collected before any printing

    def _queue_row(
        mr, left_plain, left_styled, tail_plain, tail_styled, warn_prefix, kind="metric"
    ):
        # `kind` separates the right-aligned METRIC column (rows ·
        # time, bytes · duration — short, comparable) from a "path"
        # tail (an egress destination — long, variable, free text).
        # Audit-fixed 2026-08-23: an egress row used to join the
        # SAME right-alignment group as metric rows, so one long
        # absolute path inflated the natural width and dragged
        # every metric row's gap out to match it (`raw_orders` with
        # 60+ spaces before a bare "14 ms").
        _pending_rows.append(
            {
                "mr": mr,
                "left_plain": left_plain,
                "left_styled": left_styled,
                "tail_plain": tail_plain,
                "tail_styled": tail_styled,
                "warn_prefix": warn_prefix,
                "kind": kind,
            }
        )

    def _flush_rows():
        _tw = _term_width()
        _metric_rows = [r for r in _pending_rows if r["tail_plain"] and r["kind"] == "metric"]
        _natural = (
            max(_dw(r["left_plain"]) + 2 + _dw(r["tail_plain"]) for r in _metric_rows)
            if _metric_rows
            else 0
        )
        _fits = bool(_metric_rows) and _natural <= _tw
        # Path rows align among THEMSELVES (and with the metric
        # rows' name column, for a tidy shared left edge) — never
        # against the metric column's own width.
        _name_width = max((_dw(r["left_plain"]) for r in _pending_rows), default=0)
        for r in _pending_rows:
            mr, warn_prefix = r["mr"], r["warn_prefix"]
            if not r["tail_plain"]:
                line = r["left_styled"]
            elif r["kind"] == "path":
                pad = max(2, _name_width - _dw(r["left_plain"]) + 2)
                line = r["left_styled"] + (" " * pad) + r["tail_styled"]
            elif _fits:
                pad = max(2, _natural - _dw(r["left_plain"]) - _dw(r["tail_plain"]))
                line = r["left_styled"] + (" " * pad) + r["tail_styled"]
            else:
                # Narrow terminal — fall back to a simple 2-space gap
                # rather than compute a negative/degenerate pad.
                line = f"{r['left_styled']}  {r['tail_styled']}"
            click.echo(line, err=False)
            for rule_id, msg in mr.warnings:
                from aqueduct.cli.render.funnel import warn as _output_warn

                _output_warn(rule_id, msg, prefix=warn_prefix, err=False)
            _notes = tuple(getattr(mr, "notes", ()) or ())
            _cap = len(_notes) if verbosity >= 1 else 10
            from aqueduct.cli.render.style import dim as _dim2

            for note in _notes[:_cap]:
                click.echo(_dim2(f"{warn_prefix}{note}"), err=False)
            if len(_notes) > _cap:
                click.echo(
                    _dim2(f"{warn_prefix}· {len(_notes) - _cap} more  ·  -v for full output"),
                    err=False,
                )
        _pending_rows.clear()

    def _print_failure_block(mr, name_or_boundary, lead):
        """Classified label + wrapped detail + candidates + hint
        (SCREEN 2/6). TTY: multi-line, structured. Piped/CI: ONE
        merged logical record (grep-safe) — built explicitly here
        rather than via `wrap_line`'s newline-splitting, since the
        piped shape (label — detail — candidates all on one line)
        differs from the TTY shape (candidates always its own
        line) and `wrap_line` alone can't express that difference."""
        from aqueduct.cli.render.width import is_tty as _is_tty
        from aqueduct.cli.render.wrap import wrap_line as _wrap_line

        _is_failed_module = failure_ctx is not None and mr.module_id == failure_ctx.failed_module
        _ec = failure_ctx.error_class if _is_failed_module else None
        _label = _classify_error_label(_ec)
        _candidates = (
            list(failure_ctx.suggested_columns)
            if _is_failed_module and getattr(failure_ctx, "suggested_columns", None)
            else []
        )
        _detail = concise_error(mr.error, limit=100_000) if mr.error else ""
        _cand_text = f"candidates: {', '.join(_candidates)}" if _candidates else ""

        if _is_tty(err=False):
            click.echo(
                f"{lead}{_icon(mr)} {name_or_boundary}  " + click.style(_label, fg="red"),
                err=False,
            )
            for line in _wrap_line(
                _detail,
                gutter="      ",
                err=False,
                verbose=verbosity >= 1,
                max_lines=None if verbosity >= 1 else 3,
                hint="full error text",
            ):
                click.echo(line, err=False)
            if _cand_text:
                for line in _wrap_line(_cand_text, gutter="      ", err=False, verbose=True):
                    click.echo(line, err=False)
        else:
            _parts = [_label]
            if _detail:
                _parts.append(_detail)
            if _cand_text:
                _parts.append(_cand_text)
            _combined = " — ".join(_parts)
            for line in _wrap_line(_combined, gutter="", err=False, verbose=True):
                click.echo(f"{lead}{_icon(mr)} {name_or_boundary}  {line}", err=False)

    def _mr_line(mr, name, pad, lead, warn_prefix):
        from aqueduct.cli.render.style import dim as _dim

        if mr.status == ExecutionStatus.ERROR and mr.error:
            _flush_rows()  # preserve chronological order vs queued rows
            _print_failure_block(mr, name, lead)
            return
        _m = _metrics.get(mr.module_id, {})
        rows, dur = _m.get("records_written"), _m.get("duration_ms")
        if mr.module_id in _egress_dest:
            tail_plain = f"→ {_egress_dest[mr.module_id]}"
            tail_styled = tail_plain
        else:
            meta = []
            if mr.status == ExecutionStatus.SKIPPED and mr.module_id in _disabled_reason:
                meta.append(_disabled_reason[mr.module_id])
            if rows is not None:
                meta.append(f"{rows:,} rows")
            if _fmt_dur(dur):
                meta.append(_fmt_dur(dur))
            if healed_module is not None and mr.module_id == healed_module:
                meta.append(
                    f"healed patch #{healed_patch_num}"
                    if healed_patch_num is not None
                    else "healed"
                )
            tail_plain = "  ·  ".join(meta)
            tail_styled = _dim(tail_plain) if tail_plain else ""
        left_styled = f"{lead}{_icon(mr)} {name}"
        _kind = "path" if mr.module_id in _egress_dest else "metric"
        _queue_row(
            mr,
            f"{lead}{name}",
            left_styled,
            tail_plain,
            tail_styled,
            warn_prefix,
            kind=_kind,
        )

    def _handoff_line(mr, pad, lead, warn_prefix):
        """First-class rendering for a synthetic Handoff module's
        result — a dedicated engine-boundary line (SCREEN 1 notes:
        "engine appears ONLY at a polyglot handover boundary"),
        distinct from an ordinary module row."""
        from aqueduct.cli.render.funnel import format_bytes as _format_bytes
        from aqueduct.cli.render.style import dim as _dim

        _cfg = _handoff_info[mr.module_id]
        _boundary = f"handoff · {_cfg.get('from_engine')} → {_cfg.get('to_engine')}"
        if mr.status == ExecutionStatus.ERROR and mr.error:
            _flush_rows()
            _print_failure_block(mr, f"⇄ {_boundary}", lead)
            return
        _m = _metrics.get(mr.module_id, {})
        meta = []
        _bw, _br = _m.get("bytes_written"), _m.get("bytes_read")
        _fmt = _cfg.get("format")
        if _fmt:
            meta.append(str(_fmt))
        if _bw is not None:
            meta.append(f"{_format_bytes(_bw)} written")
        if _br is not None:
            meta.append(f"{_format_bytes(_br)} read")
        if _fmt_dur(_m.get("duration_ms")):
            meta.append(_fmt_dur(_m.get("duration_ms")))
        tail_plain = "  ·  ".join(meta)
        tail_styled = _dim(tail_plain) if tail_plain else ""
        left_styled = f"{lead}{_icon(mr)} ⇄ {_boundary}"
        _queue_row(mr, f"{lead}⇄ {_boundary}", left_styled, tail_plain, tail_styled, warn_prefix)

    for kind, item in _rows:
        if kind == "module":
            _mr_line(item, item.module_id, _w, "  ", "   ↳ ")
            continue
        if kind == "handoff":
            _handoff_line(item, _w, "  ", "   ↳ ")
            continue
        _kids = _arc_children[item]
        # Parent row = worst child: any ✗ → ✗, else any ✓ → ✓, else ⏭.
        if any(m.status == ExecutionStatus.ERROR for m in _kids):
            _p_icon = click.style("✗", fg="red", bold=True)
        elif any(m.status == ExecutionStatus.SUCCESS for m in _kids):
            _p_icon = click.style("✓", fg="green")
        else:
            _p_icon = click.style("⏭", fg="cyan")
        _flush_rows()
        click.echo(f"  {_p_icon} {item}", err=False)
        for _i, _kid in enumerate(_kids):
            _glyph = "└─" if _i == len(_kids) - 1 else "├─"
            _lead = "    " + click.style(_glyph, fg="bright_black") + " "
            _mr_line(_kid, _kid.module_id.split("__", 1)[1], _w - _CHILD_PAD, _lead, "       ↳ ")
        _flush_rows()

    _flush_rows()  # trailing queued metric rows
