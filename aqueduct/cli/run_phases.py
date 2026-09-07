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
from pathlib import Path
from typing import TYPE_CHECKING as _t

import click

from aqueduct import exit_codes

if _t:
    from aqueduct.compiler.models import Manifest


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
