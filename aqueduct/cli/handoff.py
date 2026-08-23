"""`aqueduct handoff` command group — cross-engine handoff spill maintenance.

A ``handoff`` group (not a flat ``handoff-sweep`` command): unlike ``report``
(which could not become a click ``Group`` because it already carries an
optional positional argument, forcing the ``report-prune``/``report-costs``
flat-command precedent), there is no pre-existing ``handoff`` command for
``sweep`` to collide with, and ``patch``/``stores``/``mcp``/``blueprint``/
``dev`` already establish "a bare noun is a group" as the repo's default
shape. ``sweep`` is the group's first verb; a future ``handoff list`` or
``handoff show <run_id>`` has a natural home here.

Sweeping itself is engine-agnostic: ``aqueduct/executor/spill.py`` already
implements the orphan decision rule (``plan_orphan_sweep`` /
``sweep_orphan_spills``) and the actual delete (local ``shutil.rmtree`` or,
for a remote ``handoff.root``, ``fsspec`` — same "local, or remote-with-
fsspec, else refuse loudly" contract every other handoff-adjacent surface
in this codebase follows, see ``local_only_or_fsspec_available``). This
module is the CLI wiring on top of that: config resolution, run-liveness
lookups across every discovered observability store (a shared
``handoff.root`` can carry spill from more than one Blueprint), and
rendering.
"""

from __future__ import annotations

import sys
from contextlib import contextmanager
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
from aqueduct.cli.render.funnel import echo as _echo
from aqueduct.cli.render.funnel import emit, format_bytes
from aqueduct.cli.render.tables import Column, render_table


class _FederatedCursor:
    """Read-only ``RelationalCursor`` shim fanning ONE query out across
    several observability stores, first non-empty result wins.

    ``aqueduct/executor/spill.py``'s ``_run_status``/
    ``_superseded_by_later_success`` each open exactly one ``obs_store``,
    run exactly one ``execute`` + one ``fetchone``, per call — the shape a
    real run always sees, because a real run's Manifest carries exactly one
    ``blueprint_id`` and therefore one observability store. ``handoff
    sweep`` has no such anchor: ``handoff.root`` can be shared by several
    Blueprints (nothing in the spill directory layout —
    ``<root>/<manifest_hash>/<run_id>/<edge_id>/`` — scopes it to one), and
    with the DuckDB backend each Blueprint's ``run_records`` lives in its
    OWN file. Querying only one of them would make every OTHER Blueprint's
    run_ids resolve as "no row" — which ``spill.py``'s rule treats as
    "unknown, reclaim" — misclassifying a live run's spill as orphaned
    whenever it belongs to a Blueprint this invocation didn't happen to
    pick. This shim tries each discovered store in turn so the SAME
    liveness rule spill.py already enforces sees every store, not just one.
    """

    def __init__(self, stores: list[Any]) -> None:
        self._stores = stores
        self._row: tuple | None = None

    def execute(self, sql: str, params: Any = None) -> None:
        self._row = None
        for store in self._stores:
            try:
                with store.connect() as cur:
                    cur.execute(sql, params if params is not None else [])
                    row = cur.fetchone()
            except Exception:
                row = None
            if row is not None:
                self._row = row
                return

    def fetchone(self) -> tuple | None:
        return self._row


class _FederatedObsStore:
    """``obs_store``-shaped facade over every discovered observability
    store — see ``_FederatedCursor``. Read-only; never used for writes."""

    def __init__(self, stores: list[Any]) -> None:
        self._stores = stores

    @contextmanager
    def connect(self):
        yield _FederatedCursor(self._stores)


def _resolve_handoff_root(root: str, project_root: Path) -> str:
    """Anchor a relative ``handoff.root`` against *project_root* — same
    convention ``run_doctor``'s handoff checks and ``aqueduct run``'s own
    ``_handoff_root_abs`` use. A remote URI is returned untouched (no local
    CWD to anchor a URI against)."""
    from aqueduct.executor.spill import is_remote_uri

    if is_remote_uri(root):
        return root
    p = Path(root)
    if not p.is_absolute():
        p = project_root / root
    return str(p.resolve())


@cli.group("handoff")
def handoff() -> None:
    """Cross-engine handoff spill maintenance (Phase 81/82 transport)."""


@handoff.command("sweep")
@click.option(
    "--config",
    "config_path",
    default=None,
    type=click.Path(dir_okay=False),
    help="Path to aqueduct.yml",
)
@click.option(
    "--store-dir",
    default=None,
    help="Observability store directory to discover run_records in "
    "(default: aqueduct.yml or .aqueduct). A shared handoff.root can carry "
    "spill from more than one Blueprint, so this discovers every store "
    "found there, not just one.",
)
@click.option(
    "--dry-run/--execute",
    "dry_run",
    default=True,
    show_default=True,
    help="--dry-run (the default) lists every orphaned spill directory and "
    "deletes nothing. --execute performs the deletion. This deletes "
    "materialised data with no undo, so the safe default shows what would "
    "go before anything goes — the same reason `report-prune --vacuum` (a "
    "cheaper, reversible-by-recompute deletion) stays opt-in-only in that "
    "command, taken further here because a handoff spill is a full "
    "island's output and recomputing it can be expensive.",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format. `text` (default) renders a table. `json` emits a "
    "machine-readable, ANSI-free document.",
)
@click.option(
    "--older-than",
    "older_than_raw",
    default=None,
    metavar="DURATION",
    help="Additionally reclaim a kept-failure spill whose run FINISHED more "
    "than this long ago, even though no later success has superseded it yet "
    "(Phase 89 item 4 — the operator/watchdog reclaim the spec's gap "
    "sentence names). Duration literal: an integer plus d/h/m, e.g. '7d', "
    "'24h', '90m'. Omit to leave today's behaviour byte-identical — this "
    "flag is additive, never automatic, and applies to both --dry-run and "
    "--execute.",
)
@_env_options
def handoff_sweep(
    config_path: str | None,
    store_dir: str | None,
    dry_run: bool,
    fmt: str,
    older_than_raw: str | None,
    env_file: str | None,
    cli_env: tuple[str, ...],
) -> None:
    """Remove handoff spill directories orphaned by a run that never
    reached its own cleanup (a driver crash, a killed process, or a heal
    that moved the Manifest's hash and left the pre-patch hash directory
    unreachable).

    A spill directory (``<handoff.root>/<manifest_hash>/<run_id>/...``) is
    orphaned — never live — when its ``run_id`` is in ``run_records`` with a
    TERMINAL status and is not a kept failure still awaiting a resume: a
    non-terminal run (no ``finished_at`` yet — still running, or crashed
    before writing one) is NEVER swept, a terminal failure is kept while
    ``handoff.keep_on_failure`` is set until a LATER run of the same
    Blueprint succeeds (the resume story an `aqueduct run --resume` depends
    on), and a ``run_id`` with no ``run_records`` row at all is treated as
    unknown and reclaimed (see ``aqueduct/executor/spill.py::sweep_orphan_spills``
    for the full rule — this command applies exactly that rule, with no
    "current run" to exempt).

    Every registered engine already runs an automatic version of this sweep
    at the start of each polyglot run; this is the explicit, on-demand
    counterpart for a Blueprint that failed and was never rerun, or for
    routine disk hygiene between runs.

    ``--older-than <duration>`` extends that on-demand counterpart further:
    a kept-failure spill still protected by ``handoff.keep_on_failure``
    (no later success has superseded it) is ALSO reclaimed once its run
    finished longer ago than the given age — an explicit operator/watchdog
    action for a Blueprint that failed and is never going to be rerun.
    Automatic, config-driven time-based deletion is out of scope: this only
    ever runs when an operator or an external monitoring tool passes the
    flag.
    """
    from aqueduct.config import ConfigError, load_config
    from aqueduct.executor.spill import (
        dir_size_bytes,
        local_only_or_fsspec_available,
        parse_duration,
        plan_orphan_sweep,
        sweep_orphan_spills,
    )
    from aqueduct.stores.queries import discover_stores

    older_than = None
    if older_than_raw is not None:
        try:
            older_than = parse_duration(older_than_raw)
        except ValueError as exc:
            click.echo(f"✗ {exc}", err=True)
            sys.exit(exit_codes.CONFIG_ERROR)

    try:
        _resolve_and_load_env(env_file, Path(config_path) if config_path else None, cli_env=cli_env)
        cfg = load_config(Path(config_path) if config_path else None)
        _apply_warnings_from_cfg(cfg)
    except ConfigError as exc:
        click.echo(f"✗ config error: {exc}", err=True)
        sys.exit(exit_codes.CONFIG_ERROR)

    from aqueduct.cli import _resolve_project_root

    project_root = _resolve_project_root(config_path=Path(config_path) if config_path else None)
    root = _resolve_handoff_root(cfg.handoff.root, project_root)

    if not local_only_or_fsspec_available(root):
        msg = (
            f"handoff.root {root!r} is a remote URI and the fsspec package is "
            "not installed — Aqueduct cannot list or delete spill directories "
            "there. Install a store-backend extra that bundles fsspec (e.g. "
            "aqueduct-core[object-store]) to enable this command for a remote "
            "root."
        )
        if fmt.lower() == "json":
            emit({"root": root, "error": msg, "candidates": []}, fmt="json")
        else:
            click.echo(f"✗ {msg}", err=True)
        sys.exit(exit_codes.DATA_OR_RUNTIME)

    handles = discover_stores(cfg, store_dir=store_dir)
    obs_store: Any = _FederatedObsStore([h.store for h in handles]) if handles else None

    candidates = plan_orphan_sweep(
        root,
        current_run_id=None,  # no run is "in progress" for a standalone sweep
        keep_on_failure=cfg.handoff.keep_on_failure,
        obs_store=obs_store,
        older_than=older_than,
    )
    # Measured BEFORE any deletion — `--execute` removes these paths below,
    # after which `dir_size_bytes` can only see "gone".
    sizes = {c.path: dir_size_bytes(c.path) for c in candidates}

    removed: list[str] = []
    if candidates and not dry_run:
        removed = sweep_orphan_spills(
            root,
            current_run_id=None,
            keep_on_failure=cfg.handoff.keep_on_failure,
            obs_store=obs_store,
            dry_run=False,
            older_than=older_than,
        )
    removed_set = set(removed)

    if fmt.lower() == "json":
        emit(
            {
                "root": root,
                "dry_run": dry_run,
                "candidates": [
                    {
                        "path": c.path,
                        "manifest_hash": c.manifest_hash,
                        "run_id": c.run_id,
                        "status": c.status,
                        "reason": c.reason,
                        "reclaimed_by_age": c.reclaimed_by_age,
                        "bytes": sizes.get(c.path),
                        "removed": (not dry_run) and c.path in removed_set,
                    }
                    for c in candidates
                ],
            },
            fmt="json",
        )
        return

    if not candidates:
        _echo(f"handoff.root {root} — no orphaned spill directories found.", err=False)
        return

    _echo(
        f"handoff.root {root} — {len(candidates)} orphaned spill "
        f"director{'y' if len(candidates) == 1 else 'ies'}"
        + (" (dry run — nothing removed):" if dry_run else ":"),
        err=False,
    )
    render_table(
        [
            Column("run_id"),
            Column("manifest_hash"),
            Column("status"),
            Column("size", align="right"),
            Column("reason", flex=True),
        ],
        [
            [
                c.run_id or "(hash dir)",
                c.manifest_hash[:12],
                c.status or "-",
                format_bytes(sizes.get(c.path)),
                c.reason,
            ]
            for c in candidates
        ],
    )
    if dry_run:
        _echo("Pass --execute to actually delete these directories.", err=False)
    else:
        _echo(f"removed {len(removed)}/{len(candidates)} director(ies).", err=False)
