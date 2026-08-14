"""Multi-island polyglot execution orchestrator (Phase 81 step 3).

A single-engine Manifest runs through one engine's own
``ExecutorProtocol.execute()`` directly — unchanged, e.g. via
``aqueduct/cli/run.py``. A Manifest whose ``islands`` span more than one
engine needs a coordinator ABOVE that per-engine call: open a session per
island (lazily, in island-topological order — an island's upstream
boundary(ies) must finish before it starts), execute that island's modules
through its OWN engine on an island-scoped sub-Manifest, perform each
boundary's synthetic Handoff module's write (upstream island) / read
(downstream island) half, and close each island's session IMMEDIATELY
after that island finishes.

**Deliberate v1 choice, not yet optimized:** an engine's session closes
after its island even if that SAME engine recurs later in the run (e.g.
spark -> duckdb -> spark). Keeping a live session across a re-occurrence is
a recorded, statically-decidable optimization for a later step, not this
one — every island gets its own fresh session, always.

``run_polyglot()`` is a strict superset of the single-engine case: a
Manifest with exactly one island (including a single-engine Blueprint)
runs correctly through this same code path (one island, no handoffs,
executed once) — this module does not special-case "only one engine".
Disjoint components on different engines have zero boundary edges (Step
1's free lunch) and simply have no dependency between them in the island
execution order below; both still run, unconditionally.

Islands are derived at COMPILE time (``aqueduct.compiler.islands``) and
carried on ``Manifest.islands`` — this module never re-derives them and
never imports pyspark/duckdb; it only knows engines by NAME, resolved
through ``aqueduct.executor.protocol.get_protocol()``.

**Wired into ``aqueduct run`` (2.37).** ``aqueduct/cli/run.py``'s healing
loop routes a >1-island Manifest through ``run_polyglot(..., record_result=
False)`` instead of the single-engine ``execute()`` call — a single-engine
Manifest (``len(manifest.islands) <= 1``) is entirely unaffected, the exact
same call it has always made. ``record_result=False`` lets the CLI call
``surveyor.record(result, exc=..., engine=result.failed_engine)`` itself,
so a failed run is attributed to the ISLAND that failed rather than
``deployment.engine`` — the same shape the single-engine path already
uses. This module's own ``ExecuteError`` wrap around each island's
``execute()`` call (below) is what makes ``result.failed_engine`` reliable
even for a structural failure (a cycle, a bad checkpoint) that raises
rather than returning a ``ModuleResult``.
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from aqueduct.executor.models import (
    ExecutionResult,
    ExecutionStatus,
    ModuleResult,
    manifest_hash as _manifest_hash,
    resolve_observability_store,
)
from aqueduct.executor.protocol import SessionSpec, get_protocol
from aqueduct.executor.spill import (
    RULE_ID_HANDOFF_CLEANUP_UNAVAILABLE,
    delete_spill_tree,
    ensure_parent_exists,
    is_remote_uri,
    local_only_or_fsspec_available,
    spill_dir_for,
    sweep_orphan_spills,
)
from aqueduct.models import Island, Manifest, Module, ModuleType

logger = logging.getLogger(__name__)


class OrchestratorError(Exception):
    """Raised for orchestration-level setup failures (not per-module errors,
    which are always reported as ModuleResult(status="error", ...))."""


@dataclass(frozen=True)
class _HandoffEdge:
    module: Module  # the synthetic Handoff module
    from_module: str
    to_module: str
    from_island_idx: int
    to_island_idx: int


def _handoff_edges(manifest: Manifest) -> list[_HandoffEdge]:
    """Every Handoff module in *manifest*, resolved to the island INDICES it
    bridges. A Handoff module's id is never a member of any
    ``Island.module_ids`` (see ``aqueduct.compiler.handoff``), so this looks
    up its `from_module`/`to_module` neighbors' islands instead."""
    island_of: dict[str, int] = {}
    for idx, isl in enumerate(manifest.islands):
        for mid in isl.module_ids:
            island_of[mid] = idx

    out: list[_HandoffEdge] = []
    for m in manifest.modules:
        if m.type != ModuleType.Handoff or not m.enabled:
            continue
        from_module = m.config.get("from_module")
        to_module = m.config.get("to_module")
        from_idx = island_of.get(from_module)
        to_idx = island_of.get(to_module)
        if from_idx is None or to_idx is None:
            raise OrchestratorError(
                f"handoff module {m.id!r} references a module not in any "
                f"island (from_module={from_module!r}, to_module={to_module!r}) "
                "— this should be unreachable; please report this as a bug."
            )
        out.append(
            _HandoffEdge(
                module=m,
                from_module=from_module,
                to_module=to_module,
                from_island_idx=from_idx,
                to_island_idx=to_idx,
            )
        )
    return out


def _island_execution_order(manifest: Manifest, handoffs: list[_HandoffEdge]) -> list[int]:
    """Topological order over island INDICES: an island depends on every
    island that writes a handoff it reads from. Islands with no dependency
    relation (disjoint components — Step 1's zero-handoff free lunch) sort
    stably by their position in ``manifest.islands``."""
    n = len(manifest.islands)
    depends_on: dict[int, set[int]] = {i: set() for i in range(n)}
    for h in handoffs:
        if h.from_island_idx != h.to_island_idx:
            depends_on[h.to_island_idx].add(h.from_island_idx)

    in_degree = {i: len(depends_on[i]) for i in range(n)}
    dependents: dict[int, list[int]] = {i: [] for i in range(n)}
    for i, deps in depends_on.items():
        for d in deps:
            dependents[d].append(i)

    queue: deque[int] = deque(i for i in range(n) if in_degree[i] == 0)
    order: list[int] = []
    while queue:
        i = queue.popleft()
        order.append(i)
        for dep in dependents[i]:
            in_degree[dep] -= 1
            if in_degree[dep] == 0:
                queue.append(dep)

    if len(order) != n:
        raise OrchestratorError(
            "island dependency graph has a cycle — this should be "
            "unreachable (a boundary edge only ever points from an upstream "
            "island to a downstream one); please report this as a bug."
        )
    return order


def _sub_manifest(
    manifest: Manifest,
    island: Island,
    handoffs: list[_HandoffEdge],
    island_idx: int,
) -> Manifest:
    """Filter *manifest* down to one island's modules, plus the Handoff
    module(s) bridging it — on the WRITE side (this island produced
    ``from_module``) or the READ side (this island consumes via
    ``to_module``), never both in the same sub-manifest for the same
    boundary. Edges are filtered to those whose BOTH endpoints are in the
    resulting module set, which is exactly what makes a Handoff module's
    per-engine dispatch see only the relevant half of its two edges (an
    incoming edge on the write side, none on the read side).
    """
    module_ids = set(island.module_ids)
    for h in handoffs:
        if h.from_island_idx == island_idx or h.to_island_idx == island_idx:
            module_ids.add(h.module.id)

    modules = tuple(m for m in manifest.modules if m.id in module_ids)
    edges = tuple(e for e in manifest.edges if e.from_id in module_ids and e.to_id in module_ids)
    return replace(manifest, modules=modules, edges=edges)


def _spill_uris_for_island(
    handoffs: list[_HandoffEdge],
    island_idx: int,
    root: str,
    manifest_h: str,
    run_id: str,
) -> dict[str, str]:
    uris: dict[str, str] = {}
    for h in handoffs:
        if h.from_island_idx == island_idx or h.to_island_idx == island_idx:
            edge_id = h.module.config.get("edge_id", h.module.id)
            uris[h.module.id] = spill_dir_for(root, manifest_h, run_id, edge_id)
    return uris


def _resume_spill_uris_for_island(
    handoffs: list[_HandoffEdge],
    island_idx: int,
    root: str,
    manifest_h: str,
    resume_run_id: str | None,
) -> dict[str, str]:
    """For handoffs whose WRITE side is this island, the resume run's
    already-spilled directory, if one exists — used to decide whether this
    island can be skipped entirely (see the inline ``can_resume`` block in
    ``run_polyglot`` below, around the ``_resume_spill_uris_for_island``
    call site)."""
    if not resume_run_id:
        return {}
    out: dict[str, str] = {}
    for h in handoffs:
        if h.from_island_idx != island_idx:
            continue
        edge_id = h.module.config.get("edge_id", h.module.id)
        out[h.module.id] = spill_dir_for(root, manifest_h, resume_run_id, edge_id)
    return out


def _spill_exists(uri: str) -> bool:
    """True when *uri* is a real, non-empty spill directory. A falsy/empty
    ``uri`` (the ``resume_uris.get(h.module.id, "")`` default a caller uses
    when there is no resume candidate at all) means "no resume URI was
    resolved" and must be False here without ever touching the filesystem —
    ``Path("")`` normalizes to ``Path(".")``, the CURRENT WORKING DIRECTORY,
    which always exists, and ``aqueduct run`` chdirs to the project root
    before this ever runs. A project root that happens to contain ANY
    ``*.parquet``-named entry directly in it (a totally ordinary layout —
    Spark writes a Parquet ``path`` as a directory, and a Blueprint's own
    Ingress/Egress path is very often one) previously made this return a
    false True, which `run_polyglot()`'s `can_resume` branch then trusted
    to skip re-execution and read a resume URI that was never actually
    resolved (`resume_uris[h.module.id]` — no such key), raising `KeyError`
    on the very first real polyglot `aqueduct run`."""
    if not uri:
        return False
    if is_remote_uri(uri):
        # Engine-native remote reads work without fsspec; existence-checking
        # a remote URI generically does not (see spill.py's module
        # docstring) — conservatively treat as "not resumable" rather than
        # guessing, so the island re-runs and re-spills instead of reading a
        # possibly-partial or absent directory.
        return False
    p = Path(uri)
    return p.exists() and any(p.glob("*.parquet"))


class _PolyglotResult:
    """Internal accumulator — not part of the public return contract."""

    def __init__(self) -> None:
        self.module_results: list[Any] = []
        self.failed_engine: str | None = None
        self.trigger_agent = False
        self.status = ExecutionStatus.SUCCESS


def run_polyglot(
    manifest: Manifest,
    *,
    run_id: str,
    handoff_root: str,
    keep_on_failure: bool = True,
    resume_run_id: str | None = None,
    store_dir: Path | None = None,
    checkpoint_root: Path | None = None,
    surveyor: Any = None,
    depot: Any = None,
    observability_store: Any = None,
    warnings_suppress: set[str] | None = None,
    warnings_silence_all: bool = False,
    engine_configs: dict[str, dict[str, Any]] | None = None,
    master_url: str = "",
    quiet_startup: bool = True,
    timezone: str | None = None,
    secrets_config: dict[str, Any] | None = None,
    block_full_actions: bool = False,
    parallel: bool = False,
    use_observe: bool = False,
    sampling: Any = None,
    explain_capture: dict[str, dict] | None = None,
    record_result: bool = True,
) -> ExecutionResult:
    """Execute a (possibly polyglot) compiled Manifest island by island.

    Args:
        manifest:       Compiled Manifest with ``.islands`` populated
                         (always true for anything ``compiler.compile()``
                         produced).
        run_id:          This run's identifier — the SAME run_id every
                         island's ``execute()`` call receives (run_id was
                         never an engine's own session/app id; it does not
                         change here).
        handoff_root:    ``aqueduct.yml``'s ``handoff.root`` — the spill
                         root BOTH engines on either side of any boundary
                         can read and write.
        keep_on_failure: ``aqueduct.yml``'s ``handoff.keep_on_failure``.
        resume_run_id:   If set, an island whose OUTGOING handoff(s) already
                         have a spilled directory under this prior run_id is
                         skipped entirely (status="skipped" for its
                         modules) and the downstream island reads that
                         prior spill instead — the resume story: a rerun
                         after a heal does not recompute an already-healthy
                         upstream island. NOT forwarded to a non-skipped
                         island's own ``execute()`` call — both engines'
                         executors treat a non-empty ``resume_run_id`` as
                         "a checkpoint must exist for THIS island's modules
                         under this run_id" and raise otherwise, which is
                         the common case for an island that never
                         checkpointed under the failed prior run. Per-module
                         ``checkpoint: true`` resume for an arbitrary island
                         is not wired by this function; only cross-engine
                         handoff-spill resume is. When a resume actually
                         happened AND this run then SUCCEEDS, the resumed-FROM
                         run's spill directory is DELETED — it was kept for
                         exactly the rerun that has now consumed it. A failed
                         resume keeps it (still resumable).
        checkpoint_root, block_full_actions: forwarded to every island's
                         ``execute()`` unfiltered — part of ``ExecutorProtocol
                         .execute``'s COMMON kwargs (`aqueduct/executor/
                         protocol.py`), which every registered engine must
                         accept.
        parallel, use_observe, sampling, explain_capture: forwarded to every
                         island's ``execute()`` THROUGH ``call_execute()``
                         (`aqueduct/executor/protocol.py`) — these are
                         ``OPTIONAL_EXECUTE_KWARGS``, so an island whose
                         engine doesn't declare support gets the kwarg
                         dropped with a suppressible ``engine_kwarg_ignored``
                         warning instead of a ``TypeError``, exactly the
                         single-engine path's existing behavior.
        engine_configs:  ``{engine_name: SessionSpec.engine_config}`` — e.g.
                         ``{"spark": merged_spark_conf}``. An engine not
                         present here gets ``{}``.
        master_url:      Passed to every island's ``SessionSpec`` (engines
                         that ignore it, e.g. DuckDB, simply don't read it).
        secrets_config:  ``aqueduct.yml``'s resolved ``secrets:`` block —
                         ``{"provider": ..., "region": ..., "resolver": ...,
                         "base_dir": ...}`` — passed through every island's
                         ``SessionSpec.engine_options["secrets"]`` so an
                         engine that needs to resolve a secret KEY NAME into
                         a VALUE (DuckDB's ``engine.duckdb.s3_key_id_secret``
                         -> DuckDB's own ``CREATE SECRET``) calls the SAME
                         ``aqueduct.secrets.resolve_secret`` ``@aq.secret()``
                         uses, never a parallel credential path. An engine
                         with no use for it (Spark) ignores the key.
        timezone:        ``aqueduct.yml``'s top-level ``timezone:`` (Phase
                         81/82), passed to EVERY island's ``SessionSpec`` —
                         each engine applies it its own way (Spark's
                         ``spark.sql.session.timeZone``, DuckDB's ``SET
                         TimeZone``) and Spark warns on a divergence from its
                         own ``engine.spark.conf`` override. This is
                         precisely the shape that makes cross-engine
                         timezone divergence visible instead of silent —
                         the whole reason this key exists.
        record_result:   When True (the default — preserves this function's
                         existing standalone/tested contract), this call
                         records the run's outcome itself via
                         ``surveyor.record(merged, engine=...)`` before
                         returning. A caller that needs the returned
                         ``FailureContext`` to drive its OWN healing
                         decision (``aqueduct/cli/run.py``) passes False and
                         calls ``surveyor.record(result, exc=..., engine=
                         result.failed_engine)`` itself instead — the same
                         shape the single-engine path already uses, so a
                         failed run is attributed to the ISLAND that
                         failed rather than ``deployment.engine``.

    Returns:
        One merged ``ExecutionResult`` spanning every island executed
        (fail-fast: an island that errors stops the run; islands after it
        in topological order are never started), with ``.failed_engine``
        set to the failing island's engine when ``status == "error"``.
        When ``record_result`` is True (the default), ``surveyor.record()``
        is also called internally (if ``surveyor`` is given) with that same
        engine — this is the one piece of per-run bookkeeping the
        orchestrator must own by default, since it is the only caller that
        knows which island actually failed.
    """
    from aqueduct.errors import ExecuteError
    from aqueduct.executor.protocol import call_execute

    manifest_h = _manifest_hash(manifest)
    obs_store = resolve_observability_store(store_dir, observability_store)

    # ── Orphan sweep — before this run's own spill exists on disk ───────────
    if not local_only_or_fsspec_available(handoff_root):
        from aqueduct.warnings import emit as _emit

        _emit(
            RULE_ID_HANDOFF_CLEANUP_UNAVAILABLE,
            f"handoff.root {handoff_root!r} is a remote URI and the fsspec "
            "package is not installed — Aqueduct cannot list or delete "
            "handoff spill directories there (the ENGINES still write/read "
            "them natively; only Aqueduct's own cleanup needs fsspec). Spill "
            "will accumulate under this root until fsspec is installed. "
            "Install a store-backend extra that bundles fsspec (e.g. "
            "aqueduct-core[object-store]) to enable cleanup.",
            suppress=warnings_suppress,
        )
    else:
        sweep_orphan_spills(
            handoff_root,
            current_run_id=run_id,
            keep_on_failure=keep_on_failure,
            obs_store=obs_store,
        )

    handoffs = _handoff_edges(manifest)
    order = _island_execution_order(manifest, handoffs)

    acc = _PolyglotResult()
    run_spill_uris: dict[str, str] = {}  # every handoff's THIS-run spill_uri, for cleanup
    # True once at least one island was actually SKIPPED by reading
    # `resume_run_id`'s spill — i.e. that spill was provably consumed. A
    # `resume_run_id` that resolved to nothing on disk never sets this, so
    # the release below can't delete a directory this run did not use.
    resumed_from = False

    for island_idx in order:
        island = manifest.islands[island_idx]
        sub_manifest = _sub_manifest(manifest, island, handoffs, island_idx)

        this_run_uris = _spill_uris_for_island(
            handoffs, island_idx, handoff_root, manifest_h, run_id
        )
        # `setdefault`, not `update`: a handoff's WRITE-side island always
        # precedes its READ-side island in `order` (see
        # `_island_execution_order`), so by the time a downstream island's
        # turn comes around, `run_spill_uris` may ALREADY hold that handoff's
        # authoritative URI — either this run's own (the common case) or a
        # RESUMED run's (set in the `can_resume` branch below, on an earlier
        # iteration). Blindly overwriting it here with this island's own
        # freshly-computed (this-run) guess would silently point a resumed
        # read at a spill directory that was never written this run.
        for _hid, _uri in this_run_uris.items():
            run_spill_uris.setdefault(_hid, _uri)

        resume_uris = _resume_spill_uris_for_island(
            handoffs,
            island_idx,
            handoff_root,
            manifest_h,
            resume_run_id,
        )
        resumable_exits = [h for h in handoffs if h.from_island_idx == island_idx]
        can_resume = bool(resumable_exits) and all(
            _spill_exists(resume_uris.get(h.module.id, "")) for h in resumable_exits
        )

        if can_resume:
            logger.info(
                "Island (engine=%s) skipped — resuming from run %r's existing "
                "handoff spill for %s",
                island.engine,
                resume_run_id,
                [h.module.id for h in resumable_exits],
            )
            resumed_from = True
            for m in sub_manifest.modules:
                acc.module_results.append(_skipped_result(m.id))
            # Downstream islands must read the RESUME run's spill, not this
            # run's (which was never written).
            for h in resumable_exits:
                run_spill_uris[h.module.id] = resume_uris[h.module.id]
            continue

        # The URIs actually handed to `execute()` come from the authoritative
        # `run_spill_uris` accumulator (not the freshly-computed
        # `this_run_uris`), so a READ-side handoff whose WRITE side was
        # resumed from a prior run gets that prior run's directory, never
        # this run's un-populated one.
        exec_uris = {
            h.module.id: run_spill_uris[h.module.id]
            for h in handoffs
            if h.from_island_idx == island_idx or h.to_island_idx == island_idx
        }

        for uri in exec_uris.values():
            ensure_parent_exists(uri)

        protocol = get_protocol(island.engine)
        session = protocol.session_factory()(
            SessionSpec(
                blueprint_id=manifest.blueprint_id,
                engine_config=(engine_configs or {}).get(island.engine, {}),
                master_url=master_url,
                quiet_startup=quiet_startup,
                timezone=timezone,
                engine_options={"secrets": secrets_config} if secrets_config else {},
            )
        )
        try:
            try:
                # `call_execute()` (not `protocol.execute()` directly) so the
                # OPTIONAL capability kwargs (parallel/use_observe/sampling/
                # explain_capture) get the same per-engine filter-and-warn
                # treatment the single-engine CLI path already applies —
                # an island whose engine doesn't declare support for one
                # gets it dropped with a suppressible `engine_kwarg_ignored`
                # warning instead of a TypeError.
                result = call_execute(
                    island.engine,
                    sub_manifest,
                    session,
                    run_id=run_id,
                    store_dir=store_dir,
                    checkpoint_root=checkpoint_root,
                    # NOT `resume_run_id=resume_run_id` — that argument is
                    # reserved above for the cross-engine HANDOFF-spill
                    # resume decision (skip an island whose outgoing spill
                    # already exists). Forwarding it into a per-island
                    # `execute()` call as well was tried and reverted: both
                    # engines' executors treat a non-empty `resume_run_id`
                    # as "a checkpoint MUST exist for this run_id, for THIS
                    # island's modules" and raise `ExecuteError` when it
                    # doesn't — which is the common case, since most islands
                    # in a resumed run never checkpointed under the failed
                    # prior run_id at all. Per-module `checkpoint: true`
                    # resume for an arbitrary island is real, separate work
                    # (deciding per-island whether a matching checkpoint
                    # actually exists before forwarding), not part of this
                    # change — only cross-engine handoff-spill resume is
                    # wired here.
                    block_full_actions=block_full_actions,
                    surveyor=surveyor,
                    depot=depot,
                    observability_store=obs_store,
                    warnings_suppress=warnings_suppress,
                    warnings_silence_all=warnings_silence_all,
                    handoff_spill_uris=exec_uris,
                    parallel=parallel,
                    use_observe=use_observe,
                    sampling=sampling,
                    explain_capture=explain_capture,
                    suppress=warnings_suppress,
                )
            except ExecuteError as exc:
                # A structural execution failure (cycle detection, a bad
                # --from/--to selector, a missing resume checkpoint) raises
                # rather than returning a ModuleResult — both engines now
                # raise this SAME engine-agnostic `aqueduct.errors.ExecuteError`
                # (previously each engine defined its own private
                # `ExecuteError` subclass of `AqueductError`, so this handler
                # had no shared type to import without naming an engine and
                # caught the broader `AqueductError` instead — fixed as part
                # of the ExecuteError unification, see `aqueduct/errors.py`).
                # Without this, the exception would escape `run_polyglot()`
                # entirely: no island/engine attribution, no spill-lifecycle
                # bookkeeping below — exactly the gap the single-engine CLI
                # path already closes for itself with its own try/except
                # around `execute()`. Mirrored here so a polyglot run gets
                # the same attribution.
                result = ExecutionResult(
                    blueprint_id=manifest.blueprint_id,
                    run_id=run_id,
                    status=ExecutionStatus.ERROR,
                    module_results=(
                        ModuleResult(
                            module_id="_executor", status=ExecutionStatus.ERROR, error=str(exc)
                        ),
                    ),
                )
        finally:
            # Deliberate v1 choice: close THIS island's session now, even if
            # `island.engine` recurs later in `order` — see module docstring.
            protocol.session_closer()(session)

        acc.module_results.extend(result.module_results)
        acc.trigger_agent = acc.trigger_agent or result.trigger_agent
        if result.status != ExecutionStatus.SUCCESS:
            acc.status = ExecutionStatus.ERROR
            acc.failed_engine = island.engine
            break

    merged = ExecutionResult(
        blueprint_id=manifest.blueprint_id,
        run_id=run_id,
        status=acc.status,
        module_results=tuple(acc.module_results),
        trigger_agent=acc.trigger_agent,
        failed_engine=acc.failed_engine if acc.status != ExecutionStatus.SUCCESS else None,
    )

    # ── Spill lifecycle: delete on success; keep on failure unless the
    # config says not to. One directory covers every boundary this run. ────
    run_dir = f"{handoff_root.rstrip('/')}/{manifest_h}/{run_id}"
    if run_spill_uris:
        if merged.status == ExecutionStatus.SUCCESS or not keep_on_failure:
            delete_spill_tree(run_dir)

    # A successful resume RELEASES the spill it just consumed. That spill was
    # kept for exactly one documented purpose — "a rerun reads this instead
    # of recomputing it" — and this run is that rerun, so the purpose has
    # been served. Deleting only `run_id`'s own directory (above) leaked the
    # resumed-FROM one permanently: the prior run's `run_records` row stays
    # `status='error'` forever, so `sweep_orphan_spills` keeps exempting it
    # under `keep_on_failure` too, and a provably-consumed spill was the one
    # thing nothing could ever reclaim.
    #
    # A FAILED resume keeps it — the spill is still resumable, which is the
    # whole point of keeping a failure's spill in the first place.
    if (
        resumed_from
        and merged.status == ExecutionStatus.SUCCESS
        and resume_run_id
        and resume_run_id != run_id
    ):
        delete_spill_tree(f"{handoff_root.rstrip('/')}/{manifest_h}/{resume_run_id}")

    if record_result and surveyor is not None:
        surveyor.record(merged, engine=acc.failed_engine)

    return merged


def _skipped_result(module_id: str) -> Any:
    return ModuleResult(module_id=module_id, status=ExecutionStatus.SKIPPED)


__all__ = ["OrchestratorError", "run_polyglot"]
