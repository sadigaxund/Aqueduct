"""Multi-island polyglot execution orchestrator (Phase 81 step 3).

A single-engine Manifest runs through one engine's own
``ExecutorProtocol.execute()`` directly — unchanged, e.g. via
``aqueduct/cli/run.py``. A Manifest whose ``islands`` span more than one
engine needs a coordinator ABOVE that per-engine call: open a session per
island (lazily, in island-topological order — an island's upstream
boundary(ies) must finish before it starts), execute that island's modules
through its OWN engine on an island-scoped sub-Manifest, perform each
boundary's synthetic Handoff module's write (upstream island) / read
(downstream island) half, and tear down sessions according to
``execution.session_keep_alive``.

**Session keep-alive (Phase 89 item 1).** When ``session_keep_alive`` is
true (the default), a session is closed only when the run moves to an
island on a DIFFERENT engine (or when the run ends) — an island that shares
its engine with the PREVIOUS island in execution order (any same-engine
adjacency, not only a provably-sequential dependency) is instead handed
that still-live session directly, skipping a fresh session build entirely.
Because a reused session is not observationally fresh, session-scoped state
the finishing island's modules created (Spark/DuckDB catalog objects
registered via ``register_as_table``) is dropped at the boundary — through
each engine's own ``ExecutorProtocol.cleanup_reused_session`` hook, never a
hardcoded per-engine call here — UNLESS ``execution.share_island_state`` is
also true, which deliberately skips that cleanup so the next same-engine
island sees the previous one's registered tables (e.g. to avoid reloading
state when a run returns to an engine it already visited). Regardless of
either flag, EVERY session this function ever built or reused is closed by
the time it returns — success, failure, or a raised exception — so a
single-engine same-island decision never leaks a live session past this
call. Setting ``session_keep_alive`` to false restores the original v1
behavior exactly: every island gets its own fresh session, always, closed
the moment that island finishes.

A cross-ENGINE boundary is entirely unaffected by any of this — it stays
storage-mediated (the handoff spill), never in-memory session state.

**Same-run eager spill pruning (Phase 89 item 3).** Independently of
session keep-alive, once an island finishes SUCCESSFULLY, every handoff
edge it just READ (``h.to_island_idx == island_idx``) is provably done for
the rest of THIS run — an edge has exactly one reader island (verified: a
Handoff module's ``to_module`` resolves to one island, ``_handoff_edges``
above), so nothing later in this run's execution order will ever touch
that spill again. When ``handoff.prune_eagerly`` is true (the
default), that edge's directory is deleted right there instead of waiting
for the end-of-run ``delete_spill_tree(run_dir)``, bounding peak
spill-storage for a long chain instead of holding every boundary's output
until the whole run ends. This is deliberately narrower than the
``keep_on_failure``/orphan-sweep/supersession machinery in
``aqueduct.executor.spill``, which is entirely unmodified by this feature:
an edge whose WRITE side was itself resumed from a PRIOR run
(``resume_run_id``) is never eagerly pruned here — that spill's release is
owned exclusively by the "successful resume releases what it consumed"
logic further down this function, so the two mechanisms never race or
double-decide the same directory. Failure semantics are unchanged: pruning
only ever happens for an edge whose reader ALREADY succeeded, so a run that
fails at island N still has every spill feeding island N (and everything
after it) intact on disk for `--resume` — only spills belonging to islands
that are already fully done, in a run that is itself still succeeding so
far, are ever removed early.

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
    resolve_observability_store,
)
from aqueduct.executor.models import (
    manifest_hash as _manifest_hash,
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
    session_keep_alive: bool = True,
    share_island_state: bool = False,
    prune_eagerly: bool = True,
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
        session_keep_alive: ``aqueduct.yml``'s ``execution.session_keep_alive``
                         (default True). See the module docstring's "Session
                         keep-alive" section.
        share_island_state: ``aqueduct.yml``'s ``execution.share_island_state``
                         (default False). See the module docstring.
        prune_eagerly: ``aqueduct.yml``'s
                         ``handoff.prune_eagerly`` (default True).
                         See the module docstring's "Same-run eager spill
                         pruning" section. When False, no spill is deleted
                         until the run itself ends — the pre-Phase-89
                         behavior exactly.
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

    # ── Session keep-alive bookkeeping (Phase 89 item 1) ─────────────────
    # `live_*` tracks the session (if any) that is CURRENTLY open and not
    # yet closed — whether this run just built it or is reusing it from a
    # previous island. Wrapping the whole loop in this function-level
    # try/finally is what guarantees a run never leaks a live session past
    # `run_polyglot()`'s return, on success, failure, OR a raised exception
    # (e.g. an engine bug that isn't `ExecuteError`) — the invariant the v1
    # code got for free by closing every island's session in its own
    # per-island `finally` immediately below `call_execute()`. Keeping it at
    # the FUNCTION level instead is what lets a session survive past one
    # island's own `try` block into the next iteration.
    live_session: Any = None
    live_protocol: Any = None  # the ExecutorProtocol `live_session` belongs to
    live_sub_manifest: Manifest | None = None  # island sub-Manifest that last ran on it
    session_reused: list[str] = []  # one engine name per reuse boundary, in order

    # ── Eager spill pruning bookkeeping (Phase 89 item 3) ─────────────────
    # An edge's module id lands here the moment its WRITE side is resumed
    # from a PRIOR run (`resume_run_id`) — never eagerly pruned below, since
    # that spill's release belongs exclusively to the resume-release logic
    # at the end of this function, not to this same-run optimization.
    resumed_edge_module_ids: set[str] = set()
    pruned_spills: list[str] = []  # edge_ids pruned eagerly, in the order they were pruned

    try:
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
                    # Owned by the resume-release logic at the end of this
                    # function from here on — eager pruning below must never
                    # touch it.
                    resumed_edge_module_ids.add(h.module.id)
                # No session was touched for a resume-skipped island — `live_*`
                # (whatever it holds from an earlier island) is left exactly as
                # is, still eligible for reuse by a LATER island on the same
                # engine. A skipped island must never force a pointless build.
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

            # ── Resolve this island's session: reuse the live one when it's
            # the SAME engine as whatever is still open, otherwise close
            # that one (order matters — close THEN build, never a bare
            # `getOrCreate()` reuse, same invariant the single-engine
            # heal-retry funnel enforces) and build fresh. ────────────────
            reused = (
                session_keep_alive
                and live_session is not None
                and live_protocol is not None
                and live_protocol.engine == island.engine
            )
            if reused:
                session = live_session
                if not share_island_state:
                    try:
                        live_protocol.session_cleanup()(session, live_sub_manifest)
                    except Exception as exc:  # noqa: BLE001 — cleanup must never abort the reuse
                        logger.warning(
                            "session-reuse cleanup failed for engine %r before "
                            "island %d (%s) — continuing with the reused session "
                            "as-is",
                            island.engine,
                            island_idx,
                            exc,
                        )
                session_reused.append(island.engine)
            else:
                if live_session is not None:
                    live_protocol.session_closer()(live_session)
                    live_session = None
                    live_protocol = None
                    live_sub_manifest = None
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

            # From this point `session` is live and MUST be reachable through
            # `live_*` before anything that could raise runs — the function-
            # level `finally` below is what closes it if `call_execute` (or
            # anything else) raises something other than `ExecuteError`.
            live_session = session
            live_protocol = protocol
            live_sub_manifest = sub_manifest

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

            if not session_keep_alive:
                # Restores the original v1 behavior exactly: every island's
                # session closes the moment that island finishes.
                protocol.session_closer()(session)
                live_session = None
                live_protocol = None
                live_sub_manifest = None

            acc.module_results.extend(result.module_results)
            acc.trigger_agent = acc.trigger_agent or result.trigger_agent
            if result.status != ExecutionStatus.SUCCESS:
                acc.status = ExecutionStatus.ERROR
                acc.failed_engine = island.engine
                break

            # ── Eager spill pruning (Phase 89 item 3) — this island just
            # succeeded, so every handoff edge it READ is provably done for
            # the rest of this run (one edge has exactly one reader island).
            # Skip an edge whose write side was resumed from a PRIOR run —
            # that spill belongs to the resume-release logic below, not to
            # this same-run optimization (see module docstring).
            if prune_eagerly:
                for h in handoffs:
                    if h.to_island_idx != island_idx:
                        continue
                    if h.module.id in resumed_edge_module_ids:
                        continue
                    uri = run_spill_uris.get(h.module.id)
                    if not uri:
                        continue
                    if delete_spill_tree(uri):
                        pruned_spills.append(h.module.id)
    finally:
        # Whatever is still live when the loop exits — normally, via
        # `break` on failure, or because an exception propagated past the
        # `except ExecuteError` above — is closed exactly once here. This is
        # the guarantee that makes keep-alive safe: a run must never leak a
        # live session past `run_polyglot()`'s return.
        if live_session is not None:
            live_protocol.session_closer()(live_session)

    merged = ExecutionResult(
        blueprint_id=manifest.blueprint_id,
        run_id=run_id,
        status=acc.status,
        module_results=tuple(acc.module_results),
        trigger_agent=acc.trigger_agent,
        failed_engine=acc.failed_engine if acc.status != ExecutionStatus.SUCCESS else None,
        session_reused=tuple(session_reused),
        pruned_spills=tuple(pruned_spills),
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
