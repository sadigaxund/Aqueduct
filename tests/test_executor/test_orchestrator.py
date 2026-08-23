"""Unit tests for the pure, engine-agnostic helpers in
``aqueduct.executor.orchestrator`` (Phase 81 step 3) — island dependency
ordering, sub-manifest slicing, and spill-URI resolution. No real Spark/DuckDB
session is built here; ``tests/test_executor/test_orchestrator_e2e.py`` covers
the full ``run_polyglot()`` path against real engines.
"""

from __future__ import annotations

import pytest

from aqueduct.compiler.islands import Island
from aqueduct.compiler.models import Manifest
from aqueduct.executor.orchestrator import (
    OrchestratorError,
    _handoff_edges,
    _island_execution_order,
    _resume_spill_uris_for_island,
    _spill_exists,
    _spill_uris_for_island,
    _sub_manifest,
)
from aqueduct.parser.models import Edge, Module, ModuleType

pytestmark = pytest.mark.unit


def _handoff_module(id_, from_module, to_module, from_engine="spark", to_engine="duckdb"):
    return Module(
        id=id_,
        type=ModuleType.Handoff,
        label=id_,
        config={
            "edge_id": id_,
            "from_module": from_module,
            "to_module": to_module,
            "from_engine": from_engine,
            "to_engine": to_engine,
            "port": "main",
        },
        synthetic=True,
        engine=None,
    )


def _manifest(modules, edges, islands):
    return Manifest(
        blueprint_id="bp",
        context={},
        modules=tuple(modules),
        edges=tuple(edges),
        engine_config={},
        islands=tuple(islands),
    )


def _m(id_, engine, type_=ModuleType.Channel, config=None):
    return Module(id=id_, type=type_, label=id_, config=config or {}, engine=engine)


# ── _handoff_edges ───────────────────────────────────────────────────────────


def test_handoff_edges_resolves_island_indices():
    a = _m("a", "spark")
    b = _m("b", "duckdb")
    h = _handoff_module("a__handoff__b", "a", "b")
    manifest = _manifest(
        [a, h, b],
        [Edge(from_id="a", to_id="a__handoff__b"), Edge(from_id="a__handoff__b", to_id="b")],
        [
            Island(engine="spark", module_ids=frozenset({"a"})),
            Island(engine="duckdb", module_ids=frozenset({"b"})),
        ],
    )
    edges = _handoff_edges(manifest)
    assert len(edges) == 1
    assert edges[0].from_module == "a"
    assert edges[0].to_module == "b"
    assert edges[0].from_island_idx == 0
    assert edges[0].to_island_idx == 1


def test_handoff_edges_ignores_disabled_module():
    a = _m("a", "spark")
    b = _m("b", "duckdb")
    from dataclasses import replace

    h = replace(_handoff_module("h", "a", "b"), enabled=False)
    manifest = _manifest(
        [a, h, b],
        [Edge(from_id="a", to_id="h"), Edge(from_id="h", to_id="b")],
        [
            Island(engine="spark", module_ids=frozenset({"a"})),
            Island(engine="duckdb", module_ids=frozenset({"b"})),
        ],
    )
    assert _handoff_edges(manifest) == []


def test_handoff_edges_raises_when_endpoint_not_in_any_island():
    h = _handoff_module("h", "missing_a", "missing_b")
    manifest = _manifest([h], [], [])
    with pytest.raises(OrchestratorError, match="not in any island"):
        _handoff_edges(manifest)


# ── _island_execution_order ──────────────────────────────────────────────────


def test_island_execution_order_linear_dependency():
    a = _m("a", "spark")
    b = _m("b", "duckdb")
    h = _handoff_module("h", "a", "b")
    manifest = _manifest(
        [a, h, b],
        [Edge(from_id="a", to_id="h"), Edge(from_id="h", to_id="b")],
        [
            Island(engine="spark", module_ids=frozenset({"a"})),
            Island(engine="duckdb", module_ids=frozenset({"b"})),
        ],
    )
    handoffs = _handoff_edges(manifest)
    order = _island_execution_order(manifest, handoffs)
    assert order == [0, 1]


def test_island_execution_order_disjoint_components_sort_stably():
    """Zero handoffs between two disjoint different-engine islands — both
    still run, ordered by their position in ``manifest.islands``."""
    a = _m("a", "spark")
    b = _m("b", "duckdb")
    manifest = _manifest(
        [a, b],
        [],
        [
            Island(engine="spark", module_ids=frozenset({"a"})),
            Island(engine="duckdb", module_ids=frozenset({"b"})),
        ],
    )
    order = _island_execution_order(manifest, [])
    assert order == [0, 1]


def test_island_execution_order_cycle_raises():
    a = _m("a", "spark")
    b = _m("b", "duckdb")
    h1 = _handoff_module("h1", "a", "b")
    h2 = _handoff_module("h2", "b", "a")
    manifest = _manifest(
        [a, h1, h2, b],
        [],
        [
            Island(engine="spark", module_ids=frozenset({"a"})),
            Island(engine="duckdb", module_ids=frozenset({"b"})),
        ],
    )
    from aqueduct.executor.orchestrator import _HandoffEdge

    handoffs = [
        _HandoffEdge(module=h1, from_module="a", to_module="b", from_island_idx=0, to_island_idx=1),
        _HandoffEdge(module=h2, from_module="b", to_module="a", from_island_idx=1, to_island_idx=0),
    ]
    with pytest.raises(OrchestratorError, match="cycle"):
        _island_execution_order(manifest, handoffs)


# ── _sub_manifest ─────────────────────────────────────────────────────────────


def test_sub_manifest_write_side_includes_handoff_and_incoming_edge():
    a = _m("a", "spark")
    b = _m("b", "duckdb")
    h = _handoff_module("h", "a", "b")
    manifest = _manifest(
        [a, h, b],
        [Edge(from_id="a", to_id="h"), Edge(from_id="h", to_id="b")],
        [
            Island(engine="spark", module_ids=frozenset({"a"})),
            Island(engine="duckdb", module_ids=frozenset({"b"})),
        ],
    )
    handoffs = _handoff_edges(manifest)
    sub = _sub_manifest(manifest, manifest.islands[0], handoffs, 0)
    assert {m.id for m in sub.modules} == {"a", "h"}
    assert {(e.from_id, e.to_id) for e in sub.edges} == {("a", "h")}


def test_sub_manifest_read_side_includes_handoff_and_outgoing_edge():
    a = _m("a", "spark")
    b = _m("b", "duckdb")
    h = _handoff_module("h", "a", "b")
    manifest = _manifest(
        [a, h, b],
        [Edge(from_id="a", to_id="h"), Edge(from_id="h", to_id="b")],
        [
            Island(engine="spark", module_ids=frozenset({"a"})),
            Island(engine="duckdb", module_ids=frozenset({"b"})),
        ],
    )
    handoffs = _handoff_edges(manifest)
    sub = _sub_manifest(manifest, manifest.islands[1], handoffs, 1)
    assert {m.id for m in sub.modules} == {"b", "h"}
    assert {(e.from_id, e.to_id) for e in sub.edges} == {("h", "b")}


def test_sub_manifest_disjoint_island_has_no_handoff_module():
    a = _m("a", "spark")
    b = _m("b", "duckdb")
    manifest = _manifest(
        [a, b],
        [],
        [
            Island(engine="spark", module_ids=frozenset({"a"})),
            Island(engine="duckdb", module_ids=frozenset({"b"})),
        ],
    )
    sub_a = _sub_manifest(manifest, manifest.islands[0], [], 0)
    sub_b = _sub_manifest(manifest, manifest.islands[1], [], 1)
    assert {m.id for m in sub_a.modules} == {"a"}
    assert {m.id for m in sub_b.modules} == {"b"}
    assert not any(m.type == ModuleType.Handoff for m in sub_a.modules)
    assert not any(m.type == ModuleType.Handoff for m in sub_b.modules)


# ── _spill_uris_for_island / _resume_spill_uris_for_island / _spill_exists ──


def test_spill_uris_for_island_covers_both_sides():
    a = _m("a", "spark")
    b = _m("b", "duckdb")
    h = _handoff_module("h", "a", "b")
    manifest = _manifest(
        [a, h, b],
        [Edge(from_id="a", to_id="h"), Edge(from_id="h", to_id="b")],
        [
            Island(engine="spark", module_ids=frozenset({"a"})),
            Island(engine="duckdb", module_ids=frozenset({"b"})),
        ],
    )
    handoffs = _handoff_edges(manifest)
    write_uris = _spill_uris_for_island(handoffs, 0, "/root", "abc123", "run1")
    read_uris = _spill_uris_for_island(handoffs, 1, "/root", "abc123", "run1")
    assert write_uris == {"h": "/root/abc123/run1/h"}
    assert read_uris == {"h": "/root/abc123/run1/h"}


def test_resume_spill_uris_only_for_write_side():
    a = _m("a", "spark")
    b = _m("b", "duckdb")
    h = _handoff_module("h", "a", "b")
    manifest = _manifest(
        [a, h, b],
        [Edge(from_id="a", to_id="h"), Edge(from_id="h", to_id="b")],
        [
            Island(engine="spark", module_ids=frozenset({"a"})),
            Island(engine="duckdb", module_ids=frozenset({"b"})),
        ],
    )
    handoffs = _handoff_edges(manifest)
    # Write side (island 0) resolves a resume URI.
    assert _resume_spill_uris_for_island(handoffs, 0, "/root", "abc123", "prevrun") == {
        "h": "/root/abc123/prevrun/h"
    }
    # Read side (island 1) never resolves one — it only ever consumes.
    assert _resume_spill_uris_for_island(handoffs, 1, "/root", "abc123", "prevrun") == {}


def test_resume_spill_uris_empty_when_no_resume_run_id():
    a = _m("a", "spark")
    b = _m("b", "duckdb")
    h = _handoff_module("h", "a", "b")
    manifest = _manifest(
        [a, h, b],
        [Edge(from_id="a", to_id="h"), Edge(from_id="h", to_id="b")],
        [
            Island(engine="spark", module_ids=frozenset({"a"})),
            Island(engine="duckdb", module_ids=frozenset({"b"})),
        ],
    )
    handoffs = _handoff_edges(manifest)
    assert _resume_spill_uris_for_island(handoffs, 0, "/root", "abc123", None) == {}


def test_spill_exists_local(tmp_path):
    spill = tmp_path / "spill"
    assert _spill_exists(str(spill)) is False
    spill.mkdir()
    (spill / "part-0.parquet").write_bytes(b"x")
    assert _spill_exists(str(spill)) is True


def test_spill_exists_remote_always_false():
    """A remote URI's existence is never guessed at — treated as
    not-resumable so the island always re-runs rather than reading a
    possibly-partial spill (see the function's docstring)."""
    assert _spill_exists("s3://bucket/root/abc123/run1/h") is False


def test_spill_exists_empty_uri_never_touches_cwd(tmp_path, monkeypatch):
    """Regression: `Path("")` normalizes to `Path(".")` — the CWD, which
    always exists. A caller passing the empty-string default (no resume URI
    resolved at all) must get False regardless of what happens to be sitting
    in the current working directory, never a filesystem-dependent answer.
    This is exactly what a real `aqueduct run` hits: it chdirs to the
    project root, which very often contains a `*.parquet`-named Ingress/
    Egress path directly in it (Spark writes a `path` as a directory) —
    `_spill_exists("")` used to read that as "the spill already exists" and
    `run_polyglot()`'s resume branch then KeyError'd resolving a resume URI
    that was never actually computed."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "something.parquet").mkdir()
    (tmp_path / "something.parquet" / "part-0.parquet").write_bytes(b"x")
    assert _spill_exists("") is False


# ── The resumed-FROM spill is released by a successful resume ────────────────
#
# `run_polyglot()` used to delete only `<root>/<hash>/<run_id>` — the CURRENT
# run's directory — so the spill it had just CONSUMED (`<hash>/<resume_run_id>`)
# survived. That prior run's `run_records` row stays `status='error'` forever,
# so `sweep_orphan_spills` kept exempting it under `keep_on_failure` as well:
# a provably-consumed spill was the one directory nothing in the system could
# ever reclaim. These tests drive the real `run_polyglot()` with a stub engine
# protocol (no Spark/DuckDB) so the lifecycle is asserted directly.


def _two_island_manifest():
    a = _m("a", "spark")
    b = _m("b", "duckdb")
    h = _handoff_module("h", "a", "b")
    return _manifest(
        [a, h, b],
        [Edge(from_id="a", to_id="h"), Edge(from_id="h", to_id="b")],
        [
            Island(engine="spark", module_ids=frozenset({"a"})),
            Island(engine="duckdb", module_ids=frozenset({"b"})),
        ],
    )


def _install_stub_engine(monkeypatch, *, island_status):
    """Patch out engine resolution + execution so `run_polyglot()` runs its
    real orchestration/spill-lifecycle code with no engine installed.

    `island_status` maps an executed module id -> the ExecutionStatus that
    module's result carries. Any handoff module handed to a call gets its
    spill directory materialized, exactly as a real engine's write side
    would."""
    import aqueduct.executor.orchestrator as orch
    import aqueduct.executor.protocol as proto
    from aqueduct.executor.models import ExecutionResult, ExecutionStatus, ModuleResult

    class _StubProtocol:
        def __init__(self, engine):
            self.engine = engine

        def session_factory(self):
            return lambda spec: object()

        def session_closer(self):
            return lambda session: None

        def session_cleanup(self):
            return lambda session, manifest: None

    monkeypatch.setattr(orch, "get_protocol", lambda engine: _StubProtocol(engine))

    def _fake_call_execute(engine, manifest, session, **kw):
        from pathlib import Path as _P

        for uri in (kw.get("handoff_spill_uris") or {}).values():
            _P(uri).mkdir(parents=True, exist_ok=True)
            (_P(uri) / "part-0.parquet").write_bytes(b"spilled")
        results = []
        status = ExecutionStatus.SUCCESS
        for m in manifest.modules:
            st = island_status.get(m.id, ExecutionStatus.SUCCESS)
            results.append(ModuleResult(module_id=m.id, status=st))
            if st == ExecutionStatus.ERROR:
                status = ExecutionStatus.ERROR
        return ExecutionResult(
            blueprint_id=manifest.blueprint_id,
            run_id=kw.get("run_id", ""),
            status=status,
            module_results=tuple(results),
        )

    monkeypatch.setattr(proto, "call_execute", _fake_call_execute)


def _seed_resume_spill(root, manifest, run_id, edge_id="h", *, populated=True):
    from aqueduct.executor.models import manifest_hash as _mh

    d = root / _mh(manifest) / run_id / edge_id
    d.mkdir(parents=True)
    if populated:
        (d / "part-0.parquet").write_bytes(b"prior run output")
    return d


@pytest.fixture
def failed_prior_run_store(tmp_path):
    """A real local DuckDB observability store carrying ONE terminal
    ``error`` row for ``prev_run`` of blueprint ``bp``, and no later
    success. That is exactly the state in which the orphan sweep must keep
    ``prev_run``'s spill, so anything these tests observe being deleted was
    deleted by the resume-release path under test, not by the sweep."""
    import contextlib

    from aqueduct.surveyor.ddl import _DDL

    path = tmp_path / "obs.db"

    class _Store:
        def connect(self):
            import duckdb

            from aqueduct.stores.base import RelationalCursor

            @contextlib.contextmanager
            def _cm():
                conn = duckdb.connect(str(path))
                try:
                    yield RelationalCursor(conn.cursor(), paramstyle="qmark")
                finally:
                    conn.close()

            return _cm()

    from datetime import UTC, datetime, timedelta

    store = _Store()
    now = datetime.now(tz=UTC)
    with store.connect() as cur:
        cur.execute(_DDL)
        cur.execute(
            "INSERT INTO run_records (run_id, blueprint_id, status, started_at, finished_at) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                "prev_run",
                "bp",
                "error",
                (now - timedelta(minutes=5)).isoformat(),
                (now - timedelta(minutes=4)).isoformat(),
            ],
        )
    return store


def test_successful_resume_deletes_the_spill_it_consumed(
    tmp_path, monkeypatch, failed_prior_run_store
):
    from aqueduct.executor.models import ExecutionStatus
    from aqueduct.executor.orchestrator import run_polyglot

    manifest = _two_island_manifest()
    root = tmp_path / "handoff"
    prior = _seed_resume_spill(root, manifest, "prev_run")
    _install_stub_engine(monkeypatch, island_status={})

    result = run_polyglot(
        manifest,
        run_id="cur_run",
        handoff_root=str(root),
        resume_run_id="prev_run",
        observability_store=failed_prior_run_store,
    )

    assert result.status == ExecutionStatus.SUCCESS
    # Island A was SKIPPED — the resume actually happened. Without this the
    # deletion assertion below could pass for the wrong reason (the spill
    # having been swept away before the resume check ever looked at it).
    statuses = {r.module_id: r.status for r in result.module_results}
    assert statuses["a"] == ExecutionStatus.SKIPPED
    assert not prior.exists(), "the consumed resumed-FROM spill must be released"
    assert not prior.parent.exists(), "the resumed-FROM run directory goes with it"


def test_failed_resume_keeps_the_spill_it_read(tmp_path, monkeypatch, failed_prior_run_store):
    """Positive control for the test above: the SAME setup with a failing
    downstream island must LEAVE the resumed-FROM spill in place — it is
    still resumable, which is the entire reason a failure's spill is kept."""
    from aqueduct.executor.models import ExecutionStatus
    from aqueduct.executor.orchestrator import run_polyglot

    manifest = _two_island_manifest()
    root = tmp_path / "handoff"
    prior = _seed_resume_spill(root, manifest, "prev_run")
    _install_stub_engine(monkeypatch, island_status={"b": ExecutionStatus.ERROR})

    result = run_polyglot(
        manifest,
        run_id="cur_run",
        handoff_root=str(root),
        resume_run_id="prev_run",
        observability_store=failed_prior_run_store,
    )

    assert result.status == ExecutionStatus.ERROR
    assert prior.exists(), "a failed resume must keep the spill it read"


def test_success_without_an_actual_resume_keeps_the_candidate_directory(
    tmp_path, monkeypatch, failed_prior_run_store
):
    """Positive control on the TRIGGER: ``resume_run_id`` is passed, but the
    candidate directory holds no parquet, so no island is skipped and nothing
    is consumed. A successful run must then leave it alone — the release is
    keyed on a resume having actually happened, not on the flag being set."""
    from aqueduct.executor.models import ExecutionStatus
    from aqueduct.executor.orchestrator import run_polyglot

    manifest = _two_island_manifest()
    root = tmp_path / "handoff"
    prior = _seed_resume_spill(root, manifest, "prev_run", populated=False)
    _install_stub_engine(monkeypatch, island_status={})

    result = run_polyglot(
        manifest,
        run_id="cur_run",
        handoff_root=str(root),
        resume_run_id="prev_run",
        observability_store=failed_prior_run_store,
    )

    assert result.status == ExecutionStatus.SUCCESS
    statuses = {r.module_id: r.status for r in result.module_results}
    assert statuses["a"] == ExecutionStatus.SUCCESS  # re-executed, nothing resumed
    assert prior.exists()


# ── Session keep-alive (Phase 89 item 1) ──────────────────────────────────────
#
# A counting protocol double — separate from `_install_stub_engine`'s
# `_StubProtocol` above — that records session_factory/session_closer/
# session_cleanup calls per engine name, so these tests can assert ON the
# lifecycle itself (build/close/cleanup counts), not just on the final
# ExecutionResult.


class _CountingProtocol:
    def __init__(self, engine, calls):
        self.engine = engine
        self.calls = calls  # shared dict: engine -> list of event strings
        self.calls.setdefault(engine, [])

    def session_factory(self):
        def _build(spec):
            self.calls[self.engine].append("build")
            return object()

        return _build

    def session_closer(self):
        def _close(session):
            self.calls[self.engine].append("close")

        return _close

    def session_cleanup(self):
        def _clean(session, manifest):
            self.calls[self.engine].append(f"clean:{sorted(m.id for m in manifest.modules)}")

        return _clean


def _install_counting_engine(monkeypatch, *, island_status=None, raise_for=None):
    """Same shape as `_install_stub_engine`, but with `_CountingProtocol`
    (lifecycle call tracking) and an optional `raise_for` module-id set that
    makes `call_execute` raise a plain (non-`ExecuteError`) exception instead
    of returning a result — for the "exception mid-island still closes
    everything" case, which `except ExecuteError` never catches."""
    import aqueduct.executor.orchestrator as orch
    import aqueduct.executor.protocol as proto
    from aqueduct.executor.models import ExecutionResult, ExecutionStatus, ModuleResult

    island_status = island_status or {}
    raise_for = raise_for or set()
    calls: dict[str, list[str]] = {}

    monkeypatch.setattr(orch, "get_protocol", lambda engine: _CountingProtocol(engine, calls))

    def _fake_call_execute(engine, manifest, session, **kw):
        from pathlib import Path as _P

        if raise_for & {m.id for m in manifest.modules}:
            raise RuntimeError("boom — not an ExecuteError")
        for uri in (kw.get("handoff_spill_uris") or {}).values():
            _P(uri).mkdir(parents=True, exist_ok=True)
            (_P(uri) / "part-0.parquet").write_bytes(b"spilled")
        results = []
        status = ExecutionStatus.SUCCESS
        for m in manifest.modules:
            st = island_status.get(m.id, ExecutionStatus.SUCCESS)
            results.append(ModuleResult(module_id=m.id, status=st))
            if st == ExecutionStatus.ERROR:
                status = ExecutionStatus.ERROR
        return ExecutionResult(
            blueprint_id=manifest.blueprint_id,
            run_id=kw.get("run_id", ""),
            status=status,
            module_results=tuple(results),
        )

    monkeypatch.setattr(proto, "call_execute", _fake_call_execute)
    return calls


def _two_disjoint_same_engine_islands(engine="spark"):
    """Zero handoffs, same engine, no dependency relation — the ANY
    same-engine-adjacency case the design explicitly chose over "provably
    sequential pairs" (recon section C / the owner-ratified design note)."""
    a = _m("a", engine)
    b = _m("b", engine)
    return _manifest(
        [a, b],
        [],
        [
            Island(engine=engine, module_ids=frozenset({"a"})),
            Island(engine=engine, module_ids=frozenset({"b"})),
        ],
    )


def test_same_engine_adjacent_islands_reuse_the_session(tmp_path, monkeypatch):
    from aqueduct.executor.models import ExecutionStatus
    from aqueduct.executor.orchestrator import run_polyglot

    manifest = _two_disjoint_same_engine_islands("spark")
    calls = _install_counting_engine(monkeypatch)

    result = run_polyglot(manifest, run_id="r1", handoff_root=str(tmp_path / "handoff"))

    assert result.status == ExecutionStatus.SUCCESS
    assert result.session_reused == ("spark",)
    # One build (island b reused island a's session), one cleanup at the
    # reuse boundary (default: cleanup ON), one close at run end — never a
    # second build.
    assert calls["spark"] == ["build", "clean:['a']", "close"]


def test_share_island_state_true_skips_the_boundary_cleanup(tmp_path, monkeypatch):
    from aqueduct.executor.models import ExecutionStatus
    from aqueduct.executor.orchestrator import run_polyglot

    manifest = _two_disjoint_same_engine_islands("spark")
    calls = _install_counting_engine(monkeypatch)

    result = run_polyglot(
        manifest,
        run_id="r1",
        handoff_root=str(tmp_path / "handoff"),
        share_island_state=True,
    )

    assert result.status == ExecutionStatus.SUCCESS
    assert result.session_reused == ("spark",)
    assert calls["spark"] == ["build", "close"]  # no "clean:" entry at all


def test_session_keep_alive_false_restores_close_every_island(tmp_path, monkeypatch):
    from aqueduct.executor.models import ExecutionStatus
    from aqueduct.executor.orchestrator import run_polyglot

    manifest = _two_disjoint_same_engine_islands("spark")
    calls = _install_counting_engine(monkeypatch)

    result = run_polyglot(
        manifest,
        run_id="r1",
        handoff_root=str(tmp_path / "handoff"),
        session_keep_alive=False,
    )

    assert result.status == ExecutionStatus.SUCCESS
    assert result.session_reused == ()  # nothing was ever reused
    assert calls["spark"] == ["build", "close", "build", "close"]


def test_engine_switch_closes_before_building_the_next(tmp_path, monkeypatch):
    """spark -> duckdb -> spark (a real cross-engine boundary on both sides):
    every adjacent pair in execution order differs, so keep-alive never
    finds a reuse opportunity — each island still gets its own fresh
    session, closed before the next is built, exactly like today."""
    from aqueduct.executor.models import ExecutionStatus
    from aqueduct.executor.orchestrator import run_polyglot

    a = _m("a", "spark")
    b = _m("b", "duckdb")
    c = _m("c", "spark")
    h1 = _handoff_module("h1", "a", "b", from_engine="spark", to_engine="duckdb")
    h2 = _handoff_module("h2", "b", "c", from_engine="duckdb", to_engine="spark")
    manifest = _manifest(
        [a, h1, b, h2, c],
        [
            Edge(from_id="a", to_id="h1"),
            Edge(from_id="h1", to_id="b"),
            Edge(from_id="b", to_id="h2"),
            Edge(from_id="h2", to_id="c"),
        ],
        [
            Island(engine="spark", module_ids=frozenset({"a"})),
            Island(engine="duckdb", module_ids=frozenset({"b"})),
            Island(engine="spark", module_ids=frozenset({"c"})),
        ],
    )
    calls = _install_counting_engine(monkeypatch)

    result = run_polyglot(manifest, run_id="r1", handoff_root=str(tmp_path / "handoff"))

    assert result.status == ExecutionStatus.SUCCESS
    assert result.session_reused == ()
    assert calls["spark"] == ["build", "close", "build", "close"]
    assert calls["duckdb"] == ["build", "close"]


def test_exception_mid_island_still_closes_everything(tmp_path, monkeypatch):
    """A non-`ExecuteError` exception from the SECOND island (after the first
    island's session is already live) must still close it — the function-
    level `finally` is what makes this hold, not the per-island `finally`
    the v1 code used to have."""
    from aqueduct.executor.orchestrator import run_polyglot

    manifest = _two_disjoint_same_engine_islands("spark")
    calls = _install_counting_engine(monkeypatch, raise_for={"b"})

    with pytest.raises(RuntimeError, match="boom"):
        run_polyglot(manifest, run_id="r1", handoff_root=str(tmp_path / "handoff"))

    # island "a" built + reused by "b" (no second build); closed exactly
    # once despite the exception.
    assert calls["spark"].count("build") == 1
    assert calls["spark"].count("close") == 1


def test_resume_skipped_island_builds_no_session(tmp_path, monkeypatch, failed_prior_run_store):
    """An island entirely skipped by resume must never force a session
    build — `_install_counting_engine`'s per-engine call log for the
    downstream engine must be empty until a NON-skipped island actually
    needs it."""
    from aqueduct.executor.models import ExecutionStatus
    from aqueduct.executor.orchestrator import run_polyglot

    manifest = _two_island_manifest()  # a(spark) --h--> b(duckdb)
    root = tmp_path / "handoff"
    _seed_resume_spill(root, manifest, "prev_run")
    calls = _install_counting_engine(monkeypatch)

    result = run_polyglot(
        manifest,
        run_id="cur_run",
        handoff_root=str(root),
        resume_run_id="prev_run",
        observability_store=failed_prior_run_store,
    )

    assert result.status == ExecutionStatus.SUCCESS
    statuses = {r.module_id: r.status for r in result.module_results}
    assert statuses["a"] == ExecutionStatus.SKIPPED
    # island "a" (spark) was resumed/skipped — no spark session ever built.
    assert calls.get("spark", []) == []
    assert calls["duckdb"] == ["build", "close"]


# ── Engine-side cleanup hooks (Phase 89 item 1) ───────────────────────────────
#
# Exercises `ExecutorProtocol.cleanup_reused_session` for both shipped
# engines directly (not through `run_polyglot`) — a fake session records
# what it was asked to drop, so these assert on the ACTUAL SQL issued, not
# just on whether the orchestrator called through the seam.


def _egress_module(id_, *, register_as_table=None, table=None):
    cfg: dict = {}
    if register_as_table is not None:
        cfg["register_as_table"] = register_as_table
    if table is not None:
        cfg["table"] = table
    return _m(id_, "spark", type_=ModuleType.Egress, config=cfg)


def test_spark_cleanup_reused_session_drops_register_as_table():
    from aqueduct.executor.spark.engine import _cleanup_reused_session

    class _FakeSparkSession:
        def __init__(self):
            self.sql_calls: list[str] = []

        def sql(self, stmt):
            self.sql_calls.append(stmt)

    session = _FakeSparkSession()
    manifest = _manifest(
        [_egress_module("out", register_as_table="my_table")],
        [],
        [],
    )

    _cleanup_reused_session(session, manifest)

    assert session.sql_calls == ["DROP TABLE IF EXISTS my_table"]


def test_spark_cleanup_reused_session_skips_table_write_egress():
    """`table:` writes ignore `register_as_table` at write time (see
    egress.py) — cleanup must not try to drop a table the island never
    registered as a SEPARATE object."""
    from aqueduct.executor.spark.engine import _cleanup_reused_session

    class _FakeSparkSession:
        def sql(self, stmt):
            raise AssertionError(f"must not be called: {stmt}")

    manifest = _manifest(
        [_egress_module("out", register_as_table="ignored", table="real_table")],
        [],
        [],
    )

    _cleanup_reused_session(_FakeSparkSession(), manifest)  # no assertion raised = pass


def test_spark_cleanup_reused_session_best_effort_on_failure():
    """A drop failure must never raise past the cleanup hook — best-effort,
    same posture as `_register_external_table` itself."""
    from aqueduct.executor.spark.engine import _cleanup_reused_session

    class _FailingSparkSession:
        def sql(self, stmt):
            raise RuntimeError("catalog unavailable")

    manifest = _manifest([_egress_module("out", register_as_table="t")], [], [])

    _cleanup_reused_session(_FailingSparkSession(), manifest)  # must not raise


def test_duckdb_cleanup_reused_session_drops_register_as_table_view():
    from aqueduct.executor.duckdb_.engine import _cleanup_reused_session

    class _FakeDuckDBConnection:
        def __init__(self):
            self.executed: list[str] = []

        def execute(self, stmt):
            self.executed.append(stmt)

    con = _FakeDuckDBConnection()
    manifest = _manifest(
        [_m("out", "duckdb", type_=ModuleType.Egress, config={"register_as_table": "v"})],
        [],
        [],
    )

    _cleanup_reused_session(con, manifest)

    assert con.executed == ["DROP VIEW IF EXISTS v"]
