"""Unit tests for the pure, engine-agnostic helpers in
``aqueduct.executor.orchestrator`` (Phase 81 step 3) — island dependency
ordering, sub-manifest slicing, and spill-URI resolution. No real Spark/DuckDB
session is built here; ``tests/test_executor/test_orchestrator_e2e.py`` covers
the full ``run_polyglot()`` path against real engines.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit

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
        spark_config={},
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
        [Island(engine="spark", module_ids=frozenset({"a"})), Island(engine="duckdb", module_ids=frozenset({"b"}))],
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
        [Island(engine="spark", module_ids=frozenset({"a"})), Island(engine="duckdb", module_ids=frozenset({"b"}))],
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
        [Island(engine="spark", module_ids=frozenset({"a"})), Island(engine="duckdb", module_ids=frozenset({"b"}))],
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
        [Island(engine="spark", module_ids=frozenset({"a"})), Island(engine="duckdb", module_ids=frozenset({"b"}))],
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
        [Island(engine="spark", module_ids=frozenset({"a"})), Island(engine="duckdb", module_ids=frozenset({"b"}))],
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
        [Island(engine="spark", module_ids=frozenset({"a"})), Island(engine="duckdb", module_ids=frozenset({"b"}))],
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
        [Island(engine="spark", module_ids=frozenset({"a"})), Island(engine="duckdb", module_ids=frozenset({"b"}))],
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
        [Island(engine="spark", module_ids=frozenset({"a"})), Island(engine="duckdb", module_ids=frozenset({"b"}))],
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
        [Island(engine="spark", module_ids=frozenset({"a"})), Island(engine="duckdb", module_ids=frozenset({"b"}))],
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
        [Island(engine="spark", module_ids=frozenset({"a"})), Island(engine="duckdb", module_ids=frozenset({"b"}))],
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
        [Island(engine="spark", module_ids=frozenset({"a"})), Island(engine="duckdb", module_ids=frozenset({"b"}))],
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
