"""Phase 81 Step 2 — synthetic cross-engine handoff module insertion.

Covers: insertion at a boundary edge (id, type, config contract), edge
rewiring (`A -> B` becomes `A -> handoff -> B`: the write-side edge keeps
the original port, the read-side edge is always normalized to `main` — see
`test_non_main_port_boundary_edge_normalizes_read_side_to_main`), zero
insertions for disjoint different-engine components and for a
single-engine Blueprint (byte-identical to Step 1), a user-declared
`type: Handoff` module rejected at parse time, lineage passthrough across
a handoff, the compile-time `cross_engine_handoff_io` warning (suppressible,
names the handoff point), and the `handoff:` config block's validation and
defaults.

Pure compiler-layer tests — no pyspark, no live Spark/DuckDB session.
"""

from __future__ import annotations

import warnings as _pywarnings
from pathlib import Path

import pytest

# Register both engines' capability tables.
import aqueduct.executor.duckdb_.capabilities  # noqa: F401
import aqueduct.executor.spark.capabilities  # noqa: F401
from aqueduct.compiler.compiler import compile as ccompile
from aqueduct.compiler.handoff import (
    RULE_ID_CROSS_ENGINE_HANDOFF,
    HandoffInsertion,
    insert_handoff_modules,
)
from aqueduct.compiler.islands import BoundaryEdge, derive_islands, find_boundary_edges
from aqueduct.compiler.lineage import compute_lineage_rows
from aqueduct.config import AqueductConfig, HandoffConfig
from aqueduct.errors import ParseError
from aqueduct.parser.models import Edge, Module, ModuleType
from aqueduct.parser.parser import parse_dict

pytestmark = pytest.mark.unit


def _m(id_, type_=ModuleType.Channel, engine=None, config=None):
    return Module(id=id_, type=type_, label=id_, config=config or {}, engine=engine)


def _bp(modules, edges=None, udf_registry=None):
    d = {
        "aqueduct": "1.0",
        "id": "bp",
        "name": "t",
        "modules": modules,
        **({"edges": edges} if edges is not None else {}),
        **({"udf_registry": udf_registry} if udf_registry is not None else {}),
    }
    return parse_dict(d, base_dir=Path("/tmp"))


# ── insert_handoff_modules: unit level ──────────────────────────────────────


def test_handoff_inserted_at_boundary_edge():
    modules = [_m("a"), _m("b")]
    edges = [Edge(from_id="a", to_id="b")]
    boundaries = [BoundaryEdge(edge=edges[0], from_engine="spark", to_engine="duckdb")]

    new_modules, new_edges, insertions = insert_handoff_modules(modules, edges, boundaries)

    assert len(insertions) == 1
    handoff = insertions[0].module
    assert handoff.type == ModuleType.Handoff
    assert handoff.synthetic is True
    assert handoff.engine is None
    assert handoff.id not in {"a", "b"}
    assert handoff in new_modules
    assert handoff.config == {
        "edge_id": handoff.id,
        "from_module": "a",
        "to_module": "b",
        "from_engine": "spark",
        "to_engine": "duckdb",
        "port": "main",
    }


def test_handoff_id_is_collision_proof_against_user_ids():
    """User module ids may never contain `__` (parser.schema rejects it), so
    a `__`-joined handoff id can never collide with an authored one."""
    modules = [_m("a"), _m("b")]
    edges = [Edge(from_id="a", to_id="b")]
    boundaries = [BoundaryEdge(edge=edges[0], from_engine="spark", to_engine="duckdb")]
    _, _, insertions = insert_handoff_modules(modules, edges, boundaries)
    assert "__" in insertions[0].module.id


def test_edge_rewiring_preserves_port_and_leaves_other_edges_untouched():
    modules = [_m("a"), _m("b"), _m("c"), _m("d")]
    boundary_edge = Edge(from_id="a", to_id="b", port="main")
    untouched_edge = Edge(from_id="c", to_id="d", port="main")
    edges = [boundary_edge, untouched_edge]
    boundaries = [BoundaryEdge(edge=boundary_edge, from_engine="spark", to_engine="duckdb")]

    new_modules, new_edges, insertions = insert_handoff_modules(modules, edges, boundaries)

    handoff_id = insertions[0].module.id
    assert untouched_edge in new_edges
    assert boundary_edge not in new_edges
    assert Edge(from_id="a", to_id=handoff_id, port="main") in new_edges
    assert Edge(from_id=handoff_id, to_id="b", port="main") in new_edges
    assert len(new_edges) == 3  # untouched + 2 rewired


def test_non_main_port_boundary_edge_normalizes_read_side_to_main():
    """A Junction branch edge that crosses the boundary DIRECTLY (port is
    the branch id, e.g. `port="big"`) must keep that port on the WRITE-side
    edge (`a -> handoff`) — that's what lets the executor's write-side
    lookup find `frame_store[_frame_key("a", "big")]` — but the READ-side
    edge (`handoff -> b`) must be `port="main"`, never the original branch
    port: both engines' Handoff runtime write the read side's value to
    `frame_store[handoff_id]` (a bare key, never port-suffixed), so `b`'s
    own incoming-edge lookup must resolve to that same bare key
    (`_frame_key(handoff_id, "main") == handoff_id`). Preserving the branch
    port on this edge instead made `b` look for a key that was never
    written — the regression this test guards."""
    modules = [_m("a", type_=ModuleType.Junction), _m("b")]
    boundary_edge = Edge(from_id="a", to_id="b", port="big")
    edges = [boundary_edge]
    boundaries = [BoundaryEdge(edge=boundary_edge, from_engine="spark", to_engine="duckdb")]

    new_modules, new_edges, insertions = insert_handoff_modules(modules, edges, boundaries)

    handoff_id = insertions[0].module.id
    assert Edge(from_id="a", to_id=handoff_id, port="big") in new_edges
    assert Edge(from_id=handoff_id, to_id="b", port="main") in new_edges
    # The module's own config still records the ORIGINAL port (informational).
    assert insertions[0].module.config["port"] == "big"


def test_zero_boundaries_returns_inputs_unchanged():
    """Disjoint different-engine components (or a single-engine Blueprint)
    produce zero boundary edges — `insert_handoff_modules` must return the
    SAME objects, not just an equal copy: byte-identical to Step 1."""
    modules = [_m("a"), _m("b")]
    edges = [Edge(from_id="a", to_id="b")]

    new_modules, new_edges, insertions = insert_handoff_modules(modules, edges, [])

    assert new_modules is modules
    assert new_edges is edges
    assert insertions == []


def test_disjoint_different_engine_components_get_zero_handoffs():
    modules = [
        _m("a1", engine="spark"),
        _m("a2", engine="spark"),
        _m("b1", engine="duckdb"),
        _m("b2", engine="duckdb"),
    ]
    edges = [
        Edge(from_id="a1", to_id="a2"),
        Edge(from_id="b1", to_id="b2"),
    ]
    resolved = {"a1": "spark", "a2": "spark", "b1": "duckdb", "b2": "duckdb"}
    boundaries = find_boundary_edges(modules, edges, resolved)
    assert boundaries == []

    new_modules, new_edges, insertions = insert_handoff_modules(modules, edges, boundaries)
    assert insertions == []
    assert new_modules is modules
    assert new_edges is edges


# ── Lineage passthrough ──────────────────────────────────────────────────────


def test_lineage_passes_through_a_handoff_unchanged():
    modules = (
        _m("a", config={"op": "sql", "query": "SELECT 1"}),
        Module(
            id="h",
            type=ModuleType.Handoff,
            label="h",
            config={
                "edge_id": "h",
                "from_module": "a",
                "to_module": "b",
                "from_engine": "spark",
                "to_engine": "duckdb",
                "port": "main",
            },
            synthetic=True,
        ),
        _m("b", config={"op": "sql", "query": "SELECT * FROM h"}),
    )
    edges = (Edge(from_id="a", to_id="h"), Edge(from_id="h", to_id="b"))

    rows = compute_lineage_rows(modules, edges)
    handoff_rows = [r for r in rows if r["channel_id"] == "h"]
    assert handoff_rows == [
        {"channel_id": "h", "output_column": "*", "source_table": "a", "source_column": "*"}
    ]


# ── User-declared Handoff rejected ──────────────────────────────────────────


def test_user_declared_handoff_module_is_rejected():
    with pytest.raises(ParseError, match="reserved"):
        _bp(
            [
                {"id": "sneaky", "label": "sneaky", "type": "Handoff", "config": {}},
            ]
        )


# ── Full compile(): the compile-time warning ────────────────────────────────


def test_compile_emits_cross_engine_handoff_io_warning_naming_the_point():
    bp = _bp(
        [
            {
                "id": "extract",
                "label": "extract",
                "type": "Channel",
                "engine": "spark",
                "config": {"op": "sql", "query": "SELECT 1 AS x"},
            },
            {
                "id": "agg",
                "label": "agg",
                "type": "Channel",
                "engine": "duckdb",
                "config": {"op": "sql", "query": "SELECT * FROM extract"},
            },
        ],
        edges=[{"from": "extract", "to": "agg"}],
    )
    with _pywarnings.catch_warnings(record=True) as caught:
        _pywarnings.simplefilter("always")
        manifest = ccompile(bp, engine="spark")
    messages = [str(w.message) for w in caught]
    assert any(f"[aqueduct:{RULE_ID_CROSS_ENGINE_HANDOFF}]" in m for m in messages)
    assert any("extract" in m and "agg" in m for m in messages)
    handoff_ids = [m.id for m in manifest.modules if m.type == ModuleType.Handoff]
    assert len(handoff_ids) == 1

    handoff_dict = next(m for m in manifest.to_dict()["modules"] if m["type"] == ModuleType.Handoff)
    assert handoff_dict["synthetic"] is True
    assert handoff_dict["config"]["from_module"] == "extract"
    assert handoff_dict["config"]["to_module"] == "agg"


def test_compile_handoff_warning_suppressed():
    bp = _bp(
        [
            {
                "id": "extract",
                "label": "extract",
                "type": "Channel",
                "engine": "spark",
                "config": {"op": "sql", "query": "SELECT 1 AS x"},
            },
            {
                "id": "agg",
                "label": "agg",
                "type": "Channel",
                "engine": "duckdb",
                "config": {"op": "sql", "query": "SELECT * FROM extract"},
            },
        ],
        edges=[{"from": "extract", "to": "agg"}],
    )
    with _pywarnings.catch_warnings(record=True) as caught:
        _pywarnings.simplefilter("always")
        ccompile(bp, engine="spark", warnings_suppress={RULE_ID_CROSS_ENGINE_HANDOFF})
    messages = [str(w.message) for w in caught]
    assert not any(RULE_ID_CROSS_ENGINE_HANDOFF in m for m in messages)


def test_compile_single_engine_blueprint_no_handoff_no_warning():
    bp = _bp(
        [
            {
                "id": "in",
                "label": "in",
                "type": "Ingress",
                "config": {"format": "csv", "path": "/tmp/x.csv"},
            },
            {
                "id": "out",
                "label": "out",
                "type": "Egress",
                "config": {"format": "parquet", "path": "/tmp/y", "mode": "overwrite"},
            },
        ],
        edges=[{"from": "in", "to": "out"}],
    )
    with _pywarnings.catch_warnings(record=True) as caught:
        _pywarnings.simplefilter("always")
        manifest = ccompile(bp, engine="spark")
    messages = [str(w.message) for w in caught]
    assert not any(RULE_ID_CROSS_ENGINE_HANDOFF in m for m in messages)
    assert not any(m.type == ModuleType.Handoff for m in manifest.modules)
    assert {m.id for m in manifest.modules} == {"in", "out"}


# ── `handoff:` config block ─────────────────────────────────────────────────


def test_handoff_config_defaults():
    cfg = AqueductConfig()
    assert cfg.handoff.root == ".aqueduct/handoff"
    assert cfg.handoff.keep_on_failure is True


def test_handoff_config_accepts_remote_uri_root():
    """Unlike `checkpoint_root` (local-only), `handoff.root` must accept a
    remote URI scheme — both engines need to reach it."""
    cfg = AqueductConfig(handoff=HandoffConfig(root="s3://my-bucket/handoff"))
    assert cfg.handoff.root == "s3://my-bucket/handoff"


def test_handoff_config_rejects_unknown_key():
    with pytest.raises(Exception):
        HandoffConfig(root=".aqueduct/handoff", bogus_key=True)


def test_handoff_config_overrides_from_yaml():
    cfg = AqueductConfig.model_validate(
        {
            "handoff": {"root": "/mnt/shared/handoff", "keep_on_failure": False},
        }
    )
    assert cfg.handoff.root == "/mnt/shared/handoff"
    assert cfg.handoff.keep_on_failure is False
