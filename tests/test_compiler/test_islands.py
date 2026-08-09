"""Phase 81 Step 1 — engine islands.

Covers: the per-module `engine:` grammar field, the four inheritance-
precedence rules (explicit pin / single-parent inheritance / ambiguous-
multi-parent CompileError / no-parents default), island + boundary-edge
derivation, the Probe/Assert colocation gate, the v1 cross-island spillway
gate, and the per-island compile-time capability gate (including the
"unregistered engine" failure and the "disjoint components need zero
handoffs" free lunch).

Pure compiler-layer tests — no pyspark, no live Spark/DuckDB session. Two
styles: direct unit tests against `aqueduct.compiler.islands` functions
(fastest, exercises exact edge cases) and full parse+compile integration
tests (proves the wiring inside `compiler.compile()` is correct end to end).
"""

from __future__ import annotations

from pathlib import Path

import pytest

# Register both engines' capability tables — islands.py / capability_check.py
# resolve engines through the registry, same import-side-effect pattern as
# tests/test_capabilities/test_gate_module_and_feature.py.
import aqueduct.executor.duckdb_.capabilities  # noqa: F401
import aqueduct.executor.spark.capabilities  # noqa: F401
from aqueduct.compiler.compiler import compile as ccompile
from aqueduct.compiler.islands import (
    derive_islands,
    find_boundary_edges,
    resolve_module_engines,
    validate_colocation,
    validate_spillway_islands,
)
from aqueduct.errors import CompileError, UnknownEngineError
from aqueduct.parser.models import Edge, Module, ModuleType
from aqueduct.parser.parser import parse_dict

pytestmark = pytest.mark.unit


def _m(id_, type_=ModuleType.Channel, engine=None, attach_to=None, depends_on=()):
    return Module(
        id=id_, type=type_, label=id_, config={}, engine=engine,
        attach_to=attach_to, depends_on=tuple(depends_on),
    )


def _bp(modules, edges=None):
    return parse_dict(
        {
            "aqueduct": "1.0", "id": "bp", "name": "t",
            "modules": modules,
            **({"edges": edges} if edges is not None else {}),
        },
        base_dir=Path("/tmp"),
    )


# ── Grammar: the per-module `engine:` field ─────────────────────────────────


def test_module_schema_accepts_explicit_engine_field():
    bp = _bp([
        {"id": "in", "label": "in", "type": "Ingress", "engine": "duckdb",
         "config": {"format": "csv", "path": "/tmp/x.csv"}},
    ])
    assert bp.modules[0].engine == "duckdb"


def test_module_schema_engine_defaults_to_none():
    bp = _bp([
        {"id": "in", "label": "in", "type": "Ingress",
         "config": {"format": "csv", "path": "/tmp/x.csv"}},
    ])
    assert bp.modules[0].engine is None


def test_module_engine_field_and_blueprint_engine_block_coexist():
    """The per-module `engine:` FIELD (scalar engine name) and the
    blueprint-level `engine:` BLOCK (per-engine settings, Q4/2.0) are
    deliberately the same word at two levels — both must parse unambiguously
    in the same Blueprint."""
    bp = _bp([
        {"id": "in", "label": "in", "type": "Ingress", "engine": "spark",
         "config": {"format": "csv", "path": "/tmp/x.csv"}},
    ])
    bp = parse_dict(
        {
            "aqueduct": "1.0", "id": "bp", "name": "t",
            "modules": [
                {"id": "in", "label": "in", "type": "Ingress", "engine": "spark",
                 "config": {"format": "csv", "path": "/tmp/x.csv"}},
            ],
            "engine": {"spark": {"conf": {"spark.sql.shuffle.partitions": 10}}},
        },
        base_dir=Path("/tmp"),
    )
    assert bp.modules[0].engine == "spark"
    assert bp.engine_config["spark"] == {"spark.sql.shuffle.partitions": 10}


def test_blueprint_engine_config_has_an_entry_for_every_registered_engine_block():
    """`Blueprint.engine_config`/`Manifest.engine_config` is derived
    GENERICALLY off `EngineBlockSchema`'s own fields (Phase 82 remediation:
    the `spark_config` -> `engine_config` rename) — every engine named in
    the `engine:` block gets a dict entry, not just `spark`, even when that
    engine (DuckDB today) declares no Blueprint-level fields yet."""
    bp = parse_dict(
        {
            "aqueduct": "1.0", "id": "bp", "name": "t",
            "modules": [
                {"id": "in", "label": "in", "type": "Ingress",
                 "config": {"format": "csv", "path": "/tmp/x.csv"}},
            ],
            "engine": {"spark": {"conf": {"spark.sql.shuffle.partitions": 10}}},
        },
        base_dir=Path("/tmp"),
    )
    assert set(bp.engine_config) == {"spark", "duckdb"}
    assert bp.engine_config["spark"] == {"spark.sql.shuffle.partitions": 10}
    # This Blueprint's `engine:` block never mentions `duckdb:` at all —
    # the generic derivation still produces an (empty) entry for it (every
    # registered engine gets a key), never omits it entirely, even though
    # DuckDBEngineBlockSchema (2.54) now declares real fields (memory_limit/
    # threads) a Blueprint COULD set.
    assert bp.engine_config["duckdb"] == {}

    manifest = ccompile(bp, engine="spark")
    assert manifest.engine_config == bp.engine_config


# ── Rule 1: explicit pin wins ────────────────────────────────────────────────


def test_explicit_pin_wins_over_inherited_parent():
    modules = [_m("a", engine="duckdb"), _m("b", engine="spark")]
    edges = [Edge(from_id="a", to_id="b")]
    resolved = resolve_module_engines(modules, edges, default_engine="spark")
    assert resolved == {"a": "duckdb", "b": "spark"}


def test_explicit_pin_beats_default_engine_with_no_parents():
    """Precedence against config: `deployment.engine` only moves the
    DEFAULT (rule 4) — an explicit per-module pin (rule 1) always wins."""
    modules = [_m("a", engine="spark")]
    resolved = resolve_module_engines(modules, [], default_engine="duckdb")
    assert resolved["a"] == "spark"


# ── Rule 2: unset -> inherit the single upstream parent's engine ───────────


def test_single_parent_inheritance():
    modules = [_m("a", engine="duckdb"), _m("b")]
    edges = [Edge(from_id="a", to_id="b")]
    resolved = resolve_module_engines(modules, edges, default_engine="spark")
    assert resolved["b"] == "duckdb"


def test_inheritance_chains_transitively():
    modules = [_m("a", engine="duckdb"), _m("b"), _m("c")]
    edges = [Edge(from_id="a", to_id="b"), Edge(from_id="b", to_id="c")]
    resolved = resolve_module_engines(modules, edges, default_engine="spark")
    assert resolved["b"] == "duckdb"
    assert resolved["c"] == "duckdb"


def test_multiple_same_resolved_engine_parents_is_not_ambiguous():
    """Rule 3 only fires on DIFFERING engines — two parents that happen to
    resolve to the same engine are not an ambiguity."""
    modules = [_m("a", engine="spark"), _m("b", engine="spark"), _m("c", type_=ModuleType.Funnel)]
    edges = [Edge(from_id="a", to_id="c"), Edge(from_id="b", to_id="c")]
    resolved = resolve_module_engines(modules, edges, default_engine="duckdb")
    assert resolved["c"] == "spark"


# ── Rule 3: unset + multiple DIFFERING-engine parents -> CompileError ───────


def test_multiple_differing_parents_without_pin_raises():
    modules = [_m("a", engine="spark"), _m("b", engine="duckdb"), _m("c", type_=ModuleType.Funnel)]
    edges = [Edge(from_id="a", to_id="c"), Edge(from_id="b", to_id="c")]
    with pytest.raises(CompileError, match="differing engines"):
        resolve_module_engines(modules, edges, default_engine="spark")


def test_compile_raises_on_ambiguous_engine_inheritance():
    bp = _bp(
        [
            {"id": "a", "label": "a", "type": "Ingress", "engine": "spark",
             "config": {"format": "csv", "path": "/tmp/a.csv"}},
            {"id": "b", "label": "b", "type": "Ingress", "engine": "duckdb",
             "config": {"format": "csv", "path": "/tmp/b.csv"}},
            {"id": "c", "label": "c", "type": "Funnel", "config": {"mode": "union_all"}},
        ],
        edges=[{"from": "a", "to": "c"}, {"from": "b", "to": "c"}],
    )
    with pytest.raises(CompileError, match="differing engines"):
        ccompile(bp, engine="spark")


# ── Rule 4: unset + no parents -> default_engine (deployment.engine) ───────


def test_no_parents_falls_back_to_default_engine():
    modules = [_m("a")]
    resolved = resolve_module_engines(modules, [], default_engine="duckdb")
    assert resolved["a"] == "duckdb"


def test_compile_default_engine_param_sets_unpinned_modules():
    bp = _bp(
        [
            {"id": "in", "label": "in", "type": "Ingress",
             "config": {"format": "csv", "path": "/tmp/x.csv"}},
            {"id": "out", "label": "out", "type": "Egress",
             "config": {"format": "parquet", "path": "/tmp/y", "mode": "overwrite"}},
        ],
        edges=[{"from": "in", "to": "out"}],
    )
    manifest = ccompile(bp, engine="duckdb")
    assert {m.engine for m in manifest.modules} == {"duckdb"}


def test_compile_explicit_module_pin_beats_default_engine_param():
    bp = _bp([
        {"id": "in", "label": "in", "type": "Ingress", "engine": "spark",
         "config": {"format": "csv", "path": "/tmp/x.csv"}},
    ])
    manifest = ccompile(bp, engine="duckdb")
    assert manifest.modules[0].engine == "spark"


def test_manifest_to_dict_serializes_resolved_engine():
    bp = _bp([
        {"id": "in", "label": "in", "type": "Ingress",
         "config": {"format": "csv", "path": "/tmp/x.csv"}},
    ])
    manifest = ccompile(bp, engine="duckdb")
    assert manifest.to_dict()["modules"][0]["engine"] == "duckdb"


# ── Islands & boundary edges ─────────────────────────────────────────────────


def test_boundary_edge_between_differing_engine_modules():
    modules = [_m("a", engine="spark"), _m("b", engine="duckdb")]
    edges = [Edge(from_id="a", to_id="b")]
    resolved = resolve_module_engines(modules, edges, default_engine="spark")

    boundaries = find_boundary_edges(modules, edges, resolved)
    assert len(boundaries) == 1
    assert boundaries[0].from_engine == "spark"
    assert boundaries[0].to_engine == "duckdb"

    islands = derive_islands(modules, edges, resolved)
    assert len(islands) == 2
    assert {isl.engine for isl in islands} == {"spark", "duckdb"}


def test_disjoint_components_different_engines_zero_boundaries():
    """Two disconnected flows on different engines are the free-lunch
    selling point: this falls out of the existing (engine-agnostic here)
    connected-component derivation with ZERO handoff nodes needed."""
    modules = [
        _m("a1", engine="spark"), _m("a2"),
        _m("b1", engine="duckdb"), _m("b2"),
    ]
    edges = [
        Edge(from_id="a1", to_id="a2"),
        Edge(from_id="b1", to_id="b2"),
    ]
    resolved = resolve_module_engines(modules, edges, default_engine="spark")
    assert resolved["a2"] == "spark"
    assert resolved["b2"] == "duckdb"

    assert find_boundary_edges(modules, edges, resolved) == []

    islands = derive_islands(modules, edges, resolved)
    assert len(islands) == 2
    assert {isl.engine for isl in islands} == {"spark", "duckdb"}
    assert {frozenset(isl.module_ids) for isl in islands} == {
        frozenset({"a1", "a2"}), frozenset({"b1", "b2"}),
    }


def test_compile_two_registered_engines_boundary_gets_a_handoff_module():
    """Step 2: a legitimate two-engine boundary compiles cleanly AND gets a
    real synthetic Handoff module spliced in (`aqueduct/compiler/handoff.py`)
    — `extract -> agg` becomes `extract -> handoff -> agg`."""
    bp = _bp(
        [
            {"id": "extract", "label": "extract", "type": "Channel", "engine": "spark",
             "config": {"op": "sql", "query": "SELECT 1 AS x"}},
            {"id": "agg", "label": "agg", "type": "Channel", "engine": "duckdb",
             "config": {"op": "sql", "query": "SELECT * FROM extract"}},
        ],
        edges=[{"from": "extract", "to": "agg"}],
    )
    manifest = ccompile(bp, engine="spark")
    engines = {m.id: m.engine for m in manifest.modules}
    assert engines == {"extract": "spark", "agg": "duckdb", "extract__handoff__agg": None}


# ── Probe / Assert colocation ───────────────────────────────────────────────


def test_probe_colocates_with_target_by_default():
    modules = [_m("src", engine="duckdb"), _m("p", type_=ModuleType.Probe, attach_to="src")]
    resolved = resolve_module_engines(modules, [], default_engine="spark")
    assert resolved["p"] == "duckdb"
    assert validate_colocation(modules, [], resolved) is None  # must not raise


def test_probe_explicit_pin_mismatch_raises():
    modules = [
        _m("src", engine="duckdb"),
        _m("p", type_=ModuleType.Probe, engine="spark", attach_to="src"),
    ]
    resolved = resolve_module_engines(modules, [], default_engine="spark")
    with pytest.raises(CompileError, match="must colocate"):
        validate_colocation(modules, [], resolved)


def test_assert_mismatch_with_upstream_raises():
    modules = [
        _m("src", engine="spark"),
        _m("chk", type_=ModuleType.Assert, engine="duckdb"),
    ]
    edges = [Edge(from_id="src", to_id="chk")]
    resolved = resolve_module_engines(modules, edges, default_engine="spark")
    with pytest.raises(CompileError, match="must colocate"):
        validate_colocation(modules, edges, resolved)


def test_compile_probe_colocation_mismatch_raises():
    bp = _bp(
        [
            {"id": "in", "label": "in", "type": "Ingress", "engine": "duckdb",
             "config": {"format": "csv", "path": "/tmp/x.csv"}},
            {"id": "out", "label": "out", "type": "Egress", "engine": "duckdb",
             "config": {"format": "parquet", "path": "/tmp/y", "mode": "overwrite"}},
            {"id": "p", "label": "p", "type": "Probe", "engine": "spark", "attach_to": "in",
             "config": {"signals": [{"type": "row_count_estimate"}]}},
        ],
        edges=[{"from": "in", "to": "out"}],
    )
    with pytest.raises(CompileError, match="must colocate"):
        ccompile(bp, engine="spark")


def test_compile_assert_colocation_mismatch_raises():
    bp = _bp(
        [
            {"id": "in", "label": "in", "type": "Ingress", "engine": "duckdb",
             "config": {"format": "csv", "path": "/tmp/x.csv"}},
            {"id": "chk", "label": "chk", "type": "Assert", "engine": "spark",
             "config": {"rules": [{"type": "min_rows", "min": 1}]}},
            {"id": "out", "label": "out", "type": "Egress", "engine": "duckdb",
             "config": {"format": "parquet", "path": "/tmp/y", "mode": "overwrite"}},
        ],
        edges=[{"from": "in", "to": "chk"}, {"from": "chk", "to": "out"}],
    )
    with pytest.raises(CompileError, match="must colocate"):
        ccompile(bp, engine="spark")


# ── Cross-island spillway (v1: not supported) ───────────────────────────────


def test_spillway_across_islands_raises():
    modules = [_m("src", engine="spark"), _m("quarantine", engine="duckdb")]
    edges = [Edge(from_id="src", to_id="quarantine", port="spillway")]
    resolved = resolve_module_engines(modules, edges, default_engine="spark")
    with pytest.raises(CompileError, match="cross-island spillway"):
        validate_spillway_islands(edges, resolved)


def test_spillway_within_one_island_is_fine():
    modules = [_m("src", engine="spark"), _m("quarantine", engine="spark")]
    edges = [Edge(from_id="src", to_id="quarantine", port="spillway")]
    resolved = resolve_module_engines(modules, edges, default_engine="spark")
    assert validate_spillway_islands(edges, resolved) is None  # must not raise


def test_compile_spillway_crossing_island_raises():
    bp = _bp(
        [
            {"id": "in", "label": "in", "type": "Ingress", "engine": "spark",
             "config": {"format": "csv", "path": "/tmp/x.csv"}},
            {"id": "ch", "label": "ch", "type": "Channel", "engine": "spark",
             "config": {"op": "filter", "condition": "a > 1"}, "spillway": "q"},
            {"id": "out", "label": "out", "type": "Egress", "engine": "spark",
             "config": {"format": "parquet", "path": "/tmp/y", "mode": "overwrite"}},
            {"id": "q", "label": "q", "type": "Egress", "engine": "duckdb",
             "config": {"format": "csv", "path": "/tmp/q.csv", "mode": "overwrite"}},
        ],
        edges=[
            {"from": "in", "to": "ch"},
            {"from": "ch", "to": "out"},
            {"from": "ch", "to": "q", "port": "spillway"},
        ],
    )
    with pytest.raises(CompileError, match="cross-island spillway"):
        ccompile(bp, engine="spark")


# ── An island whose engine is not registered ────────────────────────────────


def test_compile_unregistered_engine_pin_raises():
    bp = _bp(
        [
            {"id": "in", "label": "in", "type": "Ingress", "engine": "spark",
             "config": {"format": "csv", "path": "/tmp/x.csv"}},
            {"id": "out", "label": "out", "type": "Egress", "engine": "flink",
             "config": {"format": "parquet", "path": "/tmp/y", "mode": "overwrite"}},
        ],
        edges=[{"from": "in", "to": "out"}],
    )
    with pytest.raises(UnknownEngineError):
        ccompile(bp, engine="spark")


# ── Invariant enforcement: a handoff module must never reach the gate ──────


def test_handoff_modules_never_reach_the_capability_gate(monkeypatch):
    """Enforces the invariant `module.type.Handoff: unsupported` (both
    engines' capabilities.yml) depends on for safety: a synthetic handoff
    module's id must NEVER be a member of any island's `module_ids`, because
    `compiler.compile()`'s per-island capability-gate loop filters
    `manifest.modules` down to island membership before calling
    `check_capabilities` (see `aqueduct/compiler/handoff.py`'s module
    docstring and `docs/specs.md` §10.9). Today that holds because islands
    are derived from the PRE-insertion module graph and handoff synthesis
    runs strictly after.

    This is exactly the shape of guarantee that holds by inspection and can
    silently stop holding later — the most likely breaker is a future step
    that must make a handoff module actually EXECUTE (upstream writes,
    downstream reads), which is a natural reason to fold it into an island.
    The moment that happens, `check_capabilities` sees a Handoff module,
    looks up `module.type.Handoff`, finds `unsupported`, and EVERY polyglot
    Blueprint stops compiling — a change that will look like a capability
    regression, not like a broken invariant, unless something here catches
    it first.

    Spies on `aqueduct.compiler.compiler.check_capabilities` (the exact call
    site) rather than re-deriving islands independently, so this proves the
    REAL code path, not a parallel computation of the same thing.
    """
    import aqueduct.compiler.compiler as compiler_module

    seen_manifests: list = []
    real_check_capabilities = compiler_module.check_capabilities

    def _spy(manifest, engine):
        seen_manifests.append(manifest)
        return real_check_capabilities(manifest, engine=engine)

    monkeypatch.setattr(compiler_module, "check_capabilities", _spy)

    bp = _bp(
        [
            {"id": "extract", "label": "extract", "type": "Channel", "engine": "spark",
             "config": {"op": "sql", "query": "SELECT 1 AS x"}},
            {"id": "agg", "label": "agg", "type": "Channel", "engine": "duckdb",
             "config": {"op": "sql", "query": "SELECT * FROM extract"}},
        ],
        edges=[{"from": "extract", "to": "agg"}],
    )
    manifest = ccompile(bp, engine="spark")

    # Sanity: a handoff module really was synthesized — otherwise the rest
    # of this test proves nothing.
    assert any(m.type == ModuleType.Handoff for m in manifest.modules)
    assert seen_manifests, "check_capabilities was never called — test setup is broken"

    for island_manifest in seen_manifests:
        offenders = [m.id for m in island_manifest.modules if m.type == ModuleType.Handoff]
        assert not offenders, (
            f"check_capabilities was called with Handoff module(s) {offenders} "
            "present in a per-island filtered manifest. A handoff module's id "
            "must NEVER be a member of any island's module_ids — see "
            "aqueduct/compiler/handoff.py's module docstring. If handoff "
            "modules have started participating in islands (e.g. because a "
            "later step wires them into real per-island execution), "
            "module.type.Handoff MUST be re-declared with a REAL verdict "
            "(and a `tests:` link, if `supported`) in BOTH capabilities.yml "
            "files BEFORE that change ships — otherwise every polyglot "
            "Blueprint will fail to compile against the current "
            "`unsupported` verdict."
        )
