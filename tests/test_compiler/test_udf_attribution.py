"""UDF -> island attribution (Phase 81 follow-up).

Manifest-scoped UDF capability leaves (`feature.python_udf`/`feature.java_udf`,
UDF `return_type` `type.*` leaves) must be gated only against the island(s)
whose SQL actually references the UDF — not against every island in the
Blueprint, which would reject the phase's flagship shape (a Python UDF used
only on a Spark island, with an unrelated DuckDB island present elsewhere).

Covers: `aqueduct.compiler.lineage`'s sqlglot-backed function-name extraction
(`referenced_function_names`/`referenced_function_names_in_expr`),
`aqueduct.compiler.udf_attribution.attribute_udfs_to_islands`'s positive
attribution and fail-closed fallback, and the full compile()-level behavior
in both directions (a Spark-only UDF compiles cleanly; a UDF genuinely
referenced from a DuckDB island still raises naming the unsupported leaf).
"""

from __future__ import annotations

from pathlib import Path

import pytest

# Register both engines' capability tables.
import aqueduct.executor.duckdb_.capabilities  # noqa: F401
import aqueduct.executor.spark.capabilities  # noqa: F401
from aqueduct.compiler.compiler import compile as ccompile
from aqueduct.compiler.islands import Island, derive_islands, resolve_module_engines
from aqueduct.compiler.lineage import (
    referenced_function_names,
    referenced_function_names_in_expr,
)
from aqueduct.compiler.udf_attribution import attribute_udfs_to_islands
from aqueduct.errors import CompileError
from aqueduct.parser.models import Module, ModuleType
from aqueduct.parser.parser import parse_dict

pytestmark = pytest.mark.unit


# ── lineage.py helpers ───────────────────────────────────────────────────────


def test_referenced_function_names_finds_custom_and_builtin():
    names = referenced_function_names("SELECT mask(name) AS n, SUM(amount) FROM up")
    assert names == {"mask", "sum"}


def test_referenced_function_names_returns_none_on_parse_failure():
    assert referenced_function_names("SELECT FROM (((") is None


def test_referenced_function_names_in_expr_finds_functions_in_condition():
    names = referenced_function_names_in_expr("mask(a.id) = mask(b.id) AND amount > 0")
    assert names == {"mask"}


def test_referenced_function_names_in_expr_returns_none_on_parse_failure():
    assert referenced_function_names_in_expr("AND AND (((") is None


# ── attribute_udfs_to_islands ───────────────────────────────────────────────


def _m(id_, type_=ModuleType.Channel, config=None):
    return Module(id=id_, type=type_, label=id_, config=config or {})


def test_udf_attributed_only_to_the_island_that_references_it():
    modules = [
        _m("extract", config={"op": "sql", "query": "SELECT mask(name) FROM up"}),
        _m("agg", config={"op": "sql", "query": "SELECT * FROM extract"}),
    ]
    from aqueduct.parser.models import Edge

    edges = [Edge(from_id="extract", to_id="agg")]
    resolved = {"extract": "spark", "agg": "duckdb"}
    islands = derive_islands(modules, edges, resolved)
    spark_island = next(i for i in islands if i.engine == "spark")
    duckdb_island = next(i for i in islands if i.engine == "duckdb")

    attribution = attribute_udfs_to_islands(
        modules, ({"id": "mask", "lang": "python"},), islands,
    )
    assert attribution["mask"] == {spark_island}
    assert duckdb_island not in attribution["mask"]


def test_unreferenced_udf_falls_back_to_every_island():
    """No positive hit and no unparseable construct anywhere -> conservative
    fallback to every island (the pre-attribution default), not zero."""
    modules = [_m("a", config={"op": "sql", "query": "SELECT 1"})]
    islands = derive_islands(modules, [], {"a": "spark"})
    attribution = attribute_udfs_to_islands(
        modules, ({"id": "unused_udf", "lang": "python"},), islands,
    )
    assert attribution["unused_udf"] == set(islands)


def test_unparseable_construct_forces_conservative_fallback_for_every_udf():
    """A UDF used only on the Spark island still gets checked against a
    DuckDB island that carries SQL sqlglot cannot parse — we cannot rule
    out that unparseable text calling the UDF too."""
    modules = [
        _m("extract", config={"op": "sql", "query": "SELECT mask(name) FROM up"}),
        _m("agg", config={"op": "sql", "query": "SELECT FROM (((", }),
    ]
    from aqueduct.parser.models import Edge

    edges = [Edge(from_id="extract", to_id="agg")]
    resolved = {"extract": "spark", "agg": "duckdb"}
    islands = derive_islands(modules, edges, resolved)
    duckdb_island = next(i for i in islands if i.engine == "duckdb")

    attribution = attribute_udfs_to_islands(
        modules, ({"id": "mask", "lang": "python"},), islands,
    )
    assert duckdb_island in attribution["mask"]


def test_filter_condition_positively_attributes_udf():
    modules = [_m("a", config={"op": "filter", "condition": "mask(id) > 0"})]
    islands = derive_islands(modules, [], {"a": "spark"})
    attribution = attribute_udfs_to_islands(
        modules, ({"id": "mask", "lang": "python"},), islands,
    )
    assert attribution["mask"] == set(islands)  # only island — same as "attributed to it"


def test_junction_conditional_branch_attributes_udf():
    modules = [_m(
        "j", type_=ModuleType.Junction,
        config={"mode": "conditional", "branches": [
            {"id": "b1", "condition": "mask(region) = 1"},
            {"id": "b2", "condition": "_else_"},
        ]},
    )]
    islands = derive_islands(modules, [], {"j": "spark"})
    attribution = attribute_udfs_to_islands(
        modules, ({"id": "mask", "lang": "python"},), islands,
    )
    assert attribution["mask"] == set(islands)


def test_assert_sql_row_rule_attributes_udf():
    modules = [_m(
        "chk", type_=ModuleType.Assert,
        config={"rules": [{"type": "sql_row", "expr": "mask(id) IS NOT NULL"}]},
    )]
    islands = derive_islands(modules, [], {"chk": "spark"})
    attribution = attribute_udfs_to_islands(
        modules, ({"id": "mask", "lang": "python"},), islands,
    )
    assert attribution["mask"] == set(islands)


def test_deduplicate_order_by_attributes_udf():
    modules = [_m("a", config={
        "op": "deduplicate", "key": "id", "order_by": "mask(name) DESC",
    })]
    islands = derive_islands(modules, [], {"a": "spark"})
    attribution = attribute_udfs_to_islands(
        modules, ({"id": "mask", "lang": "python"},), islands,
    )
    assert attribution["mask"] == set(islands)


def test_deduplicate_order_by_only_attributes_its_own_island():
    modules = [
        _m("extract", config={"op": "deduplicate", "key": "id", "order_by": "mask(name) DESC"}),
        _m("agg", config={"op": "sql", "query": "SELECT * FROM extract"}),
    ]
    from aqueduct.parser.models import Edge

    edges = [Edge(from_id="extract", to_id="agg")]
    resolved = {"extract": "spark", "agg": "duckdb"}
    islands = derive_islands(modules, edges, resolved)
    spark_island = next(i for i in islands if i.engine == "spark")
    duckdb_island = next(i for i in islands if i.engine == "duckdb")

    attribution = attribute_udfs_to_islands(
        modules, ({"id": "mask", "lang": "python"},), islands,
    )
    assert spark_island in attribution["mask"]
    assert duckdb_island not in attribution["mask"]


def test_sort_order_by_string_attributes_udf():
    modules = [_m("a", config={"op": "sort", "order_by": "mask(name) DESC"})]
    islands = derive_islands(modules, [], {"a": "spark"})
    attribution = attribute_udfs_to_islands(
        modules, ({"id": "mask", "lang": "python"},), islands,
    )
    assert attribution["mask"] == set(islands)


def test_sort_order_by_list_attributes_udf():
    modules = [_m("a", config={"op": "sort", "order_by": ["region ASC", "mask(name) DESC"]})]
    islands = derive_islands(modules, [], {"a": "spark"})
    attribution = attribute_udfs_to_islands(
        modules, ({"id": "mask", "lang": "python"},), islands,
    )
    assert attribution["mask"] == set(islands)


def test_sort_columns_spelling_attributes_udf():
    """`sort` accepts EITHER key spelling — `columns` is the fallback when
    `order_by` is absent (`_execute_sort`: `cfg.get("order_by") or
    cfg.get("columns")`)."""
    modules = [_m("a", config={"op": "sort", "columns": ["mask(name) DESC"]})]
    islands = derive_islands(modules, [], {"a": "spark"})
    attribution = attribute_udfs_to_islands(
        modules, ({"id": "mask", "lang": "python"},), islands,
    )
    assert attribution["mask"] == set(islands)


def test_sort_order_by_only_attributes_its_own_island():
    modules = [
        _m("extract", config={"op": "sort", "order_by": "mask(name) DESC"}),
        _m("agg", config={"op": "sql", "query": "SELECT * FROM extract"}),
    ]
    from aqueduct.parser.models import Edge

    edges = [Edge(from_id="extract", to_id="agg")]
    resolved = {"extract": "spark", "agg": "duckdb"}
    islands = derive_islands(modules, edges, resolved)
    spark_island = next(i for i in islands if i.engine == "spark")
    duckdb_island = next(i for i in islands if i.engine == "duckdb")

    attribution = attribute_udfs_to_islands(
        modules, ({"id": "mask", "lang": "python"},), islands,
    )
    assert spark_island in attribution["mask"]
    assert duckdb_island not in attribution["mask"]


# ── Full compile(): both directions ─────────────────────────────────────────


def _bp(modules, edges=None, udf_registry=None):
    d = {
        "aqueduct": "1.0", "id": "bp", "name": "t",
        "modules": modules,
        **({"edges": edges} if edges is not None else {}),
        **({"udf_registry": udf_registry} if udf_registry is not None else {}),
    }
    return parse_dict(d, base_dir=Path("/tmp"))


def test_compile_spark_only_udf_with_duckdb_island_present_compiles_cleanly():
    """The flagship polyglot shape: heavy Spark transform with a Python UDF,
    a DuckDB aggregate downstream that never touches the UDF. Must compile."""
    bp = _bp(
        [
            {"id": "extract", "label": "extract", "type": "Channel", "engine": "spark",
             "config": {"op": "sql", "query": "SELECT mask(name) AS name FROM up"}},
            {"id": "agg", "label": "agg", "type": "Channel", "engine": "duckdb",
             "config": {"op": "sql", "query": "SELECT * FROM extract"}},
        ],
        edges=[{"from": "extract", "to": "agg"}],
        udf_registry=[{"id": "mask", "lang": "python", "module": "udfs.mask", "entry": "mask"}],
    )
    manifest = ccompile(bp, engine="spark")
    # A real cross-engine handoff module now exists at this boundary (Phase 81
    # Step 2) — assert the two authored modules keep their engines rather than
    # the exact module set, so this UDF-attribution test doesn't also pin
    # down handoff-naming details that belong to test_handoff.py.
    engines = {m.id: m.engine for m in manifest.modules}
    assert engines["extract"] == "spark"
    assert engines["agg"] == "duckdb"


def test_compile_udf_referenced_from_duckdb_island_raises():
    bp = _bp(
        [
            {"id": "extract", "label": "extract", "type": "Channel", "engine": "spark",
             "config": {"op": "sql", "query": "SELECT name FROM up"}},
            {"id": "agg", "label": "agg", "type": "Channel", "engine": "duckdb",
             "config": {"op": "sql", "query": "SELECT mask(name) AS name FROM extract"}},
        ],
        edges=[{"from": "extract", "to": "agg"}],
        udf_registry=[{"id": "mask", "lang": "python", "module": "udfs.mask", "entry": "mask"}],
    )
    with pytest.raises(CompileError, match="feature.python_udf"):
        ccompile(bp, engine="spark")


def test_compile_spark_dedup_order_by_udf_with_duckdb_island_compiles_cleanly():
    bp = _bp(
        [
            {"id": "extract", "label": "extract", "type": "Channel", "engine": "spark",
             "config": {"op": "deduplicate", "key": "id", "order_by": "mask(name) DESC"}},
            {"id": "agg", "label": "agg", "type": "Channel", "engine": "duckdb",
             "config": {"op": "sql", "query": "SELECT * FROM extract"}},
        ],
        edges=[{"from": "extract", "to": "agg"}],
        udf_registry=[{"id": "mask", "lang": "python", "module": "udfs.mask", "entry": "mask"}],
    )
    manifest = ccompile(bp, engine="spark")
    # A real cross-engine handoff module now exists at this boundary (Phase 81
    # Step 2) — assert the two authored modules keep their engines rather than
    # the exact module set, so this UDF-attribution test doesn't also pin
    # down handoff-naming details that belong to test_handoff.py.
    engines = {m.id: m.engine for m in manifest.modules}
    assert engines["extract"] == "spark"
    assert engines["agg"] == "duckdb"


def test_compile_udf_referenced_from_duckdb_dedup_order_by_raises():
    bp = _bp(
        [
            {"id": "extract", "label": "extract", "type": "Channel", "engine": "spark",
             "config": {"op": "sql", "query": "SELECT name, id FROM up"}},
            {"id": "agg", "label": "agg", "type": "Channel", "engine": "duckdb",
             "config": {"op": "deduplicate", "key": "id", "order_by": "mask(name) DESC"}},
        ],
        edges=[{"from": "extract", "to": "agg"}],
        udf_registry=[{"id": "mask", "lang": "python", "module": "udfs.mask", "entry": "mask"}],
    )
    with pytest.raises(CompileError, match="feature.python_udf"):
        ccompile(bp, engine="spark")


def test_compile_spark_sort_order_by_udf_with_duckdb_island_compiles_cleanly():
    bp = _bp(
        [
            {"id": "extract", "label": "extract", "type": "Channel", "engine": "spark",
             "config": {"op": "sort", "order_by": "mask(name) DESC"}},
            {"id": "agg", "label": "agg", "type": "Channel", "engine": "duckdb",
             "config": {"op": "sql", "query": "SELECT * FROM extract"}},
        ],
        edges=[{"from": "extract", "to": "agg"}],
        udf_registry=[{"id": "mask", "lang": "python", "module": "udfs.mask", "entry": "mask"}],
    )
    manifest = ccompile(bp, engine="spark")
    # A real cross-engine handoff module now exists at this boundary (Phase 81
    # Step 2) — assert the two authored modules keep their engines rather than
    # the exact module set, so this UDF-attribution test doesn't also pin
    # down handoff-naming details that belong to test_handoff.py.
    engines = {m.id: m.engine for m in manifest.modules}
    assert engines["extract"] == "spark"
    assert engines["agg"] == "duckdb"


def test_compile_udf_referenced_from_duckdb_sort_order_by_raises():
    bp = _bp(
        [
            {"id": "extract", "label": "extract", "type": "Channel", "engine": "spark",
             "config": {"op": "sql", "query": "SELECT name FROM up"}},
            {"id": "agg", "label": "agg", "type": "Channel", "engine": "duckdb",
             "config": {"op": "sort", "order_by": ["mask(name) DESC"]}},
        ],
        edges=[{"from": "extract", "to": "agg"}],
        udf_registry=[{"id": "mask", "lang": "python", "module": "udfs.mask", "entry": "mask"}],
    )
    with pytest.raises(CompileError, match="feature.python_udf"):
        ccompile(bp, engine="spark")


def test_compile_single_engine_udf_still_gated_as_before():
    """Sanity: a single-engine (all-spark) Blueprint with a Python UDF is
    unaffected by attribution — it never had a DuckDB island to worry about."""
    bp = _bp(
        [
            {"id": "in", "label": "in", "type": "Channel",
             "config": {"op": "sql", "query": "SELECT mask(name) AS name FROM up"}},
        ],
        udf_registry=[{"id": "mask", "lang": "python", "module": "udfs.mask", "entry": "mask"}],
    )
    manifest = ccompile(bp, engine="spark")
    assert manifest.blueprint_id == "bp"
