"""``feature.table_addressing`` — catalog ``table:`` addressing on Ingress/Egress.

DuckDB Ingress/Egress require ``format:`` + ``path:``; there is no catalog to
resolve a bare ``table:`` name against. Before this leaf existed, nothing
gated ``table:`` addressing at compile time on ``engine=duckdb`` — the
gallery snippet ``gallery/snippets/23_table_first/blueprint.yml`` (Ingress
``table: demo_table`` / Egress ``format: parquet, table: demo_output``) would
compile clean and then die mid-run with a confusing ``'format' is required``
runtime error. These tests pin that the gate now fires a clean
``CompileError`` at compile time on DuckDB and stays a no-op on Spark, both
via the unit-level ``leaves_for_module``/``check_capabilities`` API and
end-to-end against the real gallery snippet.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# Register both engines' capability tables.
import aqueduct.executor.duckdb_.capabilities  # noqa: F401
import aqueduct.executor.spark.capabilities  # noqa: F401
from aqueduct.compiler.capability_check import check_capabilities, leaves_for_module
from aqueduct.compiler.compiler import compile as compile_bp
from aqueduct.errors import CompileError
from aqueduct.models import Manifest, Module
from aqueduct.parser.parser import parse

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_TABLE_FIRST_BP = _REPO / "gallery" / "snippets" / "23_table_first" / "blueprint.yml"


def _module(id_, type_, config=None):
    return Module(id=id_, type=type_, label=id_, config=config or {})


def _manifest(modules):
    return Manifest(
        blueprint_id="bp", context={}, modules=tuple(modules), edges=(),
        spark_config={},
    )


# ── leaves_for_module: emitted only when table: addressing is actually used ─


def test_ingress_table_addressing_emits_feature_leaf():
    leaves = leaves_for_module(_module("i", "Ingress", {"table": "demo_table"}))
    assert "feature.table_addressing" in leaves


def test_egress_table_addressing_emits_feature_leaf():
    leaves = leaves_for_module(
        _module("e", "Egress", {"format": "parquet", "table": "demo_output", "mode": "overwrite"})
    )
    assert "feature.table_addressing" in leaves


def test_path_addressing_does_not_emit_table_feature_leaf():
    leaves = leaves_for_module(_module("i", "Ingress", {"format": "csv", "path": "x.csv"}))
    assert "feature.table_addressing" not in leaves

    leaves = leaves_for_module(
        _module("e", "Egress", {"format": "parquet", "path": "o.parquet", "mode": "overwrite"})
    )
    assert "feature.table_addressing" not in leaves


# ── The gate fires on DuckDB, stays a no-op on Spark ────────────────────────


def test_table_addressing_gated_unsupported_on_duckdb_not_spark():
    m = _manifest([
        _module("src", "Ingress", {"table": "demo_table"}),
        _module("out", "Egress", {"format": "parquet", "table": "demo_output", "mode": "overwrite"}),
    ])
    problems = check_capabilities(m, engine="duckdb")
    leaf_ids = {p.leaf_id for p in problems}
    assert "feature.table_addressing" in leaf_ids
    # Both modules use table: addressing -> both flagged.
    module_ids = {p.module_id for p in problems if p.leaf_id == "feature.table_addressing"}
    assert module_ids == {"src", "out"}
    # Spark supports table: addressing -> no problem for the same manifest.
    assert check_capabilities(m, engine="spark") == []


# ── End-to-end against the real gallery snippet ─────────────────────────────


def test_table_first_snippet_compiles_clean_on_spark():
    manifest = compile_bp(
        parse(_TABLE_FIRST_BP), blueprint_path=_TABLE_FIRST_BP,
        deployment_env="local", deployment_target="local",
        engine="spark",
    )
    assert check_capabilities(manifest, engine="spark") == []


def test_table_first_snippet_fails_clean_compile_error_on_duckdb():
    with pytest.raises(CompileError) as exc_info:
        compile_bp(
            parse(_TABLE_FIRST_BP), blueprint_path=_TABLE_FIRST_BP,
            deployment_env="local", deployment_target="local",
            engine="duckdb",
        )
    msg = str(exc_info.value)
    # Names the module, the unsupported capability, and the hint.
    assert "'src'" in msg
    assert "feature.table_addressing" in msg
    assert "no catalog to resolve a bare table" in msg
