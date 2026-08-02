"""``feature.table_addressing`` — catalog ``table:`` addressing on Ingress/Egress.

DuckDB genuinely has a catalog (``memory.main``,
``system.main``/``information_schema``/``pg_catalog``, plus whatever
``ATTACH`` adds — verified 2026-07-31), and Pass G2 built the missing
IMPLEMENTATION mapping a Blueprint's ``table:`` name onto it: Ingress reads
via ``con.table()``, Egress writes via ``CREATE OR REPLACE``/``CREATE``/
``INSERT INTO`` mode-mapped onto DuckDB's own DDL guards (see
``duckdb_/ingress.py::_read_table`` / ``duckdb_/egress.py::_write_table`` for
the full catalog defaulting rule). ``feature.table_addressing`` flipped from
``unsupported`` to ``supported`` on DuckDB — these tests now pin the gate
staying a NO-OP on both engines (the compile-time refusal these tests used to
pin was the state BEFORE the implementation landed), both via the unit-level
``leaves_for_module``/``check_capabilities`` API and end-to-end against the
real gallery snippet ``gallery/snippets/23_table_first/blueprint.yml``
(Ingress ``table: demo_table`` / Egress ``format: parquet, table:
demo_output``).
"""

from __future__ import annotations

from pathlib import Path

import pytest

# Register both engines' capability tables.
import aqueduct.executor.duckdb_.capabilities  # noqa: F401
import aqueduct.executor.spark.capabilities  # noqa: F401
from aqueduct.compiler.capability_check import check_capabilities, leaves_for_module
from aqueduct.compiler.compiler import compile as compile_bp
from aqueduct.models import Manifest, Module
from aqueduct.parser.parser import parse

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_TABLE_FIRST_BP = _REPO / "gallery" / "snippets" / "23_table_first" / "blueprint.yml"


def _module(id_, type_, config=None):
    return Module(id=id_, type=type_, label=id_, config=config or {})


def _manifest(modules):
    return Manifest(
        blueprint_id="bp",
        context={},
        modules=tuple(modules),
        edges=(),
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


# ── The gate is a no-op on BOTH engines (Pass G2 — DuckDB implements it now) ─


def test_table_addressing_clean_on_both_engines():
    m = _manifest(
        [
            _module("src", "Ingress", {"table": "demo_table"}),
            _module(
                "out", "Egress", {"format": "parquet", "table": "demo_output", "mode": "overwrite"}
            ),
        ]
    )
    assert check_capabilities(m, engine="duckdb") == []
    assert check_capabilities(m, engine="spark") == []


# ── End-to-end against the real gallery snippet ─────────────────────────────


def test_table_first_snippet_compiles_clean_on_spark():
    manifest = compile_bp(
        parse(_TABLE_FIRST_BP),
        blueprint_path=_TABLE_FIRST_BP,
        deployment_env="local",
        deployment_target="local",
        engine="spark",
    )
    assert check_capabilities(manifest, engine="spark") == []


def test_table_first_snippet_compiles_clean_on_duckdb():
    manifest = compile_bp(
        parse(_TABLE_FIRST_BP),
        blueprint_path=_TABLE_FIRST_BP,
        deployment_env="local",
        deployment_target="local",
        engine="duckdb",
    )
    assert check_capabilities(manifest, engine="duckdb") == []
