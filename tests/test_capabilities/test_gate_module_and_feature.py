"""The compile-time capability gate enforces ``module.type.*`` and ``feature.*``.

Before this, ``capability_check.leaves_for_module()`` only emitted
channel.op / egress.* / ingress.format / junction.mode / funnel.mode, so a
verdict like ``module.type.Assert: unsupported`` or ``feature.python_udf:
unsupported`` was DECORATIVE — compile passed and the failure surfaced as a raw
runtime error (a DuckDB ``Catalog Error`` for a missing UDF; an ``ExecuteError``
for an unhandled module type) instead of a clean ``CompileError``. These tests
pin that the gate now FIRES on those leaves for an engine that declares them
unsupported (DuckDB Stage A), and stays a no-op for Spark (all supported).
"""

from __future__ import annotations

import pytest

# Register both engines' capability tables.
import aqueduct.executor.duckdb_.capabilities  # noqa: F401
import aqueduct.executor.spark.capabilities  # noqa: F401
from aqueduct.compiler.capability_check import (
    check_capabilities,
    feature_leaves_for_manifest,
    leaves_for_module,
)
from aqueduct.models import Edge, Manifest, Module

pytestmark = pytest.mark.unit


def _module(id_, type_, config=None):
    return Module(id=id_, type=type_, label=id_, config=config or {})


def _manifest(modules, udf_registry=()):
    return Manifest(
        blueprint_id="bp", context={}, modules=tuple(modules), edges=(),
        spark_config={}, udf_registry=tuple(udf_registry),
    )


# ── leaves_for_module now emits module.type.<Type> ──────────────────────────


def test_leaves_for_module_emits_module_type():
    assert "module.type.Assert" in leaves_for_module(_module("a", "Assert"))
    assert "module.type.Ingress" in leaves_for_module(_module("i", "Ingress", {"format": "csv"}))


# ── feature_leaves_for_manifest derives UDF-language features off udf_registry ──


def test_feature_leaves_from_udf_registry_python():
    m = _manifest([_module("ch", "Channel", {"op": "sql", "query": "SELECT f(x) FROM up"})],
                  udf_registry=[{"id": "f", "lang": "python"}])
    pairs = feature_leaves_for_manifest(m)
    assert ("feature.python_udf", "f") in pairs


def test_feature_leaves_from_udf_registry_java_and_scala_map_to_java_udf():
    m = _manifest([], udf_registry=[
        {"id": "j", "lang": "java"}, {"id": "s", "lang": "scala"},
    ])
    leaves = {leaf for leaf, _ in feature_leaves_for_manifest(m)}
    assert leaves == {"feature.java_udf"}


def test_feature_leaves_empty_when_no_udfs():
    m = _manifest([_module("i", "Ingress", {"format": "csv"})])
    assert feature_leaves_for_manifest(m) == []


# ── The gate FIRES on DuckDB, stays a no-op on Spark ────────────────────────


def test_assert_module_gated_unsupported_on_duckdb_not_spark():
    m = _manifest([_module("q", "Assert", {})])
    problems = check_capabilities(m, engine="duckdb")
    leaf_ids = {p.leaf_id for p in problems}
    assert "module.type.Assert" in leaf_ids
    # Spark supports every module type → no problem for the same manifest.
    assert check_capabilities(m, engine="spark") == []


def test_python_udf_gated_unsupported_on_duckdb_not_spark():
    m = _manifest(
        [_module("ch", "Channel", {"op": "sql", "query": "SELECT mask(x) FROM up"})],
        udf_registry=[{"id": "mask", "lang": "python"}],
    )
    problems = check_capabilities(m, engine="duckdb")
    hit = [p for p in problems if p.leaf_id == "feature.python_udf"]
    assert hit, f"feature.python_udf not gated on duckdb: {[p.leaf_id for p in problems]}"
    # The problem names the UDF that pulled the feature in.
    assert hit[0].module_id == "mask"
    assert check_capabilities(m, engine="spark") == []


def test_supported_module_and_no_udf_is_clean_on_duckdb():
    # Ingress(csv) -> Channel(filter) -> Egress(parquet/overwrite): all within
    # DuckDB Stage A support, no UDFs. The gate must stay silent.
    m = Manifest(
        blueprint_id="bp", context={},
        modules=(
            _module("i", "Ingress", {"format": "csv", "path": "x.csv"}),
            _module("c", "Channel", {"op": "filter", "condition": "a > 1"}),
            _module("e", "Egress", {"format": "parquet", "path": "o.parquet", "mode": "overwrite"}),
        ),
        edges=(Edge(from_id="i", to_id="c"), Edge(from_id="c", to_id="e")),
        spark_config={},
    )
    assert check_capabilities(m, engine="duckdb") == []
