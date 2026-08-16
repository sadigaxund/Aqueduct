"""The compile-time capability gate enforces ``module.type.*`` and ``feature.*``.

Before this, ``capability_check.leaves_for_module()`` only emitted
channel.op / egress.* / ingress.format / junction.mode / funnel.mode, so a
verdict like ``feature.java_udf: unsupported`` was DECORATIVE — compile passed
and the failure surfaced as a raw runtime error (a DuckDB ``Catalog Error``
for a missing UDF; an ``ExecuteError`` for an unhandled module type) instead
of a clean ``CompileError``. These tests pin that the gate now FIRES on a
leaf an engine declares unsupported (``feature.java_udf`` on DuckDB — a
genuine, permanent cross-engine gap), and stays a no-op for Spark (all
supported). Assert (Pass D) and Probe (Pass F) both moved from the "gated
unsupported" side to the "clean" side — see
``test_assert_module_supported_on_duckdb`` /
``test_probe_module_supported_on_duckdb``. ``feature.python_udf`` moved the
same way in Pass E — see ``test_python_udf_clean_on_both_engines``.
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
        blueprint_id="bp",
        context={},
        modules=tuple(modules),
        edges=(),
        engine_config={},
        udf_registry=tuple(udf_registry),
    )


# ── leaves_for_module now emits module.type.<Type> ──────────────────────────


def test_leaves_for_module_emits_module_type():
    assert "module.type.Assert" in leaves_for_module(_module("a", "Assert"))
    assert "module.type.Ingress" in leaves_for_module(_module("i", "Ingress", {"format": "csv"}))


# ── Parametrized constructors emit the CONSTRUCTOR leaf, never a per-argument
# one (Phase 81/82 regression — found while adding `duration(unit)`: the
# fallback branch in `_type_leaves_for_hub_type` renders the WHOLE spelling,
# including the argument, unless the constructor has its own special case —
# `Decimal` already had one; `Duration` initially did not, so a Channel cast
# to `duration(us)` minted a bogus `type.duration(us)` leaf with no row in
# either engine's capabilities.yml instead of the governed `type.duration`,
# and compilation failed with a CompileError naming a leaf that doesn't
# exist. Both constructors are pinned here so neither regresses silently.) ──


def test_duration_cast_emits_type_duration_not_the_parametrized_spelling():
    m = _module("ch", "Channel", {"op": "cast", "columns": {"n": "duration(us)"}})
    leaves = leaves_for_module(m)
    assert "type.duration" in leaves
    assert not any(leaf.startswith("type.duration(") for leaf in leaves)


def test_decimal_cast_emits_type_decimal_not_the_parametrized_spelling():
    m = _module("ch", "Channel", {"op": "cast", "columns": {"n": "decimal(10,2)"}})
    leaves = leaves_for_module(m)
    assert "type.decimal" in leaves
    assert not any(leaf.startswith("type.decimal(") for leaf in leaves)


# ── feature_leaves_for_manifest derives UDF-language features off udf_registry ──


def test_feature_leaves_from_udf_registry_python():
    m = _manifest(
        [_module("ch", "Channel", {"op": "sql", "query": "SELECT f(x) FROM up"})],
        udf_registry=[{"id": "f", "lang": "python"}],
    )
    pairs = feature_leaves_for_manifest(m)
    assert ("feature.python_udf", "f") in pairs


def test_feature_leaves_from_udf_registry_java_and_scala_map_to_java_udf():
    m = _manifest(
        [],
        udf_registry=[
            {"id": "j", "lang": "java"},
            {"id": "s", "lang": "scala"},
        ],
    )
    leaves = {leaf for leaf, _ in feature_leaves_for_manifest(m)}
    assert leaves == {"feature.java_udf"}


def test_feature_leaves_empty_when_no_udfs():
    m = _manifest([_module("i", "Ingress", {"format": "csv"})])
    assert feature_leaves_for_manifest(m) == []


# ── The gate FIRES on DuckDB, stays a no-op on Spark ────────────────────────


def test_probe_module_supported_on_duckdb():
    # Pass F — Probe moved from "gated unsupported" to genuinely implemented
    # on DuckDB (aqueduct/executor/duckdb_/probe.py); the gate must stay
    # silent for it on both engines now, same pattern as Assert (Pass D).
    m = _manifest([_module("q", "Probe", {})])
    assert check_capabilities(m, engine="duckdb") == []
    assert check_capabilities(m, engine="spark") == []


def test_assert_module_supported_on_duckdb():
    # Pass D — Assert moved from "gated unsupported" to genuinely implemented
    # on DuckDB; the gate must stay silent for it on both engines now.
    m = _manifest([_module("q", "Assert", {"rules": []})])
    assert check_capabilities(m, engine="duckdb") == []
    assert check_capabilities(m, engine="spark") == []


def test_python_udf_clean_on_both_engines():
    """feature.python_udf is `supported` on both engines (DuckDB's Pass E
    implementation via conn.create_function — see
    aqueduct/executor/duckdb_/udf.py) — the gate must stay silent on both."""
    m = _manifest(
        [_module("ch", "Channel", {"op": "sql", "query": "SELECT mask(x) FROM up"})],
        udf_registry=[{"id": "mask", "lang": "python"}],
    )
    assert check_capabilities(m, engine="duckdb") == []
    assert check_capabilities(m, engine="spark") == []


def test_java_udf_gated_unsupported_on_duckdb_not_spark():
    """DuckDB is not on the JVM — feature.java_udf stays unsupported there
    while Spark (JVM-native) supports it — unlike python_udf, this one is a
    genuine, permanent cross-engine gap, not a Pass E gap."""
    m = _manifest(
        [_module("ch", "Channel", {"op": "sql", "query": "SELECT mask(x) FROM up"})],
        udf_registry=[
            {"id": "mask", "lang": "java", "jar": "geo.jar", "class": "com.example.Mask"}
        ],
    )
    problems = check_capabilities(m, engine="duckdb")
    hit = [p for p in problems if p.leaf_id == "feature.java_udf"]
    assert hit, f"feature.java_udf not gated on duckdb: {[p.leaf_id for p in problems]}"
    # The problem names the UDF that pulled the feature in.
    assert hit[0].module_id == "mask"
    assert check_capabilities(m, engine="spark") == []


# ── probe.signal.* (Pass G2) — per-signal-type leaves ───────────────────────


def test_leaves_for_module_emits_probe_signal_types_but_not_custom():
    m = _module(
        "p",
        "Probe",
        {
            "signals": [
                {"type": "threshold", "expr": "COUNT(*) > 0"},
                {"type": "null_rates"},
                {"type": "custom", "sql": "MAX(x)"},
            ]
        },
    )
    leaves = leaves_for_module(m)
    assert "probe.signal.threshold" in leaves
    assert "probe.signal.null_rates" in leaves
    # `custom` is a user-code escape valve, not a governed per-engine
    # capability — see BUILTIN_SIGNAL_TYPES's docstring — so it never emits a
    # probe.signal.* leaf.
    assert "probe.signal.custom" not in leaves


def test_execution_partitions_gated_unsupported_on_duckdb_not_spark():
    """DuckDB has no partition concept (single-process engine) — this is the
    compile-time counterpart to duckdb_/probe.py's dedicated
    `runtime_probe_signal_unsupported` warning: the same gap must now be
    caught BEFORE a run starts, like every other unsupported leaf, rather
    than only being discovered mid-execution."""
    m = _manifest([_module("p", "Probe", {"signals": [{"type": "execution_partitions"}]})])
    problems = check_capabilities(m, engine="duckdb")
    hit = [p for p in problems if p.leaf_id == "probe.signal.execution_partitions"]
    assert (
        hit
    ), f"probe.signal.execution_partitions not gated on duckdb: {[p.leaf_id for p in problems]}"
    assert hit[0].module_id == "p"
    assert check_capabilities(m, engine="spark") == []


def test_all_other_built_in_signal_types_clean_on_both_engines():
    from aqueduct.executor.probe_plugins import BUILTIN_SIGNAL_TYPES

    non_partition = sorted(BUILTIN_SIGNAL_TYPES - {"execution_partitions"})
    signals = [{"type": t} for t in non_partition]
    # threshold/data_freshness need a config key to be realistic, but the
    # capability gate only looks at `type:` — an empty/minimal config is
    # enough to prove the leaf itself is clean on both engines.
    m = _manifest([_module("p", "Probe", {"signals": signals})])
    assert check_capabilities(m, engine="duckdb") == []
    assert check_capabilities(m, engine="spark") == []


def test_supported_module_and_no_udf_is_clean_on_duckdb():
    # Ingress(csv) -> Channel(filter) -> Egress(parquet/overwrite): all within
    # DuckDB Stage A support, no UDFs. The gate must stay silent.
    m = Manifest(
        blueprint_id="bp",
        context={},
        modules=(
            _module("i", "Ingress", {"format": "csv", "path": "x.csv"}),
            _module("c", "Channel", {"op": "filter", "condition": "a > 1"}),
            _module("e", "Egress", {"format": "parquet", "path": "o.parquet", "mode": "overwrite"}),
        ),
        edges=(Edge(from_id="i", to_id="c"), Edge(from_id="c", to_id="e")),
        engine_config={},
    )
    assert check_capabilities(m, engine="duckdb") == []
