"""Unit tests for spillway error_types filtering (typed catch)."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit


def test_is_gate_closed_detects_sentinel():
    from aqueduct.executor.spark.executor import _is_gate_closed, _GATE_CLOSED
    assert _is_gate_closed(_GATE_CLOSED) is True
    assert _is_gate_closed(None) is False
    assert _is_gate_closed("dataframe") is False


def test_apply_spillway_filter_noop_on_main_port():
    """_apply_spillway_filter returns val unchanged for non-spillway edges."""
    from aqueduct.executor.spark.executor import _apply_spillway_filter
    from aqueduct.parser.models import Edge
    edge = Edge(from_id="a", to_id="b", port="main", error_types=())
    val = "not_a_dataframe"
    result = _apply_spillway_filter(val, edge)
    assert result is val


def test_apply_spillway_filter_noop_on_spillway_without_error_types():
    """Spillway edge without error_types → catch-all, no filter."""
    from aqueduct.executor.spark.executor import _apply_spillway_filter
    from aqueduct.parser.models import Edge
    edge = Edge(from_id="a", to_id="b", port="spillway", error_types=())
    val = "not_a_dataframe"
    result = _apply_spillway_filter(val, edge)
    assert result is val


def test_apply_spillway_filter_noop_on_none():
    """None value returns None."""
    from aqueduct.executor.spark.executor import _apply_spillway_filter
    from aqueduct.parser.models import Edge
    edge = Edge(from_id="a", to_id="b", port="spillway", error_types=["DataQualityViolation"])
    result = _apply_spillway_filter(None, edge)
    assert result is None


def test_apply_spillway_filter_noop_on_gate_closed():
    """_GATE_CLOSED sentinel passes through unfiltered."""
    from aqueduct.executor.spark.executor import _apply_spillway_filter, _GATE_CLOSED
    from aqueduct.parser.models import Edge
    edge = Edge(from_id="a", to_id="b", port="spillway", error_types=["DataQualityViolation"])
    result = _apply_spillway_filter(_GATE_CLOSED, edge)
    assert result is _GATE_CLOSED


def test_spillway_edge_error_types_preserved_in_arcade_expansion():
    """Arcade expansion preserves error_types on spillway edges."""
    from aqueduct.parser.models import Edge
    edge = Edge(from_id="arc__src", to_id="arc__sink", port="spillway", error_types=("DataQuality",))
    assert edge.error_types == ("DataQuality",)
    assert edge.port == "spillway"


def test_assert_quarantine_stamps_aq_error_type():
    """Assert quarantine rows stamp _aq_error_type = rule's error_type label."""
    et = "MyCustomViolation"
    rule = {"name": "my_rule", "error_type": et, "on_fail": "quarantine", "sql_row": "SELECT 1"}
    assert rule.get("error_type") == et


# ── Phase 35 — spillway error_types Spark integration tests ────────────────────

class TestSpillwayErrorTypesSpark:
    """These tests need a real SparkSession — conftest.py provides the spark fixture."""

    def test_spillway_error_type_filters_rows(self, spark):
        """Spillway edge with error_types → only matching _aq_error_type rows pass through."""
        from aqueduct.executor.spark.executor import _apply_spillway_filter
        from aqueduct.parser.models import Edge

        df = spark.createDataFrame([
            (1, "TypeA"), (2, "TypeB"), (3, "TypeA"),
        ], ["id", "_aq_error_type"])

        edge = Edge(from_id="a", to_id="b", port="spillway", error_types=("TypeA",))
        result = _apply_spillway_filter(df, edge)

        rows = result.collect()
        assert len(rows) == 2
        assert all(r._aq_error_type == "TypeA" for r in rows)

    def test_spillway_error_type_no_match_returns_empty(self, spark):
        """Spillway edge with error_types that match nothing → empty DataFrame."""
        from aqueduct.executor.spark.executor import _apply_spillway_filter
        from aqueduct.parser.models import Edge

        df = spark.createDataFrame([
            (1, "TypeA"), (2, "TypeB"),
        ], ["id", "_aq_error_type"])

        edge = Edge(from_id="a", to_id="b", port="spillway", error_types=("TypeX",))
        result = _apply_spillway_filter(df, edge)

        assert result.count() == 0

    def test_spillway_two_edges_from_one_assert(self, spark):
        """Two spillway edges with different error_types from one Assert → each gets only its rows."""
        from aqueduct.executor.spark.executor import _apply_spillway_filter
        from aqueduct.parser.models import Edge

        df = spark.createDataFrame([
            (1, "TypeA"), (2, "TypeB"), (3, "TypeA"),
        ], ["id", "_aq_error_type"])

        edge_a = Edge(from_id="a1", to_id="sink_a", port="spillway", error_types=("TypeA",))
        edge_b = Edge(from_id="a1", to_id="sink_b", port="spillway", error_types=("TypeB",))

        result_a = _apply_spillway_filter(df, edge_a)
        result_b = _apply_spillway_filter(df, edge_b)

        rows_a = result_a.collect()
        rows_b = result_b.collect()
        assert len(rows_a) == 2
        assert all(r._aq_error_type == "TypeA" for r in rows_a)
        assert len(rows_b) == 1
        assert all(r._aq_error_type == "TypeB" for r in rows_b)

    def test_funnel_consumes_filtered_spillway(self, spark):
        """Funnel consuming a spillway edge with error_types gets the filtered frame."""
        from aqueduct.executor.spark.executor import _apply_spillway_filter
        from aqueduct.executor.spark.funnel import execute_funnel
        from aqueduct.parser.models import Edge, Module

        df = spark.createDataFrame([
            (1, "TypeA"), (2, "TypeB"), (3, "TypeA"),
        ], ["id", "_aq_error_type"])

        edge = Edge(from_id="a1", to_id="sink", port="spillway", error_types=("TypeA",))
        filtered = _apply_spillway_filter(df, edge)

        funnel = Module(id="f1", type="Funnel", label="F1",
                        config={"mode": "union_all", "inputs": ["s1", "s2"]})
        result = execute_funnel(funnel, {"s1": filtered, "s2": df})
        assert result is not None
        assert result.count() > 0
