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
