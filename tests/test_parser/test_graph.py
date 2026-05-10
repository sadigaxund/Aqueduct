"""Tests for the Parser layer: Cycle detection and graph structure."""

from __future__ import annotations
from pathlib import Path
import pytest
from aqueduct.parser.parser import ParseError, parse

FIXTURES = Path(__file__).parent.parent / "fixtures"


class TestCycleDetection:
    def test_cycle_raises(self):
        with pytest.raises(ParseError, match="[Cc]ycle"):
            parse(FIXTURES / "invalid_cycle.yml")

    def test_valid_dag_passes(self):
        bp = parse(FIXTURES / "valid_minimal.yml")
        assert len(bp.modules) == 3

    def test_depends_on_cycle_raises(self, tmp_path):
        bad = tmp_path / "depends_cycle.yml"
        bad.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\ncontext: {}\n"
            "modules:\n"
            "  - id: a\n    type: Channel\n    label: A\n    depends_on: [b]\n"
            "  - id: b\n    type: Channel\n    label: B\n    depends_on: [a]\n"
            "edges: []\n"
        )
        with pytest.raises(ParseError, match="[Cc]ycle"):
            parse(bad)

    def test_edge_to_unknown_module_raises(self, tmp_path):
        bad = tmp_path / "unknown_edge.yml"
        bad.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\ncontext: {}\n"
            "modules:\n  - id: a\n    type: Channel\n    label: A\n"
            "edges:\n  - from: a\n    to: ghost\n"
        )
        with pytest.raises(ParseError):
            parse(bad)

    def test_invalid_spillway_target_raises(self, tmp_path):
        bad = tmp_path / "bad_spillway.yml"
        bad.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\ncontext: {}\n"
            "modules:\n"
            "  - id: a\n    type: Channel\n    label: A\n    spillway: ghost\n"
            "edges: []\n"
        )
        with pytest.raises(ParseError, match="spillway"):
            parse(bad)

    def test_self_loop_cycle_raises(self, tmp_path):
        bad = tmp_path / "self_loop.yml"
        bad.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\ncontext: {}\n"
            "modules:\n  - id: a\n    type: Channel\n    label: A\n"
            "edges:\n  - from: a\n    to: a\n"
        )
        with pytest.raises(ParseError, match="[Cc]ycle"):
            parse(bad)

    def test_three_node_cycle_raises(self, tmp_path):
        bad = tmp_path / "three_node_cycle.yml"
        bad.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\ncontext: {}\n"
            "modules:\n"
            "  - id: a\n    type: Channel\n    label: A\n"
            "  - id: b\n    type: Channel\n    label: B\n"
            "  - id: c\n    type: Channel\n    label: C\n"
            "edges:\n"
            "  - from: a\n    to: b\n"
            "  - from: b\n    to: c\n"
            "  - from: c\n    to: a\n"
        )
        with pytest.raises(ParseError, match="[Cc]ycle"):
            parse(bad)

    def test_disconnected_graph_parses_successfully(self, tmp_path):
        good = tmp_path / "disconnected.yml"
        good.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\ncontext: {}\n"
            "modules:\n"
            "  - id: branch1\n    type: Ingress\n    label: A\n"
            "  - id: branch2\n    type: Ingress\n    label: B\n"
            "edges: []\n"
        )
        bp = parse(good)
        assert len(bp.modules) == 2
        assert len(bp.edges) == 0


class TestParserGraph:
    def test_validate_spillway_targets_with_missing_target(self):
        from aqueduct.parser.graph import validate_spillway_targets
        from aqueduct.parser.models import Module

        modules = (
            Module(id="m1", type="Channel", label="M1", config={}, spillway="nonexistent_egress"),
        )
        with pytest.raises(ValueError, match="nonexistent_egress"):
            validate_spillway_targets(list(modules))

    def test_validate_spillway_targets_valid(self):
        from aqueduct.parser.graph import validate_spillway_targets
        from aqueduct.parser.models import Module

        modules = (
            Module(id="m1", type="Channel", label="M1", config={}, spillway="eg1"),
            Module(id="eg1", type="Egress", label="Eg1", config={}),
        )
        validate_spillway_targets(list(modules))  # should not raise


class TestParserGraphTopologicalSort:
    """topological_order and _build_adjacency uncovered paths."""

    def test_topological_order_linear(self):
        from aqueduct.parser.graph import topological_order
        from aqueduct.parser.models import Module, Edge

        m1 = Module(id="a", type="Ingress", label="A", config={})
        m2 = Module(id="b", type="Channel", label="B", config={})
        edge = Edge(from_id="a", to_id="b", port="main")
        order = topological_order([m1, m2], [edge])
        assert order == ["a", "b"]

    def test_topological_order_with_depends_on(self):
        from aqueduct.parser.graph import topological_order
        from aqueduct.parser.models import Module, Edge
        import dataclasses

        m1 = Module(id="x", type="Ingress", label="X", config={})
        m2 = dataclasses.replace(
            Module(id="y", type="Channel", label="Y", config={}),
            depends_on=("x",),
        )
        order = topological_order([m1, m2], [])
        assert "x" in order
        assert "y" in order
        assert order.index("x") < order.index("y")

    def test_build_adjacency_unknown_from_raises(self):
        from aqueduct.parser.graph import _build_adjacency
        from aqueduct.parser.models import Module, Edge

        m1 = Module(id="a", type="Ingress", label="A", config={})
        bad_edge = Edge(from_id="NOPE", to_id="a", port="main")
        with pytest.raises(ValueError, match="NOPE"):
            _build_adjacency([m1], [bad_edge])

    def test_build_adjacency_unknown_to_raises(self):
        from aqueduct.parser.graph import _build_adjacency
        from aqueduct.parser.models import Module, Edge

        m1 = Module(id="a", type="Ingress", label="A", config={})
        bad_edge = Edge(from_id="a", to_id="NOPE", port="main")
        with pytest.raises(ValueError, match="NOPE"):
            _build_adjacency([m1], [bad_edge])
