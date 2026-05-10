"""Tests for the Executor layer: Sub-DAG selectors and reachability."""

from __future__ import annotations
from pathlib import Path
import pytest
from pyspark.sql import SparkSession

from aqueduct.executor.spark.executor import (
    ExecuteError,
    _reachable_backward,
    _reachable_forward,
    _selector_included,
    execute,
)
from aqueduct.parser.models import Edge, Module
from aqueduct.compiler.models import Manifest

FIXTURES = Path(__file__).parent.parent / "fixtures"


def _edge(from_id: str, to_id: str, port: str = "main") -> Edge:
    return Edge(from_id=from_id, to_id=to_id, port=port)


def _module(id: str, type: str = "Channel") -> Module:
    return Module(id=id, type=type, label=id, config={})


def _edges(*pairs) -> tuple[Edge, ...]:
    return tuple(_edge(f, t) for f, t in pairs)


class TestSubDagSelectors:
    def test_reachable_forward_linear_from_start(self):
        edges = _edges(("A", "B"), ("B", "C"))
        assert _reachable_forward("A", edges) == {"A", "B", "C"}

    def test_reachable_forward_linear_from_middle(self):
        edges = _edges(("A", "B"), ("B", "C"))
        assert _reachable_forward("B", edges) == {"B", "C"}

    def test_reachable_forward_fanout(self):
        edges = _edges(("A", "B"), ("A", "C"))
        assert _reachable_forward("A", edges) == {"A", "B", "C"}

    def test_reachable_backward_linear_from_end(self):
        edges = _edges(("A", "B"), ("B", "C"))
        assert _reachable_backward("C", edges) == {"A", "B", "C"}

    def test_reachable_backward_linear_from_middle(self):
        edges = _edges(("A", "B"), ("B", "C"))
        assert _reachable_backward("B", edges) == {"A", "B"}

    def test_reachable_backward_fanin(self):
        edges = _edges(("A", "C"), ("B", "C"))
        assert _reachable_backward("C", edges) == {"A", "B", "C"}

    def test_selector_included_intersection(self):
        modules = (_module("A"), _module("B"), _module("C"), _module("D"))
        edges = _edges(("A", "B"), ("B", "C"), ("C", "D"))
        result = _selector_included(modules, edges, "B", "C")
        assert result == {"B", "C"}

    def test_selector_included_from_only(self):
        modules = (_module("A"), _module("B"), _module("C"))
        edges = _edges(("A", "B"), ("B", "C"))
        result = _selector_included(modules, edges, "B", None)
        assert result == {"B", "C"}

    def test_selector_included_to_only(self):
        modules = (_module("A"), _module("B"), _module("C"))
        edges = _edges(("A", "B"), ("B", "C"))
        result = _selector_included(modules, edges, None, "B")
        assert result == {"A", "B"}

    @pytest.mark.usefixtures("spark")
    def test_executor_skips_module_not_in_included_set(self, spark, tmp_path):
        in_path = str(tmp_path / "in.parquet")
        out_path = str(tmp_path / "out.parquet")
        spark.range(5).write.parquet(in_path)

        manifest = Manifest(
            blueprint_id="test.subdag",
            modules=(
                Module(id="ing", type="Ingress", label="Ingress", config={"format": "parquet", "path": in_path}),
                Module(id="ch", type="Channel", label="Ch", config={"op": "sql", "query": "SELECT * FROM ing"}),
                Module(id="eg", type="Egress", label="Egress", config={"format": "parquet", "path": out_path}),
            ),
            edges=(
                Edge(from_id="ing", to_id="ch"),
                Edge(from_id="ch", to_id="eg"),
            ),
            context={},
            spark_config={},
        )

        result = execute(manifest, spark, to_module="ch")
        statuses = {r.module_id: r.status for r in result.module_results}
        assert statuses["ing"] == "success"
        assert statuses["ch"] == "success"
        assert statuses["eg"] == "skipped"
