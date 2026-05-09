import pytest
from aqueduct.parser.models import Module, Edge
from aqueduct.compiler.models import Manifest
from aqueduct.executor.spark.executor import (
    _topo_sort,
    _find_connected_components,
    _reachable_forward,
    _reachable_backward,
    _selector_included,
    ExecuteError
)

@pytest.fixture
def linear_dag():
    m_a = Module(id="A", type="Ingress", label="A", config={})
    m_b = Module(id="B", type="Channel", label="B", config={})
    m_c = Module(id="C", type="Egress", label="C", config={})
    edges = (
        Edge(from_id="A", to_id="B", port="main"),
        Edge(from_id="B", to_id="C", port="main"),
    )
    return (m_a, m_b, m_c), edges

def test_topo_sort_linear(linear_dag):
    modules, edges = linear_dag
    order = _topo_sort(modules, edges)
    assert [m.id for m in order] == ["A", "B", "C"]

def test_topo_sort_cycle():
    m_a = Module(id="A", type="Channel", label="A", config={})
    m_b = Module(id="B", type="Channel", label="B", config={})
    edges = (
        Edge(from_id="A", to_id="B", port="main"),
        Edge(from_id="B", to_id="A", port="main"),
    )
    with pytest.raises(ExecuteError, match="Cycle detected"):
        _topo_sort((m_a, m_b), edges)

def test_find_connected_components_disconnected():
    # A->B, C->D
    ids = {"A", "B", "C", "D"}
    edges = (
        Edge(from_id="A", to_id="B", port="main"),
        Edge(from_id="C", to_id="D", port="main"),
    )
    components = _find_connected_components(ids, edges)
    assert len(components) == 2
    # Sort for comparison
    comp_sets = sorted([sorted(list(c)) for c in components])
    assert comp_sets == [["A", "B"], ["C", "D"]]

def test_find_connected_components_signal_edge():
    # A->B (data), B->C (signal)
    ids = {"A", "B", "C"}
    edges = (
        Edge(from_id="A", to_id="B", port="main"),
        Edge(from_id="B", to_id="C", port="signal"),
    )
    components = _find_connected_components(ids, edges)
    assert len(components) == 2 # C is separate because B->C is signal-only
    comp_sets = sorted([sorted(list(c)) for c in components])
    assert comp_sets == [["A", "B"], ["C"]]

def test_reachable_forward(linear_dag):
    modules, edges = linear_dag
    assert _reachable_forward("A", edges) == {"A", "B", "C"}
    assert _reachable_forward("B", edges) == {"B", "C"}
    assert _reachable_forward("C", edges) == {"C"}

def test_reachable_forward_fan_out():
    # A->B, A->C
    edges = (
        Edge(from_id="A", to_id="B", port="main"),
        Edge(from_id="A", to_id="C", port="main"),
    )
    assert _reachable_forward("A", edges) == {"A", "B", "C"}

def test_reachable_backward(linear_dag):
    modules, edges = linear_dag
    assert _reachable_backward("C", edges) == {"A", "B", "C"}
    assert _reachable_backward("B", edges) == {"A", "B"}
    assert _reachable_backward("A", edges) == {"A"}

def test_selector_included_none(linear_dag):
    modules, edges = linear_dag
    assert _selector_included(modules, edges, None, None) is None

def test_selector_included_from(linear_dag):
    modules, edges = linear_dag
    assert _selector_included(modules, edges, "B", None) == {"B", "C"}

def test_selector_included_to(linear_dag):
    modules, edges = linear_dag
    assert _selector_included(modules, edges, None, "B") == {"A", "B"}

def test_selector_included_both(linear_dag):
    modules, edges = linear_dag
    # A->B->C, from=B, to=B -> {B}
    assert _selector_included(modules, edges, "B", "B") == {"B"}
    # from=A, to=B -> {A, B}
    assert _selector_included(modules, edges, "A", "B") == {"A", "B"}

def test_selector_included_invalid(linear_dag):
    modules, edges = linear_dag
    with pytest.raises(ExecuteError, match="not found in Manifest"):
        _selector_included(modules, edges, "X", None)
    with pytest.raises(ExecuteError, match="not found in Manifest"):
        _selector_included(modules, edges, None, "X")
