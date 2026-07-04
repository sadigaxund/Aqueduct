import pytest

from aqueduct.parser.models import Edge, Module, ModuleType
from aqueduct.compiler.wirer import WireError, validate_probe_source_edges

pytestmark = pytest.mark.unit


def _modules():
    return [
        Module(id="session_metrics", type=ModuleType.Ingress, label="M", config={}),
        Module(
            id="probe_signal_gate",
            type=ModuleType.Probe,
            label="P",
            config={},
            attach_to="session_metrics",
        ),
        Module(id="quality_gate", type=ModuleType.Regulator, label="R", config={}),
    ]


def test_probe_as_main_port_source_raises():
    modules = _modules()
    edges = [
        Edge(from_id="session_metrics", to_id="probe_signal_gate"),
        Edge(from_id="probe_signal_gate", to_id="quality_gate"),
    ]
    with pytest.raises(WireError, match=r"probe_signal_gate.*cannot be a data source"):
        validate_probe_source_edges(modules, edges)


def test_probe_signal_port_edge_is_allowed():
    modules = _modules()
    edges = [
        Edge(from_id="session_metrics", to_id="probe_signal_gate"),
        Edge(from_id="probe_signal_gate", to_id="quality_gate", port="signal"),
    ]
    assert validate_probe_source_edges(modules, edges) is None


def test_no_probe_edges_is_a_no_op():
    modules = [
        Module(id="a", type=ModuleType.Ingress, label="A", config={}),
        Module(id="b", type=ModuleType.Egress, label="B", config={}),
    ]
    edges = [Edge(from_id="a", to_id="b")]
    assert validate_probe_source_edges(modules, edges) is None
