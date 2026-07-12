import pytest

from aqueduct.config import AgentConnectionConfig
from aqueduct.parser.parser import parse
from aqueduct.parser.schema import AgentSchema

pytestmark = pytest.mark.unit


def test_progressive_config_accepts_values():
    # Blueprint-level schema (None = inherit)
    s = AgentSchema(progressive=True)
    assert s.progressive is True

    s = AgentSchema(progressive=False)
    assert s.progressive is False

    s = AgentSchema()
    assert s.progressive is None

    # Engine-level config (default False)
    c = AgentConnectionConfig()
    assert c.progressive is False

    c = AgentConnectionConfig(progressive=True)
    assert c.progressive is True


def test_max_chain_config_accepts_values():
    s = AgentSchema(max_chain=5)
    assert s.max_chain == 5

    s = AgentSchema()
    assert s.max_chain is None

    c = AgentConnectionConfig()
    assert c.max_chain == 3

    c = AgentConnectionConfig(max_chain=7)
    assert c.max_chain == 7


def test_max_chain_rejects_zero():
    with pytest.raises(Exception):
        AgentSchema(max_chain=0)
    with pytest.raises(Exception):
        AgentConnectionConfig(max_chain=0)


def test_blueprint_progressive_round_trips(tmp_path):
    bp_file = tmp_path / "bp.yml"
    bp_file.write_text(
        "aqueduct: '1.0'\nid: test\nname: Test\n"
        "agent:\n  progressive: true\n  max_chain: 5\n"
        "modules:\n  - id: m\n    type: Channel\n    label: M\n"
        "edges: []\n"
    )
    bp = parse(bp_file)
    assert bp.agent.progressive is True
    assert bp.agent.max_chain == 5


def test_blueprint_progressive_null_is_preserved(tmp_path):
    bp_file = tmp_path / "bp.yml"
    bp_file.write_text(
        "aqueduct: '1.0'\nid: test\nname: Test\n"
        "agent:\n  progressive: null\n"
        "modules:\n  - id: m\n    type: Channel\n    label: M\n"
        "edges: []\n"
    )
    bp = parse(bp_file)
    # None = inherit from engine config default (False)
    assert bp.agent.progressive is None
    assert bp.agent.max_chain is None


def test_agent_config_to_dict_includes_progressive(tmp_path):
    bp_file = tmp_path / "bp.yml"
    bp_file.write_text(
        "aqueduct: '1.0'\nid: test\nname: Test\n"
        "agent:\n  progressive: true\n  max_chain: 4\n"
        "modules:\n  - id: m\n    type: Channel\n    label: M\n"
        "edges: []\n"
    )
    bp = parse(bp_file)
    d = bp.agent.to_dict()
    assert d["progressive"] is True
    assert d["max_chain"] == 4


def test_resolve_agent_connection_inherits_progressive(tmp_path):
    from aqueduct.cli import resolve_agent_connection

    eng = AgentConnectionConfig(progressive=True, max_chain=6)
    r = resolve_agent_connection(eng, None)
    assert r.progressive is True
    assert r.max_chain == 6

    # Blueprint explicitly sets progressive: false (wins over engine True)
    # but leaves max_chain unset (None -> inherits engine default).
    bp_file = tmp_path / "bp.yml"
    bp_file.write_text(
        "aqueduct: '1.0'\nid: test\nname: Test\n"
        "agent:\n  progressive: false\n"
        "modules:\n  - id: m\n    type: Channel\n    label: M\n"
        "edges: []\n"
    )
    bp = parse(bp_file)
    r2 = resolve_agent_connection(eng, bp.agent)
    assert r2.progressive is False   # blueprint False wins over engine True
    assert r2.max_chain == 6         # blueprint None inherits engine default
