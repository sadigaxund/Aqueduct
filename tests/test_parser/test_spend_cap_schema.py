import pytest
from pydantic import ValidationError
from aqueduct.parser.schema import AgentSchema
from aqueduct.config import AgentConnectionConfig
from aqueduct.parser.parser import parse
from pathlib import Path

pytestmark = pytest.mark.unit

def test_agent_schema_accepts_spend_cap():
    # Valid integer
    s = AgentSchema(max_heal_attempts_per_hour=10)
    assert s.max_heal_attempts_per_hour == 10
    
    # Valid null
    s = AgentSchema(max_heal_attempts_per_hour=None)
    assert s.max_heal_attempts_per_hour is None
    
    # Invalid type
    with pytest.raises(ValidationError):
        AgentSchema(max_heal_attempts_per_hour="ten")

def test_agent_connection_config_accepts_spend_cap():
    # Valid integer
    c = AgentConnectionConfig(max_heal_attempts_per_hour=5)
    assert c.max_heal_attempts_per_hour == 5
    
    # Valid null
    c = AgentConnectionConfig(max_heal_attempts_per_hour=None)
    assert c.max_heal_attempts_per_hour is None

def test_blueprint_spend_cap_round_trips(tmp_path):
    bp_file = tmp_path / "bp.yml"
    bp_file.write_text(
        "aqueduct: '1.0'\nid: test\nname: Test\n"
        "agent:\n  max_heal_attempts_per_hour: 42\n"
        "modules:\n  - id: m\n    type: Channel\n    label: M\n"
        "edges: []\n"
    )
    bp = parse(bp_file)
    assert bp.agent.max_heal_attempts_per_hour == 42

def test_blueprint_spend_cap_null_round_trips(tmp_path):
    bp_file = tmp_path / "bp.yml"
    bp_file.write_text(
        "aqueduct: '1.0'\nid: test\nname: Test\n"
        "agent:\n  max_heal_attempts_per_hour: null\n"
        "modules:\n  - id: m\n    type: Channel\n    label: M\n"
        "edges: []\n"
    )
    bp = parse(bp_file)
    assert bp.agent.max_heal_attempts_per_hour is None
