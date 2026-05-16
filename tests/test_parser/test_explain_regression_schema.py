import pytest
from pydantic import ValidationError
from aqueduct.parser.schema import AgentSchema
from aqueduct.config import AgentConnectionConfig
from aqueduct.parser.parser import parse
from pathlib import Path

pytestmark = pytest.mark.unit

def test_explain_regression_config_accepts_values():
    # Schema
    s = AgentSchema(block_on_explain_regression=True)
    assert s.block_on_explain_regression is True
    
    s = AgentSchema(block_on_explain_regression=False)
    assert s.block_on_explain_regression is False
    
    # Engine Config
    c = AgentConnectionConfig(block_on_explain_regression=True)
    assert c.block_on_explain_regression is True

def test_blueprint_explain_regression_round_trips(tmp_path):
    bp_file = tmp_path / "bp.yml"
    bp_file.write_text(
        "aqueduct: '1.0'\nid: test\nname: Test\n"
        "agent:\n  block_on_explain_regression: true\n"
        "modules:\n  - id: m\n    type: Channel\n    label: M\n"
        "edges: []\n"
    )
    bp = parse(bp_file)
    assert bp.agent.block_on_explain_regression is True

def test_blueprint_explain_regression_null_is_preserved(tmp_path):
    bp_file = tmp_path / "bp.yml"
    bp_file.write_text(
        "aqueduct: '1.0'\nid: test\nname: Test\n"
        "agent:\n  block_on_explain_regression: null\n"
        "modules:\n  - id: m\n    type: Channel\n    label: M\n"
        "edges: []\n"
    )
    bp = parse(bp_file)
    # The default is None, which signifies "inherit from engine config"
    assert bp.agent.block_on_explain_regression is None
