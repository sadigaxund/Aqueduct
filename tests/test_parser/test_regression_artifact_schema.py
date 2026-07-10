import pytest

from aqueduct.config import AgentConnectionConfig
from aqueduct.parser.parser import parse
from aqueduct.parser.schema import AgentSchema

pytestmark = pytest.mark.unit


def test_regression_artifact_config_accepts_values():
    # Blueprint-level schema (None = inherit)
    s = AgentSchema(regression_artifact=True)
    assert s.regression_artifact is True

    s = AgentSchema(regression_artifact=False)
    assert s.regression_artifact is False

    s = AgentSchema()
    assert s.regression_artifact is None

    # Engine-level config (default False)
    c = AgentConnectionConfig()
    assert c.regression_artifact is False

    c = AgentConnectionConfig(regression_artifact=True)
    assert c.regression_artifact is True


def test_blueprint_regression_artifact_round_trips(tmp_path):
    bp_file = tmp_path / "bp.yml"
    bp_file.write_text(
        "aqueduct: '1.0'\nid: test\nname: Test\n"
        "agent:\n  regression_artifact: true\n"
        "modules:\n  - id: m\n    type: Channel\n    label: M\n"
        "edges: []\n"
    )
    bp = parse(bp_file)
    assert bp.agent.regression_artifact is True


def test_blueprint_regression_artifact_null_is_preserved(tmp_path):
    bp_file = tmp_path / "bp.yml"
    bp_file.write_text(
        "aqueduct: '1.0'\nid: test\nname: Test\n"
        "agent:\n  regression_artifact: null\n"
        "modules:\n  - id: m\n    type: Channel\n    label: M\n"
        "edges: []\n"
    )
    bp = parse(bp_file)
    # None = inherit from engine config default (False)
    assert bp.agent.regression_artifact is None


def test_agent_config_to_dict_includes_regression_artifact(tmp_path):
    bp_file = tmp_path / "bp.yml"
    bp_file.write_text(
        "aqueduct: '1.0'\nid: test\nname: Test\n"
        "agent:\n  regression_artifact: true\n"
        "modules:\n  - id: m\n    type: Channel\n    label: M\n"
        "edges: []\n"
    )
    bp = parse(bp_file)
    assert bp.agent.to_dict()["regression_artifact"] is True
