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


def test_agent_config_to_dict_includes_all_guardrails_fields(tmp_path):
    """AgentConfig.to_dict()'s own docstring warns "forgetting to add
    [a field] here means the LLM won't see the field" — but its
    "guardrails" sub-dict serialized only forbidden_ops/allowed_paths and
    silently dropped heal_on_errors/never_heal_errors, even though
    GuardrailsConfig carries all four. The live agent PROMPT is unaffected
    (agent/loop.py reads agent_cfg.guardrails, the raw dataclass, not this
    dict) — but Manifest.to_dict() (compiler/models.py) calls THIS method
    for the manifest JSON snapshot used by report --json / manifest
    hashing / storage, where the two fields were invisible."""
    bp_file = tmp_path / "bp.yml"
    bp_file.write_text(
        "aqueduct: '1.0'\nid: test\nname: Test\n"
        "agent:\n"
        "  guardrails:\n"
        "    forbidden_ops: [remove_module]\n"
        "    allowed_paths: ['modules.*.config.*']\n"
        "    heal_on_errors: [AnalysisException]\n"
        "    never_heal_errors: [OutOfMemoryError]\n"
        "modules:\n  - id: m\n    type: Channel\n    label: M\n"
        "edges: []\n"
    )
    bp = parse(bp_file)
    d = bp.agent.to_dict()
    assert d["guardrails"]["forbidden_ops"] == ["remove_module"]
    assert d["guardrails"]["allowed_paths"] == ["modules.*.config.*"]
    assert d["guardrails"]["heal_on_errors"] == ["AnalysisException"]
    assert d["guardrails"]["never_heal_errors"] == ["OutOfMemoryError"]


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
    assert r2.progressive is False  # blueprint False wins over engine True
    assert r2.max_chain == 6  # blueprint None inherits engine default


def test_resolve_agent_connection_never_reads_connection_fields_from_blueprint(tmp_path):
    """2.59 security fix — resolve_agent_connection resolves EVERY
    connection field from the engine config alone. `AgentConfig`
    (`bp.agent`, the parsed Blueprint policy) has no provider/base_url/
    api_key/model/provider_options/timeout/cascade attributes at all any
    more, so there is nothing for a Blueprint to override even in
    principle — this test pins that as a resolved-value assertion, not
    just a schema-rejection one (test_agent_policy_split.py covers the
    rejection)."""
    from aqueduct.cli import resolve_agent_connection

    eng = AgentConnectionConfig(
        provider="anthropic",
        base_url="https://api.anthropic.example",
        api_key="sk-engine-key",
        model="claude-sonnet-4-6",
        provider_options={"temperature": 0.2},
        timeout=90.0,
    )
    bp_file = tmp_path / "bp.yml"
    bp_file.write_text(
        "aqueduct: '1.0'\nid: test\nname: Test\n"
        "agent:\n  approval: auto\n  max_patches: 2\n"
        "modules:\n  - id: m\n    type: Channel\n    label: M\n"
        "edges: []\n"
    )
    bp = parse(bp_file)
    assert not hasattr(bp.agent, "provider")
    assert not hasattr(bp.agent, "base_url")
    assert not hasattr(bp.agent, "api_key")
    assert not hasattr(bp.agent, "model")
    assert not hasattr(bp.agent, "provider_options")
    assert not hasattr(bp.agent, "timeout")
    assert not hasattr(bp.agent, "cascade")

    r = resolve_agent_connection(eng, bp.agent)
    assert r.provider == "anthropic"
    assert r.base_url == "https://api.anthropic.example"
    assert r.api_key == "sk-engine-key"
    assert r.model == "claude-sonnet-4-6"
    assert r.provider_options == {"temperature": 0.2}
    assert r.timeout == 90.0
