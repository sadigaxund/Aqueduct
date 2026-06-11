"""Tests for the Parser layer: Schema validation, immutability, and structure."""

from __future__ import annotations
from dataclasses import FrozenInstanceError
from pathlib import Path
import pytest
from aqueduct.parser.parser import ParseError, parse

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).parent.parent / "fixtures"


class TestSchemaValidation:
    def test_valid_minimal_parses(self):
        bp = parse(FIXTURES / "valid_minimal.yml")
        assert bp.id == "blueprint.hello.world"
        assert bp.name == "Hello World Blueprint"
        assert len(bp.modules) == 3
        assert len(bp.edges) == 2

    def test_invalid_module_type_raises(self):
        with pytest.raises(ParseError, match="validation error"):
            parse(FIXTURES / "invalid_schema.yml")

    def test_invalid_yaml_syntax_raises(self, tmp_path):
        bad = tmp_path / "bad.yml"
        bad.write_text("key: [unclosed\nnext: value")
        with pytest.raises(ParseError, match="Invalid YAML"):
            parse(bad)

    def test_non_mapping_yaml_raises(self, tmp_path):
        bad = tmp_path / "list.yml"
        bad.write_text("- item1\n- item2\n")
        with pytest.raises(ParseError, match="YAML mapping"):
            parse(bad)

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(ParseError, match="not found"):
            parse(tmp_path / "does_not_exist.yml")

    def test_unsupported_version_raises(self, tmp_path):
        bad = tmp_path / "bad_version.yml"
        bad.write_text(
            "aqueduct: '2.0'\nid: test\nname: Test\ncontext: {}\nmodules:\n  - id: m\n    type: Channel\n    label: M\nedges: []\n"
        )
        with pytest.raises(ParseError, match="validation error"):
            parse(bad)

    def test_unknown_top_level_field_raises(self, tmp_path):
        bad = tmp_path / "extra_field.yml"
        bad.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\ncontext: {}\n"
            "modules:\n  - id: m\n    type: Channel\n    label: M\n"
            "edges: []\nunknown_field: oops\n"
        )
        with pytest.raises(ParseError, match="validation error"):
            parse(bad)

    def test_duplicate_module_ids_raises(self, tmp_path):
        bad = tmp_path / "dupes.yml"
        bad.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\ncontext: {}\n"
            "modules:\n"
            "  - id: dup\n    type: Channel\n    label: A\n"
            "  - id: dup\n    type: Egress\n    label: B\n"
            "edges: []\n"
        )
        with pytest.raises(ParseError, match="validation error"):
            parse(bad)

    def test_modules_field_required(self, tmp_path):
        bad = tmp_path / "no_modules.yml"
        bad.write_text("aqueduct: '1.0'\nid: test\nname: Test\ncontext: {}\nedges: []\n")
        with pytest.raises(ParseError, match="validation error"):
            parse(bad)

    def test_audit_01_strict_schema_validation(self):
        """Verify that unknown fields at top-level and module-level raise ParseError."""
        blueprint_path = FIXTURES / "audit" / "01-strict-schema-validation" / "invalid_blueprint.yml"
        with pytest.raises(ParseError) as excinfo:
            parse(blueprint_path)
        error_msg = str(excinfo.value)
        assert "unknown_field" in error_msg
        assert "unknown_module_field" in error_msg


class TestImmutability:
    def test_blueprint_is_frozen(self):
        bp = parse(FIXTURES / "valid_minimal.yml")
        with pytest.raises(FrozenInstanceError):
            bp.id = "hacked"  # type: ignore[misc]

    def test_module_is_frozen(self):
        bp = parse(FIXTURES / "valid_minimal.yml")
        with pytest.raises(FrozenInstanceError):
            bp.modules[0].id = "hacked"  # type: ignore[misc]

    def test_edge_is_frozen(self):
        bp = parse(FIXTURES / "valid_minimal.yml")
        with pytest.raises(FrozenInstanceError):
            bp.edges[0].from_id = "hacked"  # type: ignore[misc]

    def test_context_registry_is_frozen(self):
        bp = parse(FIXTURES / "valid_minimal.yml")
        with pytest.raises(FrozenInstanceError):
            bp.context.values = {}  # type: ignore[misc]

    def test_retry_policy_is_frozen(self):
        bp = parse(FIXTURES / "valid_minimal.yml")
        with pytest.raises(FrozenInstanceError):
            bp.retry_policy.max_attempts = 99  # type: ignore[misc]

    def test_modules_is_tuple(self):
        bp = parse(FIXTURES / "valid_minimal.yml")
        assert isinstance(bp.modules, tuple)

    def test_edges_is_tuple(self):
        bp = parse(FIXTURES / "valid_minimal.yml")
        assert isinstance(bp.edges, tuple)


class TestBlueprintStructure:
    def test_module_types_preserved(self):
        bp = parse(FIXTURES / "valid_minimal.yml")
        types = {m.id: m.type for m in bp.modules}
        assert types["read_input"] == "Ingress"
        assert types["passthrough"] == "Channel"
        assert types["write_output"] == "Egress"

    def test_edges_wired_correctly(self):
        bp = parse(FIXTURES / "valid_minimal.yml")
        edge_pairs = {(e.from_id, e.to_id) for e in bp.edges}
        assert ("read_input", "passthrough") in edge_pairs
        assert ("passthrough", "write_output") in edge_pairs

    def test_default_retry_policy(self):
        bp = parse(FIXTURES / "valid_minimal.yml")
        assert bp.retry_policy.max_attempts == 1
        assert bp.retry_policy.backoff_strategy == "exponential"
        assert bp.retry_policy.on_exhaustion == "trigger_agent"

    def test_default_agent_config(self):
        bp = parse(FIXTURES / "valid_minimal.yml")
        assert bp.agent.approval_mode == "disabled"
        # 1.1.0 — renamed from aggressive_max_patches → max_patches, default 1.
        assert bp.agent.max_patches == 1


class TestCheckpointField:
    def test_blueprint_checkpoint_default_false(self, tmp_path):
        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            "modules:\n  - id: m\n    type: Channel\n    label: M\n"
            "edges: []\n"
        )
        bp = parse(bp_file)
        assert bp.checkpoint is False

    def test_blueprint_checkpoint_true_round_trips(self, tmp_path):
        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            "checkpoint: true\n"
            "modules:\n  - id: m\n    type: Channel\n    label: M\n"
            "edges: []\n"
        )
        bp = parse(bp_file)
        assert bp.checkpoint is True

    def test_module_checkpoint_default_false(self, tmp_path):
        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            "modules:\n  - id: m\n    type: Channel\n    label: M\n"
            "edges: []\n"
        )
        bp = parse(bp_file)
        assert bp.modules[0].checkpoint is False

    def test_module_checkpoint_true_round_trips(self, tmp_path):
        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            "modules:\n  - id: m\n    type: Channel\n    label: M\n    checkpoint: true\n"
            "edges: []\n"
        )
        bp = parse(bp_file)
        assert bp.modules[0].checkpoint is True

    def test_manifest_checkpoint_populated_from_blueprint(self, tmp_path):
        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            "checkpoint: true\n"
            "modules:\n  - id: m\n    type: Channel\n    label: M\n"
            "edges: []\n"
        )
        from aqueduct.compiler.compiler import compile as cc
        bp = parse(bp_file)
        manifest = cc(bp, blueprint_path=bp_file)
        assert manifest.checkpoint is True
        assert manifest.to_dict()["checkpoint"] is True


class TestPromptContext:
    def test_agent_config_prompt_context_round_trips(self, tmp_path):
        """AgentConfig.prompt_context round-trips through Parser -> Blueprint.agent.prompt_context"""
        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            "agent:\n  approval_mode: auto\n  prompt_context: 'Use PySpark version 3.5.'\n"
            "modules:\n  - id: m\n    type: Channel\n    label: M\n"
            "edges: []\n"
        )
        bp = parse(bp_file)
        assert bp.agent.prompt_context == "Use PySpark version 3.5."

    def test_manifest_to_dict_includes_prompt_context(self, tmp_path):
        """Manifest.to_dict()['agent']['prompt_context'] present when set"""
        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            "agent:\n  approval_mode: auto\n  prompt_context: 'Contextual info'\n"
            "modules:\n  - id: m\n    type: Channel\n    label: M\n"
            "edges: []\n"
        )
        from aqueduct.compiler.compiler import compile as cc
        bp = parse(bp_file)
        manifest = cc(bp, blueprint_path=bp_file)
        m_dict = manifest.to_dict()
        assert "agent" in m_dict
        assert "prompt_context" in m_dict["agent"]
        assert m_dict["agent"]["prompt_context"] == "Contextual info"


class TestErrorTypesValidation:
    def test_error_types_on_non_spillway_edge_raises(self, tmp_path):
        """error_types on a main-port edge → ParseError mentioning port='spillway'."""
        bp_file = tmp_path / "bp.yml"
        bp_file.write_text("""
aqueduct: '1.0'
id: test
name: Test
modules:
  - id: src
    type: Ingress
    label: Src
    config: {format: parquet, path: data}
  - id: sink
    type: Egress
    label: Sink
    config: {format: parquet, path: out, mode: overwrite}
edges:
  - from: src
    to: sink
    error_types: ["DataQualityViolation"]
""")
        from aqueduct.parser.parser import ParseError, parse
        with pytest.raises(ParseError, match="spillway"):
            parse(str(bp_file))

    def test_error_types_on_spillway_edge_parses(self, tmp_path):
        """error_types on a spillway edge parses successfully."""
        bp_file = tmp_path / "bp.yml"
        bp_file.write_text("""
aqueduct: '1.0'
id: test
name: Test
modules:
  - id: src
    type: Ingress
    label: Src
    config: {format: parquet, path: data}
  - id: sink
    type: Egress
    label: Sink
    config: {format: parquet, path: out, mode: overwrite}
edges:
  - from: src
    to: sink
    port: spillway
    error_types: ["DataQualityViolation"]
""")
        from aqueduct.parser.parser import parse
        bp = parse(str(bp_file))
        assert len(bp.edges) == 1
        assert bp.edges[0].port == "spillway"
        assert bp.edges[0].error_types == ("DataQualityViolation",)
