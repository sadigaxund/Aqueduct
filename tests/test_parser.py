"""Tests for the Parser layer.

Covers: schema validation, Tier 0 context resolution, cycle detection,
AST immutability, and edge validation.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from aqueduct.parser.models import Blueprint, Edge, Module
from aqueduct.parser.parser import ParseError, parse

FIXTURES = Path(__file__).parent / "fixtures"


# ──────────────────────────────────────────────────────────────────────────────
# Schema validation
# ──────────────────────────────────────────────────────────────────────────────


class TestSchemaValidation:
    def test_valid_minimal_parses(self):
        bp = parse(FIXTURES / "valid_minimal.yml")
        assert bp.id == "blueprint.hello.world"
        assert bp.name == "Hello World Blueprint"
        assert len(bp.modules) == 3
        assert len(bp.edges) == 2

    def test_invalid_module_type_raises(self):
        with pytest.raises(ParseError, match="schema validation"):
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
        with pytest.raises(ParseError, match="schema validation"):
            parse(bad)

    def test_unknown_top_level_field_raises(self, tmp_path):
        bad = tmp_path / "extra_field.yml"
        bad.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\ncontext: {}\n"
            "modules:\n  - id: m\n    type: Channel\n    label: M\n"
            "edges: []\nunknown_field: oops\n"
        )
        with pytest.raises(ParseError, match="schema validation"):
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
        with pytest.raises(ParseError, match="schema validation"):
            parse(bad)

    def test_modules_field_required(self, tmp_path):
        bad = tmp_path / "no_modules.yml"
        bad.write_text("aqueduct: '1.0'\nid: test\nname: Test\ncontext: {}\nedges: []\n")
        with pytest.raises(ParseError, match="schema validation"):
            parse(bad)


# ──────────────────────────────────────────────────────────────────────────────
# Tier 0 context resolution
# ──────────────────────────────────────────────────────────────────────────────


class TestContextResolution:
    def test_env_var_default_used_when_unset(self, monkeypatch):
        monkeypatch.delenv("AQUEDUCT_ENV", raising=False)
        bp = parse(FIXTURES / "valid_minimal.yml")
        assert bp.context.values["env"] == "dev"

    def test_env_var_override(self, monkeypatch):
        monkeypatch.setenv("AQUEDUCT_ENV", "prod")
        bp = parse(FIXTURES / "valid_minimal.yml")
        assert bp.context.values["env"] == "prod"

    def test_ctx_cross_reference_resolved(self, monkeypatch):
        monkeypatch.delenv("AQUEDUCT_ENV", raising=False)
        bp = parse(FIXTURES / "valid_minimal.yml")
        assert bp.context.values["paths.input"] == "data/dev/input.parquet"
        assert bp.context.values["paths.output"] == "data/dev/output.parquet"

    def test_ctx_substituted_in_module_config(self, monkeypatch):
        monkeypatch.delenv("AQUEDUCT_ENV", raising=False)
        bp = parse(FIXTURES / "valid_minimal.yml")
        ingress = next(m for m in bp.modules if m.id == "read_input")
        assert ingress.config["path"] == "data/dev/input.parquet"

    def test_cli_overrides_take_effect(self, monkeypatch):
        monkeypatch.delenv("AQUEDUCT_ENV", raising=False)
        bp = parse(FIXTURES / "valid_minimal.yml", cli_overrides={"env": "staging"})
        assert bp.context.values["env"] == "staging"

    def test_cli_overrides_propagate_to_cross_refs(self, monkeypatch):
        monkeypatch.delenv("AQUEDUCT_ENV", raising=False)
        bp = parse(FIXTURES / "valid_minimal.yml", cli_overrides={"env": "uat"})
        assert bp.context.values["paths.input"] == "data/uat/input.parquet"

    def test_profile_overrides_context(self, monkeypatch):
        monkeypatch.delenv("AQUEDUCT_ENV", raising=False)
        bp = parse(FIXTURES / "valid_with_profile.yml", profile="prod")
        assert bp.context.values["env"] == "prod"
        assert "prod" in bp.context.values["paths.input"]

    def test_missing_env_var_no_default_raises(self, tmp_path, monkeypatch):
        monkeypatch.delenv("REQUIRED_VAR", raising=False)
        bad = tmp_path / "no_default.yml"
        bad.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            "context:\n  key: ${REQUIRED_VAR}\n"
            "modules:\n  - id: m\n    type: Channel\n    label: M\nedges: []\n"
        )
        with pytest.raises(ParseError, match="Context resolution"):
            parse(bad)

    def test_undefined_ctx_reference_raises(self, tmp_path):
        bad = tmp_path / "bad_ctx.yml"
        bad.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            "context:\n  key: ${ctx.does_not_exist}\n"
            "modules:\n  - id: m\n    type: Channel\n    label: M\nedges: []\n"
        )
        with pytest.raises(ParseError):
            parse(bad)

    def test_aqueduct_ctx_env_var(self, monkeypatch):
        monkeypatch.setenv("AQUEDUCT_CTX_ENV", "override")
        bp = parse(FIXTURES / "valid_minimal.yml")
        assert bp.context.values["env"] == "override"

    def test_nested_context_resolution(self, tmp_path):
        good = tmp_path / "nested_ctx.yml"
        good.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            "context:\n"
            "  foo:\n"
            "    bar: 'nested_value'\n"
            "  params:\n"
            "    my_val: ${ctx.foo.bar}\n"
            "modules:\n"
            "  - id: branch1\n    type: Ingress\n    label: A\n"
            "edges: []\n"
        )
        bp = parse(good)
        assert bp.context.values["params.my_val"] == "nested_value"

    def test_resolver_missing_required_env_var_raises(self, tmp_path, monkeypatch):
        monkeypatch.delenv("STRICT_VAR", raising=False)
        bad = tmp_path / "strict_env.yml"
        bad.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            "context:\n  db_url: ${STRICT_VAR}\n"
            "modules:\n"
            "  - id: m\n    type: Ingress\n    label: M\n"
            "edges: []\n"
        )
        with pytest.raises(ParseError, match="Context resolution"):
            parse(bad)


# ──────────────────────────────────────────────────────────────────────────────
# Cycle detection
# ──────────────────────────────────────────────────────────────────────────────


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


# ──────────────────────────────────────────────────────────────────────────────
# AST immutability
# ──────────────────────────────────────────────────────────────────────────────


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


# ──────────────────────────────────────────────────────────────────────────────
# Blueprint structure correctness
# ──────────────────────────────────────────────────────────────────────────────


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
        assert bp.agent.max_patches_per_run == 5


class TestCheckpointField:
    def test_blueprint_checkpoint_default_false(self, tmp_path):
        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            "modules:\n  - id: m\n    type: Channel\n    label: M\n"
            "edges: []\n"
        )
        from aqueduct.parser.parser import parse
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
        from aqueduct.parser.parser import parse
        bp = parse(bp_file)
        assert bp.checkpoint is True

    def test_module_checkpoint_default_false(self, tmp_path):
        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            "modules:\n  - id: m\n    type: Channel\n    label: M\n"
            "edges: []\n"
        )
        from aqueduct.parser.parser import parse
        bp = parse(bp_file)
        assert bp.modules[0].checkpoint is False

    def test_module_checkpoint_true_round_trips(self, tmp_path):
        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: test\nname: Test\n"
            "modules:\n  - id: m\n    type: Channel\n    label: M\n    checkpoint: true\n"
            "edges: []\n"
        )
        from aqueduct.parser.parser import parse
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
        from aqueduct.parser.parser import parse
        bp = parse(bp_file)
        manifest = cc(bp, blueprint_path=bp_file)
        assert manifest.checkpoint is True
        assert manifest.to_dict()["checkpoint"] is True
