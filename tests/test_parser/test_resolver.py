"""Tests for the Parser layer: Tier 0 context resolution."""

from __future__ import annotations
from pathlib import Path
import pytest
from aqueduct.parser.parser import ParseError, parse

FIXTURES = Path(__file__).parent.parent / "fixtures"


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

    def test_audit_02_context_resolution(self, monkeypatch):
        """Verify recursive context resolution and profile overrides."""
        from aqueduct.compiler.compiler import compile as aq_compile
        import datetime

        monkeypatch.setenv("MY_VAR", "env_value")
        blueprint_path = FIXTURES / "audit" / "02-context-resolution" / "blueprint.yml"
        blueprint = parse(blueprint_path)
        manifest = aq_compile(blueprint, blueprint_path=blueprint_path, run_id="test-run")

        m1 = next(m for m in manifest.modules if m.id == "m1")
        assert m1.config["path"] == "/data/raw/users"
        assert m1.config["env_check"] == "env_value"
        assert m1.config["direct_env"] == "env_value"

        today = datetime.date.today().strftime("%Y-%m-%d")
        assert m1.config["tier1_check"] == today
