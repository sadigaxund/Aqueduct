"""Tests for the Parser layer: Tier 0 context resolution."""

from __future__ import annotations
from pathlib import Path
import pytest
from aqueduct.parser.parser import ParseError, parse

pytestmark = pytest.mark.unit

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

    # ── Reserved-deferred carve-out: ${ctx._watermark} ────────────────────────

    def test_sub_ctx_watermark_not_in_map_preserved_verbatim(self):
        """${ctx._watermark} absent from ctx_map → token preserved, no error."""
        from aqueduct.parser.resolver import _sub_ctx
        out = _sub_ctx("ts > ${ctx._watermark}", {})
        assert out == "ts > ${ctx._watermark}"

    def test_sub_ctx_unknown_non_reserved_still_raises(self):
        """Non-reserved unknown ${ctx.foo} still raises (carve-out is exact-set)."""
        from aqueduct.parser.resolver import _sub_ctx
        with pytest.raises(ValueError, match=r"Undefined context reference: \$\{ctx\.foo\}"):
            _sub_ctx("${ctx.foo}", {})

    def test_sub_ctx_preserves_watermark_resolves_others_same_string(self):
        """${ctx._watermark} preserved while a real ${ctx.*} key resolves in one string."""
        from aqueduct.parser.resolver import _sub_ctx
        out = _sub_ctx("p=${ctx.real} w=${ctx._watermark}", {"real": "R"})
        assert out == "p=R w=${ctx._watermark}"

    def test_incremental_watermark_token_survives_to_manifest(self, tmp_path):
        """End-to-end: incremental Blueprint with ${ctx._watermark} parses+compiles;
        Manifest Channel query still holds the literal token."""
        from aqueduct.compiler.compiler import compile as aq_compile

        bp_file = tmp_path / "inc_wm.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\n"
            "id: inc_wm\n"
            "name: Inc WM\n"
            "modules:\n"
            "  - id: src\n    type: Ingress\n    label: Src\n"
            "    config: {format: parquet, path: /tmp/in}\n"
            "  - id: inc\n    type: Channel\n    label: Inc\n"
            "    config:\n"
            "      op: sql\n"
            "      materialize: incremental\n"
            "      watermark_column: ts\n"
            "      query: \"SELECT * FROM src WHERE ts > ${ctx._watermark}\"\n"
            "edges:\n"
            "  - from: src\n    to: inc\n"
        )
        bp = parse(bp_file)
        manifest = aq_compile(bp, blueprint_path=bp_file, run_id="test-run")
        inc = next(m for m in manifest.modules if m.id == "inc")
        assert "${ctx._watermark}" in inc.config["query"]

    # ── ISSUE-027: Tier-0 resolution in spark_config + macros ─────────────────

    def test_spark_config_env_default_applied(self, tmp_path, monkeypatch):
        """spark_config ${MY_PKG:-default} → default applied when env unset."""
        monkeypatch.delenv("MY_PKG", raising=False)
        bp_file = tmp_path / "sc_env.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: sc\nname: SC\n"
            "spark_config:\n"
            "  spark.jars.packages: \"${MY_PKG:-org.example:pkg:1.0}\"\n"
            "modules:\n  - id: m\n    type: Ingress\n    label: M\n"
            "edges: []\n"
        )
        bp = parse(bp_file)
        assert bp.spark_config["spark.jars.packages"] == "org.example:pkg:1.0"

    def test_macros_env_resolved_jinja_placeholder_untouched(self, tmp_path, monkeypatch):
        """macros: ${AQ_REGION:-US} resolved; {{ param }} left for the compiler."""
        monkeypatch.delenv("AQ_REGION", raising=False)
        bp_file = tmp_path / "mac.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: mac\nname: Mac\n"
            "macros:\n"
            "  region_filter: \"country = '${AQ_REGION:-US}' AND id = {{ id }}\"\n"
            "modules:\n  - id: m\n    type: Ingress\n    label: M\n"
            "edges: []\n"
        )
        bp = parse(bp_file)
        assert bp.macros["region_filter"] == "country = 'US' AND id = {{ id }}"

    def test_spark_config_ctx_resolves_from_context_map(self, tmp_path):
        """${ctx.key} in spark_config resolves from the context map."""
        good = tmp_path / "sc_ctx_ok.yml"
        good.write_text(
            "aqueduct: '1.0'\nid: scok\nname: SCOK\n"
            "context:\n  warehouse: /data/wh\n"
            "spark_config:\n"
            "  spark.sql.warehouse.dir: \"${ctx.warehouse}\"\n"
            "modules:\n  - id: m\n    type: Ingress\n    label: M\n"
            "edges: []\n"
        )
        bp = parse(good)
        assert bp.spark_config["spark.sql.warehouse.dir"] == "/data/wh"

    def test_spark_config_undefined_ctx_raises_parseerror(self, tmp_path):
        """Undefined non-reserved ${ctx.x} in spark_config → ParseError (ISSUE-027 fixed)."""
        bad = tmp_path / "sc_ctx_bad.yml"
        bad.write_text(
            "aqueduct: '1.0'\nid: scbad\nname: SCBAD\n"
            "spark_config:\n"
            "  spark.sql.warehouse.dir: \"${ctx.does_not_exist}\"\n"
            "modules:\n  - id: m\n    type: Ingress\n    label: M\n"
            "edges: []\n"
        )
        with pytest.raises(ParseError):
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
