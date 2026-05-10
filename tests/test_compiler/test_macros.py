"""Tests for the Compiler layer: SQL Macros expansion."""

from __future__ import annotations
from pathlib import Path
import pytest
pytestmark = pytest.mark.unit
from aqueduct.compiler.macros import MacroError, resolve_macros, resolve_macros_in_config
from aqueduct.compiler.compiler import compile
from aqueduct.parser.parser import parse

FIXTURES = Path(__file__).parent.parent / "fixtures"


class TestSqlMacros:
    def test_resolve_macros_simple(self):
        macros = {"my_fragment": "SELECT 1"}
        text = "Query: {{ macros.my_fragment }}"
        assert resolve_macros(text, macros) == "Query: SELECT 1"

    def test_resolve_macros_parameterized(self):
        macros = {"filter": "WHERE id = {{ id_val }} AND type = {{ type_val }}"}
        text = "SELECT * FROM t {{ macros.filter(id_val=123, type_val='A') }}"
        assert resolve_macros(text, macros) == "SELECT * FROM t WHERE id = 123 AND type = A"

    def test_resolve_macros_quoted_params(self):
        macros = {"match": "name = '{{ name }}'"}
        text = "SELECT * FROM t WHERE {{ macros.match(name='Bob') }}"
        assert resolve_macros(text, macros) == "SELECT * FROM t WHERE name = 'Bob'"

    def test_resolve_macros_unknown_name(self):
        macros = {"a": "1"}
        with pytest.raises(MacroError, match="Macro 'b' is not defined"):
            resolve_macros("{{ macros.b }}", macros)

    def test_resolve_macros_missing_param(self):
        macros = {"p": "{{ x }} {{ y }}"}
        with pytest.raises(MacroError, match="parameter 'y' not supplied"):
            resolve_macros("{{ macros.p(x=1) }}", macros)

    def test_resolve_macros_in_config_recursion(self):
        macros = {"v": "123"}
        config = {
            "query": "SELECT {{ macros.v }}",
            "nested": {"val": "X: {{ macros.v }}"},
            "list": ["{{ macros.v }}", 456]
        }
        resolved = resolve_macros_in_config(config, macros)
        assert resolved["query"] == "SELECT 123"
        assert resolved["nested"]["val"] == "X: 123"
        assert resolved["list"] == ["123", 456]

    def test_full_compile_expands_macros(self, tmp_path):
        bp_text = """
aqueduct: "1.0"
id: test.macros
name: Macro Test
macros:
  base_query: "SELECT * FROM input"
  filter: "WHERE status = '{{ s }}'"
modules:
  - id: input
    type: Ingress
    label: In
    config: { format: parquet, path: /tmp/in }
  - id: filtered
    type: Channel
    label: Filtered
    config:
      op: sql
      query: "{{ macros.base_query }} {{ macros.filter(s='ACTIVE') }}"
edges:
  - from: input
    to: filtered
"""
        bp_path = tmp_path / "bp.yml"
        bp_path.write_text(bp_text)
        
        bp = parse(str(bp_path))
        manifest = compile(bp)
        
        filtered_mod = next(m for m in manifest.modules if m.id == "filtered")
        assert filtered_mod.config["query"] == "SELECT * FROM input WHERE status = 'ACTIVE'"


class TestMacroResolution:
    def test_simple_substitution(self):
        from aqueduct.compiler.macros import resolve_macros
        result = resolve_macros("SELECT * FROM t WHERE {{ macros.f }}", {"f": "x = 1"})
        assert result == "SELECT * FROM t WHERE x = 1"

    def test_parameterized_substitution(self):
        from aqueduct.compiler.macros import resolve_macros
        result = resolve_macros(
            "{{ macros.trunc(col=ts, period=day) }}",
            {"trunc": "DATE_TRUNC('{{ period }}', {{ col }})"},
        )
        assert result == "DATE_TRUNC('day', ts)"

    def test_quoted_param_value(self):
        from aqueduct.compiler.macros import resolve_macros
        result = resolve_macros(
            "{{ macros.trunc(col=ts, period='month') }}",
            {"trunc": "DATE_TRUNC('{{ period }}', {{ col }})"},
        )
        assert result == "DATE_TRUNC('month', ts)"

    def test_unknown_macro_raises(self):
        from aqueduct.compiler.macros import MacroError, resolve_macros
        with pytest.raises(MacroError, match="not defined"):
            resolve_macros("{{ macros.nonexistent }}", {"other": "x"})

    def test_missing_param_raises(self):
        from aqueduct.compiler.macros import MacroError, resolve_macros
        with pytest.raises(MacroError, match="not supplied"):
            resolve_macros("{{ macros.f(a=1) }}", {"f": "{{ a }} AND {{ b }}"})

    def test_no_macros_passthrough(self):
        from aqueduct.compiler.macros import resolve_macros
        sql = "SELECT 1"
        assert resolve_macros(sql, {}) is sql

    def test_no_tokens_passthrough(self):
        from aqueduct.compiler.macros import resolve_macros
        sql = "SELECT * FROM t"
        assert resolve_macros(sql, {"f": "x"}) == sql

    def test_resolve_in_dict(self):
        from aqueduct.compiler.macros import resolve_macros_in_config
        result = resolve_macros_in_config(
            {"query": "SELECT {{ macros.col }} FROM t"},
            {"col": "amount"},
        )
        assert result["query"] == "SELECT amount FROM t"

    def test_resolve_in_list(self):
        from aqueduct.compiler.macros import resolve_macros_in_config
        result = resolve_macros_in_config(["{{ macros.x }}", 42], {"x": "hello"})
        assert result == ["hello", 42]

    def test_resolve_non_string_passthrough(self):
        from aqueduct.compiler.macros import resolve_macros_in_config
        assert resolve_macros_in_config(99, {"x": "y"}) == 99
