"""Conditional module execution — `enabled:` field + compile-time cascade-disable."""

from pathlib import Path

import pytest

from aqueduct.compiler.compiler import CompileError
from aqueduct.compiler.compiler import compile as compiler_compile
from aqueduct.parser.parser import ParseError, parse_dict

pytestmark = pytest.mark.unit

BASE = Path(".")


def _bp(ctx_value: str = "false", **over) -> dict:
    d = {
        "aqueduct": "1.0",
        "id": "bp1",
        "name": "BP",
        "context": {"enable_enrich": ctx_value},
        "modules": [
            {"id": "raw_users", "label": "R", "type": "Ingress",
             "config": {"format": "csv", "path": "data/u.csv"}},
            {"id": "crm_events", "label": "C", "type": "Ingress",
             "enabled": "${ctx.enable_enrich}",
             "config": {"format": "csv", "path": "data/c.csv"}},
            {"id": "enriched", "label": "E", "type": "Channel",
             "config": {"steps": [{"op": "sql", "sql": "SELECT * FROM raw_users"}]}},
            {"id": "out_main", "label": "O", "type": "Egress",
             "config": {"format": "parquet", "path": "out/main", "coalesce": 1}},
            {"id": "out_enriched", "label": "O2", "type": "Egress",
             "config": {"format": "parquet", "path": "out/enr"}},
        ],
        "edges": [
            {"from": "raw_users", "to": "enriched"},
            {"from": "crm_events", "to": "enriched"},
            {"from": "raw_users", "to": "out_main"},
            {"from": "enriched", "to": "out_enriched"},
        ],
    }
    d.update(over)
    return d


class TestEnabledCascade:
    def test_disable_cascades_to_downstream_consumers(self):
        m = compiler_compile(parse_dict(_bp(), BASE))
        states = {x.id: (x.enabled, x.disabled_reason) for x in m.modules}
        assert states["crm_events"] == (False, "disabled")
        assert states["enriched"][0] is False
        assert "crm_events" in states["enriched"][1]
        assert states["out_enriched"][0] is False
        assert states["raw_users"] == (True, None)
        assert states["out_main"] == (True, None)

    def test_ctx_true_enables_everything(self):
        m = compiler_compile(parse_dict(_bp(ctx_value="true"), BASE))
        assert all(x.enabled for x in m.modules)
        assert all(x.disabled_reason is None for x in m.modules)

    def test_all_disabled_is_compile_error(self):
        d = _bp()
        for mod in d["modules"]:
            mod["enabled"] = False
        with pytest.raises(CompileError, match="All modules are disabled"):
            compiler_compile(parse_dict(d, BASE))

    def test_non_boolean_value_is_parse_error(self):
        d = _bp()
        d["modules"][1]["enabled"] = "maybe"
        with pytest.raises(ParseError, match="must resolve to a boolean"):
            parse_dict(d, BASE)

    def test_manifest_keeps_disabled_modules(self):
        """Executor needs the rows to mark ⏭ — compile must not prune them."""
        m = compiler_compile(parse_dict(_bp(), BASE))
        assert len(m.modules) == 5
        md = m.to_dict()["modules"][0]
        assert {"enabled", "disabled_reason"} <= set(md.keys())

    def test_disabled_modules_excluded_from_compile_warnings(self):
        """out_enriched (disabled, no coalesce) must not fire
        file_format_no_repartition; out_main is quiet via coalesce."""
        import warnings as W
        with W.catch_warnings(record=True) as caught:
            W.simplefilter("always")
            compiler_compile(parse_dict(_bp(), BASE))
        msgs = [str(w.message) for w in caught]
        assert not any("out_enriched" in s for s in msgs), msgs


class TestEnabledArcadePropagation:
    def test_disabled_arcade_disables_expanded_children(self, tmp_path):
        sub = tmp_path / "sub.yml"
        sub.write_text(
            """
aqueduct: "1.0"
id: sub_bp
name: SubBP
modules:
  - id: stage
    type: Channel
    label: S
    config:
      steps: [{op: sql, sql: "SELECT 1 AS x"}]
  - id: save
    type: Egress
    label: E
    config: {format: parquet, path: out/sub, coalesce: 1}
edges:
  - {from: stage, to: save}
"""
        )
        d = {
            "aqueduct": "1.0", "id": "parent", "name": "Parent",
            "modules": [
                {"id": "raw", "label": "R", "type": "Ingress",
                 "config": {"format": "csv", "path": "data/u.csv"}},
                {"id": "arc", "label": "A", "type": "Arcade",
                 "ref": "sub.yml", "enabled": False, "config": {}},
                {"id": "out", "label": "O", "type": "Egress",
                 "config": {"format": "parquet", "path": "out/p", "coalesce": 1}},
            ],
            "edges": [
                {"from": "raw", "to": "arc"},
                {"from": "raw", "to": "out"},
            ],
        }
        bp_file = tmp_path / "parent.yml"
        import yaml
        bp_file.write_text(yaml.safe_dump(d))
        from aqueduct.parser.parser import parse
        m = compiler_compile(parse(str(bp_file)), blueprint_path=bp_file)
        states = {x.id: (x.enabled, x.disabled_reason) for x in m.modules}
        assert states["arc__stage"] == (False, "arcade 'arc' disabled")
        assert states["arc__save"] == (False, "arcade 'arc' disabled")
        assert states["raw"][0] is True and states["out"][0] is True
