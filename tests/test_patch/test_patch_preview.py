"""Tests for Phase 29a — Patch preview + validation pyramid (Gates 2 and 3)."""

from __future__ import annotations
import pytest
from pathlib import Path
from unittest.mock import MagicMock

pytestmark = pytest.mark.unit

from aqueduct.patch.preview import (
    touched_module_ids,
    _live_lineage_rows,
    run_lineage_gate,
    render_unified_diff,
)
from aqueduct.patch.grammar import PatchSpec


def _patch(*ops):
    return PatchSpec.model_validate({
        "patch_id": "test-patch",
        "rationale": "test",
        "operations": list(ops),
    })


class TestTouchedModuleIds:
    def test_set_module_config_key_returns_id(self):
        spec = _patch({"op": "set_module_config_key", "module_id": "m1", "key": "k", "value": "v"})
        assert touched_module_ids(spec) == ["m1"]

    def test_multi_ops_preserve_order_and_dedup(self):
        spec = _patch(
            {"op": "replace_module_config", "module_id": "m1", "config": {}},
            {"op": "insert_module", "module": {"id": "m2", "type": "Ingress", "config": {}}, "edges_to_add": []},
            {"op": "add_probe", "module": {"id": "p1", "type": "Probe", "attach_to": "m1", "config": {}}, "edges_to_add": []},
            {"op": "set_module_config_key", "module_id": "m1", "key": "k", "value": "v"},
        )
        assert touched_module_ids(spec) == ["m1", "m2", "p1"]

    def test_replace_context_value_returns_empty(self):
        spec = _patch({"op": "replace_context_value", "key": "ctx.v", "value": 1})
        assert touched_module_ids(spec) == []

    def test_replace_edge_returns_to_id(self):
        spec = _patch({"op": "replace_edge", "from_id": "m1", "to_id": "m2", "new_to_id": "m3"})
        # to_id is the consumer side, interesting for downstream impact
        assert touched_module_ids(spec) == ["m2"]


class TestLiveLineageRows:
    def test_empty_blueprint_returns_empty(self):
        assert _live_lineage_rows({}) == []

    def test_sql_channel_lineage(self):
        bp = {
            "modules": [
                {
                    "id": "ch1",
                    "type": "Channel",
                    "config": {"op": "sql", "query": "SELECT a, b FROM upstream"}
                }
            ],
            "edges": [{"from": "m1", "to": "ch1"}]
        }
        # m1 is the upstream for ch1. sqlglot should resolve 'upstream' to 'm1' if unique
        rows = _live_lineage_rows(bp)
        assert len(rows) == 2
        assert {r["output_column"] for r in rows} == {"a", "b"}

    def test_select_all_returns_wildcard(self):
        bp = {
            "modules": [
                {
                    "id": "ch1",
                    "type": "Channel",
                    "config": {"op": "sql", "query": "SELECT * FROM __input__"}
                }
            ],
            "edges": [{"from": "m1", "to": "ch1"}]
        }
        rows = _live_lineage_rows(bp)
        assert len(rows) == 1
        assert rows[0]["output_column"] == "*"


    def test_multi_input_channel_resolves_upstream(self):
        bp = {
            "modules": [
                {"id": "m1", "type": "Ingress", "config": {"format": "parquet", "path": "p1"}},
                {"id": "m2", "type": "Ingress", "config": {"format": "parquet", "path": "p2"}},
                {
                    "id": "ch1",
                    "type": "Channel",
                    "config": {"op": "sql", "query": "SELECT m1.a, m2.b FROM m1 JOIN m2 ON m1.id = m2.id"}
                }
            ],
            "edges": [{"from": "m1", "to": "ch1"}, {"from": "m2", "to": "ch1"}]
        }
        rows = _live_lineage_rows(bp)
        assert len(rows) == 2
        sources = {(r["source_table"], r["source_column"]) for r in rows}
        assert ("m1", "a") in sources
        assert ("m2", "b") in sources

    def test_no_sql_channels_returns_empty(self):
        bp = {"modules": [{"id": "m1", "type": "Ingress", "config": {}}]}
        assert _live_lineage_rows(bp) == []


class TestGate2Lineage:
    def test_no_change_passes(self):
        bp = {
            "modules": [{"id": "ch1", "type": "Channel", "config": {"op": "sql", "query": "SELECT a FROM m1"}}],
            "edges": [{"from": "m1", "to": "ch1"}]
        }
        spec = _patch({"op": "set_module_config_key", "module_id": "ch1", "key": "label", "value": "New"})
        result = run_lineage_gate(bp, bp, spec)
        assert result.status == "pass"
        assert not result.warnings

    def test_rename_column_consumed_downstream_warns(self):
        bp_before = {
            "modules": [
                {"id": "m1", "type": "Ingress", "config": {"format": "parquet", "path": "p"}},
                {"id": "ch1", "type": "Channel", "config": {"op": "sql", "query": "SELECT a FROM m1"}},
                {"id": "ch2", "type": "Channel", "config": {"op": "sql", "query": "SELECT a FROM ch1"}}
            ],
            "edges": [
                {"from": "m1", "to": "ch1"},
                {"from": "ch1", "to": "ch2"}
            ]
        }
        # Patch ch1 to return 'b' instead of 'a'
        bp_after = {
            "modules": [
                {"id": "m1", "type": "Ingress", "config": {"format": "parquet", "path": "p"}},
                {"id": "ch1", "type": "Channel", "config": {"op": "sql", "query": "SELECT a AS b FROM m1"}},
                {"id": "ch2", "type": "Channel", "config": {"op": "sql", "query": "SELECT a FROM ch1"}}
            ],
            "edges": [
                {"from": "m1", "to": "ch1"},
                {"from": "ch1", "to": "ch2"}
            ]
        }
        spec = _patch({"op": "replace_module_config", "module_id": "ch1", "config": {"op": "sql", "query": "SELECT a AS b FROM m1"}})
        result = run_lineage_gate(bp_before, bp_after, spec)
        assert result.status == "warn"
        assert len(result.warnings) == 1
        assert result.warnings[0].missing_column == "a"
        assert result.warnings[0].consumer_module == "ch2"

    def test_select_all_suppresses_false_positives(self):
        bp_before = {
            "modules": [
                {"id": "ch1", "type": "Channel", "config": {"op": "sql", "query": "SELECT a FROM m1"}},
                {"id": "ch2", "type": "Channel", "config": {"op": "sql", "query": "SELECT a FROM ch1"}}
            ],
            "edges": [{"from": "m1", "to": "ch1"}, {"from": "ch1", "to": "ch2"}]
        }
        # Patch ch1 to SELECT *
        bp_after = {
            "modules": [
                {"id": "ch1", "type": "Channel", "config": {"op": "sql", "query": "SELECT * FROM m1"}},
                {"id": "ch2", "type": "Channel", "config": {"op": "sql", "query": "SELECT a FROM ch1"}}
            ],
            "edges": [{"from": "m1", "to": "ch1"}, {"from": "ch1", "to": "ch2"}]
        }
        spec = _patch({"op": "replace_module_config", "module_id": "ch1", "config": {"op": "sql", "query": "SELECT * FROM m1"}})
        result = run_lineage_gate(bp_before, bp_after, spec)
        assert result.status == "pass" # Wildcard in NEW outputs suppresses check


from aqueduct.config import AgentConnectionConfig
from pydantic import ValidationError

class TestAgentConfigValidation:
    def test_patch_validation_modes(self):
        # Default
        assert AgentConnectionConfig().patch_validation == "full_run"
        # Explicit
        assert AgentConnectionConfig(patch_validation="sandbox").patch_validation == "sandbox"
        # Invalid
        with pytest.raises(ValidationError, match="Input should be 'full_run' or 'sandbox'"):
            AgentConnectionConfig(patch_validation="bogus")


class TestUnifiedDiff:
    def test_identical_returns_empty(self):
        bp = {"id": "test"}
        assert render_unified_diff(bp, bp) == ""

    def test_change_returns_diff(self):
        bp1 = {"id": "test", "name": "A"}
        bp2 = {"id": "test", "name": "B"}
        diff = render_unified_diff(bp1, bp2)
        assert "-name: A" in diff
        assert "+name: B" in diff

def test_patch_validation_override_logic():
    from aqueduct.config import AqueductConfig
    from aqueduct.parser.models import AgentConfig
    
    # Engine default is full_run
    cfg = AqueductConfig()
    assert cfg.agent.patch_validation == "full_run"
    
    # AgentConfig (from Blueprint) with no override
    ac = AgentConfig()
    assert ac.patch_validation is None
    
    # Logic in cli.py: override if not None
    effective = ac.patch_validation or cfg.agent.patch_validation
    assert effective == "full_run"
    
    # AgentConfig WITH override
    ac_over = AgentConfig(patch_validation="sandbox")
    assert ac_over.patch_validation == "sandbox"
    effective_over = ac_over.patch_validation or cfg.agent.patch_validation
    assert effective_over == "sandbox"


# ── Sandbox gate ───────────────────────────────────────────────────────────────

class TestSandboxGateBaseDir:
    """Phase 29a — run_sandbox_gate uses blueprint_path.parent as base_dir (not /tmp/)."""

    def test_sandbox_gate_uses_blueprint_parent_as_base_dir(self, tmp_path):
        """run_sandbox_gate passes base_dir=blueprint_parent to parse_dict."""
        bp_file = tmp_path / "blueprints" / "pipe.yml"
        bp_file.parent.mkdir(parents=True)
        bp_file.write_text("""\
aqueduct: '1.0'
id: test.bp
name: Test
modules: []
edges: []
""")
        bp_after = {
            "aqueduct": "1.0",
            "id": "test.bp",
            "name": "Test",
            "modules": [],
            "edges": [],
        }

        from unittest.mock import patch, MagicMock
        import aqueduct.executor as executor_pkg

        # Seed ExecuteError directly instead of mock.patch-ing it: patch()
        # getattr()s the original first, which would trigger the package's
        # lazy __getattr__ → Spark executor import → fails without pyspark.
        executor_pkg.ExecuteError = Exception
        try:
            with patch("aqueduct.executor.get_executor") as mock_get_exec, \
                 patch("aqueduct.parser.parser.parse_dict") as mock_parse, \
                 patch("aqueduct.compiler.compiler.compile") as mock_compile:
                from aqueduct.compiler.models import Manifest
                real_manifest = Manifest(
                    blueprint_id="test.bp", context={}, modules=(), edges=(), spark_config={},
                )
                mock_compile.return_value = real_manifest
                mock_exec_fn = MagicMock(return_value=MagicMock(status="success", module_results=()))
                mock_get_exec.return_value = mock_exec_fn

                from aqueduct.patch.preview import run_sandbox_gate
                result = run_sandbox_gate(
                    bp_after,
                    blueprint_path=bp_file,
                    patch_id="p1",
                    failed_module=None,
                    spark_session=MagicMock(),
                    sample_rows=0,
                )
        finally:
            del executor_pkg.ExecuteError  # restore lazy resolution

        assert result.status == "pass"
        call_kwargs = mock_parse.call_args[1]
        assert call_kwargs["base_dir"] == bp_file.parent

    def test_sandbox_gate_runs_whole_dag_not_from_failed_module(self, tmp_path):
        """run_sandbox_gate calls execute() without from_module — runs entire DAG."""
        bp_file = tmp_path / "blueprints" / "pipe.yml"
        bp_file.parent.mkdir(parents=True)
        bp_file.write_text("""\
aqueduct: '1.0'
id: test.bp
name: Test
modules:
  - id: src
    type: Ingress
    config: {format: parquet, path: data/in.parquet}
  - id: ch1
    type: Channel
    config: {op: sql, query: SELECT * FROM src}
edges:
  - {from: src, to: ch1}
""")
        bp_after = {
            "aqueduct": "1.0",
            "id": "test.bp",
            "name": "Test",
            "modules": [
                {"id": "src", "type": "Ingress", "config": {"format": "parquet", "path": "data/in.parquet"}},
                {"id": "ch1", "type": "Channel", "config": {"op": "sql", "query": "SELECT * FROM src"}},
            ],
            "edges": [{"from": "src", "to": "ch1"}],
        }

        from unittest.mock import patch, MagicMock
        import aqueduct.executor as executor_pkg

        # Seed ExecuteError directly — see test_sandbox_gate_uses_blueprint_parent_as_base_dir.
        executor_pkg.ExecuteError = Exception
        try:
            with patch("aqueduct.executor.get_executor") as mock_get_exec, \
                 patch("aqueduct.parser.parser.parse_dict") as mock_parse, \
                 patch("aqueduct.compiler.compiler.compile") as mock_compile:
                from aqueduct.compiler.models import Manifest
                real_manifest = Manifest(
                    blueprint_id="test.bp", context={}, modules=(), edges=(), spark_config={},
                )
                mock_compile.return_value = real_manifest
                mock_exec_fn = MagicMock(return_value=MagicMock(status="success", module_results=()))
                mock_get_exec.return_value = mock_exec_fn

                from aqueduct.patch.preview import run_sandbox_gate
                result = run_sandbox_gate(
                    bp_after,
                    blueprint_path=bp_file,
                    patch_id="p1",
                    failed_module="ch1",
                    spark_session=MagicMock(),
                    sample_rows=0,
                )
        finally:
            del executor_pkg.ExecuteError  # restore lazy resolution

        assert result.status == "pass"
        # Verify execute() was called without from_module
        _, call_kwargs = mock_exec_fn.call_args
        assert "from_module" not in call_kwargs,\
            "sandbox must run the WHOLE DAG, not from failed_module"
