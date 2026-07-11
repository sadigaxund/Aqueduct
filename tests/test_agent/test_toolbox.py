"""Tests for aqueduct.agent.toolbox — the agentic-mode ToolBox (Phase 75).

Unit-level: stub manifest/failure_ctx objects, no Spark session (session-bound
tools must degrade to a structured "unavailable" result, never raise).
"""

from __future__ import annotations

import types
from unittest.mock import patch

import pytest

from aqueduct.agent.toolbox import ToolBox
from aqueduct.parser.models import ModuleType

pytestmark = pytest.mark.unit


class _FakeFailureCtx:
    def __init__(self, yaml_text="id: bp1\nmodules: []\n"):
        self.blueprint_source_yaml = yaml_text


def _manifest(modules=()):
    return types.SimpleNamespace(modules=modules, base_dir="/tmp/bp")


class TestDeclarations:
    def test_declarations_include_registry_and_own_tools(self):
        tb = ToolBox(manifest=_manifest(), failure_ctx=_FakeFailureCtx())
        names = {d["name"] for d in tb.declarations()}
        for expected in (
            "list_runs", "run_detail", "lineage", "patch_list", "patch_show",
            "probe_signals", "blueprint_history",
            "read_blueprint", "get_source_schema", "sample_rows",
        ):
            assert expected in names

    def test_doctor_excluded(self):
        # doctor is a health-probe, not a failure-diagnosis tool (module docstring).
        tb = ToolBox(manifest=_manifest(), failure_ctx=_FakeFailureCtx())
        names = {d["name"] for d in tb.declarations()}
        assert "doctor" not in names

    def test_declaration_shape_matches_registry_tool(self):
        tb = ToolBox(manifest=_manifest(), failure_ctx=_FakeFailureCtx())
        for d in tb.declarations():
            assert set(d.keys()) == {"name", "description", "params_schema"}
            assert isinstance(d["params_schema"], dict)


class TestReadBlueprint:
    def test_returns_source_yaml(self):
        tb = ToolBox(manifest=_manifest(), failure_ctx=_FakeFailureCtx("id: x\n"))
        result = tb.call("read_blueprint", {})
        assert result["available"] is True
        assert result["yaml"] == "id: x\n"
        assert result["truncated"] is False

    def test_truncates_oversized_yaml(self):
        big = "a" * 25_000
        tb = ToolBox(manifest=_manifest(), failure_ctx=_FakeFailureCtx(big))
        result = tb.call("read_blueprint", {})
        assert result["truncated"] is True
        assert len(result["yaml"]) < len(big)

    def test_unavailable_when_no_source_captured(self):
        tb = ToolBox(manifest=_manifest(), failure_ctx=_FakeFailureCtx(None))
        result = tb.call("read_blueprint", {})
        assert result["available"] is False


class TestSessionBoundTools:
    def test_get_source_schema_unavailable_without_session(self):
        tb = ToolBox(manifest=_manifest(), failure_ctx=_FakeFailureCtx(), spark_session=None)
        result = tb.call("get_source_schema", {"module_id": "src"})
        assert result["available"] is False
        assert "no Spark session" in result["reason"]

    def test_sample_rows_unavailable_without_session(self):
        tb = ToolBox(manifest=_manifest(), failure_ctx=_FakeFailureCtx(), spark_session=None)
        result = tb.call("sample_rows", {"module_id": "src", "n": 5})
        assert result["available"] is False
        assert "no Spark session" in result["reason"]

    def test_get_source_schema_unknown_module(self):
        session = object()  # any non-None sentinel — module lookup fails first
        tb = ToolBox(manifest=_manifest(), failure_ctx=_FakeFailureCtx(), spark_session=session)
        result = tb.call("get_source_schema", {"module_id": "nope"})
        assert result["available"] is False
        assert "unknown module_id" in result["reason"]

    def test_sample_rows_rejects_non_ingress_module(self):
        session = object()
        mod = types.SimpleNamespace(id="chan1", type=ModuleType.Channel)
        tb = ToolBox(manifest=_manifest((mod,)), failure_ctx=_FakeFailureCtx(), spark_session=session)
        result = tb.call("sample_rows", {"module_id": "chan1"})
        assert result["available"] is False
        assert "not an Ingress module" in result["reason"]

    def test_sample_rows_caps_n_hard_limit(self):
        session = object()
        mod = types.SimpleNamespace(id="src", type=ModuleType.Ingress)
        tb = ToolBox(manifest=_manifest((mod,)), failure_ctx=_FakeFailureCtx(), spark_session=session)

        fake_rows = [types.SimpleNamespace(asDict=lambda recursive=True: {"a": 1})] * 3

        class _FakeDF:
            def limit(self, n):
                assert n <= 20  # hard cap enforced regardless of requested n
                self._n = n
                return self

            def collect(self):
                return fake_rows[: self._n]

        with patch(
            "aqueduct.executor.spark.ingress.read_ingress",
            return_value=_FakeDF(),
        ):
            result = tb.call("sample_rows", {"module_id": "src", "n": 999})
        assert result["available"] is True
        assert result["n"] == 20


class TestRegistryDispatch:
    def test_forwards_config_path_and_store_dir_when_unset(self):
        tb = ToolBox(
            manifest=_manifest(), failure_ctx=_FakeFailureCtx(),
            config_path="/proj/aqueduct.yml", store_dir="/proj/.aqueduct",
        )
        captured = {}

        def _fake_call_tool(name, **kwargs):
            captured.update(kwargs)
            return {"ok": True}

        with patch("aqueduct.tools.registry.call_tool", _fake_call_tool):
            tb.call("list_runs", {"limit": 5})
        assert captured["config_path"] == "/proj/aqueduct.yml"
        assert captured["store_dir"] == "/proj/.aqueduct"
        assert captured["limit"] == 5

    def test_explicit_config_path_wins_over_toolbox_default(self):
        tb = ToolBox(
            manifest=_manifest(), failure_ctx=_FakeFailureCtx(),
            config_path="/proj/aqueduct.yml",
        )
        captured = {}

        def _fake_call_tool(name, **kwargs):
            captured.update(kwargs)
            return {"ok": True}

        with patch("aqueduct.tools.registry.call_tool", _fake_call_tool):
            tb.call("list_runs", {"config_path": "/other/aqueduct.yml"})
        assert captured["config_path"] == "/other/aqueduct.yml"


class TestUnknownAndFailingTools:
    def test_unknown_tool_returns_error_dict_not_raise(self):
        tb = ToolBox(manifest=_manifest(), failure_ctx=_FakeFailureCtx())
        result = tb.call("not_a_real_tool", {})
        assert "error" in result

    def test_handler_exception_is_caught_and_redacted(self):
        tb = ToolBox(manifest=_manifest(), failure_ctx=_FakeFailureCtx())
        with patch(
            "aqueduct.tools.registry.call_tool",
            side_effect=RuntimeError("boom"),
        ):
            result = tb.call("list_runs", {})
        assert "error" in result
        assert "boom" in result["error"]
