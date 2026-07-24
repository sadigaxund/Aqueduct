"""Tests for the ToolBox's engine-routed diagnostic readers (Phase 78).

Covers the fix for the coupling where ``aqueduct/agent/toolbox.py`` used to
import ``aqueduct.executor.spark.ingress.read_source_schema`` directly, so a
DuckDB heal's ``get_source_schema``/``sample_rows`` tools silently read
through Spark regardless of the run's actual engine. The ToolBox now resolves
both readers via ``aqueduct.executor.protocol.get_protocol(self.engine)``.

Three things are asserted here:
  - A DuckDB-engine ToolBox reads a real DuckDB parquet fixture via the
    DuckDB engine's own reader (``aqueduct.executor.duckdb_.schema_reader``),
    never via ``aqueduct.executor.spark.ingress``.
  - The Spark path is unchanged: engine="spark" (the ToolBox default) still
    routes through the Spark reader, exercised with a fake ingress module
    (no real pyspark import needed — the Spark protocol wrapper's import is
    intercepted via ``sys.modules``, same technique ``test_toolbox.py`` uses).
  - An engine registered with no reader (``read_source_schema``/
    ``sample_source_rows`` both ``None``) degrades to the SAME structured
    "unavailable" shape the ToolBox already returns when there is no live
    session — never a crash, never a silent empty result.
"""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from aqueduct.agent.toolbox import ToolBox
from aqueduct.executor.protocol import ExecutorProtocol, PromptRules, DeferRules, register_protocol
from aqueduct.parser.models import ModuleType

pytestmark = pytest.mark.unit


class _FakeFailureCtx:
    def __init__(self, yaml_text="id: bp1\nmodules: []\n"):
        self.blueprint_source_yaml = yaml_text


def _manifest(modules=()):
    return types.SimpleNamespace(modules=modules, base_dir="/tmp/bp")


def _minimal_prompt_rules() -> PromptRules:
    return PromptRules(
        persona="You are a test-engine repair agent.",
        root_cause_note="the test engine's structured root-cause block",
        rules="- test rule",
        defer=DeferRules(infra_examples="test infra failure", udf_languages="Python"),
    )


class TestDuckDBRoutesThroughDuckDB:
    def test_get_source_schema_reads_via_duckdb_not_spark(self, tmp_path):
        parquet_path = tmp_path / "src.parquet"
        con = duckdb.connect(":memory:")
        con.execute(
            "COPY (SELECT 1 AS a, 'x' AS b) TO ? (FORMAT PARQUET)", [str(parquet_path)],
        )

        mod = types.SimpleNamespace(
            id="src", type=ModuleType.Ingress,
            config={"format": "parquet", "path": str(parquet_path)},
        )
        tb = ToolBox(
            manifest=_manifest((mod,)), failure_ctx=_FakeFailureCtx(),
            spark_session=con, engine="duckdb",
        )

        # The spark ingress module must never be imported on this path —
        # poison it so the test fails loudly if the old hardcoded coupling
        # (or a regression of it) is still live.
        poisoned = MagicMock()
        poisoned.read_source_schema.side_effect = AssertionError(
            "spark ingress reader must not be called for a duckdb-engine ToolBox"
        )
        with patch.dict(sys.modules, {"aqueduct.executor.spark.ingress": poisoned}):
            result = tb.call("get_source_schema", {"module_id": "src"})

        assert result["available"] is True
        assert result["schema"] == {"a": "INTEGER", "b": "VARCHAR"}
        poisoned.read_source_schema.assert_not_called()

    def test_sample_rows_reads_real_rows_via_duckdb(self, tmp_path):
        parquet_path = tmp_path / "src2.parquet"
        con = duckdb.connect(":memory:")
        con.execute(
            "COPY (SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(n, s)) "
            "TO ? (FORMAT PARQUET)",
            [str(parquet_path)],
        )

        mod = types.SimpleNamespace(
            id="src", type=ModuleType.Ingress,
            config={"format": "parquet", "path": str(parquet_path)},
        )
        tb = ToolBox(
            manifest=_manifest((mod,)), failure_ctx=_FakeFailureCtx(),
            spark_session=con, engine="duckdb",
        )
        result = tb.call("sample_rows", {"module_id": "src", "n": 2})

        assert result["available"] is True
        assert result["n"] == 2
        assert len(result["rows"]) == 2
        assert set(result["rows"][0].keys()) == {"n", "s"}


class TestSparkPathUnchanged:
    def test_get_source_schema_still_routes_through_spark_for_spark_engine(self):
        session = object()
        mod = types.SimpleNamespace(id="src", type=ModuleType.Ingress)
        # Default engine is "spark" — omit `engine=` entirely to prove the
        # pre-existing call sites (which never passed `engine=`) still work.
        tb = ToolBox(manifest=_manifest((mod,)), failure_ctx=_FakeFailureCtx(), spark_session=session)

        mock_ingress = MagicMock()
        mock_ingress.read_source_schema.return_value = {"col": "string"}
        with patch.dict(sys.modules, {"aqueduct.executor.spark.ingress": mock_ingress}):
            result = tb.call("get_source_schema", {"module_id": "src"})

        assert result["available"] is True
        assert result["schema"] == {"col": "string"}
        mock_ingress.read_source_schema.assert_called_once_with(mod, session)

    def test_sample_rows_still_routes_through_spark_for_spark_engine(self):
        session = object()
        mod = types.SimpleNamespace(id="src", type=ModuleType.Ingress)
        tb = ToolBox(
            manifest=_manifest((mod,)), failure_ctx=_FakeFailureCtx(),
            spark_session=session, engine="spark",
        )

        fake_rows = [types.SimpleNamespace(asDict=lambda recursive=True: {"a": 1})] * 2

        class _FakeDF:
            def limit(self, n):
                self._n = n
                return self

            def collect(self):
                return fake_rows[: self._n]

        mock_ingress = MagicMock()
        mock_ingress.read_ingress.return_value = _FakeDF()
        with patch.dict(sys.modules, {"aqueduct.executor.spark.ingress": mock_ingress}):
            result = tb.call("sample_rows", {"module_id": "src", "n": 2})

        assert result["available"] is True
        assert result["n"] == 2
        assert len(result["rows"]) == 2


class TestEngineWithNoReaderDegradesGracefully:
    def test_get_source_schema_unavailable_when_engine_has_no_reader(self):
        register_protocol(ExecutorProtocol(
            engine="_test_no_reader_engine",
            execute=lambda *a, **k: None,
            extract_error=lambda exc: None,
            prompt_rules=_minimal_prompt_rules(),
            # read_source_schema / sample_source_rows both default to None.
        ))
        session = object()
        mod = types.SimpleNamespace(id="src", type=ModuleType.Ingress)
        tb = ToolBox(
            manifest=_manifest((mod,)), failure_ctx=_FakeFailureCtx(),
            spark_session=session, engine="_test_no_reader_engine",
        )

        result = tb.call("get_source_schema", {"module_id": "src"})
        assert result["available"] is False
        assert "does not support get_source_schema" in result["reason"]

    def test_sample_rows_unavailable_when_engine_has_no_reader(self):
        register_protocol(ExecutorProtocol(
            engine="_test_no_reader_engine_2",
            execute=lambda *a, **k: None,
            extract_error=lambda exc: None,
            prompt_rules=_minimal_prompt_rules(),
        ))
        session = object()
        mod = types.SimpleNamespace(id="src", type=ModuleType.Ingress)
        tb = ToolBox(
            manifest=_manifest((mod,)), failure_ctx=_FakeFailureCtx(),
            spark_session=session, engine="_test_no_reader_engine_2",
        )

        result = tb.call("sample_rows", {"module_id": "src", "n": 3})
        assert result["available"] is False
        assert "does not support sample_rows" in result["reason"]

    def test_no_session_degrades_with_engine_labelled_reason(self):
        tb = ToolBox(
            manifest=_manifest(), failure_ctx=_FakeFailureCtx(),
            spark_session=None, engine="duckdb",
        )
        result = tb.call("get_source_schema", {"module_id": "src"})
        assert result["available"] is False
        assert "no duckdb session" in result["reason"]
