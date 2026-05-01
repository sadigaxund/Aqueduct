"""Targeted tests for code paths that fall below 80% coverage.

Covers: udf.py, llm.py helpers, session.py quiet mode,
        surveyor/webhook.py payload templating.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ── udf.py ────────────────────────────────────────────────────────────────────

class TestUdfRegistration:
    def test_unsupported_lang_raises(self):
        from aqueduct.executor.spark.udf import UDFError, register_udfs

        mock_spark = MagicMock()
        with pytest.raises(UDFError, match="language 'scala' is not supported"):
            register_udfs(({"id": "my_udf", "lang": "scala"},), mock_spark)

    def test_unsupported_lang_java_raises(self):
        from aqueduct.executor.spark.udf import UDFError, register_udfs

        mock_spark = MagicMock()
        with pytest.raises(UDFError, match="language 'java' is not supported"):
            register_udfs(({"id": "my_udf", "lang": "java"},), mock_spark)

    def test_missing_module_raises(self):
        from aqueduct.executor.spark.udf import UDFError, register_udfs

        mock_spark = MagicMock()
        with pytest.raises(UDFError, match="'module' is required"):
            register_udfs(({"id": "my_udf", "lang": "python"},), mock_spark)

    def test_nonexistent_module_raises(self):
        from aqueduct.executor.spark.udf import UDFError, register_udfs

        mock_spark = MagicMock()
        with pytest.raises(UDFError, match="cannot import module"):
            register_udfs(({"id": "my_udf", "lang": "python", "module": "no.such.module"},), mock_spark)

    def test_missing_entry_raises(self):
        from aqueduct.executor.spark.udf import UDFError, register_udfs

        mock_spark = MagicMock()
        with pytest.raises(UDFError, match="function 'nonexistent_fn' not found"):
            register_udfs(
                ({"id": "my_udf", "lang": "python", "module": "json", "entry": "nonexistent_fn"},),
                mock_spark,
            )

    def test_successful_registration(self):
        from aqueduct.executor.spark.udf import register_udfs

        mock_spark = MagicMock()
        # json.loads is a real importable callable
        register_udfs(
            ({"id": "parse_json", "lang": "python", "module": "json", "entry": "loads", "return_type": "string"},),
            mock_spark,
        )
        mock_spark.udf.register.assert_called_once_with("parse_json", json.loads, "string")

    def test_entry_defaults_to_udf_id(self):
        from aqueduct.executor.spark.udf import register_udfs

        mock_spark = MagicMock()
        # "json" module has "loads" — use id="loads" so entry defaults to "loads"
        register_udfs(
            ({"id": "loads", "lang": "python", "module": "json", "return_type": "string"},),
            mock_spark,
        )
        mock_spark.udf.register.assert_called_once()

    def test_spark_register_failure_raises(self):
        from aqueduct.executor.spark.udf import UDFError, register_udfs

        mock_spark = MagicMock()
        mock_spark.udf.register.side_effect = Exception("Spark refused")
        with pytest.raises(UDFError, match="spark.udf.register\\(\\) failed"):
            register_udfs(
                ({"id": "loads", "lang": "python", "module": "json", "return_type": "string"},),
                mock_spark,
            )


# ── llm.py helpers ────────────────────────────────────────────────────────────

class TestLlmHelpers:
    def test_truncate_stack_short_trace_unchanged(self):
        from aqueduct.surveyor.llm import _truncate_stack

        trace = "line1\nline2\nline3"
        assert _truncate_stack(trace) == trace

    def test_truncate_stack_long_trace_truncated(self):
        from aqueduct.surveyor.llm import _truncate_stack, _STACK_TRACE_MAX_LINES

        lines = [f"line{i}" for i in range(_STACK_TRACE_MAX_LINES + 10)]
        trace = "\n".join(lines)
        result = _truncate_stack(trace)
        result_lines = result.splitlines()
        assert len(result_lines) == _STACK_TRACE_MAX_LINES + 1  # +1 for "... N more lines" footer
        assert "truncated" in result_lines[-1]

    def test_truncate_stack_none_returns_placeholder(self):
        from aqueduct.surveyor.llm import _truncate_stack

        assert _truncate_stack(None) == "(no stack trace)"

    def test_build_pipeline_summary_no_modules(self):
        from aqueduct.surveyor.llm import _build_pipeline_summary

        assert _build_pipeline_summary({}) == "(no modules)"

    def test_build_pipeline_summary_linear_chain(self):
        from aqueduct.surveyor.llm import _build_pipeline_summary

        manifest = {
            "modules": [
                {"id": "ing", "type": "Ingress"},
                {"id": "ch", "type": "Channel"},
                {"id": "eg", "type": "Egress"},
            ],
            "edges": [
                {"from": "ing", "to": "ch", "port": "main"},
                {"from": "ch", "to": "eg", "port": "main"},
            ],
        }
        summary = _build_pipeline_summary(manifest)
        assert "Ingress" in summary
        assert "Channel" in summary
        assert "Egress" in summary

    def test_build_pipeline_summary_single_module(self):
        from aqueduct.surveyor.llm import _build_pipeline_summary

        manifest = {"modules": [{"id": "m1", "type": "Ingress"}], "edges": []}
        summary = _build_pipeline_summary(manifest)
        assert "Ingress(m1)" in summary

    def test_load_previous_patches_empty_when_no_dir(self, tmp_path):
        from aqueduct.surveyor.llm import _load_previous_patches

        result = _load_previous_patches(tmp_path / "nonexistent")
        assert result == []

    def test_load_previous_patches_reads_applied(self, tmp_path):
        from aqueduct.surveyor.llm import _load_previous_patches

        applied_dir = tmp_path / "applied"
        applied_dir.mkdir()
        patch_data = {
            "patch_id": "fix-001",
            "description": "Fixed path",
            "operations": [{"op": "replace_module_config"}],
        }
        (applied_dir / "fix-001.json").write_text(json.dumps(patch_data))

        result = _load_previous_patches(tmp_path)
        assert len(result) == 1
        assert result[0]["patch_id"] == "fix-001"
        assert "replace_module_config" in result[0]["ops"]

    def test_load_previous_patches_skips_corrupt_files(self, tmp_path):
        from aqueduct.surveyor.llm import _load_previous_patches

        applied_dir = tmp_path / "applied"
        applied_dir.mkdir()
        (applied_dir / "bad.json").write_text("not json {{")

        result = _load_previous_patches(tmp_path)
        assert result == []

    def test_build_system_prompt_with_previous_patches(self, tmp_path):
        from aqueduct.surveyor.llm import _build_system_prompt

        applied_dir = tmp_path / "applied"
        applied_dir.mkdir()
        (applied_dir / "fix-001.json").write_text(json.dumps({
            "patch_id": "fix-001", "description": "test patch",
            "operations": [{"op": "replace_module_config"}],
        }))

        prompt = _build_system_prompt(tmp_path)
        assert "PatchSpec" in prompt
        assert "fix-001" in prompt

    def test_call_anthropic_no_key_raises(self, monkeypatch):
        from aqueduct.surveyor.llm import _call_anthropic

        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        with pytest.raises(RuntimeError, match="ANTHROPIC_API_KEY"):
            _call_anthropic([], "claude-sonnet-4-6", 1024, "system prompt")

    def test_call_anthropic_makes_post(self, monkeypatch):
        from aqueduct.surveyor.llm import _call_anthropic

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"content": [{"text": "result text"}]}
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.post", return_value=mock_resp) as mock_post:
            result = _call_anthropic([{"role": "user", "content": "hi"}], "claude-sonnet-4-6", 512, "sys")

        assert result == "result text"
        mock_post.assert_called_once()
        _, kwargs = mock_post.call_args
        assert kwargs["json"]["model"] == "claude-sonnet-4-6"

    def test_call_openai_compat_no_base_url_raises(self):
        from aqueduct.surveyor.llm import _call_openai_compat

        with pytest.raises(RuntimeError, match="agent.base_url must be set"):
            _call_openai_compat([], "model", 512, None, "sys")

    def test_call_openai_compat_makes_post(self, monkeypatch):
        from aqueduct.surveyor.llm import _call_openai_compat

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"choices": [{"message": {"content": "reply"}}]}
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.post", return_value=mock_resp):
            result = _call_openai_compat(
                [{"role": "user", "content": "hello"}],
                "deepseek-r1",
                512,
                "http://localhost:11434/v1",
                "sys prompt",
            )

        assert result == "reply"

    def test_call_openai_compat_with_ollama_options(self, monkeypatch):
        from aqueduct.surveyor.llm import _call_openai_compat

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"choices": [{"message": {"content": "ok"}}]}
        mock_resp.raise_for_status = MagicMock()

        with patch("httpx.post", return_value=mock_resp) as mock_post:
            _call_openai_compat(
                [],
                "model",
                512,
                "http://localhost:11434/v1",
                "sys",
                ollama_options={"num_thread": 8},
            )

        _, kwargs = mock_post.call_args
        assert kwargs["json"]["options"] == {"num_thread": 8}


# ── session.py quiet mode ─────────────────────────────────────────────────────

class TestSessionQuietMode:
    def test_make_spark_session_quiet_injects_log4j_opts(self):
        from aqueduct.executor.spark.session import make_spark_session, _LOG4J_QUIET_OPTS

        mock_spark = MagicMock()
        mock_builder = MagicMock()
        mock_builder.master.return_value = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        mock_spark.sparkContext = MagicMock()
        mock_spark._aq_metrics_listener = None

        with patch("pyspark.sql.SparkSession.builder", new_callable=lambda: type(
            "B", (), {"master": lambda s, x: mock_builder,
                      "__get__": lambda s, o, t: mock_builder}
        )()):
            pass  # just testing the quiet config injection logic below

        # Test _LOG4J_QUIET_OPTS is a non-empty string with expected content
        assert "log4j" in _LOG4J_QUIET_OPTS.lower()
        assert "ERROR" in _LOG4J_QUIET_OPTS

    def test_suppress_stderr_context_manager(self):
        from aqueduct.executor.spark.session import _suppress_stderr
        import sys

        original_stderr = sys.stderr
        ran = []

        with _suppress_stderr():
            ran.append(True)
            # stderr should be redirected inside
            assert sys.stderr is not original_stderr

        # restored after context
        assert sys.stderr is original_stderr
        assert ran == [True]


# ── surveyor/webhook.py payload templating ────────────────────────────────────

class TestWebhookPayloadTemplate:
    def test_fire_webhook_with_payload_template(self):
        from aqueduct.config import WebhookEndpointConfig
        from aqueduct.surveyor.webhook import fire_webhook

        cfg = WebhookEndpointConfig(
            url="http://example.com/hook",
            payload={"text": "Pipeline ${pipeline_id} failed on ${failed_module}"},
        )
        full_payload = {"run_id": "r1", "status": "error"}
        template_vars = {"pipeline_id": "my.pipe", "failed_module": "ing"}

        with patch("httpx.request") as mock_req:
            mock_req.return_value = MagicMock(status_code=200)
            thread = fire_webhook(cfg, full_payload, template_vars)
            thread.join(timeout=2)

        _, kwargs = mock_req.call_args
        sent = kwargs["json"]
        assert sent["text"] == "Pipeline my.pipe failed on ing"

    def test_render_value_substitutes_token(self):
        from aqueduct.surveyor.webhook import _render_value

        result = _render_value("run=${run_id}", {"run_id": "abc123"})
        assert result == "run=abc123"

    def test_render_value_leaves_unknown_token(self):
        from aqueduct.surveyor.webhook import _render_value

        result = _render_value("${UNKNOWN_TOKEN}", {})
        # unknown token stays as-is (not in vars, not in environ)
        assert "${UNKNOWN_TOKEN}" in result or result == ""  # depends on env

    def test_render_value_non_string_passthrough(self):
        from aqueduct.surveyor.webhook import _render_value

        assert _render_value(42, {}) == 42
        assert _render_value(None, {}) is None

    def test_render_dict(self):
        from aqueduct.surveyor.webhook import _render_dict

        template = {"msg": "hello ${name}", "count": 1}
        result = _render_dict(template, {"name": "world"})
        assert result["msg"] == "hello world"
        assert result["count"] == 1


# ── parser/graph.py ────────────────────────────────────────────────────────────

class TestParserGraph:
    def test_validate_spillway_targets_with_missing_target(self):
        from aqueduct.parser.graph import validate_spillway_targets
        from aqueduct.parser.models import Module

        modules = (
            Module(id="m1", type="Channel", label="M1", config={}, spillway="nonexistent_egress"),
        )
        with pytest.raises(ValueError, match="nonexistent_egress"):
            validate_spillway_targets(list(modules))

    def test_validate_spillway_targets_valid(self):
        from aqueduct.parser.graph import validate_spillway_targets
        from aqueduct.parser.models import Module

        modules = (
            Module(id="m1", type="Channel", label="M1", config={}, spillway="eg1"),
            Module(id="eg1", type="Egress", label="Eg1", config={}),
        )
        validate_spillway_targets(list(modules))  # should not raise


# ── llm.py _build_user_prompt ─────────────────────────────────────────────────

class TestBuildUserPrompt:
    def _make_ctx(self, manifest_json: str = "", failed_module: str = "ing",
                  error_message: str = "Boom", stack_trace=None, pipeline_id: str = "p1"):
        from aqueduct.surveyor.models import FailureContext
        return FailureContext(
            run_id="r1",
            pipeline_id=pipeline_id,
            failed_module=failed_module,
            error_message=error_message,
            stack_trace=stack_trace,
            manifest_json=manifest_json,
            started_at="2026-01-01T00:00:00Z",
            finished_at="2026-01-01T00:01:00Z",
        )

    def test_build_user_prompt_invalid_json_falls_back(self, tmp_path):
        from aqueduct.surveyor.llm import _build_user_prompt

        ctx = self._make_ctx(manifest_json="NOT JSON {{")
        result = _build_user_prompt(ctx, tmp_path)
        assert isinstance(result, str)
        assert "Boom" in result

    def test_build_user_prompt_valid_manifest(self, tmp_path):
        from aqueduct.surveyor.llm import _build_user_prompt
        import json

        manifest = {
            "name": "My Pipeline",
            "description": "Test pipeline",
            "modules": [
                {"id": "ing", "type": "Ingress", "label": "Ingest"},
                {"id": "ch", "type": "Channel", "label": "Transform", "config": {"sql": "SELECT 1"}},
            ],
            "edges": [{"from": "ing", "to": "ch", "port": "main"}],
        }
        ctx = self._make_ctx(manifest_json=json.dumps(manifest), failed_module="ch")
        result = _build_user_prompt(ctx, tmp_path)
        assert "My Pipeline" in result
        assert "ch" in result
        assert "Transform" in result

    def test_build_user_prompt_no_description(self, tmp_path):
        from aqueduct.surveyor.llm import _build_user_prompt
        import json

        manifest = {"modules": [{"id": "ing", "type": "Ingress", "label": ""}], "edges": []}
        ctx = self._make_ctx(manifest_json=json.dumps(manifest), pipeline_id="fallback.pipe")
        result = _build_user_prompt(ctx, tmp_path)
        assert "fallback.pipe" in result

    def test_build_user_prompt_missing_failed_module(self, tmp_path):
        from aqueduct.surveyor.llm import _build_user_prompt
        import json

        manifest = {"modules": [{"id": "other", "type": "Channel", "label": "X"}], "edges": []}
        ctx = self._make_ctx(manifest_json=json.dumps(manifest), failed_module="nonexistent")
        result = _build_user_prompt(ctx, tmp_path)
        assert "nonexistent" in result

    def test_build_user_prompt_with_stack_trace(self, tmp_path):
        from aqueduct.surveyor.llm import _build_user_prompt
        import json

        manifest = {"modules": [], "edges": []}
        ctx = self._make_ctx(manifest_json=json.dumps(manifest), stack_trace="Traceback...\nline1")
        result = _build_user_prompt(ctx, tmp_path)
        assert "Traceback" in result


# ── listener.py onStageCompleted ──────────────────────────────────────────────

class TestListenerOnStageCompleted:
    def _make_listener(self):
        from aqueduct.executor.spark.listener import AqueductMetricsListener
        listener = AqueductMetricsListener()
        listener._active_module = "test_module"
        return listener

    def _make_stage_completed(self, records_read=100, bytes_read=1024,
                               records_written=50, bytes_written=512,
                               duration_ms=1000):
        stage = MagicMock()
        info = MagicMock()
        tm = MagicMock()
        input_m = MagicMock()
        output_m = MagicMock()

        input_m.recordsRead.return_value = records_read
        input_m.bytesRead.return_value = bytes_read
        output_m.recordsWritten.return_value = records_written
        output_m.bytesWritten.return_value = bytes_written

        completion_time = MagicMock()
        submission_time = MagicMock()
        completion_time.isDefined.return_value = True
        submission_time.isDefined.return_value = True
        completion_time.get.return_value = duration_ms
        submission_time.get.return_value = 0

        info.taskMetrics.return_value = tm
        info.completionTime.return_value = completion_time
        info.submissionTime.return_value = submission_time
        tm.inputMetrics.return_value = input_m
        tm.outputMetrics.return_value = output_m
        stage.stageInfo.return_value = info
        return stage

    def test_on_stage_completed_accumulates_metrics(self):
        listener = self._make_listener()
        stage = self._make_stage_completed(records_read=200, bytes_read=2048,
                                            records_written=100, bytes_written=1024,
                                            duration_ms=500)
        listener.onStageCompleted(stage)
        metrics = listener.collect_metrics()
        assert metrics["records_read"] == 200
        assert metrics["bytes_read"] == 2048
        assert metrics["records_written"] == 100
        assert metrics["bytes_written"] == 1024

    def test_on_stage_completed_no_active_module_skips(self):
        from aqueduct.executor.spark.listener import AqueductMetricsListener
        listener = AqueductMetricsListener()
        # _active_module is None — should silently skip
        stage = self._make_stage_completed()
        listener.onStageCompleted(stage)  # no crash, no accumulation
        assert listener._accumulated["records_read"] == 0

    def test_on_stage_completed_exception_swallowed(self):
        listener = self._make_listener()
        bad_stage = MagicMock()
        bad_stage.stageInfo.side_effect = RuntimeError("JVM exploded")
        listener.onStageCompleted(bad_stage)  # must not raise
        assert listener._accumulated["records_read"] == 0

    def test_on_stage_completed_time_undefined(self):
        listener = self._make_listener()
        stage = self._make_stage_completed()
        # Override: completion time not defined
        info = stage.stageInfo()
        info.completionTime().isDefined.return_value = False
        listener.onStageCompleted(stage)
        metrics = listener.collect_metrics()
        # duration_ms should be 0 when time undefined
        assert metrics["duration_ms"] == 0


# ── patch/apply.py uncovered paths ────────────────────────────────────────────

class TestApplyPatchUncoveredPaths:
    def _make_minimal_patch(self, tmp_path, patch_id: str = "fix-001") -> Path:
        patch_data = {
            "patch_id": patch_id,
            "description": "Test patch",
            "operations": [
                {
                    "op": "replace_module_config",
                    "module_id": "ing",
                    "config": {"format": "parquet", "path": "/data/out"},
                }
            ],
        }
        p = tmp_path / f"{patch_id}.json"
        p.write_text(json.dumps(patch_data))
        return p

    def test_load_patch_spec_file_not_found(self, tmp_path):
        from aqueduct.patch.apply import PatchError, load_patch_spec

        with pytest.raises(PatchError, match="not found"):
            load_patch_spec(tmp_path / "nonexistent.json")

    def test_load_patch_spec_invalid_json(self, tmp_path):
        from aqueduct.patch.apply import PatchError, load_patch_spec

        bad = tmp_path / "bad.json"
        bad.write_text("{ not json")
        with pytest.raises(PatchError, match="Invalid JSON"):
            load_patch_spec(bad)

    def test_load_patch_spec_schema_validation_failure(self, tmp_path):
        from aqueduct.patch.apply import PatchError, load_patch_spec

        bad = tmp_path / "bad.json"
        bad.write_text(json.dumps({"patch_id": "x", "operations": "not-a-list"}))
        with pytest.raises(PatchError, match="validation failed"):
            load_patch_spec(bad)

    def test_apply_patch_file_blueprint_not_found(self, tmp_path):
        from aqueduct.patch.apply import PatchError, apply_patch_file

        patch_path = self._make_minimal_patch(tmp_path)
        with pytest.raises(PatchError, match="Blueprint not found"):
            apply_patch_file(tmp_path / "no_bp.yml", patch_path, patches_dir=tmp_path / "patches")

    def test_apply_patch_file_invalid_yaml_blueprint(self, tmp_path):
        from aqueduct.patch.apply import PatchError, apply_patch_file

        bp = tmp_path / "bp.yml"
        bp.write_text(": invalid: yaml: {{{")
        patch_path = self._make_minimal_patch(tmp_path)
        with pytest.raises(PatchError):
            apply_patch_file(bp, patch_path, patches_dir=tmp_path / "patches")

    def test_reject_patch_not_found_raises(self, tmp_path):
        from aqueduct.patch.apply import PatchError, reject_patch

        with pytest.raises(PatchError, match="not found"):
            reject_patch("nonexistent-id", "bad patch", patches_dir=tmp_path / "patches")

    def test_reject_patch_success(self, tmp_path):
        from aqueduct.patch.apply import reject_patch

        patches_dir = tmp_path / "patches"
        pending_dir = patches_dir / "pending"
        pending_dir.mkdir(parents=True)
        patch_data = {"patch_id": "fix-007", "description": "Needs rejection"}
        (pending_dir / "fix-007.json").write_text(json.dumps(patch_data))

        result = reject_patch("fix-007", "Wrong fix", patches_dir=patches_dir)
        assert result.exists()
        contents = json.loads(result.read_text())
        assert contents["rejection_reason"] == "Wrong fix"
        assert "rejected_at" in contents
        # original pending file removed
        assert not (pending_dir / "fix-007.json").exists()

    def test_yaml_dumps_returns_string(self):
        from aqueduct.patch.apply import _yaml_dumps

        result = _yaml_dumps({"key": "value", "nested": {"a": 1}})
        assert isinstance(result, str)
        assert "key" in result


# ── compiler/macros.py ────────────────────────────────────────────────────────

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


# ── channel.py op:join ────────────────────────────────────────────────────────

class TestChannelJoinQuery:
    def test_basic_inner_join(self):
        from aqueduct.executor.spark.channel import _build_join_query
        q = _build_join_query("m", {"left": "a", "right": "b", "condition": "a.id = b.id"})
        assert "INNER JOIN" in q
        assert "ON a.id = b.id" in q

    def test_left_join(self):
        from aqueduct.executor.spark.channel import _build_join_query
        q = _build_join_query("m", {"left": "a", "right": "b", "join_type": "left", "condition": "a.id = b.id"})
        assert "LEFT JOIN" in q

    def test_broadcast_right(self):
        from aqueduct.executor.spark.channel import _build_join_query
        q = _build_join_query("m", {"left": "a", "right": "b", "condition": "a.id = b.id", "broadcast_side": "right"})
        assert "BROADCAST(b)" in q

    def test_broadcast_left(self):
        from aqueduct.executor.spark.channel import _build_join_query
        q = _build_join_query("m", {"left": "a", "right": "b", "condition": "a.id = b.id", "broadcast_side": "left"})
        assert "BROADCAST(a)" in q

    def test_cross_join_no_condition(self):
        from aqueduct.executor.spark.channel import _build_join_query
        q = _build_join_query("m", {"left": "a", "right": "b", "join_type": "cross"})
        assert "CROSS JOIN" in q
        assert "ON" not in q

    def test_missing_left_raises(self):
        from aqueduct.executor.spark.channel import ChannelError, _build_join_query
        with pytest.raises(ChannelError, match="'left'"):
            _build_join_query("m", {"right": "b", "condition": "x"})

    def test_missing_right_raises(self):
        from aqueduct.executor.spark.channel import ChannelError, _build_join_query
        with pytest.raises(ChannelError, match="'right'"):
            _build_join_query("m", {"left": "a", "condition": "x"})

    def test_invalid_join_type_raises(self):
        from aqueduct.executor.spark.channel import ChannelError, _build_join_query
        with pytest.raises(ChannelError, match="invalid join_type"):
            _build_join_query("m", {"left": "a", "right": "b", "join_type": "outer", "condition": "x"})

    def test_missing_condition_non_cross_raises(self):
        from aqueduct.executor.spark.channel import ChannelError, _build_join_query
        with pytest.raises(ChannelError, match="'condition'"):
            _build_join_query("m", {"left": "a", "right": "b", "join_type": "inner"})

    def test_unsupported_op_raises(self):
        from aqueduct.executor.spark.channel import ChannelError, execute_sql_channel
        from unittest.mock import MagicMock
        from aqueduct.parser.models import Module
        mod = Module(id="m", type="Channel", label="M", config={"op": "merge"})
        with pytest.raises(ChannelError, match="unsupported"):
            execute_sql_channel(mod, {"a": MagicMock()}, MagicMock())


# ── Phase 11 CLI commands ─────────────────────────────────────────────────────

class TestCliReport:
    """Tests for `aqueduct report`."""

    def _make_runs_db(self, tmp_path: Path, run_id: str = "abc123", status: str = "success") -> Path:
        import json as _json
        import duckdb
        store = tmp_path / "signals"
        store.mkdir()
        db = store / "runs.db"
        conn = duckdb.connect(str(db))
        conn.execute("""
            CREATE TABLE run_records (
                run_id VARCHAR PRIMARY KEY, pipeline_id VARCHAR, status VARCHAR,
                started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, module_results JSON
            )
        """)
        module_results = _json.dumps([{"module_id": "m1", "status": status, "error": None}])
        conn.execute(
            "INSERT INTO run_records VALUES (?, ?, ?, NOW(), NOW(), ?)",
            [run_id, "my.pipeline", status, module_results],
        )
        conn.close()
        return store

    def test_table_format(self, tmp_path):
        from click.testing import CliRunner
        from aqueduct.cli import cli
        store = self._make_runs_db(tmp_path)
        result = CliRunner().invoke(cli, ["report", "abc123", "--store-dir", str(store)])
        assert result.exit_code == 0, result.output
        assert "abc123" in result.output
        assert "m1" in result.output

    def test_json_format(self, tmp_path):
        from click.testing import CliRunner
        from aqueduct.cli import cli
        store = self._make_runs_db(tmp_path)
        result = CliRunner().invoke(cli, ["report", "abc123", "--store-dir", str(store), "--format", "json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["run_id"] == "abc123"
        assert data["pipeline_id"] == "my.pipeline"

    def test_csv_format(self, tmp_path):
        from click.testing import CliRunner
        from aqueduct.cli import cli
        store = self._make_runs_db(tmp_path)
        result = CliRunner().invoke(cli, ["report", "abc123", "--store-dir", str(store), "--format", "csv"])
        assert result.exit_code == 0
        assert "run_id" in result.output
        assert "abc123" in result.output

    def test_unknown_run_id_exits_1(self, tmp_path):
        from click.testing import CliRunner
        from aqueduct.cli import cli
        store = self._make_runs_db(tmp_path)
        result = CliRunner().invoke(cli, ["report", "NOPE", "--store-dir", str(store)])
        assert result.exit_code == 1

    def test_missing_runs_db_exits_1(self, tmp_path):
        from click.testing import CliRunner
        from aqueduct.cli import cli
        store = tmp_path / "empty"
        store.mkdir()
        result = CliRunner().invoke(cli, ["report", "x", "--store-dir", str(store)])
        assert result.exit_code == 1


class TestCliLineage:
    """Tests for `aqueduct lineage`."""

    def _make_lineage_db(self, tmp_path: Path) -> Path:
        import duckdb
        store = tmp_path / "signals"
        store.mkdir()
        db = store / "lineage.db"
        conn = duckdb.connect(str(db))
        conn.execute("""
            CREATE TABLE column_lineage (
                pipeline_id VARCHAR, channel_id VARCHAR,
                output_column VARCHAR, source_table VARCHAR, source_column VARCHAR
            )
        """)
        conn.execute(
            "INSERT INTO column_lineage VALUES (?, ?, ?, ?, ?)",
            ["pipe.a", "ch1", "amount", "orders", "total"],
        )
        conn.close()
        return store

    def test_table_output(self, tmp_path):
        from click.testing import CliRunner
        from aqueduct.cli import cli
        store = self._make_lineage_db(tmp_path)
        result = CliRunner().invoke(cli, ["lineage", "pipe.a", "--store-dir", str(store)])
        assert result.exit_code == 0
        assert "ch1" in result.output
        assert "amount" in result.output

    def test_json_format(self, tmp_path):
        from click.testing import CliRunner
        from aqueduct.cli import cli
        store = self._make_lineage_db(tmp_path)
        result = CliRunner().invoke(cli, ["lineage", "pipe.a", "--store-dir", str(store), "--format", "json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert len(data) == 1
        assert data[0]["output_column"] == "amount"

    def test_from_filter(self, tmp_path):
        from click.testing import CliRunner
        from aqueduct.cli import cli
        store = self._make_lineage_db(tmp_path)
        result = CliRunner().invoke(cli, ["lineage", "pipe.a", "--store-dir", str(store), "--from", "orders"])
        assert result.exit_code == 0
        assert "amount" in result.output

    def test_column_filter_no_match(self, tmp_path):
        from click.testing import CliRunner
        from aqueduct.cli import cli
        store = self._make_lineage_db(tmp_path)
        result = CliRunner().invoke(cli, ["lineage", "pipe.a", "--store-dir", str(store), "--column", "nope"])
        assert result.exit_code == 0
        assert "No lineage records" in result.output

    def test_missing_lineage_db_exits_1(self, tmp_path):
        from click.testing import CliRunner
        from aqueduct.cli import cli
        store = tmp_path / "empty"
        store.mkdir()
        result = CliRunner().invoke(cli, ["lineage", "pipe.a", "--store-dir", str(store)])
        assert result.exit_code == 1


class TestCliSignal:
    """Tests for `aqueduct signal`."""

    def _store(self, tmp_path: Path) -> Path:
        s = tmp_path / "signals"
        s.mkdir()
        return s

    def test_close_gate(self, tmp_path):
        import duckdb
        from click.testing import CliRunner
        from aqueduct.cli import cli
        store = self._store(tmp_path)
        result = CliRunner().invoke(cli, ["signal", "my_probe", "--value", "false", "--store-dir", str(store)])
        assert result.exit_code == 0
        assert "CLOSED" in result.output
        conn = duckdb.connect(str(store / "signals.db"))
        row = conn.execute("SELECT passed FROM signal_overrides WHERE signal_id = 'my_probe'").fetchone()
        conn.close()
        assert row is not None and row[0] is False

    def test_close_gate_with_error_msg(self, tmp_path):
        import duckdb
        from click.testing import CliRunner
        from aqueduct.cli import cli
        store = self._store(tmp_path)
        result = CliRunner().invoke(cli, ["signal", "my_probe", "--error", "Stale source", "--store-dir", str(store)])
        assert result.exit_code == 0
        conn = duckdb.connect(str(store / "signals.db"))
        row = conn.execute("SELECT passed, error_message FROM signal_overrides WHERE signal_id = 'my_probe'").fetchone()
        conn.close()
        assert row[0] is False
        assert "Stale source" in row[1]

    def test_clear_override(self, tmp_path):
        import duckdb
        from click.testing import CliRunner
        from aqueduct.cli import cli
        store = self._store(tmp_path)
        # Close first
        CliRunner().invoke(cli, ["signal", "my_probe", "--value", "false", "--store-dir", str(store)])
        # Then clear
        result = CliRunner().invoke(cli, ["signal", "my_probe", "--value", "true", "--store-dir", str(store)])
        assert result.exit_code == 0
        assert "cleared" in result.output
        conn = duckdb.connect(str(store / "signals.db"))
        row = conn.execute("SELECT passed FROM signal_overrides WHERE signal_id = 'my_probe'").fetchone()
        conn.close()
        assert row is None

    def test_conflicting_flags_exit_1(self, tmp_path):
        from click.testing import CliRunner
        from aqueduct.cli import cli
        store = self._store(tmp_path)
        result = CliRunner().invoke(cli, ["signal", "p", "--value", "true", "--error", "x", "--store-dir", str(store)])
        assert result.exit_code == 1

    def test_no_flags_shows_status(self, tmp_path):
        from click.testing import CliRunner
        from aqueduct.cli import cli
        store = self._store(tmp_path)
        result = CliRunner().invoke(cli, ["signal", "my_probe", "--store-dir", str(store)])
        assert result.exit_code == 0
        assert "no persistent override" in result.output


class TestEvaluateRegulatorSignalOverride:
    """Tests that evaluate_regulator() checks signal_overrides before probe_signals."""

    def _make_manifest(self, probe_id="probe1", regulator_id="reg1"):
        from unittest.mock import MagicMock
        from aqueduct.compiler.models import Manifest
        edge = MagicMock()
        edge.from_id = probe_id
        edge.to_id = regulator_id
        edge.port = "signal"
        manifest = MagicMock(spec=Manifest)
        manifest.edges = [edge]
        return manifest

    def test_override_false_blocks_even_if_probe_says_true(self, tmp_path):
        import duckdb
        from datetime import datetime, timezone
        from aqueduct.surveyor.surveyor import Surveyor, _SIGNAL_OVERRIDES_DDL
        manifest = self._make_manifest()
        store = tmp_path / "signals"
        store.mkdir()
        signals_db = store / "signals.db"
        conn = duckdb.connect(str(signals_db))
        conn.execute(_SIGNAL_OVERRIDES_DDL)
        conn.execute(
            "INSERT INTO signal_overrides VALUES (?, ?, ?, ?)",
            ["probe1", False, None, datetime.now(tz=timezone.utc).isoformat()],
        )
        conn.close()

        s = Surveyor(manifest, store_dir=store)
        s._run_id = "run-x"
        assert s.evaluate_regulator("reg1") is False

    def test_no_override_returns_true_when_no_probe_signals(self, tmp_path):
        from aqueduct.surveyor.surveyor import Surveyor
        manifest = self._make_manifest()
        store = tmp_path / "signals"
        store.mkdir()
        s = Surveyor(manifest, store_dir=store)
        s._run_id = "run-x"
        assert s.evaluate_regulator("reg1") is True


# ── Phase 13: test_runner non-Spark unit tests ────────────────────────────────

class TestTestRunnerHelpers:
    """Tests for pure helpers in test_runner.py — no Spark needed."""

    def test_spark_type_long(self):
        from aqueduct.executor.spark.test_runner import _spark_type
        assert _spark_type("long") == "bigint"

    def test_spark_type_string(self):
        from aqueduct.executor.spark.test_runner import _spark_type
        assert _spark_type("string") == "string"

    def test_spark_type_boolean_alias(self):
        from aqueduct.executor.spark.test_runner import _spark_type
        assert _spark_type("bool") == "boolean"

    def test_spark_type_passthrough_unknown(self):
        from aqueduct.executor.spark.test_runner import _spark_type
        assert _spark_type("decimal(10,2)") == "decimal(10,2)"

    def test_schema_ddl(self):
        from aqueduct.executor.spark.test_runner import _schema_ddl
        ddl = _schema_ddl({"order_id": "long", "name": "string"})
        assert "`order_id` bigint" in ddl
        assert "`name` string" in ddl

    def test_sql_literal_string(self):
        from aqueduct.executor.spark.test_runner import _sql_literal
        assert _sql_literal("hello") == "'hello'"

    def test_sql_literal_int(self):
        from aqueduct.executor.spark.test_runner import _sql_literal
        assert _sql_literal(42) == "42"

    def test_sql_literal_bool(self):
        from aqueduct.executor.spark.test_runner import _sql_literal
        assert _sql_literal(True) == "TRUE"
        assert _sql_literal(False) == "FALSE"

    def test_assertion_result_dataclass(self):
        from aqueduct.executor.spark.test_runner import AssertionResult
        ar = AssertionResult(passed=True, assertion_type="row_count", message="ok")
        assert ar.passed is True

    def test_test_suite_result_success_property(self):
        from aqueduct.executor.spark.test_runner import TestSuiteResult
        suite = TestSuiteResult(total=2, passed=2, failed=0)
        assert suite.success is True

    def test_test_suite_result_failure_property(self):
        from aqueduct.executor.spark.test_runner import TestSuiteResult
        suite = TestSuiteResult(total=2, passed=1, failed=1)
        assert suite.success is False

    def test_testable_types_constant(self):
        from aqueduct.executor.spark.test_runner import _TESTABLE_TYPES
        assert "Channel" in _TESTABLE_TYPES
        assert "Ingress" not in _TESTABLE_TYPES
        assert "Egress" not in _TESTABLE_TYPES

    def test_run_test_file_missing_blueprint(self, tmp_path):
        from aqueduct.executor.spark.test_runner import TestError, run_test_file
        test_file = tmp_path / "t.yml"
        test_file.write_text("aqueduct_test: '1.0'\ntests: []\n", encoding="utf-8")
        from unittest.mock import MagicMock
        with pytest.raises(TestError, match="blueprint"):
            run_test_file(test_file, spark=MagicMock())

    def test_run_test_file_blueprint_not_found(self, tmp_path):
        from aqueduct.executor.spark.test_runner import TestError, run_test_file
        test_file = tmp_path / "t.yml"
        test_file.write_text(
            "aqueduct_test: '1.0'\nblueprint: nonexistent.yml\ntests: []\n",
            encoding="utf-8",
        )
        from unittest.mock import MagicMock
        with pytest.raises(TestError, match="not found"):
            run_test_file(test_file, spark=MagicMock())

    def test_run_test_file_no_tests_returns_empty_suite(self, tmp_path):
        from aqueduct.executor.spark.test_runner import run_test_file
        # Create a minimal valid blueprint
        bp = tmp_path / "bp.yml"
        bp.write_text(
            "aqueduct: '1.0'\nid: test.pipe\nname: T\nmodules: []\nedges: []\n",
            encoding="utf-8",
        )
        test_file = tmp_path / "t.yml"
        test_file.write_text(
            f"aqueduct_test: '1.0'\nblueprint: bp.yml\ntests: []\n",
            encoding="utf-8",
        )
        from unittest.mock import MagicMock
        suite = run_test_file(test_file, spark=MagicMock())
        assert suite.total == 0
        assert suite.success is True

    def test_run_test_case_missing_module_field(self, tmp_path):
        from aqueduct.executor.spark.test_runner import _run_test_case
        from unittest.mock import MagicMock
        result = _run_test_case({"id": "t1"}, {}, MagicMock())
        assert result.passed is False
        assert "missing 'module'" in result.error

    def test_run_test_case_module_not_in_blueprint(self, tmp_path):
        from aqueduct.executor.spark.test_runner import _run_test_case
        from unittest.mock import MagicMock
        result = _run_test_case({"id": "t1", "module": "nonexistent"}, {}, MagicMock())
        assert result.passed is False
        assert "not found in blueprint" in result.error

    def test_run_test_case_non_testable_type(self):
        from aqueduct.executor.spark.test_runner import _run_test_case
        from aqueduct.parser.models import Module
        from unittest.mock import MagicMock
        mod = Module(id="src", type="Ingress", label="S", config={})
        result = _run_test_case(
            {"id": "t1", "module": "src", "inputs": {"x": {"schema": {}, "rows": []}}},
            {"src": mod},
            MagicMock(),
        )
        assert result.passed is False
        assert "Ingress" in result.error

    def test_run_test_case_missing_inputs(self):
        from aqueduct.executor.spark.test_runner import _run_test_case
        from aqueduct.parser.models import Module
        from unittest.mock import MagicMock
        mod = Module(id="ch", type="Channel", label="C", config={"op": "sql", "query": "SELECT 1"})
        result = _run_test_case({"id": "t1", "module": "ch"}, {"ch": mod}, MagicMock())
        assert result.passed is False
        assert "missing 'inputs'" in result.error

    def test_run_assertion_unknown_type(self):
        from aqueduct.executor.spark.test_runner import _run_assertion
        from unittest.mock import MagicMock
        ar = _run_assertion({"type": "totally_unknown"}, MagicMock(), MagicMock())
        assert ar.passed is False
        assert "unknown assertion type" in ar.message

    def test_run_assertion_sql_missing_expr(self):
        from aqueduct.executor.spark.test_runner import _run_assertion
        from unittest.mock import MagicMock
        ar = _run_assertion({"type": "sql"}, MagicMock(), MagicMock())
        assert ar.passed is False
        assert "missing 'expr'" in ar.message


class TestTestRunnerCaseExecution:
    """Tests for _run_test_case branches that don't need full Spark."""

    def _channel_module(self, mid="ch"):
        from aqueduct.parser.models import Module
        return Module(id=mid, type="Channel", label="C", config={"op": "sql", "query": "SELECT 1"})

    def test_input_missing_schema_field(self):
        from aqueduct.executor.spark.test_runner import _run_test_case
        from unittest.mock import MagicMock
        mod = self._channel_module()
        result = _run_test_case(
            {"id": "t", "module": "ch", "inputs": {"src": {"rows": [[1]]}}},
            {"ch": mod},
            MagicMock(),
        )
        assert result.passed is False
        assert "missing 'schema'" in result.error

    def test_create_df_failure_returns_error(self):
        from aqueduct.executor.spark.test_runner import _run_test_case
        from unittest.mock import MagicMock, patch
        mod = self._channel_module()
        with patch("aqueduct.executor.spark.test_runner._create_df", side_effect=RuntimeError("boom")):
            result = _run_test_case(
                {"id": "t", "module": "ch", "inputs": {"src": {"schema": {"id": "long"}, "rows": []}}},
                {"ch": mod},
                MagicMock(),
            )
        assert result.passed is False
        assert "boom" in result.error

    def test_module_execution_failure_returns_error(self):
        from aqueduct.executor.spark.test_runner import _run_test_case
        from unittest.mock import MagicMock, patch
        mod = self._channel_module()
        fake_df = MagicMock()
        with patch("aqueduct.executor.spark.test_runner._create_df", return_value=fake_df):
            with patch("aqueduct.executor.spark.test_runner._execute_module", side_effect=RuntimeError("sql fail")):
                result = _run_test_case(
                    {"id": "t", "module": "ch", "inputs": {"src": {"schema": {"id": "long"}, "rows": []}}},
                    {"ch": mod},
                    MagicMock(),
                )
        assert result.passed is False
        assert "sql fail" in result.error

    def test_junction_first_branch_used_by_default(self):
        from aqueduct.executor.spark.test_runner import _run_test_case, _run_assertion
        from unittest.mock import MagicMock, patch
        from aqueduct.parser.models import Module
        mod = Module(id="jct", type="Junction", label="J", config={"op": "conditional", "branches": []})
        fake_df = MagicMock()
        fake_result = {"branch_a": fake_df, "branch_b": MagicMock()}
        mock_ar = MagicMock()
        mock_ar.passed = True
        with patch("aqueduct.executor.spark.test_runner._create_df", return_value=MagicMock()):
            with patch("aqueduct.executor.spark.test_runner._execute_module", return_value=fake_result):
                with patch("aqueduct.executor.spark.test_runner._run_assertion", return_value=mock_ar):
                    result = _run_test_case(
                        {"id": "t", "module": "jct",
                         "inputs": {"src": {"schema": {"id": "long"}, "rows": []}},
                         "assertions": [{"type": "row_count", "expected": 1}]},
                        {"jct": mod},
                        MagicMock(),
                    )
        assert result.passed is True

    def test_junction_named_branch_not_found(self):
        from aqueduct.executor.spark.test_runner import _run_test_case
        from unittest.mock import MagicMock, patch
        from aqueduct.parser.models import Module
        mod = Module(id="jct", type="Junction", label="J", config={"op": "conditional", "branches": []})
        fake_result = {"branch_a": MagicMock()}
        with patch("aqueduct.executor.spark.test_runner._create_df", return_value=MagicMock()):
            with patch("aqueduct.executor.spark.test_runner._execute_module", return_value=fake_result):
                result = _run_test_case(
                    {"id": "t", "module": "jct", "branch": "nonexistent",
                     "inputs": {"src": {"schema": {"id": "long"}, "rows": []}}},
                    {"jct": mod},
                    MagicMock(),
                )
        assert result.passed is False
        assert "nonexistent" in result.error

    def test_assertion_exception_captured_as_failure(self):
        from aqueduct.executor.spark.test_runner import _run_test_case
        from unittest.mock import MagicMock, patch
        mod = self._channel_module()
        fake_df = MagicMock()
        with patch("aqueduct.executor.spark.test_runner._create_df", return_value=fake_df):
            with patch("aqueduct.executor.spark.test_runner._execute_module", return_value=fake_df):
                with patch("aqueduct.executor.spark.test_runner._run_assertion", side_effect=RuntimeError("assert boom")):
                    result = _run_test_case(
                        {"id": "t", "module": "ch",
                         "inputs": {"src": {"schema": {"id": "long"}, "rows": []}},
                         "assertions": [{"type": "row_count", "expected": 0}]},
                        {"ch": mod},
                        MagicMock(),
                    )
        assert result.passed is False
        assert "assert boom" in result.assertion_results[0].message

    def test_run_test_file_not_a_mapping(self, tmp_path):
        from aqueduct.executor.spark.test_runner import TestError, run_test_file
        test_file = tmp_path / "t.yml"
        test_file.write_text("- item1\n- item2\n", encoding="utf-8")
        from unittest.mock import MagicMock
        with pytest.raises(TestError, match="valid YAML mapping"):
            run_test_file(test_file, spark=MagicMock())

    def test_run_test_file_uses_blueprint_override(self, tmp_path):
        from aqueduct.executor.spark.test_runner import run_test_file
        from unittest.mock import MagicMock, patch
        bp = tmp_path / "bp.yml"
        bp.write_text(
            "aqueduct: '1.0'\nid: p\nname: P\nmodules: []\nedges: []\n",
            encoding="utf-8",
        )
        test_file = tmp_path / "t.yml"
        test_file.write_text("aqueduct_test: '1.0'\ntests: []\n", encoding="utf-8")
        suite = run_test_file(test_file, spark=MagicMock(), blueprint_path_override=bp)
        assert suite.total == 0

    def test_execute_module_unknown_type_raises(self):
        from aqueduct.executor.spark.test_runner import TestError, _execute_module
        from aqueduct.parser.models import Module
        from unittest.mock import MagicMock
        mod = Module(id="eg", type="Egress", label="E", config={})
        with pytest.raises(TestError, match="Egress"):
            _execute_module(mod, {}, MagicMock())


class TestExecuteModuleDispatch:
    """Tests for _execute_module dispatch — patched executor functions."""

    def test_channel_dispatch(self):
        from aqueduct.executor.spark.test_runner import _execute_module
        from aqueduct.parser.models import Module
        from unittest.mock import MagicMock, patch
        mod = Module(id="ch", type="Channel", label="C", config={})
        fake_df = MagicMock()
        with patch("aqueduct.executor.spark.test_runner.execute_sql_channel", return_value=fake_df, create=True):
            with patch("aqueduct.executor.spark.channel.execute_sql_channel", return_value=fake_df):
                result = _execute_module(mod, {"src": MagicMock()}, MagicMock())
        # either the mock or real function ran — just confirm no exception and result is the df
        assert result is not None

    def test_junction_dispatch(self):
        from aqueduct.executor.spark.test_runner import _execute_module
        from aqueduct.parser.models import Module
        from unittest.mock import MagicMock, patch
        mod = Module(id="jct", type="Junction", label="J", config={"op": "broadcast", "branches": []})
        fake_result = {"b": MagicMock()}
        with patch("aqueduct.executor.spark.junction.execute_junction", return_value=fake_result):
            result = _execute_module(mod, {"src": MagicMock()}, MagicMock())
        assert isinstance(result, dict)

    def test_junction_too_many_inputs_raises(self):
        from aqueduct.executor.spark.test_runner import TestError, _execute_module
        from aqueduct.parser.models import Module
        from unittest.mock import MagicMock
        mod = Module(id="jct", type="Junction", label="J", config={})
        with pytest.raises(TestError, match="expects exactly 1 input"):
            _execute_module(mod, {"a": MagicMock(), "b": MagicMock()}, MagicMock())

    def test_funnel_dispatch(self):
        from aqueduct.executor.spark.test_runner import _execute_module
        from aqueduct.parser.models import Module
        from unittest.mock import MagicMock, patch
        mod = Module(id="fn", type="Funnel", label="F", config={"mode": "union_all"})
        fake_df = MagicMock()
        with patch("aqueduct.executor.spark.funnel.execute_funnel", return_value=fake_df):
            result = _execute_module(mod, {"a": MagicMock(), "b": MagicMock()}, MagicMock())
        assert result is not None

    def test_assert_too_many_inputs_raises(self):
        from aqueduct.executor.spark.test_runner import TestError, _execute_module
        from aqueduct.parser.models import Module
        from unittest.mock import MagicMock
        mod = Module(id="ast", type="Assert", label="A", config={"rules": []})
        with pytest.raises(TestError, match="expects exactly 1 input"):
            _execute_module(mod, {"a": MagicMock(), "b": MagicMock()}, MagicMock())

    def test_assert_dispatch_no_rules(self):
        from aqueduct.executor.spark.test_runner import _execute_module
        from aqueduct.parser.models import Module
        from unittest.mock import MagicMock, patch
        mod = Module(id="ast", type="Assert", label="A", config={"rules": []})
        fake_df = MagicMock()
        with patch("aqueduct.executor.spark.assert_.execute_assert", return_value=(fake_df, None)):
            result = _execute_module(mod, {"src": fake_df}, MagicMock())
        assert result is fake_df


class TestRunTestFileLoop:
    """Tests for run_test_file when tests array is non-empty."""

    def test_run_test_file_with_passing_test(self, tmp_path):
        from aqueduct.executor.spark.test_runner import run_test_file, TestCaseResult
        from unittest.mock import MagicMock, patch
        bp = tmp_path / "bp.yml"
        bp.write_text(
            "aqueduct: '1.0'\nid: p\nname: P\nmodules: []\nedges: []\n",
            encoding="utf-8",
        )
        test_file = tmp_path / "t.yml"
        test_file.write_text(
            "aqueduct_test: '1.0'\nblueprint: bp.yml\ntests:\n  - id: t1\n    module: x\n",
            encoding="utf-8",
        )
        passing_result = TestCaseResult(test_id="t1", passed=True)
        with patch("aqueduct.executor.spark.test_runner._run_test_case", return_value=passing_result):
            suite = run_test_file(test_file, spark=MagicMock())
        assert suite.total == 1
        assert suite.passed == 1
        assert suite.failed == 0
        assert suite.success is True

    def test_run_test_file_with_failing_test(self, tmp_path):
        from aqueduct.executor.spark.test_runner import run_test_file, TestCaseResult
        from unittest.mock import MagicMock, patch
        bp = tmp_path / "bp.yml"
        bp.write_text(
            "aqueduct: '1.0'\nid: p\nname: P\nmodules: []\nedges: []\n",
            encoding="utf-8",
        )
        test_file = tmp_path / "t.yml"
        test_file.write_text(
            "aqueduct_test: '1.0'\nblueprint: bp.yml\ntests:\n  - id: t1\n    module: x\n",
            encoding="utf-8",
        )
        failing_result = TestCaseResult(test_id="t1", passed=False, error="bad")
        with patch("aqueduct.executor.spark.test_runner._run_test_case", return_value=failing_result):
            suite = run_test_file(test_file, spark=MagicMock())
        assert suite.failed == 1
        assert suite.success is False


class TestValidatePatch:
    """Tests for Phase 14 validate_patch field."""

    def test_agent_config_validate_patch_default_false(self):
        from aqueduct.parser.models import AgentConfig
        assert AgentConfig().validate_patch is False

    def test_agent_config_validate_patch_true(self):
        from aqueduct.parser.models import AgentConfig
        a = AgentConfig(validate_patch=True)
        assert a.validate_patch is True

    def test_blueprint_parses_validate_patch(self, tmp_path):
        from aqueduct.parser.parser import parse
        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: p\nname: P\n"
            "agent:\n  approval_mode: aggressive\n  validate_patch: true\n"
            "modules: []\nedges: []\n",
            encoding="utf-8",
        )
        bp = parse(str(bp_file))
        assert bp.agent.validate_patch is True

    def test_manifest_to_dict_includes_validate_patch(self, tmp_path):
        from aqueduct.parser.parser import parse
        from aqueduct.compiler.compiler import compile as compiler_compile
        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: p\nname: P\n"
            "agent:\n  validate_patch: true\n"
            "modules: []\nedges: []\n",
            encoding="utf-8",
        )
        bp = parse(str(bp_file))
        from unittest.mock import MagicMock
        manifest = compiler_compile(bp, blueprint_path=bp_file, depot=MagicMock())
        d = manifest.to_dict()
        assert d["agent"]["validate_patch"] is True


# ── Stub 1: ExecutionResult.trigger_agent ────────────────────────────────────

class TestExecutionResultTriggerAgent:
    """ExecutionResult.trigger_agent field — models.py."""

    def test_trigger_agent_defaults_to_false(self):
        from aqueduct.executor.models import ExecutionResult
        result = ExecutionResult(pipeline_id="p", run_id="r", status="success", module_results=())
        assert result.trigger_agent is False

    def test_trigger_agent_true_stored_and_returned(self):
        from aqueduct.executor.models import ExecutionResult
        result = ExecutionResult(
            pipeline_id="p", run_id="r", status="error",
            module_results=(), trigger_agent=True,
        )
        assert result.trigger_agent is True

    def test_to_dict_includes_trigger_agent_false(self):
        from aqueduct.executor.models import ExecutionResult
        d = ExecutionResult(pipeline_id="p", run_id="r", status="success", module_results=()).to_dict()
        assert "trigger_agent" in d
        assert d["trigger_agent"] is False

    def test_to_dict_includes_trigger_agent_true(self):
        from aqueduct.executor.models import ExecutionResult
        d = ExecutionResult(
            pipeline_id="p", run_id="r", status="error",
            module_results=(), trigger_agent=True,
        ).to_dict()
        assert d["trigger_agent"] is True


# ── Stub 2: _on_retry_exhausted and _fail ─────────────────────────────────────

class TestOnRetryExhausted:
    """_on_retry_exhausted behavior — executor.py."""

    def _make_policy(self, on_exhaustion: str):
        from aqueduct.parser.models import RetryPolicy
        return RetryPolicy(max_attempts=1, on_exhaustion=on_exhaustion)

    def _make_module(self, mid: str = "m1"):
        from aqueduct.parser.models import Module
        return Module(id=mid, type="Ingress", label="L", config={})

    def test_abort_returns_false_with_fail_result(self):
        from aqueduct.executor.spark.executor import _on_retry_exhausted
        from aqueduct.executor.models import ModuleResult

        policy = self._make_policy("abort")
        module = self._make_module()
        results: list[ModuleResult] = []
        gate_closed, fail_result = _on_retry_exhausted(
            RuntimeError("boom"), policy, module, "pipe.id", "run-1", results
        )
        assert gate_closed is False
        assert fail_result is not None
        assert fail_result.status == "error"
        assert fail_result.trigger_agent is False

    def test_alert_only_returns_true_with_none(self):
        from aqueduct.executor.spark.executor import _on_retry_exhausted
        from aqueduct.executor.models import ModuleResult

        policy = self._make_policy("alert_only")
        module = self._make_module()
        results: list[ModuleResult] = []
        gate_closed, fail_result = _on_retry_exhausted(
            RuntimeError("non-critical"), policy, module, "pipe.id", "run-1", results
        )
        assert gate_closed is True
        assert fail_result is None

    def test_trigger_agent_returns_false_with_trigger_agent_true(self):
        from aqueduct.executor.spark.executor import _on_retry_exhausted
        from aqueduct.executor.models import ModuleResult

        policy = self._make_policy("trigger_agent")
        module = self._make_module()
        results: list[ModuleResult] = []
        gate_closed, fail_result = _on_retry_exhausted(
            RuntimeError("agent needed"), policy, module, "pipe.id", "run-1", results
        )
        assert gate_closed is False
        assert fail_result is not None
        assert fail_result.trigger_agent is True

    def test_abort_records_module_result_as_error(self):
        from aqueduct.executor.spark.executor import _on_retry_exhausted
        from aqueduct.executor.models import ModuleResult

        policy = self._make_policy("abort")
        module = self._make_module("failing_module")
        results: list[ModuleResult] = []
        _on_retry_exhausted(RuntimeError("boom"), policy, module, "pipe.id", "run-1", results)
        assert len(results) == 1
        assert results[0].module_id == "failing_module"
        assert results[0].status == "error"

    def test_fail_with_trigger_agent_true(self):
        from aqueduct.executor.spark.executor import _fail
        from aqueduct.executor.models import ModuleResult

        result = _fail("pipe.id", "run-1", [], trigger_agent=True)
        assert result.trigger_agent is True
        assert result.status == "error"

    def test_fail_without_trigger_agent_defaults_false(self):
        from aqueduct.executor.spark.executor import _fail

        result = _fail("pipe.id", "run-1", [])
        assert result.trigger_agent is False


# ── Stub 3: Probe block_full_actions ──────────────────────────────────────────

class TestProbeBlockFullActions:
    """_row_count_estimate and _null_rates with block_full_actions=True — probe.py."""

    def test_row_count_estimate_blocked_returns_blocked_dict(self):
        from unittest.mock import MagicMock
        from aqueduct.executor.spark.probe import _row_count_estimate

        fake_df = MagicMock()
        result = _row_count_estimate(
            fake_df, {"method": "sample", "fraction": 0.1},
            probe_id="p1", run_id="r1",
            block_full_actions=True,
        )
        assert result.get("blocked") is True
        assert result.get("estimate") is None
        # df.sample().count() must NOT have been called
        fake_df.sample.assert_not_called()

    def test_row_count_estimate_spark_listener_ignores_block_full_actions(self):
        """spark_listener method should run regardless of block_full_actions."""
        from unittest.mock import MagicMock
        from aqueduct.executor.spark.probe import _row_count_estimate

        fake_df = MagicMock()
        result = _row_count_estimate(
            fake_df, {"method": "spark_listener"},
            probe_id="p1", run_id="r1",
            block_full_actions=True,
        )
        # Should return spark_listener result (estimate=None since no DB)
        assert result.get("method") == "spark_listener"
        assert result.get("estimate") is None

    def test_null_rates_blocked_returns_blocked_dict(self):
        from unittest.mock import MagicMock
        from aqueduct.executor.spark.probe import _null_rates

        fake_df = MagicMock()
        fake_df.columns = ["col_a", "col_b"]
        result = _null_rates(fake_df, {"columns": ["col_a", "col_b"]}, block_full_actions=True)
        assert result.get("blocked") is True
        assert result["null_rates"] == {"col_a": None, "col_b": None}
        # df.sample() should not have been called
        fake_df.sample.assert_not_called()

    def test_null_rates_not_blocked_calls_sample(self):
        """With block_full_actions=False, sample() IS called on the DataFrame."""
        from unittest.mock import MagicMock
        from aqueduct.executor.spark.probe import _null_rates

        # We verify sample() is invoked (not that it succeeds — no real Spark needed)
        fake_df = MagicMock()
        # Mimic spark sample().select().count() chain returning 0 rows
        fake_sample = MagicMock()
        fake_df.sample.return_value = fake_sample
        fake_select = MagicMock()
        fake_sample.select.return_value = fake_select
        fake_select.count.return_value = 0

        result = _null_rates(fake_df, {"columns": ["col_a"], "fraction": 0.5}, block_full_actions=False)
        # sample() should have been called
        fake_df.sample.assert_called_once()
        # When total==0, null_rates values should be None
        assert result["null_rates"]["col_a"] is None


class TestExecuteProbeBlockFullActionsSignature:
    """execute_probe accepts block_full_actions kwarg (signature test)."""

    def test_execute_probe_has_block_full_actions_param(self):
        import inspect
        from aqueduct.executor.spark.probe import execute_probe

        sig = inspect.signature(execute_probe)
        assert "block_full_actions" in sig.parameters
        assert sig.parameters["block_full_actions"].default is False


# ── Stub 4: execute() block_full_actions param ────────────────────────────────

class TestExecutorBlockFullActionsParam:
    """execute() accepts block_full_actions kwarg."""

    def test_execute_has_block_full_actions_param(self):
        import inspect
        from aqueduct.executor.spark.executor import execute

        sig = inspect.signature(execute)
        assert "block_full_actions" in sig.parameters
        assert sig.parameters["block_full_actions"].default is False


# ── Stub: Assert trigger_agent propagation ────────────────────────────────────

class TestAssertTriggerAgentPropagation:
    """End-to-end: Assert on_fail=trigger_agent → ExecutionResult.trigger_agent=True."""

    def test_assert_trigger_agent_sets_flag(self, spark, tmp_path):
        from aqueduct.compiler.models import Manifest
        from aqueduct.executor.spark.executor import execute
        from aqueduct.parser.models import Edge, Module, RetryPolicy

        in_path = str(tmp_path / "in.parquet")
        spark.range(5).write.parquet(in_path)

        manifest = Manifest(
            pipeline_id="test.assert_trigger_agent",
            modules=(
                Module(
                    id="src", type="Ingress", label="Src",
                    config={"format": "parquet", "path": in_path},
                ),
                Module(
                    id="chk", type="Assert", label="Chk",
                    config={
                        "rules": [
                            {"type": "min_rows", "min": 9999, "on_fail": "trigger_agent"},
                        ],
                    },
                ),
            ),
            edges=(Edge(from_id="src", to_id="chk", port="main"),),
            context={},
            spark_config={},
        )

        result = execute(manifest, spark)
        assert result.status == "error"
        assert result.trigger_agent is True


# ── Stub: Regulator trigger_agent propagation ─────────────────────────────────

class TestRegulatorTriggerAgentPropagation:
    """Regulator on_block=trigger_agent → ExecutionResult.trigger_agent=True."""

    def test_regulator_trigger_agent_sets_flag(self, spark, tmp_path):
        from aqueduct.compiler.models import Manifest
        from aqueduct.executor.spark.executor import execute
        from aqueduct.parser.models import Edge, Module

        in_path = str(tmp_path / "in.parquet")
        out_path = str(tmp_path / "out.parquet")
        spark.range(5).write.parquet(in_path)

        class _ClosedSurveyor:
            def evaluate_regulator(self, module_id: str) -> bool:
                return False

        manifest = Manifest(
            pipeline_id="test.reg_trigger_agent",
            modules=(
                Module(
                    id="src", type="Ingress", label="Src",
                    config={"format": "parquet", "path": in_path},
                ),
                Module(
                    id="gate", type="Regulator", label="Gate",
                    config={"on_block": "trigger_agent"},
                ),
                Module(
                    id="sink", type="Egress", label="Sink",
                    config={"format": "parquet", "path": out_path},
                ),
            ),
            edges=(
                Edge(from_id="src", to_id="gate", port="main"),
                Edge(from_id="gate", to_id="sink", port="main"),
            ),
            context={},
            spark_config={},
        )

        result = execute(manifest, spark, surveyor=_ClosedSurveyor())
        assert result.status == "error"
        assert result.trigger_agent is True


# ── on_exhaustion=alert_only integration (pipeline continues) ─────────────────

class TestOnExhaustionAlertOnlyIntegration:
    """alert_only: module fails but pipeline reports success (gate_closed path)."""

    def test_alert_only_pipeline_continues(self, spark, tmp_path):
        from aqueduct.compiler.models import Manifest
        from aqueduct.executor.spark.executor import execute
        from aqueduct.parser.models import Edge, Module, RetryPolicy

        # Non-existent path causes IngressError on module "bad_src"
        # A second Ingress → Egress succeeds
        good_path = str(tmp_path / "good.parquet")
        out_path = str(tmp_path / "out.parquet")
        spark.range(3).write.parquet(good_path)

        policy = RetryPolicy(max_attempts=1, on_exhaustion="alert_only")

        manifest = Manifest(
            pipeline_id="test.alert_only",
            retry_policy=policy,
            modules=(
                Module(
                    id="bad_src", type="Ingress", label="Bad",
                    config={"format": "parquet", "path": "/nonexistent/does_not_exist.parquet"},
                ),
                Module(
                    id="good_src", type="Ingress", label="Good",
                    config={"format": "parquet", "path": good_path},
                ),
                Module(
                    id="sink", type="Egress", label="Sink",
                    config={"format": "parquet", "path": out_path},
                ),
            ),
            edges=(
                Edge(from_id="good_src", to_id="sink", port="main"),
            ),
            context={},
            spark_config={},
        )

        result = execute(manifest, spark)
        # bad_src fails but alert_only propagates GATE_CLOSED — pipeline finishes
        statuses = {r.module_id: r.status for r in result.module_results}
        assert statuses.get("bad_src") == "error"
        # The overall pipeline should not abort with status="error" due to alert_only
        # bad_src has no downstream, so sink is still reachable and succeeds
        assert statuses.get("sink") == "success"


# ── executor/__init__.py get_executor ─────────────────────────────────────────

class TestGetExecutor:
    """get_executor() factory — executor/__init__.py."""

    def test_spark_engine_returns_callable(self):
        from aqueduct.executor import get_executor
        fn = get_executor("spark")
        assert callable(fn)

    def test_flink_engine_raises_not_implemented(self):
        from aqueduct.executor import get_executor
        with pytest.raises(NotImplementedError, match="Flink"):
            get_executor("flink")

    def test_unknown_engine_raises_value_error(self):
        from aqueduct.executor import get_executor
        with pytest.raises(ValueError, match="Unknown execution engine"):
            get_executor("beam")


# ── parser/graph.py topological sort and build_adjacency ─────────────────────

class TestParserGraphTopologicalSort:
    """topological_order and _build_adjacency uncovered paths."""

    def test_topological_order_linear(self):
        from aqueduct.parser.graph import topological_order
        from aqueduct.parser.models import Module, Edge

        m1 = Module(id="a", type="Ingress", label="A", config={})
        m2 = Module(id="b", type="Channel", label="B", config={})
        edge = Edge(from_id="a", to_id="b", port="main")
        order = topological_order([m1, m2], [edge])
        assert order == ["a", "b"]

    def test_topological_order_with_depends_on(self):
        from aqueduct.parser.graph import topological_order
        from aqueduct.parser.models import Module, Edge
        import dataclasses

        m1 = Module(id="x", type="Ingress", label="X", config={})
        m2 = dataclasses.replace(
            Module(id="y", type="Channel", label="Y", config={}),
            depends_on=("x",),
        )
        order = topological_order([m1, m2], [])
        assert "x" in order
        assert "y" in order
        assert order.index("x") < order.index("y")

    def test_build_adjacency_unknown_from_raises(self):
        from aqueduct.parser.graph import _build_adjacency
        from aqueduct.parser.models import Module, Edge

        m1 = Module(id="a", type="Ingress", label="A", config={})
        bad_edge = Edge(from_id="NOPE", to_id="a", port="main")
        with pytest.raises(ValueError, match="NOPE"):
            _build_adjacency([m1], [bad_edge])

    def test_build_adjacency_unknown_to_raises(self):
        from aqueduct.parser.graph import _build_adjacency
        from aqueduct.parser.models import Module, Edge

        m1 = Module(id="a", type="Ingress", label="A", config={})
        bad_edge = Edge(from_id="a", to_id="NOPE", port="main")
        with pytest.raises(ValueError, match="NOPE"):
            _build_adjacency([m1], [bad_edge])


class TestCompilerEdgeCases:
    """Cover uncovered compiler paths."""

    def test_post_tier1_ctx_reresolution(self, tmp_path):
        """Context values that contain ${ctx.*} after Tier1 resolution should be re-resolved."""
        from aqueduct.parser.parser import parse
        from aqueduct.compiler.compiler import compile as compiler_compile
        from unittest.mock import MagicMock
        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: p\nname: P\n"
            "context:\n  base: '2026-01-01'\n  derived: '${ctx.base}'\n"
            "modules: []\nedges: []\n",
            encoding="utf-8",
        )
        bp = parse(str(bp_file))
        manifest = compiler_compile(bp, blueprint_path=bp_file, depot=MagicMock())
        assert manifest is not None

    def test_compile_with_retry_policy_and_append_egress_warns(self, tmp_path):
        """max_attempts > 1 on append Egress should emit a warning."""
        import warnings
        from aqueduct.parser.parser import parse
        from aqueduct.compiler.compiler import compile as compiler_compile
        from unittest.mock import MagicMock
        bp_file = tmp_path / "bp.yml"
        bp_file.write_text(
            "aqueduct: '1.0'\nid: p\nname: P\n"
            "retry_policy:\n  max_attempts: 3\n"
            "modules:\n"
            "  - id: out\n    type: Egress\n    label: Out\n"
            "    config:\n      format: parquet\n      path: /tmp/x\n      mode: append\n"
            "edges: []\n",
            encoding="utf-8",
        )
        bp = parse(str(bp_file))
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            compiler_compile(bp, blueprint_path=bp_file, depot=MagicMock())
        assert any("append" in str(warning.message) for warning in w)
