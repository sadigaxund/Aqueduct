"""Tests for the Aqueduct Test Runner (aqueduct test command logic)."""

from __future__ import annotations
import json
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest
pytestmark = [pytest.mark.spark, pytest.mark.integration]

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


class TestTestRunnerCaseExecution:
    """Tests for _run_test_case branches that don't need full Spark."""

    def _channel_module(self, mid="ch"):
        from aqueduct.parser.models import Module
        return Module(id=mid, type="Channel", label="C", config={"op": "sql", "query": "SELECT 1"})

    def test_run_test_case_missing_module_field(self):
        from aqueduct.executor.spark.test_runner import _run_test_case
        from unittest.mock import MagicMock
        result = _run_test_case({"id": "t1"}, {}, MagicMock())
        assert result.passed is False
        assert "missing 'module'" in result.error

    def test_run_test_case_module_not_in_blueprint(self):
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
