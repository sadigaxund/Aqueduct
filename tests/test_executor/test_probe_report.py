"""Probe `report: stdout` — pure formatting + ModuleResult.notes plumbing.

No SparkSession needed: `_stdout_report_lines` is pure and pyspark is only a
TYPE_CHECKING import in probe.py.
"""

import pytest

from aqueduct.executor.models import ExecutionResult, ModuleResult
from aqueduct.executor.spark.probe import _stdout_report_lines

pytestmark = [pytest.mark.unit, pytest.mark.spark]


class TestStdoutReportLines:
    def test_scalar_payload_is_one_line(self):
        lines = _stdout_report_lines("row_count_estimate", {"method": "sample", "estimate": 1234})
        assert lines == ["row_count_estimate: method=sample  ·  estimate=1234"]

    def test_nested_dict_expands_one_line_per_entry(self):
        lines = _stdout_report_lines("null_rates", {"sampled": True, "rates": {"email": 0.12, "name": 0.0}})
        assert lines[0] == "null_rates: sampled=True"
        assert "  rates.email: 0.12" in lines
        assert "  rates.name: 0.0" in lines

    def test_list_payload_expands_indexed(self):
        lines = _stdout_report_lines("sample_rows", {"rows": [{"a": 1}, {"a": 2}]})
        assert lines[0] == "sample_rows:"
        assert lines[1].startswith("  rows[0]:")
        assert len(lines) == 3

    def test_non_dict_payload(self):
        assert _stdout_report_lines("custom", 42) == ["custom: 42"]


class TestModuleResultNotes:
    def test_notes_default_empty_and_serialized(self):
        mr = ModuleResult(module_id="p", status="success")
        assert mr.notes == ()
        er = ExecutionResult(blueprint_id="b", run_id="r", status="success",
                             module_results=(ModuleResult(module_id="p", status="success",
                                                          notes=("row_count: 5",)),))
        assert er.to_dict()["module_results"][0]["notes"] == ["row_count: 5"]

    def test_notes_are_not_warnings(self):
        """Roll-up counts mr.warnings only — notes must stay a separate field."""
        mr = ModuleResult(module_id="p", status="success", notes=("x",))
        assert mr.warnings == ()
