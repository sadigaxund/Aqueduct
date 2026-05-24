"""Tests for Surveyor data models."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError

import pytest
pytestmark = pytest.mark.unit

from aqueduct.surveyor.models import FailureContext, RunRecord


def test_run_record_frozen():
    record = RunRecord(
        run_id="r1",
        blueprint_id="p1",
        status="running",
        started_at="2024-01-01T00:00:00Z",
        finished_at=None,
        module_results=()
    )
    with pytest.raises(FrozenInstanceError):
        record.status = "success"  # type: ignore[misc]


def test_run_record_to_dict():
    record = RunRecord(
        run_id="r1",
        blueprint_id="p1",
        status="success",
        started_at="2024-01-01T00:00:00Z",
        finished_at="2024-01-01T00:01:00Z",
        module_results=({"id": "m1", "status": "success"},)
    )
    d = record.to_dict()
    assert d["run_id"] == "r1"
    assert d["status"] == "success"
    assert d["module_results"] == [{"id": "m1", "status": "success"}]


def test_failure_context_frozen():
    ctx = FailureContext(
        run_id="r1",
        blueprint_id="p1",
        failed_module="m1",
        error_message="oops",
        stack_trace=None,
        manifest_json="{}",
        started_at="2024-01-01T00:00:00Z",
        finished_at="2024-01-01T00:01:00Z"
    )
    with pytest.raises(FrozenInstanceError):
        ctx.error_message = "hacked"  # type: ignore[misc]


def test_failure_context_to_dict():
    ctx = FailureContext(
        run_id="r1",
        blueprint_id="p1",
        failed_module="m1",
        error_message="oops",
        stack_trace="trace",
        manifest_json='{"id": "p1"}',
        started_at="2024-01-01T00:00:00Z",
        finished_at="2024-01-01T00:01:00Z"
    )
    d = ctx.to_dict()
    assert d["run_id"] == "r1"
    assert d["failed_module"] == "m1"
    assert d["stack_trace"] == "trace"
    assert d["manifest_json"] == '{"id": "p1"}'


def test_failure_context_to_json():
    ctx = FailureContext(
        run_id="r1",
        blueprint_id="p1",
        failed_module="m1",
        error_message="oops",
        stack_trace=None,
        manifest_json="{}",
        started_at="2024-01-01T00:00:00Z",
        finished_at="2024-01-01T00:01:00Z"
    )
    js = ctx.to_json()
    data = json.loads(js)
    assert data["run_id"] == "r1"
    assert data["error_message"] == "oops"


def test_failure_context_to_dict_includes_doctor_hints():
    """FailureContext.to_dict() includes doctor_hints list."""
    ctx = FailureContext(
        run_id="r1",
        blueprint_id="p1",
        failed_module="m1",
        error_message="oops",
        stack_trace=None,
        manifest_json="{}",
        started_at="2024-01-01T00:00:00Z",
        finished_at="2024-01-01T00:01:00Z",
        doctor_hints=("warn: bad path", "fail: missing schema"),
    )
    d = ctx.to_dict()
    assert "doctor_hints" in d
    assert d["doctor_hints"] == ["warn: bad path", "fail: missing schema"]


def test_failure_context_doctor_hints_empty_by_default():
    """FailureContext.doctor_hints defaults to empty tuple; to_dict() yields []."""
    ctx = FailureContext(
        run_id="r1",
        blueprint_id="p1",
        failed_module="m1",
        error_message="oops",
        stack_trace=None,
        manifest_json="{}",
        started_at="2024-01-01T00:00:00Z",
        finished_at="2024-01-01T00:01:00Z",
    )
    assert ctx.doctor_hints == ()
    d = ctx.to_dict()
    assert d["doctor_hints"] == []


def test_failure_context_phase35_defaults():
    """Phase 35 defaults for structured error fields."""
    ctx = FailureContext(
        run_id="r1",
        blueprint_id="p1",
        failed_module="m1",
        error_message="oops",
        stack_trace=None,
        manifest_json="{}",
        started_at="2024-01-01T00:00:00Z",
        finished_at="2024-01-01T00:01:00Z",
    )
    assert ctx.error_class is None
    assert ctx.root_exception is None
    assert ctx.sql_state is None
    assert ctx.suggested_columns == ()
    assert ctx.object_name is None


def test_failure_context_phase35_frozen():
    """Phase 35: mutating any new field raises FrozenInstanceError."""
    ctx = FailureContext(
        run_id="r1",
        blueprint_id="p1",
        failed_module="m1",
        error_message="oops",
        stack_trace=None,
        manifest_json="{}",
        started_at="2024-01-01T00:00:00Z",
        finished_at="2024-01-01T00:01:00Z",
    )
    with pytest.raises(FrozenInstanceError):
        ctx.error_class = "X"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        ctx.suggested_columns = ("a",)  # type: ignore[misc]


def test_failure_context_phase35_to_dict():
    """Phase 35: to_dict() round-trips the 5 new keys; suggested_columns serialized as list."""
    ctx = FailureContext(
        run_id="r1",
        blueprint_id="p1",
        failed_module="m1",
        error_message="oops",
        stack_trace=None,
        manifest_json="{}",
        started_at="2024-01-01T00:00:00Z",
        finished_at="2024-01-01T00:01:00Z",
        error_class="UNRESOLVED_COLUMN",
        root_exception={"type": "AnalysisException", "message": "msg"},
        sql_state="42000",
        suggested_columns=("a", "b"),
        object_name="c",
    )
    d = ctx.to_dict()
    assert d["error_class"] == "UNRESOLVED_COLUMN"
    assert d["root_exception"] == {"type": "AnalysisException", "message": "msg"}
    assert d["sql_state"] == "42000"
    assert d["suggested_columns"] == ["a", "b"]
    assert d["object_name"] == "c"
