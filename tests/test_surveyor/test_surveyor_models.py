"""Tests for Surveyor data models."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError

import pytest

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
