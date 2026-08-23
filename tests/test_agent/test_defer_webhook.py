"""Unit tests for Phase 88 Domain 6 — the dedicated on_defer webhook event.

Covers:
  - confidence_reason (previously dropped) now reaches the webhook payload
    for a defer-carrying patch.
  - on_defer routing: a defer patch fires the on_defer endpoint with event
    "on_defer" when configured.
  - Fallback: when on_defer is unset, a defer patch still fires
    on_patch_pending_webhook/webhook_event unchanged (upgrade-safe).
  - A non-defer patch never touches on_defer, even when configured.
"""

from __future__ import annotations

import pytest

from aqueduct.agent import stage_patch_for_human
from aqueduct.config import WebhookEndpointConfig
from aqueduct.patch.grammar import PatchSpec
from aqueduct.surveyor.models import FailureContext

pytestmark = pytest.mark.unit


def _fc():
    return FailureContext(
        run_id="r1",
        blueprint_id="bp",
        failed_module="clean",
        error_message="ConnectionRefusedError: could not reach warehouse",
        stack_trace="",
        manifest_json="{}",
        started_at="2026-06-16T00:00:00+00:00",
        finished_at="2026-06-16T00:00:01+00:00",
        error_class="ConnectionRefusedError",
        engine="spark",
    )


def _defer_spec(patch_id="defer1"):
    return PatchSpec(
        patch_id=patch_id,
        root_cause="warehouse unreachable",
        rationale="infra outage, not a Blueprint bug",
        confidence=0.95,
        operations=[
            {
                "op": "defer_to_human",
                "diagnosis": "the warehouse is unreachable from the cluster",
                "suggestions": ["check network ACLs", "page on-call infra"],
                "confidence_reason": "clean connection-refused signature, no ambiguity",
                "defer_reason": "infrastructure",
            }
        ],
    )


def _real_op_spec(patch_id="fix1"):
    return PatchSpec(
        patch_id=patch_id,
        root_cause="rename",
        rationale="rename the column",
        confidence=0.9,
        operations=[
            {
                "op": "set_module_config_key",
                "module_id": "clean",
                "key": "query",
                "value": "SELECT 1",
            }
        ],
    )


@pytest.fixture()
def captured_fire_webhook(monkeypatch):
    calls: list[dict] = []

    def _fake_fire_webhook(config, full_payload, template_vars=None, event=None):
        calls.append({"config": config, "payload": full_payload, "event": event})

    monkeypatch.setattr("aqueduct.surveyor.webhook.fire_webhook", _fake_fire_webhook)
    return calls


def test_defer_patch_forwards_confidence_reason_and_defer_reason(captured_fire_webhook, tmp_path):
    on_pending = WebhookEndpointConfig(url="https://hooks.example.com/pending")
    stage_patch_for_human(
        _defer_spec(),
        tmp_path / "patches",
        _fc(),
        on_patch_pending_webhook=on_pending,
    )
    assert len(captured_fire_webhook) == 1
    payload = captured_fire_webhook[0]["payload"]
    assert payload["confidence_reason"] == "clean connection-refused signature, no ambiguity"
    assert payload["defer_reason"] == "infrastructure"
    assert payload["diagnosis"] == "the warehouse is unreachable from the cluster"
    assert payload["suggestions"] == ["check network ACLs", "page on-call infra"]


def test_defer_patch_routes_to_on_defer_when_configured(captured_fire_webhook, tmp_path):
    on_pending = WebhookEndpointConfig(url="https://hooks.example.com/pending")
    on_defer = WebhookEndpointConfig(url="https://hooks.example.com/defer")
    stage_patch_for_human(
        _defer_spec(),
        tmp_path / "patches",
        _fc(),
        on_patch_pending_webhook=on_pending,
        on_defer_webhook=on_defer,
    )
    assert len(captured_fire_webhook) == 1
    call = captured_fire_webhook[0]
    assert call["config"] is on_defer
    assert call["event"] == "on_defer"


def test_defer_patch_falls_back_to_on_patch_pending_when_on_defer_unset(
    captured_fire_webhook, tmp_path
):
    on_pending = WebhookEndpointConfig(url="https://hooks.example.com/pending")
    stage_patch_for_human(
        _defer_spec(),
        tmp_path / "patches",
        _fc(),
        on_patch_pending_webhook=on_pending,
        on_defer_webhook=None,
    )
    assert len(captured_fire_webhook) == 1
    call = captured_fire_webhook[0]
    assert call["config"] is on_pending
    assert call["event"] == "on_patch_pending"


def test_non_defer_patch_never_routes_to_on_defer(captured_fire_webhook, tmp_path):
    on_pending = WebhookEndpointConfig(url="https://hooks.example.com/pending")
    on_defer = WebhookEndpointConfig(url="https://hooks.example.com/defer")
    stage_patch_for_human(
        _real_op_spec(),
        tmp_path / "patches",
        _fc(),
        on_patch_pending_webhook=on_pending,
        on_defer_webhook=on_defer,
    )
    assert len(captured_fire_webhook) == 1
    call = captured_fire_webhook[0]
    assert call["config"] is on_pending
    assert call["event"] == "on_patch_pending"
    # A non-defer patch never carries diagnosis/suggestions/confidence_reason.
    assert "confidence_reason" not in call["payload"]
    assert "defer_reason" not in call["payload"]
