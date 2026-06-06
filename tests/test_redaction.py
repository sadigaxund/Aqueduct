"""Unit tests for aqueduct/redaction.py — secret registration, redaction, and hooks."""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import click
import pytest

pytestmark = [pytest.mark.spark, pytest.mark.integration]

from aqueduct import redaction
from aqueduct.cli import _install_secret_redaction_hooks
from aqueduct.surveyor.webhook import fire_webhook
try:
    from aqueduct.surveyor.surveyor import Surveyor
except ImportError:
    pytest.skip("pyspark required by Surveyor's executor dependency", allow_module_level=True)
from aqueduct.agent import stage_patch_for_human
from aqueduct.agent.providers import _call_agent
from aqueduct.compiler.models import Manifest
try:
    from aqueduct.executor.models import ExecutionResult, ModuleResult
except ImportError:
    pytest.skip("pyspark required", allow_module_level=True)
from aqueduct.surveyor.models import FailureContext
from aqueduct.patch.grammar import PatchSpec


@pytest.fixture(autouse=True)
def clean_registry_and_hooks():
    # Clear registry
    redaction.clear()
    yield
    redaction.clear()


# ── 1. Registration gates (Length & Entropy) ─────────────────────────────────

def test_redaction_register_strong():
    """register("hunter2longenough") -> returns True, is_registered returns True."""
    secret = "hunter2longenough"
    assert redaction.register(secret, key_hint="TEST_SECRET") is True
    assert redaction.is_registered(secret) is True
    assert secret in redaction.registered_values()


def test_redaction_register_weak_length():
    """register("abc") -> returns False (below _MIN_SECRET_LENGTH); emits AQ-WARN [secret-weak-redact]."""
    secret = "abc"
    with pytest.warns(UserWarning, match=r"AQ-WARN \[secret-weak-redact\]"):
        res = redaction.register(secret, key_hint="SHORT_SECRET")
    assert res is False
    assert not redaction.is_registered(secret)


def test_redaction_register_weak_entropy():
    """register("aaaaaaaaaaaaaaaaaaaa") -> returns False (Shannon entropy below threshold); emits warning."""
    secret = "aaaaaaaaaaaaaaaaaaaa"
    with pytest.warns(UserWarning, match=r"AQ-WARN \[secret-weak-redact\]"):
        res = redaction.register(secret, key_hint="LOW_ENTROPY")
    assert res is False
    assert not redaction.is_registered(secret)


# ── 2. Redaction rules ────────────────────────────────────────────────────────

def test_redaction_scrub_string():
    """redact("connecting to db://hunter2longenough@host") -> connecting to db://[REDACTED]@host."""
    secret = "hunter2longenough"
    redaction.register(secret)
    text = "connecting to db://hunter2longenough@host"
    assert redaction.redact(text) == "connecting to db://[REDACTED]@host"


def test_redaction_token_boundary():
    """token-boundary: redact("module hunter2longenough_xyz failed") after registering hunter2longenough -> does NOT match inside hunter2longenough_xyz."""
    secret = "hunter2longenough"
    redaction.register(secret)
    text = "module hunter2longenough_xyz failed"
    # Should NOT be redacted because boundary rule fails (followed by _ and letters)
    assert redaction.redact(text) == text


def test_redaction_scrub_dict():
    """redact(dict) -> walks values recursively, scrubs strings."""
    secret = "hunter2longenough"
    redaction.register(secret)
    data = {
        "nested": {
            "key1": "my hunter2longenough secret",
            "key2": 12345,  # non-string untouched
        },
        "list_val": ["hunter2longenough", "clean"],
    }
    expected = {
        "nested": {
            "key1": "my [REDACTED] secret",
            "key2": 12345,
        },
        "list_val": ["[REDACTED]", "clean"],
    }
    assert redaction.redact(data) == expected


def test_redaction_scrub_list_tuple():
    """redact(list) / redact(tuple) -> element-wise scrub preserving type."""
    secret = "hunter2longenough"
    redaction.register(secret)

    lst = ["hunter2longenough", 42]
    tpl = ("hunter2longenough", 42)

    redacted_lst = redaction.redact(lst)
    redacted_tpl = redaction.redact(tpl)

    assert redacted_lst == ["[REDACTED]", 42]
    assert isinstance(redacted_lst, list)

    assert redacted_tpl == ("[REDACTED]", 42)
    assert isinstance(redacted_tpl, tuple)


def test_redaction_longest_first():
    """longest-first regex: two overlapping secrets abc... and abc...xyz -> longer one wins, not the prefix."""
    prefix_secret = "hunter2long"
    full_secret = "hunter2longenough"

    # Register in either order
    redaction.register(prefix_secret)
    redaction.register(full_secret)

    text = "my secret is hunter2longenough"
    # If the prefix won, it would be "[REDACTED]enough". If the longer wins, it is "[REDACTED]".
    assert redaction.redact(text) == "my secret is [REDACTED]"


# ── 3. CLI hooks ─────────────────────────────────────────────────────────────

def test_cli_redaction_hook_click_echo(capsys):
    """click.echo("…hunter2longenough…") after a registered secret -> output contains [REDACTED]."""
    secret = "hunter2longenough"
    redaction.register(secret)

    # Install/ensure hooks are active
    _install_secret_redaction_hooks()

    click.echo("secret is hunter2longenough")
    captured = capsys.readouterr()
    assert "[REDACTED]" in captured.out
    assert "hunter2longenough" not in captured.out


def test_cli_redaction_hook_idempotent():
    """idempotent: re-invoking the hook does not double-wrap (_aq_redaction_wrapped attr guard)."""
    # Ensure it's installed
    _install_secret_redaction_hooks()
    first_wrapper = click.echo

    # Invoke again
    _install_secret_redaction_hooks()
    second_wrapper = click.echo

    assert first_wrapper is second_wrapper
    assert getattr(click.echo, "_aq_redaction_wrapped", False) is True


def test_cli_redaction_hook_logging(caplog):
    """root logger emits a record with a registered value in msg -> handler output contains [REDACTED]."""
    secret = "hunter2longenough"
    redaction.register(secret)

    # Ensure hook is active (installs root logger filter)
    _install_secret_redaction_hooks()

    # Must log directly to root logger so its filters are evaluated
    with caplog.at_level(logging.WARNING):
        logging.warning("failed with secret %s", secret)

    # caplog records should be redacted by the filter
    assert len(caplog.records) == 1
    assert "[REDACTED]" in caplog.records[0].msg
    assert secret not in caplog.records[0].msg


# ── 4. Observability and Surveyor ────────────────────────────────────────────

def test_observability_redaction_surveyor(tmp_path):
    """observability failure_contexts.stack_trace row containing a registered secret stores [REDACTED] after surveyor.record().

    Phase 39 externalizes stack_trace/manifest_json/provenance_json as blob paths
    (``blobs/<run_id>/*.json.zst``) when store_dir is set.  Check the blob content
    for redaction; error_message stays inline.
    """
    secret = "hunter2longenough"
    redaction.register(secret)

    # Minimum blueprint manifest to satisfy surveyor setup
    manifest = Manifest(
        blueprint_id="test-bp",
        modules=(),
        edges=(),
        context={},
        spark_config={},
    )

    surveyor = Surveyor(
        manifest=manifest,
        store_dir=tmp_path,
    )

    run_id = "run-123"
    surveyor.start(run_id)

    # Record a failure containing the secret in stack trace and error message
    result = ExecutionResult(
        blueprint_id="test-bp",
        run_id=run_id,
        status="error",
        module_results=(
            ModuleResult(module_id="m1", status="success"),
            ModuleResult(module_id="m2", status="error", error=f"failed using secret {secret}"),
        )
    )

    # We patch fire_webhook because we're testing the DB recording side of surveyor
    with patch("aqueduct.surveyor.surveyor.fire_webhook"):
        surveyor.record(result, exc=RuntimeError(f"crash due to {secret}"))

    surveyor.stop()

    # Query database directly to check that the stored record is redacted
    import duckdb
    conn = duckdb.connect(str(tmp_path / "observability.db"))
    res = conn.execute("SELECT error_message, stack_trace, manifest_json FROM failure_contexts").fetchone()
    conn.close()

    assert res is not None
    err_msg, stack_path, manifest_path = res
    # error_message is stored inline — check redaction directly
    assert "[REDACTED]" in err_msg
    assert secret not in err_msg
    # stack_trace is a blob path (Phase 39) — materialize it
    from aqueduct.surveyor.blob_store import materialize
    stack = materialize(stack_path, tmp_path)
    assert "[REDACTED]" in stack
    assert secret not in stack


# ── 5. Patch staging ─────────────────────────────────────────────────────────

def test_patch_sidecar_redaction(tmp_path):
    """patch sidecar pending file written via stage_patch_for_human containing a registered secret in the payload writes [REDACTED] to disk."""
    secret = "hunter2longenough"
    redaction.register(secret)

    # Patch spec containing secret in rationale
    spec = PatchSpec(
        patch_id="fix-secret",
        rationale=f"update configuration containing {secret}",
        confidence=0.9,
        category="bad_path",
        root_cause="hardcoded key",
        operations=[
            {
                "op": "set_module_config_key",
                "module_id": "m1",
                "key": "password",
                "value": "hunter2longenough",
            }
        ],
    )

    failure_ctx = FailureContext(
        run_id="run-123",
        blueprint_id="test-bp",
        failed_module="m1",
        error_message="err",
        stack_trace="trace",
        manifest_json="{}",
        started_at="2026-05-23T12:00:00Z",
        finished_at="2026-05-23T12:00:05Z",
    )

    stage_patch_for_human(spec, tmp_path, failure_ctx)

    # Find the written file in patches/pending
    pending_dir = tmp_path / "pending"
    files = list(pending_dir.glob("*.json"))
    assert len(files) == 1

    content = files[0].read_text(encoding="utf-8")
    assert "[REDACTED]" in content
    assert secret not in content


# ── 6. Webhooks ──────────────────────────────────────────────────────────────

def test_webhook_redaction(monkeypatch):
    """webhook body containing a registered secret has the secret scrubbed; webhook headers and URL are NOT scrubbed."""
    secret = "hunter2longenough"
    redaction.register(secret)

    # Mock WebhookEndpointConfig
    mock_config = MagicMock()
    mock_config.url = f"http://test.com/{secret}"  # URL should NOT be scrubbed
    mock_config.method = "POST"
    mock_config.timeout = 5
    mock_config.headers = {
        "Authorization": f"Bearer {secret}",  # Header should NOT be scrubbed
        "X-Secret": secret,
    }
    mock_config.payload = {
        "data": f"sending {secret} to target",  # Body SHOULD be scrubbed
    }

    captured_reqs = []

    def mock_request(method, url, json=None, headers=None, timeout=None):
        captured_reqs.append({
            "method": method,
            "url": url,
            "json": json,
            "headers": headers,
        })
        resp = MagicMock()
        resp.status_code = 200
        return resp

    with patch("httpx.request", side_effect=mock_request):
        thread = fire_webhook(mock_config, full_payload={}, template_vars={})
        thread.join()

    assert len(captured_reqs) == 1
    req = captured_reqs[0]

    # Verify body (json) is redacted
    assert req["json"]["data"] == "sending [REDACTED] to target"

    # Verify headers and URL are NOT redacted
    assert secret in req["url"]
    assert req["headers"]["Authorization"] == f"Bearer {secret}"
    assert req["headers"]["X-Secret"] == secret


# ── 7. LLM Dispatch ──────────────────────────────────────────────────────────

def test_llm_redaction(tmp_path):
    """LLM _call_agent with a registered secret in messages -> outgoing httpx.post JSON body shows [REDACTED]."""
    secret = "hunter2longenough"
    redaction.register(secret)

    messages = [
        {"role": "user", "content": f"The secret password was {secret}"}
    ]

    captured_post_json = None

    def mock_post(url, headers=None, json=None, timeout=None):
        nonlocal captured_post_json
        captured_post_json = json
        resp = MagicMock()
        resp.status_code = 200
        # Return valid anthropic/openai compat response format
        resp.json.return_value = {
            "content": [{"text": "patch response"}],
            "choices": [{"message": {"content": "patch response"}}]
        }
        return resp

    os.environ["ANTHROPIC_API_KEY"] = "fake-key"
    try:
        # Provider switched to ``with httpx.Client(): client.post(...)``; mock
        # target moved from module-level ``httpx.post`` to ``httpx.Client``.
        mock_client = MagicMock()
        mock_client.post.side_effect = mock_post
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        with patch("httpx.Client", return_value=mock_client):
            from aqueduct.agent.providers import _ProviderConfig
            _cfg = _ProviderConfig(
                model="claude-sonnet", max_tokens=1000,
                provider="anthropic", base_url=None,
                timeout=5.0, patches_dir=tmp_path,
            )
            _call_agent(messages, _cfg, patches_dir=tmp_path)
    finally:
        os.environ.pop("ANTHROPIC_API_KEY", None)

    assert captured_post_json is not None
    # Check that system prompt and message contents are redacted in outgoing payload
    # For anthropic, messages is in json["messages"] and system prompt is in json["system"]
    assert "system" in captured_post_json
    assert "messages" in captured_post_json

    # Secrets should be redacted in system prompt (re-loaded history/context) and messages
    msg_content = captured_post_json["messages"][0]["content"]
    assert "[REDACTED]" in msg_content
    assert secret not in msg_content
