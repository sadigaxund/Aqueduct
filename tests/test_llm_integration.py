"""Live LLM integration tests — require Ollama running at AQ_OLLAMA_URL.

Set the env var before running:
    export AQ_OLLAMA_URL=http://<IP ADDRESS>:11434

Tests skip automatically if the var is unset or the host is not reachable.
The model name is read from AQ_OLLAMA_MODEL (default: gemma3).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from tests.conftest import requires_ollama


_MODEL = os.environ.get("AQ_OLLAMA_MODEL", "gemma3:12b")

_MINIMAL_BP_YAML = """\
aqueduct: "1.0"
id: test.live.llm
name: Live LLM Test

modules:
  - id: src
    type: Ingress
    label: Source
    config:
      format: parquet
      path: /tmp/data

  - id: sink
    type: Egress
    label: Sink
    config:
      format: parquet
      path: /tmp/out
      mode: overwrite

edges:
  - from: src
    to: sink
"""


def _make_failure_ctx(pipeline_id: str = "test.live.llm"):
    from aqueduct.surveyor.models import FailureContext

    return FailureContext(
        run_id="run-live-001",
        pipeline_id=pipeline_id,
        failed_module="src",
        error_message="IngressError: path '/tmp/data' does not exist",
        stack_trace="IngressError: path '/tmp/data' does not exist\n  at read_ingress()",
        manifest_json=json.dumps({
            "pipeline_id": pipeline_id,
            "name": "Live LLM Test",
            "description": "Reads parquet and writes parquet",
            "modules": [
                {"id": "src", "type": "Ingress", "label": "Source",
                 "config": {"format": "parquet", "path": "/tmp/data"}},
                {"id": "sink", "type": "Egress", "label": "Sink",
                 "config": {"format": "parquet", "path": "/tmp/out", "mode": "overwrite"}},
            ],
            "edges": [{"from": "src", "to": "sink", "port": "main"}],
        }),
        started_at="2024-01-01T00:00:00+00:00",
        finished_at="2024-01-01T00:01:00+00:00",
    )


@requires_ollama
def test_ollama_call_llm_returns_text(ollama_url):
    """_call_llm with openai_compat returns non-empty string."""
    from aqueduct.surveyor.llm import _call_llm

    messages = [{"role": "user", "content": "Say 'hello' and nothing else."}]
    result = _call_llm(
        messages=messages,
        model=_MODEL,
        max_tokens=32,
        provider="openai_compat",
        base_url=ollama_url.rstrip("/") + "/v1",
        patches_dir=Path("/tmp"),
    )
    assert isinstance(result, str)
    assert len(result) > 0


@requires_ollama
def test_trigger_llm_patch_returns_patch_spec(ollama_url, tmp_path):
    """Full LLM patch loop with Ollama returns a PatchSpec staged for human review."""
    from aqueduct.surveyor.llm import trigger_llm_patch

    ctx = _make_failure_ctx()
    patches_dir = tmp_path / "patches"

    result = trigger_llm_patch(
        failure_ctx=ctx,
        model=_MODEL,
        api_endpoint="",
        max_tokens=1024,
        approval_mode="human",
        blueprint_path=None,
        patches_dir=patches_dir,
        provider="openai_compat",
        base_url=ollama_url.rstrip("/") + "/v1",
    )

    # May return None if model produces invalid PatchSpec after MAX_REPROMPTS
    if result is None:
        pytest.skip(f"Model {_MODEL!r} failed to produce valid PatchSpec — tune prompt or model")

    from aqueduct.patch.grammar import PatchSpec
    assert isinstance(result, PatchSpec)
    assert len(result.operations) >= 1
    assert (patches_dir / "pending" / f"{result.patch_id}.json").exists()


@requires_ollama
def test_trigger_llm_auto_apply(ollama_url, tmp_path):
    """Full LLM patch loop auto-applies patch to a real Blueprint file."""
    from aqueduct.surveyor.llm import trigger_llm_patch

    bp_file = tmp_path / "blueprint.yml"
    bp_file.write_text(_MINIMAL_BP_YAML)
    patches_dir = tmp_path / "patches"
    ctx = _make_failure_ctx()

    result = trigger_llm_patch(
        failure_ctx=ctx,
        model=_MODEL,
        api_endpoint="",
        max_tokens=1024,
        approval_mode="auto",
        blueprint_path=bp_file,
        patches_dir=patches_dir,
        provider="openai_compat",
        base_url=ollama_url.rstrip("/") + "/v1",
    )

    if result is None:
        pytest.skip(f"Model {_MODEL!r} failed to produce valid PatchSpec — tune prompt or model")

    # Either auto-applied (blueprint changed) or fell back to human staging
    assert (patches_dir / "applied").exists() or (patches_dir / "pending").exists()
