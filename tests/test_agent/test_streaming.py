"""SSE streaming parse for the agent providers (mocked — no live LLM).

Covers both provider shapes: OpenAI-compatible (`reasoning_content` + `content`
deltas) and Anthropic (`thinking_delta` + `text_delta`). Verifies the live
``on_token(kind, text)`` callback fires per delta, the answer accumulates to the
returned text, and usage tokens are picked up.
"""
from __future__ import annotations

import contextlib

import httpx
import pytest

import aqueduct.agent.providers as providers
from aqueduct.errors import AqueductError

pytestmark = pytest.mark.unit


class _FakeResp:
    def __init__(self, lines: list[str]) -> None:
        self._lines = lines

    def raise_for_status(self) -> None:  # noqa: D401
        pass

    def iter_lines(self):
        yield from self._lines


class _FakeClient:
    def __init__(self, lines: list[str]) -> None:
        self._lines = lines

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    @contextlib.contextmanager
    def stream(self, *args, **kwargs):
        yield _FakeResp(self._lines)


def _collect(fn, lines):
    tokens: list[tuple[str, str]] = []
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(httpx, "Client", lambda *a, **k: _FakeClient(lines))
        text, ti, to = fn("http://x", {"model": "m"}, {}, 60.0, lambda k, t: tokens.append((k, t)))
    return text, ti, to, tokens


def test_stream_openai_compat_parses_reasoning_and_content():
    lines = [
        'data: {"choices":[{"delta":{"reasoning_content":"think A "}}]}',
        'data: {"choices":[{"delta":{"reasoning_content":"think B"}}]}',
        'data: {"choices":[{"delta":{"content":"{\\"operations\\":"}}]}',
        'data: {"choices":[{"delta":{"content":"[]}"}}]}',
        'data: {"usage":{"prompt_tokens":1200,"completion_tokens":40}}',
        "data: [DONE]",
    ]
    text, ti, to, tokens = _collect(providers._stream_openai_compat, lines)
    assert text == '{"operations":[]}'
    assert ti == 1200 and to == 40
    thinking = "".join(t for k, t in tokens if k == "thinking")
    answer = "".join(t for k, t in tokens if k == "answer")
    assert thinking == "think A think B"
    assert answer == text  # streamed answer == accumulated return


def test_stream_anthropic_parses_thinking_and_text():
    lines = [
        'data: {"type":"message_start","message":{"usage":{"input_tokens":1100}}}',
        'data: {"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"reason "}}',
        'data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"{\\"ops\\":"}}',
        'data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"[]}"}}',
        'data: {"type":"message_delta","usage":{"output_tokens":35}}',
    ]
    text, ti, to, tokens = _collect(providers._stream_anthropic, lines)
    assert text == '{"ops":[]}'
    assert ti == 1100 and to == 35
    assert "".join(t for k, t in tokens if k == "thinking") == "reason "
    assert "".join(t for k, t in tokens if k == "answer") == text


def test_stream_empty_response_raises():
    """A stream that yields no answer content is an error (caught by the loop)."""
    with pytest.raises(AqueductError, match="empty"):
        _collect(providers._stream_openai_compat, ["data: [DONE]"])
