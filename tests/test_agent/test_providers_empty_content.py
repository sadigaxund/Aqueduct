"""Empty-content handling on the NON-streaming OpenAI-compatible path.

Three harness defects, all of which rendered as "the model wrote bad JSON":

1. ``content: ""`` (an empty STRING, not null) sailed past an ``is None`` check
   and reached the JSON parser, which reported
   ``JSON parse error at line 1, column 1: Expecting value`` with a blank
   "your output near the error" — blaming the model for malformed JSON when the
   model returned nothing at all. The STREAMING path 30 lines below already
   guarded this with ``if not text``; the two paths disagreed about what an
   empty response is.

2. ``finish_reason: "length"`` (the response was TRUNCATED at max_tokens) was
   never read. Different cause, different fix (raise max_tokens) — so the error
   has to say so instead of sending the user to debug their prompt.

3. ``message.reasoning_content`` present with ``content`` empty (a
   reasoning-only turn from deepseek-r1 / o-series via a compat gateway) was
   also never read on this path, though the streaming path routes it to the
   'thinking' channel.

Scope note: none of this parses a patch OUT of ``reasoning_content`` — the
reasoning channel is not the answer channel. It only makes the raised error
state which of the three situations was actually observed.

``httpx.Client`` mocking follows ``test_providers_tools.py``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from aqueduct.agent.providers import _call_openai_compat
from aqueduct.errors import AqueductError

pytestmark = pytest.mark.unit


def _mock_client(payload: dict) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status = MagicMock()
    client = MagicMock()
    client.post.return_value = resp
    client.__enter__.return_value = client
    client.__exit__.return_value = False
    return client


def _call(mock_client_cls, payload: dict, **kwargs):
    mock_client_cls.return_value = _mock_client(payload)
    return _call_openai_compat(
        [{"role": "user", "content": "fix it"}],
        "some-model",
        kwargs.pop("max_tokens", 4096),
        "http://localhost:11434/v1",
        "system",
        **kwargs,
    )


def _choice(content, *, finish_reason=None, reasoning_content=None) -> dict:
    message: dict = {"role": "assistant", "content": content}
    if reasoning_content is not None:
        message["reasoning_content"] = reasoning_content
    choice: dict = {"index": 0, "message": message}
    if finish_reason is not None:
        choice["finish_reason"] = finish_reason
    return {"choices": [choice], "usage": {"prompt_tokens": 10, "completion_tokens": 0}}


class TestEmptyContentIsGuarded:
    """Defect 2 — falsy/whitespace-only content must not reach the JSON parser."""

    @pytest.mark.parametrize("content", ["", "   ", "\n\t ", None])
    @patch("httpx.Client")
    def test_empty_content_raises_instead_of_returning_it(self, mock_client_cls, content):
        with pytest.raises(AqueductError) as exc_info:
            _call(mock_client_cls, _choice(content))
        # The message must be about an empty response, not about JSON.
        assert "empty" in str(exc_info.value).lower()

    @patch("httpx.Client")
    def test_names_the_non_streaming_path(self, mock_client_cls):
        """The two paths stay distinguishable — a user reading the error must be
        able to tell which one produced it."""
        with pytest.raises(AqueductError) as exc_info:
            _call(mock_client_cls, _choice(""))
        assert "non-streaming" in str(exc_info.value).lower()

    @patch("httpx.Client")
    def test_real_content_still_returned(self, mock_client_cls):
        text, tin, tout = _call(mock_client_cls, _choice('{"patch_id": "p1"}'))
        assert text == '{"patch_id": "p1"}'
        assert (tin, tout) == (10, 0)

    @patch("httpx.Client")
    def test_whitespace_is_not_stripped_from_real_content(self, mock_client_cls):
        """The guard tests ``.strip()`` but must return the ORIGINAL string —
        downstream recovery passes (fence/think-block stripping) work on raw text."""
        text, _, _ = _call(mock_client_cls, _choice('  {"patch_id": "p1"}\n'))
        assert text == '  {"patch_id": "p1"}\n'


class TestTerminatingConditionIsReported:
    """Defect 3 — the response carries WHY it is empty; say so."""

    @patch("httpx.Client")
    def test_truncation_names_max_tokens_and_the_override(self, mock_client_cls):
        with pytest.raises(AqueductError) as exc_info:
            _call(mock_client_cls, _choice("", finish_reason="length"), max_tokens=4096)
        msg = str(exc_info.value)
        assert "finish_reason='length'" in msg
        assert "truncat" in msg.lower()
        # The EFFECTIVE limit, read off the request that was actually sent.
        assert "4096" in msg
        assert "agent.provider_options.max_tokens" in msg

    @patch("httpx.Client")
    def test_truncation_reports_the_provider_options_override_value(self, mock_client_cls):
        """``provider_options`` merges into the payload AFTER max_tokens is set,
        so the effective limit is the override — reporting the argument would
        name a number that was never sent."""
        with pytest.raises(AqueductError) as exc_info:
            _call(
                mock_client_cls,
                _choice("", finish_reason="length"),
                max_tokens=4096,
                provider_options={"max_tokens": 16000},
            )
        msg = str(exc_info.value)
        assert "16000" in msg
        assert "4096" not in msg

    @patch("httpx.Client")
    def test_reasoning_only_response_is_named(self, mock_client_cls):
        with pytest.raises(AqueductError) as exc_info:
            _call(mock_client_cls, _choice("", reasoning_content="let me think about this"))
        msg = str(exc_info.value)
        assert "reasoning_content" in msg
        # Reported as a distinct situation, not as truncation.
        assert "truncat" not in msg.lower()

    @patch("httpx.Client")
    def test_reasoning_text_is_not_returned_as_the_answer(self, mock_client_cls):
        """The reasoning channel is not the answer channel — no patch is ever
        parsed out of it."""
        with pytest.raises(AqueductError):
            _call(
                mock_client_cls,
                _choice("", reasoning_content='{"patch_id": "from-the-thinking-channel"}'),
            )

    @patch("httpx.Client")
    def test_neither_signal_reports_a_genuinely_empty_response(self, mock_client_cls):
        with pytest.raises(AqueductError) as exc_info:
            _call(mock_client_cls, _choice("", finish_reason="stop"))
        msg = str(exc_info.value)
        assert "truncat" not in msg.lower()
        assert "reasoning_content" not in msg
        assert "finish_reason='stop'" in msg

    @patch("httpx.Client")
    def test_tool_loop_final_turn_gets_the_same_guard(self, mock_client_cls):
        """The tool-use loop's final turn is an ordinary completion and carried
        an identical `is None` check — one shared explanation, not two copies
        that drift."""
        from aqueduct.agent.providers import ToolCallState

        class _ToolBox:
            def declarations(self):
                return [
                    {"name": "read_blueprint", "description": "x", "params_schema": {}},
                ]

            def call(self, name, args):  # pragma: no cover — never reached
                return {}

        with pytest.raises(AqueductError) as exc_info:
            _call(
                mock_client_cls,
                _choice("", finish_reason="length"),
                max_tokens=4096,
                tools=[{"name": "read_blueprint", "description": "x", "params_schema": {}}],
                toolbox=_ToolBox(),
                tool_state=ToolCallState(),
            )
        assert "truncat" in str(exc_info.value).lower()
        assert "4096" in str(exc_info.value)

    @patch("httpx.Client")
    def test_truncation_wins_over_reasoning_when_both_present(self, mock_client_cls):
        """A reasoning model that ran out of tokens mid-thought shows both.
        ``length`` is the actionable one — it names a knob."""
        with pytest.raises(AqueductError) as exc_info:
            _call(
                mock_client_cls,
                _choice("", finish_reason="length", reasoning_content="thinking..."),
            )
        assert "truncat" in str(exc_info.value).lower()
