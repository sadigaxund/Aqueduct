"""LLM provider dispatch — Anthropic Messages API and OpenAI-compatible endpoints.

All providers use httpx; no optional SDK dependency required.

This module owns:
  - Provider HTTP dispatch (_call_anthropic, _call_openai_compat)
  - Provider selection and system-prompt injection (_call_agent)
  - LLM error hint formatting (_format_llm_error_hint)
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from aqueduct.agent.constants import ANTHROPIC_API_VERSION, DEFAULT_LLM_TIMEOUT, DEFAULT_MAX_TOKENS
from aqueduct.agent.prompts import _build_system_prompt
from aqueduct.errors import AqueductError, ConfigError
from aqueduct.infra.http import RETRYABLE_PROVIDER_STATUS, backoff_delay, retry_after_seconds
from aqueduct.redaction import redact as _redact

logger = logging.getLogger(__name__)

# Temperature applied to the ONE escalated attempt that follows a
# stuck-consecutive trip. 0.8 forces sampling divergence so the model
# doesn't keep regenerating the same wrong tree.
_ESCALATION_TEMPERATURE = 0.8

# Phase 46 — transient provider errors worth retrying (429 rate limit, 503
# unavailable, 529 Anthropic "overloaded"). Single source of truth in
# infra.http; aliased here so existing patch paths keep working
# (providers._RETRYABLE_STATUS / providers._retry_after_seconds).
_RETRYABLE_STATUS = RETRYABLE_PROVIDER_STATUS
_retry_after_seconds = retry_after_seconds

_ANTHROPIC_DEFAULT_BASE_URL = "https://api.anthropic.com"

# Phase 75 — rule_id for the one-time degrade warning when supports_tools:
# auto probes an openai_compat endpoint and it rejects/can't handle `tools`.
_AGENT_TOOLS_UNSUPPORTED_RULE = "agent_tools_unsupported"

# Defensive ceiling on tool-conversation round-trips within ONE heal attempt,
# independent of (and slightly above) max_tool_calls — guards against a
# provider that keeps emitting tool_use even after tools were withdrawn from
# the payload on the forced final turn.
_TOOL_LOOP_HARD_CEILING = 32


@dataclass
class ToolCallState:
    """Mutable per-attempt state for agentic tool-use (Phase 75).

    Passed BY REFERENCE into ``_call_agent``/``_call_anthropic``/
    ``_call_openai_compat`` so they can report the tool-call count and
    capability-probe outcome back to the caller without changing
    ``_call_agent``'s stable ``(text, tokens_in, tokens_out)`` return shape —
    every existing call site and test mock that doesn't pass a ``tool_state``
    keeps working unchanged.
    """

    tool_calls_used: int = 0
    # None = not yet probed this heal session; True/False once known.
    supported: bool | None = None
    # Set once, the turn capability was determined unsupported (drives the
    # single [agent_tools_unsupported] warning — never repeated per attempt).
    degraded_reason: str | None = None
    warned: bool = False
    # Phase 75 — per-call telemetry for the transcript's verbose tool-call
    # lines: [{"name", "args_summary", "duration_ms", "result_preview"}, ...].
    # Reset per attempt by the caller (loop.py), unlike `supported`/`warned`.
    tool_call_log: list[dict[str, Any]] = field(default_factory=list)


def _tool_args_summary(args: dict[str, Any], max_len: int = 80) -> str:
    try:
        s = ", ".join(f"{k}={v!r}" for k, v in args.items())
    except Exception:
        s = str(args)
    return s if len(s) <= max_len else s[:max_len] + "…"


def _tool_result_preview(result: Any, max_len: int = 160) -> str:
    s = str(result)
    return s if len(s) <= max_len else s[:max_len] + "…"


def _looks_like_tools_rejection(exc: Exception) -> bool:
    """True when an HTTP error plausibly means 'this endpoint doesn't support tools'.

    Best-effort text sniff on the status/response body — different
    OpenAI-compatible servers phrase this differently (unknown field, 400
    bad request, unsupported parameter). False positives just mean an
    unrelated 4xx is misattributed to tool-capability once per heal session;
    the loop still degrades safely to the working oneshot path either way.
    """
    import httpx

    if not isinstance(exc, httpx.HTTPStatusError):
        return False
    status = exc.response.status_code
    if status < 400 or status >= 500:
        return False
    body = ""
    try:
        body = exc.response.text.lower()
    except Exception:
        body = ""
    return any(kw in body for kw in ("tool", "function_call", "functions"))


def _post_with_retry(
    do_post: Callable[[float], Any],
    *,
    total_seconds: float,
    max_retries: int,
    backoff_seconds: float,
) -> Any:
    """POST with bounded retry on 429/503/529 (Phase 46).

    ``do_post(read_timeout)`` performs one HTTP request and returns the
    response. ``total_seconds`` is the budget for the WHOLE call including
    retries and sleeps — the same per-call deadline Phase 40 derives from
    ``min(agent.timeout, remaining budget seconds)`` — so a retry can delay
    an attempt but never overrun ``agent.budget.max_seconds``. Sleep is the
    server's ``Retry-After`` when sent, else exponential backoff with jitter.
    When the next sleep would not leave at least 1s to actually retry, the
    retryable response is raised as-is.
    """
    t0 = time.monotonic()
    attempt = 0
    while True:
        remaining = total_seconds - (time.monotonic() - t0)
        response = do_post(max(1.0, remaining))
        if response.status_code not in _RETRYABLE_STATUS:
            response.raise_for_status()
            return response
        if attempt >= max_retries:
            response.raise_for_status()
            return response  # unreachable for >=400, keeps type-checkers happy
        attempt += 1
        sleep = _retry_after_seconds(response)
        if sleep is None:
            sleep = backoff_delay(attempt, backoff_seconds, jitter=0.25)
        remaining = total_seconds - (time.monotonic() - t0)
        if sleep >= remaining - 1.0:
            response.raise_for_status()
            return response
        logger.warning(
            "LLM provider returned %d — retry %d/%d in %.1fs",
            response.status_code, attempt, max_retries, sleep,
        )
        time.sleep(sleep)


@dataclass
class _ProviderConfig:
    """Internal configuration bundle passed through the provider dispatch chain.

    Reduces parameter noise by grouping provider-level settings that are set
    once at the start of ``generate_agent_patch`` and forwarded unchanged
    through every call to ``_call_agent``.
    """

    model: str
    max_tokens: int = DEFAULT_MAX_TOKENS
    provider: str = "anthropic"
    base_url: str | None = None
    api_key: str | None = None
    provider_options: dict[str, Any] | None = None
    timeout: float = DEFAULT_LLM_TIMEOUT
    patches_dir: Path = Path()
    engine_prompt_context: str | None = None
    blueprint_prompt_context: str | None = None
    allow_defer: bool = False
    # Phase 45 coaching: the FailureContext keys signature-matched retrieval
    # of past (failure → validated fix) pairs into the system prompt. None
    # (or coaching=False) falls back to the chronological patch-history section.
    failure_ctx: Any = None
    coaching: bool = True
    # Phase 78 Step 2 — target execution engine; selects the PromptRules pack
    # composed into the healing system prompt.
    engine: str = "spark"
    # Phase 53 — observability store backing the patch_index, used by the system
    # prompt's coaching + history sections (replaces the patches/ dir scan).
    obs_store: Any = None
    # Phase 46 — transient-error retry (429/503/529), from agent.retry config.
    retry_max_retries: int = 2
    retry_backoff_seconds: float = 2.0
    # Phase 75 — agentic tool-use. `toolbox` is None in oneshot mode (default)
    # — every tool-related branch below is skipped, byte-identical to
    # pre-Phase-75 behaviour. `supports_tools` is the resolved True/False/"auto"
    # (cascade tier > blueprint > engine); "auto" resolves True without probing
    # for provider="anthropic", probes on first call for "openai_compat".
    toolbox: Any = None
    max_tool_calls: int = 8
    supports_tools: bool | str = "auto"


def _format_llm_error_hint(
    exc: Exception,
    *,
    timeout: float | None,
    base_url: str | None,
    model: str,
) -> str:
    """Return an actionable hint suffix for common transient LLM failure modes.

    Empty string when no specific guidance applies; the caller appends this
    directly to the error log line.
    """
    cls_name = exc.__class__.__name__
    msg = str(exc).lower()

    # httpx.ReadTimeout / ConnectTimeout / WriteTimeout all subclass TimeoutException.
    if "timeout" in cls_name.lower() or "timed out" in msg or "timeout" in msg:
        seconds = f"{int(timeout)}s" if timeout else "unbounded"
        return (
            f"\n  hint: timed out after {seconds} — raise `agent.timeout` "
            f"(aqueduct.yml or the blueprint's agent: block; a cascade tier's own "
            f"`timeout:` overrides it). Local models cold-start +30–90s."
        )

    # Common connect-failure modes from httpx / OS-level networking.
    if (
        "connect" in cls_name.lower()
        or "connection refused" in msg
        or "no route to host" in msg
        or "name or service not known" in msg
    ):
        if base_url:
            return (
                f"\n  hint: cannot reach {base_url}. Check the LLM server is "
                f"running and the host is on a routable network "
                f"(`curl -sS {base_url.rstrip('/')}/models` or "
                f"`ping <host>`)."
            )
        return (
            "\n  hint: cannot reach the LLM endpoint — verify the server is "
            "running and reachable from this host."
        )

    return ""


def _call_agent(
    messages: list[dict[str, Any]],
    cfg: _ProviderConfig,
    patches_dir: Path,
    last_apply_error: str | None = None,
    temperature_override: float | None = None,
    deadline: float | None = None,
    on_token: Callable[[str, str], None] | None = None,
    tool_state: ToolCallState | None = None,
) -> tuple[str, int, int]:
    """Call the LLM provider; return (text, tokens_in, tokens_out).

    ``temperature_override`` lets the caller force a higher sampling
    temperature on the escalated attempt without mutating the caller-supplied
    configuration.

    ``deadline`` (Phase 40) overrides the per-call HTTP timeout to enforce
    the budget's ``max_seconds`` mid-call. When set, it replaces the static
    ``cfg.timeout`` for this single call.

    Token counts come from the provider response when reported; 0 otherwise
    — Phase 75: when ``cfg.toolbox`` is set (agentic mode), token counts
    accumulate across every tool round-trip within this one attempt, and
    ``tool_state`` (mutated in place, never returned — keeps this function's
    3-tuple return unchanged for every existing caller/mock) reports how many
    tool calls were made and whether the endpoint turned out to support tools.
    """
    tools_enabled = (
        cfg.toolbox is not None
        and cfg.supports_tools is not False
        # Once a prior attempt in this heal session proved the endpoint
        # tool-incapable (openai_compat probe failure), stop advertising
        # tools in the prompt too — the API call already stopped sending them.
        and not (tool_state is not None and tool_state.supported is False)
    )
    system_prompt = _build_system_prompt(
        patches_dir,
        cfg.engine_prompt_context,
        cfg.blueprint_prompt_context,
        last_apply_error,
        allow_defer=cfg.allow_defer,
        failure_ctx=cfg.failure_ctx,
        coaching=cfg.coaching,
        obs_store=cfg.obs_store,
        tools_enabled=tools_enabled,
        engine=cfg.engine,
    )

    # Scrub registered @aq.secret() values from anything leaving the process.
    system_prompt = _redact(system_prompt)
    messages = _redact(messages)

    tools_decl = cfg.toolbox.declarations() if tools_enabled else None

    if cfg.provider == "openai_compat":
        return _call_openai_compat(
            messages, cfg.model, cfg.max_tokens, cfg.base_url, system_prompt,
            cfg.provider_options, timeout=cfg.timeout,
            api_key=cfg.api_key,
            temperature_override=temperature_override,
            deadline=deadline,
            max_retries=cfg.retry_max_retries,
            backoff_seconds=cfg.retry_backoff_seconds,
            on_token=on_token,
            tools=tools_decl,
            toolbox=cfg.toolbox if tools_enabled else None,
            max_tool_calls=cfg.max_tool_calls,
            supports_tools=cfg.supports_tools,
            tool_state=tool_state,
        )
    else:
        return _call_anthropic(
            messages, cfg.model, cfg.max_tokens, system_prompt,
            timeout=cfg.timeout,
            api_key=cfg.api_key,
            temperature_override=temperature_override,
            deadline=deadline,
            base_url=cfg.base_url,
            provider_options=cfg.provider_options,
            max_retries=cfg.retry_max_retries,
            backoff_seconds=cfg.retry_backoff_seconds,
            on_token=on_token,
            tools=tools_decl,
            toolbox=cfg.toolbox if tools_enabled else None,
            max_tool_calls=cfg.max_tool_calls,
            tool_state=tool_state,
        )


def _anthropic_tools_payload(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """ToolBox declarations → Anthropic Messages API tool-use format."""
    return [
        {
            "name": t["name"],
            "description": t["description"],
            "input_schema": t["params_schema"],
        }
        for t in tools
    ]


def _call_anthropic(
    messages: list[dict[str, Any]],
    model: str,
    max_tokens: int,
    system_prompt: str,
    timeout: float = DEFAULT_LLM_TIMEOUT,
    api_key: str | None = None,
    temperature_override: float | None = None,
    deadline: float | None = None,
    base_url: str | None = None,
    provider_options: dict[str, Any] | None = None,
    max_retries: int = 2,
    backoff_seconds: float = 2.0,
    on_token: Callable[[str, str], None] | None = None,
    tools: list[dict[str, Any]] | None = None,
    toolbox: Any = None,
    max_tool_calls: int = 8,
    tool_state: ToolCallState | None = None,
) -> tuple[str, int, int]:
    import httpx

    api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ConfigError(
            "ANTHROPIC_API_KEY environment variable not set. "
            "Set it or configure agent.provider: openai_compat in aqueduct.yml."
        )
    # Phase 46 — agent.base_url is honored for anthropic too (gateways,
    # LLM proxies, regional endpoints). Default unchanged.
    url = (base_url or _ANTHROPIC_DEFAULT_BASE_URL).rstrip("/") + "/v1/messages"
    payload: dict[str, Any] = {
        "model": model,
        "max_tokens": max_tokens,
        "system": system_prompt,
        "messages": messages,
    }
    if provider_options:
        # Same config block is shared with openai_compat — drop its
        # ollama_-prefixed keys rather than corrupting the Anthropic payload.
        payload.update({
            k: v for k, v in provider_options.items()
            if not k.startswith("ollama_") and k != "response_format"
        })
    if temperature_override is not None:
        payload["temperature"] = temperature_override
    effective_timeout = float(deadline if deadline is not None else timeout)

    headers = {
        "x-api-key": api_key,
        "anthropic-version": ANTHROPIC_API_VERSION,
        "content-type": "application/json",
    }

    # Phase 75 — tool-use is out of scope for the streaming path (SSE tool_use
    # deltas add real complexity for no user-visible benefit — the live
    # transcript sink is a run/heal TTY affordance, agentic mode is not yet
    # exposed there). Anthropic is known tool-capable (no probe needed); a
    # streaming call from an agentic-mode heal simply proceeds tool-free.
    if tools and toolbox is not None and on_token is None:
        return _call_anthropic_with_tools(
            messages, payload, headers, url,
            tools=tools, toolbox=toolbox, max_tool_calls=max_tool_calls,
            tool_state=tool_state, effective_timeout=effective_timeout,
            max_retries=max_retries, backoff_seconds=backoff_seconds,
        )

    # Streaming path — only when a live-token sink is supplied (the non-streaming
    # POST below stays the default so existing callers/tests are unaffected).
    if on_token is not None:
        return _stream_anthropic(url, payload, headers, effective_timeout, on_token)

    with httpx.Client() as client:
        def _do_post(read_timeout: float) -> httpx.Response:
            return client.post(
                url,
                headers=headers,
                json=payload,
                timeout=min(effective_timeout, read_timeout),
            )
        response = _post_with_retry(
            _do_post,
            total_seconds=effective_timeout,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
        )
        data = response.json()
    content = data.get("content") or []
    if not content:
        raise AqueductError(
            "Anthropic returned empty content block. "
            "The model may have refused the request or been interrupted."
        )
    text = content[0].get("text")
    if text is None:
        raise AqueductError(
            "Anthropic returned null/empty text in content block. "
            "The model may have refused the request or been interrupted."
        )
    usage = data.get("usage") or {}
    return text, int(usage.get("input_tokens", 0) or 0), int(usage.get("output_tokens", 0) or 0)


def _call_anthropic_with_tools(
    messages: list[dict[str, Any]],
    base_payload: dict[str, Any],
    headers: dict[str, str],
    url: str,
    *,
    tools: list[dict[str, Any]],
    toolbox: Any,
    max_tool_calls: int,
    tool_state: ToolCallState | None,
    effective_timeout: float,
    max_retries: int,
    backoff_seconds: float,
) -> tuple[str, int, int]:
    """Multi-turn Anthropic tool-use conversation for ONE heal attempt.

    Loops assistant ``tool_use`` → user ``tool_result`` until the model
    answers with text-only content (the PatchSpec JSON), or ``max_tool_calls``
    is exceeded — at which point ``tools`` is dropped from the payload,
    forcing a final no-tools turn (design item 2's hard cap). The exchange is
    LOCAL to this attempt: the caller's ``messages`` list is never mutated —
    a fresh reprompt attempt starts its own tool conversation from scratch.
    """
    import httpx

    anthropic_tools = _anthropic_tools_payload(tools)
    convo = list(messages)
    ti_total = to_total = 0
    tools_withdrawn = False

    for _ in range(_TOOL_LOOP_HARD_CEILING):
        payload = dict(base_payload)
        payload["messages"] = convo
        if not tools_withdrawn:
            payload["tools"] = anthropic_tools

        with httpx.Client() as client:
            def _do_post(read_timeout: float, _payload=payload) -> httpx.Response:
                return client.post(
                    url, headers=headers, json=_payload,
                    timeout=min(effective_timeout, read_timeout),
                )
            response = _post_with_retry(
                _do_post, total_seconds=effective_timeout,
                max_retries=max_retries, backoff_seconds=backoff_seconds,
            )
            data = response.json()

        usage = data.get("usage") or {}
        ti_total += int(usage.get("input_tokens", 0) or 0)
        to_total += int(usage.get("output_tokens", 0) or 0)

        content = data.get("content") or []
        if not content:
            raise AqueductError(
                "Anthropic returned empty content block. "
                "The model may have refused the request or been interrupted."
            )

        tool_uses = [b for b in content if b.get("type") == "tool_use"]
        if not tool_uses:
            text_blocks = [b.get("text") for b in content if b.get("type") == "text"]
            text = "".join(t for t in text_blocks if t)
            if not text:
                raise AqueductError(
                    "Anthropic returned null/empty text in content block. "
                    "The model may have refused the request or been interrupted."
                )
            if tool_state is not None:
                tool_state.supported = True
            return text, ti_total, to_total

        # Assistant's tool_use turn goes back into the conversation verbatim,
        # followed by one tool_result per tool_use block (Anthropic requires
        # a result for every tool_use id in the SAME user turn).
        convo = [*convo, {"role": "assistant", "content": content}]
        tool_results: list[dict[str, Any]] = []
        for block in tool_uses:
            if tool_state is not None:
                tool_state.tool_calls_used += 1
            _name = block.get("name", "")
            _args = block.get("input") or {}
            _t0 = time.monotonic()
            result = toolbox.call(_name, _args)
            if tool_state is not None:
                tool_state.tool_call_log.append({
                    "name": _name,
                    "args_summary": _tool_args_summary(_args),
                    "duration_ms": int((time.monotonic() - _t0) * 1000),
                    "result_preview": _tool_result_preview(result),
                })
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.get("id"),
                "content": json.dumps(result, default=str),
            })
        convo = [*convo, {"role": "user", "content": tool_results}]

        if tool_state is not None and tool_state.tool_calls_used >= max_tool_calls:
            tools_withdrawn = True  # next turn is forced no-tools (final answer)

    raise AqueductError(
        f"Anthropic tool-use conversation exceeded {_TOOL_LOOP_HARD_CEILING} "
        "round-trips without a final answer."
    )


def _openai_tools_payload(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """ToolBox declarations → OpenAI ``tools`` (function-calling) format."""
    return [
        {
            "type": "function",
            "function": {
                "name": t["name"],
                "description": t["description"],
                "parameters": t["params_schema"],
            },
        }
        for t in tools
    ]


def _call_openai_compat(
    messages: list[dict[str, Any]],
    model: str,
    max_tokens: int,
    base_url: str | None,
    system_prompt: str,
    provider_options: dict[str, Any] | None = None,
    timeout: float = DEFAULT_LLM_TIMEOUT,
    api_key: str | None = None,
    temperature_override: float | None = None,
    deadline: float | None = None,
    max_retries: int = 2,
    backoff_seconds: float = 2.0,
    on_token: Callable[[str, str], None] | None = None,
    tools: list[dict[str, Any]] | None = None,
    toolbox: Any = None,
    max_tool_calls: int = 8,
    supports_tools: bool | str = "auto",
    tool_state: ToolCallState | None = None,
) -> tuple[str, int, int]:
    """Call any OpenAI-compatible endpoint (Ollama, vLLM, LM Studio, etc.)."""
    import httpx

    if not base_url:
        raise ConfigError(
            "agent.base_url must be set for provider=openai_compat "
            "(e.g. http://localhost:11434/v1)"
        )

    api_key = api_key or os.environ.get("OPENAI_API_KEY", "ollama")
    url = base_url.rstrip("/") + "/chat/completions"

    payload: dict[str, Any] = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "system", "content": system_prompt}] + messages,
        "response_format": {"type": "json_object"},
    }
    if provider_options:
        ollama_opts = {k[len("ollama_"):]: v for k, v in provider_options.items() if k.startswith("ollama_")}
        generic_opts = {k: v for k, v in provider_options.items() if not k.startswith("ollama_")}
        if ollama_opts:
            payload["options"] = ollama_opts
        payload.update(generic_opts)
        rf = generic_opts.get("response_format")
        if rf in (None, False, "off"):
            payload.pop("response_format", None)
    if temperature_override is not None:
        payload["temperature"] = temperature_override
        if "options" in payload and isinstance(payload["options"], dict):
            payload["options"]["temperature"] = temperature_override

    effective_read = float(deadline if deadline is not None else timeout)
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    # Phase 75 — tool-use is out of scope for the streaming path (see the
    # matching note in _call_anthropic). Also skipped when a prior call in
    # this heal session already proved the endpoint tool-incapable
    # (tool_state.supported is False) — no repeated probe-and-degrade churn.
    if (
        tools and toolbox is not None and on_token is None
        and not (tool_state is not None and tool_state.supported is False)
    ):
        return _call_openai_compat_with_tools(
            messages, payload, headers, url,
            tools=tools, toolbox=toolbox, max_tool_calls=max_tool_calls,
            supports_tools=supports_tools, tool_state=tool_state,
            effective_read=effective_read, max_retries=max_retries,
            backoff_seconds=backoff_seconds,
        )

    # Streaming path — only when a live-token sink is supplied (the non-streaming
    # POST below stays the default so existing callers/tests are unaffected).
    if on_token is not None:
        return _stream_openai_compat(url, payload, headers, effective_read, on_token)

    with httpx.Client() as client:
        def _do_post(read_timeout: float) -> httpx.Response:
            return client.post(
                url,
                json=payload,
                headers=headers,
                timeout=httpx.Timeout(
                    connect=15.0, read=min(effective_read, read_timeout),
                    write=30.0, pool=5.0,
                ),
            )
        response = _post_with_retry(
            _do_post,
            total_seconds=effective_read,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
        )
        data = response.json()
    text = data["choices"][0]["message"].get("content")
    if text is None:
        raise AqueductError(
            "LLM returned null/empty content from OpenAI-compatible endpoint. "
            "The model may have refused the request, hit a content filter, "
            "or been interrupted mid-generation."
        )
    usage = data.get("usage") or {}
    return text, int(usage.get("prompt_tokens", 0) or 0), int(usage.get("completion_tokens", 0) or 0)


def _call_openai_compat_with_tools(
    messages: list[dict[str, Any]],
    base_payload: dict[str, Any],
    headers: dict[str, str],
    url: str,
    *,
    tools: list[dict[str, Any]],
    toolbox: Any,
    max_tool_calls: int,
    supports_tools: bool | str,
    tool_state: ToolCallState | None,
    effective_read: float,
    max_retries: int,
    backoff_seconds: float,
) -> tuple[str, int, int]:
    """Multi-turn OpenAI-compatible tool-use conversation for ONE heal attempt.

    ``supports_tools: "auto"`` probes on the FIRST call: if the endpoint
    responds with a 4xx that looks tools-related (``_looks_like_tools_rejection``),
    the SAME turn is retried once WITHOUT the ``tools`` field,
    ``tool_state.supported`` is set False, and the caller degrades to the
    plain oneshot path for the rest of THIS heal session (no repeated probing
    — see the guard in ``_call_openai_compat``). This is the one sanctioned
    degrade path — no ReAct/text-emulation fallback (explicitly out of scope).
    """
    import httpx

    openai_tools = _openai_tools_payload(tools)
    # base_payload["messages"] is [system_msg, *original_messages] — split
    # once so each turn can rebuild [system_msg, *convo] as convo grows.
    system_msgs = [m for m in base_payload["messages"] if m.get("role") == "system"]
    convo = list(messages)
    ti_total = to_total = 0
    tools_withdrawn = False
    probed = False

    client = httpx.Client()
    try:
        return _run_openai_tool_loop(
            client, url, headers, base_payload, system_msgs, convo,
            openai_tools=openai_tools, toolbox=toolbox, max_tool_calls=max_tool_calls,
            tool_state=tool_state, effective_read=effective_read,
            max_retries=max_retries, backoff_seconds=backoff_seconds,
            ti_total=ti_total, to_total=to_total,
            tools_withdrawn=tools_withdrawn, probed=probed,
        )
    finally:
        client.close()


def _run_openai_tool_loop(
    client: Any, url: str, headers: dict[str, str], base_payload: dict[str, Any],
    system_msgs: list[dict[str, Any]], convo: list[dict[str, Any]],
    *, openai_tools: list[dict[str, Any]], toolbox: Any, max_tool_calls: int,
    tool_state: ToolCallState | None, effective_read: float,
    max_retries: int, backoff_seconds: float,
    ti_total: int, to_total: int, tools_withdrawn: bool, probed: bool,
) -> tuple[str, int, int]:
    import httpx

    for _ in range(_TOOL_LOOP_HARD_CEILING):
        payload = dict(base_payload)
        payload["messages"] = system_msgs + convo
        if not tools_withdrawn:
            payload["tools"] = openai_tools
            payload["tool_choice"] = "auto"

        def _do_post(read_timeout: float, _payload=payload) -> httpx.Response:
            return client.post(
                url, json=_payload, headers=headers,
                timeout=httpx.Timeout(
                    connect=15.0, read=min(effective_read, read_timeout),
                    write=30.0, pool=5.0,
                ),
            )

        try:
            response = _post_with_retry(
                _do_post, total_seconds=effective_read,
                max_retries=max_retries, backoff_seconds=backoff_seconds,
            )
        except Exception as exc:
            if not probed and not tools_withdrawn and _looks_like_tools_rejection(exc):
                # First-call capability probe failed — degrade for the rest
                # of this heal session and retry THIS turn tools-free.
                probed = True
                tools_withdrawn = True
                if tool_state is not None:
                    tool_state.supported = False
                    tool_state.degraded_reason = str(exc)
                    if not tool_state.warned:
                        tool_state.warned = True
                        logger.warning(
                            "[%s] endpoint rejected a tools-enabled request "
                            "(%s) — degrading to the oneshot path for the "
                            "rest of this heal.",
                            _AGENT_TOOLS_UNSUPPORTED_RULE, exc,
                        )
                continue
            raise
        data = response.json()
        probed = True

        usage = data.get("usage") or {}
        ti_total += int(usage.get("prompt_tokens", 0) or 0)
        to_total += int(usage.get("completion_tokens", 0) or 0)

        choice_msg = (data.get("choices") or [{}])[0].get("message") or {}
        tool_calls = choice_msg.get("tool_calls") or []
        if not tool_calls:
            text = choice_msg.get("content")
            if text is None:
                raise AqueductError(
                    "LLM returned null/empty content from OpenAI-compatible "
                    "endpoint. The model may have refused the request, hit "
                    "a content filter, or been interrupted mid-generation."
                )
            if tool_state is not None and tool_state.supported is None:
                tool_state.supported = True
            return text, ti_total, to_total

        convo = [*convo, {"role": "assistant", "content": choice_msg.get("content"), "tool_calls": tool_calls}]
        for tc in tool_calls:
            if tool_state is not None:
                tool_state.tool_calls_used += 1
            fn = tc.get("function") or {}
            name = fn.get("name", "")
            try:
                args = json.loads(fn.get("arguments") or "{}")
            except (json.JSONDecodeError, TypeError):
                args = {}
            _t0 = time.monotonic()
            result = toolbox.call(name, args)
            if tool_state is not None:
                tool_state.tool_call_log.append({
                    "name": name,
                    "args_summary": _tool_args_summary(args),
                    "duration_ms": int((time.monotonic() - _t0) * 1000),
                    "result_preview": _tool_result_preview(result),
                })
            convo = [*convo, {
                "role": "tool",
                "tool_call_id": tc.get("id"),
                "content": json.dumps(result, default=str),
            }]

        if tool_state is not None and tool_state.tool_calls_used >= max_tool_calls:
            tools_withdrawn = True  # next turn is forced no-tools (final answer)

    raise AqueductError(
        f"OpenAI-compatible tool-use conversation exceeded "
        f"{_TOOL_LOOP_HARD_CEILING} round-trips without a final answer."
    )


def _iter_sse_data(resp: Any):
    """Yield the JSON payload of each ``data:`` SSE line (skips blanks/comments
    and the OpenAI ``[DONE]`` sentinel). ``resp`` is an open httpx streaming
    response."""
    import json as _json
    for line in resp.iter_lines():
        if not line or not line.startswith("data:"):
            continue
        chunk = line[len("data:"):].strip()
        if not chunk or chunk == "[DONE]":
            continue
        try:
            yield _json.loads(chunk)
        except ValueError:
            continue


def _stream_openai_compat(url, payload, headers, read_timeout, on_token) -> tuple[str, int, int]:
    """SSE streaming for OpenAI-compatible endpoints.

    Fires ``on_token('thinking'|'answer', text)`` per delta as the model
    generates, accumulating the answer. ``reasoning_content`` (deepseek-r1 /
    o-series via a compat gateway) streams as the thinking channel; ``content``
    is the answer. ``stream_options.include_usage`` requests a final usage frame
    (ignored by servers that don't support it → 0 tokens)."""
    import httpx
    payload = {**payload, "stream": True, "stream_options": {"include_usage": True}}
    # Drop structured-output enforcement on the streaming path: several endpoints
    # (Ollama included) BUFFER the whole response instead of emitting incremental
    # deltas when json_object format is requested, which defeats streaming. The
    # parser already recovers JSON from fenced / think-wrapped output.
    payload.pop("response_format", None)
    parts: list[str] = []
    ti = to = 0
    timeout = httpx.Timeout(connect=15.0, read=read_timeout, write=30.0, pool=5.0)
    with httpx.Client() as client, client.stream(
        "POST", url, json=payload, headers=headers, timeout=timeout,
    ) as resp:
        resp.raise_for_status()
        for obj in _iter_sse_data(resp):
            choices = obj.get("choices") or []
            if choices:
                delta = choices[0].get("delta") or {}
                rc = delta.get("reasoning_content")
                if rc:
                    on_token("thinking", rc)
                c = delta.get("content")
                if c:
                    parts.append(c)
                    on_token("answer", c)
            usage = obj.get("usage")
            if usage:
                ti = int(usage.get("prompt_tokens", 0) or 0)
                to = int(usage.get("completion_tokens", 0) or 0)
    text = "".join(parts)
    if not text:
        raise AqueductError(
            "LLM returned empty content from the streaming OpenAI-compatible endpoint."
        )
    return text, ti, to


def _stream_anthropic(url, payload, headers, read_timeout, on_token) -> tuple[str, int, int]:
    """SSE streaming for the Anthropic Messages API.

    ``thinking_delta`` (extended thinking) streams as the thinking channel;
    ``text_delta`` is the answer. Usage arrives on ``message_start`` (input) and
    ``message_delta`` (output)."""
    import httpx
    payload = {**payload, "stream": True}
    parts: list[str] = []
    ti = to = 0
    timeout = httpx.Timeout(connect=15.0, read=read_timeout, write=30.0, pool=5.0)
    with httpx.Client() as client, client.stream(
        "POST", url, json=payload, headers=headers, timeout=timeout,
    ) as resp:
        resp.raise_for_status()
        for obj in _iter_sse_data(resp):
            t = obj.get("type")
            if t == "content_block_delta":
                d = obj.get("delta") or {}
                if d.get("type") == "thinking_delta" and d.get("thinking"):
                    on_token("thinking", d["thinking"])
                elif d.get("type") == "text_delta" and d.get("text"):
                    parts.append(d["text"])
                    on_token("answer", d["text"])
            elif t == "message_start":
                u = (obj.get("message") or {}).get("usage") or {}
                ti = int(u.get("input_tokens", 0) or 0)
            elif t == "message_delta":
                u = obj.get("usage") or {}
                if u.get("output_tokens"):
                    to = int(u.get("output_tokens") or 0)
    text = "".join(parts)
    if not text:
        raise AqueductError("Anthropic returned empty text in the streaming response.")
    return text, ti, to
