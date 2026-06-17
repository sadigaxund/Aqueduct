"""LLM provider dispatch — Anthropic Messages API and OpenAI-compatible endpoints.

All providers use httpx; no optional SDK dependency required.

This module owns:
  - Provider HTTP dispatch (_call_anthropic, _call_openai_compat)
  - Provider selection and system-prompt injection (_call_agent)
  - LLM error hint formatting (_format_llm_error_hint)
"""

from __future__ import annotations

import logging
import os
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from aqueduct.agent.prompts import _build_system_prompt
from aqueduct.redaction import redact as _redact

logger = logging.getLogger(__name__)

# Temperature applied to the ONE escalated attempt that follows a
# stuck-consecutive trip. 0.8 forces sampling divergence so the model
# doesn't keep regenerating the same wrong tree.
_ESCALATION_TEMPERATURE = 0.8

# Phase 46 — transient provider errors worth retrying. 429 rate limit,
# 503 service unavailable, 529 Anthropic "overloaded".
_RETRYABLE_STATUS = frozenset({429, 503, 529})

_ANTHROPIC_DEFAULT_BASE_URL = "https://api.anthropic.com"


def _retry_after_seconds(response: Any) -> float | None:
    """Parse a Retry-After header (delta-seconds form only) — None when absent/invalid."""
    raw = response.headers.get("retry-after")
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return None


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
            sleep = backoff_seconds * (2 ** (attempt - 1)) * (1.0 + random.random() * 0.25)
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
    max_tokens: int = 4096
    provider: str = "anthropic"
    base_url: str | None = None
    provider_options: dict[str, Any] | None = None
    timeout: float = 120.0
    patches_dir: Path = Path()
    engine_prompt_context: str | None = None
    blueprint_prompt_context: str | None = None
    allow_defer: bool = False
    # Phase 45 coaching: the FailureContext keys signature-matched retrieval
    # of past (failure → validated fix) pairs into the system prompt. None
    # (or coaching=False) falls back to the chronological patch-history section.
    failure_ctx: Any = None
    coaching: bool = True
    # Phase 53 — observability store backing the patch_index, used by the system
    # prompt's coaching + history sections (replaces the patches/ dir scan).
    obs_store: Any = None
    # Phase 46 — transient-error retry (429/503/529), from agent.retry config.
    retry_max_retries: int = 2
    retry_backoff_seconds: float = 2.0


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
        suggestion = (
            f"\n  hint: timed out after {seconds}. "
            f"Local model cold-start (first call after Ollama restart) can take "
            f"30–90s extra. Options:\n"
            f"    1. Raise the timeout:  --timeout 600  "
            f"(or set agent.timeout in aqueduct.yml)\n"
            f"    2. Pre-warm the model before benchmarking:"
        )
        if base_url:
            ollama_url = base_url.rstrip("/").removesuffix("/v1")
            suggestion += (
                f"\n         curl -sS {ollama_url}/api/generate "
                f'-d \'{{"model":"{model}","prompt":"hi","stream":false}}\''
            )
        else:
            suggestion += (
                f'\n         curl -sS <ollama_url>/api/generate '
                f'-d \'{{"model":"{model}","prompt":"hi","stream":false}}\''
            )
        return suggestion

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
) -> tuple[str, int, int]:
    """Call the LLM provider; return (text, tokens_in, tokens_out).

    ``temperature_override`` lets the caller force a higher sampling
    temperature on the escalated attempt without mutating the caller-supplied
    configuration.

    ``deadline`` (Phase 40) overrides the per-call HTTP timeout to enforce
    the budget's ``max_seconds`` mid-call. When set, it replaces the static
    ``cfg.timeout`` for this single call.

    Token counts come from the provider response when reported; 0 otherwise.
    """
    system_prompt = _build_system_prompt(
        patches_dir,
        cfg.engine_prompt_context,
        cfg.blueprint_prompt_context,
        last_apply_error,
        allow_defer=cfg.allow_defer,
        failure_ctx=cfg.failure_ctx,
        coaching=cfg.coaching,
        obs_store=cfg.obs_store,
    )

    # Scrub registered @aq.secret() values from anything leaving the process.
    system_prompt = _redact(system_prompt)
    messages = _redact(messages)

    if cfg.provider == "openai_compat":
        return _call_openai_compat(
            messages, cfg.model, cfg.max_tokens, cfg.base_url, system_prompt,
            cfg.provider_options, timeout=cfg.timeout,
            temperature_override=temperature_override,
            deadline=deadline,
            max_retries=cfg.retry_max_retries,
            backoff_seconds=cfg.retry_backoff_seconds,
        )
    else:
        return _call_anthropic(
            messages, cfg.model, cfg.max_tokens, system_prompt,
            timeout=cfg.timeout,
            temperature_override=temperature_override,
            deadline=deadline,
            base_url=cfg.base_url,
            provider_options=cfg.provider_options,
            max_retries=cfg.retry_max_retries,
            backoff_seconds=cfg.retry_backoff_seconds,
        )


def _call_anthropic(
    messages: list[dict[str, Any]],
    model: str,
    max_tokens: int,
    system_prompt: str,
    timeout: float = 120.0,
    temperature_override: float | None = None,
    deadline: float | None = None,
    base_url: str | None = None,
    provider_options: dict[str, Any] | None = None,
    max_retries: int = 2,
    backoff_seconds: float = 2.0,
) -> tuple[str, int, int]:
    import httpx

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError(
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
    with httpx.Client() as client:
        def _do_post(read_timeout: float) -> httpx.Response:
            return client.post(
                url,
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
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
        raise ValueError(
            "Anthropic returned empty content block. "
            "The model may have refused the request or been interrupted."
        )
    text = content[0].get("text")
    if text is None:
        raise ValueError(
            "Anthropic returned null/empty text in content block. "
            "The model may have refused the request or been interrupted."
        )
    usage = data.get("usage") or {}
    return text, int(usage.get("input_tokens", 0) or 0), int(usage.get("output_tokens", 0) or 0)


def _call_openai_compat(
    messages: list[dict[str, Any]],
    model: str,
    max_tokens: int,
    base_url: str | None,
    system_prompt: str,
    provider_options: dict[str, Any] | None = None,
    timeout: float = 120.0,
    temperature_override: float | None = None,
    deadline: float | None = None,
    max_retries: int = 2,
    backoff_seconds: float = 2.0,
) -> tuple[str, int, int]:
    """Call any OpenAI-compatible endpoint (Ollama, vLLM, LM Studio, etc.)."""
    import httpx

    if not base_url:
        raise RuntimeError(
            "agent.base_url must be set for provider=openai_compat "
            "(e.g. http://localhost:11434/v1)"
        )

    api_key = os.environ.get("OPENAI_API_KEY", "ollama")
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
    with httpx.Client() as client:
        def _do_post(read_timeout: float) -> httpx.Response:
            return client.post(
                url,
                json=payload,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
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
        raise ValueError(
            "LLM returned null/empty content from OpenAI-compatible endpoint. "
            "The model may have refused the request, hit a content filter, "
            "or been interrupted mid-generation."
        )
    usage = data.get("usage") or {}
    return text, int(usage.get("prompt_tokens", 0) or 0), int(usage.get("completion_tokens", 0) or 0)
