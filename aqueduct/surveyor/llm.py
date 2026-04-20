"""LLM-driven patch loop — Phase 8.

On pipeline failure, packages a trimmed failure context as a prompt, calls the
configured LLM provider, validates the response as a PatchSpec, and either
applies it automatically or writes it to patches/pending/ for human review.

All providers use httpx — no optional SDK dependency required.

Supported providers:
  - "anthropic"     → Anthropic Messages API (ANTHROPIC_API_KEY env var)
  - "openai_compat" → any OpenAI-compatible endpoint (vLLM, LM Studio, newer Ollama …)
                      Set agent.base_url in aqueduct.yml, e.g. http://localhost:11434/v1
  - "ollama"        → Ollama native API at /api/chat (always works regardless of Ollama version)
                      Set agent.base_url in aqueduct.yml, e.g. http://<IP ADDRESS>:11434

Re-prompt strategy on schema failure:
  - Up to MAX_REPROMPTS attempts.
  - Each failed parse sends the validation error back to the model.
  - If all reprompts fail, logs warning and returns None (pipeline stays failed).

Approval modes:
  - "auto"  → apply patch immediately via apply_patch_to_dict + write Blueprint
  - "human" → write PatchSpec to patches/pending/<patch_id>.json
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from aqueduct.patch.grammar import PatchSpec
from aqueduct.surveyor.models import FailureContext

logger = logging.getLogger(__name__)

MAX_REPROMPTS = 3
_STACK_TRACE_MAX_LINES = 25
_PATCH_HISTORY_MAX = 3


# ── Prompt construction ───────────────────────────────────────────────────────

_SYSTEM_PROMPT_TEMPLATE = """\
You are an expert Apache Spark pipeline repair agent for the Aqueduct pipeline engine.

A pipeline has failed. You will receive a structured failure report describing:
- What the pipeline does (human-readable summary)
- The failing module and its configuration
- The error message and relevant stack trace lines
- Previous patch attempts (if any) — do NOT repeat a fix that was already tried

Your task: produce a PatchSpec JSON that fixes the root cause.

## PatchSpec Schema
{patch_schema}

## Rules
- Respond with ONLY valid JSON matching the PatchSpec schema above. No prose, no markdown fences.
- Use only module IDs and field names that appear in the failure report.
- Prefer the simplest fix: correct a config value before adding new modules.
- SQL syntax error → fix the query via replace_module_config.
- Wrong path or format → fix those config keys only.
- patch_id: short slug (e.g. "fix-sql-syntax-clean-orders").
- description: root cause + fix in one sentence.
{previous_patches_section}"""

_USER_PROMPT_TEMPLATE = """\
## Pipeline: {pipeline_name}
{pipeline_description}

## Execution flow
{pipeline_summary}

## Failure
- **Failed module**: `{failed_module}`
- **Error**: {error_message}

## Failed module config
```json
{failed_module_config}
```

## Stack trace (truncated)
```
{stack_trace}
```

## Full module list (for reference when writing patch IDs)
{module_list}

Produce the PatchSpec JSON now.
"""

_REPROMPT_TEMPLATE = """\
Your previous response was not valid PatchSpec JSON. Validation error:

{error}

Produce a corrected PatchSpec JSON now.
"""


def _truncate_stack(trace: str | None, max_lines: int = _STACK_TRACE_MAX_LINES) -> str:
    if not trace:
        return "(no stack trace)"
    lines = trace.splitlines()
    if len(lines) <= max_lines:
        return trace
    kept = lines[:max_lines]
    kept.append(f"... ({len(lines) - max_lines} more lines truncated)")
    return "\n".join(kept)


def _build_pipeline_summary(manifest_dict: dict) -> str:
    """One-line human description of the execution flow."""
    modules = manifest_dict.get("modules", [])
    edges = manifest_dict.get("edges", [])
    if not modules:
        return "(no modules)"

    # Build adjacency for simple chain detection
    successors: dict[str, list[str]] = {m["id"]: [] for m in modules}
    predecessors: dict[str, list[str]] = {m["id"]: [] for m in modules}
    for e in edges:
        if e.get("port", "main") == "main":
            successors[e["from"]].append(e["to"])
            predecessors[e["to"]].append(e["from"])

    # Walk from root(s) to build a simple summary
    roots = [m for m in modules if not predecessors[m["id"]]]
    parts: list[str] = []
    visited: set[str] = set()

    def _walk(mid: str) -> None:
        if mid in visited:
            return
        visited.add(mid)
        m = next((x for x in modules if x["id"] == mid), None)
        if m:
            parts.append(f"{m['type']}({m['id']})")
        for succ in successors.get(mid, []):
            _walk(succ)

    for r in roots:
        _walk(r["id"])

    return " → ".join(parts) if parts else "(complex graph)"


def _load_previous_patches(patches_dir: Path, limit: int = _PATCH_HISTORY_MAX) -> list[dict]:
    applied_dir = patches_dir / "applied"
    if not applied_dir.exists():
        return []
    patches = []
    for p in sorted(applied_dir.glob("*.json"), key=lambda f: f.stat().st_mtime, reverse=True)[:limit]:
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            patches.append({
                "patch_id": data.get("patch_id", p.stem),
                "description": data.get("description", ""),
                "ops": [op.get("op", "?") for op in data.get("operations", [])],
            })
        except Exception:
            pass
    return patches


def _build_user_prompt(failure_ctx: FailureContext, patches_dir: Path) -> str:
    try:
        manifest = json.loads(failure_ctx.manifest_json)
    except Exception:
        manifest = {}

    modules = manifest.get("modules", [])
    pipeline_name = manifest.get("name") or failure_ctx.pipeline_id
    pipeline_desc = manifest.get("description", "")

    # Failed module config
    failed_mod = next((m for m in modules if m["id"] == failure_ctx.failed_module), None)
    failed_config = json.dumps(failed_mod.get("config", {}) if failed_mod else {}, indent=2)

    # Module list summary
    module_list = "\n".join(
        f"  - {m['id']} ({m['type']}): {m.get('label', '')}" for m in modules
    ) or "  (none)"

    return _USER_PROMPT_TEMPLATE.format(
        pipeline_name=pipeline_name,
        pipeline_description=f"> {pipeline_desc}" if pipeline_desc else "",
        pipeline_summary=_build_pipeline_summary(manifest),
        failed_module=failure_ctx.failed_module,
        error_message=failure_ctx.error_message,
        failed_module_config=failed_config,
        stack_trace=_truncate_stack(failure_ctx.stack_trace),
        module_list=module_list,
    )


def _build_system_prompt(patches_dir: Path) -> str:
    schema = json.dumps(PatchSpec.model_json_schema(), indent=2)
    prev = _load_previous_patches(patches_dir)
    if prev:
        lines = ["\n## Previous patch attempts (do NOT repeat these)"]
        for p in prev:
            lines.append(f"- {p['patch_id']}: {p['description']} (ops: {', '.join(p['ops'])})")
        prev_section = "\n".join(lines)
    else:
        prev_section = ""
    return _SYSTEM_PROMPT_TEMPLATE.format(
        patch_schema=schema,
        previous_patches_section=prev_section,
    )


# ── LLM provider dispatch ─────────────────────────────────────────────────────

def _call_llm(
    messages: list[dict[str, Any]],
    model: str,
    max_tokens: int,
    provider: str,
    base_url: str | None,
    patches_dir: Path,
) -> str:
    """Call the configured LLM provider and return raw text response."""
    system_prompt = _build_system_prompt(patches_dir)

    if provider == "openai_compat":
        return _call_openai_compat(messages, model, max_tokens, base_url, system_prompt)
    elif provider == "ollama":
        return _call_ollama_native(messages, model, max_tokens, base_url, system_prompt)
    else:
        return _call_anthropic(messages, model, max_tokens, system_prompt)


def _call_anthropic(
    messages: list[dict[str, Any]],
    model: str,
    max_tokens: int,
    system_prompt: str,
) -> str:
    import httpx

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY environment variable not set. "
            "Set it or configure agent.provider: openai_compat in aqueduct.yml."
        )
    response = httpx.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": model,
            "max_tokens": max_tokens,
            "system": system_prompt,
            "messages": messages,
        },
        timeout=120.0,
    )
    response.raise_for_status()
    return response.json()["content"][0]["text"]


def _call_openai_compat(
    messages: list[dict[str, Any]],
    model: str,
    max_tokens: int,
    base_url: str | None,
    system_prompt: str,
) -> str:
    """Call any OpenAI-compatible endpoint (Ollama, vLLM, LM Studio, etc.)."""
    import httpx

    if not base_url:
        raise RuntimeError(
            "agent.base_url must be set for provider=openai_compat "
            "(e.g. http://localhost:11434/v1)"
        )

    api_key = os.environ.get("OPENAI_API_KEY", "ollama")  # Ollama ignores key
    url = base_url.rstrip("/") + "/chat/completions"

    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "system", "content": system_prompt}] + messages,
    }

    response = httpx.post(
        url,
        json=payload,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        timeout=120.0,
    )
    response.raise_for_status()
    data = response.json()
    return data["choices"][0]["message"]["content"]


def _call_ollama_native(
    messages: list[dict[str, Any]],
    model: str,
    max_tokens: int,
    base_url: str | None,
    system_prompt: str,
) -> str:
    """Call Ollama native /api/chat with streaming — prints dots to stderr as tokens arrive."""
    import sys

    import httpx

    if not base_url:
        raise RuntimeError(
            "agent.base_url must be set for provider=ollama "
            "(e.g. http://10.0.0.39:11434)"
        )

    url = base_url.rstrip("/") + "/api/chat"
    payload = {
        "model": model,
        "stream": True,
        "options": {"num_predict": max_tokens},
        "messages": [{"role": "system", "content": system_prompt}] + messages,
    }

    print(f"\n[aqueduct llm] {model} thinking", end="", flush=True, file=sys.stderr)
    chunks: list[str] = []
    timeout = httpx.Timeout(connect=10.0, read=300.0, write=10.0, pool=10.0)
    with httpx.stream("POST", url, json=payload, timeout=timeout) as response:
        response.raise_for_status()
        for line in response.iter_lines():
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            content = data.get("message", {}).get("content", "")
            if content:
                chunks.append(content)
                print(".", end="", flush=True, file=sys.stderr)
            if data.get("done"):
                break
    print(" done", flush=True, file=sys.stderr)
    return "".join(chunks)


# ── Response parsing ──────────────────────────────────────────────────────────

def _parse_patch_spec(text: str) -> PatchSpec:
    """Parse and validate LLM response as PatchSpec."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(lines[1:-1] if lines[-1].startswith("```") else lines[1:])
    return PatchSpec.model_validate_json(text)


# ── Public API ────────────────────────────────────────────────────────────────

def trigger_llm_patch(
    failure_ctx: FailureContext,
    model: str,
    api_endpoint: str,
    max_tokens: int,
    approval_mode: str,
    blueprint_path: Path | None,
    patches_dir: Path,
    provider: str = "anthropic",
    base_url: str | None = None,
) -> PatchSpec | None:
    """Run the LLM patch loop for a failed pipeline run.

    Args:
        failure_ctx:    FailureContext built by Surveyor.
        model:          Model ID (e.g. claude-sonnet-4-6 or gemma3).
        api_endpoint:   Unused — kept for signature compat.
        max_tokens:     Max tokens per LLM response.
        approval_mode:  "auto" | "human".
        blueprint_path: Path to Blueprint YAML (required for auto mode).
        patches_dir:    Root directory for patch lifecycle subdirs.
        provider:       "anthropic" | "openai_compat" | "ollama".
        base_url:       Base URL for openai_compat/ollama (e.g. http://<IP ADDRESS>:11434).

    Returns:
        Applied or staged PatchSpec, or None if LLM failed to produce a valid patch.
    """
    messages: list[dict[str, Any]] = [
        {
            "role": "user",
            "content": _build_user_prompt(failure_ctx, patches_dir),
        }
    ]

    patch_spec: PatchSpec | None = None

    for attempt in range(MAX_REPROMPTS):
        try:
            raw = _call_llm(messages, model, max_tokens, provider, base_url, patches_dir)
        except Exception as exc:
            logger.error(
                "LLM API call failed (attempt %d/%d): %s", attempt + 1, MAX_REPROMPTS, exc
            )
            break

        try:
            patch_spec = _parse_patch_spec(raw)
            break
        except (ValidationError, json.JSONDecodeError, ValueError) as exc:
            logger.warning(
                "LLM patch response invalid (attempt %d/%d): %s",
                attempt + 1, MAX_REPROMPTS, exc,
            )
            messages.append({"role": "assistant", "content": raw})
            messages.append({
                "role": "user",
                "content": _REPROMPT_TEMPLATE.format(error=str(exc)),
            })

    if patch_spec is None:
        logger.error(
            "LLM agent failed to produce a valid PatchSpec after %d attempts "
            "for pipeline %r run %r",
            MAX_REPROMPTS, failure_ctx.pipeline_id, failure_ctx.run_id,
        )
        return None

    if approval_mode == "human":
        return _stage_for_human(patch_spec, patches_dir, failure_ctx)

    if blueprint_path is None or not blueprint_path.exists():
        logger.error(
            "approval_mode=auto but blueprint_path missing (%s); falling back to human staging.",
            blueprint_path,
        )
        return _stage_for_human(patch_spec, patches_dir, failure_ctx)

    return _auto_apply(patch_spec, blueprint_path, patches_dir, failure_ctx)


# ── Dispatch helpers ──────────────────────────────────────────────────────────

def _stage_for_human(
    patch_spec: PatchSpec,
    patches_dir: Path,
    failure_ctx: FailureContext,
) -> PatchSpec:
    pending_dir = patches_dir / "pending"
    pending_dir.mkdir(parents=True, exist_ok=True)
    out_path = pending_dir / f"{patch_spec.patch_id}.json"
    payload = patch_spec.model_dump()
    payload["_aq_meta"] = {
        "run_id": failure_ctx.run_id,
        "pipeline_id": failure_ctx.pipeline_id,
        "failed_module": failure_ctx.failed_module,
        "staged_at": _utcnow(),
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info(
        "LLM patch staged for human review: %s  "
        "(apply with: aqueduct patch apply %s --blueprint <path>)",
        out_path, out_path,
    )
    return patch_spec


def _auto_apply(
    patch_spec: PatchSpec,
    blueprint_path: Path,
    patches_dir: Path,
    failure_ctx: FailureContext,
) -> PatchSpec | None:
    try:
        import os as _os
        import tempfile

        from aqueduct.parser.parser import ParseError, parse
        from aqueduct.patch.apply import _yaml_dump, _yaml_load, apply_patch_to_dict

        bp_raw = _yaml_load(blueprint_path)
        patched = apply_patch_to_dict(bp_raw, patch_spec)

        with tempfile.NamedTemporaryFile(suffix=".yml", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        _yaml_dump(patched, tmp_path)

        try:
            parse(str(tmp_path))
        except ParseError as exc:
            logger.error("LLM patch produces invalid Blueprint: %s; not applying.", exc)
            return None
        finally:
            tmp_path.unlink(missing_ok=True)

        applied_dir = patches_dir / "applied"
        applied_dir.mkdir(parents=True, exist_ok=True)
        archive_path = applied_dir / f"{patch_spec.patch_id}.json"
        payload = patch_spec.model_dump()
        payload["_aq_meta"] = {
            "run_id": failure_ctx.run_id,
            "pipeline_id": failure_ctx.pipeline_id,
            "applied_at": _utcnow(),
            "auto_applied": True,
        }
        archive_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

        tmp_out = blueprint_path.with_suffix(".llm_patch.tmp.yml")
        _yaml_dump(patched, tmp_out)
        _os.replace(tmp_out, blueprint_path)

        logger.info(
            "LLM patch auto-applied: %s  (%d operations)  archived → %s",
            patch_spec.patch_id, len(patch_spec.operations), archive_path,
        )
        return patch_spec

    except Exception as exc:
        logger.error("LLM auto-apply failed: %s", exc)
        return None


def _utcnow() -> str:
    from datetime import datetime, timezone
    return datetime.now(tz=timezone.utc).isoformat()
