"""LLM-driven patch loop — Phase 8.

On blueprint failure, packages a trimmed failure context as a prompt, calls the
configured LLM provider, validates the response as a PatchSpec, and either
applies it automatically or writes it to patches/pending/ for human review.

All providers use httpx — no optional SDK dependency required.

Supported providers:
  - "anthropic"     → Anthropic Messages API (ANTHROPIC_API_KEY env var)
  - "openai_compat" → any OpenAI-compatible endpoint (vLLM, LM Studio, Ollama, …)
                      Set agent.base_url in aqueduct.yml, e.g. http://localhost:11434/v1

Re-prompt strategy on schema failure:
  - Up to MAX_REPROMPTS attempts.
  - Each failed parse sends the validation error back to the model.
  - If all reprompts fail, logs warning and returns None (blueprint stays failed).

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
You are an expert Apache Spark blueprint repair agent for the Aqueduct blueprint engine.

A blueprint has failed. You will receive a structured failure report describing:
- What the blueprint does (human-readable summary)
- The failing module and its configuration (compiled/resolved values)
- The original Blueprint YAML source (pre-compilation, with template expressions intact)
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
- CRITICAL: When patching config values that contain paths, expressions, or strings, use the template expressions from the Original Blueprint YAML (e.g. "${{ctx.paths.input}}/*.parquet"), NOT the resolved literal values shown in the compiled module config. The patch is applied to the raw Blueprint, not the compiled Manifest.
{previous_patches_section}{custom_context_section}"""

_USER_PROMPT_TEMPLATE = """\
## Blueprint: {blueprint_name}
{blueprint_description}

## Execution flow
{blueprint_summary}

## Failure
- **Failed module**: `{failed_module}`
- **Error**: {error_message}

## Failed module config (compiled — paths/expressions are resolved)
```json
{failed_module_config}
```

## Stack trace (truncated)
```
{stack_trace}
```

## Full module list (for reference when writing patch IDs)
{module_list}
{blueprint_source_section}
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


def _build_blueprint_summary(manifest_dict: dict) -> str:
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
    blueprint_name = manifest.get("name") or failure_ctx.blueprint_id
    blueprint_desc = manifest.get("description", "")

    # Failed module config (compiled — resolved values)
    failed_mod = next((m for m in modules if m["id"] == failure_ctx.failed_module), None)
    failed_config = json.dumps(failed_mod.get("config", {}) if failed_mod else {}, indent=2)

    # Module list summary
    module_list = "\n".join(
        f"  - {m['id']} ({m['type']}): {m.get('label', '')}" for m in modules
    ) or "  (none)"

    # Raw Blueprint YAML (pre-compilation) — lets LLM see template expressions
    if failure_ctx.blueprint_source_yaml:
        blueprint_source_section = (
            "\n## Original Blueprint YAML (pre-compilation — use these template expressions in your patch)\n"
            "```yaml\n"
            f"{failure_ctx.blueprint_source_yaml.strip()}\n"
            "```\n"
        )
    else:
        blueprint_source_section = ""

    return _USER_PROMPT_TEMPLATE.format(
        blueprint_name=blueprint_name,
        blueprint_description=f"> {blueprint_desc}" if blueprint_desc else "",
        blueprint_summary=_build_blueprint_summary(manifest),
        failed_module=failure_ctx.failed_module,
        error_message=failure_ctx.error_message,
        failed_module_config=failed_config,
        stack_trace=_truncate_stack(failure_ctx.stack_trace),
        module_list=module_list,
        blueprint_source_section=blueprint_source_section,
    )


def _build_system_prompt(
    patches_dir: Path,
    engine_prompt_context: str | None = None,
    blueprint_prompt_context: str | None = None,
) -> str:
    schema = json.dumps(PatchSpec.model_json_schema(), indent=2)
    prev = _load_previous_patches(patches_dir)
    if prev:
        lines = ["\n## Previous patch attempts (do NOT repeat these)"]
        for p in prev:
            lines.append(f"- {p['patch_id']}: {p['description']} (ops: {', '.join(p['ops'])})")
        prev_section = "\n".join(lines)
    else:
        prev_section = ""

    # Merge engine-level and blueprint-level prompt_context (blueprint appends after engine)
    ctx_parts = [c for c in [engine_prompt_context, blueprint_prompt_context] if c and c.strip()]
    if ctx_parts:
        custom_context_section = "\n\n## Additional context (operator-supplied)\n" + "\n\n".join(ctx_parts)
    else:
        custom_context_section = ""

    return _SYSTEM_PROMPT_TEMPLATE.format(
        patch_schema=schema,
        previous_patches_section=prev_section,
        custom_context_section=custom_context_section,
    )


# ── LLM provider dispatch ─────────────────────────────────────────────────────

def _call_llm(
    messages: list[dict[str, Any]],
    model: str,
    max_tokens: int,
    provider: str,
    base_url: str | None,
    patches_dir: Path,
    ollama_options: dict[str, Any] | None = None,
    timeout: float = 120.0,
    engine_prompt_context: str | None = None,
    blueprint_prompt_context: str | None = None,
) -> str:
    """Call the configured LLM provider and return raw text response."""
    system_prompt = _build_system_prompt(patches_dir, engine_prompt_context, blueprint_prompt_context)

    if provider == "openai_compat":
        return _call_openai_compat(messages, model, max_tokens, base_url, system_prompt, ollama_options, timeout=timeout)
    else:
        return _call_anthropic(messages, model, max_tokens, system_prompt, timeout=timeout)


def _call_anthropic(
    messages: list[dict[str, Any]],
    model: str,
    max_tokens: int,
    system_prompt: str,
    timeout: float = 120.0,
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
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()["content"][0]["text"]


def _call_openai_compat(
    messages: list[dict[str, Any]],
    model: str,
    max_tokens: int,
    base_url: str | None,
    system_prompt: str,
    ollama_options: dict[str, Any] | None = None,
    timeout: float = 120.0,
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

    payload: dict[str, Any] = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "system", "content": system_prompt}] + messages,
    }
    if ollama_options:
        payload["options"] = ollama_options

    response = httpx.post(
        url,
        json=payload,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        timeout=timeout,
    )
    response.raise_for_status()
    data = response.json()
    return data["choices"][0]["message"]["content"]


# ── Response parsing ──────────────────────────────────────────────────────────

def _parse_patch_spec(text: str) -> PatchSpec:
    """Parse and validate LLM response as PatchSpec."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(lines[1:-1] if lines[-1].startswith("```") else lines[1:])
    return PatchSpec.model_validate_json(text)


# ── Public API ────────────────────────────────────────────────────────────────

def generate_llm_patch(
    failure_ctx: FailureContext,
    model: str,
    patches_dir: Path,
    provider: str = "anthropic",
    base_url: str | None = None,
    max_tokens: int = 4096,
    ollama_options: dict[str, Any] | None = None,
    llm_timeout: float = 120.0,
    llm_max_reprompts: int = MAX_REPROMPTS,
    engine_prompt_context: str | None = None,
    blueprint_prompt_context: str | None = None,
) -> PatchSpec | None:
    """Call the LLM and return a validated PatchSpec.

    Does not apply or stage the patch — caller decides what to do with it.

    Returns None if the LLM failed to produce a valid PatchSpec after
    llm_max_reprompts attempts.
    """
    messages: list[dict[str, Any]] = [
        {
            "role": "user",
            "content": _build_user_prompt(failure_ctx, patches_dir),
        }
    ]

    patch_spec: PatchSpec | None = None

    for attempt in range(llm_max_reprompts):
        try:
            raw = _call_llm(messages, model, max_tokens, provider, base_url, patches_dir, ollama_options, timeout=llm_timeout, engine_prompt_context=engine_prompt_context, blueprint_prompt_context=blueprint_prompt_context)
        except Exception as exc:
            logger.error(
                "LLM API call failed (attempt %d/%d): %s", attempt + 1, llm_max_reprompts, exc
            )
            break

        try:
            patch_spec = _parse_patch_spec(raw)
            break
        except (ValidationError, json.JSONDecodeError, ValueError) as exc:
            logger.warning(
                "LLM patch response invalid (attempt %d/%d): %s",
                attempt + 1, llm_max_reprompts, exc,
            )
            messages.append({"role": "assistant", "content": raw})
            messages.append({
                "role": "user",
                "content": _REPROMPT_TEMPLATE.format(error=str(exc)),
            })

    if patch_spec is None:
        logger.error(
            "LLM agent failed to produce a valid PatchSpec after %d attempts "
            "for blueprint %r run %r",
            llm_max_reprompts, failure_ctx.blueprint_id, failure_ctx.run_id,
        )
    return patch_spec


def _patch_filename(patch_spec: PatchSpec, patches_dir: Path) -> str:
    """Generate structured filename: {seq:05d}_{YYYYMMDDTHHmmss}_{slug}.json."""
    from datetime import datetime, timezone

    seq = 1 + sum(
        len(list(d.glob("*.json")))
        for d in [patches_dir / "pending", patches_dir / "applied", patches_dir / "rejected"]
        if d.exists()
    )
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{seq:05d}_{ts}_{patch_spec.patch_id}.json"


def stage_patch_for_human(
    patch_spec: PatchSpec,
    patches_dir: Path,
    failure_ctx: FailureContext,
) -> None:
    """Write patch to patches/pending/ for human review."""
    pending_dir = patches_dir / "pending"
    pending_dir.mkdir(parents=True, exist_ok=True)
    filename = _patch_filename(patch_spec, patches_dir)
    out_path = pending_dir / filename
    payload = patch_spec.model_dump()
    payload["_aq_meta"] = {
        "run_id": failure_ctx.run_id,
        "blueprint_id": failure_ctx.blueprint_id,
        "failed_module": failure_ctx.failed_module,
        "staged_at": _utcnow(),
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info(
        "LLM patch staged for human review: %s  "
        "(apply with: aqueduct patch apply %s --blueprint <path>)",
        out_path, out_path,
    )


def archive_patch(
    patch_spec: PatchSpec,
    patches_dir: Path,
    failure_ctx: FailureContext,
    mode: str,
) -> None:
    """Write patch to patches/applied/ with metadata."""
    applied_dir = patches_dir / "applied"
    applied_dir.mkdir(parents=True, exist_ok=True)
    filename = _patch_filename(patch_spec, patches_dir)
    archive_path = applied_dir / filename
    payload = patch_spec.model_dump()
    payload["_aq_meta"] = {
        "run_id": failure_ctx.run_id,
        "blueprint_id": failure_ctx.blueprint_id,
        "failed_module": failure_ctx.failed_module,
        "applied_at": _utcnow(),
        "approval_mode": mode,
    }
    archive_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _utcnow() -> str:
    from datetime import datetime, timezone
    return datetime.now(tz=timezone.utc).isoformat()
