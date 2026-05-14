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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from aqueduct.patch.grammar import PatchSpec
from aqueduct.surveyor.models import FailureContext

logger = logging.getLogger(__name__)


@dataclass
class LLMPatchResult:
    """Return value of generate_llm_patch — patch + attempt metadata."""
    patch: PatchSpec | None
    attempts: int              # total LLM calls made (1 = succeeded first try)
    reprompt_errors: list[str] = field(default_factory=list)  # validation errors per failed attempt

# Bump manually when the system prompt changes significantly.
# Stored in patch _aq_meta so benchmark results can be correlated to prompt versions.
PROMPT_VERSION = "1.0"

MAX_REPROMPTS = 3
_STACK_TRACE_MAX_LINES = 25
_PATCH_HISTORY_MAX = 3


# ── Prompt construction ───────────────────────────────────────────────────────

_SYSTEM_PROMPT_TEMPLATE = """\
You are an expert Apache Spark blueprint repair agent for the Aqueduct blueprint engine.

A blueprint has failed. You will receive a structured failure report describing:
- What the blueprint does (human-readable summary)
- The failing module and its resolved configuration
- A "Value provenance" section showing exactly where each config value comes from in the Blueprint source
- The error message and relevant stack trace lines
- Previous patch attempts (if any) — do NOT repeat a fix that was already tried

Your task: produce a PatchSpec JSON that fixes the root cause.

## PatchSpec Schema
{patch_schema}

## Rules

### Patch op disambiguation (READ THIS FIRST)
There are exactly TWO ops for changing module config. They are distinct:
- `set_module_config_key` — sets ONE field inside an existing module config, leaving all other fields unchanged. Use for literal values only.
- `replace_module_config` — replaces the ENTIRE config block. Use only when restructuring the whole config.
- **THERE IS NO OP NAMED `replace_module_config_key`.** That op does not exist. Do not use it.

### Provenance-driven op selection
- **Always read the "Value provenance" section before choosing a patch op.**
- For a `context_ref` value: use `replace_context_value` with the dot-notation key shown. Do NOT use `set_module_config_key`.
- For an `arcade_inherited` value with a `context_key`: the arcade inherits this value from the parent blueprint's context via context_override. Look in the "Blueprint context values" section to find which PARENT context key has the same resolved value, then use `replace_context_value` with the PARENT key. One op is the complete fix.
- For a `literal` value: use `set_module_config_key` directly on the module.
- For an `env_ref` value: do NOT patch the Blueprint; the value comes from an environment variable.
- NEVER use `set_module_config_key` on a module whose ID contains `__` — those are arcade-expanded and do NOT exist in the Blueprint YAML.
  WRONG: {{"op": "set_module_config_key", "module_id": "yellow_process__ingress", "key": "path", "value": "..."}}
  RIGHT: {{"op": "replace_context_value", "key": "paths.yellow_path", "value": "..."}}
- NEVER use `replace_module_config` for a single-field fix — it silently drops every key you forget to re-emit.

### Path values
- **ALWAYS use relative paths** (e.g. `data/yellow/*.parquet`). NEVER construct absolute paths even if the provenance section shows an absolute `blueprint_path`. The blueprint_path is shown for reference only — never use it to build a config value.
- Preserve the original path format (relative stays relative, globs stay globs).
- **When fixing a `context_ref` path, patch the context value using template expressions** (e.g. `replace_context_value` with key `paths.foo`). Do NOT hard-code the resolved literal path into a module config — using template expressions keeps the blueprint portable and context-driven.

### Other rules
- Use only module IDs and field names that appear in the failure report.
- Prefer the simplest fix: correct a config value before adding new modules.
- SQL query wrong → `set_module_config_key` with key="query".
- SQL Channel queries reference upstream module IDs as Spark temp view names (e.g. `FROM yellow_process__ingress`). NEVER use `${{ctx.*}}` inside a SQL query string.
- If a Channel fails with unexpected column names (e.g. AnalysisException: cannot resolve column), check whether an upstream Ingress has the wrong `format` — Spark can silently misread Parquet as CSV. Fix the Ingress `format`, not the Channel SQL.

## Required output — complete example
Every response MUST be a single JSON object with ALL of these fields. The `operations` field is MANDATORY — a response without it is always wrong.

```json
{{
  "patch_id": "fix-example-path",
  "rationale": "One sentence: what was wrong and what the fix does.",
  "confidence": 0.9,
  "category": "bad_path",
  "root_cause": "One sentence: root cause.",
  "operations": [
    {{
      "op": "set_module_config_key",
      "module_id": "my_ingress",
      "key": "format",
      "value": "csv"
    }}
  ]
}}
```

Field rules:
- patch_id: short slug (e.g. "fix-yellow-taxi-path")
- rationale: field is named "rationale" — do NOT use "description"
- confidence: float 0.0–1.0 — required
- category: one of: schema_drift, bad_path, format_mismatch, oom_config, sql_column_not_found, type_mismatch, missing_context, permission_error, other
- root_cause: one-sentence diagnosis
- operations: list of patch ops — use "op" as the key (NOT "type" or "action")

Respond with ONLY valid JSON. No prose, no markdown fences, no explanation outside the JSON object.
{previous_patches_section}{custom_context_section}"""

_USER_PROMPT_TEMPLATE = """\
## Blueprint: {blueprint_name}
{blueprint_description}

## Execution flow
{blueprint_summary}

## Failure
- **Failed module**: `{failed_module}`
- **Error**: {error_message}

## Failed module config (compiled — all expressions resolved)
```json
{failed_module_config}
```

## Stack trace (truncated)
```
{stack_trace}
```

## Full module list (for reference when writing patch IDs)
{module_list}
{provenance_section}{blueprint_yaml_section}{doctor_hints_section}
Produce the complete PatchSpec JSON now. Remember: the `operations` list is REQUIRED — a response without it is invalid.
"""

_REPROMPT_TEMPLATE = """\
Your previous response failed PatchSpec validation.

## Your previous output (what you sent)
```json
{raw_truncated}
```

## Errors to fix
{error}

Rewrite the COMPLETE PatchSpec JSON from scratch, fixing the errors above. Output ONLY valid JSON — no prose, no markdown fences.
"""

# Known field name mistakes the LLM commonly makes → correct name
_FIELD_ALIASES: dict[str, str] = {
    "description": "rationale",
    "ops": "operations",
    "op_list": "operations",
    "patches": "operations",
    "steps": "operations",
    "fix": "operations",
}

# Wrong discriminator keys LLM uses instead of "op"
_OP_DISCRIMINATOR_ALIASES = ("type", "action", "operation", "method", "kind", "name")

_VALID_OPS = (
    "replace_module_config", "set_module_config_key", "replace_module_label",
    "insert_module", "remove_module", "replace_context_value", "add_probe",
    "replace_edge", "set_module_on_failure", "replace_retry_policy", "add_arcade_ref",
)


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
            # Skip unreadable / malformed patch history files — they should not
            # block prompt construction. Log at DEBUG so `-v` users can diagnose
            # missing patches in the prompt section.
            logger.debug("Skipping unreadable patch history file %s", p, exc_info=True)
    return patches


def _build_provenance_section(provenance_json: str | None) -> str:
    """Build the value provenance section for the LLM user prompt.

    Shows where each config value of the failing module came from in the Blueprint
    source, with actionable patch op guidance per source type.
    """
    if not provenance_json:
        return ""
    try:
        prov = json.loads(provenance_json)
    except Exception:
        return ""

    lines: list[str] = ["\n## Value provenance (where each config value originates in the Blueprint source)"]
    bp_path_raw = prov.get("blueprint_path", "?")
    bp_path_display = Path(bp_path_raw).name if bp_path_raw and bp_path_raw != "?" else "?"
    lines.append(f"Blueprint: {bp_path_display}  (paths shown are for reference; always use relative paths in patch ops)")

    failed_mod = prov.get("failed_module")
    if failed_mod:
        mid = failed_mod.get("module_id", "?")
        mtype = failed_mod.get("module_type", "?")
        lines.append(f"\n### Failed module: {mid} ({mtype})")
        if failed_mod.get("arcade_module_id"):
            lines.append(
                f"  **Arcade-expanded** from arcade module `{failed_mod['arcade_module_id']}` "
                f"(sub-blueprint: {failed_mod.get('sub_blueprint_path', '?')})"
            )
            lines.append(
                f"  Original sub-module ID inside sub-blueprint: `{failed_mod.get('original_module_id', '?')}`"
            )
            lines.append(
                "  **This module does NOT exist in the Blueprint YAML.** "
                "Do not use set_module_config_key on this module_id."
            )

        for key, vp in (failed_mod.get("config") or {}).items():
            src = vp.get("source_type", "?")
            orig = vp.get("original_expression", "?")
            resolved = vp.get("resolved_value")
            ctx_key = vp.get("context_key")

            if src == "context_ref":
                lines.append(
                    f"  config.{key} = {resolved!r}"
                    f"  ← context_ref  →  use replace_context_value(key={ctx_key!r}, value=<fix>)"
                )
            elif src == "arcade_inherited":
                if ctx_key:
                    lines.append(
                        f"  config.{key} = {resolved!r}"
                        f"  ← arcade_inherited via context_override key {ctx_key!r}"
                        f"  →  find parent context key with this resolved value and use replace_context_value"
                    )
                else:
                    lines.append(
                        f"  config.{key} = {resolved!r}"
                        f"  ← arcade_inherited (expr: {orig!r})"
                        f"  →  fix the arcade module's context_override in the parent Blueprint"
                    )
            elif src == "env_ref":
                env_var = vp.get("env_var", "?")
                lines.append(
                    f"  config.{key} = {resolved!r}"
                    f"  ← env_ref ${{{env_var}}}  →  change the environment variable, do NOT patch the Blueprint"
                )
            elif src == "tier1":
                lines.append(
                    f"  config.{key} = {resolved!r}"
                    f"  ← @aq.* expression: {orig!r}"
                )
            else:
                lines.append(
                    f"  config.{key} = {resolved!r}"
                    f"  ← literal  →  use set_module_config_key(module_id={mid!r}, key={key!r}, value=<fix>)"
                )

    # Context block — helps LLM find the parent context key for arcade_inherited values
    ctx_block = prov.get("context") or {}
    if ctx_block:
        lines.append("\n### Blueprint context values (all resolved — editable via replace_context_value)")
        for key, vp in ctx_block.items():
            resolved = vp.get("resolved_value")
            src = vp.get("source_type", "?")
            env_hint = f" (from env ${{{vp['env_var']}}})" if src == "env_ref" else ""
            lines.append(f"  {key} = {resolved!r}{env_hint}")

    lines.append("")
    return "\n".join(lines)


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

    # Provenance section (replaces raw Blueprint YAML dump)
    provenance_section = _build_provenance_section(getattr(failure_ctx, "provenance_json", None))

    # Doctor pre-flight hints (warn/fail from check_blueprint_sources_from_manifest)
    hints = getattr(failure_ctx, "doctor_hints", None) or ()
    if hints:
        hint_lines = "\n".join(f"- {h}" for h in hints)
        doctor_hints_section = (
            "\n## Blueprint issues detected before run (may explain the failure)\n"
            f"{hint_lines}\n"
        )
    else:
        doctor_hints_section = ""

    # Original Blueprint YAML — gives LLM the authoring-level view (unresolved expressions)
    blueprint_yaml_section = ""
    raw_yaml = getattr(failure_ctx, "blueprint_source_yaml", None)
    if raw_yaml:
        blueprint_yaml_section = (
            "\n## Original Blueprint YAML (unresolved — authoring view)\n"
            "```yaml\n"
            f"{raw_yaml.strip()}\n"
            "```\n"
        )

    return _USER_PROMPT_TEMPLATE.format(
        blueprint_name=blueprint_name,
        blueprint_description=f"> {blueprint_desc}" if blueprint_desc else "",
        blueprint_summary=_build_blueprint_summary(manifest),
        failed_module=failure_ctx.failed_module,
        error_message=failure_ctx.error_message,
        failed_module_config=failed_config,
        stack_trace=_truncate_stack(failure_ctx.stack_trace),
        module_list=module_list,
        provenance_section=provenance_section,
        blueprint_yaml_section=blueprint_yaml_section,
        doctor_hints_section=doctor_hints_section,
    )


def _build_system_prompt(
    patches_dir: Path,
    engine_prompt_context: str | None = None,
    blueprint_prompt_context: str | None = None,
    last_apply_error: str | None = None,
) -> str:
    schema = json.dumps(PatchSpec.model_json_schema(), indent=2)
    prev = _load_previous_patches(patches_dir)
    if prev:
        lines = ["\n## Previous patch attempts (do NOT repeat these)"]
        for p in prev:
            lines.append(f"- {p['patch_id']}: {p['description']} (ops: {', '.join(p['ops'])})")
        if last_apply_error:
            lines.append(
                f"\n**Last patch apply error (the previous fix failed for this reason — do not repeat it):**\n{last_apply_error}"
            )
        prev_section = "\n".join(lines)
    elif last_apply_error:
        prev_section = f"\n## Last patch apply error\n{last_apply_error}"
    else:
        prev_section = ""

    # Merge engine-level and blueprint-level prompt_context (blueprint appends after engine)
    ctx_parts = [c for c in [engine_prompt_context, blueprint_prompt_context] if c and c.strip()]

    # Load operator-managed rules file: patches/rules.md (if present)
    rules_file = patches_dir / "rules.md"
    if rules_file.exists():
        try:
            rules_content = rules_file.read_text(encoding="utf-8").strip()
            if rules_content:
                ctx_parts.append(rules_content)
        except Exception:
            # rules.md is operator-managed; a read failure must not block the
            # LLM call. Log at DEBUG for `-v` users.
            logger.debug("Could not read patches/rules.md at %s", rules_file, exc_info=True)

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
    provider_options: dict[str, Any] | None = None,
    timeout: float = 120.0,
    engine_prompt_context: str | None = None,
    blueprint_prompt_context: str | None = None,
    last_apply_error: str | None = None,
) -> str:
    """Call the configured LLM provider and return raw text response."""
    system_prompt = _build_system_prompt(patches_dir, engine_prompt_context, blueprint_prompt_context, last_apply_error)

    if provider == "openai_compat":
        return _call_openai_compat(messages, model, max_tokens, base_url, system_prompt, provider_options, timeout=timeout)
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
    provider_options: dict[str, Any] | None = None,
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
    if provider_options:
        # ollama_* keys → stripped and nested under payload["options"] (Ollama-specific field)
        # all other keys → merged into payload top-level (standard OpenAI params)
        ollama_opts = {k[len("ollama_"):]: v for k, v in provider_options.items() if k.startswith("ollama_")}
        generic_opts = {k: v for k, v in provider_options.items() if not k.startswith("ollama_")}
        if ollama_opts:
            payload["options"] = ollama_opts
        payload.update(generic_opts)

    response = httpx.post(
        url,
        json=payload,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        timeout=httpx.Timeout(connect=15.0, read=timeout, write=30.0, pool=5.0),
    )
    response.raise_for_status()
    data = response.json()
    return data["choices"][0]["message"]["content"]


# ── Response parsing ──────────────────────────────────────────────────────────

def _format_reprompt_error(exc: Exception, raw: str) -> str:
    """Turn a ValidationError or JSONDecodeError into specific, actionable feedback."""
    if isinstance(exc, json.JSONDecodeError):
        lines = raw.splitlines()
        # Show 3 lines of context around the error (1-indexed)
        err_line = exc.lineno  # 1-indexed
        ctx_start = max(0, err_line - 3)
        ctx_end = min(len(lines), err_line + 2)
        ctx_lines = []
        for i, ln in enumerate(lines[ctx_start:ctx_end], start=ctx_start + 1):
            marker = " --> " if i == err_line else "     "
            ctx_lines.append(f"{marker}{i:3}: {ln}")
        context_block = "\n".join(ctx_lines)
        return (
            f"JSON parse error at line {exc.lineno}, column {exc.colno}: {exc.msg}\n\n"
            f"Your output near the error:\n{context_block}\n\n"
            f"Common causes: missing comma between fields, trailing comma after last field, "
            f"unescaped newline or quote inside a string value, truncated output."
        )

    if not isinstance(exc, ValidationError):
        return str(exc)

    lines: list[str] = []
    try:
        parsed = json.loads(raw)
    except Exception:
        parsed = {}

    for e in exc.errors(include_url=False):
        loc = e["loc"]
        etype = e["type"]
        input_val = e.get("input")

        # Human-readable location: operations[0] not operations.0
        loc_parts: list[str] = []
        for part in loc:
            if isinstance(part, int):
                loc_parts.append(f"[{part}]")
            elif loc_parts:
                loc_parts.append(f".{part}")
            else:
                loc_parts.append(str(part))
        loc_str = "".join(loc_parts)

        if etype == "missing":
            field_name = str(loc[-1]) if loc else ""
            hint = ""
            for wrong, correct in _FIELD_ALIASES.items():
                if correct == field_name and wrong in parsed:
                    hint = f' (you used "{wrong}" — rename it to "{field_name}")'
            lines.append(f'• {loc_str}: required field missing{hint}.')

        elif etype == "extra_forbidden":
            wrong_field = str(loc[-1]) if loc else loc_str
            correct = _FIELD_ALIASES.get(wrong_field)
            if correct:
                lines.append(f'• {loc_str}: "{wrong_field}" is not valid — use "{correct}" instead.')
            else:
                lines.append(f'• {loc_str}: "{wrong_field}" is not a recognized field — remove it.')

        elif etype == "union_tag_not_found":
            # LLM used wrong discriminator for PatchOperation (should be "op", not "type" etc.)
            hint = ""
            if isinstance(input_val, dict):
                wrong_key = next((k for k in _OP_DISCRIMINATOR_ALIASES if k in input_val), None)
                if wrong_key:
                    guessed_op = input_val.get(wrong_key, "")
                    hint = f' You used "{wrong_key}: {guessed_op}" — rename the key to "op".'
                elif "op" not in input_val:
                    hint = ' The "op" field is missing entirely.'
            lines.append(
                f'• {loc_str}: invalid operation.{hint}\n'
                f'  Valid ops: {", ".join(_VALID_OPS)}'
            )

        elif etype == "literal_error":
            expected = e.get("ctx", {}).get("expected", "")
            alias_hint = ""
            if isinstance(input_val, str) and input_val in ("replace_module_config_key",):
                alias_hint = (
                    ' `replace_module_config_key` does NOT exist. '
                    'Use `set_module_config_key` (one field) or `replace_module_config` (entire config block).'
                )
            lines.append(
                f'• {loc_str}: invalid value {input_val!r}.{alias_hint}'
                + (f' Expected one of: {expected}.' if expected and not alias_hint else "")
            )

        elif etype in ("string_type", "int_type", "float_type", "bool_type"):
            expected_type = etype.replace("_type", "")
            lines.append(f'• {loc_str}: expected {expected_type}, got {type(input_val).__name__} {input_val!r}.')

        else:
            lines.append(f'• {loc_str}: {e["msg"]}')

    return "\n".join(lines) if lines else str(exc)


def _parse_patch_spec(text: str) -> PatchSpec:
    """Parse and validate LLM response as PatchSpec."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(lines[1:-1] if lines[-1].startswith("```") else lines[1:])
        text = text.strip()
    # Find the start of the first JSON object (handles leading prose like "Here is the JSON:")
    brace_idx = text.find("{")
    if brace_idx > 0:
        text = text[brace_idx:]
    # raw_decode stops at the end of the first complete JSON object, ignoring
    # any trailing prose or markdown the LLM adds after the closing brace.
    obj, _ = json.JSONDecoder().raw_decode(text)
    return PatchSpec.model_validate(obj)


# ── Public API ────────────────────────────────────────────────────────────────

def build_prompt(
    failure_ctx: FailureContext,
    patches_dir: Path,
    engine_prompt_context: str | None = None,
    blueprint_prompt_context: str | None = None,
) -> dict[str, str]:
    """Return the system and user prompts without calling the LLM.

    Useful for debugging, prompt tuning, or cost estimation.

    Returns:
        {"system": <system prompt>, "user": <user prompt>}
    """
    return {
        "system": _build_system_prompt(patches_dir, engine_prompt_context, blueprint_prompt_context),
        "user": _build_user_prompt(failure_ctx, patches_dir),
    }


def generate_llm_patch(
    failure_ctx: FailureContext,
    model: str,
    patches_dir: Path,
    provider: str = "anthropic",
    base_url: str | None = None,
    max_tokens: int = 4096,
    provider_options: dict[str, Any] | None = None,
    llm_timeout: float = 120.0,
    llm_max_reprompts: int = MAX_REPROMPTS,
    engine_prompt_context: str | None = None,
    blueprint_prompt_context: str | None = None,
    last_apply_error: str | None = None,
) -> LLMPatchResult:
    """Call the LLM and return an LLMPatchResult with patch + attempt metadata.

    Does not apply or stage the patch — caller decides what to do with it.

    result.patch is None if the LLM failed to produce a valid PatchSpec after
    llm_max_reprompts attempts. result.attempts counts all LLM calls made.
    result.reprompt_errors lists the validation error from each failed attempt.
    """
    messages: list[dict[str, Any]] = [
        {
            "role": "user",
            "content": _build_user_prompt(failure_ctx, patches_dir),
        }
    ]

    patch_spec: PatchSpec | None = None
    reprompt_errors: list[str] = []
    attempts_made = 0

    for attempt in range(llm_max_reprompts):
        attempts_made = attempt + 1
        try:
            raw = _call_llm(messages, model, max_tokens, provider, base_url, patches_dir, provider_options, timeout=llm_timeout, engine_prompt_context=engine_prompt_context, blueprint_prompt_context=blueprint_prompt_context, last_apply_error=last_apply_error)
        except Exception as exc:
            logger.error(
                "LLM API call failed (attempt %d/%d): %s", attempt + 1, llm_max_reprompts, exc
            )
            reprompt_errors.append(f"API error: {exc}")
            break

        logger.debug("LLM raw response (attempt %d):\n%s", attempt + 1, raw)
        try:
            patch_spec = _parse_patch_spec(raw)
            break
        except (ValidationError, json.JSONDecodeError, ValueError) as exc:
            friendly = _format_reprompt_error(exc, raw)
            reprompt_errors.append(friendly)
            logger.warning(
                "LLM patch response invalid (attempt %d/%d):\n%s",
                attempt + 1, llm_max_reprompts, friendly,
            )
            raw_lines = raw.splitlines()
            raw_truncated = "\n".join(raw_lines[:80])
            if len(raw_lines) > 80:
                raw_truncated += f"\n... ({len(raw_lines) - 80} more lines truncated)"
            messages.append({"role": "assistant", "content": raw})
            messages.append({
                "role": "user",
                "content": _REPROMPT_TEMPLATE.format(error=friendly, raw_truncated=raw_truncated),
            })

    if patch_spec is None:
        logger.error(
            "LLM agent failed to produce a valid PatchSpec after %d attempts "
            "for blueprint %r run %r",
            llm_max_reprompts, failure_ctx.blueprint_id, failure_ctx.run_id,
        )
    return LLMPatchResult(patch=patch_spec, attempts=attempts_made, reprompt_errors=reprompt_errors)


def _patch_filename(patch_spec: PatchSpec, patches_dir: Path) -> str:
    """Generate structured filename: {YYYYMMDDTHHmmss}_{slug}.json."""
    from datetime import datetime, timezone

    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{ts}_{patch_spec.patch_id}.json"


def stage_patch_for_human(
    patch_spec: PatchSpec,
    patches_dir: Path,
    failure_ctx: FailureContext,
    on_patch_pending_webhook=None,  # WebhookEndpointConfig | None
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
        "prompt_version": PROMPT_VERSION,
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info(
        "LLM patch staged for human review: %s  "
        "(apply with: aqueduct patch apply %s --blueprint <path>)",
        out_path, out_path,
    )

    if on_patch_pending_webhook is not None:
        try:
            from aqueduct.surveyor.webhook import fire_webhook
            fire_webhook(
                on_patch_pending_webhook,
                full_payload={
                    "patch_id": patch_spec.patch_id,
                    "run_id": failure_ctx.run_id,
                    "blueprint_id": failure_ctx.blueprint_id,
                    "failed_module": failure_ctx.failed_module,
                    "patch_path": str(out_path),
                },
                template_vars={
                    "run_id": failure_ctx.run_id,
                    "blueprint_id": failure_ctx.blueprint_id,
                    "failed_module": failure_ctx.failed_module or "",
                },
            )
        except Exception:
            pass  # webhook errors must never block staging


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
        "prompt_version": PROMPT_VERSION,
    }
    archive_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _utcnow() -> str:
    from datetime import datetime, timezone
    return datetime.now(tz=timezone.utc).isoformat()
