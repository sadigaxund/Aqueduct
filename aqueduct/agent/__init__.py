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
import re
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from aqueduct.agent.budget import (
    DEFAULT_BUDGET,
    AttemptRecord,
    BudgetConfig,
    BudgetTracker,
)
from aqueduct.agent.signature import (
    ErrorSignature,
    from_apply_error,
    from_exception,
    from_json_decode_error,
    from_validation_error,
)
from aqueduct.patch.grammar import PatchSpec
from aqueduct.redaction import redact as _redact
from aqueduct.surveyor.models import FailureContext

logger = logging.getLogger(__name__)


@dataclass
class AgentPatchResult:
    """Return value of generate_agent_patch — patch + attempt metadata.

    Phase 34 adds budget/signature fields (all optional / default-safe so older
    callers and test fixtures keep working unchanged):

    * ``stop_reason`` — which BudgetConfig axis terminated the loop. Always
      set when the loop is driven by a BudgetTracker; for the legacy code
      path it stays None until callers migrate.
    * ``tokens_in_total`` / ``tokens_out_total`` — provider-reported usage
      summed across all attempts. Zero when the provider does not report.
    * ``attempt_records`` — per-attempt structured log fed to Surveyor's
      ``heal_attempts`` table (Task 88). Each entry carries the
      ``ErrorSignature`` from that turn (None when the turn succeeded).
    * ``escalated`` — True iff Task 87 escalation was applied at least once.
    """

    patch: PatchSpec | None
    attempts: int              # total LLM calls made (1 = succeeded first try)
    reprompt_errors: list[str] = field(default_factory=list)  # validation errors per failed attempt
    stop_reason: str | None = None
    tokens_in_total: int = 0
    tokens_out_total: int = 0
    attempt_records: list[AttemptRecord] = field(default_factory=list)
    escalated: bool = False
    # List of mechanical recoveries performed during parsing (e.g.
    # "stripped_think_block", "json_repair"). Empty when the response was
    # clean. CLI auto-apply path downgrades approval_mode → human when this
    # is non-empty — patches we had to rescue never silently land.
    recovery_applied: list[str] = field(default_factory=list)

# Bump manually when the system prompt changes significantly.
# Stored in patch _aq_meta so benchmark results can be correlated to prompt versions.
PROMPT_VERSION = "1.1"

MAX_REPROMPTS = 3
_PATCH_HISTORY_MAX = 3


# ── Prompt construction ───────────────────────────────────────────────────────

_SYSTEM_PROMPT_TEMPLATE = """\
You are an expert Apache Spark blueprint repair agent for the Aqueduct blueprint engine.

A blueprint has failed. You will receive a structured failure report describing:
- What the blueprint does (human-readable summary)
- The failing module and its resolved configuration
- A "Value provenance" section showing exactly where each config value comes from in the Blueprint source
- The error message and either a structured root-cause block (Spark error class + offending column + suggestions) OR a raw stack trace if structured extraction was unavailable
- Previous patch attempts (if any) — do NOT repeat a fix that was already tried

Your task: produce a PatchSpec JSON that fixes the root cause.

## PatchSpec Schema
{patch_schema}

## Rules

### Choosing the right op (read the "Value provenance" section first)
The provenance section tells you the `source_type` of each config value. Pick the op from this table:

| source_type | Op | Notes |
|---|---|---|
| `literal` | `set_module_config_key` | Targets the module directly. |
| `context_ref` | `replace_context_value` | Use the dot-notation key shown in provenance. |
| `arcade_inherited` (with `context_key`) | `replace_context_value` on the PARENT context key | Find the parent key in "Blueprint context values" whose resolved value matches; that is the fix. |
| `env_ref` | (do not patch) | Value is from an environment variable; fix outside the Blueprint. |

**Hard rules:**
- `set_module_config_key` sets ONE field, leaving the rest intact. Use for literal-source fixes.
- `replace_module_config` REPLACES the entire config block — every key you forget is silently dropped. Never use it for a single-field fix.
- `replace_module_config_key` does NOT exist. Don't invent it.
- Module IDs containing `__` are arcade-expanded compile artefacts. They do NOT exist in the Blueprint YAML — never target them with `set_module_config_key`. Fix the parent context key instead.
- If the failure report contains a "No `context:` block" notice, the blueprint has no context section at all. Do NOT emit `replace_context_value` — it will be rejected. Use `set_module_config_key` with a literal value.

### Path values
- ALWAYS use relative paths (e.g. `data/yellow/*.parquet`). Never construct absolute paths even if the provenance shows an absolute `blueprint_path` — that field is for reference only.
- Preserve the original format (relative stays relative, globs stay globs).
- When fixing a path that's a `context_ref`, patch the context value via `replace_context_value` so the blueprint stays portable. Do not hardcode the resolved literal into a module config.

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

{root_cause_section}
## Full module list (for reference when writing patch IDs)
{module_list}
{provenance_section}{blueprint_yaml_section}{doctor_hints_section}{guardrails_section}
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

# Task 85 — stronger reprompt used either (a) when the structural detector
# fires (top-level required missing AND extra keys present) OR (b) when
# Task 87 stuck-detection escalation triggers. Drops the echo of the bad
# output (which biases small models toward small edits of their mistake)
# and shows a minimal valid PatchSpec skeleton so the model has a positive
# anchor, not just a list of negations.
_REPROMPT_TEMPLATE_ESCALATED = """\
Your previous response was structurally wrong. Do NOT make small edits to it.
START FRESH from the skeleton below.

## Minimal valid PatchSpec — the SHAPE your output must take
```json
{skeleton}
```

## Errors to fix
{error}

{structural_hint}{raw_evidence}Rewrite the COMPLETE PatchSpec JSON from scratch using the shape above.
Replace the placeholder values with the right ones for THIS failure. Output
ONLY valid JSON — no prose, no markdown fences.
"""

# Hard-coded minimal valid PatchSpec used as positive anchor in the escalated
# reprompt template. Mirrors the example in the system prompt so the model
# sees a consistent shape on both turns. The single-op shape here is the most
# common fix; the system prompt covers multi-op composition.
_PATCH_SKELETON = """\
{
  "patch_id": "fix-<short-slug>",
  "rationale": "<one sentence — what was wrong and what this fixes>",
  "confidence": 0.9,
  "category": "other",
  "root_cause": "<one sentence — the root cause>",
  "operations": [
    {
      "op": "set_module_config_key",
      "module_id": "<existing_module_id>",
      "key": "<config_field>",
      "value": "<new_value>"
    }
  ]
}"""

# Op-level field names that LLMs sometimes put at the TOP level of the patch,
# forgetting the `operations[]` wrapper. When the structural detector sees
# `operations` missing AND any of these present at top level → emits an
# explicit "you forgot the wrapper" hint instead of the field-level negation
# list (which small models often misread as "remove this field").
_OP_LEVEL_FIELDS_AT_ROOT = (
    "op", "module_id", "key", "value", "config", "label",
    "context_key", "edge", "from_module", "to_module", "query",
)


def _detect_structural_error(exc: BaseException, raw: str) -> str | None:
    """Return a one-line hint when the LLM produced a shape that won't validate.

    Catches four common shape failures small models exhibit, all of which
    survive the field-level error list as opaque noise the model can't
    recover from:

    1. Top-level JSON is a LIST — model emitted an operations array
       directly, no envelope. Pydantic's ``Input should be a valid
       dictionary`` is too generic for a small model to fix.
    2. Top-level JSON is a SCALAR (string / number / bool / null) — model
       returned prose-as-JSON or a single value.
    3. Top-level is a dict but ``operations`` is missing AND op-level
       fields (``op``, ``module_id``, …) sit at the root — model forgot
       the wrapper.
    4. Top-level is a dict but both ``rationale`` AND ``operations`` are
       missing AND no op-level fields are at root — model emitted some
       other dict shape entirely (e.g. ``{"patch": {...}}`` or
       ``{"action": "fix", "fields": [...]}``).

    Returning a non-None hint signals the caller to use the escalated
    template (skeleton + structural hint), bypassing the field-level
    negation list that small models often misread as ``remove this field``.
    """
    if not isinstance(exc, ValidationError):
        return None
    try:
        parsed = json.loads(raw)
    except Exception:
        return None

    # Case 1 — top-level list (operations array without envelope).
    if isinstance(parsed, list):
        n = len(parsed)
        return (
            f"You returned a JSON array with {n} item(s) at the TOP level. "
            "PatchSpec is a JSON OBJECT, not an array. Wrap your array as "
            "the value of the `operations` field inside an envelope with "
            "`patch_id`, `rationale`, `confidence`, `root_cause`, and "
            "`operations`."
        )

    # Case 2 — top-level scalar.
    if not isinstance(parsed, dict):
        kind = type(parsed).__name__
        return (
            f"You returned a JSON {kind} at the TOP level. PatchSpec is a "
            "JSON OBJECT (dict) with `patch_id`, `rationale`, "
            "`confidence`, `root_cause`, and `operations` fields."
        )

    try:
        errors = exc.errors(include_url=False)
    except TypeError:
        errors = exc.errors()
    operations_missing = any(
        e.get("type") == "missing" and tuple(e.get("loc") or ()) == ("operations",)
        for e in errors
    )
    rationale_missing = any(
        e.get("type") == "missing" and tuple(e.get("loc") or ()) == ("rationale",)
        for e in errors
    )

    misplaced = [k for k in _OP_LEVEL_FIELDS_AT_ROOT if k in parsed]

    # Case 3 — wrapper forgotten, op-level fields at root.
    if operations_missing and misplaced:
        return (
            f"You put op fields ({', '.join(repr(k) for k in misplaced)}) at the "
            "TOP level of the patch — they belong INSIDE an entry of the "
            "`operations` list. The top level has `patch_id`, `rationale`, "
            "`confidence`, `category`, `root_cause`, and `operations` (an "
            "array). Each entry of `operations` carries `op`, `module_id`, etc."
        )

    # Case 4 — envelope missing entirely; some other dict shape.
    if operations_missing and rationale_missing:
        emitted = ", ".join(repr(k) for k in list(parsed.keys())[:10]) or "(empty)"
        return (
            "Your JSON is missing the PatchSpec envelope. You emitted top-level "
            f"keys: {{{emitted}}}. Required envelope keys are `patch_id`, "
            "`rationale`, `confidence`, `root_cause`, `operations`. Rewrite "
            "using the skeleton below — do NOT reshape your existing keys."
        )

    return None

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


def _build_root_cause_section(failure_ctx: FailureContext) -> str:
    """Render the diagnostic section between failed-config and module-list.

    Phase 35 contract: structured fields (Spark error_class +
    messageParameters, Py4J root cause, suggested columns) replace the raw
    stack trace when present. The trace is shown verbatim ONLY when no
    structured extraction was possible. This keeps prompt tokens focused on
    actionable signal — a Spark error condition with the offending column
    name and a "did you mean" list is worth more than 40 lines of JVM
    frames the LLM has to grep through.
    """
    error_class = getattr(failure_ctx, "error_class", None)
    root_exc = getattr(failure_ctx, "root_exception", None)
    sql_state = getattr(failure_ctx, "sql_state", None)
    suggested = getattr(failure_ctx, "suggested_columns", ()) or ()
    object_name = getattr(failure_ctx, "object_name", None)

    has_structured = any((error_class, root_exc, sql_state, suggested, object_name))
    if has_structured:
        lines = ["## Root cause (structured)"]
        if error_class:
            lines.append(f"- **Error class**: `{error_class}`")
        if object_name:
            lines.append(f"- **Offending object**: `{object_name}`")
        if suggested:
            sug = ", ".join(f"`{c}`" for c in suggested)
            lines.append(f"- **Actual columns available**: {sug}")
        if sql_state:
            lines.append(f"- **SQL state**: `{sql_state}`")
        if isinstance(root_exc, dict):
            rtype = root_exc.get("type") or "Unknown"
            rmsg = (root_exc.get("message") or "").strip()
            lines.append(f"- **Root exception**: `{rtype}` — {rmsg}" if rmsg else f"- **Root exception**: `{rtype}`")
        return "\n".join(lines) + "\n"

    trace = failure_ctx.stack_trace
    if not trace:
        return "## Stack trace\n(no stack trace)\n"
    return "## Stack trace\n```\n" + trace + "\n```\n"


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
    else:
        # Steers the model away from `replace_context_value` when the blueprint
        # has no `context:` block — that op would be rejected by the apply gate
        # with "Blueprint has no 'context' block." and waste a reprompt round.
        lines.append(
            "\n### No `context:` block\n"
            "This blueprint declares no `context:` block. Do NOT emit "
            "`replace_context_value` ops — the apply gate will reject them. "
            "Use `set_module_config_key` with a literal value on the failing module."
        )

    lines.append("")
    return "\n".join(lines)


def _build_guardrails_section(guardrails: Any) -> str:
    """Terse, imperative guardrails block — appended to the user prompt.

    Phase 33 Part B Scope C step 1: surface ``agent.guardrails`` from the
    Blueprint to the LLM so the model knows the constraints its patch will
    be checked against. Without this section the model burns heal attempts
    generating patches that production then rejects post-hoc.

    Returns empty string when ``guardrails`` is None or every field is empty
    so unconstrained blueprints emit no extra prompt noise.
    """
    if guardrails is None:
        return ""

    # Accept either a dataclass (GuardrailsConfig) or a plain dict (decoded
    # from the persisted manifest JSON in heal-from-store paths).
    def _field(name: str) -> tuple:
        if isinstance(guardrails, dict):
            val = guardrails.get(name) or ()
        else:
            val = getattr(guardrails, name, ()) or ()
        return tuple(val)

    forbidden = _field("forbidden_ops")
    allowed_paths = _field("allowed_paths")
    heal_on = _field("heal_on_errors")
    never_heal = _field("never_heal_errors")
    if not (forbidden or allowed_paths or heal_on or never_heal):
        return ""
    lines = [
        "",
        "## Guardrails (your patch will be REJECTED post-generation if it violates these — follow them in your first attempt)",
    ]
    if forbidden:
        lines.append(f"- forbidden ops (must NOT appear in operations[]): {', '.join(forbidden)}")
    if allowed_paths:
        lines.append(f"- allowed file paths (operations may only target these — fnmatch patterns): {', '.join(allowed_paths)}")
    if heal_on:
        lines.append(f"- heal only on these error_types: {', '.join(heal_on)}")
    if never_heal:
        lines.append(f"- never heal these error_types (priority over heal_on): {', '.join(never_heal)}")
    lines.append("")
    return "\n".join(lines)


def _build_user_prompt(failure_ctx: FailureContext, patches_dir: Path, guardrails: Any = None) -> str:
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
    # Typed str | None on FailureContext. Anything else (e.g. a Mock in tests,
    # or a malformed ctx) is treated as absent — never fed to yaml.safe_load,
    # which would spin forever on a non-string stream.
    if isinstance(raw_yaml, str) and raw_yaml.strip():
        blueprint_yaml_section = (
            "\n## Original Blueprint YAML (unresolved — authoring view)\n"
            "```yaml\n"
            f"{raw_yaml.strip()}\n"
            "```\n"
        )
        # Soft macro-preservation hint. Macros are one-way compile-time text
        # substitution — no provenance links expanded SQL back to its
        # `{{ macros.X }}` source. If the patched query originally used a
        # macro, inlining concrete SQL silently erodes the abstraction.
        # Nudge the model to keep the reference; cannot be enforced.
        macros_defined: list[str] = []
        try:
            import yaml as _yaml
            _doc = _yaml.safe_load(raw_yaml) or {}
            _mac = _doc.get("macros") if isinstance(_doc, dict) else None
            if isinstance(_mac, dict):
                macros_defined = [str(k) for k in _mac]
        except Exception:
            pass
        if macros_defined:
            blueprint_yaml_section += (
                "\n**Macros in scope:** "
                + ", ".join(f"`{{{{ macros.{m} }}}}`" for m in macros_defined)
                + ". When editing a query that used a macro, keep the "
                "`{{ macros.NAME }}` reference instead of inlining the "
                "expanded SQL — the engine re-expands it at compile time.\n"
            )

    return _USER_PROMPT_TEMPLATE.format(
        blueprint_name=blueprint_name,
        blueprint_description=f"> {blueprint_desc}" if blueprint_desc else "",
        blueprint_summary=_build_blueprint_summary(manifest),
        failed_module=failure_ctx.failed_module,
        error_message=failure_ctx.error_message,
        failed_module_config=failed_config,
        root_cause_section=_build_root_cause_section(failure_ctx),
        module_list=module_list,
        provenance_section=provenance_section,
        blueprint_yaml_section=blueprint_yaml_section,
        doctor_hints_section=doctor_hints_section,
        guardrails_section=_build_guardrails_section(guardrails),
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

def _format_llm_error_hint(
    exc: Exception,
    *,
    timeout: float | None,
    base_url: str | None,
    model: str,
) -> str:
    """Return an actionable hint suffix for common transient LLM failure modes.

    Empty string when no specific guidance applies; the caller appends this
    directly to the error log line. Kept dependency-light: matches on
    exception class names + message substrings so we don't have to import
    httpx at module top.
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
    temperature_override: float | None = None,
) -> tuple[str, int, int]:
    """Call the LLM provider; return (text, tokens_in, tokens_out).

    ``temperature_override`` (Task 87) lets the caller force a higher sampling
    temperature on the escalated attempt without mutating the caller-supplied
    provider_options. Set to None to leave provider defaults / user config
    untouched.

    Token counts come from the provider response when reported; 0 otherwise.
    """
    system_prompt = _build_system_prompt(patches_dir, engine_prompt_context, blueprint_prompt_context, last_apply_error)

    # Scrub registered @aq.secret() values from anything leaving the process to
    # an external LLM API. FailureContext / blueprint YAML can incidentally
    # embed secrets via stack traces or config strings; the model must never
    # see them.
    system_prompt = _redact(system_prompt)
    messages = _redact(messages)

    if provider == "openai_compat":
        return _call_openai_compat(
            messages, model, max_tokens, base_url, system_prompt,
            provider_options, timeout=timeout,
            temperature_override=temperature_override,
        )
    else:
        return _call_anthropic(
            messages, model, max_tokens, system_prompt, timeout=timeout,
            temperature_override=temperature_override,
        )



def _call_anthropic(
    messages: list[dict[str, Any]],
    model: str,
    max_tokens: int,
    system_prompt: str,
    timeout: float = 120.0,
    temperature_override: float | None = None,
) -> tuple[str, int, int]:
    import httpx

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY environment variable not set. "
            "Set it or configure agent.provider: openai_compat in aqueduct.yml."
        )
    payload: dict[str, Any] = {
        "model": model,
        "max_tokens": max_tokens,
        "system": system_prompt,
        "messages": messages,
    }
    if temperature_override is not None:
        payload["temperature"] = temperature_override
    # `with httpx.Client():` so the socket is torn down by __exit__ when
    # KeyboardInterrupt propagates — server sees disconnect, can abort
    # generation (Anthropic billing stops mid-completion).
    with httpx.Client(timeout=timeout) as client:
        response = client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=payload,
        )
        response.raise_for_status()
        data = response.json()
    text = data["content"][0]["text"]
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
) -> tuple[str, int, int]:
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
        # Grammar-constrained JSON sampling. Ollama (0.1.24+), vLLM, and most
        # OpenAI-compat servers honour this — the model is masked to emit only
        # valid JSON, eliminating unescaped-quote / trailing-comma / line-
        # comment failures that no amount of postprocessing fixes. Opt out by
        # setting provider_options.response_format to a falsy value or "off".
        "response_format": {"type": "json_object"},
    }
    if provider_options:
        # ollama_* keys → stripped and nested under payload["options"] (Ollama-specific field)
        # all other keys → merged into payload top-level (standard OpenAI params)
        ollama_opts = {k[len("ollama_"):]: v for k, v in provider_options.items() if k.startswith("ollama_")}
        generic_opts = {k: v for k, v in provider_options.items() if not k.startswith("ollama_")}
        if ollama_opts:
            payload["options"] = ollama_opts
        payload.update(generic_opts)
        # Opt-out: provider_options.response_format = "off" / None / False
        # disables grammar-constrained JSON mode (useful when a specific
        # backend rejects the field; payload-level value above gets cleared).
        rf = generic_opts.get("response_format")
        if rf in (None, False, "off"):
            payload.pop("response_format", None)
    if temperature_override is not None:
        # Override AFTER provider_options so escalation wins. Apply at both
        # the top-level (standard OpenAI) and inside options (Ollama-style)
        # since clients accept either.
        payload["temperature"] = temperature_override
        if "options" in payload and isinstance(payload["options"], dict):
            payload["options"]["temperature"] = temperature_override

    # `with httpx.Client():` so the socket is torn down by __exit__ when
    # KeyboardInterrupt propagates — Ollama / vLLM see disconnect and abort
    # generation (frees GPU; on paid hosted endpoints stops billing).
    with httpx.Client(
        timeout=httpx.Timeout(connect=15.0, read=timeout, write=30.0, pool=5.0),
    ) as client:
        response = client.post(
            url,
            json=payload,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        )
        response.raise_for_status()
        data = response.json()
    text = data["choices"][0]["message"]["content"]
    usage = data.get("usage") or {}
    return text, int(usage.get("prompt_tokens", 0) or 0), int(usage.get("completion_tokens", 0) or 0)


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
            # Echo the offending op block so the model can see its own omission.
            # Without this, "operations[0].set_module_config_key.value: required
            # field missing" lands as an opaque field name — tiny models then
            # repeat the same omission on reprompt. Walk `loc` into `parsed` and
            # render the partial op compactly.
            emitted_hint = ""
            if loc and len(loc) >= 2 and isinstance(parsed, dict):
                try:
                    cursor: Any = parsed
                    for part in loc[:-1]:
                        cursor = cursor[part]
                    if isinstance(cursor, dict):
                        emitted_keys = ", ".join(repr(k) for k in cursor.keys())
                        emitted_hint = (
                            f"\n  You emitted {emitted_keys} — add the missing "
                            f"\"{field_name}\" field to this same op block."
                        )
                except (KeyError, IndexError, TypeError):
                    pass
            lines.append(f'• {loc_str}: required field missing{hint}.{emitted_hint}')

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


_THINK_BLOCK_RE = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)
_FENCE_BLOCK_RE = re.compile(r"```(?:json)?\s*\n(.*?)\n```", re.DOTALL | re.IGNORECASE)
# Strip JSON line comments — both JS-style `//` and Python/YAML-style `#`.
# LLMs trained mostly on Python or YAML routinely emit `#` comments; coder
# models trained on JS emit `//`. We only strip comments that begin at column
# 0 or after whitespace — never inside a string literal — by requiring the
# comment to be preceded by whitespace OR by the start of a line. A full
# string-aware tokenizer would be safer but is overkill: a stray comment
# marker inside a string is rare, and the strict json.loads pass below still
# catches malformed output if our cleanup mis-fires.
_LINE_COMMENT_RE = re.compile(r"(^|\s)(?://|#)[^\n]*", re.MULTILINE)


def _parse_patch_spec(text: str) -> tuple[PatchSpec, list[str]]:
    """Parse and validate LLM response as PatchSpec.

    Returns ``(spec, recovery_applied)`` where ``recovery_applied`` is the
    list of mechanical fixes performed during parsing — empty when the
    response was clean. Used downstream to downgrade ``approval_mode: auto``
    to ``human`` for any patch that needed recovery: we never trust a
    patch we had to rescue.

    Tolerates common LLM-output artifacts before strict JSON parsing:
      * `<think>...</think>` reasoning blocks (deepseek-r1 family)
      * markdown fences anywhere in the response (not just leading)
      * `// line comments` inside otherwise-valid JSON
      * a `json-repair` last-ditch pass when the soft-dep `[llm]` extra
        is installed

    Cleanups are MECHANICAL — they fix syntax, never interpret intent. The
    LLM still wrote the patch; we only un-mangle its output.
    """
    text = text.strip()
    recovery_applied: list[str] = []

    # 1. Strip <think>...</think> reasoning blocks (deepseek-r1 etc.).
    cleaned = _THINK_BLOCK_RE.sub("", text)
    if cleaned != text:
        recovery_applied.append("stripped_think_block")
    text = cleaned.strip()

    # 2. If the response contains a fenced ```json ... ``` block anywhere,
    # prefer its contents over the surrounding prose.
    fence_match = _FENCE_BLOCK_RE.search(text)
    if fence_match:
        text = fence_match.group(1).strip()
        recovery_applied.append("stripped_code_fence")

    # 3. Find the start of the first JSON object (handles leading prose like
    # "Here is the JSON:").
    brace_idx = text.find("{")
    if brace_idx > 0:
        text = text[brace_idx:]
        recovery_applied.append("stripped_leading_prose")

    # 4. Strip line comments — keep as the last cleanup so we only touch
    # the JSON region, not the upstream think block / prose we already stripped.
    cleaned = _LINE_COMMENT_RE.sub(r"\1", text)
    if cleaned != text:
        recovery_applied.append("stripped_line_comments")
    text = cleaned

    # raw_decode stops at the end of the first complete JSON object, ignoring
    # any trailing prose or markdown the LLM adds after the closing brace.
    try:
        obj, _ = json.JSONDecoder().raw_decode(text)
    except json.JSONDecodeError:
        # Last-ditch repair pass for cases strict json + our cleanups can't
        # recover (unescaped quotes inside string values, missing commas
        # between fields, smart quotes, etc.). `json-repair` is an optional
        # soft dependency — if absent, the original strict error propagates
        # to the reprompt loop unchanged so the model still gets a chance
        # to fix its own output.
        try:
            from json_repair import repair_json as _repair_json
        except ImportError:
            raise
        repaired = _repair_json(text)
        if not repaired:
            raise
        obj, _ = json.JSONDecoder().raw_decode(repaired)
        recovery_applied.append("json_repair")
    return PatchSpec.model_validate(obj), recovery_applied


# ── Public API ────────────────────────────────────────────────────────────────

def build_prompt(
    failure_ctx: FailureContext,
    patches_dir: Path,
    engine_prompt_context: str | None = None,
    blueprint_prompt_context: str | None = None,
    guardrails: Any = None,
) -> dict[str, str]:
    """Return the system and user prompts without calling the LLM.

    Useful for debugging, prompt tuning, or cost estimation.

    Returns:
        {"system": <system prompt>, "user": <user prompt>}
    """
    return {
        "system": _build_system_prompt(patches_dir, engine_prompt_context, blueprint_prompt_context),
        "user": _build_user_prompt(failure_ctx, patches_dir, guardrails=guardrails),
    }


def resolve_budget(
    pydantic_budget: Any = None,
    *,
    max_reprompts: int | None = None,
) -> BudgetConfig:
    """Build a runtime ``BudgetConfig`` from the pydantic ``AgentBudgetConfig``.

    Lets the CLI keep using its existing ``max_reprompts`` resolution flow as
    the fallback while opting users into the full multi-axis budget when the
    ``agent.budget:`` block is present in ``aqueduct.yml``.

    * ``pydantic_budget`` — an instance of ``config.AgentBudgetConfig`` (or any
      object exposing the six axis attributes). When None, defaults derive
      from ``max_reprompts`` (other axes use the dataclass defaults).
    * ``max_reprompts`` — legacy single-axis fallback. Only used when
      ``pydantic_budget`` is None.
    """
    if pydantic_budget is not None:
        same_err = int(pydantic_budget.same_error_consecutive)
        same_sig = int(pydantic_budget.same_signature_overall)
        if same_sig < same_err and "same_signature_overall" not in getattr(pydantic_budget, "model_fields_set", set()):
            same_sig = same_err
        
        return BudgetConfig(
            max_reprompts=int(pydantic_budget.max_reprompts),
            max_seconds=float(pydantic_budget.max_seconds),
            max_tokens_total=pydantic_budget.max_tokens_total,
            same_error_consecutive=same_err,
            same_signature_overall=same_sig,
            progress_stalled_window=int(pydantic_budget.progress_stalled_window),
        )
    if max_reprompts is not None:
        return BudgetConfig(max_reprompts=max(1, int(max_reprompts)))
    return BudgetConfig()


# Task 87 — temperature applied to the ONE escalated attempt that follows a
# stuck-consecutive trip. 0.8 is a deliberate jump from typical 0.0–0.2 used
# for patch generation: forces sampling divergence so the model doesn't keep
# regenerating the same wrong tree. One shot only; tracker.escalated_once
# blocks repeat escalations within the same heal call.
_ESCALATION_TEMPERATURE = 0.8


def generate_agent_patch(
    failure_ctx: FailureContext,
    model: str,
    patches_dir: Path,
    provider: str = "anthropic",
    base_url: str | None = None,
    max_tokens: int = 4096,
    provider_options: dict[str, Any] | None = None,
    timeout: float = 120.0,
    max_reprompts: int = MAX_REPROMPTS,
    engine_prompt_context: str | None = None,
    blueprint_prompt_context: str | None = None,
    last_apply_error: str | None = None,
    guardrails: Any = None,
    budget: BudgetConfig | None = None,
    apply_callback: Any = None,   # Callable[[PatchSpec], tuple[bool, str|None, str|None, str|None]] | None
    on_attempt: Any = None,        # Callable[[AttemptRecord], None] | None
) -> AgentPatchResult:
    """Call the LLM and return an AgentPatchResult with patch + attempt metadata.

    Unified Phase 34 loop. Each iteration:
      1. Call provider (records tokens + latency).
      2. Parse response → on schema/JSON failure record a signature and
         reprompt; on success continue.
      3. If ``apply_callback`` is supplied, invoke it on the parsed PatchSpec.
         Returning (False, error_class, error_message, where) flips the turn
         into a "succeeded LLM, gate rejected" failure, records the apply-time
         signature, and reprompts. Returning (True, ...) succeeds and exits.
      4. BudgetTracker.check_stop() decides whether to continue.
      5. If tracker.should_escalate() is True, the next attempt uses the
         escalated reprompt template + bumped sampling temperature (Task 87).

    Backward compatibility:
      - ``budget`` is optional; when omitted a BudgetConfig is synthesized
        from ``max_reprompts`` (other axes use defaults — generous ceilings
        that won't trip ahead of max_reprompts for typical heal calls).
      - ``apply_callback=None`` preserves the legacy "schema-only" semantics:
        the first valid PatchSpec exits the loop.
      - Existing fields on AgentPatchResult retain their meaning; new fields
        default to safe values.

    ``apply_callback`` signature::

        def cb(patch: PatchSpec) -> tuple[bool, str|None, str|None, str|None]:
            return success, error_class, error_message, where

      ``error_class`` is the signature error_class (e.g. "guardrail_violation",
      "compile_error"). ``where`` may be None.

    ``on_attempt`` is invoked with the AttemptRecord after each turn (success
    or failure) — used by Surveyor to persist into ``heal_attempts``.
    """
    if budget is None:
        budget = BudgetConfig(max_reprompts=max(1, max_reprompts))
    tracker = BudgetTracker(budget)

    messages: list[dict[str, Any]] = [
        {
            "role": "user",
            "content": _build_user_prompt(failure_ctx, patches_dir, guardrails=guardrails),
        }
    ]

    patch_spec: PatchSpec | None = None
    reprompt_errors: list[str] = []
    escalate_next = False

    while True:
        attempt_num = tracker.begin_attempt()
        temperature_override = _ESCALATION_TEMPERATURE if escalate_next else None
        t_start = time.monotonic()
        try:
            raw, tokens_in, tokens_out = _call_agent(
                messages, model, max_tokens, provider, base_url, patches_dir,
                provider_options,
                timeout=timeout,
                engine_prompt_context=engine_prompt_context,
                blueprint_prompt_context=blueprint_prompt_context,
                last_apply_error=last_apply_error,
                temperature_override=temperature_override,
            )
        except Exception as exc:
            latency_ms = int((time.monotonic() - t_start) * 1000)
            hint = _format_llm_error_hint(exc, timeout=timeout, base_url=base_url, model=model)
            logger.error(
                "LLM API call failed (attempt %d/%d): %s%s",
                attempt_num, budget.max_reprompts, exc, hint,
            )
            reprompt_errors.append(f"API error: {exc}")
            sig = from_exception(exc, where="provider")
            rec = tracker.record(
                sig,
                tokens_in=0, tokens_out=0, latency_ms=latency_ms,
                gate_that_rejected="provider", escalated=escalate_next,
            )
            if on_attempt is not None:
                try:
                    on_attempt(rec)
                except Exception:
                    logger.debug("on_attempt callback raised; ignoring", exc_info=True)
            tracker.mark_api_error()
            break

        latency_ms = int((time.monotonic() - t_start) * 1000)
        logger.debug("LLM raw response (attempt %d):\n%s", attempt_num, raw)

        # ── Parse phase ────────────────────────────────────────────────
        parse_exc: BaseException | None = None
        recovery_applied: list[str] = []
        try:
            patch_spec, recovery_applied = _parse_patch_spec(raw)
        except (ValidationError, json.JSONDecodeError, ValueError) as exc:
            parse_exc = exc

        if parse_exc is not None:
            friendly = _format_reprompt_error(parse_exc, raw)
            reprompt_errors.append(friendly)
            logger.warning(
                "LLM patch response invalid (attempt %d/%d):\n%s",
                attempt_num, budget.max_reprompts, friendly,
            )
            if isinstance(parse_exc, ValidationError):
                sig = from_validation_error(parse_exc)
            elif isinstance(parse_exc, json.JSONDecodeError):
                sig = from_json_decode_error(parse_exc)
            else:
                sig = from_exception(parse_exc, where="parse")

            rec = tracker.record(
                sig,
                tokens_in=tokens_in, tokens_out=tokens_out, latency_ms=latency_ms,
                gate_that_rejected="schema", escalated=escalate_next,
            )
            if on_attempt is not None:
                try:
                    on_attempt(rec)
                except Exception:
                    logger.debug("on_attempt callback raised; ignoring", exc_info=True)

            stop = tracker.check_stop()
            if stop is not None and stop != "solved":
                patch_spec = None
                break

            escalate_next = tracker.should_escalate()
            if escalate_next:
                tracker.mark_escalated()
                logger.info(
                    "stuck-detection escalation triggered on attempt %d "
                    "(temperature=%.2f, escalated template)",
                    attempt_num, _ESCALATION_TEMPERATURE,
                )

            structural_hint = _detect_structural_error(parse_exc, raw) or ""
            reprompt_msg = _format_reprompt_for_next_turn(
                friendly=friendly, raw=raw, escalated=escalate_next,
                structural_hint=structural_hint,
            )
            messages.append({"role": "assistant", "content": raw})
            messages.append({"role": "user", "content": reprompt_msg})
            continue

        # ── Apply phase (optional callback) ─────────────────────────────
        if apply_callback is not None:
            try:
                apply_result = apply_callback(patch_spec)
            except Exception as cb_exc:
                logger.debug("apply_callback raised; treating as gate rejection", exc_info=True)
                apply_result = (False, type(cb_exc).__name__, str(cb_exc), None)

            ok, err_class, err_msg, err_where = (apply_result + (None,) * 4)[:4]
            if ok:
                rec = tracker.record(
                    None,
                    tokens_in=tokens_in, tokens_out=tokens_out, latency_ms=latency_ms,
                    gate_that_rejected=None, escalated=escalate_next,
                )
                if on_attempt is not None:
                    try:
                        on_attempt(rec)
                    except Exception:
                        logger.debug("on_attempt callback raised; ignoring", exc_info=True)
                tracker.check_stop()  # sets 'solved'
                break

            # Gate rejection — record signature, reprompt with the gate's error.
            sig = from_apply_error(
                err_class or "apply_error",
                err_msg or "(no message)",
                where=err_where,
            )
            friendly = (
                f"Your PatchSpec parsed but was rejected by the apply gate "
                f"({err_class or 'apply_error'}): {err_msg or '(no message)'}"
            )
            reprompt_errors.append(friendly)
            logger.warning(
                "Patch apply gate rejected attempt %d/%d: %s",
                attempt_num, budget.max_reprompts, friendly,
            )
            rec = tracker.record(
                sig,
                tokens_in=tokens_in, tokens_out=tokens_out, latency_ms=latency_ms,
                gate_that_rejected="apply", escalated=escalate_next,
            )
            if on_attempt is not None:
                try:
                    on_attempt(rec)
                except Exception:
                    logger.debug("on_attempt callback raised; ignoring", exc_info=True)

            stop = tracker.check_stop()
            if stop is not None and stop != "solved":
                patch_spec = None
                break

            escalate_next = tracker.should_escalate()
            if escalate_next:
                tracker.mark_escalated()
                logger.info(
                    "stuck-detection escalation triggered on attempt %d "
                    "(temperature=%.2f, escalated template)",
                    attempt_num, _ESCALATION_TEMPERATURE,
                )

            reprompt_msg = _format_reprompt_for_next_turn(
                friendly=friendly, raw=raw, escalated=escalate_next,
                structural_hint="",
            )
            messages.append({"role": "assistant", "content": raw})
            messages.append({"role": "user", "content": reprompt_msg})
            patch_spec = None
            continue

        # No apply_callback — legacy schema-only success exits the loop.
        rec = tracker.record(
            None,
            tokens_in=tokens_in, tokens_out=tokens_out, latency_ms=latency_ms,
            gate_that_rejected=None, escalated=escalate_next,
        )
        if on_attempt is not None:
            try:
                on_attempt(rec)
            except Exception:
                logger.debug("on_attempt callback raised; ignoring", exc_info=True)
        tracker.check_stop()
        break

    if patch_spec is None:
        logger.error(
            "LLM agent failed to produce a valid PatchSpec after %d attempt(s) "
            "for blueprint %r run %r (stop_reason=%s)",
            tracker.current_attempt, failure_ctx.blueprint_id, failure_ctx.run_id,
            tracker.stop_reason,
        )
    return AgentPatchResult(
        patch=patch_spec,
        attempts=tracker.current_attempt,
        reprompt_errors=reprompt_errors,
        stop_reason=tracker.stop_reason,
        tokens_in_total=tracker.tokens_in_total,
        tokens_out_total=tracker.tokens_out_total,
        attempt_records=list(tracker.attempts),
        escalated=tracker.escalated_once,
        recovery_applied=recovery_applied if patch_spec is not None else [],
    )


def _format_reprompt_for_next_turn(
    *, friendly: str, raw: str, escalated: bool, structural_hint: str,
) -> str:
    """Render the user-role message for the next turn of the reprompt loop.

    Task 85 — when escalating OR when a structural error is detected, use the
    skeleton-anchored template. Original Task 85 dropped the raw echo entirely
    to avoid biasing small models toward small edits. We now show a short
    evidence-only snippet (≤300 chars) under a label that explicitly tells the
    model not to copy from it — small models lose track of their own output
    across turns and stay stuck without it, but the skeleton remains the
    positive anchor.
    """
    use_escalated = escalated or bool(structural_hint)
    if use_escalated:
        hint = (structural_hint + "\n\n") if structural_hint else ""
        raw_stripped = (raw or "").strip()
        if raw_stripped:
            snippet = raw_stripped[:300]
            if len(raw_stripped) > 300:
                snippet += " …(truncated)"
            raw_evidence = (
                "## What you actually sent (evidence — DO NOT edit this; "
                "rewrite from the skeleton above)\n"
                f"```\n{snippet}\n```\n\n"
            )
        else:
            raw_evidence = ""
        return _REPROMPT_TEMPLATE_ESCALATED.format(
            skeleton=_PATCH_SKELETON,
            error=friendly,
            structural_hint=hint,
            raw_evidence=raw_evidence,
        )
    raw_lines = raw.splitlines()
    raw_truncated = "\n".join(raw_lines[:80])
    if len(raw_lines) > 80:
        raw_truncated += f"\n... ({len(raw_lines) - 80} more lines truncated)"
    return _REPROMPT_TEMPLATE.format(error=friendly, raw_truncated=raw_truncated)


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
    out_path.write_text(json.dumps(_redact(payload), indent=2), encoding="utf-8")
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
    archive_path.write_text(json.dumps(_redact(payload), indent=2), encoding="utf-8")


def _utcnow() -> str:
    from datetime import datetime, timezone
    return datetime.now(tz=timezone.utc).isoformat()
