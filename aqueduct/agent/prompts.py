"""Prompt templates and construction helpers for the LLM patch loop.

This module owns everything related to what the model sees:
  - Template strings (system prompt, user prompt, reprompts)
  - Prompt construction functions (_build_user_prompt, _build_system_prompt)
  - LLM-facing metadata (_FIELD_ALIASES, _VALID_OPS, etc.)
  - Provenance, guardrails, and root-cause section builders
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from aqueduct.agent.constants import DEFAULT_MAX_TOKENS
from aqueduct.patch.grammar import PatchSpec
from aqueduct.surveyor.models import FailureContext

logger = logging.getLogger(__name__)

_PATCH_HISTORY_MAX = 3


from aqueduct.patch.grammar import (  # noqa: E402  (intentional mid-file import)
    _METADATA_ALIASES,
    VALID_PATCH_OPS,
)

# ── System-level constants exposed to the reprompt / alias machinery ───────

# Derived from the grammar — patch-level metadata aliases + prompts-only
# operation-structure aliases the LLM commonly misnames.
_FIELD_ALIASES: dict[str, str] = dict(
    _METADATA_ALIASES,
    ops="operations",
    op_list="operations",
    patches="operations",
    steps="operations",
    fix="operations",
)

# Canonical list imported from the grammar so the LLM always sees the
# actual PatchOperation discriminator set — drift-free.
_VALID_OPS = VALID_PATCH_OPS

# Wrong discriminator keys LLM uses instead of "op"
_OP_DISCRIMINATOR_ALIASES = (
    "type",
    "action",
    "operation",
    "method",
    "kind",
    "name",
)

# Op-level field names that LLMs sometimes put at the TOP level of the patch,
# forgetting the `operations[]` wrapper. Used by _detect_structural_error.
_OP_LEVEL_FIELDS_AT_ROOT = (
    "op",
    "module_id",
    "key",
    "value",
    "config",
    "label",
    "context_key",
    "edge",
    "from_module",
    "to_module",
    "query",
)

# Hard-coded minimal valid PatchSpec used as positive anchor in the escalated
# reprompt template. Mirrors the example in the system prompt so the model
# sees a consistent shape on both turns.
# Phase 75 — agentic-mode addendum. Only rendered into the system prompt when
# tools are actually offered this turn (see `tools_enabled` in
# _build_system_prompt) — a oneshot-mode heal never sees this text, so its
# prompt is byte-identical to pre-Phase-75 behaviour.
_TOOLS_SECTION = """
## Tools available (agentic mode)
Before answering, you MAY call read-only diagnostic tools to investigate the
failure further — recent runs, run detail, column lineage, patch history,
probe signals, the full Blueprint YAML, a live source schema, or a small
sample of real rows. None of them can modify anything; use as many turns as
you need, but each tool call counts against a per-heal cap, after which you
must answer with the PatchSpec JSON even without tools. When you have enough
information, answer with ONLY the PatchSpec JSON — no prose, no tool call.
"""

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


# ── Template strings ─────────────────────────────────────────────────────

# Phase 78 Step 2 — this template is the ENGINE-INDEPENDENT scaffold. Anything
# that names an execution engine, its exception vocabulary, or engine-flavored
# advice is supplied by the engine through
# `ExecutorProtocol.prompt_rules` (a `PromptRules` pack — see
# `aqueduct/executor/protocol.py`; Spark's pack lives in
# `aqueduct/executor/spark/prompt_rules.py`) and lands in the three
# `{engine_*}` slots below. The agent layer imports no engine specifics; the
# executor layer imports nothing from the agent layer.
#
# The slots are:
#   {engine_persona}          — the opening line
#   {engine_root_cause_note}  — what the engine's structured root-cause block holds
#   {engine_rules}            — engine-specific bullets inside "Other rules"
#
# Slot VALUES are `.format()` arguments, not part of this format string, so
# they carry literal braces (no `{{`/`}}` doubling) — unlike the text here.
_SYSTEM_PROMPT_TEMPLATE = """\
{engine_persona}

A blueprint has failed. You will receive a structured failure report describing:
- What the blueprint does (human-readable summary)
- The failing module and its resolved configuration
- A "Value provenance" section showing exactly where each config value comes from in the Blueprint source
- The error message and either a structured root-cause block ({engine_root_cause_note}) OR a raw stack trace if structured extraction was unavailable
- Previous patch attempts (if any) — do NOT repeat a fix that was already tried

Your task: produce a PatchSpec JSON that fixes the root cause.
{tools_section}
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
{engine_rules}
- `schema_hint field 'X' not found in source schema. Available columns: [...]` — the message lists every real column in the source. The fix is aligning the schema_hint key to a real column from that list (a rename) or removing the stale entry from schema_hint — never re-typing or re-declaring the missing key under its old name.

## Required output — complete example
Every response MUST be a single JSON object with ALL of these fields. The `operations` field is MANDATORY — a response without it is always wrong.

```json
{{
  "patch_id": "fix-example-path",
  "description": "One sentence: what was wrong and what the fix does.",
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
- description: one sentence — what was wrong and what the fix does (you can also use "rationale", "summary", "reason")
- confidence: float 0.0–1.0 — required
- category: one of: schema_drift, bad_path, format_mismatch, oom_config, sql_column_not_found, type_mismatch, missing_context, permission_error, other
- root_cause: one-sentence diagnosis
- operations: list of patch ops — use "op" as the key (NOT "type" or "action")

Respond with ONLY valid JSON. No prose, no markdown fences, no explanation outside the JSON object.{defer_rules}
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


# ── Prompt construction helpers ───────────────────────────────────────────


def _build_root_cause_section(failure_ctx: FailureContext) -> str:
    """Render the diagnostic section between failed-config and module-list.

    Structured fields (Spark error_class + messageParameters, Py4J root cause,
    suggested columns) replace the raw stack trace when present. The trace is
    shown verbatim ONLY when no structured extraction was possible.
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


def _load_previous_patches(obs_store: Any, limit: int = _PATCH_HISTORY_MAX) -> list[dict]:
    """Most-recent applied patches for the 'do not repeat' section.

    Phase 53 — served from the ``patch_index`` table (backend-blind) instead of
    an ``os.scandir`` over ``patches/applied/``. Empty when no store is given."""
    if obs_store is None:
        return []
    try:
        from aqueduct.patch import index as _ix
        with obs_store.connect() as cur:
            rows = _ix.recent_applied(cur, limit)
    except Exception:
        logger.debug("previous-patch history query failed", exc_info=True)
        return []
    patches: list[dict] = []
    for d in rows:
        ops = d.get("ops")
        patches.append({
            "patch_id": str(d.get("patch_id") or "?"),
            "description": str(d.get("rationale") or ""),
            "ops": list(ops) if isinstance(ops, list) else [],
        })
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
        logger.warning("Failed to parse provenance_json — LLM will see no provenance context")
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

    Surfaces ``agent.guardrails`` from the Blueprint to the LLM so the model
    knows the constraints its patch will be checked against.

    Returns empty string when ``guardrails`` is None or every field is empty.
    """
    if guardrails is None:
        return ""

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
        logger.warning("Failed to parse manifest_json — LLM prompt will have empty module context")
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

    # Doctor pre-flight hints
    hints = getattr(failure_ctx, "doctor_hints", None) or ()
    if hints:
        hint_lines = "\n".join(f"- {h}" for h in hints)
        doctor_hints_section = (
            "\n## Blueprint issues detected before run (may explain the failure)\n"
            f"{hint_lines}\n"
        )
    else:
        doctor_hints_section = ""

    # Original Blueprint YAML — gives LLM the authoring-level view
    blueprint_yaml_section = ""
    raw_yaml = getattr(failure_ctx, "blueprint_source_yaml", None)
    if isinstance(raw_yaml, str) and raw_yaml.strip():
        blueprint_yaml_section = (
            "\n## Original Blueprint YAML (unresolved — authoring view)\n"
            "```yaml\n"
            f"{raw_yaml.strip()}\n"
            "```\n"
        )
        # Soft macro-preservation hint
        macros_defined: list[str] = []
        try:
            import yaml as _yaml
            _doc = _yaml.safe_load(raw_yaml) or {}
            _mac = _doc.get("macros") if isinstance(_doc, dict) else None
            if isinstance(_mac, dict):
                macros_defined = [str(k) for k in _mac]
        except Exception:
            pass  # macro inspection is best-effort; YAML parse failures must not break prompt construction
        if macros_defined:
            blueprint_yaml_section += (
                "\n**Macros in scope:** "
                + ", ".join(f"`{{{{ macros.{m} }}}}`" for m in macros_defined)
                + ". When editing a query that used a macro, keep the "
                "`{{ macros.NAME }}` reference instead of inlining the "
                "expanded SQL — the engine re-expands it at compile time. "
                "If the root cause is INSIDE a macro body, fix it with the "
                "`replace_macro` op (existing macros only) — but remember the "
                "macro is shared: every module referencing it gets the new "
                "body, so keep the change minimal and preserve `{{ param }}` "
                "placeholders its callers supply.\n"
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


_COACHING_TIER_LABELS = {
    1: "same failure",
    2: "same failure shape, different module",
    3: "same error class",
    4: "recent fix",
}


def _build_coaching_section(failure_ctx: Any, obs_store: Any) -> str:
    """Phase 45 — signature-matched (failure → validated fix) few-shot section.

    Nearest-signature retrieval over applied patches, served from the
    ``patch_index`` table (Phase 53). Empty string when nothing matches
    (caller falls back to the legacy section).
    """
    from aqueduct.agent.memory import find_coaching_examples
    from aqueduct.agent.signature import from_failure_context

    try:
        sig_exact, sig_coarse = from_failure_context(failure_ctx)
        examples = find_coaching_examples(
            obs_store, sig_exact.hash, sig_coarse.hash, sig_exact.error_class,
            blueprint_id=getattr(failure_ctx, "blueprint_id", ""),
        )
    except Exception:
        logger.debug("Coaching retrieval failed — section omitted", exc_info=True)
        return ""
    if not examples:
        return ""
    lines = [
        "\n## Past validated fixes for similar failures",
        "These patches fixed failures like this one before and passed validation.",
        "Reuse the successful approach where it applies — but module ids and paths",
        "must match the CURRENT blueprint; never copy them blindly.",
    ]
    for ex in examples:
        failure_half = f"{ex.error_class} @ {ex.where}"
        if ex.normalized_message:
            failure_half += f": {ex.normalized_message}"
        lines.append(
            f"- [{_COACHING_TIER_LABELS.get(ex.tier, 'recent fix')}] {failure_half}\n"
            f"  → fix {ex.patch_id} (ops: {', '.join(ex.ops) or '?'}): {ex.rationale or '(no rationale recorded)'}"
        )
    return "\n".join(lines)


def _build_system_prompt(
    patches_dir: Path,
    engine_prompt_context: str | None = None,
    blueprint_prompt_context: str | None = None,
    last_apply_error: str | None = None,
    allow_defer: bool = False,
    failure_ctx: Any = None,
    coaching: bool = True,
    obs_store: Any = None,
    tools_enabled: bool = False,
    engine: str = "spark",
) -> str:
    """Compose the healing system prompt: generic scaffold + the engine's pack.

    Args:
        engine: The execution engine this heal targets (``deployment.engine``).
            Its ``PromptRules`` pack is pulled through the ``aqueduct.engines``
            registry (Phase 78 Step 2) and rendered into the scaffold's
            ``{engine_*}`` slots. Defaults to ``"spark"``, matching
            ``aqueduct.executor.get_executor`` / ``compile()``.

    Raises:
        UnknownEngineError: ``engine`` has no registered ``ExecutorProtocol``.
    """
    # Imported here (not at module scope) so `import aqueduct.agent` does not
    # eagerly resolve the engine entry-point group — the agent layer depends on
    # the executor's protocol seam, never on a specific engine's package.
    from aqueduct.executor.protocol import get_protocol

    engine_rules_pack = get_protocol(engine).prompt_rules

    raw_schema = PatchSpec.model_json_schema()

    # Phase 41: when allow_defer is False, hide DeferToHumanOp from the
    # schema the LLM sees. The model can't produce an op it doesn't know
    # exists. The grammar still accepts it (Pydantic needs the full union),
    # but the prompt shows only the real ops.
    if not allow_defer:
        # Strip DeferToHumanOp from the operations union. The PatchSpec model
        # itself sits at the TOP level of model_json_schema() (only nested
        # models land in $defs), and pydantic v2 renders the discriminated
        # union as `oneOf` + a discriminator mapping — scrub all three places
        # so the model sees no trace of the op and no dangling $ref.
        op_items = raw_schema.get("properties", {}).get("operations", {}).get("items", {})
        for union_key in ("oneOf", "anyOf"):
            if union_key in op_items:
                op_items[union_key] = [
                    ref for ref in op_items[union_key]
                    if not (isinstance(ref, dict) and "DeferToHumanOp" in str(ref.get("$ref", "")))
                ]
        mapping = op_items.get("discriminator", {}).get("mapping")
        if isinstance(mapping, dict):
            mapping.pop("defer_to_human", None)
        raw_schema.get("$defs", {}).pop("DeferToHumanOp", None)

    schema = json.dumps(raw_schema, indent=2)

    # Phase 41: defer rules — only shown when allow_defer is True so the
    # model doesn't see an easy way out on normal heals.
    #
    # Phase 78 Step 2: this section is assembled HERE, at runtime, not inside
    # _SYSTEM_PROMPT_TEMPLATE — so a guard that only greps the template constant
    # cannot see it. The engine-flavored parts (which infrastructure an engine
    # can actually fail on, which languages its UDFs are written in, and any
    # whole category that exists only for it) come from the engine's
    # DeferRules; the categories themselves are engine-independent and stay
    # here. See tests/test_agent/test_prompt_composition.py, which greps the
    # COMPOSED prompt for a non-Spark engine.
    defer_rules = ""
    if allow_defer:
        _defer = engine_rules_pack.defer
        defer_rules = (
            "\n\n### When to defer to a human\n"
            "Use `defer_to_human` when NO PatchSpec operation can fix the root cause:\n"
            "- **Infrastructure failures**: checkpoint corruption, S3 consistency, "
            f"{_defer.infra_examples}, cluster config — these are not Blueprint-level fixes.\n"
            "- **Upstream schema changes** requiring human judgment: ambiguous column "
            "renames, new required columns with unclear defaults.\n"
            f"- **UDF body bugs**: PatchSpec cannot modify {_defer.udf_languages} UDF code.\n"
            f"{_defer.extra_bullets}"
            "\n"
            "When deferring, include:\n"
            "- `diagnosis`: detailed explanation of why this cannot be patched automatically\n"
            "- `suggestions`: actionable next steps for the human operator\n"
            "- `confidence_reason`: why you're confident deferral is correct, not just uncertain\n"
            "\n"
            "Do NOT mix `defer_to_human` with other operations. If you defer, defer completely."
        )

    # Phase 45: signature-matched coaching replaces the chronological last-3
    # history when a FailureContext is available. Falls back to the legacy
    # "do NOT repeat" section when coaching is off, no failure_ctx was
    # threaded (debug/`aqueduct heal --show-prompt` paths), or nothing matches.
    prev_section = ""
    if coaching and failure_ctx is not None:
        prev_section = _build_coaching_section(failure_ctx, obs_store)
        if prev_section and last_apply_error:
            prev_section += (
                f"\n\n**Last patch apply error (the previous fix failed for this reason — do not repeat it):**\n{last_apply_error}"
            )
    if not prev_section:
        prev = _load_previous_patches(obs_store)
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

    # Merge engine-level and blueprint-level prompt_context
    ctx_parts = [c for c in [engine_prompt_context, blueprint_prompt_context] if c and c.strip()]

    # Load operator-managed rules file: patches/rules.md (if present)
    # Capped to prevent context overflow from an accidentally-large file.
    rules_file = patches_dir / "rules.md"
    if rules_file.exists():
        try:
            rules_content = rules_file.read_text(encoding="utf-8").strip()
            if rules_content:
                if len(rules_content) > DEFAULT_MAX_TOKENS:
                    rules_content = rules_content[:DEFAULT_MAX_TOKENS] + f"\n…(truncated at {DEFAULT_MAX_TOKENS} chars)"
                    logger.warning(f"patches/rules.md exceeds {DEFAULT_MAX_TOKENS} chars — truncated")
                ctx_parts.append(rules_content)
        except Exception:
            logger.debug("Could not read patches/rules.md at %s", rules_file, exc_info=True)

    if ctx_parts:
        custom_context_section = "\n\n## Additional context (operator-supplied)\n" + "\n\n".join(ctx_parts)
    else:
        custom_context_section = ""

    return _SYSTEM_PROMPT_TEMPLATE.format(
        patch_schema=schema,
        previous_patches_section=prev_section,
        custom_context_section=custom_context_section,
        defer_rules=defer_rules,
        tools_section=_TOOLS_SECTION if tools_enabled else "",
        engine_persona=engine_rules_pack.persona,
        engine_root_cause_note=engine_rules_pack.root_cause_note,
        engine_rules=engine_rules_pack.rules,
    )


def build_prompt(
    failure_ctx: FailureContext,
    patches_dir: Path,
    engine_prompt_context: str | None = None,
    blueprint_prompt_context: str | None = None,
    guardrails: Any = None,
    last_apply_error: str | None = None,
    allow_defer: bool = False,
    coaching: bool = True,
    obs_store: Any = None,
    tools_enabled: bool = False,
    engine: str = "spark",
) -> dict[str, str]:
    """Return the system and user prompts without calling the LLM.

    Useful for debugging, prompt tuning, or cost estimation.

    Args:
        last_apply_error: When set, includes the "previous patch failed" section
            in the system prompt so the debug view matches what the model sees
            on a re-prompt turn.
        allow_defer: When True, includes defer_to_human in the schema and defer
            rules in the prompt (Phase 41).
        obs_store: Observability store backing the patch_index — sources the
            coaching + history sections (Phase 53). None → those sections empty.
        tools_enabled: When True, includes the agentic-mode tools addendum
            (Phase 75) — matches what the model sees when ``agent.mode:
            agentic`` and tool-use is resolved supported for this call.
        engine: The execution engine this heal targets — selects the
            ``PromptRules`` pack composed into the system prompt (Phase 78
            Step 2).

    Returns:
        {"system": <system prompt>, "user": <user prompt>}
    """
    return {
        "system": _build_system_prompt(
            patches_dir, engine_prompt_context, blueprint_prompt_context,
            last_apply_error=last_apply_error,
            allow_defer=allow_defer,
            failure_ctx=failure_ctx,
            coaching=coaching,
            obs_store=obs_store,
            tools_enabled=tools_enabled,
            engine=engine,
        ),
        "user": _build_user_prompt(failure_ctx, patches_dir, guardrails=guardrails),
    }
