"""Response parsing and reprompt formatting for the LLM patch loop.

This module owns:
  - Mechanical cleanup of raw LLM output (_parse_patch_spec)
  - Structural error detection (_detect_structural_error)
  - Human-readable error formatting (_format_reprompt_error)
  - Reprompt message assembly (_format_reprompt_for_next_turn)
  - Regex patterns for common LLM artifacts
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from pydantic import ValidationError

from aqueduct.agent.prompts import (
    _FIELD_ALIASES,
    _OP_DISCRIMINATOR_ALIASES,
    _OP_LEVEL_FIELDS_AT_ROOT,
    _PATCH_SKELETON,
    _REPROMPT_TEMPLATE,
    _REPROMPT_TEMPLATE_ESCALATED,
    _VALID_OPS,
)
from aqueduct.errors import AqueductError
from aqueduct.patch.grammar import PatchSpec

logger = logging.getLogger(__name__)


class AgentParseError(AqueductError):
    """Raised when an LLM response can't even be treated as patch text — e.g. a
    non-string response (interrupted stream, empty payload). Distinct from
    ``pydantic.ValidationError`` (parsed JSON failing the PatchSpec schema) and
    ``json.JSONDecodeError`` (malformed JSON text); this is the "not text at all"
    case caught upstream in ``agent/loop.py``'s parse-phase except clause."""


_THINK_BLOCK_RE = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)
_FENCE_BLOCK_RE = re.compile(r"```(?:json)?\s*\n(.*?)\n```", re.DOTALL | re.IGNORECASE)
# Strip JSON line comments — both JS-style `//` and Python/YAML-style `#`.
# The regex cannot tell a comment from `//` or `#` INSIDE a string value
# (e.g. SQL `a // 2`, a path containing `#`), so it is applied only as a
# recovery pass AFTER strict JSON parsing has already failed — valid JSON
# never contains comments, so valid string values are never touched.
_LINE_COMMENT_RE = re.compile(r"(^|\s)(?://|#)[^\n]*", re.MULTILINE)


def _detect_structural_error(exc: BaseException, raw: str) -> str | None:
    """Return a one-line hint when the LLM produced a shape that won't validate.

    Catches four common shape failures small models exhibit, all of which
    survive the field-level error list as opaque noise the model can't
    recover from:

    1. Top-level JSON is a LIST.
    2. Top-level JSON is a SCALAR.
    3. Top-level is a dict but ``operations`` is missing AND op-level
       fields sit at the root — model forgot the wrapper.
    4. Top-level is a dict but both ``rationale`` AND ``operations`` are
       missing AND no op-level fields are at root.

    Returning a non-None hint signals the caller to use the escalated
    template (skeleton + structural hint).
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


def _format_reprompt_error(exc: Exception, raw: str) -> str:
    """Turn a ValidationError or JSONDecodeError into specific, actionable feedback."""
    if isinstance(exc, json.JSONDecodeError):
        lines = raw.splitlines()
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
        from aqueduct.utils import format_error_loc
        loc_str = format_error_loc(loc)

        if etype == "missing":
            field_name = str(loc[-1]) if loc else ""
            hint = ""
            for wrong, correct in _FIELD_ALIASES.items():
                if correct == field_name and wrong in parsed:
                    hint = f' (you used "{wrong}" — rename it to "{field_name}")'
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


def _parse_patch_spec(text: str) -> tuple[PatchSpec, list[str]]:
    """Parse and validate LLM response as PatchSpec.

    Returns ``(spec, recovery_applied)`` where ``recovery_applied`` is the
    list of mechanical fixes performed during parsing — empty when the
    response was clean.

    Tolerates common LLM-output artifacts before strict JSON parsing:
      * ``<think>...</think>`` reasoning blocks
      * markdown fences anywhere in the response
      * line comments inside otherwise-valid JSON
      * a ``json-repair`` last-ditch pass when the soft dep is installed
    """
    if not isinstance(text, str):
        raise AgentParseError(
            f"LLM returned non-string response: {type(text).__name__}. "
            "The model may have been interrupted or returned an empty payload."
        )
    text = text.strip()
    recovery_applied: list[str] = []

    # 1. Strip <think>...</think> reasoning blocks (deepseek-r1 etc.).
    cleaned = _THINK_BLOCK_RE.sub("", text)
    if cleaned != text:
        recovery_applied.append("stripped_think_block")
    text = cleaned.strip()

    # 1b. Orphan </think> — some servers strip the opening tag (or the
    #     opener fell outside the context window), leaving reasoning prose
    #     terminated by a bare closer. Everything before it is reasoning,
    #     not patch; dropping it stops brace-find from latching onto a `{`
    #     inside the reasoning text.
    _orphan_close = text.lower().rfind("</think>")
    if _orphan_close != -1:
        _remainder = text[_orphan_close + len("</think>"):].strip()
        if "{" in _remainder:  # never strip when no JSON would survive
            text = _remainder
            recovery_applied.append("stripped_orphan_think_close")

    # 2. If the response contains fenced ```json ... ``` blocks, prefer the
    #    first one that looks like a PatchSpec (contains "operations") —
    #    cheap models sometimes echo our quoted evidence snippet back in a
    #    fence BEFORE emitting the real patch.
    fence_matches = list(_FENCE_BLOCK_RE.finditer(text))
    if fence_matches:
        chosen = next(
            (m for m in fence_matches if '"operations"' in m.group(1)),
            fence_matches[0],
        )
        text = chosen.group(1).strip()
        recovery_applied.append("stripped_code_fence")

    # 3. Find the start of the first JSON object (handles leading prose).
    brace_idx = text.find("{")
    if brace_idx > 0:
        text = text[brace_idx:]
        recovery_applied.append("stripped_leading_prose")

    # raw_decode stops at the end of the first complete JSON object.
    try:
        obj, _ = json.JSONDecoder().raw_decode(text)
    except json.JSONDecodeError as decode_exc:
        # 4. Strip line comments and retry — only after strict parsing
        #    failed. Running the regex on valid JSON would corrupt string
        #    values containing ` // ` or ` # ` (see _LINE_COMMENT_RE note).
        recovered = False
        cleaned = _LINE_COMMENT_RE.sub(r"\1", text)
        if cleaned != text:
            try:
                obj, _ = json.JSONDecoder().raw_decode(cleaned)
                recovery_applied.append("stripped_line_comments")
                recovered = True
            except json.JSONDecodeError:
                pass

        # 5. json-repair last-ditch pass — on the ORIGINAL text, so a bad
        #    comment-strip can never compound into a silently mangled patch.
        if not recovered:
            try:
                from json_repair import repair_json as _repair_json
            except ImportError:
                raise decode_exc from None
            repaired = _repair_json(text)
            if not repaired:
                raise decode_exc from None
            obj, _ = json.JSONDecoder().raw_decode(repaired)
            recovery_applied.append("json_repair")

    # 6. Unwrap wrappers (e.g. {"patch": {operations: [...]}}). Also handles
    #    the multi-key variant ({"patch": {...}, "explanation": "..."}) as
    #    long as the top level itself is NOT already an envelope and exactly
    #    one nested dict carries `operations` — ambiguity falls through to
    #    normal validation (which reports the real shape problem).
    if isinstance(obj, dict) and "operations" not in obj:
        _nested = [
            (k, v) for k, v in obj.items()
            if isinstance(v, dict) and "operations" in v
        ]
        if len(_nested) == 1:
            wrapper_key, obj = _nested[0]
            recovery_applied.append(f"unwrapped_{wrapper_key}")

    return PatchSpec.model_validate(obj), recovery_applied


def _format_reprompt_for_next_turn(
    *, friendly: str, raw: str, escalated: bool, structural_hint: str,
) -> str:
    """Render the user-role message for the next turn of the reprompt loop.

    When escalating OR when a structural error is detected, use the
    skeleton-anchored template. Shows a short evidence-only snippet (≤300 chars)
    labelled as "do not edit this" — small models lose track without it, but
    the skeleton remains the positive anchor.
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
    # Char cap on top of the line cap — a single minified-JSON line can be
    # tens of KB and would otherwise dominate the next turn's context.
    if len(raw_truncated) > 4000:
        raw_truncated = raw_truncated[:4000] + "\n... (truncated at 4000 chars)"
    return _REPROMPT_TEMPLATE.format(error=friendly, raw_truncated=raw_truncated)
