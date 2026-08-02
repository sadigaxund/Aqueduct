"""SQL macro resolution — compile-time expansion of {{ macros.name }} tokens.

Macros are named SQL fragments defined in the Blueprint `macros:` block.
They are resolved at compile time; the Manifest always contains plain SQL.

Syntax:
  {{ macros.name }}                      — simple substitution
  {{ macros.name(key=value, key2='v') }} — parameterized; substitutes
                                           {{ key }} placeholders in macro body

Constraints (by design):
  - No loops, no conditionals, no runtime evaluation.
  - Macro bodies may not reference other macros (no nesting).
  - All {{ key }} placeholders in a parameterized macro must be supplied.
"""

from __future__ import annotations

import re

from aqueduct.errors import AqueductError


class MacroError(AqueductError):
    """Raised when macro resolution fails."""


# Matches {{ macros.name }} and {{ macros.name(args) }}
_MACRO_RE = re.compile(
    r"\{\{\s*macros\.(\w+)(?:\(([^)]*)\))?\s*\}\}"
)

# Matches {{ param_name }} inside a macro body
_PARAM_RE = re.compile(r"\{\{\s*(\w+)\s*\}\}")

# Quote-aware single-argument matcher for `_parse_call_args`: a quoted value
# (single or double) may contain a comma without ending the argument; only
# an unquoted (bare) value stops at the next comma.
_ARG_RE = re.compile(
    r"""
    \s*(?P<key>\w+)\s*=\s*
    (?:
        '(?P<sq>[^']*)'
      | "(?P<dq>[^"]*)"
      | (?P<bare>[^,]*)
    )
    """,
    re.VERBOSE,
)


def _parse_call_args(args_str: str) -> dict[str, str]:
    """Parse 'key=value, key2=\'val, with a comma\'' into {key: value}.

    Quote-aware: a comma inside a quoted value must not split the argument
    list. The previous implementation was `args_str.split(",")`, which
    truncated any quoted value containing a comma — {{ macros.sel(cols=
    "a,b") }} silently kept only "a" and dropped the "b" (the second
    fragment, `b"`, has no "=" so the naive parser's `continue` guard
    dropped it with no error, no warning)."""
    result: dict[str, str] = {}
    pos = 0
    length = len(args_str)
    while pos < length:
        m = _ARG_RE.match(args_str, pos)
        if not m or m.end() == pos:
            break
        key = m.group("key")
        value = m.group("sq")
        if value is None:
            value = m.group("dq")
        if value is None:
            value = (m.group("bare") or "").strip()
        result[key] = value
        pos = m.end()
        while pos < length and args_str[pos] in ", ":
            pos += 1
    return result


def resolve_macros(text: str, macros: dict[str, str]) -> str:
    """Expand all {{ macros.* }} tokens in text using the macros dict.

    Args:
        text:   SQL string (or any string) containing macro call tokens.
        macros: Dict of macro name → body (from Blueprint.macros).

    Returns:
        String with all macro tokens replaced by their expanded bodies.

    Raises:
        MacroError: Unknown macro name or missing parameter in body.
    """
    if not macros or "{{" not in text:
        return text

    def _expand(m: re.Match) -> str:
        name = m.group(1)
        args_str = m.group(2) or ""

        if name not in macros:
            raise MacroError(
                f"Macro {name!r} is not defined. "
                f"Available: {sorted(macros)}"
            )

        body = macros[name]

        # Always attempt placeholder substitution, even when called with no
        # args ({{ macros.name }}, args_str == ""). The previous code gated
        # this behind `if args_str.strip():`, so a macro body with {{ key }}
        # placeholders called WITHOUT parens/args skipped substitution
        # entirely and the literal, unresolved "{{ key }}" text reached the
        # output SQL — silently contradicting this module's own documented
        # contract ("All {{ key }} placeholders in a parameterized macro
        # must be supplied"). Running the substitution unconditionally means
        # a body with no placeholders is an inexpensive no-op (the regex
        # matches nothing) and a body WITH placeholders correctly raises the
        # existing "parameter not supplied" MacroError below.
        args = _parse_call_args(args_str) if args_str.strip() else {}

        def _sub_param(pm: re.Match) -> str:
            param = pm.group(1)
            if param not in args:
                raise MacroError(
                    f"Macro {name!r} parameter {param!r} not supplied in call. "
                    f"Supplied: {sorted(args)}"
                )
            return args[param]

        body = _PARAM_RE.sub(_sub_param, body)

        return body

    return _MACRO_RE.sub(_expand, text)


def resolve_macros_in_config(config: object, macros: dict[str, str]) -> object:
    """Recursively resolve macros in a config dict/list/str."""
    if not macros:
        return config
    if isinstance(config, str):
        return resolve_macros(config, macros)
    if isinstance(config, dict):
        return {k: resolve_macros_in_config(v, macros) for k, v in config.items()}
    if isinstance(config, list):
        return [resolve_macros_in_config(item, macros) for item in config]
    return config
