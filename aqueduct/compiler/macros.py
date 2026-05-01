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


class MacroError(Exception):
    """Raised when macro resolution fails."""


# Matches {{ macros.name }} and {{ macros.name(args) }}
_MACRO_RE = re.compile(
    r"\{\{\s*macros\.(\w+)(?:\(([^)]*)\))?\s*\}\}"
)

# Matches {{ param_name }} inside a macro body
_PARAM_RE = re.compile(r"\{\{\s*(\w+)\s*\}\}")


def _parse_call_args(args_str: str) -> dict[str, str]:
    """Parse 'key=value, key2=\'val\'' into {key: value} dict."""
    result: dict[str, str] = {}
    for part in args_str.split(","):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, _, v = part.partition("=")
        k = k.strip()
        v = v.strip().strip("'\"")
        result[k] = v
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

        if args_str.strip():
            args = _parse_call_args(args_str)
            # Substitute {{ param }} placeholders in body
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
