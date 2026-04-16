"""Tier 0 context resolution — all static substitution happens here.

Resolution order (highest priority wins):
  1. CLI flags (cli_overrides dict)
  2. AQUEDUCT_CTX_* environment variables
  3. context_profiles block for the active profile
  4. context: block static defaults

Tier 0 syntax:
  ${ENV_VAR:-default}   → shell env var with optional default
  ${ctx.key.subkey}     → cross-reference to another context key

Tier 1 (@aq.*) and Tier 2 (UDFs) are NOT resolved here — those belong to
the Compiler layer.
"""

from __future__ import annotations

import os
import re
from typing import Any

# ${ctx.some.dotted.key}
_CTX_RE = re.compile(r"\$\{ctx\.([a-zA-Z0-9_.]+)\}")

# ${VARNAME} or ${VARNAME:-default}  (must NOT start with ctx.)
_ENV_RE = re.compile(r"\$\{(?!ctx\.)([^}:\s]+?)(?::-(.*?))?\}")

# Detect any unresolved Tier 1 references so we can leave them for the Compiler
_TIER1_RE = re.compile(r"@aq\.")


def flatten_dict(d: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """Recursively flatten nested dict to dot-notation keys."""
    result: dict[str, Any] = {}
    for k, v in d.items():
        full_key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            result.update(flatten_dict(v, full_key))
        else:
            result[full_key] = v
    return result


def _sub_env(value: str) -> str:
    """Substitute ${VAR:-default} patterns against os.environ."""

    def _replace(m: re.Match) -> str:
        var_name = m.group(1)
        default = m.group(2)
        env_val = os.environ.get(var_name)
        if env_val is not None:
            return env_val
        if default is not None:
            return default
        raise ValueError(
            f"Environment variable ${{{var_name}}} is not set and no default was provided"
        )

    return _ENV_RE.sub(_replace, value)


def _sub_ctx(value: str, ctx_map: dict[str, str], depth: int = 0) -> str:
    """Substitute ${ctx.key} patterns from ctx_map. Recursive to handle transitive refs."""
    if depth > 20:
        raise ValueError(
            f"Context reference depth limit exceeded — possible cycle near: {value!r}"
        )

    def _replace(m: re.Match) -> str:
        key = m.group(1)
        if key not in ctx_map:
            raise ValueError(f"Undefined context reference: ${{ctx.{key}}}")
        resolved = ctx_map[key]
        # If resolved value itself has ctx refs, recurse
        if _CTX_RE.search(resolved):
            resolved = _sub_ctx(resolved, ctx_map, depth + 1)
        return resolved

    return _CTX_RE.sub(_replace, value)


def build_context_map(
    context_dict: dict[str, Any],
    profile: str | None = None,
    profiles: dict[str, dict[str, Any]] | None = None,
    cli_overrides: dict[str, str] | None = None,
) -> dict[str, str]:
    """Build the fully-resolved Tier 0 context map.

    Returns a flat dict of dot-notation keys to resolved string values.
    Tier 1 (@aq.*) values are preserved as-is for the Compiler to handle.
    """
    flat: dict[str, Any] = flatten_dict(context_dict)

    # Step 1 — resolve env var refs in static values
    for key in list(flat):
        v = flat[key]
        if isinstance(v, str) and not _TIER1_RE.search(v):
            flat[key] = _sub_env(v)

    # Step 2 — AQUEDUCT_CTX_* env var overrides
    _prefix = "AQUEDUCT_CTX_"
    for env_key, env_val in os.environ.items():
        if env_key.startswith(_prefix):
            ctx_key = env_key[len(_prefix) :].lower()
            flat[ctx_key] = env_val

    # Step 3 — profile overrides
    if profile and profiles and profile in profiles:
        for k, v in profiles[profile].items():
            if isinstance(v, str) and not _TIER1_RE.search(v):
                flat[k] = _sub_env(v)
            else:
                flat[k] = v

    # Step 4 — CLI overrides (highest priority)
    if cli_overrides:
        flat.update(cli_overrides)

    # Step 5 — resolve ctx cross-references iteratively until stable
    str_flat: dict[str, str] = {k: str(v) for k, v in flat.items()}
    for _ in range(20):
        changed = False
        for key, value in str_flat.items():
            if _CTX_RE.search(value) and not _TIER1_RE.search(value):
                resolved = _sub_ctx(value, str_flat)
                if resolved != value:
                    str_flat[key] = resolved
                    changed = True
        if not changed:
            break
    else:
        unresolved = {k: v for k, v in str_flat.items() if _CTX_RE.search(v)}
        if unresolved:
            raise ValueError(
                f"Unresolvable context references (possible cycle): {list(unresolved.keys())}"
            )

    return str_flat


def resolve_value(value: Any, ctx_map: dict[str, str]) -> Any:
    """Recursively substitute Tier 0 tokens in an arbitrary YAML value.

    Called on Module config dicts after the context map is built.
    Tier 1 (@aq.*) tokens pass through untouched.
    """
    if isinstance(value, str):
        if not _TIER1_RE.search(value):
            value = _sub_env(value)
        value = _sub_ctx(value, ctx_map)
        return value
    if isinstance(value, dict):
        return {k: resolve_value(v, ctx_map) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_value(item, ctx_map) for item in value]
    return value
