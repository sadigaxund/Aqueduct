"""Tier 1 runtime function registry — resolves @aq.* tokens.

Resolution happens on the driver before any Spark job starts (per spec).
Functions return scalar values that substitute into config values exactly
as Tier 0 values do.

Parsing strategy: multi-pass regex substitution. Each pass resolves the
innermost @aq.* calls (those with no nested @aq. in their argument list).
Nested calls like @aq.date.offset(base=@aq.date.today(), days=-7) are
resolved in two passes: today() first, then offset().
"""

from __future__ import annotations

import ast
import os
import re
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

# Matches the innermost @aq.* call — args must not contain another @aq.
_INNER_CALL_RE = re.compile(r"@aq\.([\w.]+)\(([^@)]*)\)")

# Detects any remaining @aq. token
_ANY_TIER1_RE = re.compile(r"@aq\.")

# Java SimpleDateFormat → Python strftime mapping (ordered: longer patterns first)
_DATE_FMT_MAP = [
    ("yyyy", "%Y"),
    ("MM", "%m"),
    ("dd", "%d"),
    ("yy", "%y"),
]


def _java_to_strftime(pattern: str) -> str:
    result = pattern
    for java, py in _DATE_FMT_MAP:
        result = result.replace(java, py)
    return result


class AqFunctions:
    """Callable registry for all @aq.* Tier 1 functions."""

    def __init__(self, run_id: str | None = None, depot: Any = None) -> None:
        self._run_id = run_id or str(uuid.uuid4())
        self._depot = depot  # connected in Phase 4 (Surveyor)

    # ── date ──────────────────────────────────────────────────────────────────

    def date_today(self, format: str = "yyyy-MM-dd") -> str:  # noqa: A002
        return date.today().strftime(_java_to_strftime(format))

    def date_yesterday(self, format: str = "yyyy-MM-dd") -> str:  # noqa: A002
        return (date.today() - timedelta(days=1)).strftime(_java_to_strftime(format))

    def date_offset(self, base: str, days: int | str) -> str:
        return (date.fromisoformat(base) + timedelta(days=int(days))).isoformat()

    def date_month_start(self, format: str = "yyyy-MM-dd") -> str:  # noqa: A002
        return date.today().replace(day=1).strftime(_java_to_strftime(format))

    def date_format(self, date_str: str, pattern: str) -> str:
        return date.fromisoformat(date_str).strftime(_java_to_strftime(pattern))

    # ── runtime ───────────────────────────────────────────────────────────────

    def runtime_timestamp(self) -> str:
        return datetime.now(tz=timezone.utc).isoformat()

    def runtime_run_id(self) -> str:
        return self._run_id

    def runtime_prev_run_id(self) -> str:
        # Phase 4: connect to Depot for real prev-run lookup
        return ""

    # ── secret / env ──────────────────────────────────────────────────────────

    def secret(self, key: str) -> str:
        val = os.environ.get(key)
        if val is None:
            raise RuntimeError(
                f"@aq.secret: secret {key!r} not found. "
                "Configure a secrets provider in aqueduct.yml or set the env var."
            )
        return val

    def env(self, var: str) -> str:
        val = os.environ.get(var)
        if val is None:
            raise RuntimeError(
                f"@aq.env: variable {var!r} is not set. "
                "Unlike ${VAR:-default}, @aq.env fails fast when the variable is absent."
            )
        return val

    # ── depot ─────────────────────────────────────────────────────────────────

    def depot_get(self, key: str, default: str = "") -> str:
        if self._depot is not None:
            return str(self._depot.get(key, default))
        return default


# ── Dispatch table ─────────────────────────────────────────────────────────────

_DISPATCH: dict[str, str] = {
    "aq.date.today": "date_today",
    "aq.date.yesterday": "date_yesterday",
    "aq.date.offset": "date_offset",
    "aq.date.month_start": "date_month_start",
    "aq.date.format": "date_format",
    "aq.runtime.timestamp": "runtime_timestamp",
    "aq.runtime.run_id": "runtime_run_id",
    "aq.runtime.prev_run_id": "runtime_prev_run_id",
    "aq.secret": "secret",
    "aq.env": "env",
    "aq.depot.get": "depot_get",
}


def _call(registry: AqFunctions, func_path: str, args_str: str) -> str:
    """Dispatch a parsed @aq.* call to the registry method."""
    method_name = _DISPATCH.get(func_path)
    if not method_name:
        raise ValueError(f"Unknown @aq function: {func_path!r}")
    method = getattr(registry, method_name)

    if not args_str.strip():
        return str(method())

    # Use Python's ast to safely parse the argument list
    try:
        tree = ast.parse(f"_f({args_str})", mode="eval")
        call_node = tree.body  # type: ignore[union-attr]
        args = [ast.literal_eval(a) for a in call_node.args]
        kwargs = {kw.arg: ast.literal_eval(kw.value) for kw in call_node.keywords}
    except (SyntaxError, ValueError) as exc:
        raise ValueError(
            f"Cannot parse arguments for {func_path}({args_str!r}): {exc}"
        ) from exc

    result = method(*args, **kwargs)
    return "" if result is None else str(result)


def resolve_tier1_str(value: str, registry: AqFunctions) -> str:
    """Resolve all @aq.* calls within a single string, innermost-first."""
    for _ in range(20):
        m = _INNER_CALL_RE.search(value)
        if not m:
            break
        func_path = f"aq.{m.group(1)}"
        args_str = m.group(2)
        replacement = _call(registry, func_path, args_str)
        value = value[: m.start()] + replacement + value[m.end() :]

    if _ANY_TIER1_RE.search(value):
        raise ValueError(
            f"Unresolvable @aq.* reference after 20 passes (possible cycle): {value!r}"
        )
    return value


def resolve_tier1(value: Any, registry: AqFunctions) -> Any:
    """Recursively resolve @aq.* in any value (str / dict / list / scalar)."""
    if isinstance(value, str):
        return resolve_tier1_str(value, registry)
    if isinstance(value, dict):
        return {k: resolve_tier1(v, registry) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_tier1(item, registry) for item in value]
    return value
