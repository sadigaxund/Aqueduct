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
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
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

    def __init__(
        self,
        run_id: str | None = None,
        depot: Any = None,
        execution_date: date | None = None,
        secrets_provider: str = "env",
        secrets_region: str | None = None,
        secrets_resolver: str | None = None,
        blueprint_id: str | None = None,
        blueprint_name: str | None = None,
        blueprint_path: "str | Path | None" = None,
        deployment_env: str | None = None,
        deployment_target: str | None = None,
    ) -> None:
        self._run_id = run_id or str(uuid.uuid4())
        self._depot = depot
        self._execution_date = execution_date  # None = use system clock
        self._secrets_provider = secrets_provider
        self._secrets_region = secrets_region
        self._secrets_resolver = secrets_resolver
        # @aq.blueprint.* / @aq.deployment.* / @aq.version — identity & deploy context.
        self._blueprint_id = blueprint_id
        self._blueprint_name = blueprint_name
        self._blueprint_path = Path(blueprint_path) if blueprint_path else None
        self._deployment_env = deployment_env
        self._deployment_target = deployment_target

    def _base_date(self) -> date:
        return self._execution_date if self._execution_date is not None else date.today()

    # ── date ──────────────────────────────────────────────────────────────────

    def date_today(self, format: str = "yyyy-MM-dd") -> str:  # noqa: A002
        return self._base_date().strftime(_java_to_strftime(format))

    def date_yesterday(self, format: str = "yyyy-MM-dd") -> str:  # noqa: A002
        return (self._base_date() - timedelta(days=1)).strftime(_java_to_strftime(format))

    def date_offset(self, base: str, days: int | str) -> str:
        return (date.fromisoformat(base) + timedelta(days=int(days))).isoformat()

    def date_month_start(self, format: str = "yyyy-MM-dd") -> str:  # noqa: A002
        return self._base_date().replace(day=1).strftime(_java_to_strftime(format))

    def date_format(self, date_str: str, pattern: str) -> str:
        return date.fromisoformat(date_str).strftime(_java_to_strftime(pattern))

    # ── run — this execution ────────────────────────────────────────────────────

    def run_timestamp(self) -> str:
        if self._execution_date is not None:
            return datetime.combine(self._execution_date, time.min, tzinfo=timezone.utc).isoformat()
        return datetime.now(tz=timezone.utc).isoformat()

    def run_id(self) -> str:
        return self._run_id

    def run_prev_id(self) -> str:
        return self.depot_get("_last_run_id", "")

    # ── secret / env ──────────────────────────────────────────────────────────

    def secret(self, key: str) -> str:
        from aqueduct.secrets import SecretsError, resolve_secret
        try:
            return resolve_secret(
                key,
                provider=self._secrets_provider,
                region=self._secrets_region,
                resolver=self._secrets_resolver,
            )
        except SecretsError as exc:
            raise RuntimeError(str(exc)) from exc

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
        import logging as _logging
        _logging.getLogger(__name__).warning(
            "@aq.depot.get('%s') called but no depot backend is configured — "
            "returning default '%s'. Incremental pipelines will re-read all data.",
            key, default,
        )
        return default

    # ── blueprint / deployment / version — identity & deploy context ─────────────
    # Resolvable only during blueprint compilation (NOT in aqueduct.yml, where no
    # blueprint/run exists yet). Grouped by subject, not by dynamic-vs-static.

    def _require(self, token: str, value: Any) -> str:
        if value is None or value == "":
            raise RuntimeError(
                f"@aq.{token} is not available here — it resolves only during blueprint "
                "compilation, not in aqueduct.yml (no blueprint/run context exists at "
                "config-load time)."
            )
        return str(value)

    def blueprint_id(self) -> str:
        return self._require("blueprint.id", self._blueprint_id)

    def blueprint_name(self) -> str:
        return self._require("blueprint.name", self._blueprint_name)

    def blueprint_path(self) -> str:
        return self._require(
            "blueprint.path", str(self._blueprint_path) if self._blueprint_path else None)

    def blueprint_dir(self) -> str:
        return self._require(
            "blueprint.dir", str(self._blueprint_path.parent) if self._blueprint_path else None)

    def deployment_env(self) -> str:
        return self._require("deployment.env", self._deployment_env)

    def deployment_target(self) -> str:
        return self._require("deployment.target", self._deployment_target)

    def engine_version(self) -> str:
        from aqueduct import __version__
        return __version__


# ── Dispatch table ─────────────────────────────────────────────────────────────
# Subject-grouped namespaces: date · run · blueprint · deployment · depot ·
# secret · env · version. The namespace names what the value is ABOUT.

_DISPATCH: dict[str, str] = {
    "aq.date.today": "date_today",
    "aq.date.yesterday": "date_yesterday",
    "aq.date.offset": "date_offset",
    "aq.date.month_start": "date_month_start",
    "aq.date.format": "date_format",
    "aq.run.id": "run_id",
    "aq.run.timestamp": "run_timestamp",
    "aq.run.prev_id": "run_prev_id",
    "aq.secret": "secret",
    "aq.env": "env",
    "aq.depot.get": "depot_get",
    "aq.blueprint.id": "blueprint_id",
    "aq.blueprint.name": "blueprint_name",
    "aq.blueprint.path": "blueprint_path",
    "aq.blueprint.dir": "blueprint_dir",
    "aq.deployment.env": "deployment_env",
    "aq.deployment.target": "deployment_target",
    "aq.version": "engine_version",
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
