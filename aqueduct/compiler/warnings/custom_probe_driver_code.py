"""Custom probe signal that resolves to driver-side code (Phase 60; reworded
engine-neutral in Pass F once DuckDB grew a real Probe implementation).

A ``type: custom`` probe using a ``module:``/``entry:`` pointer or a ``plugin:``
entry-point runs arbitrary Python. That code is trusted like a UDF, but unlike
the built-in signals the engine cannot guarantee it honours the
zero-cost-observability contract — a callable is free to materialize the full
dataset (``.collect()``/``.count()`` on Spark, ``.fetchall()``/``.df()`` on
DuckDB). Surface it so the cost is a conscious choice, regardless of engine —
the risk is the same on either: arbitrary Python holding a live handle to the
full relation/DataFrame.

Inline-SQL custom signals (``sql:`` / ``passed_when:``) do NOT trigger this rule:
they execute as ordinary SQL expressions, not driver code.
"""

from __future__ import annotations

from typing import Any

from aqueduct.parser.models import ModuleType

RULE_ID = "custom_probe_driver_code"


def check(manifest: Any, engine: str = "spark") -> list[str]:  # noqa: ARG001 — engine kept for RULES call-signature parity
    """Fires on every engine — the risk and wording are both engine-neutral
    (see module docstring). ``engine`` is accepted only because ``run_all()``
    calls every rule uniformly as ``check(manifest, engine)``."""
    out: list[str] = []
    for m in manifest.modules:
        if m.type != ModuleType.Probe:
            continue
        for sig in (m.config or {}).get("signals", []) or []:
            if not isinstance(sig, dict) or sig.get("type") != "custom":
                continue
            if sig.get("plugin") or sig.get("module") or sig.get("entry"):
                ref = sig.get("plugin") or f"{sig.get('module')}:{sig.get('entry')}"
                out.append(
                    f"Probe '{m.id}' uses a custom signal backed by user code "
                    f"({ref}). It runs as ordinary Python and the engine cannot "
                    "enforce zero-cost observability — if the callable materializes "
                    "the full dataset you own that cost. Prefer the inline-SQL form "
                    "(sql:/passed_when:) when the signal can be expressed as SQL."
                )
    return out
