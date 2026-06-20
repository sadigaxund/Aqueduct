"""Custom probe signal that resolves to driver-side code (Phase 60).

A ``type: custom`` probe using a ``module:``/``entry:`` pointer or a ``plugin:``
entry-point runs arbitrary Python on the Spark driver. That code is trusted like
a UDF, but unlike the built-in signals the engine cannot guarantee it honours the
zero-cost-observability contract — a callable is free to ``.collect()`` /
``.count()`` the full DataFrame. Surface it so the cost is a conscious choice.

Inline-SQL custom signals (``sql:`` / ``passed_when:``) do NOT trigger this rule:
they execute as ordinary Spark expressions, not driver code.
"""

from __future__ import annotations

from typing import Any

from aqueduct.parser.models import ModuleType

RULE_ID = "custom_probe_driver_code"


def check(manifest: Any) -> list[str]:
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
                    f"Probe '{m.id}' uses a custom signal backed by driver-side "
                    f"code ({ref}). It runs on the driver and the engine cannot "
                    "enforce zero-cost observability — if the callable performs a "
                    "full Spark action (.collect()/.count()) you own that cost. "
                    "Prefer the inline-SQL form (sql:/passed_when:) when the signal "
                    "can be expressed as a Spark expression."
                )
    return out
