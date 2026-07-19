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


def check(manifest: Any, engine: str = "spark") -> list[str]:
    # HALF-true: the underlying risk (a custom callable materializing the
    # full dataset) is not Spark-exclusive, but this rule's wording is —
    # "runs on the driver" and ".collect()/.count()" are Spark's process
    # topology and action API. DuckDB is single-process (no driver/executor
    # split), and today Probe is `module.type.Probe: unsupported` on DuckDB
    # (duckdb_/capabilities.yml) so this can't fire there yet regardless.
    # Gated on Spark for now rather than inventing DuckDB-flavored wording
    # this rule was never verified against — see PR notes for a human
    # follow-up once DuckDB grows a Probe implementation.
    if engine != "spark":
        return []
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
