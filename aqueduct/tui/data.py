"""Read-only data layer for the frozen `aqueduct studio` TUI.

The actual queries now live in the shared, viewer-agnostic
``aqueduct.stores.queries`` module (one read layer behind the TUI, the Streamlit
dashboard, and `report --json` — no duplication). This module re-exports the
names the textual app already imports so the frozen TUI keeps working unchanged.
"""

from __future__ import annotations

from aqueduct.stores.queries import (  # noqa: F401
    LineageRow,
    ModuleResult,
    ProfileRow,
    RunDetail,
    RunRow,
    StoreHandle,
    discover_stores,
    lineage,
    list_runs,
    run_detail,
    run_sql_readonly,
)

__all__ = [
    "LineageRow",
    "ModuleResult",
    "ProfileRow",
    "RunDetail",
    "RunRow",
    "StoreHandle",
    "discover_stores",
    "lineage",
    "list_runs",
    "run_detail",
    "run_sql_readonly",
]
