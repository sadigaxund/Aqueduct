"""Internal, read-only diagnostics tool registry (Phase 73).

Thin re-export — the registry, ``Tool`` dataclass, and built-in tools live in
``registry.py``. See that module's docstring for the contract.
"""

from __future__ import annotations

from aqueduct.tools.registry import (
    AQ_TOOLS_ENTRYPOINT_GROUP,
    REGISTRY,
    Tool,
    call_tool,
    get_tools,
)

__all__ = [
    "AQ_TOOLS_ENTRYPOINT_GROUP",
    "REGISTRY",
    "Tool",
    "call_tool",
    "get_tools",
]
