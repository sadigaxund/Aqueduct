"""MCP stdio server over the read-only diagnostics ToolRegistry (Phase 74).

Thin package: ``server.py`` owns the declaration builder + the stdio serve
loop. Top-level ``import aqueduct.mcp`` must NOT require the ``mcp`` SDK —
the SDK is only imported inside ``serve()`` (the ``[mcp]`` dev-tooling
extra), mirroring how ``aqueduct studio`` guards on ``textual``.
"""

from __future__ import annotations

from aqueduct.mcp.server import build_tool_declarations, serve

__all__ = ["build_tool_declarations", "serve"]
