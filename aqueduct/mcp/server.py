"""`aqueduct mcp serve` — stdio MCP server over the diagnostics ToolRegistry.

A thin MCP (Model Context Protocol) binding for ``aqueduct/tools/registry.py``:
every registered ``Tool`` is exposed as one MCP tool, its ``name`` /
``description`` / ``params_schema`` handed straight through as the MCP tool
declaration (the registry promises ``params_schema`` is JSON-schema-shaped for
exactly this hand-off). The server is:

- **Read-only, structurally.** It imports only the registry — no store write
  APIs anywhere in this module. Every invocation goes through
  ``aqueduct.tools.call_tool()`` (the redaction chokepoint), never a handler
  directly; exception messages surfaced to the client are redacted too.
- **stdio / local-only in v1.** No network transport, no ports — the client
  (Claude Desktop, an IDE, ``mcp`` CLI) spawns ``aqueduct mcp serve`` as a
  subprocess and speaks JSON-RPC over stdin/stdout.
- **SDK-gated.** The ``mcp`` SDK is a dev-tooling extra
  (``pip install aqueduct-core[mcp]``); this module's top level stays
  SDK-free — only ``serve()`` (and the private builders it calls) import it,
  mirroring the ``studio``/``textual`` pattern. ``build_tool_declarations()``
  is pure and importable on a base install.

Tool handlers run synchronously on the event loop — acceptable for a local
single-client stdio server whose tools are short DuckDB/Postgres reads; a
long ``doctor --preflight`` blocks other requests for its duration.
"""

from __future__ import annotations

from typing import Any

from aqueduct.tools.registry import REGISTRY, call_tool, get_tools

_SERVER_NAME = "aqueduct"
_INSTRUCTIONS = (
    "Read-only diagnostics over Aqueduct's observability store: recent runs, "
    "run detail, column lineage, patch lifecycle, probe signals, health "
    "checks, and per-blueprint remediation timelines. No tool can modify "
    "pipelines, patches, or stores."
)


def build_tool_declarations() -> list[dict[str, Any]]:
    """Registry tools as plain MCP tool-declaration dicts (SDK-free, pure).

    ``params_schema`` is passed through verbatim as ``inputSchema`` — no
    translation layer. If a schema is ever malformed for MCP, fix it in
    ``tools/registry.py``, not here.
    """
    return [
        {
            "name": t.name,
            "description": t.description,
            "inputSchema": t.params_schema,
        }
        for t in get_tools()
    ]


def _build_server(config_path: str | None = None):
    """Construct the MCP ``Server`` (imports the SDK — call only when gated).

    ``config_path`` (the CLI's ``--config``) is injected into any invocation
    whose tool accepts a ``config_path`` param and whose arguments don't
    already set it — so one server flag scopes every tool to the right
    ``aqueduct.yml`` without each client call repeating it.
    """
    import mcp.types as types
    from mcp.server import Server

    server = Server(_SERVER_NAME, instructions=_INSTRUCTIONS)

    @server.list_tools()
    async def _list_tools() -> list[types.Tool]:
        return [types.Tool(**decl) for decl in build_tool_declarations()]

    @server.call_tool()
    async def _call_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        kwargs = dict(arguments)
        tool = REGISTRY.get(name)
        if (
            config_path is not None
            and tool is not None
            and "config_path" in tool.params_schema.get("properties", {})
            and kwargs.get("config_path") is None
        ):
            kwargs["config_path"] = config_path
        try:
            # The ONE invocation path — call_tool() applies redaction to the
            # result. Never call a handler directly from here.
            result = call_tool(name, **kwargs)
        except Exception as exc:
            # The SDK turns any exception into a structured isError tool
            # result (never crashing the server loop) using str(exc) — but
            # without redaction, and an error message can embed a resolved
            # secret (DSNs, tracebacked config values). Re-raise with the
            # message scrubbed.
            from aqueduct import redaction

            raise RuntimeError(redaction.redact(f"{type(exc).__name__}: {exc}")) from None
        # dict → MCP structuredContent; anything else (list/str/None/scalar)
        # would be misread as an iterable of content blocks — wrap it.
        return result if isinstance(result, dict) else {"result": result}

    return server


async def _run_stdio(server: Any) -> None:
    from mcp.server.stdio import stdio_server

    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


def serve(config_path: str | None = None) -> int:
    """Run the stdio MCP server until the client disconnects. Returns 0.

    Requires the ``mcp`` SDK (the CLI guards with ``find_spec`` and an
    install hint before calling this).
    """
    import anyio  # a dependency of the mcp SDK — present whenever mcp is

    server = _build_server(config_path=config_path)

    async def _main() -> None:
        await _run_stdio(server)

    anyio.run(_main)
    return 0
