"""`mcp` command group — the stdio MCP diagnostics server.

`aqueduct mcp serve` exposes the read-only ToolRegistry (`aqueduct/tools/`)
over the Model Context Protocol on stdin/stdout, so MCP clients (Claude
Desktop, IDEs) can query runs, lineage, patches, probe signals, doctor, and
blueprint history. Local-only, no network transport in v1. Requires the
optional `mcp` extra; guarded with find_spec + an install hint, mirroring
`studio`'s textual gate.
"""

from __future__ import annotations

import sys

import click

from aqueduct import exit_codes
from aqueduct.cli import cli


@cli.group("mcp")
def mcp_group() -> None:
    """MCP diagnostics server (read-only)."""


@mcp_group.command("serve")
@click.option(
    "--config",
    "config_path",
    default=None,
    help="Path to aqueduct.yml — injected into every tool call that accepts "
    "config_path (unless the client sets it explicitly).",
)
def mcp_serve(config_path: str | None) -> None:
    """Serve the read-only diagnostics tools over MCP on stdin/stdout.

    The MCP client spawns this command as a subprocess and speaks JSON-RPC
    over the pipes — do not run it interactively. Every tool is read-only
    (no tool can modify pipelines, patches, or stores) and every result is
    secret-redacted. Requires the optional 'mcp' extra:
    pip install aqueduct-core[mcp]
    """
    import importlib.util

    if importlib.util.find_spec("mcp") is None:
        click.echo(
            "✗ aqueduct mcp serve needs the 'mcp' extra: pip install aqueduct-core[mcp]",
            err=True,
        )
        sys.exit(exit_codes.CONFIG_ERROR)

    from aqueduct.mcp.server import serve

    try:
        code = serve(config_path=config_path)
    except KeyboardInterrupt:
        sys.exit(130)
    sys.exit(exit_codes.SUCCESS if code == 0 else exit_codes.DATA_OR_RUNTIME)
