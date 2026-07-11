"""CLI tests for the `aqueduct mcp` group (serve is stdio-blocking, so only
the registration + extra-missing gate are testable here; the server itself
is covered by tests/test_mcp/)."""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from aqueduct import exit_codes
from aqueduct.cli import cli

pytestmark = pytest.mark.unit


def test_mcp_group_and_serve_are_registered():
    assert "mcp" in cli.commands
    assert "serve" in cli.commands["mcp"].commands


def test_mcp_serve_without_sdk_prints_install_hint(monkeypatch):
    import importlib.util

    real_find_spec = importlib.util.find_spec

    def fake_find_spec(name, *args, **kwargs):
        if name == "mcp":
            return None
        return real_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)
    runner = CliRunner()
    result = runner.invoke(cli, ["mcp", "serve"])
    assert result.exit_code == exit_codes.CONFIG_ERROR
    assert "aqueduct-core[mcp]" in result.output
