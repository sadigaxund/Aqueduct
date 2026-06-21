"""Phase 68 — `aqueduct dashboard` CLI wiring (Streamlit launcher).

The dashboard app itself runs only under `streamlit run` (it imports streamlit +
plotly at module top), so it isn't exercised here. These tests cover the launcher
contract: the command is registered, base CLI import never pulls streamlit, and a
missing `dashboard` extra fails gracefully.
"""
from __future__ import annotations

import importlib.util

import pytest
from click.testing import CliRunner

from aqueduct.cli import cli
from aqueduct import exit_codes

pytestmark = pytest.mark.unit


def test_dashboard_command_registered():
    assert "dashboard" in cli.commands


def test_base_cli_import_does_not_require_streamlit():
    # Importing the CLI must not pull the optional dashboard deps. Checked in a
    # clean subprocess so another test importing streamlit can't false-positive.
    import subprocess
    import sys

    r = subprocess.run(
        [sys.executable, "-c",
         "import aqueduct.cli, sys; "
         "assert 'streamlit' not in sys.modules, 'CLI import pulled streamlit'"],
        capture_output=True, text=True,
    )
    assert r.returncode == 0, r.stderr


def test_dashboard_missing_extra_errors_gracefully(monkeypatch):
    # Force the "streamlit not installed" path regardless of the environment.
    real = importlib.util.find_spec

    def fake(name, *a, **k):
        return None if name == "streamlit" else real(name, *a, **k)

    monkeypatch.setattr(importlib.util, "find_spec", fake)
    result = CliRunner().invoke(cli, ["dashboard"])
    assert result.exit_code == exit_codes.CONFIG_ERROR
    assert "dashboard" in result.output and "pip install" in result.output


def test_dashboard_launch_invokes_streamlit(monkeypatch):
    """When the extra is present, it shells out to `streamlit run <app.py>`."""
    real = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util, "find_spec",
        lambda name, *a, **k: object() if name == "streamlit" else real(name, *a, **k),
    )
    calls = {}

    def fake_call(args, env=None):
        calls["args"] = args
        calls["env"] = env
        return 0

    import subprocess
    monkeypatch.setattr(subprocess, "call", fake_call)
    result = CliRunner().invoke(
        cli, ["dashboard", "--config", "aqueduct.yml", "--store-dir", "/tmp/obs", "--port", "9999"]
    )
    assert result.exit_code == 0
    assert "streamlit" in calls["args"] and "run" in calls["args"]
    assert calls["args"][-1] == "false" or "9999" in calls["args"]
    assert calls["env"].get("AQ_DASH_CONFIG") == "aqueduct.yml"
    assert calls["env"].get("AQ_DASH_STORE_DIR") == "/tmp/obs"
