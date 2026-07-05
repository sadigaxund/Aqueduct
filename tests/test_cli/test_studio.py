"""Phase 67 — `aqueduct studio` CLI guard (TUI behaviour itself is verified manually).

The textual app can't be unit-tested headless; here we only assert the CLI tells
the user to install the optional extra when textual is absent.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest
from click.testing import CliRunner

from aqueduct.cli import cli

pytestmark = pytest.mark.unit


def test_studio_without_textual_prompts_install():
    # Force the "textual not installed" branch regardless of the test env.
    with patch("importlib.util.find_spec", return_value=None):
        res = CliRunner().invoke(cli, ["studio"])
    assert res.exit_code != 0
    assert "tui" in res.output and "pip install" in res.output


def test_studio_is_registered():
    assert "studio" in cli.commands
