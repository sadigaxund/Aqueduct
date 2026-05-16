import pytest
from click.testing import CliRunner
from aqueduct.cli import cli
import aqueduct
import importlib

def test_cli_version():
    """aqueduct --version exits 0 and prints aqueduct <version>"""
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert result.output.startswith("aqueduct ")

def test_version_fallback(monkeypatch):
    """aqueduct.__version__ falls back to '0.0.0+unknown' when PackageNotFoundError occurs"""
    import importlib.metadata
    def mock_version(pkg):
        raise importlib.metadata.PackageNotFoundError
    
    monkeypatch.setattr(importlib.metadata, "version", mock_version)
    
    # Reload aqueduct to re-trigger version detection
    importlib.reload(aqueduct)
    
    assert aqueduct.__version__ == "0.0.0+unknown"

def test_cli_help_lists_version():
    """aqueduct --help lists --version in its options block"""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "--version" in result.output
    assert "Show the version and exit." in result.output
