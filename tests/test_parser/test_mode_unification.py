"""2.0 mode config: canonical keys only (deprecated aliases removed).

Removed in 2.0: the `approval_mode` YAML key, `aggressive_max_patches`,
`danger.allow_aggressive_patching`, and the `--allow-aggressive` CLI flag. 
"""
from __future__ import annotations

import json

import pytest

pytestmark = pytest.mark.unit


def _write_bp(tmp_path, agent_block: str = "", extra: str = ""):
    bp = tmp_path / "bp.yml"
    bp.write_text(
        "aqueduct: '1.0'\n"
        "id: test_bp\n"
        "name: Test BP\n"
        f"{agent_block}"
        "modules:\n"
        "  - id: m1\n"
        "    type: Ingress\n"
        "    label: M1\n"
        "    config: {format: csv, path: data.csv}\n"
        "edges: []\n"
        f"{extra}",
        encoding="utf-8",
    )
    return bp


def test_approval_aggressive_value_rejected(tmp_path):
    """`approval: aggressive` no longer parses (removed in 2.0) → ParseError."""
    from aqueduct.parser.parser import parse, ParseError

    bp = _write_bp(tmp_path, agent_block="agent:\n  approval: aggressive\n")
    with pytest.raises(ParseError):
        parse(bp)


def test_approval_mode_key_rejected(tmp_path):
    """The former `approval_mode` YAML key is rejected (extra=forbid)."""
    from aqueduct.parser.parser import parse, ParseError

    bp = _write_bp(tmp_path, agent_block="agent:\n  approval_mode: auto\n")
    with pytest.raises(ParseError):
        parse(bp)


def test_aggressive_max_patches_rejected(tmp_path):
    """`aggressive_max_patches` alias removed → rejected."""
    from aqueduct.parser.parser import parse, ParseError

    bp = _write_bp(tmp_path, agent_block="agent:\n  aggressive_max_patches: 3\n")
    with pytest.raises(ParseError):
        parse(bp)


def test_max_patches_canonical(tmp_path):
    from aqueduct.parser.parser import parse

    bp = _write_bp(tmp_path, agent_block="agent:\n  max_patches: 3\n")
    assert parse(bp).agent.max_patches == 3


def test_default_max_patches_is_one(tmp_path):
    from aqueduct.parser.parser import parse

    assert parse(_write_bp(tmp_path)).agent.max_patches == 1


def test_danger_allow_multi_patch_canonical(tmp_path):
    """`danger.allow_multi_patch: true` is honoured."""
    from aqueduct.config import load_config

    cfg_file = tmp_path / "aqueduct.yml"
    cfg_file.write_text(
        "aqueduct_config: '1.0'\ndanger:\n  allow_multi_patch: true\n", encoding="utf-8"
    )
    assert load_config(cfg_file).danger.allow_multi_patch is True


def test_danger_allow_aggressive_patching_rejected(tmp_path):
    """The `allow_aggressive_patching` alias was removed → rejected."""
    from aqueduct.config import load_config, ConfigError

    cfg_file = tmp_path / "aqueduct.yml"
    cfg_file.write_text(
        "aqueduct_config: '1.0'\ndanger:\n  allow_aggressive_patching: true\n", encoding="utf-8"
    )
    with pytest.raises(ConfigError):
        load_config(cfg_file)


def test_compiler_manifest_serialises_max_patches(tmp_path):
    from aqueduct.parser.parser import parse
    from aqueduct.compiler.compiler import compile as compile_bp

    bp = _write_bp(tmp_path, agent_block="agent:\n  max_patches: 4\n")
    manifest = compile_bp(parse(bp), blueprint_path=bp)
    agent_dump = manifest.to_dict()["agent"]
    assert agent_dump["max_patches"] == 4
    assert "aggressive_max_patches" not in agent_dump
    json.dumps(agent_dump)  # serialisable


@pytest.mark.spark
def test_allow_multi_patch_flag_works(tmp_path):
    """`--allow-multi-patch` overrides danger.allow_multi_patch=false (no block)."""
    from click.testing import CliRunner
    from unittest.mock import MagicMock, patch
    from aqueduct.cli import cli

    bp = _write_bp(tmp_path, agent_block="agent:\n  approval: auto\n  max_patches: 2\n")
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text("aqueduct_config: '1.0'\ndanger:\n  allow_multi_patch: false\n", encoding="utf-8")

    runner = CliRunner()
    with patch("aqueduct.surveyor.surveyor.Surveyor"), \
         patch("aqueduct.executor.get_executor") as mock_get_exec:
        mock_exec = MagicMock()
        mock_exec.return_value = MagicMock(status="success", module_results=(), trigger_agent=False)
        mock_get_exec.return_value = mock_exec
        result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg), "--allow-multi-patch"])
    assert "requires danger.allow_multi_patch: true" not in result.output


@pytest.mark.spark
def test_max_patches_gt_one_blocks_without_danger_or_flag(tmp_path):
    """`max_patches: 2` without danger gate AND without --allow-multi-patch → exit 1."""
    from click.testing import CliRunner
    from unittest.mock import patch
    from aqueduct.cli import cli

    bp = _write_bp(tmp_path, agent_block="agent:\n  approval: auto\n  max_patches: 2\n")
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text("aqueduct_config: '1.0'\ndanger:\n  allow_multi_patch: false\n", encoding="utf-8")

    runner = CliRunner()
    with patch("aqueduct.surveyor.surveyor.Surveyor"):
        result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])
    assert result.exit_code == 1
    assert "max_patches=2" in result.output
    assert "allow_multi_patch" in result.output
