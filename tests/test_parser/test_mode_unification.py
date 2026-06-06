"""1.1.0 mode unification: approval_mode aliases, max_patches rename, danger
alias resolution, default values, and manifest serialisation.

Covers TEST_MANIFEST.md ⏳ items under
"Mode unification: approval_mode: aggressive → auto + max_patches (1.1.0)".
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


def test_approval_mode_aggressive_emits_deprecation(tmp_path, capsys):
    """Blueprint with approval_mode: aggressive parses + emits [deprecated] on stderr."""
    from aqueduct.parser.parser import parse

    bp = _write_bp(
        tmp_path,
        agent_block="agent:\n  approval_mode: aggressive\n",
    )
    blueprint = parse(bp)
    err = capsys.readouterr().err
    assert "[deprecated] approval_mode: aggressive" in err
    # NOTE: parser intentionally keeps the literal "aggressive" string; CLI
    # bootstrap normalises it to "auto" before downstream branching. See
    # aqueduct/cli.py:1383 — the parser layer is a passthrough so the
    # deprecation surface is exposed to both human and tooling readers.
    assert blueprint.agent.approval_mode == "aggressive"


def test_aggressive_max_patches_alias_resolves_to_max_patches(tmp_path, capsys):
    """`aggressive_max_patches: 3` (no max_patches) populates manifest.agent.max_patches == 3."""
    from aqueduct.parser.parser import parse

    bp = _write_bp(
        tmp_path,
        agent_block="agent:\n  aggressive_max_patches: 3\n",
    )
    blueprint = parse(bp)
    assert blueprint.agent.max_patches == 3
    err = capsys.readouterr().err
    assert "[deprecated] aggressive_max_patches" in err


def test_both_max_patches_and_alias_max_patches_wins(tmp_path):
    """Both `max_patches` and `aggressive_max_patches` provided → max_patches wins per validation_alias order."""
    from aqueduct.parser.parser import parse

    bp = _write_bp(
        tmp_path,
        agent_block="agent:\n  max_patches: 3\n  aggressive_max_patches: 5\n",
    )
    blueprint = parse(bp)
    assert blueprint.agent.max_patches == 3


def test_default_max_patches_is_one(tmp_path):
    """Default `max_patches` is 1 (formerly 5 for `aggressive_max_patches`)."""
    from aqueduct.parser.parser import parse

    bp = _write_bp(tmp_path)
    blueprint = parse(bp)
    assert blueprint.agent.max_patches == 1


def test_danger_allow_aggressive_patching_alias_resolves_to_allow_multi_patch(tmp_path):
    """`danger.allow_aggressive_patching: true` honoured as alias for `allow_multi_patch: true`."""
    from aqueduct.config import load_config

    cfg_file = tmp_path / "aqueduct.yml"
    cfg_file.write_text(
        "aqueduct_config: '1.0'\n"
        "danger:\n"
        "  allow_aggressive_patching: true\n",
        encoding="utf-8",
    )
    cfg = load_config(cfg_file)
    assert cfg.danger.allow_multi_patch is True


def test_compiler_manifest_serialises_max_patches_not_aggressive_max(tmp_path):
    """Compiler manifest JSON dump carries `max_patches`, not legacy `aggressive_max_patches`."""
    from aqueduct.parser.parser import parse
    from aqueduct.compiler.compiler import compile as compile_bp

    bp = _write_bp(
        tmp_path,
        agent_block="agent:\n  max_patches: 4\n",
    )
    blueprint = parse(bp)
    manifest = compile_bp(blueprint, blueprint_path=bp)
    payload = manifest.to_dict()
    agent_dump = payload["agent"]
    assert "max_patches" in agent_dump
    assert agent_dump["max_patches"] == 4
    assert "aggressive_max_patches" not in agent_dump
    # Round-trips through json.dumps without TypeError.
    json.dumps(agent_dump)


@pytest.mark.spark
def test_cli_allow_aggressive_flag_still_works(tmp_path):
    """CLI --allow-aggressive still works as alias for --allow-multi-patch (no exit-1 on max_patches > 1)."""
    from click.testing import CliRunner
    from unittest.mock import MagicMock, patch
    from aqueduct.cli import cli

    bp = _write_bp(
        tmp_path,
        agent_block=(
            "agent:\n"
            "  approval_mode: auto\n"
            "  max_patches: 2\n"
        ),
    )
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(
        "aqueduct_config: '1.0'\n"
        "danger:\n"
        "  allow_multi_patch: false\n",
        encoding="utf-8",
    )

    runner = CliRunner()
    with patch("aqueduct.surveyor.surveyor.Surveyor"), \
         patch("aqueduct.executor.get_executor") as mock_get_exec:
        mock_exec = MagicMock()
        mock_exec.return_value = MagicMock(status="success", module_results=(), trigger_agent=False)
        mock_get_exec.return_value = mock_exec
        result = runner.invoke(
            cli,
            ["run", str(bp), "--config", str(cfg), "--allow-aggressive"],
        )
    # Should NOT have been blocked by multi-patch gate (the flag overrides the
    # `danger.allow_multi_patch=false` config).
    assert "requires danger.allow_multi_patch: true" not in result.output


@pytest.mark.spark
def test_max_patches_gt_one_blocks_without_danger_or_flag(tmp_path):
    """`max_patches: 2` without danger gate AND without --allow-multi-patch → exit 1 with new-name error."""
    from click.testing import CliRunner
    from unittest.mock import patch
    from aqueduct.cli import cli

    bp = _write_bp(
        tmp_path,
        agent_block=(
            "agent:\n"
            "  approval_mode: auto\n"
            "  max_patches: 2\n"
        ),
    )
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(
        "aqueduct_config: '1.0'\n"
        "danger:\n"
        "  allow_multi_patch: false\n",
        encoding="utf-8",
    )

    runner = CliRunner()
    with patch("aqueduct.surveyor.surveyor.Surveyor"):
        result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])
    assert result.exit_code == 1
    assert "max_patches=2" in result.output
    assert "allow_multi_patch" in result.output
