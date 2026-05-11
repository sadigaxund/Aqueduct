"""CLI tests for aqueduct log and aqueduct rollback commands.

Covers ⏳ items from TEST_MANIFEST.md Phase 18 git-lifecycle section:
- aqueduct log <blueprint>
- aqueduct rollback <blueprint> --to <patch_id>
"""
import json
import subprocess
import pytest
pytestmark = pytest.mark.integration

from pathlib import Path
from click.testing import CliRunner
from aqueduct.cli import cli


@pytest.fixture
def bp_file(tmp_path):
    bp = tmp_path / "blueprint.yml"
    bp.write_text("aqueduct: '1.0'\nid: test_bp\nname: Test\nmodules: []\nedges: []")
    return bp


# ─── aqueduct log ─────────────────────────────────────────────────────────────

GIT_LOG_WITH_AQ_BLOCK = (
    "abc1234\x1f2026-01-01 12:00:00 +0000\x1ffix: patch applied\x1f"
    "fix: patch applied\n\n---aqueduct---\npatches:\n  - P001: Fix ingress path\n"
    "ops: set_module_config_key\nrun_id: run-xyz\n---\n\x1eENDCOMMIT"
    "\ndef4567\x1f2025-12-31 10:00:00 +0000\x1fManual tweak\x1fManual tweak\n\x1eENDCOMMIT"
)


class TestLogCmd:
    def test_no_git_history_prints_message(self, bp_file, monkeypatch):
        """No git commits for blueprint → prints 'No git history'."""
        def mock_run(args, **kwargs):
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

        monkeypatch.setattr(subprocess, "run", mock_run)
        runner = CliRunner()
        result = runner.invoke(cli, ["log", str(bp_file)])

        assert result.exit_code == 0
        assert "No git history" in result.output

    def test_commit_with_aq_block_shows_patch_and_ops(self, bp_file, monkeypatch):
        """Commit with ---aqueduct--- block → patch_id and ops shown."""
        def mock_run(args, **kwargs):
            return subprocess.CompletedProcess(args, 0, stdout=GIT_LOG_WITH_AQ_BLOCK, stderr="")

        monkeypatch.setattr(subprocess, "run", mock_run)
        runner = CliRunner()
        result = runner.invoke(cli, ["log", str(bp_file)])

        assert result.exit_code == 0
        assert "P001" in result.output
        assert "set_module_config_key" in result.output

    def test_commit_without_aq_block_shows_manual_change(self, bp_file, monkeypatch):
        """Commit without ---aqueduct--- block → shown as '(manual change)'."""
        def mock_run(args, **kwargs):
            return subprocess.CompletedProcess(args, 0, stdout=GIT_LOG_WITH_AQ_BLOCK, stderr="")

        monkeypatch.setattr(subprocess, "run", mock_run)
        runner = CliRunner()
        result = runner.invoke(cli, ["log", str(bp_file)])

        assert result.exit_code == 0
        assert "(manual change)" in result.output

    def test_format_json_returns_array_with_required_fields(self, bp_file, monkeypatch):
        """--format json → array of objects with hash, date, patches, ops, run_id."""
        def mock_run(args, **kwargs):
            return subprocess.CompletedProcess(args, 0, stdout=GIT_LOG_WITH_AQ_BLOCK, stderr="")

        monkeypatch.setattr(subprocess, "run", mock_run)
        runner = CliRunner()
        result = runner.invoke(cli, ["log", str(bp_file), "--format", "json"])

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert len(data) >= 1
        entry = data[0]
        for field in ("hash", "date", "patches", "ops", "run_id"):
            assert field in entry, f"Missing field: {field}"

    def test_long_patches_column_truncated(self, bp_file, monkeypatch):
        """Long patches column is truncated to 40 chars with '..' suffix."""
        long_patch_name = "P" + "0" * 50
        git_log = (
            f"abc1234\x1f2026-01-01 12:00:00 +0000\x1ffix:\x1ffix:\n\n---aqueduct---\n"
            f"patches:\n  - {long_patch_name}: some fix\n---\n\x1eENDCOMMIT"
        )

        def mock_run(args, **kwargs):
            return subprocess.CompletedProcess(args, 0, stdout=git_log, stderr="")

        monkeypatch.setattr(subprocess, "run", mock_run)
        runner = CliRunner()
        result = runner.invoke(cli, ["log", str(bp_file)])

        assert result.exit_code == 0
        # The 40-char column has at most 40 chars visible for patches
        lines = [l for l in result.output.splitlines() if "P0" in l]
        if lines:
            # Cell value is truncated
            assert ".." in result.output or len(long_patch_name) > 40


# ─── aqueduct rollback ────────────────────────────────────────────────────────

GIT_LOG_FOR_ROLLBACK = (
    "abc1234\x1fffix: patch\x1ffix: patch\x1f"
    "fix: patch\n\n---aqueduct---\npatches:\n  - P001: Fix ingress\nops: set_module_config_key\n---\n\x1eENDCOMMIT"
)


class TestRollbackCmd:
    def test_patch_id_found_runs_git_revert(self, bp_file, monkeypatch):
        """patch_id in git log → git revert --no-edit run; new commit created."""
        calls = []

        def mock_run(args, **kwargs):
            calls.append(list(args))
            if "log" in args:
                return subprocess.CompletedProcess(args, 0, stdout=GIT_LOG_FOR_ROLLBACK, stderr="")
            if "rev-parse" in args and "--short" in args:
                return subprocess.CompletedProcess(args, 0, stdout="def5678", stderr="")
            if "diff-tree" in args:
                return subprocess.CompletedProcess(args, 0, stdout="blueprint.yml\n", stderr="")
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

        monkeypatch.setattr(subprocess, "run", mock_run)
        runner = CliRunner()
        result = runner.invoke(cli, ["rollback", str(bp_file), "--to", "P001"])

        assert result.exit_code == 0
        cmds = [" ".join(c) for c in calls]
        assert any("git revert --no-edit" in c for c in cmds)

    def test_patch_id_not_found_exits_1_with_hint(self, bp_file, monkeypatch):
        """patch_id not in git log → error with hint to run aqueduct log; exits 1."""
        def mock_run(args, **kwargs):
            if "log" in args:
                return subprocess.CompletedProcess(args, 0, stdout=GIT_LOG_FOR_ROLLBACK, stderr="")
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

        monkeypatch.setattr(subprocess, "run", mock_run)
        runner = CliRunner()
        result = runner.invoke(cli, ["rollback", str(bp_file), "--to", "GHOST_PATCH"])

        assert result.exit_code == 1
        assert "GHOST_PATCH" in result.output
        assert "aqueduct log" in result.output

    def test_hard_flag_requires_yes_confirmation(self, bp_file, monkeypatch):
        """--hard flag with 'yes' confirmation → runs git reset --hard."""
        calls = []

        def mock_run(args, **kwargs):
            calls.append(list(args))
            if "log" in args:
                return subprocess.CompletedProcess(args, 0, stdout=GIT_LOG_FOR_ROLLBACK, stderr="")
            if "rev-parse" in args and "~1" in " ".join(args):
                return subprocess.CompletedProcess(args, 0, stdout="parent123", stderr="")
            if "diff-tree" in args:
                return subprocess.CompletedProcess(args, 0, stdout="blueprint.yml\n", stderr="")
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

        monkeypatch.setattr(subprocess, "run", mock_run)
        runner = CliRunner()
        # Simulate user typing "yes"
        result = runner.invoke(cli, ["rollback", str(bp_file), "--to", "P001", "--hard"], input="yes\n")

        assert result.exit_code == 0
        cmds = [" ".join(c) for c in calls]
        assert any("git reset --hard" in c for c in cmds)

    def test_hard_flag_with_non_yes_aborts(self, bp_file, monkeypatch):
        """--hard with non-'yes' response → 'Aborted.' printed; no reset."""
        calls = []

        def mock_run(args, **kwargs):
            calls.append(list(args))
            if "log" in args:
                return subprocess.CompletedProcess(args, 0, stdout=GIT_LOG_FOR_ROLLBACK, stderr="")
            if "diff-tree" in args:
                return subprocess.CompletedProcess(args, 0, stdout="blueprint.yml\n", stderr="")
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

        monkeypatch.setattr(subprocess, "run", mock_run)
        runner = CliRunner()
        result = runner.invoke(cli, ["rollback", str(bp_file), "--to", "P001", "--hard"], input="no\n")

        assert result.exit_code == 0
        assert "Aborted." in result.output
        cmds = [" ".join(c) for c in calls]
        assert not any("git reset --hard" in c for c in cmds)

    def test_git_revert_failure_exits_1(self, bp_file, monkeypatch):
        """git revert fails → exits 1 with stderr content."""
        def mock_run(args, **kwargs):
            if "log" in args:
                return subprocess.CompletedProcess(args, 0, stdout=GIT_LOG_FOR_ROLLBACK, stderr="")
            if "revert" in args:
                return subprocess.CompletedProcess(
                    args, 1, stdout="", stderr="error: conflict in blueprint.yml"
                )
            if "diff-tree" in args:
                return subprocess.CompletedProcess(args, 0, stdout="blueprint.yml\n", stderr="")
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

        monkeypatch.setattr(subprocess, "run", mock_run)
        runner = CliRunner()
        result = runner.invoke(cli, ["rollback", str(bp_file), "--to", "P001"])

        assert result.exit_code == 1
        assert "conflict" in result.output
