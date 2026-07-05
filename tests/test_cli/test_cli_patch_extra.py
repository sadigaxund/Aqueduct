"""Additional CLI tests for patch discard, patch list, and _patches_root_from_blueprint.

Covers ⏳ items from TEST_MANIFEST.md Phase 18 git-lifecycle section.
"""
import json
import subprocess
import pytest

pytestmark = pytest.mark.integration

from pathlib import Path
from click.testing import CliRunner
from aqueduct.cli import cli


# ── shared fixture ─────────────────────────────────────────────────────────────

@pytest.fixture
def setup(tmp_path):
    """Minimal project with one pending and one applied patch."""
    project = tmp_path / "project"
    project.mkdir()

    bp_path = project / "blueprint.yml"
    bp_path.write_text("""\
aqueduct: '1.0'
id: test_bp
name: Test
modules:
  - id: src
    type: Ingress
    label: Source
    config: {path: data.csv}
edges: []
""")
    (project / "aqueduct.yml").write_text(f"""
stores:
  depots: {{default: {{path: "{project}/depot.db"}}}}
  obs: {{path: "{project}/obs.db"}}
""")

    patches_dir = project / "patches"
    for sub in ("pending", "applied", "rejected"):
        (patches_dir / sub).mkdir(parents=True)

    pending_patch = {
        "patch_id": "P001",
        "rationale": "Fix the source path to use new location",
        "operations": [{"op": "set_module_config_key", "module_id": "src", "key": "path", "value": "new.csv"}],
    }
    patch_file = patches_dir / "pending" / "P001.json"
    patch_file.write_text(json.dumps(pending_patch))

    return project, bp_path, patches_dir, patch_file


# ── patch discard tests ────────────────────────────────────────────────────────

class TestPatchDiscard:
    def test_no_uncommitted_patches_git_checkout_still_runs(self, setup, monkeypatch):
        """No applied patches → git checkout runs, 0 patches moved."""
        project, bp_path, patches_dir, patch_file = setup
        runner = CliRunner()

        mock_calls = []
        def mock_run(args, **kwargs):
            mock_calls.append(list(args) if isinstance(args, list) else args)
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

        monkeypatch.setattr(subprocess, "run", mock_run)
        result = runner.invoke(cli, ["patch", "discard", "--blueprint", str(bp_path)])

        assert result.exit_code == 0
        assert "✓ blueprint restored" in result.output
        cmds = [" ".join(c) if isinstance(c, list) else c for c in mock_calls]
        assert any("git checkout HEAD" in c for c in cmds)
        # No "moved" message since nothing to move
        assert "moved" not in result.output

    def test_git_checkout_failure_exits_1(self, setup, monkeypatch):
        """git checkout failing → exit code 1 with error message."""
        project, bp_path, patches_dir, patch_file = setup
        runner = CliRunner()

        def mock_run(args, **kwargs):
            if isinstance(args, list) and "checkout" in args:
                return subprocess.CompletedProcess(
                    args, 1, stdout="", stderr="error: pathspec did not match any file(s)"
                )
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

        monkeypatch.setattr(subprocess, "run", mock_run)
        result = runner.invoke(cli, ["patch", "discard", "--blueprint", str(bp_path)])

        from aqueduct.exit_codes import DATA_OR_RUNTIME
        assert result.exit_code == DATA_OR_RUNTIME

    def test_patches_moved_count_printed(self, setup, monkeypatch):
        """After discard with applied patches → 'moved N applied patch(es) back' in output."""
        project, bp_path, patches_dir, patch_file = setup
        runner = CliRunner()

        # Apply the patch first
        apply_result = runner.invoke(
            cli, ["patch", "apply", str(patch_file), "--blueprint", str(bp_path)]
        )
        assert apply_result.exit_code == 0, apply_result.output

        def mock_run(args, **kwargs):
            # Make git log return non-zero so all patches count as uncommitted
            if isinstance(args, list) and "log" in args:
                return subprocess.CompletedProcess(args, 1, stdout="", stderr="")
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

        monkeypatch.setattr(subprocess, "run", mock_run)
        result = runner.invoke(cli, ["patch", "discard", "--blueprint", str(bp_path)])

        assert result.exit_code == 0
        assert "moved" in result.output
        assert "1" in result.output


# ── patch list tests ───────────────────────────────────────────────────────────

class TestPatchList:
    def test_pending_patches_tabular_output(self, setup):
        """Pending patches → tabular output with file, patch_id, rationale."""
        project, bp_path, patches_dir, patch_file = setup
        runner = CliRunner()

        result = runner.invoke(cli, ["patch", "list", "--blueprint", str(bp_path)])
        assert result.exit_code == 0
        assert "P001.json" in result.output
        assert "P001" in result.output
        assert "Fix the source path" in result.output

    def test_long_filename_shown_in_full(self, setup):
        """The `file` column is the unique key copied into apply/reject — it must
        appear in FULL, not truncated (regression: a 30-char cut produced an
        unusable key that matched nothing on reject)."""
        project, bp_path, patches_dir, patch_file = setup
        long_name = "20260628T080624_fix-stock-query-join.json"
        (patches_dir / "pending" / long_name).write_text(
            json.dumps({"patch_id": "fix-stock-query-join", "rationale": "r"})
        )
        result = CliRunner().invoke(cli, ["patch", "list", "--blueprint", str(bp_path)])
        assert result.exit_code == 0
        assert long_name in result.output  # full filename, untruncated

    def test_no_pending_patches_message(self, setup):
        """No pending patches → message about none found."""
        project, bp_path, patches_dir, patch_file = setup
        runner = CliRunner()

        # Remove the pending patch
        patch_file.unlink()

        result = runner.invoke(cli, ["patch", "list", "--blueprint", str(bp_path)])
        assert result.exit_code == 0
        assert "No pending patches found" in result.output

    def test_status_applied(self, setup):
        """--status=applied → lists applied/ dir."""
        project, bp_path, patches_dir, patch_file = setup
        runner = CliRunner()

        # Write a fake applied patch
        applied_data = {"patch_id": "P000", "rationale": "Old fix"}
        (patches_dir / "applied" / "P000.json").write_text(json.dumps(applied_data))

        result = runner.invoke(
            cli, ["patch", "list", "--blueprint", str(bp_path), "--status", "applied"]
        )
        assert result.exit_code == 0
        assert "P000" in result.output

    def test_status_all_shows_all_sections(self, setup):
        """--status=all → lists pending/, applied/, rejected/ sections."""
        project, bp_path, patches_dir, patch_file = setup
        runner = CliRunner()

        (patches_dir / "applied" / "PA.json").write_text(json.dumps({"patch_id": "PA", "rationale": "Applied"}))
        (patches_dir / "rejected" / "PR.json").write_text(json.dumps({"patch_id": "PR", "rationale": "Rejected"}))

        result = runner.invoke(
            cli, ["patch", "list", "--blueprint", str(bp_path), "--status", "all"]
        )
        assert result.exit_code == 0
        assert "pending" in result.output
        assert "applied" in result.output
        assert "rejected" in result.output
        assert "P001" in result.output
        assert "PA" in result.output
        assert "PR" in result.output

    def test_rationale_truncated_to_60_chars(self, setup):
        """Rationale longer than 60 chars is truncated in output."""
        project, bp_path, patches_dir, patch_file = setup
        runner = CliRunner()

        long_rationale = "A" * 80
        patch_file.write_text(json.dumps({"patch_id": "P001", "rationale": long_rationale}))

        result = runner.invoke(cli, ["patch", "list", "--blueprint", str(bp_path)])
        assert result.exit_code == 0
        # Only 60 chars visible, the extra 20 should be cut
        assert "A" * 60 in result.output
        assert "A" * 80 not in result.output

    def test_apply_reject_hints_after_pending_table(self, setup):
        """Hint lines are printed after the pending table."""
        project, bp_path, patches_dir, patch_file = setup
        runner = CliRunner()

        result = runner.invoke(cli, ["patch", "list", "--blueprint", str(bp_path)])
        assert result.exit_code == 0
        assert "Apply:" in result.output or "aqueduct patch apply" in result.output
        assert "Reject:" in result.output or "aqueduct patch reject" in result.output

    def test_patch_list_json_format(self, setup):
        """JSON output mode lists all fields including meta. Missing meta defaults to null."""
        project, bp_path, patches_dir, patch_file = setup
        runner = CliRunner()

        # 1. First patch (from setup) has no _aq_meta
        # 2. Write a second patch that has _aq_meta
        meta_patch = {
            "patch_id": "P002",
            "rationale": "Meta patch",
            "operations": [],
            "_aq_meta": {
                "run_id": "run-xyz",
                "blueprint_id": "test_bp",
                "failed_module": "src",
            }
        }
        (patches_dir / "pending" / "P002.json").write_text(json.dumps(meta_patch))

        result = runner.invoke(cli, ["patch", "list", "--blueprint", str(bp_path), "--format", "json"])
        assert result.exit_code == 0

        parsed = json.loads(result.output)
        assert len(parsed) == 2

        # P001 has no meta, run_id etc should be null
        p1 = next(p for p in parsed if p["patch_id"] == "P001")
        assert p1["run_id"] is None
        assert p1["blueprint_id"] is None
        assert p1["failed_module"] is None
        assert p1["status"] == "pending"

        # P002 has meta
        p2 = next(p for p in parsed if p["patch_id"] == "P002")
        assert p2["run_id"] == "run-xyz"
        assert p2["blueprint_id"] == "test_bp"
        assert p2["failed_module"] == "src"
        assert p2["status"] == "pending"


# ── _patches_root_from_blueprint unit tests ────────────────────────────────────

class TestPatchesRootFromBlueprint:
    def test_finds_aqueduct_yml_in_parent(self, tmp_path):
        """blueprint in blueprints/ subdir → aqueduct.yml at project root → patches there."""
        from aqueduct.cli import _patches_root_from_blueprint

        project = tmp_path / "myproject"
        (project / "blueprints").mkdir(parents=True)
        bp = project / "blueprints" / "etl.yml"
        bp.write_text("id: test")
        (project / "aqueduct.yml").write_text("stores: {}")

        result = _patches_root_from_blueprint(bp)
        assert result == project / "patches"

    def test_fallback_to_blueprint_parent_when_no_aqueduct_yml(self, tmp_path):
        """No aqueduct.yml found → returns <blueprint_parent>/patches."""
        from aqueduct.cli import _patches_root_from_blueprint

        deep = tmp_path / "a" / "b" / "c" / "d" / "e" / "f" / "g" / "h" / "i"
        deep.mkdir(parents=True)
        bp = deep / "bp.yml"
        bp.write_text("id: test")

        result = _patches_root_from_blueprint(bp)
        assert result == deep / "patches"
