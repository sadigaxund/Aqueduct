"""Phase 87 — `aqueduct patch pr` (heal-as-PR).

Drives a real temp git repo (same pattern as `tests/test_patch/test_ci.py`'s
`patch import` tests) so branch/commit/push mechanics are exercised for real;
`gh` is stubbed via a fake executable on PATH for the happy path, and via
monkeypatched `subprocess.run` for failure-injection (push failure, `gh`
failure) so no network call is ever attempted.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

pytestmark = pytest.mark.unit

from aqueduct.cli import cli  # noqa: E402

_FAKE_GH = """#!/bin/sh
if [ "$1" = "pr" ] && [ "$2" = "create" ]; then
  echo "https://github.com/example/repo/pull/1"
  exit 0
fi
exit 0
"""


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True, check=True)


def _bp_dict() -> dict:
    return {
        "aqueduct": "1.0",
        "id": "test.bp",
        "name": "Test Blueprint",
        "modules": [
            {
                "id": "in",
                "type": "Ingress",
                "label": "In",
                "config": {"format": "csv", "path": "p1"},
            }
        ],
        "edges": [],
    }


@pytest.fixture
def repo_with_remote(tmp_path):
    """A project repo with a bare `origin` remote, so `git push` succeeds for
    real in the happy-path test."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test")

    (repo / "aqueduct.yml").write_text('aqueduct_config: "2.0"\n', encoding="utf-8")
    bp_path = repo / "blueprint.yml"
    bp_path.write_text(yaml.dump(_bp_dict()), encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "initial")

    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "-q", "--bare", str(remote)], check=True)
    _git(repo, "remote", "add", "origin", str(remote))

    return repo, bp_path


def _patch_file(repo: Path, patch_id: str = "p1") -> Path:
    patch_path = repo / f"{patch_id}.json"
    patch_path.write_text(
        json.dumps(
            {
                "patch_id": patch_id,
                "rationale": "fix format",
                "operations": [
                    {
                        "op": "set_module_config_key",
                        "module_id": "in",
                        "key": "format",
                        "value": "parquet",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return patch_path


def _install_fake_gh(tmp_path, monkeypatch) -> None:
    bindir = tmp_path / "fakebin"
    bindir.mkdir(exist_ok=True)
    gh = bindir / "gh"
    gh.write_text(_FAKE_GH, encoding="utf-8")
    gh.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bindir}{os.pathsep}{os.environ.get('PATH', '')}")


# ── happy path ────────────────────────────────────────────────────────────


def test_patch_pr_happy_path_opens_pr(repo_with_remote, tmp_path, monkeypatch):
    repo, bp_path = repo_with_remote
    patch_path = _patch_file(repo)
    _install_fake_gh(tmp_path, monkeypatch)
    monkeypatch.chdir(repo)

    result = CliRunner().invoke(cli, ["patch", "pr", str(patch_path), "--blueprint", str(bp_path)])
    assert result.exit_code == 0, result.output
    assert "https://github.com/example/repo/pull/1" in result.output

    # A branch was created, committed to, and pushed.
    branches = _git(repo, "branch", "-a").stdout
    assert "aqueduct/heal/p1" in branches
    remote_refs = subprocess.run(
        ["git", "ls-remote", str(repo.parent / "remote.git")],
        capture_output=True,
        text=True,
    ).stdout
    assert "refs/heads/aqueduct/heal/p1" in remote_refs

    log = subprocess.run(
        ["git", "log", "-1", "--format=%B", "aqueduct/heal/p1"],
        cwd=repo,
        capture_output=True,
        text=True,
    ).stdout
    assert "---aqueduct---" in log


# ── --dry-run ─────────────────────────────────────────────────────────────


def test_patch_pr_dry_run_touches_nothing(repo_with_remote, tmp_path, monkeypatch):
    repo, bp_path = repo_with_remote
    patch_path = _patch_file(repo)
    _install_fake_gh(tmp_path, monkeypatch)

    monkeypatch.chdir(repo)
    before_branch = _git(repo, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()

    result = CliRunner().invoke(
        cli, ["patch", "pr", str(patch_path), "--blueprint", str(bp_path), "--dry-run"]
    )
    assert result.exit_code == 0, result.output
    assert "would create branch" in result.output
    assert "aqueduct/heal/p1" in result.output

    after_branch = _git(repo, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    assert after_branch == before_branch
    branches = _git(repo, "branch", "-a").stdout
    assert "aqueduct/heal/p1" not in branches


# ── refusal paths ─────────────────────────────────────────────────────────


def test_patch_pr_outside_git_repo_refuses(tmp_path):
    bp_path = tmp_path / "blueprint.yml"
    bp_path.write_text(yaml.dump(_bp_dict()), encoding="utf-8")
    (tmp_path / "aqueduct.yml").write_text('aqueduct_config: "2.0"\n', encoding="utf-8")
    patch_path = _patch_file(tmp_path)

    result = CliRunner().invoke(cli, ["patch", "pr", str(patch_path), "--blueprint", str(bp_path)])
    assert result.exit_code != 0
    assert "not inside a git work tree" in result.output


def test_patch_pr_repo_root_mismatch_refuses(tmp_path):
    """Project nested inside a larger repo, no `git.expected_root` pin → hard
    refuse (2026-08-24 design audit, item 4)."""
    outer = tmp_path / "outer"
    outer.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=outer, check=True)
    subprocess.run(["git", "config", "user.email", "a@b.com"], cwd=outer, check=True)
    subprocess.run(["git", "config", "user.name", "T"], cwd=outer, check=True)
    (outer / "root.txt").write_text("x", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=outer, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "root"], cwd=outer, check=True)

    project = outer / "sub" / "project"
    project.mkdir(parents=True)
    (project / "aqueduct.yml").write_text('aqueduct_config: "2.0"\n', encoding="utf-8")
    bp_path = project / "blueprint.yml"
    bp_path.write_text(yaml.dump(_bp_dict()), encoding="utf-8")
    patch_path = _patch_file(project)

    result = CliRunner().invoke(cli, ["patch", "pr", str(patch_path), "--blueprint", str(bp_path)])
    assert result.exit_code != 0
    assert "does not match the project root" in result.output
    assert "git.expected_root" in result.output
    # Nothing written: still on the outer repo's original branch, no heal branch.
    branches = subprocess.run(
        ["git", "branch", "-a"], cwd=outer, capture_output=True, text=True
    ).stdout
    assert "aqueduct/heal" not in branches


def test_patch_pr_expected_root_pin_allows_monorepo(tmp_path, monkeypatch):
    """The ratified escape hatch: pinning `git.expected_root` to the actual
    toplevel makes the nested-project case explicit instead of refusing it."""
    outer = tmp_path / "outer"
    outer.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=outer, check=True)
    subprocess.run(["git", "config", "user.email", "a@b.com"], cwd=outer, check=True)
    subprocess.run(["git", "config", "user.name", "T"], cwd=outer, check=True)

    project = outer / "sub" / "project"
    project.mkdir(parents=True)
    (project / "aqueduct.yml").write_text(
        f'aqueduct_config: "2.0"\ngit:\n  expected_root: "{outer}"\n', encoding="utf-8"
    )
    bp_path = project / "blueprint.yml"
    bp_path.write_text(yaml.dump(_bp_dict()), encoding="utf-8")
    (outer / "root.txt").write_text("x", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=outer, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "root"], cwd=outer, check=True)
    patch_path = _patch_file(project)
    _install_fake_gh(tmp_path, monkeypatch)
    monkeypatch.chdir(project)

    result = CliRunner().invoke(
        cli, ["patch", "pr", str(patch_path), "--blueprint", str(bp_path), "--dry-run"]
    )
    assert result.exit_code == 0, result.output
    assert "would create branch" in result.output


# ── push failure reporting ───────────────────────────────────────────────


def test_patch_pr_push_failure_reports_and_restores_branch(repo_with_remote, monkeypatch):
    """`git push` fails (e.g. no network) → loud error naming the recovery
    command, and the ONE allowed auto-rollback (switch back to the original
    branch) runs — the commit on the heal branch is never discarded."""
    repo, bp_path = repo_with_remote
    patch_path = _patch_file(repo)
    original_branch = _git(repo, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()

    real_run = subprocess.run

    def fake_run(args, *a, **kw):
        if args[:2] == ["git", "push"]:
            return subprocess.CompletedProcess(
                args, 1, stdout="", stderr="fatal: unable to access remote"
            )
        return real_run(args, *a, **kw)

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.chdir(repo)

    result = CliRunner().invoke(cli, ["patch", "pr", str(patch_path), "--blueprint", str(bp_path)])
    assert result.exit_code != 0
    assert "git push failed" in result.output
    assert "aqueduct/heal/p1" in result.output

    monkeypatch.undo()
    # The heal branch still exists with its commit (never deleted/reset)...
    branches = _git(repo, "branch", "-a").stdout
    assert "aqueduct/heal/p1" in branches
    # ...but HEAD was switched back to the original branch (the one allowed
    # auto-rollback).
    current = _git(repo, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    assert current == original_branch
