"""Phase 54 — CI kit: payload schema, commit-message builder, `patch import`.

Pure/local only (no Spark, no live GitHub). The `patch import` test drives a
real temp git repo via subprocess.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

pytestmark = pytest.mark.unit

from aqueduct.cli import cli
from aqueduct.patch.ci import (
    CI_WEBHOOK_REQUIRED_KEYS,
    build_commit_message,
    validate_ci_payload,
)


# ── validate_ci_payload ──────────────────────────────────────────────────────

def _good_payload() -> dict:
    return {
        "patch_id": "00001_fix-path",
        "run_id": "run-123",
        "blueprint_id": "demo.pipeline",
        "failed_module": "load_orders",
        "source": "llm",
    }


def test_validate_ci_payload_accepts_complete_envelope():
    assert validate_ci_payload(_good_payload()) == []


def test_validate_ci_payload_allows_null_failed_module():
    payload = _good_payload()
    payload["failed_module"] = None  # present-but-null is valid
    assert validate_ci_payload(payload) == []


def test_validate_ci_payload_reports_each_missing_key():
    violations = validate_ci_payload({"patch_id": "p1"})
    missing = {k for k in CI_WEBHOOK_REQUIRED_KEYS if k != "patch_id"}
    for key in missing:
        assert any(key in v for v in violations), f"{key} not flagged"


def test_validate_ci_payload_rejects_empty_patch_id():
    payload = _good_payload()
    payload["patch_id"] = ""
    violations = validate_ci_payload(payload)
    assert any("patch_id" in v for v in violations)


def test_validate_ci_payload_rejects_non_dict():
    assert validate_ci_payload(["not", "a", "dict"])


# ── build_commit_message ─────────────────────────────────────────────────────

def test_build_commit_message_single_patch_subject_is_rationale():
    msg = build_commit_message(
        "demo.pipeline",
        [{"patch_id": "p1", "rationale": "widen amount to double",
          "operations": [{"op": "replace_module_config"}]}],
    )
    assert msg.startswith("fix(aqueduct/demo.pipeline): widen amount to double")
    assert "---aqueduct---" in msg
    assert "  - p1: widen amount to double" in msg
    assert "ops: replace_module_config" in msg
    assert msg.rstrip().endswith("---")


def test_build_commit_message_multi_patch_summarises_count():
    msg = build_commit_message(
        "bp",
        [
            {"patch_id": "p1", "rationale": "a", "operations": [{"op": "x"}]},
            {"patch_id": "p2", "rationale": "b", "operations": [{"op": "y"}]},
        ],
    )
    assert "fix(aqueduct/bp): 2 patches applied" in msg
    assert "  - p1: a" in msg and "  - p2: b" in msg


def test_build_commit_message_dedups_ops_and_carries_run_id():
    msg = build_commit_message(
        "bp",
        [{"patch_id": "p1", "rationale": "r",
          "operations": [{"op": "set"}, {"op": "set"}, {"op": "add"}],
          "_aq_meta": {"run_id": "run-9"}}],
    )
    assert "ops: set, add" in msg          # deduplicated, order-preserving
    assert "run_id: run-9" in msg


def test_build_commit_message_missing_rationale_falls_back():
    msg = build_commit_message("bp", [{"patch_id": "p1", "operations": []}])
    assert "  - p1: (no rationale)" in msg


# ── patch import (apply + commit on a real git repo) ─────────────────────────

def _git(repo: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True, check=True)


@pytest.fixture
def git_repo_with_blueprint(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test")
    bp = {
        "aqueduct": "1.0",
        "id": "test.bp",
        "name": "Test Blueprint",
        "modules": [
            {"id": "in", "type": "Ingress", "config": {"format": "parquet", "path": "p1"}}
        ],
        "edges": [],
    }
    bp_path = repo / "blueprint.yml"
    bp_path.write_text(yaml.dump(bp), encoding="utf-8")
    _git(repo, "add", "blueprint.yml")
    _git(repo, "commit", "-q", "-m", "initial")
    return repo, bp_path


def _patch_file(repo: Path) -> Path:
    patch_path = repo / "received-patch.json"
    patch_path.write_text(json.dumps({
        "patch_id": "00007_new-label",
        "rationale": "relabel ingress",
        "operations": [{"op": "replace_module_label", "module_id": "in", "label": "Renamed"}],
    }), encoding="utf-8")
    return patch_path


def test_patch_import_applies_and_commits(git_repo_with_blueprint):
    repo, bp_path = git_repo_with_blueprint
    patch_path = _patch_file(repo)

    result = CliRunner().invoke(
        cli, ["patch", "import", str(patch_path), "--blueprint", str(bp_path)]
    )
    assert result.exit_code == 0, result.output

    # Blueprint mutated.
    updated = yaml.safe_load(bp_path.read_text())
    assert updated["modules"][0]["label"] == "Renamed"

    # A new commit landed with the structured trailer.
    log = subprocess.run(
        ["git", "log", "-1", "--format=%B"], cwd=repo, capture_output=True, text=True
    ).stdout
    assert "fix(aqueduct/test.bp): relabel ingress" in log
    assert "---aqueduct---" in log
    assert "00007_new-label" in log

    # Working tree is clean — the change was committed, not left staged.
    status = subprocess.run(
        ["git", "status", "--porcelain"], cwd=repo, capture_output=True, text=True
    ).stdout.strip()
    assert "blueprint.yml" not in status


def test_patch_import_no_commit_stages_only(git_repo_with_blueprint):
    repo, bp_path = git_repo_with_blueprint
    patch_path = _patch_file(repo)
    head_before = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True
    ).stdout.strip()

    result = CliRunner().invoke(
        cli, ["patch", "import", str(patch_path), "--blueprint", str(bp_path), "--no-commit"]
    )
    assert result.exit_code == 0, result.output

    updated = yaml.safe_load(bp_path.read_text())
    assert updated["modules"][0]["label"] == "Renamed"

    # No new commit — HEAD unchanged.
    head_after = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True
    ).stdout.strip()
    assert head_after == head_before
