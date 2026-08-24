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
    HEAL_BRANCH_PREFIX,
    build_commit_message,
    heal_branch_name,
    render_pr_body,
    render_pr_title,
    resolve_repo_root_conflict,
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
        [
            {
                "patch_id": "p1",
                "rationale": "widen amount to double",
                "operations": [{"op": "replace_module_config"}],
            }
        ],
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
        [
            {
                "patch_id": "p1",
                "rationale": "r",
                "operations": [{"op": "set"}, {"op": "set"}, {"op": "add"}],
                "_aq_meta": {"run_id": "run-9"},
            }
        ],
    )
    assert "ops: set, add" in msg  # deduplicated, order-preserving
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
        "modules": [{"id": "in", "type": "Ingress", "config": {"format": "parquet", "path": "p1"}}],
        "edges": [],
    }
    bp_path = repo / "blueprint.yml"
    bp_path.write_text(yaml.dump(bp), encoding="utf-8")
    _git(repo, "add", "blueprint.yml")
    _git(repo, "commit", "-q", "-m", "initial")
    return repo, bp_path


def _patch_file(repo: Path) -> Path:
    patch_path = repo / "received-patch.json"
    patch_path.write_text(
        json.dumps(
            {
                "patch_id": "00007_new-label",
                "rationale": "relabel ingress",
                "operations": [
                    {"op": "replace_module_label", "module_id": "in", "label": "Renamed"}
                ],
            }
        ),
        encoding="utf-8",
    )
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


def test_patch_import_accepts_ci_webhook_envelope(git_repo_with_blueprint):
    """`patch import` unwraps a CI webhook envelope ({...envelope, patch: {...}})
    after validating it, not just a bare PatchSpec."""
    repo, bp_path = git_repo_with_blueprint
    envelope = repo / "envelope.json"
    envelope.write_text(
        json.dumps(
            {
                "patch_id": "00007_new-label",
                "run_id": "run-1",
                "blueprint_id": "test.bp",
                "failed_module": "in",
                "source": "llm",
                "patch": {
                    "patch_id": "00007_new-label",
                    "rationale": "relabel via envelope",
                    "operations": [
                        {"op": "replace_module_label", "module_id": "in", "label": "Renamed"}
                    ],
                },
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        cli, ["patch", "import", str(envelope), "--blueprint", str(bp_path)]
    )
    assert result.exit_code == 0, result.output
    assert yaml.safe_load(bp_path.read_text())["modules"][0]["label"] == "Renamed"
    log = subprocess.run(
        ["git", "log", "-1", "--format=%B"], cwd=repo, capture_output=True, text=True
    ).stdout
    assert "relabel via envelope" in log


def test_patch_import_rejects_invalid_envelope(git_repo_with_blueprint):
    repo, bp_path = git_repo_with_blueprint
    bad = repo / "bad.json"
    # has a `patch` key (→ treated as envelope) but is missing required keys
    bad.write_text(
        json.dumps(
            {
                "patch": {"patch_id": "p", "rationale": "r", "operations": []},
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(cli, ["patch", "import", str(bad), "--blueprint", str(bp_path)])
    assert result.exit_code != 0
    assert "invalid CI webhook payload" in result.output


def test_patch_import_outside_git_repo_fails_before_mutating(tmp_path):
    """Without --no-commit, a non-repo checkout fails BEFORE the Blueprint is
    touched (no applied-but-uncommittable state)."""
    bp = tmp_path / "blueprint.yml"
    bp.write_text(
        yaml.dump(
            {
                "aqueduct": "1.0",
                "id": "test.bp",
                "name": "T",
                "modules": [
                    {
                        "id": "in",
                        "type": "Ingress",
                        "label": "In",
                        "config": {"format": "parquet", "path": "p1"},
                    }
                ],
                "edges": [],
            }
        ),
        encoding="utf-8",
    )
    before = bp.read_text()
    patch = tmp_path / "p.json"
    patch.write_text(
        json.dumps(
            {
                "patch_id": "p",
                "rationale": "r",
                "operations": [{"op": "replace_module_label", "module_id": "in", "label": "X"}],
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(cli, ["patch", "import", str(patch), "--blueprint", str(bp)])
    assert result.exit_code != 0
    assert "not inside a git work tree" in result.output
    assert bp.read_text() == before  # Blueprint untouched


# ── heal_branch_name (Phase 87) ──────────────────────────────────────────────


def test_heal_branch_name_uses_the_owned_prefix():
    assert heal_branch_name("00001_fix-path") == f"{HEAL_BRANCH_PREFIX}/00001_fix-path"


def test_heal_branch_name_prefix_is_aqueduct_heal():
    assert HEAL_BRANCH_PREFIX == "aqueduct/heal"


# ── resolve_repo_root_conflict (Phase 87 — item 4 pre-flight guard) ─────────


def test_repo_root_conflict_matching_roots_returns_none():
    assert resolve_repo_root_conflict("/repo", "/repo", None) is None


def test_repo_root_conflict_parent_git_mismatch_refuses():
    """toplevel resolves ABOVE the project root (parent .git footgun)."""
    msg = resolve_repo_root_conflict("/repo", "/repo/sub/project", None)
    assert msg is not None
    assert "/repo" in msg and "project" in msg
    assert "git.expected_root" in msg


def test_repo_root_conflict_child_git_mismatch_refuses():
    """toplevel resolves to a stale child .git (not an ancestor-or-equal of
    the project root)."""
    msg = resolve_repo_root_conflict("/repo/project/vendored", "/repo/project", None)
    assert msg is not None


def test_repo_root_conflict_pin_satisfied_allows_monorepo():
    """An explicit git.expected_root pin overrides the project-root default —
    the ratified monorepo escape hatch."""
    assert resolve_repo_root_conflict("/repo", "/repo/sub/project", "/repo") is None


def test_repo_root_conflict_pin_mismatch_refuses_and_names_the_pin():
    msg = resolve_repo_root_conflict("/repo", "/repo/sub/project", "/somewhere/else")
    assert msg is not None
    assert "git.expected_root" in msg
    assert "/somewhere/else" in msg


# ── render_pr_title / render_pr_body (Phase 87) ─────────────────────────────


def test_render_pr_title_fills_all_tokens():
    title = render_pr_title(
        "heal {blueprint_id}/{module}: {patch_id}",
        patch_id="p1",
        blueprint_id="demo.pipeline",
        module="load_orders",
    )
    assert title == "heal demo.pipeline/load_orders: p1"


def test_render_pr_title_falls_back_to_unknown_module():
    title = render_pr_title(
        "{blueprint_id}/{module}/{patch_id}", patch_id="p1", blueprint_id="bp", module=None
    )
    assert "/unknown/" in title


def test_render_pr_body_carries_rationale_root_cause_and_ops():
    body = render_pr_body(
        {
            "patch_id": "p1",
            "rationale": "widen amount to double",
            "root_cause": "schema drift",
            "confidence": 0.9,
            "category": "schema_drift",
            "operations": [{"op": "replace_module_config"}, {"op": "replace_module_config"}],
        },
        "fix(aqueduct/bp): widen amount to double",
    )
    assert "widen amount to double" in body
    assert "schema drift" in body
    assert "confidence=0.9" in body
    assert "category=schema_drift" in body
    assert "replace_module_config" in body
    assert "fix(aqueduct/bp): widen amount to double" in body  # trailer embedded verbatim


def test_render_pr_body_handles_missing_optional_fields():
    body = render_pr_body({"patch_id": "p1", "operations": []}, "fix(aqueduct/bp): x")
    assert "(no rationale)" in body
