"""Phase 73 — blueprint_history / patch_show / git_blueprint_commits.

Backend-agnostic query tests (duckdb fixture) + a real-git-repo test for the
commit-classification helper. No textual/pyspark.
"""

from __future__ import annotations

import subprocess
from types import SimpleNamespace

import duckdb
import pytest

from aqueduct.stores import queries as q

pytestmark = pytest.mark.unit


def _cfg(path=None, backend="duckdb"):
    return SimpleNamespace(
        stores=SimpleNamespace(observability=SimpleNamespace(path=path, backend=backend))
    )


def _seed_history_store(path):
    """A patch_index + healing_outcomes + heal_attempts fixture for one blueprint."""
    c = duckdb.connect(str(path))
    from aqueduct.patch.index import PATCH_INDEX_DDL
    from aqueduct.surveyor.ddl import _DDL, _HEAL_ATTEMPTS_DDL

    c.execute(_DDL)
    c.execute(_HEAL_ATTEMPTS_DDL)
    c.execute(PATCH_INDEX_DDL)
    c.execute(
        "INSERT INTO run_records VALUES "
        "('run1','bp1','error','2026-01-01T00:00:00','2026-01-01T00:01:00', '[]', NULL)"
    )
    c.execute(
        "INSERT INTO heal_attempts (id, run_id, attempt_num, recorded_at) "
        "VALUES ('a1','run1',1,'2026-01-01T00:00:30')"
    )
    c.execute(
        "INSERT INTO patch_index (patch_id, blueprint_id, run_id, status, object_key, "
        "rationale, ops, source, prompt_version, created_at, updated_at) VALUES "
        "('p1','bp1','run1','applied','k1','fix null check','[]','llm','v1',"
        "'2026-01-01T00:02:00','2026-01-01T00:02:00')"
    )
    c.execute(
        "INSERT INTO healing_outcomes (id, run_id, patch_id, confidence, patch_applied, "
        "run_success_after_patch, applied_at) VALUES "
        "('h1','run1','p1',0.9,true,true,'2026-01-01T00:03:00')"
    )
    c.close()


def test_blueprint_history_orders_events_chronologically(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    _seed_history_store(store_dir / "observability.db")
    events = q.blueprint_history(_cfg(), "bp1", store_dir=str(store_dir))
    types = [e.event_type for e in events]
    assert types == ["heal_run_started", "patch_apply", "outcome"]
    assert events[1].patch_id == "p1"
    assert events[1].confidence == 0.9
    assert events[2].description == "run succeeded after patch"


def test_blueprint_history_empty_when_no_activity(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    _seed_history_store(store_dir / "observability.db")
    assert q.blueprint_history(_cfg(), "nonexistent", store_dir=str(store_dir)) == []


def test_patch_show_returns_index_row(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    _seed_history_store(store_dir / "observability.db")
    row = q.patch_show(_cfg(), "p1", store_dir=str(store_dir))
    assert row is not None
    assert row["blueprint_id"] == "bp1"
    assert row["status"] == "applied"


def test_patch_show_missing_returns_none(tmp_path):
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    _seed_history_store(store_dir / "observability.db")
    assert q.patch_show(_cfg(), "does-not-exist", store_dir=str(store_dir)) is None


def _git(args, cwd):
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)


def test_git_blueprint_commits_classifies_manual_vs_patch(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(["init", "-q"], repo)
    _git(["config", "user.email", "t@example.com"], repo)
    _git(["config", "user.name", "Test"], repo)
    bp = repo / "blueprint.yml"
    bp.write_text("aqueduct: '1.0'\n")
    _git(["add", "blueprint.yml"], repo)
    _git(["commit", "-m", "manual edit"], repo)
    bp.write_text("aqueduct: '1.0'\nid: bp1\n")
    _git(["add", "blueprint.yml"], repo)
    aq_msg = (
        "fix(agent): apply patch\n\n"
        "---aqueduct---\npatches:\n  - p1: fix null check\n"
        "ops: set_module_config_key\nrun_id: run1\n---"
    )
    _git(["commit", "-m", aq_msg], repo)

    commits = q.git_blueprint_commits(bp)
    assert len(commits) == 2
    assert commits[0]["manual_edit"] is False
    assert commits[0]["patch_ids"] == ["p1"]
    assert commits[1]["manual_edit"] is True


def test_git_blueprint_commits_not_a_repo(tmp_path):
    bp = tmp_path / "blueprint.yml"
    bp.write_text("aqueduct: '1.0'\n")
    assert q.git_blueprint_commits(bp) == []
