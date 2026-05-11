"""Tests for Phase 18 — Git-Integrated Patch Lifecycle.

Covers `_uncommitted_applied_patches()` from `aqueduct/cli.py`.
Uses subprocess monkeypatching to simulate git environments without a real repo.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.unit

from aqueduct.cli import _uncommitted_applied_patches


def _write_patch(applied_dir: Path, name: str, applied_at: str) -> Path:
    """Write a minimal applied patch JSON with an applied_at timestamp."""
    p = applied_dir / name
    p.write_text(
        json.dumps({"patch_id": name.replace(".json", ""), "applied_at": applied_at}),
        encoding="utf-8",
    )
    return p


class TestUncommittedAppliedPatches:
    """Unit tests for _uncommitted_applied_patches()."""

    def test_no_applied_patches_dir_returns_empty(self, tmp_path):
        """No applied/ dir → returns []."""
        bp = tmp_path / "bp.yml"
        bp.write_text("")
        patches_root = tmp_path / "patches"
        result = _uncommitted_applied_patches(bp, patches_root)
        assert result == []

    def test_empty_applied_dir_returns_empty(self, tmp_path):
        """applied/ dir exists but is empty → returns []."""
        bp = tmp_path / "bp.yml"
        bp.write_text("")
        applied_dir = tmp_path / "patches" / "applied"
        applied_dir.mkdir(parents=True)
        result = _uncommitted_applied_patches(bp, tmp_path / "patches")
        assert result == []

    def test_not_in_git_repo_returns_all(self, tmp_path, monkeypatch):
        """Not in a git repo (git log fails) → all applied patches returned."""
        bp = tmp_path / "bp.yml"
        bp.write_text("")
        applied_dir = tmp_path / "patches" / "applied"
        applied_dir.mkdir(parents=True)
        p1 = _write_patch(applied_dir, "p001.json", "2026-05-10T10:00:00+00:00")
        p2 = _write_patch(applied_dir, "p002.json", "2026-05-10T11:00:00+00:00")

        def mock_run(args, **kwargs):
            return MagicMock(returncode=1, stdout="", stderr="fatal: not a git repo")

        monkeypatch.setattr(subprocess, "run", mock_run)
        result = _uncommitted_applied_patches(bp, tmp_path / "patches")
        assert sorted(r.name for r in result) == ["p001.json", "p002.json"]

    def test_blueprint_never_committed_returns_all(self, tmp_path, monkeypatch):
        """git log returns empty string (blueprint never committed) → all patches returned."""
        bp = tmp_path / "bp.yml"
        bp.write_text("")
        applied_dir = tmp_path / "patches" / "applied"
        applied_dir.mkdir(parents=True)
        p1 = _write_patch(applied_dir, "p001.json", "2026-05-10T10:00:00+00:00")

        def mock_run(args, **kwargs):
            # returncode=0 but stdout empty = blueprint never committed
            return MagicMock(returncode=0, stdout="", stderr="")

        monkeypatch.setattr(subprocess, "run", mock_run)
        result = _uncommitted_applied_patches(bp, tmp_path / "patches")
        assert len(result) == 1
        assert result[0].name == "p001.json"

    def test_patch_applied_after_last_commit_is_returned(self, tmp_path, monkeypatch):
        """Patch applied_at > last git commit timestamp → returned."""
        bp = tmp_path / "bp.yml"
        bp.write_text("")
        applied_dir = tmp_path / "patches" / "applied"
        applied_dir.mkdir(parents=True)
        # Patch applied after the last commit
        _write_patch(applied_dir, "p001.json", "2026-05-10T12:00:00+00:00")

        def mock_run(args, **kwargs):
            # Last commit was at 10:00, patch at 12:00 → uncommitted
            return MagicMock(returncode=0, stdout="2026-05-10T10:00:00+00:00\n", stderr="")

        monkeypatch.setattr(subprocess, "run", mock_run)
        result = _uncommitted_applied_patches(bp, tmp_path / "patches")
        assert len(result) == 1
        assert result[0].name == "p001.json"

    def test_patch_applied_before_last_commit_not_returned(self, tmp_path, monkeypatch):
        """Patch applied_at ≤ last git commit timestamp → NOT returned."""
        bp = tmp_path / "bp.yml"
        bp.write_text("")
        applied_dir = tmp_path / "patches" / "applied"
        applied_dir.mkdir(parents=True)
        # Patch applied before the last commit
        _write_patch(applied_dir, "p001.json", "2026-05-10T08:00:00+00:00")

        def mock_run(args, **kwargs):
            # Last commit was at 10:00, patch at 08:00 → already committed
            return MagicMock(returncode=0, stdout="2026-05-10T10:00:00+00:00\n", stderr="")

        monkeypatch.setattr(subprocess, "run", mock_run)
        result = _uncommitted_applied_patches(bp, tmp_path / "patches")
        assert result == []

    def test_mixed_patches_only_uncommitted_returned(self, tmp_path, monkeypatch):
        """Only patches newer than last commit are returned."""
        bp = tmp_path / "bp.yml"
        bp.write_text("")
        applied_dir = tmp_path / "patches" / "applied"
        applied_dir.mkdir(parents=True)
        # Old patch (committed), new patch (uncommitted)
        _write_patch(applied_dir, "p001.json", "2026-05-10T08:00:00+00:00")
        _write_patch(applied_dir, "p002.json", "2026-05-10T14:00:00+00:00")

        def mock_run(args, **kwargs):
            return MagicMock(returncode=0, stdout="2026-05-10T10:00:00+00:00\n", stderr="")

        monkeypatch.setattr(subprocess, "run", mock_run)
        result = _uncommitted_applied_patches(bp, tmp_path / "patches")
        assert len(result) == 1
        assert result[0].name == "p002.json"

    def test_aq_meta_applied_at_used_when_top_level_absent(self, tmp_path, monkeypatch):
        """_aq_meta.applied_at is used when top-level applied_at is absent."""
        bp = tmp_path / "bp.yml"
        bp.write_text("")
        applied_dir = tmp_path / "patches" / "applied"
        applied_dir.mkdir(parents=True)
        # No top-level applied_at, but has _aq_meta.applied_at
        p = applied_dir / "p001.json"
        p.write_text(
            json.dumps({
                "patch_id": "p001",
                "_aq_meta": {"applied_at": "2026-05-10T14:00:00+00:00"},
            }),
            encoding="utf-8",
        )

        def mock_run(args, **kwargs):
            # Last commit at 10:00, _aq_meta.applied_at at 14:00 → uncommitted
            return MagicMock(returncode=0, stdout="2026-05-10T10:00:00+00:00\n", stderr="")

        monkeypatch.setattr(subprocess, "run", mock_run)
        result = _uncommitted_applied_patches(bp, tmp_path / "patches")
        assert len(result) == 1
        assert result[0].name == "p001.json"
