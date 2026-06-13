"""Unit tests for aqueduct/agent/memory.py — Phase 45 heal memory."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit


def _write_patch(path: Path, patch_id: str, sig_hash: str | None = None, **extra):
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "patch_id": patch_id,
        "rationale": "fix",
        "operations": [{"op": "set_module_config_key", "module_id": "m1", "key": "k", "value": "v"}],
    }
    if sig_hash is not None:
        meta = data.setdefault("_aq_meta", {})
        meta["failure_signature"] = {"hash": sig_hash, "error_class": "err", "where": "m1", "normalized_message": "boom"}
        meta["failure_signature_coarse"] = sig_hash[:8]
    for k, v in extra.items():
        if k == "_aq_meta" and isinstance(v, dict):
            data.setdefault("_aq_meta", {}).update(v)
        else:
            data[k] = v
    path.write_text(json.dumps(data))


class TestFindPending:
    def test_exact_match_returns_pending_hit(self, tmp_path):
        from aqueduct.agent.memory import find_pending
        _write_patch(tmp_path / "pending" / "001_fix.json", "fix-1", sig_hash="abc123")
        hit = find_pending("abc123", tmp_path)
        assert hit is not None
        assert hit.patch_id == "fix-1"
        assert hit.source == "llm"

    def test_no_match_returns_none(self, tmp_path):
        from aqueduct.agent.memory import find_pending
        _write_patch(tmp_path / "pending" / "001_fix.json", "fix-1", sig_hash="abc123")
        hit = find_pending("nonexistent", tmp_path)
        assert hit is None

    def test_empty_pending_dir_returns_none(self, tmp_path):
        from aqueduct.agent.memory import find_pending
        hit = find_pending("abc", tmp_path)
        assert hit is None

    def test_pre_phase45_no_signature_not_matched(self, tmp_path):
        from aqueduct.agent.memory import find_pending
        _write_patch(tmp_path / "pending" / "001_old.json", "old-fix")
        hit = find_pending("abc123", tmp_path)
        assert hit is None

    def test_newest_match_wins(self, tmp_path):
        from aqueduct.agent.memory import find_pending
        _write_patch(tmp_path / "pending" / "001_old.json", "old", sig_hash="abc123", _aq_meta={"staged_at": "2020-01-01"})
        _write_patch(tmp_path / "pending" / "002_new.json", "new", sig_hash="abc123", _aq_meta={"staged_at": "2025-01-01"})
        hit = find_pending("abc123", tmp_path)
        assert hit is not None
        assert hit.patch_id == "new"


class TestFindReplayCandidate:
    def test_exact_match_and_successful_returns_candidate(self, tmp_path):
        from aqueduct.agent.memory import find_replay_candidate
        _write_patch(tmp_path / "applied" / "001_fix.json", "fix-1", sig_hash="abc123")
        result = find_replay_candidate("abc123", tmp_path, {"fix-1"})
        assert result is not None
        assert result.patch_id == "fix-1"
        assert "operations" in result.payload

    def test_patch_id_not_in_successful_set_returns_none(self, tmp_path):
        from aqueduct.agent.memory import find_replay_candidate
        _write_patch(tmp_path / "applied" / "001_fix.json", "fix-1", sig_hash="abc123")
        result = find_replay_candidate("abc123", tmp_path, set())
        assert result is None

    def test_hash_mismatch_returns_none(self, tmp_path):
        from aqueduct.agent.memory import find_replay_candidate
        _write_patch(tmp_path / "applied" / "001_fix.json", "fix-1", sig_hash="abc123")
        result = find_replay_candidate("xxxxxx", tmp_path, {"fix-1"})
        assert result is None


class TestFindCoachingExamples:
    def test_tier_1_exact_hash_picked_first(self, tmp_path):
        from aqueduct.agent.memory import find_coaching_examples
        _write_patch(tmp_path / "applied" / "001_exact.json", "exact", sig_hash="abc123",
                     _aq_meta={"failure_signature": {"hash": "abc123", "error_class": "E1", "where": "m1", "normalized_message": "boom"},
                                "failure_signature_coarse": "abc12345"})
        _write_patch(tmp_path / "applied" / "002_coarse.json", "coarse", sig_hash="def456",
                     _aq_meta={"failure_signature": {"hash": "def456", "error_class": "E1", "where": "m2", "normalized_message": "boom"},
                                "failure_signature_coarse": "def45678"})
        examples = find_coaching_examples("abc123", "abc12345", "E1", tmp_path, limit=1)
        assert len(examples) == 1
        assert examples[0].patch_id == "exact"
        assert examples[0].tier == 1

    def test_no_matches_returns_empty(self, tmp_path):
        from aqueduct.agent.memory import find_coaching_examples
        # Empty applied dir → no examples at all
        examples = find_coaching_examples("xxxx", "yyyy", "Z", tmp_path)
        assert examples == []

    def test_deduplication_by_patch_id(self, tmp_path):
        from aqueduct.agent.memory import find_coaching_examples
        _write_patch(tmp_path / "applied" / "001_fix.json", "same-id", sig_hash="abc123",
                     _aq_meta={"failure_signature": {"hash": "abc123", "error_class": "E1", "where": "m1", "normalized_message": "boom"},
                                "failure_signature_coarse": "abc12345"})
        examples = find_coaching_examples("abc123", "abc12345", "E1", tmp_path)
        assert len(examples) == 1

    def test_sig_meta_tolerates_bare_hash_string(self, tmp_path):
        from aqueduct.agent.memory import _sig_meta
        payload = {"_aq_meta": {"failure_signature": "bare_hash_str"}}
        exact, coarse, sig = _sig_meta(payload)
        assert exact == "bare_hash_str"
        assert coarse is None
        assert sig == {}

    def test_sig_meta_tolerates_dict_signature(self, tmp_path):
        from aqueduct.agent.memory import _sig_meta
        payload = {"_aq_meta": {"failure_signature": {"hash": "abc123", "error_class": "E"}, "failure_signature_coarse": "abc12345"}}
        exact, coarse, sig = _sig_meta(payload)
        assert exact == "abc123"
        assert coarse == "abc12345"
        assert sig["error_class"] == "E"

    def test_sig_meta_no_signature_returns_none(self):
        from aqueduct.agent.memory import _sig_meta
        exact, coarse, sig = _sig_meta({})
        assert exact is None
        assert coarse is None
        assert sig == {}

    def test_limit_capped(self, tmp_path):
        from aqueduct.agent.memory import find_coaching_examples
        for i in range(5):
            _write_patch(tmp_path / "applied" / f"{i:03d}_fix.json", f"fix-{i}", sig_hash=f"abc{i:03d}",
                         _aq_meta={"failure_signature": {"hash": f"abc{i:03d}", "error_class": "E1", "where": "m1", "normalized_message": "x"},
                                    "failure_signature_coarse": f"abc{i:03d}x"})
        examples = find_coaching_examples("none", "none", "E1", tmp_path, limit=3)
        assert len(examples) == 3


class TestPendingHitFrozen:
    def test_frozen_mutation_raises(self):
        from aqueduct.agent.memory import PendingHit
        from dataclasses import FrozenInstanceError
        from pathlib import Path
        h = PendingHit(path=Path("/p"), patch_id="p1", staged_at=None, source="llm")
        with pytest.raises(FrozenInstanceError):
            h.patch_id = "other"


class TestReplayCandidateFrozen:
    def test_frozen_mutation_raises(self):
        from aqueduct.agent.memory import ReplayCandidate
        from dataclasses import FrozenInstanceError
        from pathlib import Path
        c = ReplayCandidate(path=Path("/p"), patch_id="p1", payload={})
        with pytest.raises(FrozenInstanceError):
            c.patch_id = "other"


class TestCoachingExampleDefaults:
    def test_tier_defaults_to_4(self):
        from aqueduct.agent.memory import CoachingExample
        ex = CoachingExample(patch_id="p1", error_class="E", where="m1", normalized_message="x", rationale="fix")
        assert ex.tier == 4
        assert ex.ops == []
