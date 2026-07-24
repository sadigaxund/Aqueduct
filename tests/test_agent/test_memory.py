"""Unit tests for aqueduct/agent/memory.py — Phase 53 heal memory.

The memory layer queries the ``patch_index`` SQL table via an
ObservabilityStore.  These tests use a real DuckDB store so the SQL paths
are exercised without mocking.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
import duckdb

from aqueduct.stores.duckdb_ import DuckDBObservabilityStore


pytestmark = pytest.mark.unit


def _make_store(db_path: Path) -> DuckDBObservabilityStore:
    from aqueduct.patch.index import ensure_schema

    s = DuckDBObservabilityStore(db_path)
    # Real schema (incl. migrations, e.g. the Phase 78 `engine` column) —
    # single source of truth instead of a hand-duplicated CREATE TABLE that
    # silently drifts from aqueduct/patch/index.py's DDL.
    with s.connect() as cur:
        ensure_schema(cur)
    return s


def _stamp(store, **kw):
    defaults = dict(
        patch_id="p1", object_key="/obj/p1.json", blueprint_id="bp1",
        status="pending", source="llm",
        signature="abc123", signature_coarse="abc12345",
        error_class="E1", where_field="m1", normalized_message="boom",
        rationale="fix", ops='[{"op": "set_module_config_key"}]',
        engine="spark",
    )
    defaults.update(kw)
    now = "2025-01-01T00:00:00"
    with store.connect() as cur:
        cur.execute(
            "INSERT OR REPLACE INTO patch_index "
            "(patch_id, object_key, blueprint_id, status, source, "
            " signature, signature_coarse, "
            " error_class, where_field, normalized_message, rationale, ops, "
            " created_at, updated_at, engine) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [defaults.get(k, "") for k in (
                "patch_id", "object_key", "blueprint_id", "status", "source",
                "signature", "signature_coarse",
                "error_class", "where_field", "normalized_message",
                "rationale", "ops",
            )] + [defaults.get("created_at", now), now, defaults.get("engine", "spark")],
        )


class TestFindPending:
    def test_exact_match_returns_pending_hit(self, tmp_path):
        from aqueduct.agent.memory import find_pending
        store = _make_store(tmp_path / "obs.db")
        _stamp(store, patch_id="fix-1", signature="abc123")
        hit = find_pending(store, "abc123")
        assert hit is not None
        assert hit.patch_id == "fix-1"
        assert hit.source == "llm"

    def test_no_match_returns_none(self, tmp_path):
        from aqueduct.agent.memory import find_pending
        store = _make_store(tmp_path / "obs.db")
        _stamp(store, signature="abc123")
        hit = find_pending(store, "nonexistent")
        assert hit is None

    def test_empty_table_returns_none(self, tmp_path):
        from aqueduct.agent.memory import find_pending
        store = _make_store(tmp_path / "obs.db")
        hit = find_pending(store, "abc")
        assert hit is None

    def test_newest_match_wins(self, tmp_path):
        from aqueduct.agent.memory import find_pending
        store = _make_store(tmp_path / "obs.db")
        _stamp(store, patch_id="old", signature="abc123",
               created_at="2020-01-01 00:00:00")
        _stamp(store, patch_id="new", signature="abc123",
               created_at="2025-01-01 00:00:00")
        hit = find_pending(store, "abc123")
        assert hit is not None
        assert hit.patch_id == "new"

    def test_none_store_returns_none(self, tmp_path):
        from aqueduct.agent.memory import find_pending
        assert find_pending(None, "abc") is None


class TestFindReplayCandidate:
    def test_exact_match_and_successful_returns_candidate(self, tmp_path):
        from aqueduct.agent.memory import find_replay_candidate
        store = _make_store(tmp_path / "obs.db")
        _stamp(store, patch_id="fix-1", status="applied",
               signature="abc123",
               ops=json.dumps([{"op": "set_module_config_key", "module_id": "m1", "key": "k", "value": "v"}]))
        from unittest.mock import MagicMock
        mock_patch_store = MagicMock()
        mock_patch_store.get_json.return_value = {
            "patch_id": "fix-1",
            "operations": [{"op": "set_module_config_key", "module_id": "m1", "key": "k", "value": "v"}],
        }
        result = find_replay_candidate(store, mock_patch_store, "abc123", {"fix-1"})
        assert result is not None
        assert result.patch_id == "fix-1"
        assert "operations" in result.payload

    def test_patch_id_not_in_successful_set_returns_none(self, tmp_path):
        from aqueduct.agent.memory import find_replay_candidate
        store = _make_store(tmp_path / "obs.db")
        _stamp(store, patch_id="fix-1", status="applied", signature="abc123")
        result = find_replay_candidate(store, None, "abc123", set())
        assert result is None

    def test_hash_mismatch_returns_none(self, tmp_path):
        from aqueduct.agent.memory import find_replay_candidate
        store = _make_store(tmp_path / "obs.db")
        _stamp(store, patch_id="fix-1", status="applied", signature="abc123")
        result = find_replay_candidate(store, None, "xxxxxx", {"fix-1"})
        assert result is None

    def test_none_store_returns_none(self, tmp_path):
        from aqueduct.agent.memory import find_replay_candidate
        assert find_replay_candidate(None, None, "abc", set()) is None


class TestFindCoachingExamples:
    def test_tier_1_exact_hash_picked_first(self, tmp_path):
        from aqueduct.agent.memory import find_coaching_examples
        store = _make_store(tmp_path / "obs.db")
        _stamp(store, patch_id="exact", status="applied",
               signature="abc123", signature_coarse="abc12345",
               error_class="E1", where_field="m1")
        _stamp(store, patch_id="coarse", status="applied",
               signature="def456", signature_coarse="def45678",
               error_class="E1", where_field="m2")
        examples = find_coaching_examples(store, "abc123", "abc12345", "E1", limit=1)
        assert len(examples) == 1
        assert examples[0].patch_id == "exact"
        assert examples[0].tier == 1

    def test_no_matches_returns_empty(self, tmp_path):
        from aqueduct.agent.memory import find_coaching_examples
        store = _make_store(tmp_path / "obs.db")
        examples = find_coaching_examples(store, "xxxx", "yyyy", "Z")
        assert examples == []

    def test_deduplication_by_patch_id(self, tmp_path):
        from aqueduct.agent.memory import find_coaching_examples
        store = _make_store(tmp_path / "obs.db")
        _stamp(store, patch_id="same-id", status="applied",
               signature="abc123", signature_coarse="abc12345",
               error_class="E1", where_field="m1")
        examples = find_coaching_examples(store, "abc123", "abc12345", "E1")
        assert len(examples) == 1

    def test_limit_capped(self, tmp_path):
        from aqueduct.agent.memory import find_coaching_examples
        store = _make_store(tmp_path / "obs.db")
        for i in range(5):
            _stamp(store, patch_id=f"fix-{i}", status="applied",
                   signature=f"abc{i:03d}",
                   signature_coarse=f"abc{i:03d}x",
                   error_class="E1", where_field="m1")
        examples = find_coaching_examples(store, "none", "none", "E1", limit=3)
        assert len(examples) == 3

    def test_none_store_returns_empty(self, tmp_path):
        from aqueduct.agent.memory import find_coaching_examples
        assert find_coaching_examples(None, "abc", "abc", "E1") == []


class TestPendingHitFrozen:
    def test_frozen_mutation_raises(self):
        from aqueduct.agent.memory import PendingHit
        from dataclasses import FrozenInstanceError
        h = PendingHit(object_key="/obj/p1.json", patch_id="p1", staged_at=None, source="llm")
        with pytest.raises(FrozenInstanceError):
            h.patch_id = "other"


class TestReplayCandidateFrozen:
    def test_frozen_mutation_raises(self):
        from aqueduct.agent.memory import ReplayCandidate
        from dataclasses import FrozenInstanceError
        c = ReplayCandidate(object_key="/obj/p1.json", patch_id="p1", payload={})
        with pytest.raises(FrozenInstanceError):
            c.patch_id = "other"


class TestCoachingExampleDefaults:
    def test_tier_defaults_to_4(self):
        from aqueduct.agent.memory import CoachingExample
        ex = CoachingExample(patch_id="p1", error_class="E", where="m1", normalized_message="x", rationale="fix")
        assert ex.tier == 4
        assert ex.ops == []
