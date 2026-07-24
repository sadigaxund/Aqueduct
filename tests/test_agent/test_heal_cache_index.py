"""Unit tests for the Phase 53 index-backed heal cache.

Exercises the full patch lifecycle wiring without Spark or an LLM: stage /
archive write a body via a local PatchStore and an index row into a DuckDB
observability store, and memory.py resolves pending-reuse / replay / coaching /
history from that index. Pure ``unit`` layer.
"""

from __future__ import annotations

import pytest

from aqueduct.agent import archive_patch, memory, stage_patch_for_human
from aqueduct.agent.prompts import _load_previous_patches
from aqueduct.agent.signature import from_failure_context
from aqueduct.patch.grammar import PatchSpec
from aqueduct.patch.index import ensure_schema
from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
from aqueduct.stores.object_store import make_patch_store
from aqueduct.surveyor.models import FailureContext

pytestmark = pytest.mark.unit


@pytest.fixture()
def env(tmp_path):
    obs = DuckDBObservabilityStore(tmp_path / "obs.db")
    with obs.connect() as cur:
        ensure_schema(cur)
    ps = make_patch_store("local", "", tmp_path / "patches")
    fc = FailureContext(
        run_id="r1", blueprint_id="bp", failed_module="clean",
        error_message="AnalysisException: col x missing", stack_trace="",
        manifest_json="{}", started_at="2026-06-16T00:00:00+00:00",
        finished_at="2026-06-16T00:00:01+00:00", error_class="AnalysisException",
     engine="spark",)
    return tmp_path, obs, ps, fc


def _spec(patch_id="fix1"):
    return PatchSpec(
        patch_id=patch_id, root_cause="rename", rationale="rename the column",
        confidence=0.9,
        operations=[{"op": "set_module_config_key", "module_id": "clean",
                     "key": "query", "value": "SELECT 1"}],
    )


def test_stage_writes_body_and_indexes_pending(env):
    tmp_path, obs, ps, fc = env
    sig, _ = from_failure_context(fc)
    stage_patch_for_human(_spec(), tmp_path / "patches", fc, patch_store=ps, obs_store=obs)
    # body written under pending/
    assert list((tmp_path / "patches" / "pending").glob("*_fix1.json"))
    # index resolves it
    hit = memory.find_pending(obs, sig.hash)
    assert hit is not None and hit.patch_id == "fix1"
    assert hit.object_key.startswith("pending/")


def test_archive_marks_applied_and_clears_pending(env):
    tmp_path, obs, ps, fc = env
    sig, _ = from_failure_context(fc)
    stage_patch_for_human(_spec(), tmp_path / "patches", fc, patch_store=ps, obs_store=obs)
    archive_patch(_spec(), tmp_path / "patches", fc, mode="auto", patch_store=ps, obs_store=obs)
    assert memory.find_pending(obs, sig.hash) is None  # no longer pending
    # now visible to coaching + history
    co = memory.find_coaching_examples(obs, sig.hash, "", "AnalysisException")
    assert [c.patch_id for c in co] == ["fix1"]
    assert _load_previous_patches(obs)[0]["patch_id"] == "fix1"


def test_local_only_writes_file_but_skips_index(env):
    tmp_path, obs, ps, fc = env
    sig, _ = from_failure_context(fc)
    # no obs_store → body still written (back-compat), index empty
    stage_patch_for_human(_spec(), tmp_path / "patches", fc)
    assert list((tmp_path / "patches" / "pending").glob("*_fix1.json"))
    assert memory.find_pending(obs, sig.hash) is None


def test_find_replay_requires_success_set(env):
    tmp_path, obs, ps, fc = env
    sig, _ = from_failure_context(fc)
    archive_patch(_spec(), tmp_path / "patches", fc, mode="auto", patch_store=ps, obs_store=obs)
    assert memory.find_replay_candidate(obs, ps, sig.hash, set()) is None
    cand = memory.find_replay_candidate(obs, ps, sig.hash, {"fix1"})
    assert cand is not None and cand.patch_id == "fix1"
    # body (with operations) fetched from the object store
    assert cand.payload["operations"][0]["op"] == "set_module_config_key"


def test_find_helpers_return_empty_without_store(env):
    _, _, ps, _ = env
    assert memory.find_pending(None, "sig") is None
    assert memory.find_replay_candidate(None, ps, "sig", {"x"}) is None
    assert memory.find_coaching_examples(None, "a", "b", "c") == []
    assert _load_previous_patches(None) == []
