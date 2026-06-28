"""Unit tests for the patch_index table (patch/index.py).

Pure relational logic exercised against a real DuckDB observability store (no
Spark, no network) — the ``unit`` layer. patch_index is the backend-blind truth
the heal cache queries instead of scanning ``patches/``.
"""

from __future__ import annotations

import pytest

from aqueduct.patch import index as ix
from aqueduct.stores.duckdb_ import DuckDBObservabilityStore

pytestmark = pytest.mark.unit


@pytest.fixture()
def store(tmp_path):
    s = DuckDBObservabilityStore(tmp_path / "obs.db")
    with s.connect() as cur:
        ix.ensure_schema(cur)
    return s


def _row(patch_id, status, **kw):
    return ix.PatchIndexRow(
        patch_id=patch_id,
        status=status,
        object_key=f"{status}/{patch_id}.json",
        signature=kw.get("signature", "sigA"),
        signature_coarse=kw.get("signature_coarse", "coarseA"),
        error_class=kw.get("error_class", "AnalysisException"),
        where_field=kw.get("where_field", "clean"),
        normalized_message=kw.get("normalized_message", "col missing"),
        rationale=kw.get("rationale", "rename col"),
        ops=kw.get("ops", ["set_module_config_key"]),
        source=kw.get("source", "llm"),
    )


def test_upsert_and_find_pending(store):
    with store.connect() as cur:
        ix.upsert(cur, _row("p1", "pending"))
    with store.connect() as cur:
        hit = ix.find_pending(cur, "sigA")
    assert hit is not None
    assert hit["patch_id"] == "p1"
    assert hit["object_key"] == "pending/p1.json"
    assert hit["ops"] == ["set_module_config_key"]  # JSON column round-trips to list


def test_find_pending_none_for_unknown_or_empty_signature(store):
    with store.connect() as cur:
        assert ix.find_pending(cur, "nope") is None
        assert ix.find_pending(cur, "") is None


def test_set_status_moves_out_of_pending(store):
    with store.connect() as cur:
        ix.upsert(cur, _row("p1", "pending"))
        ix.set_status(cur, "p1", "applied")
    with store.connect() as cur:
        assert ix.find_pending(cur, "sigA") is None
        assert ix.get(cur, "p1")["status"] == "applied"


def _row_bp(patch_id, status, blueprint_id, **kw):
    import dataclasses
    return dataclasses.replace(_row(patch_id, status, **kw), blueprint_id=blueprint_id)


def test_list_by_status_filters_by_status_and_blueprint(store):
    """`patch list` is backend-blind — list_by_status filters the index by
    lifecycle status and optionally one blueprint, regardless of where bodies live."""
    with store.connect() as cur:
        ix.upsert(cur, _row_bp("p1", "pending", "bp.A", signature="s1"))
        ix.upsert(cur, _row_bp("p2", "applied", "bp.A", signature="s2"))
        ix.upsert(cur, _row_bp("p3", "pending", "bp.B", signature="s3"))
    with store.connect() as cur:
        assert {r["patch_id"] for r in ix.list_by_status(cur, status="pending")} == {"p1", "p3"}
        assert {r["patch_id"] for r in ix.list_by_status(cur, blueprint_id="bp.A")} == {"p1", "p2"}
        assert {
            r["patch_id"] for r in ix.list_by_status(cur, status="pending", blueprint_id="bp.A")
        } == {"p1"}
        assert len(ix.list_by_status(cur)) == 3  # status=None → all states


def test_upsert_is_idempotent_on_patch_id(store):
    with store.connect() as cur:
        ix.upsert(cur, _row("p1", "pending"))
        ix.upsert(cur, _row("p1", "applied", rationale="v2"))
        row = ix.get(cur, "p1")
    assert row["status"] == "applied"
    assert row["rationale"] == "v2"


def test_find_replay_only_confirmed_successful(store):
    with store.connect() as cur:
        ix.upsert(cur, _row("p1", "applied"))
    with store.connect() as cur:
        assert ix.find_replay(cur, "sigA", set()) is None          # no success set
        assert ix.find_replay(cur, "sigA", {"other"}) is None      # id not successful
        hit = ix.find_replay(cur, "sigA", {"p1"})
    assert hit["patch_id"] == "p1"


def test_find_coaching_tiers_and_dedupe(store):
    with store.connect() as cur:
        ix.upsert(cur, _row("exact", "applied", signature="sigA"))
        ix.upsert(cur, _row("coarse", "applied", signature="other", signature_coarse="coarseA"))
        ix.upsert(cur, _row("klass", "applied", signature="z", signature_coarse="z",
                            error_class="AnalysisException"))
    with store.connect() as cur:
        out = ix.find_coaching(cur, "sigA", "coarseA", "AnalysisException", limit=3)
    by_id = {d["patch_id"]: d["_tier"] for d in out}
    assert by_id["exact"] == 1
    assert by_id["coarse"] == 2
    assert by_id["klass"] == 3
    # deduped: each patch_id appears once
    assert len(out) == len({d["patch_id"] for d in out})


def test_find_coaching_only_applied(store):
    with store.connect() as cur:
        ix.upsert(cur, _row("pend", "pending"))
    with store.connect() as cur:
        assert ix.find_coaching(cur, "sigA", "coarseA", "AnalysisException") == []


def test_recent_applied_newest_first_and_limit(store):
    with store.connect() as cur:
        for i in range(5):
            ix.upsert(cur, _row(f"p{i}", "applied"))
    with store.connect() as cur:
        out = ix.recent_applied(cur, limit=3)
    assert len(out) == 3
    assert all(d["status"] == "applied" for d in out)


def test_get_by_id(store):
    with store.connect() as cur:
        ix.upsert(cur, _row("p1", "pending"))
        assert ix.get(cur, "p1")["patch_id"] == "p1"
        assert ix.get(cur, "missing") is None
        assert ix.get(cur, "") is None
