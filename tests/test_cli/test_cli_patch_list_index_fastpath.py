"""`aqueduct patch list` served from `patch_index` (Phase 84 item 4).

`_list_from_store` (aqueduct/cli/patch.py) used to read every patch BODY
(`PatchStore.iter_payloads`) just to render TEXT output, an O(n) scan. It now
serves TEXT output from `patch_index` metadata when an observability store is
available — no body reads at all for indexed patches — and falls back to the
full body scan when the index is unavailable, the query errors, or JSON
output is requested (JSON needs `confidence`/`failed_module`, which live only
in the body).
"""

from __future__ import annotations

import json

import pytest

from aqueduct.cli.patch import _list_from_store, _list_rows_via_index
from aqueduct.patch import index as ix
from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
from aqueduct.stores.object_store import make_patch_store

pytestmark = pytest.mark.unit


@pytest.fixture()
def obs_store(tmp_path):
    s = DuckDBObservabilityStore(tmp_path / "obs.db")
    with s.connect() as cur:
        ix.ensure_schema(cur)
    return s


@pytest.fixture()
def ps(tmp_path):
    return make_patch_store("local", "", tmp_path / "proj" / "patches")


def _seed_body(ps, status: str, filename: str, patch_id: str, **extra) -> str:
    """Write a patch body straight to the store; returns its object_key."""
    payload = {
        "patch_id": patch_id,
        "rationale": extra.get("rationale", f"fix for {patch_id}"),
        "confidence": extra.get("confidence", 0.8),
        "operations": [],
    }
    if "meta" in extra:
        payload["_aq_meta"] = extra["meta"]
    if status == "pending":
        return ps.write_pending(filename, payload)
    if status == "applied":
        return ps.write_applied(filename, payload)
    return ps.write_rejected(filename, payload)


def _index_row(object_key: str, patch_id: str, status: str, **kw) -> ix.PatchIndexRow:
    return ix.PatchIndexRow(
        patch_id=patch_id,
        status=status,
        object_key=object_key,
        blueprint_id=kw.get("blueprint_id", ""),
        run_id=kw.get("run_id", ""),
        rationale=kw.get("rationale", f"fix for {patch_id}"),
    )


class TestIndexFastPath:
    def test_indexed_patches_list_with_zero_body_reads(self, ps, obs_store, monkeypatch):
        key = _seed_body(ps, "pending", "20260101T000000_p1.json", "p1")
        with obs_store.connect() as cur:
            ix.upsert(cur, _index_row(key, "p1", "pending", blueprint_id="bp.A"))

        reads: list[str] = []
        orig_get_json = ps.get_json
        monkeypatch.setattr(ps, "get_json", lambda k: (reads.append(k), orig_get_json(k))[1])

        rows = _list_rows_via_index(ps, obs_store, ("pending",))

        assert rows is not None
        assert len(rows) == 1
        assert rows[0]["patch_id"] == "p1"
        assert rows[0]["blueprint_id"] == "bp.A"
        assert rows[0]["status"] == "pending"
        assert reads == []  # no body read needed — everything came from the index

    def test_pre_index_patch_still_lists_via_body_read(self, ps, obs_store):
        """A patch body with no matching patch_index row (pre-Phase-53, or a
        write path that skipped the index) must still appear — read
        individually since only its metadata is actually missing."""
        _seed_body(ps, "pending", "20260101T000000_old.json", "old")
        # No index row for "old" at all.

        rows = _list_rows_via_index(ps, obs_store, ("pending",))

        assert rows is not None
        assert {r["patch_id"] for r in rows} == {"old"}

    def test_no_obs_store_falls_back_to_none(self, ps):
        _seed_body(ps, "pending", "20260101T000000_p1.json", "p1")
        assert _list_rows_via_index(ps, None, ("pending",)) is None

    def test_query_error_falls_back_to_none(self, ps, obs_store, monkeypatch):
        _seed_body(ps, "pending", "20260101T000000_p1.json", "p1")

        class _BoomStore:
            def connect(self):
                raise RuntimeError("boom")

        assert _list_rows_via_index(ps, _BoomStore(), ("pending",)) is None

    def test_stale_index_row_pointing_at_deleted_body_is_dropped(self, ps, obs_store):
        with obs_store.connect() as cur:
            ix.upsert(cur, _index_row("pending/ghost.json", "ghost", "pending"))
        # No corresponding body in the store at all.

        rows = _list_rows_via_index(ps, obs_store, ("pending",))

        assert rows == []


class TestListFromStoreEndToEnd:
    def test_text_output_uses_index_and_omits_body_only_fields(self, ps, obs_store, capsys):
        key = _seed_body(ps, "pending", "20260101T000000_p1.json", "p1", rationale="rename col")
        with obs_store.connect() as cur:
            ix.upsert(
                cur, _index_row(key, "p1", "pending", blueprint_id="bp.A", rationale="rename col")
            )

        _list_from_store(ps, "pending", "text", obs_store=obs_store)
        out = capsys.readouterr().out
        assert "20260101T000000_p1.json" in out
        assert "bp.A" in out
        assert "rename col" in out

    def test_json_output_includes_body_only_fields_via_full_scan(self, ps, obs_store, capsys):
        key = _seed_body(
            ps,
            "pending",
            "20260101T000000_p1.json",
            "p1",
            confidence=0.42,
            meta={"run_id": "r1", "blueprint_id": "bp.A", "failed_module": "m1"},
        )
        with obs_store.connect() as cur:
            ix.upsert(cur, _index_row(key, "p1", "pending", blueprint_id="bp.A", run_id="r1"))

        _list_from_store(ps, "pending", "json", obs_store=obs_store)
        payload = json.loads(capsys.readouterr().out)
        assert len(payload) == 1
        assert payload[0]["confidence"] == 0.42
        assert payload[0]["failed_module"] == "m1"
        assert payload[0]["run_id"] == "r1"

    def test_empty_store_no_obs_store_still_reports_no_patches(self, ps, capsys):
        _list_from_store(ps, "pending", "text", obs_store=None)
        out = capsys.readouterr().out
        assert "No pending patches found" in out
