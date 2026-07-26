"""Unit tests for ``aqueduct.executor.spill`` (Phase 81 step 3) — the
handoff spill directory lifecycle: URI classification, directory layout,
deletion, size measurement, and the orphan sweep. Pure filesystem + a real
local DuckDB observability store for the ``run_records``-driven sweep tests
— no Spark/DuckDB *execution* engine involved, so this stays ``unit``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

pytestmark = pytest.mark.unit

from aqueduct.executor.spill import (
    RULE_ID_HANDOFF_CLEANUP_UNAVAILABLE,
    delete_spill_tree,
    dir_size_bytes,
    ensure_parent_exists,
    is_remote_uri,
    local_only_or_fsspec_available,
    spill_dir_for,
    sweep_orphan_spills,
)
from aqueduct.surveyor.ddl import _DDL


# ── is_remote_uri / local_only_or_fsspec_available ──────────────────────────


@pytest.mark.parametrize(
    "uri,expected",
    [
        ("/tmp/aqueduct/handoff", False),
        ("./relative/path", False),
        ("file:///tmp/aqueduct/handoff", False),
        ("s3://bucket/handoff", True),
        ("gs://bucket/handoff", True),
        ("abfss://container@acct.dfs.core.windows.net/handoff", True),
        ("custom-scheme://somewhere", True),
    ],
)
def test_is_remote_uri(uri, expected):
    assert is_remote_uri(uri) is expected


def test_local_only_or_fsspec_available_true_for_local_path():
    assert local_only_or_fsspec_available("/tmp/whatever") is True


def test_local_only_or_fsspec_available_false_for_remote_without_fsspec(monkeypatch):
    import aqueduct.executor.spill as spill_mod

    monkeypatch.setattr(spill_mod, "_fsspec_available", lambda: False)
    assert local_only_or_fsspec_available("s3://bucket/handoff") is False


def test_local_only_or_fsspec_available_true_for_remote_with_fsspec(monkeypatch):
    import aqueduct.executor.spill as spill_mod

    monkeypatch.setattr(spill_mod, "_fsspec_available", lambda: True)
    assert local_only_or_fsspec_available("s3://bucket/handoff") is True


# ── spill_dir_for ─────────────────────────────────────────────────────────────


def test_spill_dir_for_layout():
    assert spill_dir_for("/root", "hash123", "run1", "edge1") == "/root/hash123/run1/edge1"


def test_spill_dir_for_strips_trailing_slash_on_root():
    assert spill_dir_for("/root/", "hash123", "run1", "edge1") == "/root/hash123/run1/edge1"


# ── ensure_parent_exists / delete_spill_tree (local) ─────────────────────────


def test_ensure_parent_exists_creates_local_dir(tmp_path):
    target = tmp_path / "a" / "b" / "c"
    ensure_parent_exists(str(target))
    assert target.exists() and target.is_dir()


def test_ensure_parent_exists_noop_for_remote_uri():
    # Must not raise even though the path doesn't exist anywhere real.
    ensure_parent_exists("s3://bucket/does/not/exist")


def test_delete_spill_tree_removes_local_directory(tmp_path):
    target = tmp_path / "spill"
    target.mkdir()
    (target / "part-0.parquet").write_bytes(b"data")
    assert delete_spill_tree(str(target)) is True
    assert not target.exists()


def test_delete_spill_tree_local_missing_dir_is_a_noop_success(tmp_path):
    target = tmp_path / "does_not_exist"
    assert delete_spill_tree(str(target)) is True


def test_delete_spill_tree_remote_without_fsspec_returns_false(monkeypatch):
    import aqueduct.executor.spill as spill_mod

    monkeypatch.setattr(spill_mod, "_fsspec_available", lambda: False)
    assert delete_spill_tree("s3://bucket/handoff/run1") is False


# ── dir_size_bytes ────────────────────────────────────────────────────────────


def test_dir_size_bytes_local(tmp_path):
    d = tmp_path / "spill"
    d.mkdir()
    (d / "a.parquet").write_bytes(b"12345")
    (d / "b.parquet").write_bytes(b"1234567")
    assert dir_size_bytes(str(d)) == 12


def test_dir_size_bytes_missing_local_dir_returns_none(tmp_path):
    assert dir_size_bytes(str(tmp_path / "nope")) is None


def test_dir_size_bytes_remote_without_fsspec_returns_none(monkeypatch):
    import aqueduct.executor.spill as spill_mod

    monkeypatch.setattr(spill_mod, "_fsspec_available", lambda: False)
    assert dir_size_bytes("s3://bucket/handoff/run1") is None


# ── sweep_orphan_spills ───────────────────────────────────────────────────────


class _DuckDBObsStore:
    """Minimal local DuckDB observability store for sweep tests — same
    ``connect()`` contract ``aqueduct.stores.duckdb_.DuckDBObservabilityStore``
    provides, built directly here to avoid pulling in the full Surveyor."""

    def __init__(self, path):
        self._path = path

    def connect(self):
        import contextlib

        import duckdb

        from aqueduct.stores.base import RelationalCursor

        @contextlib.contextmanager
        def _cm():
            conn = duckdb.connect(str(self._path))
            try:
                yield RelationalCursor(conn.cursor(), paramstyle="qmark")
            finally:
                conn.close()

        return _cm()


@pytest.fixture
def obs_store(tmp_path):
    store = _DuckDBObsStore(tmp_path / "observability.db")
    with store.connect() as cur:
        cur.execute(_DDL)
    return store


def _insert_run(obs_store, run_id, status, finished: bool):
    now = datetime.now(tz=UTC)
    finished_at = (now - timedelta(minutes=1)).isoformat() if finished else None
    with obs_store.connect() as cur:
        cur.execute(
            "INSERT INTO run_records (run_id, blueprint_id, status, started_at, finished_at) "
            "VALUES (?, ?, ?, ?, ?)",
            [run_id, "bp", status, now.isoformat(), finished_at],
        )


def _make_spill(root, manifest_hash, run_id):
    d = root / manifest_hash / run_id / "some_edge"
    d.mkdir(parents=True)
    (d / "part-0.parquet").write_bytes(b"data")
    return d


def test_sweep_deletes_a_successful_run_whose_own_cleanup_never_ran(tmp_path, obs_store):
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_success")
    _insert_run(obs_store, "run_success", "success", finished=True)

    deleted = sweep_orphan_spills(str(root), "hash1", current_run_id="run_current", keep_on_failure=True, obs_store=obs_store)

    assert not (root / "hash1" / "run_success").exists()
    assert len(deleted) == 1


def test_sweep_keeps_a_failed_run_when_keep_on_failure_true(tmp_path, obs_store):
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_failed")
    _insert_run(obs_store, "run_failed", "error", finished=True)

    deleted = sweep_orphan_spills(str(root), "hash1", current_run_id="run_current", keep_on_failure=True, obs_store=obs_store)

    assert (root / "hash1" / "run_failed").exists()
    assert deleted == []


def test_sweep_deletes_a_failed_run_when_keep_on_failure_false(tmp_path, obs_store):
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_failed")
    _insert_run(obs_store, "run_failed", "error", finished=True)

    deleted = sweep_orphan_spills(str(root), "hash1", current_run_id="run_current", keep_on_failure=False, obs_store=obs_store)

    assert not (root / "hash1" / "run_failed").exists()
    assert len(deleted) == 1


def test_sweep_never_touches_a_still_running_run(tmp_path, obs_store):
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_live")
    _insert_run(obs_store, "run_live", "success", finished=False)

    deleted = sweep_orphan_spills(str(root), "hash1", current_run_id="run_current", keep_on_failure=True, obs_store=obs_store)

    assert (root / "hash1" / "run_live").exists()
    assert deleted == []


def test_sweep_deletes_a_run_with_no_run_records_row_at_all(tmp_path, obs_store):
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_unknown")
    # No _insert_run call — no row exists for this run_id.

    deleted = sweep_orphan_spills(str(root), "hash1", current_run_id="run_current", keep_on_failure=True, obs_store=obs_store)

    assert not (root / "hash1" / "run_unknown").exists()
    assert len(deleted) == 1


def test_sweep_skips_current_run_id(tmp_path, obs_store):
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_current")
    # No run_records row for it either — it would otherwise be swept as
    # "unknown"; the explicit current_run_id skip must take priority.

    deleted = sweep_orphan_spills(str(root), "hash1", current_run_id="run_current", keep_on_failure=True, obs_store=obs_store)

    assert (root / "hash1" / "run_current").exists()
    assert deleted == []


def test_sweep_returns_empty_and_does_nothing_for_remote_root_without_fsspec(tmp_path, obs_store, monkeypatch):
    import aqueduct.executor.spill as spill_mod

    monkeypatch.setattr(spill_mod, "_fsspec_available", lambda: False)
    deleted = sweep_orphan_spills("s3://bucket/handoff", "hash1", current_run_id="run_current", keep_on_failure=True, obs_store=obs_store)
    assert deleted == []


def test_sweep_leaves_no_manifest_hash_directory_untouched(tmp_path, obs_store):
    """No spill directory at all under this manifest hash — sweep is a no-op,
    not an error."""
    root = tmp_path / "handoff"
    deleted = sweep_orphan_spills(str(root), "hash_never_run", current_run_id="run_current", keep_on_failure=True, obs_store=obs_store)
    assert deleted == []
