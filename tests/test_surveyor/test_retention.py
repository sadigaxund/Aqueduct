"""Phase 85 B1 — observability-store retention: prune_store/vacuum_store/
maybe_prune_store, and the run_records.engine migration's idempotency."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from aqueduct.config import ObservabilityRetentionConfig
from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
from aqueduct.surveyor.retention import maybe_prune_store, prune_store, vacuum_store
from aqueduct.surveyor.surveyor import _DDL, _HEAL_ATTEMPTS_DDL

pytestmark = pytest.mark.unit


def _store(tmp_path):
    obs = DuckDBObservabilityStore(str(tmp_path / "o.db"))
    with obs.connect() as cur:
        cur.execute(_DDL)
        cur.execute(_HEAL_ATTEMPTS_DDL)
    return obs


def _insert_run_record(obs, run_id: str, started_at: datetime) -> None:
    with obs.connect() as cur:
        cur.execute(
            """
            INSERT INTO run_records (run_id, blueprint_id, status, started_at, finished_at,
                                      module_results, parent_run_id, engine)
            VALUES (?, 'bp.x', 'success', ?, ?, '[]', NULL, 'duckdb')
            """,
            [run_id, started_at, started_at],
        )


def test_prune_store_deletes_only_rows_past_the_window(tmp_path):
    obs = _store(tmp_path)
    now = datetime.now(tz=UTC)
    _insert_run_record(obs, "old", now - timedelta(days=200))
    _insert_run_record(obs, "recent", now - timedelta(days=1))

    retention = ObservabilityRetentionConfig(run_records_days=90)
    prune_store(obs, retention, now=now)

    with obs.connect() as cur:
        remaining = {r[0] for r in cur.execute("SELECT run_id FROM run_records").fetchall()}
    assert remaining == {"recent"}


def test_prune_store_returns_deleted_counts(tmp_path):
    obs = _store(tmp_path)
    now = datetime.now(tz=UTC)
    _insert_run_record(obs, "old1", now - timedelta(days=200))
    _insert_run_record(obs, "old2", now - timedelta(days=200))

    retention = ObservabilityRetentionConfig(run_records_days=90)
    deleted = prune_store(obs, retention, now=now)
    assert deleted["run_records"] == 2


def test_maybe_prune_store_throttles_within_a_day(tmp_path):
    obs = _store(tmp_path)
    now = datetime.now(tz=UTC)
    _insert_run_record(obs, "old", now - timedelta(days=200))

    retention = ObservabilityRetentionConfig(run_records_days=90)

    ran_first = maybe_prune_store(obs, retention, now=now)
    assert ran_first is True
    with obs.connect() as cur:
        remaining = {r[0] for r in cur.execute("SELECT run_id FROM run_records").fetchall()}
    assert remaining == set()  # the old row was pruned

    # Insert another stale row and call again a few hours later — SAME day,
    # must be throttled (assert on the marker behaviour, not wall-clock sleep).
    _insert_run_record(obs, "old2", now - timedelta(days=200))
    ran_second = maybe_prune_store(obs, retention, now=now + timedelta(hours=3))
    assert ran_second is False
    with obs.connect() as cur:
        remaining2 = {r[0] for r in cur.execute("SELECT run_id FROM run_records").fetchall()}
    assert remaining2 == {"old2"}, "throttled call must not have pruned the new stale row"


def test_maybe_prune_store_runs_again_after_a_day(tmp_path):
    obs = _store(tmp_path)
    now = datetime.now(tz=UTC)
    retention = ObservabilityRetentionConfig(run_records_days=90)

    assert maybe_prune_store(obs, retention, now=now) is True
    _insert_run_record(obs, "old2", now - timedelta(days=200))
    assert maybe_prune_store(obs, retention, now=now + timedelta(days=1, minutes=1)) is True
    with obs.connect() as cur:
        remaining = {r[0] for r in cur.execute("SELECT run_id FROM run_records").fetchall()}
    assert remaining == set()


def test_vacuum_store_reclaims_without_destroying_rows(tmp_path):
    """`VACUUM` is the one operation that rewrites the store's own file, so
    the property worth pinning is not "it did not raise" — it is that the
    rows survive it and the store is still usable afterwards. A vacuum that
    silently emptied the table would satisfy "no exception" perfectly."""
    obs = _store(tmp_path)
    now = datetime.now(tz=UTC)
    _insert_run_record(obs, "keep1", now)
    _insert_run_record(obs, "keep2", now)

    vacuum_store(obs)

    with obs.connect() as cur:
        surviving = {r[0] for r in cur.execute("SELECT run_id FROM run_records").fetchall()}
    assert surviving == {"keep1", "keep2"}

    # Still writable after the file rewrite — a vacuum that left the store in
    # a read-only or corrupt state would pass the read above.
    _insert_run_record(obs, "after_vacuum", now)
    with obs.connect() as cur:
        assert (
            cur.execute(
                "SELECT count(*) FROM run_records WHERE run_id = 'after_vacuum'"
            ).fetchone()[0]
            == 1
        )


def test_engine_migration_idempotent_against_columnless_store(tmp_path):
    """The migration this ranking item actually needs proven: run it TWICE
    against a store created WITHOUT `run_records.engine`, assert no error
    and no data loss."""
    path = tmp_path / "observability.db"
    con = duckdb.connect(str(path))
    con.execute(
        """
        CREATE TABLE run_records (
            run_id VARCHAR PRIMARY KEY,
            blueprint_id VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            started_at TIMESTAMPTZ NOT NULL,
            finished_at TIMESTAMPTZ,
            module_results JSON,
            parent_run_id VARCHAR
        )
        """
    )
    con.execute("INSERT INTO run_records VALUES ('r1', 'bp', 'success', now(), now(), '[]', NULL)")
    con.close()

    from aqueduct.surveyor.surveyor import Surveyor

    class _FakeManifest:
        blueprint_id = "bp"
        modules = ()
        edges = ()
        name = "test"
        provenance_map = None

    Surveyor(_FakeManifest(), tmp_path, engine="duckdb").start("r2")
    Surveyor(_FakeManifest(), tmp_path, engine="duckdb").start("r3")  # run migration TWICE

    con = duckdb.connect(str(path))
    rows = con.execute("SELECT run_id, engine FROM run_records ORDER BY run_id").fetchall()
    con.close()
    assert rows == [("r1", None), ("r2", "duckdb"), ("r3", "duckdb")]
