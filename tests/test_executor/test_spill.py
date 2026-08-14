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


def test_ensure_parent_exists_noop_for_remote_uri(monkeypatch):
    # Must not raise even though the path doesn't exist anywhere real, and
    # must short-circuit BEFORE any local filesystem work — a remote URI has
    # no local directory to create, so `_local_path`/`Path.mkdir` must never
    # even be reached (not just "happen to not raise").
    import aqueduct.executor.spill as spill_mod

    calls: list[str] = []
    monkeypatch.setattr(
        spill_mod,
        "_local_path",
        lambda uri: (calls.append(uri), spill_mod.Path(uri))[1],
    )
    ensure_parent_exists("s3://bucket/does/not/exist")
    assert calls == []


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


def _insert_run(obs_store, run_id, status, finished: bool, *, blueprint_id="bp", minutes_ago=1):
    """``minutes_ago`` positions ``finished_at`` relative to now — the ONLY
    thing the retention rule reads it for is ordering two rows against each
    other (never a duration or a threshold), so tests state the order
    explicitly rather than relying on insertion timing."""
    now = datetime.now(tz=UTC)
    finished_at = (now - timedelta(minutes=minutes_ago)).isoformat() if finished else None
    with obs_store.connect() as cur:
        cur.execute(
            "INSERT INTO run_records (run_id, blueprint_id, status, started_at, finished_at) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                run_id,
                blueprint_id,
                status,
                (now - timedelta(minutes=minutes_ago + 1)).isoformat(),
                finished_at,
            ],
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

    deleted = sweep_orphan_spills(
        str(root), current_run_id="run_current", keep_on_failure=True, obs_store=obs_store
    )

    assert not (root / "hash1" / "run_success").exists()
    # The run dir AND its now-empty parent hash dir are both reclaimed.
    assert not (root / "hash1").exists()
    assert len(deleted) == 2


def test_sweep_keeps_a_failed_run_when_keep_on_failure_true(tmp_path, obs_store):
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_failed")
    _insert_run(obs_store, "run_failed", "error", finished=True)

    deleted = sweep_orphan_spills(
        str(root), current_run_id="run_current", keep_on_failure=True, obs_store=obs_store
    )

    assert (root / "hash1" / "run_failed").exists()
    # hash1 still holds a kept failure — not empty, so the hash dir survives too.
    assert (root / "hash1").exists()
    assert deleted == []


def test_sweep_deletes_a_failed_run_when_keep_on_failure_false(tmp_path, obs_store):
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_failed")
    _insert_run(obs_store, "run_failed", "error", finished=True)

    deleted = sweep_orphan_spills(
        str(root), current_run_id="run_current", keep_on_failure=False, obs_store=obs_store
    )

    assert not (root / "hash1" / "run_failed").exists()
    assert not (root / "hash1").exists()
    assert len(deleted) == 2


def test_sweep_never_touches_a_still_running_run(tmp_path, obs_store):
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_live")
    _insert_run(obs_store, "run_live", "success", finished=False)

    deleted = sweep_orphan_spills(
        str(root), current_run_id="run_current", keep_on_failure=True, obs_store=obs_store
    )

    assert (root / "hash1" / "run_live").exists()
    assert (root / "hash1").exists()
    assert deleted == []


def test_sweep_deletes_a_run_with_no_run_records_row_at_all(tmp_path, obs_store):
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_unknown")
    # No _insert_run call — no row exists for this run_id.

    deleted = sweep_orphan_spills(
        str(root), current_run_id="run_current", keep_on_failure=True, obs_store=obs_store
    )

    assert not (root / "hash1" / "run_unknown").exists()
    assert not (root / "hash1").exists()
    assert len(deleted) == 2


def test_sweep_skips_current_run_id(tmp_path, obs_store):
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_current")
    # No run_records row for it either — it would otherwise be swept as
    # "unknown"; the explicit current_run_id skip must take priority.

    deleted = sweep_orphan_spills(
        str(root), current_run_id="run_current", keep_on_failure=True, obs_store=obs_store
    )

    assert (root / "hash1" / "run_current").exists()
    assert deleted == []


def test_sweep_returns_empty_and_does_nothing_for_remote_root_without_fsspec(
    tmp_path, obs_store, monkeypatch
):
    import aqueduct.executor.spill as spill_mod

    monkeypatch.setattr(spill_mod, "_fsspec_available", lambda: False)
    deleted = sweep_orphan_spills(
        "s3://bucket/handoff",
        current_run_id="run_current",
        keep_on_failure=True,
        obs_store=obs_store,
    )
    assert deleted == []


def test_sweep_of_empty_root_is_a_noop(tmp_path, obs_store):
    """No spill directory at all under this root — sweep is a no-op, not an
    error."""
    root = tmp_path / "handoff"
    deleted = sweep_orphan_spills(
        str(root), current_run_id="run_current", keep_on_failure=True, obs_store=obs_store
    )
    assert deleted == []


# ── Root-level sweep across manifest-hash directories ────────────────────────
#
# A heal changes the compiled Manifest, which changes `manifest_hash`
# (executor.models.manifest_hash hashes the WHOLE Manifest) — so a run after
# a patch writes under a brand-new hash directory. Before this fix, the sweep
# only ever looked inside the CURRENT run's hash directory, so a prior hash
# directory's kept-failure spill (handoff.keep_on_failure defaults true) was
# never revisited by anything and accumulated on disk forever, one heal at a
# time. These tests guard the root-level rewrite that scans every hash
# directory under `root` and decides each run_id's fate from `run_records`
# alone.


def test_sweep_reclaims_a_prior_hash_directory_after_a_heal_changed_the_hash(tmp_path, obs_store):
    root = tmp_path / "handoff"
    _make_spill(root, "hashA", "run_a")
    _insert_run(obs_store, "run_a", "success", finished=True)  # own cleanup never ran

    # The run under the NEW (post-patch) hash never mentions "hashA" — the
    # sweep must still find and reclaim it by scanning the whole root.
    deleted = sweep_orphan_spills(
        str(root), current_run_id="run_b", keep_on_failure=True, obs_store=obs_store
    )

    assert not (root / "hashA" / "run_a").exists()
    assert not (root / "hashA").exists()
    assert any("hashA" in d for d in deleted)


def test_sweep_respects_keep_on_failure_in_a_prior_hash_directory(tmp_path, obs_store):
    """The root-level rewrite must not over-reclaim: a kept failure under an
    OLD hash is exactly as protected as one under the current hash."""
    root = tmp_path / "handoff"
    _make_spill(root, "hashA", "run_a_failed")
    _insert_run(obs_store, "run_a_failed", "error", finished=True)

    deleted = sweep_orphan_spills(
        str(root), current_run_id="run_b", keep_on_failure=True, obs_store=obs_store
    )

    assert (root / "hashA" / "run_a_failed").exists()
    assert (root / "hashA").exists()
    assert deleted == []


def test_sweep_scans_multiple_hash_directories_independently(tmp_path, obs_store):
    """Two different Blueprints (or two heals of the same one) sharing one
    handoff.root each get their own hash directory — the sweep must decide
    each run_id's fate independently rather than scoping to one hash.

    The two runs belong to DIFFERENT blueprints on purpose: the release rule
    below reclaims a kept failure once a LATER run of the SAME blueprint has
    succeeded, and this test is about hash-directory independence, not about
    that rule."""
    root = tmp_path / "handoff"
    _make_spill(root, "hashA", "run_a_success")
    _insert_run(obs_store, "run_a_success", "success", finished=True, blueprint_id="bp_a")
    _make_spill(root, "hashB", "run_b_failed")
    _insert_run(obs_store, "run_b_failed", "error", finished=True, blueprint_id="bp_b")

    deleted = sweep_orphan_spills(
        str(root), current_run_id="run_c", keep_on_failure=True, obs_store=obs_store
    )

    assert not (root / "hashA" / "run_a_success").exists()
    assert not (root / "hashA").exists()
    assert (root / "hashB" / "run_b_failed").exists()
    assert (root / "hashB").exists()


# ── Release-on-success: the missing counterpart to keep_on_failure ───────────
#
# `keep_on_failure` was an acquisition with no release event: nothing ever
# deleted a kept spill, and a heal then moved `manifest_hash` so no future run
# could even reach it, while the sweep's `status == 'error'` exemption stayed
# in force forever. The release is a LATER run of the SAME blueprint that
# SUCCEEDED — deterministic and action-based. `finished_at` orders two rows;
# it is never read as a duration, and no threshold, window, or clock exists
# anywhere in the rule.


def test_sweep_reclaims_a_kept_failure_once_a_later_run_succeeded(tmp_path, obs_store):
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_failed")
    _insert_run(obs_store, "run_failed", "error", finished=True, minutes_ago=10)
    # A later run of the same blueprint succeeded — the failure is resolved,
    # so no future rerun will ever resume from that spill.
    _insert_run(obs_store, "run_later_ok", "success", finished=True, minutes_ago=2)

    deleted = sweep_orphan_spills(
        str(root), current_run_id="run_current", keep_on_failure=True, obs_store=obs_store
    )

    assert not (root / "hash1" / "run_failed").exists()
    assert any("run_failed" in d for d in deleted)


def test_sweep_keeps_a_kept_failure_when_the_only_success_came_BEFORE_it(tmp_path, obs_store):
    """Positive control on the ordering half of the rule: a success that
    finished EARLIER than the failure resolves nothing. Same two rows as the
    test above, with the timestamps swapped — if this passes as well as that
    one, the comparison is genuinely load-bearing."""
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_failed")
    _insert_run(obs_store, "run_failed", "error", finished=True, minutes_ago=2)
    _insert_run(obs_store, "run_earlier_ok", "success", finished=True, minutes_ago=10)

    deleted = sweep_orphan_spills(
        str(root), current_run_id="run_current", keep_on_failure=True, obs_store=obs_store
    )

    assert (root / "hash1" / "run_failed").exists()
    assert deleted == []


def test_sweep_keeps_a_kept_failure_when_the_later_success_is_a_DIFFERENT_blueprint(
    tmp_path, obs_store
):
    """Positive control on the blueprint half of the rule: an unrelated
    Blueprint succeeding on a shared `handoff.root` says nothing about this
    Blueprint's failure."""
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_failed")
    _insert_run(
        obs_store, "run_failed", "error", finished=True, minutes_ago=10, blueprint_id="bp_a"
    )
    _insert_run(
        obs_store, "run_other_ok", "success", finished=True, minutes_ago=2, blueprint_id="bp_b"
    )

    deleted = sweep_orphan_spills(
        str(root), current_run_id="run_current", keep_on_failure=True, obs_store=obs_store
    )

    assert (root / "hash1" / "run_failed").exists()
    assert deleted == []


def test_sweep_keeps_a_kept_failure_when_later_heal_iterations_also_FAILED(tmp_path, obs_store):
    """A multi-patch heal mints one terminal `run_records` row per iteration.
    Those rows are `error`, and a failed heal attempt is not a resolution —
    the rule must not degrade into "superseded by N later terminal runs",
    which would reclaim the spill minutes after keeping it, while the heal
    that needs it is still running."""
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_failed")
    _insert_run(obs_store, "run_failed", "error", finished=True, minutes_ago=10)
    _insert_run(obs_store, "heal_iter_1", "error", finished=True, minutes_ago=8)
    _insert_run(obs_store, "heal_iter_2", "error", finished=True, minutes_ago=6)
    _insert_run(obs_store, "heal_iter_3", "error", finished=True, minutes_ago=4)

    deleted = sweep_orphan_spills(
        str(root), current_run_id="run_current", keep_on_failure=True, obs_store=obs_store
    )

    assert (root / "hash1" / "run_failed").exists()
    assert deleted == []


def test_sweep_reclaims_a_kept_failure_after_a_PATCHED_run(tmp_path, obs_store):
    """`Surveyor.record()` stamps `patched` (never `success`) on the iteration
    that succeeded after a heal applied a patch — the most common way an
    Aqueduct failure is resolved. A rule that accepted only `success` would
    leave every healed failure's spill unreleased forever, which is the leak
    this whole rule exists to close."""
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_failed")
    _insert_run(obs_store, "run_failed", "error", finished=True, minutes_ago=10)
    _insert_run(obs_store, "heal_iter_1", "error", finished=True, minutes_ago=8)
    _insert_run(obs_store, "heal_iter_2", "patched", finished=True, minutes_ago=6)

    deleted = sweep_orphan_spills(
        str(root), current_run_id="run_current", keep_on_failure=True, obs_store=obs_store
    )

    assert not (root / "hash1" / "run_failed").exists()
    assert any("run_failed" in d for d in deleted)


def test_sweep_keeps_a_kept_failure_when_the_later_success_never_FINISHED(tmp_path, obs_store):
    """A still-running (or crashed) later run has no `finished_at` and cannot
    order against anything. It is not a resolution."""
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_failed")
    _insert_run(obs_store, "run_failed", "error", finished=True, minutes_ago=10)
    _insert_run(obs_store, "run_live", "success", finished=False)

    deleted = sweep_orphan_spills(
        str(root), current_run_id="run_current", keep_on_failure=True, obs_store=obs_store
    )

    assert (root / "hash1" / "run_failed").exists()
    assert deleted == []


def test_sweep_keeps_a_kept_failure_when_the_store_cannot_be_queried(tmp_path):
    """Best-effort, fail-safe direction: a store whose query raises must
    never be read as "a later success exists". The spill stays."""
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_failed")

    class _Broken:
        """Answers the sweep's own status probe (terminal `error`) but blows
        up on the supersession query — isolating the release predicate's
        failure mode from the sweep's."""

        def __init__(self):
            self.calls = 0

        def connect(self):
            import contextlib

            self.calls += 1
            if self.calls == 1:

                @contextlib.contextmanager
                def _ok():
                    class _Cur:
                        def execute(self, *a, **k):
                            return None

                        def fetchone(self):
                            return ("error", datetime.now(tz=UTC))

                    yield _Cur()

                return _ok()
            raise RuntimeError("observability store unreachable")

    broken = _Broken()
    deleted = sweep_orphan_spills(
        str(root), current_run_id="run_current", keep_on_failure=True, obs_store=broken
    )

    assert (root / "hash1" / "run_failed").exists()
    assert deleted == []
    assert broken.calls >= 2, "the supersession query must actually have been attempted"
