"""Unit tests for ``aqueduct.executor.spill`` (Phase 81 step 3) — the
handoff spill directory lifecycle: URI classification, directory layout,
deletion, size measurement, and the orphan sweep. Pure filesystem + a real
local DuckDB observability store for the ``run_records``-driven sweep tests
— no Spark/DuckDB *execution* engine involved, so this stays ``unit``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from aqueduct.executor.spill import (
    delete_spill_tree,
    dir_size_bytes,
    ensure_parent_exists,
    find_run_under_other_hash,
    is_remote_uri,
    local_only_or_fsspec_available,
    parse_duration,
    plan_orphan_sweep,
    spill_dir_for,
    sweep_orphan_spills,
)
from aqueduct.surveyor.ddl import _DDL

pytestmark = pytest.mark.unit

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


# ── find_run_under_other_hash (P4: --resume fail-closed detector) ───────────


def test_find_run_under_other_hash_finds_stale_hash(tmp_path):
    root = tmp_path / "handoff"
    (root / "old_hash" / "r1" / "edge1").mkdir(parents=True)
    (root / "new_hash").mkdir(parents=True)

    assert find_run_under_other_hash(str(root), "r1", "new_hash") == "old_hash"


def test_find_run_under_other_hash_none_when_only_under_current_hash(tmp_path):
    root = tmp_path / "handoff"
    (root / "new_hash" / "r1" / "edge1").mkdir(parents=True)

    assert find_run_under_other_hash(str(root), "r1", "new_hash") is None


def test_find_run_under_other_hash_none_when_run_id_not_found_anywhere(tmp_path):
    root = tmp_path / "handoff"
    (root / "some_hash" / "r_other" / "edge1").mkdir(parents=True)

    assert find_run_under_other_hash(str(root), "r1", "new_hash") is None


def test_find_run_under_other_hash_none_when_root_does_not_exist(tmp_path):
    root = tmp_path / "does-not-exist"
    assert find_run_under_other_hash(str(root), "r1", "new_hash") is None


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


# ── plan_orphan_sweep / dry_run — the `aqueduct handoff sweep` preview path ────


def test_dry_run_deletes_nothing_but_lists_the_same_candidates(tmp_path, obs_store):
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_success")
    _insert_run(obs_store, "run_success", "success", finished=True)

    previewed = sweep_orphan_spills(
        str(root),
        current_run_id="run_current",
        keep_on_failure=True,
        obs_store=obs_store,
        dry_run=True,
    )

    assert (root / "hash1" / "run_success").exists()  # nothing deleted
    assert (root / "hash1").exists()
    assert previewed == plan_orphan_sweep_paths(root, obs_store)


def plan_orphan_sweep_paths(root, obs_store):
    return [
        c.path
        for c in plan_orphan_sweep(
            str(root), current_run_id="run_current", keep_on_failure=True, obs_store=obs_store
        )
    ]


def test_plan_orphan_sweep_never_touches_a_still_running_run(tmp_path, obs_store):
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_live")
    _insert_run(obs_store, "run_live", "success", finished=False)

    candidates = plan_orphan_sweep(
        str(root), current_run_id="run_current", keep_on_failure=True, obs_store=obs_store
    )
    assert candidates == []


def test_plan_orphan_sweep_reports_a_reclaimable_hash_dir_only_when_every_run_is_reclaimed(
    tmp_path, obs_store
):
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_reclaim")
    _make_spill(root, "hash1", "run_live")
    _insert_run(obs_store, "run_reclaim", "success", finished=True)
    _insert_run(obs_store, "run_live", "success", finished=False)

    candidates = plan_orphan_sweep(
        str(root), current_run_id="run_current", keep_on_failure=True, obs_store=obs_store
    )
    run_ids = {c.run_id for c in candidates}
    assert run_ids == {"run_reclaim"}  # run_live protects the hash dir from reclaim
    assert not any(c.run_id is None for c in candidates)


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

    sweep_orphan_spills(
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


def test_sweep_reclaims_even_the_MOST_RECENT_failure_after_a_later_success(tmp_path, obs_store):
    """The accepted trade-off, pinned as behaviour.

    A pipeline fails Monday, an operator is still investigating Tuesday, and an
    unrelated scheduled run succeeds Monday night. The spill goes. That outcome
    is DECIDED and accepted: no most-recent-failure exemption, no opt-out knob.
    The hazard is unmeasured, and every surface an operator debugs from — the
    `run_records` row, the `failure_contexts` row, the stack trace — is a store
    record the sweep never touches. A handoff spill is an intermediate parquet
    materialisation, so losing it costs a rerun, not a diagnosis.

    The scenario is two failures of ONE blueprint at different ages plus one
    later success. If an exemption for "the failure most likely under
    investigation" were ever added, `run_failed_recent` would survive and this
    goes red naming it.
    """
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_failed_old")
    _make_spill(root, "hash1", "run_failed_recent")
    _insert_run(obs_store, "run_failed_old", "error", finished=True, minutes_ago=30)
    _insert_run(obs_store, "run_failed_recent", "error", finished=True, minutes_ago=10)
    _insert_run(obs_store, "run_later_ok", "success", finished=True, minutes_ago=2)

    deleted = sweep_orphan_spills(
        str(root), current_run_id="run_current", keep_on_failure=True, obs_store=obs_store
    )

    assert not (root / "hash1" / "run_failed_recent").exists()
    assert not (root / "hash1" / "run_failed_old").exists()
    # Both run dirs plus the hash dir they emptied.
    assert len(deleted) == 3


def test_sweep_keeps_BOTH_failures_when_no_later_success_exists(tmp_path, obs_store):
    """Positive control for the test above: the identical two-failure fixture
    with the success row removed. Without this, that test would pass just as
    happily against a sweep that reclaimed every kept failure unconditionally —
    which is the opposite of the rule, not evidence for it."""
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_failed_old")
    _make_spill(root, "hash1", "run_failed_recent")
    _insert_run(obs_store, "run_failed_old", "error", finished=True, minutes_ago=30)
    _insert_run(obs_store, "run_failed_recent", "error", finished=True, minutes_ago=10)

    deleted = sweep_orphan_spills(
        str(root), current_run_id="run_current", keep_on_failure=True, obs_store=obs_store
    )

    assert (root / "hash1" / "run_failed_recent").exists()
    assert (root / "hash1" / "run_failed_old").exists()
    assert deleted == []


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


# ── parse_duration / --older-than reclaim (Phase 89 item 4) ─────────────────


@pytest.mark.parametrize(
    "text,expected",
    [
        ("7d", timedelta(days=7)),
        ("24h", timedelta(hours=24)),
        ("90m", timedelta(minutes=90)),
        ("1d", timedelta(days=1)),
    ],
)
def test_parse_duration_accepts_the_documented_shapes(text, expected):
    assert parse_duration(text) == expected


@pytest.mark.parametrize("junk", ["", "7", "d", "7 d", "7days", "-7d", "7.5d", "7dd", "seven days"])
def test_parse_duration_rejects_junk(junk):
    with pytest.raises(ValueError, match="invalid duration"):
        parse_duration(junk)


def test_sweep_older_than_reclaims_a_kept_failure_past_the_cutoff(tmp_path, obs_store):
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_old_failed")
    _insert_run(obs_store, "run_old_failed", "error", finished=True, minutes_ago=60 * 24 * 30)

    deleted = sweep_orphan_spills(
        str(root),
        current_run_id="run_current",
        keep_on_failure=True,
        obs_store=obs_store,
        older_than=timedelta(days=7),
    )

    assert not (root / "hash1" / "run_old_failed").exists()
    assert any("run_old_failed" in d for d in deleted)


def test_sweep_older_than_spares_a_kept_failure_within_the_cutoff(tmp_path, obs_store):
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_young_failed")
    _insert_run(obs_store, "run_young_failed", "error", finished=True, minutes_ago=5)

    deleted = sweep_orphan_spills(
        str(root),
        current_run_id="run_current",
        keep_on_failure=True,
        obs_store=obs_store,
        older_than=timedelta(days=7),
    )

    assert (root / "hash1" / "run_young_failed").exists()
    assert deleted == []


def test_sweep_without_older_than_is_byte_identical_to_before_the_flag_existed(tmp_path, obs_store):
    """Positive control: the SAME old-failure fixture with `older_than=None`
    (the default) must leave it exactly as untouched as the supersession
    rule alone always left it — proving `older_than` is additive, never a
    change to the default path."""
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_old_failed")
    _insert_run(obs_store, "run_old_failed", "error", finished=True, minutes_ago=60 * 24 * 30)

    deleted = sweep_orphan_spills(
        str(root), current_run_id="run_current", keep_on_failure=True, obs_store=obs_store
    )

    assert (root / "hash1" / "run_old_failed").exists()
    assert deleted == []


def test_plan_orphan_sweep_older_than_marks_reclaimed_by_age(tmp_path, obs_store):
    root = tmp_path / "handoff"
    _make_spill(root, "hash1", "run_old_failed")
    _insert_run(obs_store, "run_old_failed", "error", finished=True, minutes_ago=60 * 24 * 30)

    candidates = plan_orphan_sweep(
        str(root),
        current_run_id="run_current",
        keep_on_failure=True,
        obs_store=obs_store,
        older_than=timedelta(days=7),
    )

    by_run_id = {c.run_id: c for c in candidates}
    assert by_run_id["run_old_failed"].reclaimed_by_age is True
    # The now-empty hash dir is also reported, same as any other reclaim —
    # it is not itself "reclaimed by age" (it carries no run_id/status).
    assert by_run_id[None].reclaimed_by_age is False


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
