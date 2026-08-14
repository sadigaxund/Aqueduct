"""Cross-engine handoff spill directory lifecycle (Phase 81 step 3).

The engine-native WRITE and READ of a handoff's spilled parquet live in each
engine's own executor (``aqueduct/executor/spark/executor.py``,
``aqueduct/executor/duckdb_/executor.py`` — ``df.write.parquet``/
``spark.read.parquet`` on Spark, ``COPY ... TO ... (FORMAT PARQUET)``/a
parquet scan on DuckDB). This module owns everything AROUND that write/read:
where one boundary's spill directory lives, deleting it when the run that
produced it succeeds, keeping it when the run fails (the resume story — a
manual ``aqueduct run --resume <run_id>`` after a plain failure, with no
Blueprint edit in between, reads island A's already-materialized spill
instead of recomputing it. A heal-triggered rerun does NOT reuse it: a heal
patches the Manifest, which changes the whole-Manifest hash and therefore
the spill path — even when the patch touched only a downstream island —
and ``cli/run.py``'s heal-retry path passes no resume id at all once a
patch has been applied, so the two never even try to line up), and
sweeping directories orphaned by a run that never
reached its own success/failure cleanup (a driver crash, a killed process,
or — since a heal changes the compiled Manifest and therefore its
``manifest_hash`` — a PRIOR hash directory a post-patch rerun will never
recompile its way back into). ``sweep_orphan_spills`` scans the entire
``handoff.root``, across every ``manifest_hash`` subdirectory, deciding each
run_id's fate from ``run_records`` alone (never from "is this hash mine") —
see its docstring.

**Keeping a spill is an acquisition, so it needs a release.** Nothing used
to delete a kept-failure spill, ever: the sweep exempted it on
``status == 'error'`` and a heal then moved ``manifest_hash``, putting it
out of reach of every future run while the exemption stayed in force. The
release event is ACTION-based and deterministic — a later run of the SAME
blueprint that SUCCEEDED (``_superseded_by_later_success``), meaning the
failure is resolved and no rerun will ever resume from that spill. No
clock, no retention window, no threshold: ``finished_at`` orders two rows
and is never read as a duration.

Directory layout: ``<root>/<manifest_hash>/<run_id>/<edge_id>/`` — see
``aqueduct.compiler.handoff`` for where ``edge_id`` comes from and
``docs/specs.md`` §10.4.3 for the config surface (``handoff.root``,
``handoff.keep_on_failure``).

**Two IO stacks touch one directory.** The ENGINES write the spill
(engine-native, no fsspec — Spark and DuckDB both already speak local and
remote URIs natively, which is what makes step 2 cluster-ready with no new
backend abstraction). AQUEDUCT's OWN cleanup (this module) uses ``fsspec``
for a remote root, because unlike an engine's writer, this module has no
"native" way to list/delete an arbitrary URI scheme. On a remote
``handoff.root`` with ``fsspec`` NOT installed, engine writes keep
succeeding while this module's cleanup silently no-ops — spill would
accumulate forever behind one debug log line if that were left quiet.
That is deliberately NOT shipped silent: ``local_only_or_fsspec_available``
below is the single predicate a caller (the orchestrator, and later a
doctor check) uses to decide whether to emit a LOUD, suppressible
``handoff_cleanup_unavailable`` warning naming the exact reason (fsspec
absent + remote root), instead of the two-stacks divergence draining disk
quietly. Chosen over "have the engine delete its own spill" because that
would duplicate cleanup logic per engine (two more code paths to keep in
sync) for a problem this module already centralizes into one place; a
missing optional dependency should be a loud, one-time, actionable warning,
not a second write-path per engine.
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Any

from aqueduct.executor.models import SUCCEEDED_STATUSES
from aqueduct.executor.path_keys import CLOUD_SCHEMES

logger = logging.getLogger(__name__)

RULE_ID_HANDOFF_CLEANUP_UNAVAILABLE = "handoff_cleanup_unavailable"

# Same remote-scheme vocabulary the rest of the codebase already uses
# (`aqueduct.executor.path_keys.CLOUD_SCHEMES`) plus the generic RFC3986
# scheme shape, so a scheme this tuple doesn't enumerate by name (e.g. a
# custom fsspec protocol) is still recognized as "not a local path".
_LOCAL_SCHEME_PREFIXES: tuple[str, ...] = ("file://",)


def is_remote_uri(uri: str) -> bool:
    """True when *uri* is not a plain local filesystem path."""
    if uri.startswith(_LOCAL_SCHEME_PREFIXES):
        return False
    if any(uri.startswith(s) for s in CLOUD_SCHEMES):
        return True
    return "://" in uri


def _fsspec_available() -> bool:
    try:
        import fsspec  # noqa: F401
    except ImportError:
        return False
    return True


def local_only_or_fsspec_available(root: str) -> bool:
    """True when cleanup CAN work for *root*: it's a local path, or it's
    remote and ``fsspec`` is installed. False is exactly the silent-divergence
    condition callers must warn loudly about."""
    return not is_remote_uri(root) or _fsspec_available()


def spill_dir_for(root: str, manifest_hash: str, run_id: str, edge_id: str) -> str:
    """``<root>/<manifest_hash>/<run_id>/<edge_id>`` — works for a local path
    or a URI root (plain string join; every scheme here uses ``/`` as its
    path separator, including Windows-hostile ones we don't support anyway).
    """
    root = root.rstrip("/")
    return f"{root}/{manifest_hash}/{run_id}/{edge_id}"


def _local_path(uri: str) -> Path:
    return Path(uri.removeprefix("file://"))


def ensure_parent_exists(uri: str) -> None:
    """Best-effort local directory creation. A no-op for a remote URI — the
    engine's own writer creates what it needs (S3/GCS have no directories to
    pre-create; HDFS creates them on write)."""
    if is_remote_uri(uri):
        return
    _local_path(uri).mkdir(parents=True, exist_ok=True)


def delete_spill_tree(uri: str) -> bool:
    """Delete one spill directory (a single boundary's ``<edge_id>/``, or a
    whole ``<run_id>/`` directory of them). Returns True if deletion was
    attempted and did not raise; False when it could not even be attempted
    (remote + no fsspec) — callers use False to decide whether to warn.
    """
    if not is_remote_uri(uri):
        try:
            shutil.rmtree(_local_path(uri), ignore_errors=True)
        except Exception as exc:
            logger.debug("delete_spill_tree: local rmtree failed for %r: %s", uri, exc)
        return True
    if not _fsspec_available():
        logger.info(
            "[%s] handoff spill at %r is on a remote root and fsspec is not "
            "installed — cannot delete it. Install aqueduct-core[object-store] "
            "(or any fsspec-backed extra) to enable cleanup.",
            RULE_ID_HANDOFF_CLEANUP_UNAVAILABLE,
            uri,
        )
        return False
    try:
        import fsspec

        fs, path = fsspec.core.url_to_fs(uri)
        if fs.exists(path):
            fs.rm(path, recursive=True)
    except Exception as exc:
        logger.debug("delete_spill_tree: fsspec rm failed for %r: %s", uri, exc)
    return True


def dir_size_bytes(uri: str) -> int | None:
    """Total bytes under *uri*, or None when it cannot be measured (remote
    root, fsspec absent, or the directory doesn't exist / isn't readable
    yet). Engine-agnostic — used for the handoff module's `bytes_written`/
    `bytes_read` metric regardless of which engine performed the write."""
    if not is_remote_uri(uri):
        p = _local_path(uri)
        if not p.exists():
            return None
        total = 0
        for dirpath, _dirnames, filenames in os.walk(p):
            for fname in filenames:
                try:
                    total += os.path.getsize(os.path.join(dirpath, fname))
                except OSError:
                    continue
        return total
    if not _fsspec_available():
        return None
    try:
        import fsspec

        fs, path = fsspec.core.url_to_fs(uri)
        if not fs.exists(path):
            return None
        return int(fs.du(path))
    except Exception as exc:
        logger.debug("dir_size_bytes: could not measure %r: %s", uri, exc)
        return None


def _list_run_dirs(root: str, manifest_hash: str) -> list[str]:
    """Every ``run_id`` subdirectory name under ``<root>/<manifest_hash>/``."""
    base = f"{root.rstrip('/')}/{manifest_hash}"
    if not is_remote_uri(base):
        p = Path(base)
        if not p.exists():
            return []
        return [child.name for child in p.iterdir() if child.is_dir()]
    if not _fsspec_available():
        logger.info(
            "[%s] cannot list handoff spill runs under remote root %r — "
            "fsspec is not installed. Orphan sweep skipped for this root.",
            RULE_ID_HANDOFF_CLEANUP_UNAVAILABLE,
            base,
        )
        return []
    try:
        import fsspec

        fs, path = fsspec.core.url_to_fs(base)
        if not fs.exists(path):
            return []
        return [os.path.basename(p.rstrip("/")) for p in fs.ls(path, detail=False)]
    except Exception as exc:
        logger.debug("_list_run_dirs: fsspec ls failed for %r: %s", base, exc)
        return []


def _list_hash_dirs(root: str) -> list[str]:
    """Every ``manifest_hash`` subdirectory name directly under ``<root>/``.

    A heal that changes the compiled Manifest changes ``manifest_hash``
    (``executor.models.manifest_hash`` hashes the WHOLE Manifest), so
    consecutive runs of the same Blueprint across a patch write under
    DIFFERENT hash directories. Sweeping this list — every hash directory,
    not just the current run's — is what lets the orphan sweep below reclaim
    a hash directory a prior, now-superseded compile will never revisit.
    """
    if not is_remote_uri(root):
        p = Path(root)
        if not p.exists():
            return []
        return [child.name for child in p.iterdir() if child.is_dir()]
    if not _fsspec_available():
        logger.info(
            "[%s] cannot list handoff spill hash directories under remote "
            "root %r — fsspec is not installed. Orphan sweep skipped for "
            "this root.",
            RULE_ID_HANDOFF_CLEANUP_UNAVAILABLE,
            root,
        )
        return []
    try:
        import fsspec

        fs, path = fsspec.core.url_to_fs(root)
        if not fs.exists(path):
            return []
        return [os.path.basename(p.rstrip("/")) for p in fs.ls(path, detail=False)]
    except Exception as exc:
        logger.debug("_list_hash_dirs: fsspec ls failed for %r: %s", root, exc)
        return []


def _dir_is_empty(uri: str) -> bool:
    """True when *uri* exists and has no children — used to decide whether a
    manifest-hash directory can be reclaimed once every run underneath it
    has been swept. False (not empty / doesn't exist / can't be listed) is
    always the safe answer — the caller only deletes on True."""
    if not is_remote_uri(uri):
        p = _local_path(uri)
        return p.exists() and p.is_dir() and not any(p.iterdir())
    if not _fsspec_available():
        return False
    try:
        import fsspec

        fs, path = fsspec.core.url_to_fs(uri)
        if not fs.exists(path):
            return False
        return len(fs.ls(path, detail=False)) == 0
    except Exception as exc:
        logger.debug("_dir_is_empty: could not check %r: %s", uri, exc)
        return False


def _run_status(obs_store: Any, run_id: str) -> tuple[str | None, bool]:
    """``(status, is_terminal)`` for *run_id* from ``run_records`` — best
    effort. ``status`` is None when no row exists at all (an unknown run,
    conservatively treated as safe to reclaim by the caller). ``is_terminal``
    is False for a row with no ``finished_at`` (still running, or a crashed
    run that never got a terminal status written) — callers must not delete
    a non-terminal run's spill.
    """
    if obs_store is None:
        return None, False
    try:
        with obs_store.connect() as cur:
            cur.execute(
                "SELECT status, finished_at FROM run_records WHERE run_id = ?",
                [run_id],
            )
            row = cur.fetchone()
    except Exception as exc:
        logger.debug("_run_status: query failed for %r: %s", run_id, exc)
        return None, False
    if row is None:
        return None, False
    status, finished_at = row[0], row[1]
    return status, finished_at is not None


def _superseded_by_later_success(obs_store: Any, run_id: str) -> bool:
    """True when a LATER run of the SAME blueprint has succeeded since
    *run_id* failed — the release event for a kept-failure spill.

    ``keep_on_failure`` is an acquisition with no release: nothing else in
    the system ever deletes a spill kept for a failure, and the sweep's
    ``status == "error"`` exemption keeps exempting it forever, so disk
    grows without bound. This predicate supplies the missing release. It is
    deliberately ACTION-based, not time-based: the sweep only runs at the
    start of a run, so a wall-clock retention window would be an
    action-trigger wearing a fuzzy predicate, and no measurement exists that
    would justify a number of days anyway.

    The comparison lives in SQL so the ``finished_at`` ordering is done by
    whichever backend owns the TIMESTAMPTZ column, not by Python against
    values whose tz-awareness varies by driver. ``finished_at`` is used for
    ORDERING TWO ROWS ONLY — there is no duration, threshold, or clock
    anywhere in this rule.

    Why "a later SUCCESS" and not "N later terminal runs": a multi-patch
    heal mints a terminal ``run_records`` row per iteration, so a count
    would reclaim the spill minutes after keeping it, while the heal that
    needs it is still running. A failed heal iteration is an ``error`` row
    and correctly does not count.

    ``SUCCEEDED_STATUSES`` (not just ``"success"``) is load-bearing:
    ``Surveyor.record()`` stamps ``patched`` on the iteration that succeeded
    after a patch was applied, which is the most common way a failure gets
    resolved. Accepting only ``"success"`` would leave every healed failure's
    spill permanently unreleased — the exact leak this rule closes.

    ``run_records`` carries no ``manifest_hash`` column, so "this spill is
    unreachable because a heal moved the hash" is not computable from the
    store. This rule does not need it: a later success means no future
    rerun will resume from this failure, whatever hash it sits under.

    Best effort — a store that cannot be queried returns False, i.e. keep
    the spill. Never delete on a failed lookup.
    """
    if obs_store is None:
        return False
    placeholders = ", ".join("?" for _ in SUCCEEDED_STATUSES)
    try:
        with obs_store.connect() as cur:
            cur.execute(
                f"""
                SELECT 1
                FROM run_records failed
                JOIN run_records later
                  ON later.blueprint_id = failed.blueprint_id
                WHERE failed.run_id = ?
                  AND failed.finished_at IS NOT NULL
                  AND later.finished_at IS NOT NULL
                  AND later.status IN ({placeholders})
                  AND later.finished_at > failed.finished_at
                LIMIT 1
                """,
                [run_id, *SUCCEEDED_STATUSES],
            )
            row = cur.fetchone()
    except Exception as exc:
        logger.debug("_superseded_by_later_success: query failed for %r: %s", run_id, exc)
        return False
    return row is not None


def sweep_orphan_spills(
    root: str,
    *,
    current_run_id: str,
    keep_on_failure: bool,
    obs_store: Any = None,
) -> list[str]:
    """Delete spill directories of runs that are neither live nor
    kept-failures. Run at the START of a new run, before that run's own
    spill exists on disk, so ``current_run_id`` never appears among the
    candidates (nothing to special-case).

    Scans the ENTIRE ``root``, across every ``manifest_hash`` subdirectory,
    not just the hash the current run happens to compile to. A heal changes
    the compiled Manifest, which changes ``manifest_hash``, so a run after a
    patch writes under a brand-new hash directory and a sweep scoped to only
    that hash would never revisit the pre-patch hash directory again —
    silently abandoning it (and every kept-failure spill under it) forever,
    since reaching it would require recompiling the exact pre-patch
    Manifest. `run_records` is keyed by ``run_id`` alone (never by hash), so
    a run_id's eligibility is decided the same way regardless of which hash
    directory it happens to sit under — including on a ``root`` shared by
    several different Blueprints, each with its own hash: the decision is
    "what does `run_records` say about this run_id", never "is this hash
    mine".

    A run_id's ``<root>/<hash>/<run_id>/`` directory is deleted unless
    ``run_records`` says it is still non-terminal (possibly still running —
    never delete out from under a live process) or it is a terminal FAILURE
    that ``keep_on_failure`` protects AND no later run of the same blueprint
    has succeeded yet (the resume story, see
    ``_superseded_by_later_success``). Everything else — a successful run
    whose own cleanup never ran, a failed run when ``keep_on_failure`` is
    false, a failed run a later success has already resolved, or a run_id
    with no ``run_records`` row at all (unknown — nothing positively says it
    is still needed) — is reclaimed. A ``<hash>/`` directory left empty once
    every run underneath it has been swept is reclaimed too, so a superseded
    hash directory doesn't linger as an empty shell forever.

    Two consequences of the release rule, both stated rather than papered
    over:

    * A blueprint that fails and is never run again keeps its spill
      forever. Nothing here can reclaim it, because nothing here ever
      runs again for that blueprint. Closing that gap needs an explicit
      operator-invoked command, not a clock.
    * A failure being actively debugged loses its spill if an unrelated
      scheduled run of the same blueprint succeeds in the meantime. Whether
      that should be prevented (by exempting the most recent failure per
      blueprint, by an opt-out, or not at all) is an OPEN QUESTION with no
      decision taken — this function implements plain release-on-success
      and does not pretend to answer it.

    Returns the list of deleted directory paths (best-effort; a remote root
    with no fsspec returns ``[]`` and the caller is expected to have already
    surfaced ``local_only_or_fsspec_available``'s warning).
    """
    if not local_only_or_fsspec_available(root):
        return []

    deleted: list[str] = []
    for manifest_hash in _list_hash_dirs(root):
        hash_dir = f"{root.rstrip('/')}/{manifest_hash}"
        for run_id in _list_run_dirs(root, manifest_hash):
            if run_id == current_run_id:
                continue
            status, is_terminal = _run_status(obs_store, run_id)
            # `status is None` means no `run_records` row exists at all —
            # distinct from a row that exists but is non-terminal (still
            # running). Only the latter is protected; a run_id with no row
            # is "unknown" and reclaimed per this function's own contract
            # (see docstring above) — conflating the two here previously
            # left every unknown run_id untouched forever, which is the
            # exact silent-accumulation failure mode this sweep exists to
            # prevent.
            if status is not None and not is_terminal:
                continue  # still running — never touch
            if status == "error" and keep_on_failure:
                # A kept failure is kept only until it is RESOLVED. A later
                # succeeded run of the same blueprint is that resolution:
                # no future rerun will resume from this failure, so the
                # spill's whole documented purpose has expired. Without
                # this release the `keep_on_failure` exemption is
                # permanent and disk grows without bound.
                if not _superseded_by_later_success(obs_store, run_id):
                    continue  # kept failure, not yet superseded — the resume story
            path = f"{hash_dir}/{run_id}"
            if delete_spill_tree(path):
                deleted.append(path)
        # Every run under this hash directory has now been decided. If none
        # of them survived (including `current_run_id`'s — impossible here,
        # since the sweep runs before the current run's own spill exists),
        # the hash directory itself is a superseded, now-empty shell — the
        # thing this whole root-level rewrite exists to stop leaking.
        if _dir_is_empty(hash_dir):
            if delete_spill_tree(hash_dir):
                deleted.append(hash_dir)
    return deleted


__all__ = [
    "RULE_ID_HANDOFF_CLEANUP_UNAVAILABLE",
    "delete_spill_tree",
    "dir_size_bytes",
    "ensure_parent_exists",
    "is_remote_uri",
    "local_only_or_fsspec_available",
    "spill_dir_for",
    "sweep_orphan_spills",
]
