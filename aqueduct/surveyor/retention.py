"""Observability-store retention — throttled auto-prune + explicit deep clean.

Phase 85 B1. Two distinct operations, deliberately kept separate:

  * ``prune_store()`` — age-based ``DELETE``s against every governed table,
    keyed off each table's own timestamp column and the windows configured
    in ``aqueduct.yml``'s ``observability.retention:`` block
    (``aqueduct.config.ObservabilityRetentionConfig``). Cheap, safe to call
    on every run. Never reclaims disk space.
  * ``vacuum_store()`` — issues the backend's actual space-reclaim
    operation (DuckDB ``VACUUM``; a no-op on Postgres, whose autovacuum
    already handles this — see ``docs/observability_guide.md``). **NEVER
    called automatically.** Wired only to the explicit ``aqueduct report
    prune --vacuum`` CLI verb (owned by the CLI worker, not this module).

``maybe_prune_store()`` is the automatic half: called at the end of every
``Surveyor.record()``, it checks a ``store_maintenance`` marker row (one
indexed ``SELECT`` by primary key) and only calls ``prune_store()`` when at
least a day has passed since the last prune for THIS store — so the common
case (a store pruned earlier today) costs one cheap lookup, not a sweep of
every table on every run.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from aqueduct.surveyor.ddl import _STORE_MAINTENANCE_DDL

if TYPE_CHECKING:
    from aqueduct.config import ObservabilityRetentionConfig
    from aqueduct.stores.base import ObservabilityStore

logger = logging.getLogger(__name__)

_THROTTLE = timedelta(days=1)

# (table, timestamp_column, is_iso_varchar) — is_iso_varchar=True means the
# column is a VARCHAR ISO-8601 string (compared lexicographically, which is
# safe for UTC ISO-8601), False means a native TIMESTAMPTZ (compared against
# a bound datetime).
_GOVERNED_TABLES: tuple[tuple[str, str, str, bool], ...] = (
    ("run_records", "started_at", "run_records_days", False),
    ("failure_contexts", "started_at", "failure_contexts_days", False),
    ("healing_outcomes", "applied_at", "healing_outcomes_days", True),
    ("heal_attempts", "recorded_at", "heal_attempts_days", True),
    ("patch_simulation", "recorded_at", "patch_simulation_days", True),
    ("column_lineage", "captured_at", "column_lineage_days", False),
    ("probe_signals", "captured_at", "probe_signals_days", False),
)


def prune_store(
    store: ObservabilityStore,
    retention: ObservabilityRetentionConfig,
    *,
    now: datetime | None = None,
) -> dict[str, int]:
    """Delete observability rows older than their configured retention window.

    One ``DELETE`` per governed table (``run_records``, ``failure_contexts``,
    ``healing_outcomes``, ``heal_attempts``, ``patch_simulation``,
    ``column_lineage``, ``probe_signals``), each keyed off that table's own
    timestamp column. A table missing entirely (very old pre-migration
    store) is skipped, not an error.

    Never calls ``VACUUM`` — freeing disk space is ``vacuum_store()``'s job.

    Args:
        store:     The ``ObservabilityStore`` backend (DuckDB or Postgres).
        retention: Resolved ``ObservabilityRetentionConfig``.
        now:       Injectable clock for tests; defaults to real UTC now.

    Returns:
        ``{table_name: rows_deleted}`` for every table actually pruned —
        used by the CLI verb to report what happened and by tests to assert
        on the sweep without depending on wall-clock timing.
    """
    _now = now or datetime.now(tz=UTC)
    deleted: dict[str, int] = {}
    with store.connect() as cur:
        for table, ts_col, days_field, is_iso in _GOVERNED_TABLES:
            days = getattr(retention, days_field)
            cutoff_dt = _now - timedelta(days=days)
            cutoff: Any = cutoff_dt.isoformat() if is_iso else cutoff_dt
            try:
                cur.execute(
                    f"DELETE FROM {table} WHERE {ts_col} < ?",  # noqa: S608 — table/col from a fixed internal tuple, never user input
                    [cutoff],
                )
                # Backend-specific count reporting: psycopg2 (Postgres)
                # populates DB-API `.rowcount` correctly for DELETE; DuckDB's
                # driver always reports rowcount=-1 for DELETE but returns a
                # one-row/one-column result set with the count instead —
                # fall back to that when rowcount isn't a real count.
                rc = getattr(cur, "rowcount", -1)
                if not (isinstance(rc, int) and rc >= 0):
                    try:
                        row = cur.fetchone()
                        rc = int(row[0]) if row else 0
                    except Exception:
                        rc = 0
                deleted[table] = rc
            except Exception as exc:
                logger.debug("prune_store: %s skipped (%s)", table, exc)
    return deleted


def vacuum_store(store: ObservabilityStore) -> None:
    """Reclaim disk space freed by ``prune_store()``'s deletes.

    **NEVER called automatically** — this is the deep-clean half of the
    ``aqueduct report-prune --vacuum`` CLI verb only. DuckDB: issues
    ``VACUUM``. Postgres: no-op (TOAST + autovacuum already reclaim space;
    see ``docs/observability_guide.md``'s "Blob externalisation" note for
    the parallel reasoning on why Postgres doesn't need the externalisation
    DuckDB does).

    Raises nothing — a failed VACUUM (e.g. DuckDB's exclusive-lock
    requirement colliding with a concurrent reader) is logged and swallowed;
    the caller can retry, but a vacuum failure must never look like data
    loss or abort an otherwise-successful CLI invocation.
    """
    if store.backend != "duckdb":
        return
    try:
        with store.connect() as cur:
            cur.execute("VACUUM")
    except Exception as exc:
        logger.warning("vacuum_store: VACUUM failed (%s)", exc)


def maybe_prune_store(
    store: ObservabilityStore,
    retention: ObservabilityRetentionConfig,
    *,
    now: datetime | None = None,
) -> bool:
    """Run ``prune_store()`` if (and only if) it hasn't run today for this store.

    Throttle check is a single indexed ``SELECT`` against
    ``store_maintenance`` (primary-keyed on ``key='global'``) — cheap on the
    overwhelming majority of calls, which no-op. Called from
    ``Surveyor.record()`` at the end of every run.

    Returns:
        ``True`` if a prune actually ran, ``False`` if throttled or on any
        error (best-effort — a pruning failure must never fail a run).
    """
    _now = now or datetime.now(tz=UTC)
    try:
        with store.connect() as cur:
            cur.execute(_STORE_MAINTENANCE_DDL)
            row = cur.execute(
                "SELECT last_pruned_at FROM store_maintenance WHERE key = 'global'"
            ).fetchone()
            if row is not None and row[0] is not None:
                last = row[0]
                if isinstance(last, str):
                    last = datetime.fromisoformat(last)
                if last.tzinfo is None:
                    last = last.replace(tzinfo=UTC)
                if _now - last < _THROTTLE:
                    return False
    except Exception as exc:
        logger.debug("maybe_prune_store: throttle check failed (%s)", exc)
        return False

    try:
        prune_store(store, retention, now=_now)
        with store.connect() as cur:
            cur.execute(
                """
                INSERT INTO store_maintenance (key, last_pruned_at)
                VALUES ('global', ?)
                ON CONFLICT (key) DO UPDATE SET last_pruned_at = EXCLUDED.last_pruned_at
                """,
                [_now.isoformat()],
            )
        return True
    except Exception as exc:
        logger.debug("maybe_prune_store: prune failed (%s)", exc)
        return False
