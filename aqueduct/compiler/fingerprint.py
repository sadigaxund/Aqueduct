"""SQL-AST normalised fingerprints for Channel modules (Phase 56, Lineage v2).

For each ``op: sql`` Channel, sqlglot canonicalises the query (whitespace,
comment, keyword-case and function-case insensitive) and hashes the result.
The hash is a *semantic* fingerprint: two queries that differ only in
formatting share a fingerprint, while a real predicate/column change produces a
new one.

Persistence is a **changelog, not a run-log** (see ``write_fingerprints``): one
row per *distinct* fingerprint per ``(blueprint_id, channel_id)``, so the table
grows with the number of times the SQL actually changed — not with the number
of runs. Zero Spark actions; runs at compile/record time on the driver.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import UTC
from typing import Any

from aqueduct.parser.models import ModuleType

logger = logging.getLogger(__name__)


def _canonicalize(sql: str) -> str:
    """Return a canonical, formatting-insensitive form of a SparkSQL string.

    Falls back to whitespace-normalised raw SQL when sqlglot cannot parse the
    query, so a fingerprint is still produced (and still changes when the SQL
    changes) for the ~10% of expressions sqlglot does not model.
    """
    try:
        import sqlglot

        expr = sqlglot.parse_one(sql, dialect="spark")
        return expr.sql(
            dialect="spark",
            comments=False,
            normalize_functions="upper",
            pretty=False,
        )
    except Exception as exc:  # parse failure or sqlglot missing
        logger.debug("Fingerprint canonicalize fell back to raw SQL: %s", exc)
        return " ".join(sql.split())


def compute_channel_fingerprints(modules: tuple[Any, ...]) -> list[dict[str, str]]:
    """Compute ``{channel_id, fingerprint, canonical_sql}`` per ``op: sql`` Channel.

    Pure (no I/O). ``fingerprint`` is the SHA-256 hex digest of the canonical
    SQL, prefixed with the sqlglot dialect tag so the scheme is self-describing.
    """
    rows: list[dict[str, str]] = []
    for m in modules:
        if m.type != ModuleType.Channel or m.config.get("op") != "sql":
            continue
        query = m.config.get("query", "")
        if not query:
            continue
        canonical = _canonicalize(query)
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        rows.append(
            {
                "channel_id": m.id,
                "fingerprint": digest,
                "canonical_sql": canonical,
            }
        )
    return rows


def write_fingerprints(
    blueprint_id: str,
    run_id: str,
    modules: tuple[Any, ...],
    observability_store: Any = None,
) -> None:
    """Upsert Channel fingerprints into the ``channel_fingerprints`` changelog.

    For each Channel: ``INSERT`` a new row the first time a fingerprint is seen;
    on a repeat (same ``(blueprint_id, channel_id, fingerprint)``) only bump
    ``last_seen``/``last_run_id``. Net effect: the table is a version history of
    semantic SQL changes, not one row per run. Never raises — fingerprinting must
    not block execution.
    """
    try:
        if observability_store is None:
            return

        rows = compute_channel_fingerprints(modules)
        if not rows:
            return

        from datetime import datetime

        now = datetime.now(tz=UTC).isoformat()

        with observability_store.connect() as cur:
            cur.executemany(
                """
                INSERT INTO channel_fingerprints
                    (blueprint_id, channel_id, fingerprint, canonical_sql,
                     first_seen, last_seen, first_run_id, last_run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (blueprint_id, channel_id, fingerprint) DO UPDATE SET
                    last_seen   = excluded.last_seen,
                    last_run_id = excluded.last_run_id
                """,
                [
                    (
                        blueprint_id,
                        r["channel_id"],
                        r["fingerprint"],
                        r["canonical_sql"],
                        now,
                        now,
                        run_id,
                        run_id,
                    )
                    for r in rows
                ],
            )

        logger.debug(
            "Fingerprints: upserted %d Channel rows for blueprint %r run %r",
            len(rows), blueprint_id, run_id,
        )
    except Exception as exc:
        logger.debug("Fingerprint write skipped (non-fatal): %s", exc)
