"""Persistence for ``aqueduct drift`` — the ``drift_checks`` audit + baseline.

The baseline is *self-owned*: the most recent ``drift_checks`` row for an
``(blueprint_id, module_id)`` carries the ``live_schema`` last seen, which is the
baseline the next check diffs against. No ``schema_snapshot`` Probe is required.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

#: Sole owner of the drift_checks DDL — created lazily by `aqueduct drift`, not
#: at every run. status ∈ {baseline_set, no_drift, drift_benign, drift_breaking}.
DRIFT_CHECKS_DDL = """
CREATE TABLE IF NOT EXISTS drift_checks (
    id               VARCHAR PRIMARY KEY,
    blueprint_id     VARCHAR NOT NULL,
    module_id        VARCHAR NOT NULL,
    checked_at       TIMESTAMPTZ NOT NULL,
    baseline_schema  JSON,
    live_schema      JSON NOT NULL,
    status           VARCHAR NOT NULL,
    breaking_changes JSON,
    benign_changes   JSON,
    patch_id         VARCHAR
);
CREATE INDEX IF NOT EXISTS idx_drift_module
    ON drift_checks (blueprint_id, module_id, checked_at);
"""


def ensure_schema(observability_store: Any) -> None:
    """Create the drift_checks table if absent (idempotent)."""
    with observability_store.connect() as cur:
        cur.execute(DRIFT_CHECKS_DDL)


def get_baseline(observability_store: Any, blueprint_id: str, module_id: str) -> dict[str, str] | None:
    """Return the last-seen schema for a module, or None when no baseline exists."""
    with observability_store.connect() as cur:
        row = cur.execute(
            """
            SELECT live_schema FROM drift_checks
            WHERE blueprint_id = ? AND module_id = ?
            ORDER BY checked_at DESC LIMIT 1
            """,
            [blueprint_id, module_id],
        ).fetchone()
    if row is None or row[0] is None:
        return None
    payload = row[0]
    return json.loads(payload) if isinstance(payload, str) else dict(payload)


def record_check(
    observability_store: Any,
    *,
    blueprint_id: str,
    module_id: str,
    baseline_schema: dict[str, str] | None,
    live_schema: dict[str, str],
    status: str,
    breaking_changes: list[dict[str, Any]] | None = None,
    benign_changes: list[dict[str, Any]] | None = None,
    patch_id: str | None = None,
) -> str:
    """Insert one drift-check audit row. Returns the row id."""
    check_id = uuid.uuid4().hex
    now = datetime.now(tz=UTC).isoformat()
    with observability_store.connect() as cur:
        cur.execute(
            """
            INSERT INTO drift_checks
                (id, blueprint_id, module_id, checked_at, baseline_schema,
                 live_schema, status, breaking_changes, benign_changes, patch_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                check_id,
                blueprint_id,
                module_id,
                now,
                json.dumps(baseline_schema) if baseline_schema is not None else None,
                json.dumps(live_schema),
                status,
                json.dumps(breaking_changes) if breaking_changes is not None else None,
                json.dumps(benign_changes) if benign_changes is not None else None,
                patch_id,
            ],
        )
    return check_id
