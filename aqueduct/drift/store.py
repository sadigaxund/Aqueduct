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


def get_baseline(
    observability_store: Any, blueprint_id: str, module_id: str
) -> dict[str, str] | None:
    """Return the last-seen schema for a module, or None when no baseline exists.

    Single-module convenience wrapper (one `connect()`, kept for direct/
    programmatic callers and unit tests). `aqueduct drift` itself calls
    `get_baselines` below to fetch every Ingress module's baseline in ONE
    `connect()` instead of one per module — see that function's docstring.
    """
    with observability_store.connect() as cur:
        return _fetch_baseline(cur, blueprint_id, module_id)


def _fetch_baseline(cur: Any, blueprint_id: str, module_id: str) -> dict[str, str] | None:
    """Shared SELECT body for `get_baseline`/`get_baselines` — takes an
    already-open cursor so the batched caller pays for one `connect()` total."""
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


def get_baselines(
    observability_store: Any, blueprint_id: str, module_ids: list[str]
) -> dict[str, dict[str, str]]:
    """Return the last-seen schema for every module in ``module_ids``, keyed
    by module_id (a module with no baseline yet is simply absent from the
    result).

    Batches all lookups into ONE `connect()` — `aqueduct drift` checks every
    Ingress module of a Blueprint in a single command invocation, and used to
    call `get_baseline` (one `connect()`/`close()` each) per module. For a
    Blueprint with N Ingress modules that opened N connections just for
    baselines, plus another N for `record_check` — 2N DuckDB connections for
    one `drift` run. This still issues one SELECT per module (DuckDB has no
    trivial "latest row per key" batch form without a window-function query
    on a dynamic IN-list), but they all run inside a single held connection.
    """
    baselines: dict[str, dict[str, str]] = {}
    if not module_ids:
        return baselines
    with observability_store.connect() as cur:
        for module_id in module_ids:
            baseline = _fetch_baseline(cur, blueprint_id, module_id)
            if baseline is not None:
                baselines[module_id] = baseline
    return baselines


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


def record_checks(observability_store: Any, checks: list[dict[str, Any]]) -> list[str]:
    """Insert multiple drift-check audit rows in ONE `connect()` call.

    ``aqueduct drift`` used to call `record_check` once per Ingress module
    (one `connect()`/`close()` each — see `get_baselines`'s docstring for the
    matching read-side fix). The CLI now accumulates one dict per module
    while it iterates (same keys as `record_check`'s keyword args, minus
    ``observability_store``) and flushes them all here with a single
    `executemany`, at the end of the command's per-module loop — not held
    open across the whole `drift` run's schema reads, just around this one
    batched write.

    Returns the generated ids, same order as ``checks``.
    """
    if not checks:
        return []
    ids = [uuid.uuid4().hex for _ in checks]
    now = datetime.now(tz=UTC).isoformat()
    with observability_store.connect() as cur:
        cur.executemany(
            """
            INSERT INTO drift_checks
                (id, blueprint_id, module_id, checked_at, baseline_schema,
                 live_schema, status, breaking_changes, benign_changes, patch_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    check_id,
                    check["blueprint_id"],
                    check["module_id"],
                    now,
                    (
                        json.dumps(check["baseline_schema"])
                        if check.get("baseline_schema") is not None
                        else None
                    ),
                    json.dumps(check["live_schema"]),
                    check["status"],
                    (
                        json.dumps(check["breaking_changes"])
                        if check.get("breaking_changes") is not None
                        else None
                    ),
                    (
                        json.dumps(check["benign_changes"])
                        if check.get("benign_changes") is not None
                        else None
                    ),
                    check.get("patch_id"),
                )
                for check_id, check in zip(ids, checks, strict=True)
            ],
        )
    return ids
