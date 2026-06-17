"""Patch index — the relational truth about every patch (Phase 53).

The patch *bodies* live in the object store (``PatchStore``); their *status and
metadata* live here, in a relational table inside the observability store. This
split is what makes the heal cache backend-blind: lookups are O(1) SQL queries
instead of an ``os.scandir`` over the local ``patches/`` directory, so they work
identically whether bodies sit on local disk, s3, gcs, or adls.

One row per ``patch_id``. Status moves ``pending`` → ``applied`` | ``rejected``.
The row carries enough metadata (signature, error class, rationale, op names) to
serve pending-reuse, coaching retrieval, and prompt history **without reading a
body**; only zero-token *replay* fetches the body (via ``object_key``) because
it needs the full operation list.

All SQL uses ``?`` placeholders — the `RelationalCursor` rewrites them to ``%s``
for Postgres. ``ON CONFLICT (patch_id)`` upsert is supported by both DuckDB and
Postgres.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aqueduct.stores.base import RelationalCursor

PATCH_INDEX_DDL = """
CREATE TABLE IF NOT EXISTS patch_index (
    patch_id           VARCHAR PRIMARY KEY,
    blueprint_id       VARCHAR,
    run_id             VARCHAR,
    status             VARCHAR NOT NULL,        -- pending | applied | rejected
    object_key         VARCHAR NOT NULL,        -- key in the PatchStore
    signature          VARCHAR,                 -- exact failure-signature hash
    signature_coarse   VARCHAR,
    error_class        VARCHAR,
    where_field        VARCHAR,
    normalized_message VARCHAR,
    rationale          VARCHAR,
    ops                JSON,                    -- list[str] op names (coaching)
    source             VARCHAR,                 -- llm | replay
    prompt_version     VARCHAR,
    created_at         VARCHAR NOT NULL,
    updated_at         VARCHAR NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_patch_index_sig ON patch_index (signature, status);
CREATE INDEX IF NOT EXISTS idx_patch_index_sig_created
    ON patch_index (signature, status, created_at);
"""


def _now() -> str:
    return datetime.now(tz=UTC).isoformat()


@dataclass(frozen=True)
class PatchIndexRow:
    """A row of ``patch_index`` — the metadata recorded alongside a patch body."""

    patch_id: str
    status: str  # pending | applied | rejected
    object_key: str
    blueprint_id: str = ""
    run_id: str = ""
    signature: str = ""
    signature_coarse: str = ""
    error_class: str = ""
    where_field: str = ""
    normalized_message: str = ""
    rationale: str = ""
    ops: list[str] = field(default_factory=list)
    source: str = "llm"
    prompt_version: str = ""


def ensure_schema(cur: RelationalCursor) -> None:
    """Create the ``patch_index`` table + index if absent (idempotent)."""
    cur.execute(PATCH_INDEX_DDL)


def upsert(cur: RelationalCursor, row: PatchIndexRow) -> None:
    """Insert or update the row keyed by ``patch_id``.

    ``created_at`` is preserved on update; ``updated_at`` always advances."""
    import json as _json

    now = _now()
    cur.execute(
        """
        INSERT INTO patch_index
            (patch_id, blueprint_id, run_id, status, object_key, signature,
             signature_coarse, error_class, where_field, normalized_message,
             rationale, ops, source, prompt_version, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (patch_id) DO UPDATE SET
            status             = EXCLUDED.status,
            object_key         = EXCLUDED.object_key,
            signature          = EXCLUDED.signature,
            signature_coarse   = EXCLUDED.signature_coarse,
            error_class        = EXCLUDED.error_class,
            where_field        = EXCLUDED.where_field,
            normalized_message = EXCLUDED.normalized_message,
            rationale          = EXCLUDED.rationale,
            ops                = EXCLUDED.ops,
            source             = EXCLUDED.source,
            prompt_version     = EXCLUDED.prompt_version,
            updated_at         = EXCLUDED.updated_at
        """,
        [
            row.patch_id, row.blueprint_id, row.run_id, row.status, row.object_key,
            row.signature, row.signature_coarse, row.error_class, row.where_field,
            row.normalized_message, row.rationale, _json.dumps(list(row.ops)),
            row.source, row.prompt_version, now, now,
        ],
    )


def set_status(
    cur: RelationalCursor,
    patch_id: str,
    status: str,
    object_key: str | None = None,
) -> None:
    """Move a patch to a new lifecycle status (and optionally a new body key)."""
    if object_key is None:
        cur.execute(
            "UPDATE patch_index SET status = ?, updated_at = ? WHERE patch_id = ?",
            [status, _now(), patch_id],
        )
    else:
        cur.execute(
            "UPDATE patch_index SET status = ?, object_key = ?, updated_at = ? "
            "WHERE patch_id = ?",
            [status, object_key, _now(), patch_id],
        )


def _row_to_dict(cols: list[str], row: Any) -> dict:
    import json as _json

    d = dict(zip(cols, row))
    if isinstance(d.get("ops"), str):
        try:
            d["ops"] = _json.loads(d["ops"])
        except Exception:
            d["ops"] = []
    return d


_SELECT_COLS = [
    "patch_id", "blueprint_id", "run_id", "status", "object_key", "signature",
    "signature_coarse", "error_class", "where_field", "normalized_message",
    "rationale", "ops", "source", "prompt_version", "created_at", "updated_at",
]
_SELECT = ", ".join(_SELECT_COLS)


def get(cur: RelationalCursor, patch_id: str) -> dict | None:
    """The index row for *patch_id*, or None (used by ``aqueduct patch pull``)."""
    if not patch_id:
        return None
    cur.execute(f"SELECT {_SELECT} FROM patch_index WHERE patch_id = ? LIMIT 1", [patch_id])
    r = cur.fetchone()
    return _row_to_dict(_SELECT_COLS, r) if r else None


def find_pending(cur: RelationalCursor, signature: str) -> dict | None:
    """Newest pending patch whose exact signature matches, or None."""
    if not signature:
        return None
    cur.execute(
        f"SELECT {_SELECT} FROM patch_index "
        "WHERE signature = ? AND status = 'pending' "
        "ORDER BY created_at DESC LIMIT 1",
        [signature],
    )
    r = cur.fetchone()
    return _row_to_dict(_SELECT_COLS, r) if r else None


def find_replay(
    cur: RelationalCursor, signature: str, successful_ids: set[str]
) -> dict | None:
    """Newest applied patch matching the signature AND confirmed successful.

    ``successful_ids`` comes from ``healing_outcomes.run_success_after_patch``
    — an applied patch with no success record is never replayed."""
    if not signature or not successful_ids:
        return None
    cur.execute(
        f"SELECT {_SELECT} FROM patch_index "
        "WHERE signature = ? AND status = 'applied' "
        "ORDER BY created_at DESC",
        [signature],
    )
    for r in cur.fetchall():
        d = _row_to_dict(_SELECT_COLS, r)
        if d["patch_id"] in successful_ids:
            return d
    return None


def find_coaching(
    cur: RelationalCursor,
    exact_hash: str,
    coarse_hash: str,
    error_class: str,
    limit: int = 3,
) -> list[dict]:
    """Tiered nearest-signature (failure → fix) pairs from applied patches.

    Tier 1 exact sig, 2 coarse sig, 3 same error_class, 4 chronological fill;
    deduped by patch_id, newest first within tier, capped at *limit*. Pure
    metadata — no body reads."""
    cur.execute(
        f"SELECT {_SELECT} FROM patch_index WHERE status = 'applied' "
        "ORDER BY created_at DESC",
        [],
    )
    rows = [_row_to_dict(_SELECT_COLS, r) for r in cur.fetchall()]
    tiers: dict[int, list[dict]] = {1: [], 2: [], 3: [], 4: []}
    for d in rows:
        if exact_hash and d.get("signature") == exact_hash:
            tier = 1
        elif coarse_hash and d.get("signature_coarse") == coarse_hash:
            tier = 2
        elif error_class and d.get("error_class") == error_class:
            tier = 3
        else:
            tier = 4
        d["_tier"] = tier
        tiers[tier].append(d)
    picked: list[dict] = []
    seen: set[str] = set()
    for tier in (1, 2, 3, 4):
        for d in tiers[tier]:
            if d["patch_id"] in seen:
                continue
            seen.add(d["patch_id"])
            picked.append(d)
            if len(picked) >= limit:
                return picked
    return picked


def recent_applied(cur: RelationalCursor, limit: int = 5) -> list[dict]:
    """Most-recent applied patches for the prompt's 'do not repeat' history."""
    cur.execute(
        f"SELECT {_SELECT} FROM patch_index WHERE status = 'applied' "
        "ORDER BY created_at DESC LIMIT ?",
        [limit],
    )
    return [_row_to_dict(_SELECT_COLS, r) for r in cur.fetchall()]
