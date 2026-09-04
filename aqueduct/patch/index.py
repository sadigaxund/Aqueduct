"""Patch index — the relational truth about every patch (Phase 53).

The patch *bodies* live in the object store (``PatchStore``); their *status and
metadata* live here, in a relational table inside the observability store. This
split makes the index backend-blind: lookups are O(1) SQL queries instead of an
``os.scandir`` over the local ``patches/`` directory, so they work identically
whether bodies sit on local disk, s3, gcs, or adls.

One row per ``patch_id``. Status moves ``pending`` → ``applied`` | ``rejected``.
The row carries enough metadata (signature, error class, rationale, op names) to
serve ``aqueduct patch list``/``pull`` and prompt history **without reading a
body**.

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
    updated_at         VARCHAR NOT NULL,
    -- Phase 78 — execution engine this patch was healed against ("spark",
    -- "duckdb", ...). The signature already scopes lookups by engine (it's
    -- folded into the hash — see agent/signature.py), so this column exists
    -- for auditability ("show me all duckdb heals"), not for lookup filtering.
    engine             VARCHAR
);
"""
# This DDL used to also create `idx_patch_index_sig (signature, status)` and
# `idx_patch_index_sig_created (signature, status, created_at)` — added for a
# signature-keyed coaching lookup (CHANGELOG: "coaching-order covering
# index... ORDER BY created_at DESC coaching lookups"). That lookup path was
# removed by the later signature-keyed pending-reuse/retrieval cleanup (see
# `aqueduct/surveyor/surveyor.py`'s `record_heal_attempt` docstring: "every
# heal since Phase 92 removed the signature-keyed pending-reuse ... paths"),
# and no function in this file (or anywhere else — checked
# aqueduct/stores/queries.py, aqueduct/cli/) filters `patch_index` on
# `signature` any more; every read here (`get`, `recent_applied`,
# `list_by_status`) keys on `patch_id`, `status`, or `blueprint_id` instead.
# Store-hygiene audit removed both indexes rather than let them sit unused.
# A store created before this change keeps them — harmless, just unused disk
# space and a marginally slower write; not worth a migration to drop them.

# Schema-evolution rule (see aqueduct/surveyor/ddl.py's comment for the full
# rationale): CREATE TABLE IF NOT EXISTS never adds columns to an existing
# table, so a new column needs an idempotent ALTER migration too.
PATCH_INDEX_MIGRATIONS: tuple[str, ...] = (
    "ALTER TABLE patch_index ADD COLUMN IF NOT EXISTS engine VARCHAR",
)


def _now() -> str:
    return datetime.now(tz=UTC).isoformat()


# Single source of truth for the op-name string that marks a patch as
# environment-specific (an engine/session config value is not portable
# across engines — see ``aqueduct.patch.grammar.SetEngineConfigOp``).
SET_ENGINE_CONFIG_OP = "set_engine_config"


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
    engine: str = ""


def ensure_schema(cur: RelationalCursor) -> None:
    """Create the ``patch_index`` table + index if absent (idempotent)."""
    cur.execute(PATCH_INDEX_DDL)
    # In-place column migration for pre-existing databases — see the
    # schema-evolution rule above.
    for _migration in PATCH_INDEX_MIGRATIONS:
        cur.execute(_migration)


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
             rationale, ops, source, prompt_version, created_at, updated_at, engine)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            updated_at         = EXCLUDED.updated_at,
            engine             = EXCLUDED.engine
        """,
        [
            row.patch_id,
            row.blueprint_id,
            row.run_id,
            row.status,
            row.object_key,
            row.signature,
            row.signature_coarse,
            row.error_class,
            row.where_field,
            row.normalized_message,
            row.rationale,
            _json.dumps(list(row.ops)),
            row.source,
            row.prompt_version,
            now,
            now,
            row.engine,
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
    "patch_id",
    "blueprint_id",
    "run_id",
    "status",
    "object_key",
    "signature",
    "signature_coarse",
    "error_class",
    "where_field",
    "normalized_message",
    "rationale",
    "ops",
    "source",
    "prompt_version",
    "created_at",
    "updated_at",
    "engine",
]
_SELECT = ", ".join(_SELECT_COLS)


def get(cur: RelationalCursor, patch_id: str) -> dict | None:
    """The index row for *patch_id*, or None (used by ``aqueduct patch pull``)."""
    if not patch_id:
        return None
    cur.execute(f"SELECT {_SELECT} FROM patch_index WHERE patch_id = ? LIMIT 1", [patch_id])
    r = cur.fetchone()
    return _row_to_dict(_SELECT_COLS, r) if r else None


def recent_applied(cur: RelationalCursor, limit: int = 5) -> list[dict]:
    """Most-recent applied patches for the prompt's 'do not repeat' history."""
    cur.execute(
        f"SELECT {_SELECT} FROM patch_index WHERE status = 'applied' "
        "ORDER BY created_at DESC LIMIT ?",
        [limit],
    )
    return [_row_to_dict(_SELECT_COLS, r) for r in cur.fetchall()]


def list_by_status(
    cur: RelationalCursor,
    *,
    status: str | None = None,
    blueprint_id: str | None = None,
    limit: int = 200,
) -> list[dict]:
    """List patch_index rows by lifecycle status (optionally one blueprint).

    The backend-blind truth for ``aqueduct patch list`` — it works against
    whichever store backend holds the index (DuckDB or Postgres) regardless of
    where the patch *bodies* live (local dir or object store). ``status=None``
    lists every state. Newest first."""
    where: list[str] = []
    params: list[Any] = []
    if status:
        where.append("status = ?")
        params.append(status)
    if blueprint_id:
        where.append("blueprint_id = ?")
        params.append(blueprint_id)
    clause = (" WHERE " + " AND ".join(where)) if where else ""
    params.append(limit)
    cur.execute(
        f"SELECT {_SELECT} FROM patch_index{clause} ORDER BY created_at DESC LIMIT ?",
        params,
    )
    return [_row_to_dict(_SELECT_COLS, r) for r in cur.fetchall()]
