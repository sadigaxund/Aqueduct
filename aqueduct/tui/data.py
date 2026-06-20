"""Read-only data layer for `aqueduct studio` (Phase 67).

Pure DuckDB queries — NO ``textual``, NO ``pyspark``. The studio TUI (``app.py``)
renders what these return; keeping the queries here makes them unit-testable
without a UI or a Spark install. **Every connection is opened read-only**, so the
ad-hoc SQL pane physically cannot mutate the store.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

_DEFAULT_OBS_ROOT = ".aqueduct/observability"


@dataclass(frozen=True)
class StoreInfo:
    """One discovered observability database."""
    blueprint_id: str   # "" when discovered via an explicit --store-dir
    db_path: Path


@dataclass(frozen=True)
class RunRow:
    run_id: str
    blueprint_id: str
    status: str
    started_at: str
    finished_at: str | None


@dataclass(frozen=True)
class ModuleResult:
    module_id: str
    status: str
    error: str


@dataclass(frozen=True)
class ProfileRow:
    module_id: str
    records_written: int | None
    bytes_written: int | None
    duration_ms: int | None


@dataclass(frozen=True)
class RunDetail:
    run: RunRow
    modules: list[ModuleResult]
    profile: list[ProfileRow]


@dataclass(frozen=True)
class LineageRow:
    channel_id: str
    output_column: str
    source_table: str
    source_column: str


def discover_stores(
    store_dir: str | None = None, root: str = _DEFAULT_OBS_ROOT
) -> list[StoreInfo]:
    """Locate observability DBs.

    With ``store_dir`` → just ``<store_dir>/observability.db`` (blueprint id
    unknown). Otherwise scan ``<root>/<blueprint_id>/observability.db``.
    """
    if store_dir:
        p = Path(store_dir) / "observability.db"
        return [StoreInfo("", p)] if p.exists() else []

    base = Path(root)
    stores: list[StoreInfo] = []
    if base.is_dir():
        for sub in sorted(base.iterdir()):
            db = sub / "observability.db"
            if db.exists():
                stores.append(StoreInfo(sub.name, db))
    return stores


def _connect(db_path: "str | Path") -> duckdb.DuckDBPyConnection:
    # read_only=True: the ad-hoc SQL pane cannot DELETE/UPDATE/CREATE.
    return duckdb.connect(str(db_path), read_only=True)


def list_runs(db_path: "str | Path", limit: int = 50) -> list[RunRow]:
    """Most-recent runs first."""
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT run_id, blueprint_id, status,
                   CAST(started_at AS VARCHAR), CAST(finished_at AS VARCHAR)
            FROM run_records
            ORDER BY started_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
    finally:
        conn.close()
    return [RunRow(*r) for r in rows]


def run_detail(db_path: "str | Path", run_id: str) -> RunDetail | None:
    """Module results + resource profile for one run, or None if not found."""
    conn = _connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT run_id, blueprint_id, status,
                   CAST(started_at AS VARCHAR), CAST(finished_at AS VARCHAR),
                   module_results
            FROM run_records WHERE run_id = ?
            """,
            [run_id],
        ).fetchone()
        if row is None:
            return None
        try:
            prof = conn.execute(
                """
                SELECT module_id, records_written, bytes_written, duration_ms
                FROM module_metrics WHERE run_id = ?
                ORDER BY duration_ms DESC NULLS LAST
                """,
                [run_id],
            ).fetchall()
        except Exception:
            prof = []  # module_metrics may not exist yet
    finally:
        conn.close()

    run = RunRow(row[0], row[1], row[2], row[3], row[4])
    raw = row[5]
    mr = json.loads(raw) if isinstance(raw, str) else (raw or [])
    modules = [
        ModuleResult(m.get("module_id", ""), m.get("status", ""), m.get("error") or "")
        for m in mr
    ]
    return RunDetail(run, modules, [ProfileRow(*p) for p in prof])


def run_sql(db_path: "str | Path", query: str) -> tuple[list[str], list[tuple[Any, ...]]]:
    """Execute an ad-hoc read-only query → (column_names, rows).

    The connection is read-only, so any write (INSERT/UPDATE/DELETE/CREATE) raises
    rather than mutating the store — the error message is surfaced to the user.
    """
    conn = _connect(db_path)
    try:
        cur = conn.execute(query)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
    finally:
        conn.close()
    return cols, rows


def lineage(
    db_path: "str | Path", blueprint_id: str | None = None, limit: int = 500
) -> list[LineageRow]:
    """Column-level lineage rows (empty if the table is absent)."""
    q = "SELECT channel_id, output_column, source_table, source_column FROM column_lineage"
    params: list[Any] = []
    if blueprint_id:
        q += " WHERE blueprint_id = ?"
        params.append(blueprint_id)
    q += " LIMIT ?"
    params.append(limit)

    conn = _connect(db_path)
    try:
        try:
            rows = conn.execute(q, params).fetchall()
        except Exception:
            rows = []  # column_lineage may not exist
    finally:
        conn.close()
    return [LineageRow(*r) for r in rows]
