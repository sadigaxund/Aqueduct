"""Read-only data layer for `aqueduct studio` (Phase 67 / Phase 69).

NO ``textual``, NO ``pyspark`` — unit-tested directly. Backend-agnostic: the
structured queries run against an ``ObservabilityStore`` (DuckDB *or* Postgres)
via its ``RelationalCursor`` (`?` placeholders work on both). The ad-hoc SQL pane
is the one exception — it opens a dedicated **read-only DuckDB** connection so a
typo can't mutate the store, and is therefore offered for the DuckDB backend only.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from aqueduct.config import DEFAULT_OBS_DB_FILENAME

_DEFAULT_OBS_FILE = f".aqueduct/{DEFAULT_OBS_DB_FILENAME}"
_DEFAULT_OBS_ROOT = ".aqueduct/observability"


@dataclass
class StoreHandle:
    """One selectable observability store for the picker."""
    label: str               # blueprint id, "(postgres)", or a path stem
    store: Any               # ObservabilityStore (anything with .connect())
    duckdb_path: Path | None  # set for duckdb (enables the read-only SQL pane); None for pg


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


def _duckdb_files(obs_path: str | None, store_dir: str | None, root: str) -> list[tuple[str, Path]]:
    """All DuckDB observability files for the picker → (blueprint_id, path)."""
    out: list[tuple[str, Path]] = []
    seen: set[Path] = set()

    def add(bp: str, p: "str | Path") -> None:
        path = Path(p)
        if path.is_file() and path not in seen:
            seen.add(path)
            out.append((bp, path))

    if store_dir:
        add("", Path(store_dir) / DEFAULT_OBS_DB_FILENAME)
        return out
    if obs_path and obs_path != _DEFAULT_OBS_FILE:
        ep = Path(obs_path)
        if ep.is_dir():
            add("", ep / DEFAULT_OBS_DB_FILENAME)
            for sub in sorted(ep.glob(f"*/{DEFAULT_OBS_DB_FILENAME}")):
                add(sub.parent.name, sub)
        else:
            add("", ep)
        return out
    add("", Path(_DEFAULT_OBS_FILE))
    base = Path(root)
    if base.is_dir():
        for sub in sorted(base.glob(f"*/{DEFAULT_OBS_DB_FILENAME}")):
            add(sub.parent.name, sub)
    return out


def discover_stores(
    cfg: Any, store_dir: str | None = None, root: str = _DEFAULT_OBS_ROOT
) -> list[StoreHandle]:
    """Selectable stores, backend-aware.

    Postgres → a single handle (one schema holds every run; SQL pane disabled).
    DuckDB   → one handle per discovered file (override / non-default path /
    flat default / per-blueprint routing).
    """
    obs = cfg.stores.observability
    if getattr(obs, "backend", "duckdb") == "postgres":
        from aqueduct.stores.base import get_stores

        return [StoreHandle("(postgres)", get_stores(cfg).observability, None)]

    from aqueduct.stores.duckdb_ import DuckDBObservabilityStore

    handles: list[StoreHandle] = []
    for bp, path in _duckdb_files(getattr(obs, "path", None), store_dir, root):
        handles.append(StoreHandle(bp or path.parent.name, DuckDBObservabilityStore(path), path))
    return handles


def list_runs(store: Any, limit: int = 50) -> list[RunRow]:
    """Most-recent runs first (works on any ObservabilityStore backend)."""
    with store.connect() as cur:
        cur.execute(
            """
            SELECT run_id, blueprint_id, status,
                   CAST(started_at AS VARCHAR), CAST(finished_at AS VARCHAR)
            FROM run_records
            ORDER BY started_at DESC
            LIMIT ?
            """,
            [limit],
        )
        rows = cur.fetchall()
    return [RunRow(*r) for r in rows]


def run_detail(store: Any, run_id: str) -> RunDetail | None:
    """Module results + resource profile for one run, or None if not found."""
    with store.connect() as cur:
        cur.execute(
            """
            SELECT run_id, blueprint_id, status,
                   CAST(started_at AS VARCHAR), CAST(finished_at AS VARCHAR),
                   module_results
            FROM run_records WHERE run_id = ?
            """,
            [run_id],
        )
        row = cur.fetchone()
        if row is None:
            return None
        try:
            cur.execute(
                """
                SELECT module_id, records_written, bytes_written, duration_ms
                FROM module_metrics WHERE run_id = ?
                ORDER BY duration_ms DESC NULLS LAST
                """,
                [run_id],
            )
            prof = cur.fetchall()
        except Exception:
            prof = []  # module_metrics may not exist yet

    run = RunRow(row[0], row[1], row[2], row[3], row[4])
    raw = row[5]
    mr = json.loads(raw) if isinstance(raw, str) else (raw or [])
    modules = [
        ModuleResult(m.get("module_id", ""), m.get("status", ""), m.get("error") or "")
        for m in mr
    ]
    return RunDetail(run, modules, [ProfileRow(*p) for p in prof])


def lineage(store: Any, blueprint_id: str | None = None, limit: int = 500) -> list[LineageRow]:
    """Column-level lineage rows (empty if the table is absent)."""
    q = "SELECT channel_id, output_column, source_table, source_column FROM column_lineage"
    params: list[Any] = []
    if blueprint_id:
        q += " WHERE blueprint_id = ?"
        params.append(blueprint_id)
    q += " LIMIT ?"
    params.append(limit)
    try:
        with store.connect() as cur:
            cur.execute(q, params)
            rows = cur.fetchall()
    except Exception:
        rows = []  # column_lineage may not exist
    return [LineageRow(*r) for r in rows]


def run_sql_readonly(duckdb_path: "str | Path", query: str) -> tuple[list[str], list[tuple[Any, ...]]]:
    """Ad-hoc query over a **read-only** DuckDB connection → (columns, rows).

    DuckDB-only: the read-only connection physically rejects writes, which is the
    safety guarantee the SQL pane needs. (Postgres lacks an equally cheap per-call
    read-only guarantee here, so the pane is disabled for it.)
    """
    conn = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        cur = conn.execute(query)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
    finally:
        conn.close()
    return cols, rows
