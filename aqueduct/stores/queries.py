"""Shared read-time observability query layer (Phase 68).

The ONE read layer behind every observability *viewer* — the Streamlit
dashboard and `report --json`. No duplication: each surface is rendering only;
all queries live here.

Design rules:
- **Backend-agnostic.** Structured queries run against an ``ObservabilityStore``
  (DuckDB *or* Postgres) via its ``RelationalCursor`` (`?` placeholders work on
  both). NO ``textual``, NO ``pyspark`` — unit-testable directly.
- **Read-time only, no duplication.** Cross-blueprint "fleet" aggregates are
  computed by iterating the per-blueprint stores and merging in Python (DuckDB)
  or one ``GROUP BY blueprint_id`` (Postgres) — never materialised into a second
  copy.
- **Short-lived reads.** Every query opens, reads, and closes its cursor
  (``with store.connect()``) so a held handle never blocks a running pipeline's
  writer (DuckDB takes an exclusive lock on its file). Fleet readers must NOT
  hold connections open across refreshes.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from aqueduct.config import DEFAULT_OBS_DB_FILENAME, DEFAULT_OBS_ROUTING_ROOT
from aqueduct.executor.models import ExecutionStatus
from aqueduct.stores.object_store import PatchStore

_DEFAULT_OBS_ROOT = DEFAULT_OBS_ROUTING_ROOT


# ── Single-store row shapes ─────────────────────────────────────────────────


@dataclass
class StoreHandle:
    """One selectable observability store."""

    label: str  # blueprint id, "(postgres)", or a path stem
    store: Any  # ObservabilityStore (anything with .connect())
    duckdb_path: Path | None  # set for duckdb (enables read-only SQL pane); None for pg
    blob_root: Path | None = None  # local blob dir (duckdb parent or obs store dir)
    blob_backend: str = "local"  # blob store backend (local, s3, gcs, adls)
    blob_location: str = ""  # blob store path/location


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
    records_read: int | None = None
    bytes_read: int | None = None


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


# ── Fleet (cross-blueprint) row shapes ──────────────────────────────────────


@dataclass(frozen=True)
class BlueprintSummary:
    blueprint_id: str
    runs: int
    successes: int
    errors: int
    last_run: str | None  # most recent started_at
    heal_attempts: int  # best-effort (0 if healing_outcomes absent)

    @property
    def success_rate(self) -> float:
        return (self.successes / self.runs) if self.runs else 0.0


@dataclass(frozen=True)
class FingerprintRow:
    channel_id: str
    fingerprint: str
    first_seen: str
    last_seen: str
    first_run_id: str
    last_run_id: str
    canonical_sql: str = ""


@dataclass(frozen=True)
class DayCount:
    day: str
    status: str
    count: int


# ── Store discovery ─────────────────────────────────────────────────────────


def _duckdb_files(obs_path: str | None, store_dir: str | None, root: str) -> list[tuple[str, Path]]:
    """All DuckDB observability files → (blueprint_id, path).

    ``base`` is the routing directory: ``--store-dir`` when given, else the
    configured ``path``, else the default root — both resolve identically
    (docs/specs.md §10.4.1: ``--store-dir`` is "same per-blueprint split, but
    under your directory"). Per-blueprint files at
    ``<base>/<blueprint_id>/observability.db`` are discovered by globbing;
    a flat ``<base>/observability.db`` (a store written directly at the
    routing root, e.g. a single-pipeline ``--store-dir``) is also included.
    """
    out: list[tuple[str, Path]] = []
    seen: set[Path] = set()

    def add(bp: str, p: str | Path) -> None:
        path = Path(p)
        if path.is_file() and path not in seen:
            seen.add(path)
            out.append((bp, path))

    base = Path(store_dir) if store_dir else (Path(obs_path) if obs_path else Path(root))
    if base.is_dir():
        for sub in sorted(base.glob(f"*/{DEFAULT_OBS_DB_FILENAME}")):
            add(sub.parent.name, sub)
    add("", base / DEFAULT_OBS_DB_FILENAME)
    return out


def discover_stores(
    cfg: Any, store_dir: str | None = None, root: str = _DEFAULT_OBS_ROOT
) -> list[StoreHandle]:
    """Selectable stores, backend-aware.

    Postgres → a single handle (one schema holds every run; SQL pane disabled).
    DuckDB   → one handle per discovered file.
    """
    obs = cfg.stores.observability
    if getattr(obs, "backend", "duckdb") == "postgres":
        from aqueduct.stores.base import get_stores
        from aqueduct.stores.read import _OBS_ROUTING_ROOT, _is_default_obs_path

        _blob_root: Path | None = None
        if store_dir:
            _blob_root = Path(store_dir)
        else:
            _raw = getattr(obs, "path", "") or ""
            if _is_default_obs_path(_raw):
                _blob_root = Path(_OBS_ROUTING_ROOT)
            elif not any(_raw.startswith(p) for p in ("postgresql://", "postgres://")):
                _p = Path(_raw)
                _blob_root = _p if not _p.suffix else _p.parent
            else:
                _blob_root = Path(_OBS_ROUTING_ROOT)
        _blob_cfg = getattr(getattr(cfg, "stores", None), "blob", None)
        _blob_be = getattr(_blob_cfg, "backend", None) or "local"
        _blob_loc = getattr(_blob_cfg, "path", None) or ""
        return [
            StoreHandle(
                "(postgres)", get_stores(cfg).observability, None, _blob_root, _blob_be, _blob_loc
            )
        ]

    from aqueduct.stores.duckdb_ import DuckDBObservabilityStore

    _blob_cfg = getattr(getattr(cfg, "stores", None), "blob", None)
    _blob_be = getattr(_blob_cfg, "backend", None) or "local"
    _blob_loc = getattr(_blob_cfg, "path", None) or ""
    handles: list[StoreHandle] = []
    for bp, path in _duckdb_files(getattr(obs, "path", None), store_dir, root):
        handles.append(
            StoreHandle(
                bp or path.parent.name,
                DuckDBObservabilityStore(path),
                path,
                path.parent,
                _blob_be,
                _blob_loc,
            )
        )
    return handles


# ── Single-store queries ────────────────────────────────────────────────────


def list_runs(store: Any, limit: int = 50, blueprint_id: str | None = None) -> list[RunRow]:
    """Most-recent runs first (any backend). Optional blueprint filter (pg)."""
    q = (
        "SELECT run_id, blueprint_id, status, "
        "CAST(started_at AS VARCHAR), CAST(finished_at AS VARCHAR) FROM run_records"
    )
    params: list[Any] = []
    if blueprint_id:
        q += " WHERE blueprint_id = ?"
        params.append(blueprint_id)
    q += " ORDER BY started_at DESC LIMIT ?"
    params.append(limit)
    with store.connect() as cur:
        cur.execute(q, params)
        rows = cur.fetchall()
    return [RunRow(*r) for r in rows]


def run_detail(store: Any, run_id: str) -> RunDetail | None:
    """Module results + resource profile for one run, or None if not found.

    The profile is returned in **execution order** (matching the module-results
    order), not slowest-first — consistent with the Modules view.
    """
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
                SELECT module_id, records_written, bytes_written, duration_ms,
                       records_read, bytes_read
                FROM module_metrics WHERE run_id = ?
                """,
                [run_id],
            )
            prof_rows = [{d[0]: v for d, v in zip(cur.description, row)} for row in cur.fetchall()]
        except Exception:
            try:
                cur.execute(
                    """
                    SELECT module_id, records_written, bytes_written, duration_ms
                    FROM module_metrics WHERE run_id = ?
                    """,
                    [run_id],
                )
                prof_rows = [
                    {d[0]: v for d, v in zip(cur.description, row)} for row in cur.fetchall()
                ]
            except Exception:
                prof_rows = []  # module_metrics may not exist yet

    run = RunRow(row[0], row[1], row[2], row[3], row[4])
    raw = row[5]
    mr = json.loads(raw) if isinstance(raw, str) else (raw or [])
    modules = [
        ModuleResult(m.get("module_id", ""), m.get("status", ""), m.get("error") or "") for m in mr
    ]
    # Order the profile to match execution order (the module_results order).
    #
    # A module_id can own MORE than one module_metrics row — a synthetic
    # Handoff module (§10.9) gets exactly two: the WRITE side (bytes_written,
    # its own duration) and the READ side (bytes_read, its own duration),
    # never both fields on one row. Every ordinary module still gets exactly
    # one row, so this merge is a no-op there. MERGE by module_id rather
    # than a last-row-wins dict comprehension — the latter silently dropped
    # whichever fields the LATER row didn't carry (a Handoff module would
    # report only its read-side bytes, never its write-side ones, or vice
    # versa depending on row order).
    by_id: dict[str, ProfileRow] = {}
    for p in prof_rows:
        mid = p["module_id"]
        existing = by_id.get(mid)
        if existing is None:
            by_id[mid] = ProfileRow(
                module_id=mid,
                records_written=p.get("records_written"),
                bytes_written=p.get("bytes_written"),
                duration_ms=p.get("duration_ms"),
                records_read=p.get("records_read"),
                bytes_read=p.get("bytes_read"),
            )
            continue
        by_id[mid] = ProfileRow(
            module_id=mid,
            records_written=(
                existing.records_written
                if p.get("records_written") is None
                else p.get("records_written")
            ),
            bytes_written=(
                existing.bytes_written if p.get("bytes_written") is None else p.get("bytes_written")
            ),
            duration_ms=(
                (existing.duration_ms or 0) + (p.get("duration_ms") or 0)
                if p.get("duration_ms") is not None or existing.duration_ms is not None
                else None
            ),
            records_read=(
                existing.records_read if p.get("records_read") is None else p.get("records_read")
            ),
            bytes_read=existing.bytes_read if p.get("bytes_read") is None else p.get("bytes_read"),
        )
    order = {m.module_id: i for i, m in enumerate(modules)}
    profile = sorted(by_id.values(), key=lambda p: order.get(p.module_id, len(order)))
    return RunDetail(run, modules, profile)


@dataclass(frozen=True)
class FailureContext:
    failed_module: str
    error_message: str
    error_class: str | None
    object_name: str | None
    suggested_columns: list
    stack_trace: str | None
    manifest_json: str | None = None
    provenance_json: str | None = None


def failure_context(store: Any, run_id: str) -> FailureContext | None:
    """Full structured failure for a run (None if absent). The engine stores the
    complete error here — the Runs table only shows a preview."""
    try:
        with store.connect() as cur:
            cur.execute(
                """
                SELECT failed_module, error_message, error_class, object_name,
                       suggested_columns, stack_trace, manifest_json, provenance_json
                FROM failure_contexts WHERE run_id = ?
                """,
                [run_id],
            )
            row = cur.fetchone()
    except Exception:
        try:
            with store.connect() as cur:
                cur.execute(
                    """
                    SELECT failed_module, error_message, error_class, object_name,
                           suggested_columns, stack_trace
                    FROM failure_contexts WHERE run_id = ?
                    """,
                    [run_id],
                )
                row = cur.fetchone()
            if row:
                row = tuple(row) + (None, None)
        except Exception:
            # Inner tier of a two-tier fallback: the outer SELECT (above)
            # targets the current schema (with manifest_json/provenance_json);
            # this retry targets the pre-migration schema without them. The
            # EXPECTED case here is "column/table doesn't exist yet" — but the
            # actual exception type differs per backend (duckdb.BinderException
            # / duckdb.CatalogException vs. psycopg2's ProgrammingError
            # subclasses) and this module is deliberately backend-agnostic (see
            # the module docstring — no duckdb-/psycopg2-specific imports), so
            # narrowing the catch would mean importing both optional driver
            # packages just to name their error classes. This also swallows a
            # genuine connection failure on the retry, which is an accepted
            # trade-off: `failure_context()` already documents "None if
            # absent" as a normal outcome (most runs have no failure context
            # at all), so a best-effort display read degrading to None on any
            # DB error is consistent with its contract, not a new failure
            # mode — the caller (report/heal/TUI) just shows no failure
            # detail instead of crashing over a nice-to-have.
            return None
    if not row:
        return None
    sc = row[4]
    if isinstance(sc, str):
        try:
            sc = json.loads(sc)
        except Exception:
            sc = []
    mj = row[6] if len(row) > 6 else None
    pj = row[7] if len(row) > 7 else None
    return FailureContext(
        row[0] or "", row[1] or "", row[2], row[3], list(sc or []), row[5], mj, pj
    )


def lineage(
    store: Any, blueprint_id: str | None = None, run_id: str | None = None, limit: int = 500
) -> list[LineageRow]:
    """Column-level lineage rows (empty if the table is absent).

    Phase 85 B2 — when ``run_id`` is not given, the read is scoped to the
    LATEST run in scope (optionally within ``blueprint_id``) instead of an
    unscoped ``LIMIT 500`` across every historical run: `column_lineage` has
    no `DISTINCT`/`ORDER BY`/latest-run filter by construction (append-only,
    one row per `(channel, output_column)` per compile), so an unscoped read
    used to mix stale and current lineage up to an arbitrary cap. ``DISTINCT``
    also guards against any accidental duplicate row at the same
    ``captured_at``.
    """
    params: list[Any] = []
    clauses: list[str] = []
    if blueprint_id:
        clauses.append("blueprint_id = ?")
        params.append(blueprint_id)
    if run_id:
        clauses.append("run_id = ?")
        params.append(run_id)

    try:
        with store.connect() as cur:
            if not run_id:
                sub_where = " WHERE blueprint_id = ?" if blueprint_id else ""
                sub_params = [blueprint_id] if blueprint_id else []
                cur.execute(
                    f"SELECT run_id FROM column_lineage{sub_where} "
                    "ORDER BY captured_at DESC LIMIT 1",
                    sub_params,
                )
                latest = cur.fetchone()
                if not latest or not latest[0]:
                    return []
                clauses.append("run_id = ?")
                params.append(latest[0])

            where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
            q = (
                "SELECT DISTINCT channel_id, output_column, source_table, source_column "
                f"FROM column_lineage{where} ORDER BY channel_id, output_column LIMIT ?"
            )
            params.append(limit)
            cur.execute(q, params)
            rows = cur.fetchall()
    except Exception:
        rows = []  # column_lineage may not exist
    return [LineageRow(*r) for r in rows]


@dataclass(frozen=True)
class ModuleTrendRow:
    run_id: str
    started_at: str
    module_id: str
    duration_ms: int | None
    records_read: int | None = None
    bytes_read: int | None = None
    records_written: int | None = None
    bytes_written: int | None = None


METRIC_LABELS: dict[str, str] = {
    "duration_ms": "Duration (ms)",
    "records_read": "Records Read",
    "records_written": "Records Written",
    "bytes_read": "Bytes Read",
    "bytes_written": "Bytes Written",
}


def module_trends(
    store: Any, blueprint_id: str, module_id: str, limit: int = 20
) -> list[ModuleTrendRow]:
    """Module metrics across the *limit* most recent runs of *blueprint_id*."""
    try:
        with store.connect() as cur:
            cur.execute(
                """
                SELECT m.run_id, CAST(r.started_at AS VARCHAR), m.module_id,
                       m.duration_ms, m.records_read, m.bytes_read,
                       m.records_written, m.bytes_written
                FROM module_metrics m
                JOIN run_records r ON r.run_id = m.run_id
                WHERE r.blueprint_id = ? AND m.module_id = ?
                ORDER BY r.started_at DESC
                LIMIT ?
                """,
                [blueprint_id, module_id, limit],
            )
            rows = cur.fetchall()
        return [ModuleTrendRow(*r) for r in rows]
    except Exception:
        return []


@dataclass(frozen=True)
class ProbeSignalRow:
    run_id: str
    started_at: str
    signal_type: str
    payload: dict


PROBE_METRIC_LABELS: dict[str, str] = {
    "null_rates": "Null Rates",
    "value_distribution": "Value Distribution",
    "distinct_count": "Distinct Count",
    "schema_snapshot": "Schema Snapshot",
    "row_count_estimate": "Row Count Estimate",
    "sample_rows": "Sample Rows",
    "data_freshness": "Data Freshness",
    "execution_partitions": "Execution Partitions",
    "threshold": "Threshold Check",
    "custom": "Custom Signal",
}


def probe_signals(
    store: Any, blueprint_id: str, signal_type: str, limit: int = 20, run_id: str | None = None
) -> list[ProbeSignalRow]:
    """Probe signal payloads across recent runs of *blueprint_id*.
    Pass *run_id* to filter to a specific run only."""
    try:
        with store.connect() as cur:
            params: list[Any] = [blueprint_id, signal_type]
            run_filter = ""
            if run_id:
                params.append(run_id)
                run_filter = " AND p.run_id = ?"
            cur.execute(
                f"""
                SELECT p.run_id, CAST(r.started_at AS VARCHAR), p.signal_type, p.payload
                FROM probe_signals p
                JOIN run_records r ON r.run_id = p.run_id
                WHERE r.blueprint_id = ? AND p.signal_type = ?{run_filter}
                ORDER BY r.started_at DESC
                LIMIT ?
                """,
                [*params, limit],
            )
            rows = cur.fetchall()
        # payload is a JSON column: DuckDB returns a str, psycopg2 returns a
        # parsed dict — handle both.
        return [
            ProbeSignalRow(
                r[0], r[1], r[2], json.loads(r[3]) if isinstance(r[3], str) else (r[3] or {})
            )
            for r in rows
        ]
    except Exception:
        return []


def probe_signal_types(store: Any, blueprint_id: str) -> list[str]:
    """Distinct signal types recorded for *blueprint_id*."""
    try:
        with store.connect() as cur:
            cur.execute(
                """
                SELECT DISTINCT p.signal_type
                FROM probe_signals p
                JOIN run_records r ON r.run_id = p.run_id
                WHERE r.blueprint_id = ?
                """,
                [blueprint_id],
            )
            return [row[0] for row in cur.fetchall()]
    except Exception:
        return []


def channel_fingerprints(store: Any, blueprint_id: str) -> list[FingerprintRow]:
    """SQL fingerprint changelog per channel (empty if table absent)."""
    try:
        with store.connect() as cur:
            cur.execute(
                """
                SELECT channel_id, fingerprint,
                       CAST(first_seen AS VARCHAR), CAST(last_seen AS VARCHAR),
                       first_run_id, last_run_id,
                       canonical_sql
                FROM channel_fingerprints
                WHERE blueprint_id = ?
                ORDER BY channel_id, last_seen DESC
                """,
                [blueprint_id],
            )
            return [FingerprintRow(*r) for r in cur.fetchall()]
    except Exception:
        return []


def run_sql_readonly(
    duckdb_path: str | Path, query: str
) -> tuple[list[str], list[tuple[Any, ...]]]:
    """Ad-hoc query over a **read-only** DuckDB connection → (columns, rows).

    Goes through `_connect_read_only_with_retry` so a concurrent writer's
    commit does not turn an ad-hoc read into a spurious lock error.
    """
    from aqueduct.stores.duckdb_ import _connect_read_only_with_retry

    conn = _connect_read_only_with_retry(Path(duckdb_path))
    try:
        cur = conn.execute(query)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
    finally:
        conn.close()
    return cols, rows


# ── Fleet (cross-run + cross-blueprint) aggregates — read-time, no duplication ─


def _heal_attempts(cur: Any) -> dict[str, int]:
    """Best-effort heal-outcome counts per blueprint (0 / {} if table absent).

    ``healing_outcomes`` has no ``blueprint_id`` column — it is reached via a join
    on ``run_records.run_id`` (works on both DuckDB per-blueprint files and the
    single Postgres schema).
    """
    try:
        cur.execute(
            "SELECT r.blueprint_id, COUNT(*) "
            "FROM healing_outcomes h JOIN run_records r ON r.run_id = h.run_id "
            "GROUP BY r.blueprint_id"
        )
        return {bp: n for bp, n in cur.fetchall()}
    except Exception:
        return {}


def fleet_summary(cfg: Any, store_dir: str | None = None) -> list[BlueprintSummary]:
    """One row per blueprint across ALL stores. Computed at read time.

    Groups by ``blueprint_id`` inside each store (handles the Postgres case where
    one store holds many blueprints) and merges across handles (the DuckDB case
    where each handle is one blueprint). Short-lived reads throughout.
    """
    agg: dict[str, dict[str, Any]] = {}
    for h in discover_stores(cfg, store_dir=store_dir):
        try:
            with h.store.connect() as cur:
                cur.execute(
                    """
                    SELECT blueprint_id, status,
                           COUNT(*), MAX(CAST(started_at AS VARCHAR))
                    FROM run_records GROUP BY blueprint_id, status
                    """
                )
                rows = cur.fetchall()
                heals = _heal_attempts(cur)
        except Exception:
            continue
        for bp, status, n, last in rows:
            bp = bp or h.label
            a = agg.setdefault(
                bp, {"runs": 0, "successes": 0, "errors": 0, "last": None, "heals": 0}
            )
            a["runs"] += n
            if status == ExecutionStatus.SUCCESS:
                a["successes"] += n
            elif status == ExecutionStatus.ERROR:
                a["errors"] += n
            if last and (a["last"] is None or last > a["last"]):
                a["last"] = last
        for bp, n in heals.items():
            agg.setdefault(bp, {"runs": 0, "successes": 0, "errors": 0, "last": None, "heals": 0})[
                "heals"
            ] += n

    return sorted(
        (
            BlueprintSummary(bp, a["runs"], a["successes"], a["errors"], a["last"], a["heals"])
            for bp, a in agg.items()
        ),
        key=lambda s: (s.last_run or ""),
        reverse=True,
    )


def runs_over_time(cfg: Any, store_dir: str | None = None, days: int = 30) -> list[DayCount]:
    """Daily run counts by status across the fleet (read-time merge)."""
    merged: dict[tuple[str, str], int] = {}
    for h in discover_stores(cfg, store_dir=store_dir):
        try:
            with h.store.connect() as cur:
                cur.execute(
                    """
                    SELECT SUBSTR(CAST(started_at AS VARCHAR), 1, 10) AS day,
                           status, COUNT(*)
                    FROM run_records
                    GROUP BY day, status
                    """
                )
                rows = cur.fetchall()
        except Exception:
            continue
        for day, status, n in rows:
            if not day:
                continue
            merged[(day, status)] = merged.get((day, status), 0) + n
    out = [DayCount(day, status, n) for (day, status), n in merged.items()]
    out.sort(key=lambda d: d.day)
    if days:
        keep = sorted({d.day for d in out})[-days:]
        out = [d for d in out if d.day in keep]
    return out


def failure_categories(cfg: Any, store_dir: str | None = None) -> dict[str, int]:
    """Failure-category distribution across the fleet (best-effort).

    Phase 85 E1 — the second fallback query used to select a `category`
    column from `failure_contexts` that has never existed in the DDL
    (`aqueduct/surveyor/ddl.py`; the real column is `error_class`). It was
    silently dead: wrapped in `except Exception: continue`, and unreachable
    in practice besides — `healing_outcomes` is created by the SAME `_DDL`
    string as `failure_contexts` (both `CREATE TABLE IF NOT EXISTS` in one
    execute()), so the first query in this loop always succeeds (even
    against an empty table) and `break`s before the second ever runs. The
    fallback's real purpose is a genuinely pre-`healing_outcomes` legacy
    store (one created before that table existed) — fixed to use the real
    column name so that legacy-store case actually works instead of always
    silently no-op'ing via the except clause.
    """
    dist: dict[str, int] = {}
    for h in discover_stores(cfg, store_dir=store_dir):
        for sql in (
            "SELECT failure_category, COUNT(*) FROM healing_outcomes GROUP BY failure_category",
            "SELECT error_class, COUNT(*) FROM failure_contexts GROUP BY error_class",
        ):
            try:
                with h.store.connect() as cur:
                    cur.execute(sql)
                    for cat, n in cur.fetchall():
                        dist[cat or "unknown"] = dist.get(cat or "unknown", 0) + n
                break  # first table that exists wins for this store
            except Exception:
                continue
    return dist


def heal_coverage(cfg: Any, store_dir: str | None = None) -> dict[str, int]:
    """Heal resolution counts across the fleet, keyed by ``resolution``.

    ``llm`` is the only value written since the signature-keyed heal cache
    was removed; a store written by an older version may still carry
    historical ``cached``/``replayed`` rows, so the counts are returned as
    found rather than collapsed into a ratio.
    """
    agg: dict[str, int] = {}
    for h in discover_stores(cfg, store_dir=store_dir):
        try:
            with h.store.connect() as cur:
                cur.execute(
                    "SELECT resolution, COUNT(*) FROM healing_outcomes "
                    "WHERE resolution IS NOT NULL GROUP BY resolution"
                )
                for res, n in cur.fetchall():
                    agg[res] = agg.get(res, 0) + n
        except Exception:
            continue
    return agg


def heal_stop_vs_success(cfg: Any, store_dir: str | None = None) -> list[dict[str, Any]]:
    """Cross-reference heal_attempts.stop_reason with run success after patch."""
    rows: list[dict[str, Any]] = []
    for h in discover_stores(cfg, store_dir=store_dir):
        try:
            with h.store.connect() as cur:
                cur.execute(
                    """
                    SELECT ha.stop_reason, ho.run_success_after_patch, COUNT(*) AS cnt
                    FROM heal_attempts ha
                    JOIN healing_outcomes ho ON ho.run_id = ha.run_id
                    WHERE ha.stop_reason IS NOT NULL
                    GROUP BY ha.stop_reason, ho.run_success_after_patch
                    """
                )
                for stop_reason, success, cnt in cur.fetchall():
                    rows.append(
                        {
                            "stop_reason": stop_reason,
                            "run_success_after_patch": "success" if success else "failed",
                            "count": cnt,
                        }
                    )
        except Exception:
            continue
    return rows


def cascade_position_outcomes(cfg: Any, store_dir: str | None = None) -> list[dict[str, Any]]:
    """Model-cascade-tier vs outcome (Phase 85 C1).

    ``healing_outcomes.model_cascade_position`` is written by every cascade
    step (the 0-based tier index of the model that produced this outcome —
    0 = the cheap/fast model tried first, 1+ = escalation tiers) but was
    never selected by any reader — cascade-tier-vs-outcome correlation was
    unqueryable despite the data existing. One row per
    ``(model_cascade_position, resolution, run_success_after_patch)``
    combination with a count, merged across every discovered store.
    """
    agg: dict[tuple[Any, Any, Any], int] = {}
    for h in discover_stores(cfg, store_dir=store_dir):
        try:
            with h.store.connect() as cur:
                cur.execute(
                    """
                    SELECT model_cascade_position, resolution, run_success_after_patch,
                           COUNT(*) AS cnt
                    FROM healing_outcomes
                    WHERE model_cascade_position IS NOT NULL
                    GROUP BY model_cascade_position, resolution, run_success_after_patch
                    """
                )
                for position, resolution, success, cnt in cur.fetchall():
                    key = (position, resolution, success)
                    agg[key] = agg.get(key, 0) + int(cnt)
        except Exception:
            continue
    return [
        {
            "model_cascade_position": position,
            "resolution": resolution,
            "run_success_after_patch": "success" if success else "failed",
            "count": cnt,
        }
        for (position, resolution, success), cnt in sorted(agg.items(), key=lambda kv: kv[0][0])
    ]


def heal_costs(cfg: Any, store_dir: str | None = None) -> list[dict[str, Any]]:
    """Token cost per blueprint per month (Phase 85 D7).

    Raw data was already fully captured in ``heal_attempts.tokens_in``/
    ``tokens_out`` (per LLM turn) but had no aggregation query — only a flat,
    un-grouped 100-row detail list (``heal_attempt_details``). Groups by
    ``(blueprint_id, month)`` where month is the ``YYYY-MM`` prefix of
    ``heal_attempts.recorded_at`` (a VARCHAR ISO-8601 timestamp — lexical
    prefix slicing works identically on DuckDB and Postgres, no
    backend-specific date-trunc function needed). ``blueprint_id`` comes via
    a join to ``run_records`` — ``heal_attempts`` itself carries only
    ``run_id``.
    """
    agg: dict[tuple[str, str], dict[str, int]] = {}
    for h in discover_stores(cfg, store_dir=store_dir):
        try:
            with h.store.connect() as cur:
                cur.execute(
                    """
                    SELECT r.blueprint_id,
                           SUBSTR(CAST(ha.recorded_at AS VARCHAR), 1, 7) AS month,
                           SUM(ha.tokens_in) AS tokens_in,
                           SUM(ha.tokens_out) AS tokens_out,
                           COUNT(*) AS attempts
                    FROM heal_attempts ha
                    JOIN run_records r ON r.run_id = ha.run_id
                    GROUP BY r.blueprint_id, month
                    """
                )
                for blueprint_id, month, tokens_in, tokens_out, attempts in cur.fetchall():
                    key = (blueprint_id, month)
                    slot = agg.setdefault(key, {"tokens_in": 0, "tokens_out": 0, "attempts": 0})
                    slot["tokens_in"] += int(tokens_in or 0)
                    slot["tokens_out"] += int(tokens_out or 0)
                    slot["attempts"] += int(attempts or 0)
        except Exception:
            continue
    return [
        {
            "blueprint_id": bp,
            "month": month,
            "tokens_in": v["tokens_in"],
            "tokens_out": v["tokens_out"],
            "tokens_total": v["tokens_in"] + v["tokens_out"],
            "attempts": v["attempts"],
        }
        for (bp, month), v in sorted(agg.items(), key=lambda kv: (kv[0][1], kv[0][0]))
    ]


def heal_attempt_details(
    cfg: Any, store_dir: str | None = None, limit: int = 100
) -> list[dict[str, Any]]:
    """Cross-store heal attempts with outcome enrichment (latest *limit* rows)."""
    out: list[dict[str, Any]] = []
    for h in discover_stores(cfg, store_dir=store_dir):
        try:
            with h.store.connect() as cur:
                cur.execute(
                    """
                    SELECT ha.run_id, ha.attempt_num, ha.latency_ms,
                           ha.tokens_in, ha.tokens_out, ha.stop_reason,
                           ha.gate_that_rejected, ha.error_class,
                           ho.failure_category, ho.resolution,
                           ho.patch_applied, ho.run_success_after_patch,
                           ho.patch_id, ho.model
                    FROM heal_attempts ha
                    LEFT JOIN healing_outcomes ho ON ho.run_id = ha.run_id
                    ORDER BY ha.recorded_at DESC
                    LIMIT ?
                    """,
                    [limit],
                )
                cols = [d[0] for d in cur.description]
                for row in cur.fetchall():
                    out.append(dict(zip(cols, row)))
        except Exception:
            continue
    return out


def drift_events(store: Any, blueprint_id: str) -> list[dict[str, Any]]:
    """Drift-check timeline for a single blueprint, ordered by check time."""
    rows: list[dict[str, Any]] = []
    try:
        with store.connect() as cur:
            cur.execute(
                """
                SELECT id, checked_at, status, baseline_schema, live_schema,
                       breaking_changes, benign_changes, patch_id
                FROM drift_checks
                WHERE blueprint_id = ?
                ORDER BY checked_at
                """,
                [blueprint_id],
            )
            cols = [d[0] for d in cur.description]
            for row in cur.fetchall():
                d = dict(zip(cols, row))
                d["checked_at"] = str(d["checked_at"]) if d.get("checked_at") else ""
                d["breaking_changes"] = (
                    json.loads(d["breaking_changes"]) if d.get("breaking_changes") else []
                )
                d["benign_changes"] = (
                    json.loads(d["benign_changes"]) if d.get("benign_changes") else []
                )
                d["baseline_schema"] = (
                    json.loads(d["baseline_schema"]) if d.get("baseline_schema") else {}
                )
                d["live_schema"] = json.loads(d["live_schema"]) if d.get("live_schema") else {}
                rows.append(d)
    except Exception:
        return []
    return rows


@dataclass(frozen=True)
class AssertFailureRow:
    blueprint_id: str
    run_id: str
    started_at: str
    module_id: str
    error_type: str
    error_message: str


_ASSERT_FAIL_PREFIXES = (
    "failed:",
    "null_rate[",
    "min_rows:",
    "max_rows:",
    "freshness:",
    "sql assertion failed:",
    "spillway_rate:",
)


def assert_failures(
    cfg: Any, store_dir: str | None = None, limit: int = 100
) -> list[AssertFailureRow]:
    """Assert rule failures across recent runs (joined across stores).

    Parses the ``module_results`` JSON column from ``run_records`` and
    returns entries where ``error_type`` is set, or where a module has
    ``status=error`` with a message matching known assert failure patterns.
    Empty list when no assert failures exist.
    """
    rows: list[AssertFailureRow] = []
    for h in discover_stores(cfg, store_dir=store_dir):
        try:
            with h.store.connect() as cur:
                cur.execute(
                    """
                    SELECT run_id, blueprint_id,
                           CAST(started_at AS VARCHAR), module_results
                    FROM run_records
                    ORDER BY started_at DESC LIMIT ?
                    """,
                    [limit],
                )
                for rec in cur.fetchall():
                    run_id, bp_id, started, raw = rec
                    mr = json.loads(raw) if isinstance(raw, str) else (raw or [])
                    for m in mr:
                        et = m.get("error_type")
                        err = m.get("error") or ""
                        if et:
                            rows.append(
                                AssertFailureRow(
                                    blueprint_id=bp_id,
                                    run_id=run_id,
                                    started_at=started,
                                    module_id=m.get("module_id", ""),
                                    error_type=et,
                                    error_message=err,
                                )
                            )
                        elif m.get("status") == "error" and err:
                            match = next(
                                (p for p in _ASSERT_FAIL_PREFIXES if err.startswith(p)),
                                None,
                            )
                            if match:
                                rows.append(
                                    AssertFailureRow(
                                        blueprint_id=bp_id,
                                        run_id=run_id,
                                        started_at=started,
                                        module_id=m.get("module_id", ""),
                                        error_type=match.rstrip(":"),
                                        error_message=err,
                                    )
                                )
        except Exception:
            continue
    rows.sort(key=lambda r: r.started_at, reverse=True)
    return rows[:limit]


@dataclass(frozen=True)
class QuarantineVolumeRow:
    blueprint_id: str
    run_id: str
    started_at: str
    module_id: str
    records_written: int


def quarantine_volumes(
    cfg: Any, store_dir: str | None = None, limit: int = 100
) -> list[QuarantineVolumeRow]:
    """Per-run quarantine/spillway write volume across the fleet.

    Returns rows from ``module_metrics`` for modules whose ``module_id``
    indicates a spillway consumer (contains "spillway") or that are Egress
    modules with non-zero ``records_written`` that ran after an Assert module.
    Falls back to all ``records_written`` per blueprint/run so rising volumes
    are still visible.
    """
    rows: list[QuarantineVolumeRow] = []
    for h in discover_stores(cfg, store_dir=store_dir):
        try:
            with h.store.connect() as cur:
                cur.execute(
                    """
                    SELECT m.run_id, r.blueprint_id,
                           CAST(r.started_at AS VARCHAR),
                           m.module_id, m.records_written
                    FROM module_metrics m
                    JOIN run_records r ON r.run_id = m.run_id
                    WHERE m.records_written IS NOT NULL AND m.records_written > 0
                    ORDER BY r.started_at DESC LIMIT ?
                    """,
                    [limit],
                )
                for rec in cur.fetchall():
                    rows.append(
                        QuarantineVolumeRow(
                            blueprint_id=rec[1],
                            run_id=rec[0],
                            started_at=rec[2],
                            module_id=rec[3],
                            records_written=rec[4] or 0,
                        )
                    )
        except Exception:
            continue
    return rows


def maintenance_metrics(cfg: Any, store_dir: str | None = None, limit: int = 50) -> list[dict]:
    """Post-write maintenance (optimize/vacuum) durations.

    Returns empty list when no maintenance ops have run yet —
    which is the common case in development.
    """
    rows: list[dict] = []
    for h in discover_stores(cfg, store_dir=store_dir):
        try:
            with h.store.connect() as cur:
                cur.execute(
                    """
                    SELECT run_id, module_id, optimize_ms, vacuum_ms,
                           CAST(captured_at AS VARCHAR)
                    FROM maintenance_metrics
                    ORDER BY captured_at DESC LIMIT ?
                    """,
                    [limit],
                )
                for rec in cur.fetchall():
                    rows.append(
                        {
                            "run_id": rec[0],
                            "module_id": rec[1],
                            "optimize_ms": rec[2],
                            "vacuum_ms": rec[3],
                            "captured_at": rec[4],
                        }
                    )
        except Exception:
            continue
    return rows


def patch_lifecycle_counts(cfg: Any, store_dir: str | None = None) -> dict[str, int]:
    """Aggregate patch_index status counts (pending, applied, rejected) across fleet."""
    counts: dict[str, int] = {
        PatchStore.PENDING: 0,
        PatchStore.APPLIED: 0,
        PatchStore.REJECTED: 0,
    }
    for h in discover_stores(cfg, store_dir=store_dir):
        try:
            with h.store.connect() as cur:
                cur.execute("SELECT status, COUNT(*) FROM patch_index GROUP BY status")
                for status, cnt in cur.fetchall():
                    counts[status] = counts.get(status, 0) + cnt
        except Exception:
            continue
    return counts


@dataclass(frozen=True)
class PatchRow:
    patch_id: str
    blueprint_id: str
    run_id: str
    status: str
    error_class: str | None
    where_field: str | None
    rationale: str | None
    ops: list[str]
    source: str | None
    prompt_version: str | None
    created_at: str | None


@dataclass(frozen=True)
class PatchSimulationRow:
    patch_id: str
    gate: str
    status: str
    detail: str | None
    duration_ms: int | None


def patch_list(cfg: Any, store_dir: str | None = None) -> list[PatchRow]:
    """All patches across the fleet, most recent first."""
    out: list[PatchRow] = []
    for h in discover_stores(cfg, store_dir=store_dir):
        try:
            with h.store.connect() as cur:
                cur.execute(
                    "SELECT patch_id, blueprint_id, run_id, status, "
                    "error_class, where_field, rationale, ops, "
                    "source, prompt_version, "
                    "CAST(created_at AS VARCHAR) AS created_at "
                    "FROM patch_index ORDER BY created_at DESC"
                )
                cols = [d[0] for d in cur.description]
                for row in cur.fetchall():
                    d = dict(zip(cols, row))
                    ops_raw = d.get("ops", [])
                    if isinstance(ops_raw, str):
                        try:
                            ops_raw = json.loads(ops_raw)
                        except Exception:
                            ops_raw = []
                    out.append(
                        PatchRow(
                            patch_id=d["patch_id"],
                            blueprint_id=d["blueprint_id"],
                            run_id=d["run_id"],
                            status=d["status"],
                            error_class=d["error_class"],
                            where_field=d["where_field"],
                            rationale=d["rationale"],
                            ops=list(ops_raw or []),
                            source=d["source"],
                            prompt_version=d["prompt_version"],
                            created_at=d["created_at"],
                        )
                    )
        except Exception:
            continue
    out.sort(key=lambda r: r.created_at, reverse=True)
    return out


def patch_simulation_for_patch(
    cfg: Any, patch_id: str, store_dir: str | None = None
) -> list[PatchSimulationRow]:
    """Gate validation results for a specific patch (from patch_simulation table)."""
    out: list[PatchSimulationRow] = []
    for h in discover_stores(cfg, store_dir=store_dir):
        try:
            with h.store.connect() as cur:
                cur.execute(
                    "SELECT patch_id, gate, status, detail, duration_ms "
                    "FROM patch_simulation WHERE patch_id = ? "
                    "ORDER BY gate",
                    [patch_id],
                )
                cols = [d[0] for d in cur.description]
                for row in cur.fetchall():
                    d = dict(zip(cols, row))
                    out.append(
                        PatchSimulationRow(
                            patch_id=d["patch_id"],
                            gate=d["gate"],
                            status=d["status"],
                            detail=d.get("detail"),
                            duration_ms=d.get("duration_ms"),
                        )
                    )
        except Exception:
            continue
    return out


def _find_patch_file(patches_root: str | Path, patch_id: str) -> Path | None:
    """Find a patch file by patch_id in any status subdirectory."""
    root = Path(patches_root)
    for sub in ("pending", "applied", "rejected"):
        d = root / sub
        if not d.is_dir():
            continue
        for f in d.iterdir():
            if f.suffix == ".json" and (patch_id in f.stem or f.stem.endswith(f"_{patch_id}")):
                return f
    return None


def load_patch_file(patch_id: str, patches_root: str | Path = "") -> dict | None:
    """Load a patch JSON file by patch_id.

    Searches ``patches_root/{pending,applied,rejected}/`` for a matching file.
    If ``patches_root`` is empty, searches CWD-relative ``patches/``,
    ``tmp/test_studio/patches/``, and the absolute project root ``patches/``.
    """
    roots: list[Path] = [Path(patches_root)] if patches_root else []
    if not roots:
        roots = [Path("patches"), Path("tmp/test_studio/patches")]
        # Also try the absolute project root (one level above tmp/test_studio)
        proj = Path.cwd().parent / "patches"
        if proj.is_dir():
            roots.append(proj)
    for root in roots:
        if not root.is_dir():
            continue
        found = _find_patch_file(root, patch_id)
        if found:
            try:
                with open(found) as f:
                    return json.load(f)
            except Exception:
                return None
    return None


def patch_show(cfg: Any, patch_id: str, store_dir: str | None = None) -> dict | None:
    """One patch's ``patch_index`` metadata (+ body, best-effort), or None.

    Searches every discovered store for the patch_id (patch_index is small —
    a full scan per store is cheap). The body load is best-effort local-dir
    only (``load_patch_file``'s CWD/``patches/`` search) — object-store
    backends surface metadata only until a body-fetch path is threaded
    through here.
    """
    from aqueduct.patch import index as patch_index

    for h in discover_stores(cfg, store_dir=store_dir):
        try:
            with h.store.connect() as cur:
                row = patch_index.get(cur, patch_id)
        except Exception:
            continue
        if row is not None:
            body = load_patch_file(patch_id)
            if body is not None:
                row = {**row, "body": body}
            return row
    return None


def gate_rejection_rates(cfg: Any, store_dir: str | None = None) -> dict[str, int]:
    """Gate rejection counts across fleet, keyed by gate name.

    Counts patch_simulation rows with status = 'fail' — the only status this
    module's gates ever write that unambiguously means "this patch was
    turned back." The full status vocabulary a gate can write is `pass` |
    `warn` | `fail` | `not_applicable` | `unavailable` (see
    `aqueduct/patch/gate_status.py` and `Surveyor.record_patch_simulation`);
    `fail` is deliberately the sole value counted here:

    - `warn` is NOT a rejection: a lineage `warn` never blocks at all.
    - `not_applicable` means no check was OWED — the patch has no surface
      this gate looks at, or the operator declared none is owed
      (`sandbox_mode: off`). Informational, never blocking.
    - `unavailable` means a check WAS owed and the environment prevented it.
      It is not a rejection either — no patch was judged wrong — but for the
      sandbox gate it DOES block auto-apply
      (`gate_status.sandbox_gate_permits_auto_apply`). A rising `unavailable`
      count therefore means heals are stalling on missing engines rather than
      on bad patches, which is an environment problem, not a model one; count
      it separately instead of reading its absence from this dict as health.
    - `not_requested` means the sandbox gate was never invoked for that
      preview — a caller-level fact synthesized outside this module
      (`cli/patch.py`'s `patch preview` with no `--sandbox`), not a gate
      verdict. It also blocks auto-apply, for the same fail-closed reason as
      `unavailable`. Nothing currently persists a `not_requested` row to
      `patch_simulation` (the one caller that synthesizes it never calls
      `record_patch_simulation`), so it should not appear in this dict's
      counts today; it is documented here so a future caller that DOES
      persist it does not have to re-derive this partition.

    ⚠ Rows written before 2.1.0 may carry `skip`, the single word that used
    to cover both of the last two. It is not migrated and cannot be — the
    distinction was never recorded — so a pre-2.1.0 `skip` row means "one of
    those two, unknown which".

    The `engine_config` gate is the one whose `fail` rows are written for
    the audit trail alone: its refusal is enforced at apply time by
    `patch/apply.py::_check_guardrails`, so a counted rejection here always
    corresponds to a patch that never reached a Blueprint.

    Falls back to `heal_attempts.gate_that_rejected` (COUNT per gate) when
    the `patch_simulation` table is unavailable (e.g. an older store).

    The query below GROUPs BY gate and returns `(gate, COUNT(*))` — matching
    the `(label, count)` shape the row-accumulation loop expects (it is
    shared with the `heal_attempts` fallback below). A prior version
    selected `(gate, status)` with no aggregation; the loop then added the
    STATUS STRING (`row[1]`) onto the running int total, raising `TypeError`
    on the first row of any non-empty result. That exception was swallowed
    by the `except Exception: continue` below, so the patch_simulation
    branch never actually contributed data — every call silently fell
    through to the heal_attempts fallback (or returned `{}` when that table
    had no `gate_that_rejected` rows either), independent of the `!=
    'passed'` predicate bug this function also had.
    """
    agg: dict[str, int] = {}
    for h in discover_stores(cfg, store_dir=store_dir):
        for sql in (
            "SELECT gate, COUNT(*) FROM patch_simulation " "WHERE status = 'fail' GROUP BY gate",
            "SELECT gate_that_rejected, COUNT(*) FROM heal_attempts "
            "WHERE gate_that_rejected IS NOT NULL GROUP BY gate_that_rejected",
        ):
            try:
                with h.store.connect() as cur:
                    cur.execute(sql)
                    for row in cur.fetchall():
                        gate = row[0] or "unknown"
                        agg[gate] = agg.get(gate, 0) + (row[1] if len(row) > 1 else 1)
                break
            except Exception:
                continue
    return agg
