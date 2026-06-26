"""Shared read-time observability query layer (Phase 68).

The ONE read layer behind every observability *viewer* — the frozen `aqueduct
studio` TUI (`aqueduct/tui/data.py` re-exports from here), the Streamlit
dashboard, and `report --json`. No duplication: each surface is rendering only;
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

import duckdb

from aqueduct.config import DEFAULT_OBS_DB_FILENAME

_DEFAULT_OBS_FILE = f".aqueduct/{DEFAULT_OBS_DB_FILENAME}"
_DEFAULT_OBS_ROOT = ".aqueduct/observability"


# ── Single-store row shapes ─────────────────────────────────────────────────

@dataclass
class StoreHandle:
    """One selectable observability store."""
    label: str               # blueprint id, "(postgres)", or a path stem
    store: Any               # ObservabilityStore (anything with .connect())
    duckdb_path: Path | None  # set for duckdb (enables read-only SQL pane); None for pg
    blob_root: Path | None = None  # local blob dir (duckdb parent or obs store dir)
    blob_backend: str = "local"    # blob store backend (local, s3, gcs, adls)
    blob_location: str = ""        # blob store path/location


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
    last_run: str | None        # most recent started_at
    heal_attempts: int          # best-effort (0 if healing_outcomes absent)

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


@dataclass(frozen=True)
class PlanMetricRow:
    """Per-module plan complexity from explain_snapshot (zero-Spark-action, every run)."""
    run_id: str
    started_at: str
    module_id: str
    exchange_count: int       # Exchange nodes ≈ shuffles
    python_udf_count: int     # row-at-a-time python UDFs (non-vectorized)
    broadcast_count: int


# ── Store discovery ─────────────────────────────────────────────────────────

def _duckdb_files(obs_path: str | None, store_dir: str | None, root: str) -> list[tuple[str, Path]]:
    """All DuckDB observability files → (blueprint_id, path).

    Covers: ``--store-dir`` override; a non-default configured ``path`` that is a
    file (single store) OR a directory (location-only routing → per-blueprint
    files under it); the flat default + per-blueprint routing root.
    """
    out: list[tuple[str, Path]] = []
    seen: set[Path] = set()

    def add(bp: str, p: str | Path) -> None:
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
        return [StoreHandle("(postgres)", get_stores(cfg).observability, None, _blob_root,
                           _blob_be, _blob_loc)]

    from aqueduct.stores.duckdb_ import DuckDBObservabilityStore

    _blob_cfg = getattr(getattr(cfg, "stores", None), "blob", None)
    _blob_be = getattr(_blob_cfg, "backend", None) or "local"
    _blob_loc = getattr(_blob_cfg, "path", None) or ""
    handles: list[StoreHandle] = []
    for bp, path in _duckdb_files(getattr(obs, "path", None), store_dir, root):
        handles.append(StoreHandle(bp or path.parent.name, DuckDBObservabilityStore(path), path,
                                   path.parent, _blob_be, _blob_loc))
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
            prof_rows = [
                {d[0]: v for d, v in zip(cur.description, row)}
                for row in cur.fetchall()
            ]
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
                    {d[0]: v for d, v in zip(cur.description, row)}
                    for row in cur.fetchall()
                ]
            except Exception:
                prof_rows = []  # module_metrics may not exist yet

    run = RunRow(row[0], row[1], row[2], row[3], row[4])
    raw = row[5]
    mr = json.loads(raw) if isinstance(raw, str) else (raw or [])
    modules = [
        ModuleResult(m.get("module_id", ""), m.get("status", ""), m.get("error") or "")
        for m in mr
    ]
    # Order the profile to match execution order (the module_results order).
    by_id = {
        p["module_id"]: ProfileRow(
            module_id=p["module_id"],
            records_written=p.get("records_written"),
            bytes_written=p.get("bytes_written"),
            duration_ms=p.get("duration_ms"),
            records_read=p.get("records_read"),
            bytes_read=p.get("bytes_read"),
        )
        for p in prof_rows
    }
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
    return FailureContext(row[0] or "", row[1] or "", row[2], row[3],
                          list(sc or []), row[5], mj, pj)


def lineage(store: Any, blueprint_id: str | None = None,
            run_id: str | None = None, limit: int = 500) -> list[LineageRow]:
    """Column-level lineage rows (empty if the table is absent)."""
    q = "SELECT channel_id, output_column, source_table, source_column FROM column_lineage"
    params: list[Any] = []
    clauses: list[str] = []
    if blueprint_id:
        clauses.append("blueprint_id = ?")
        params.append(blueprint_id)
    if run_id:
        clauses.append("run_id = ?")
        params.append(run_id)
    if clauses:
        q += " WHERE " + " AND ".join(clauses)
    q += " LIMIT ?"
    params.append(limit)
    try:
        with store.connect() as cur:
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


def module_trends(store: Any, blueprint_id: str,
                   module_id: str, limit: int = 20) -> list[ModuleTrendRow]:
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
}


def probe_signals(store: Any, blueprint_id: str,
                   signal_type: str, limit: int = 20) -> list[ProbeSignalRow]:
    """Probe signal payloads across recent runs of *blueprint_id*."""
    try:
        with store.connect() as cur:
            cur.execute(
                """
                SELECT p.run_id, CAST(r.started_at AS VARCHAR), p.signal_type, p.payload
                FROM probe_signals p
                JOIN run_records r ON r.run_id = p.run_id
                WHERE r.blueprint_id = ? AND p.signal_type = ?
                ORDER BY r.started_at DESC
                LIMIT ?
                """,
                [blueprint_id, signal_type, limit],
            )
            rows = cur.fetchall()
        # payload is a JSON column: DuckDB returns a str, psycopg2 returns a
        # parsed dict — handle both.
        return [
            ProbeSignalRow(r[0], r[1], r[2],
                           json.loads(r[3]) if isinstance(r[3], str) else (r[3] or {}))
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


def plan_metrics(store: Any, blueprint_id: str, limit: int = 200) -> list[PlanMetricRow]:
    """Per-module plan complexity across recent runs of *blueprint_id*.

    Reads ``explain_snapshot`` (captured every real run at zero Spark action by
    analysing the query plan): exchange_count ≈ shuffles, python_udf_count =
    non-vectorized python UDFs, broadcast_count. Empty if the table is absent.
    """
    try:
        with store.connect() as cur:
            cur.execute(
                """
                SELECT e.run_id, CAST(r.started_at AS VARCHAR), e.module_id,
                       e.exchange_count, e.python_udf_count, e.broadcast_count
                FROM explain_snapshot e
                JOIN run_records r ON r.run_id = e.run_id
                WHERE r.blueprint_id = ?
                ORDER BY r.started_at DESC
                LIMIT ?
                """,
                [blueprint_id, limit],
            )
            return [PlanMetricRow(*row) for row in cur.fetchall()]
    except Exception:
        return []


def run_sql_readonly(duckdb_path: str | Path, query: str) -> tuple[list[str], list[tuple[Any, ...]]]:
    """Ad-hoc query over a **read-only** DuckDB connection → (columns, rows)."""
    conn = duckdb.connect(str(duckdb_path), read_only=True)
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
            if status == "success":
                a["successes"] += n
            elif status == "error":
                a["errors"] += n
            if last and (a["last"] is None or last > a["last"]):
                a["last"] = last
        for bp, n in heals.items():
            agg.setdefault(
                bp, {"runs": 0, "successes": 0, "errors": 0, "last": None, "heals": 0}
            )["heals"] += n

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
    """Failure-category distribution across the fleet (best-effort)."""
    dist: dict[str, int] = {}
    for h in discover_stores(cfg, store_dir=store_dir):
        for sql in (
            "SELECT failure_category, COUNT(*) FROM healing_outcomes GROUP BY failure_category",
            "SELECT category, COUNT(*) FROM failure_contexts GROUP BY category",
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
    """Zero-token (cached/replayed) vs LLM heal resolution counts across fleet."""
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


def heal_stop_vs_success(cfg: Any, store_dir: str | None = None
                         ) -> list[dict[str, Any]]:
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
                    rows.append({
                        "stop_reason": stop_reason,
                        "run_success_after_patch": "success" if success else "failed",
                        "count": cnt,
                    })
        except Exception:
            continue
    return rows


def heal_attempt_details(cfg: Any, store_dir: str | None = None,
                          limit: int = 100) -> list[dict[str, Any]]:
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
                d["breaking_changes"] = json.loads(d["breaking_changes"]) if d.get("breaking_changes") else []
                d["benign_changes"] = json.loads(d["benign_changes"]) if d.get("benign_changes") else []
                d["baseline_schema"] = json.loads(d["baseline_schema"]) if d.get("baseline_schema") else {}
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


_ASSERT_FAIL_PREFIXES = ("failed:", "null_rate[", "min_rows:", "max_rows:",
                         "freshness:", "sql assertion failed:", "spillway_rate:")

def assert_failures(cfg: Any, store_dir: str | None = None,
                    limit: int = 100) -> list[AssertFailureRow]:
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
                            rows.append(AssertFailureRow(
                                blueprint_id=bp_id, run_id=run_id,
                                started_at=started, module_id=m.get("module_id", ""),
                                error_type=et, error_message=err,
                            ))
                        elif m.get("status") == "error" and err:
                            match = next(
                                (p for p in _ASSERT_FAIL_PREFIXES if err.startswith(p)),
                                None,
                            )
                            if match:
                                rows.append(AssertFailureRow(
                                    blueprint_id=bp_id, run_id=run_id,
                                    started_at=started, module_id=m.get("module_id", ""),
                                    error_type=match.rstrip(":"),
                                    error_message=err,
                                ))
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


def quarantine_volumes(cfg: Any, store_dir: str | None = None,
                       limit: int = 100) -> list[QuarantineVolumeRow]:
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
                    rows.append(QuarantineVolumeRow(
                        blueprint_id=rec[1], run_id=rec[0],
                        started_at=rec[2], module_id=rec[3],
                        records_written=rec[4] or 0,
                    ))
        except Exception:
            continue
    return rows


def maintenance_metrics(cfg: Any, store_dir: str | None = None,
                        limit: int = 50) -> list[dict]:
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
                    rows.append({
                        "run_id": rec[0], "module_id": rec[1],
                        "optimize_ms": rec[2], "vacuum_ms": rec[3],
                        "captured_at": rec[4],
                    })
        except Exception:
            continue
    return rows


def patch_lifecycle_counts(cfg: Any, store_dir: str | None = None) -> dict[str, int]:
    """Aggregate patch_index status counts (pending, applied, rejected) across fleet."""
    counts: dict[str, int] = {"pending": 0, "applied": 0, "rejected": 0}
    for h in discover_stores(cfg, store_dir=store_dir):
        try:
            with h.store.connect() as cur:
                cur.execute(
                    "SELECT status, COUNT(*) FROM patch_index GROUP BY status"
                )
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
                    out.append(PatchRow(
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
                    ))
        except Exception:
            continue
    out.sort(key=lambda r: r.created_at, reverse=True)
    return out


def patch_simulation_for_patch(cfg: Any, patch_id: str,
                                store_dir: str | None = None) -> list[PatchSimulationRow]:
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
                    out.append(PatchSimulationRow(
                        patch_id=d["patch_id"],
                        gate=d["gate"],
                        status=d["status"],
                        detail=d.get("detail"),
                        duration_ms=d.get("duration_ms"),
                    ))
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


def gate_rejection_rates(cfg: Any, store_dir: str | None = None) -> dict[str, int]:
    """Gate rejection counts across fleet (from patch_simulation or heal_attempts)."""
    agg: dict[str, int] = {}
    for h in discover_stores(cfg, store_dir=store_dir):
        for sql in (
            "SELECT gate, status FROM patch_simulation WHERE status != 'passed'",
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
