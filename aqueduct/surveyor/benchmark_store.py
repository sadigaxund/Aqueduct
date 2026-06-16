"""Benchmark persistence + regression detection — Phase 33 Part A.

Stores one row per ``(scenario_id, model, prompt_version)`` benchmark run in a
DuckDB file. Used by ``aqueduct benchmark`` (writes) and
``aqueduct benchmark diff`` / ``--gate-on-regression`` (reads).

Schema lives in its own DB (``.aqueduct/benchmark.duckdb`` by default, or
under ``<scenarios_dir>/.aqueduct/`` when scenarios are anchored to a path)
to keep benchmark history disjoint from per-blueprint observability rows.
The two stores have no foreign-key relationship — benchmark runs are not
tied to a real ``run_id``.

The store is intentionally lightweight: one table, no migrations beyond
``CREATE TABLE IF NOT EXISTS``, no schema versioning. Add migration scaffolding
here only if a future column needs back-population for existing rows.
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aqueduct.surveyor.scenario import ScenarioResult


logger = logging.getLogger(__name__)


_BENCHMARK_DDL = """
CREATE TABLE IF NOT EXISTS benchmark_results (
    id                  VARCHAR PRIMARY KEY,
    recorded_at         VARCHAR NOT NULL,
    scenario_id         VARCHAR NOT NULL,
    model               VARCHAR NOT NULL,
    prompt_version      VARCHAR,
    provider            VARCHAR,
    base_url            VARCHAR,
    passed              BOOLEAN NOT NULL,
    patch_valid         BOOLEAN NOT NULL,
    patch_applies       BOOLEAN NOT NULL,
    confidence          DOUBLE PRECISION,
    duration_seconds    DOUBLE PRECISION,
    attempts_to_parse   INTEGER,
    diag_score          DOUBLE PRECISION,
    root_cause_match    BOOLEAN,
    category_match      BOOLEAN,
    failures            JSON,
    soft_failures       JSON,
    violated_guardrails JSON,
    stop_reason         VARCHAR,
    escalated           BOOLEAN,
    tokens_in_total     INTEGER,
    tokens_out_total    INTEGER
);
CREATE INDEX IF NOT EXISTS idx_benchmark_triple
    ON benchmark_results (scenario_id, model, prompt_version, recorded_at);
"""


def default_store_path(scenarios_dir: Path) -> Path:
    """Resolve the default benchmark store path for a given scenarios target.

    Anchored to the scenarios directory (or its parent when a single file is
    passed) so each scenario suite carries its own history. Falls back to
    ``./.aqueduct/benchmark.duckdb`` when the anchor cannot be determined.
    """
    if scenarios_dir.is_file():
        anchor = scenarios_dir.parent
    else:
        anchor = scenarios_dir
    return (anchor / ".aqueduct" / "benchmark.duckdb").resolve()


# ── Connection helpers ────────────────────────────────────────────────────────

def _connect(store_path: Path):
    """Open a DuckDB connection, creating the parent dir + DDL on first use.

    Also runs idempotent additive migrations for pre-1.0.3 benchmark stores so
    older rows survive (just NULL on the new columns).
    """
    import duckdb
    store_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(store_path))
    con.execute(_BENCHMARK_DDL)
    # Additive ALTER for pre-existing stores created before
    # `violated_guardrails` landed — guardrail-compliance tracking column.
    try:
        vg_exists = con.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name='benchmark_results' AND column_name='violated_guardrails'"
        ).fetchone()
        if not vg_exists:
            con.execute("ALTER TABLE benchmark_results ADD COLUMN violated_guardrails JSON")
    except Exception:
        pass
    # Additive ALTER for stop_reason + escalation + token totals (added
    # by the unified reprompt loop) so pre-1.0.4 benchmark stores survive
    # intact (NULL on old rows).
    for col, ddl in (
        ("stop_reason", "ALTER TABLE benchmark_results ADD COLUMN stop_reason VARCHAR"),
        ("escalated", "ALTER TABLE benchmark_results ADD COLUMN escalated BOOLEAN"),
        ("tokens_in_total", "ALTER TABLE benchmark_results ADD COLUMN tokens_in_total INTEGER"),
        ("tokens_out_total", "ALTER TABLE benchmark_results ADD COLUMN tokens_out_total INTEGER"),
    ):
        try:
            exists = con.execute(
                "SELECT 1 FROM information_schema.columns "
                f"WHERE table_name='benchmark_results' AND column_name='{col}'"
            ).fetchone()
            if not exists:
                con.execute(ddl)
        except Exception:
            pass
    return con


# ── Backend abstraction (DuckDB file | Postgres schema) ─────────────────────────

import contextlib as _contextlib

# Postgres schema the benchmark table lives in (disjoint from the
# observability/lineage/depot schemas the StoreBundle uses).
_PG_BENCHMARK_SCHEMA = "benchmark"


@dataclass(frozen=True)
class BenchmarkStore:
    """Where benchmark rows live. DuckDB file or a Postgres schema.

    The whole module speaks DuckDB-flavoured SQL (`?` placeholders, `JSON`,
    `DOUBLE PRECISION`); the Postgres path reuses ``stores.postgres`` —
    ``RelationalCursor`` rewrites `?` → `%s` and the DDL is already portable.
    """

    backend: str = "duckdb"       # "duckdb" | "postgres"
    location: str = ""            # DuckDB file path OR Postgres libpq DSN
    schema: str = _PG_BENCHMARK_SCHEMA

    @classmethod
    def from_config(cls, bench_cfg: Any, scenarios_dir: Path) -> "BenchmarkStore":
        """Build from a ``stores.benchmark`` config block + scenarios anchor.

        DuckDB with no explicit path → scenario-anchored default. Postgres
        requires an explicit DSN (``path``).
        """
        backend = getattr(bench_cfg, "backend", "duckdb")
        path = getattr(bench_cfg, "path", None)
        if backend == "postgres":
            if not path:
                raise ValueError(
                    "stores.benchmark.backend=postgres requires stores.benchmark.path "
                    "(a libpq DSN, e.g. postgresql://user:pass@host/db)"
                )
            return cls(backend="postgres", location=path)
        location = path or str(default_store_path(scenarios_dir))
        return cls(backend="duckdb", location=location)

    @property
    def label(self) -> str:
        return f"postgres:{self.schema}" if self.backend == "postgres" else self.location

    @_contextlib.contextmanager
    def cursor(self):
        """Yield a cursor exposing ``execute(sql, params).fetchone()/.fetchall()``.

        DuckDB connection and ``RelationalCursor`` share that surface, so every
        query in this module is backend-agnostic. Postgres commits on exit.
        """
        if self.backend == "postgres":
            from aqueduct.stores.postgres import _pg_relational
            with _pg_relational(self.location, self.schema) as cur:
                cur.execute(_BENCHMARK_DDL)
                yield cur
        elif self.backend == "duckdb":
            con = _connect(Path(self.location))
            try:
                yield con
            finally:
                con.close()
        else:  # pragma: no cover — config Literal blocks other values
            raise ValueError(f"unknown benchmark backend: {self.backend!r}")


def _as_store(store: "Path | str | BenchmarkStore") -> BenchmarkStore:
    """Normalise a path/str (legacy DuckDB API) or a BenchmarkStore to a store."""
    if isinstance(store, BenchmarkStore):
        return store
    return BenchmarkStore(backend="duckdb", location=str(store))


# ── Persist ───────────────────────────────────────────────────────────────────

def persist_results(
    results: dict[str, dict[str, "ScenarioResult"]],
    store: "Path | str | BenchmarkStore",
) -> int:
    """Insert one row per ``(scenario, model)`` ScenarioResult into the store.

    ``store`` accepts a DuckDB path (legacy) or a :class:`BenchmarkStore`.
    Returns the number of rows written. Errors are logged but do not raise —
    benchmark output is the primary signal; persistence is best-effort so a
    locked file, missing driver, or unreachable DB cannot fail the command.
    """
    bs = _as_store(store)
    now = _dt.datetime.now(_dt.timezone.utc).isoformat()
    written = 0
    try:
        with bs.cursor() as con:
            for sid, by_model in results.items():
                for model, r in by_model.items():
                    vg = getattr(r, "violated_guardrails", None)
                    vg_json = json.dumps(list(vg)) if vg is not None else None
                    con.execute(
                        """
                        INSERT INTO benchmark_results (
                            id, recorded_at, scenario_id, model, prompt_version,
                            provider, base_url,
                            passed, patch_valid, patch_applies,
                            confidence, duration_seconds, attempts_to_parse,
                            diag_score, root_cause_match, category_match,
                            failures, soft_failures, violated_guardrails,
                            stop_reason, escalated, tokens_in_total, tokens_out_total
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            str(uuid.uuid4()),
                            now,
                            sid,
                            model,
                            r.prompt_version,
                            r.provider,
                            r.base_url,
                            r.passed,
                            r.patch_valid,
                            r.patch_applies,
                            r.confidence,
                            r.duration_seconds,
                            r.attempts_to_parse,
                            r.diag_score,
                            r.root_cause_match,
                            r.category_match,
                            json.dumps(list(r.failures)),
                            json.dumps(list(r.soft_failures)),
                            vg_json,
                            getattr(r, "stop_reason", None),
                            getattr(r, "escalated", False),
                            int(getattr(r, "tokens_in_total", 0) or 0),
                            int(getattr(r, "tokens_out_total", 0) or 0),
                        ],
                    )
                    written += 1
    except Exception as exc:  # noqa: BLE001
        logger.warning("benchmark persistence skipped — cannot write to %s: %s",
                       bs.label, exc)
        return written
    return written


# ── Diff / regression detection ───────────────────────────────────────────────

@dataclass(frozen=True)
class BenchmarkRow:
    recorded_at: str
    scenario_id: str
    model: str
    prompt_version: str | None
    passed: bool
    patch_valid: bool
    patch_applies: bool
    confidence: float | None
    duration_seconds: float | None
    attempts_to_parse: int | None
    diag_score: float | None


@dataclass(frozen=True)
class DiffEntry:
    scenario_id: str
    model: str
    baseline: BenchmarkRow | None        # None when no prior row exists
    current: BenchmarkRow
    baseline_prompt_mismatch: bool       # True when we fell back to a different prompt_version
    regressions: tuple[str, ...]         # human-readable regression descriptions
    improvements: tuple[str, ...]


def _row_from_record(rec: tuple) -> BenchmarkRow:
    return BenchmarkRow(
        recorded_at=rec[0],
        scenario_id=rec[1],
        model=rec[2],
        prompt_version=rec[3],
        passed=bool(rec[4]),
        patch_valid=bool(rec[5]),
        patch_applies=bool(rec[6]),
        confidence=rec[7],
        duration_seconds=rec[8],
        attempts_to_parse=rec[9],
        diag_score=rec[10],
    )


_SELECT_COLS = (
    "recorded_at, scenario_id, model, prompt_version, "
    "passed, patch_valid, patch_applies, "
    "confidence, duration_seconds, attempts_to_parse, diag_score"
)


def _fetch_baseline(
    con,
    scenario_id: str,
    model: str,
    prompt_version: str | None,
    current_recorded_at: str,
) -> tuple[BenchmarkRow | None, bool]:
    """Return ``(baseline_row, prompt_mismatch)`` for the most recent prior row.

    Lookup order:
      1. Most recent row matching the full triple (scenario, model, prompt_version)
         BEFORE the current recorded_at — exact baseline.
      2. Most recent row matching (scenario, model) regardless of prompt_version
         BEFORE the current recorded_at — fallback baseline, prompt_mismatch=True.
      3. None — no prior row exists for this pair at all.
    """
    if prompt_version is not None:
        rec = con.execute(
            f"SELECT {_SELECT_COLS} FROM benchmark_results "
            "WHERE scenario_id = ? AND model = ? AND prompt_version = ? "
            "AND recorded_at < ? "
            "ORDER BY recorded_at DESC LIMIT 1",
            [scenario_id, model, prompt_version, current_recorded_at],
        ).fetchone()
        if rec is not None:
            return _row_from_record(rec), False

    rec = con.execute(
        f"SELECT {_SELECT_COLS} FROM benchmark_results "
        "WHERE scenario_id = ? AND model = ? AND recorded_at < ? "
        "ORDER BY recorded_at DESC LIMIT 1",
        [scenario_id, model, current_recorded_at],
    ).fetchone()
    if rec is None:
        return None, False
    return _row_from_record(rec), True


# Threshold for flagging diag_score as regressed. Tuned for low false-positive
# rate; small noise from a single rerun should NOT trigger gating.
_DIAG_DROP_THRESHOLD = 0.05         # diag_score must drop by > 5pp


def _compare(baseline: BenchmarkRow, current: BenchmarkRow) -> tuple[list[str], list[str]]:
    """Compare two BenchmarkRows. Returns (regressions, improvements) lists.

    LLM self-reported `confidence` is deliberately NOT part of the gate:
    LLMs are systematically overconfident (RLHF tilts toward assertive
    answers), confidence is a generated token not a measured posterior,
    and absolute thresholds across runs / models produce noise rather
    than signal. The column is still persisted on every row for
    inspection / debugging — it just does not flip the CI gate.
    """
    regressions: list[str] = []
    improvements: list[str] = []

    if baseline.passed and not current.passed:
        regressions.append("passed: True → False")
    elif not baseline.passed and current.passed:
        improvements.append("passed: False → True")

    if baseline.patch_applies and not current.patch_applies:
        regressions.append("patch_applies: True → False")
    elif not baseline.patch_applies and current.patch_applies:
        improvements.append("patch_applies: False → True")

    if baseline.patch_valid and not current.patch_valid:
        regressions.append("patch_valid: True → False")
    elif not baseline.patch_valid and current.patch_valid:
        improvements.append("patch_valid: False → True")

    if baseline.diag_score is not None and current.diag_score is not None:
        delta = current.diag_score - baseline.diag_score
        if delta < -_DIAG_DROP_THRESHOLD:
            regressions.append(
                f"diag_score: {baseline.diag_score:.2f} → {current.diag_score:.2f}"
            )
        elif delta > _DIAG_DROP_THRESHOLD:
            improvements.append(
                f"diag_score: {baseline.diag_score:.2f} → {current.diag_score:.2f}"
            )

    return regressions, improvements


def diff_latest(
    results: dict[str, dict[str, "ScenarioResult"]],
    store: "Path | str | BenchmarkStore",
) -> list[DiffEntry]:
    """Compare every ``(scenario, model)`` in ``results`` against its baseline.

    ``store`` accepts a DuckDB path (legacy) or a :class:`BenchmarkStore`.
    Returns one ``DiffEntry`` per pair. An entry with ``baseline is None``
    means this is the first ever benchmark for that pair — never treated as a
    regression. An entry with ``baseline_prompt_mismatch=True`` means the
    baseline was recorded under a different ``prompt_version``; comparison
    still runs but the caller may choose to soften the gate.

    Reads only — does not write. Call ``persist_results`` first so the current
    run's rows are in the store; this function then looks back ONE step.
    """
    bs = _as_store(store)
    entries: list[DiffEntry] = []
    try:
        with bs.cursor() as con:
            for sid, by_model in results.items():
                for model, r in by_model.items():
                    # Most recent persisted row for this pair — the "current"
                    # row we just wrote.
                    rec = con.execute(
                        f"SELECT {_SELECT_COLS} FROM benchmark_results "
                        "WHERE scenario_id = ? AND model = ? "
                        "ORDER BY recorded_at DESC LIMIT 1",
                        [sid, model],
                    ).fetchone()
                    if rec is None:
                        # Persistence skipped (locked file, dep missing); nothing to diff.
                        continue
                    current = _row_from_record(rec)
                    baseline, prompt_mismatch = _fetch_baseline(
                        con,
                        scenario_id=sid,
                        model=model,
                        prompt_version=current.prompt_version,
                        current_recorded_at=current.recorded_at,
                    )
                    if baseline is None:
                        entries.append(DiffEntry(
                            scenario_id=sid,
                            model=model,
                            baseline=None,
                            current=current,
                            baseline_prompt_mismatch=False,
                            regressions=(),
                            improvements=(),
                        ))
                        continue
                    regs, imps = _compare(baseline, current)
                    entries.append(DiffEntry(
                        scenario_id=sid,
                        model=model,
                        baseline=baseline,
                        current=current,
                        baseline_prompt_mismatch=prompt_mismatch,
                        regressions=tuple(regs),
                        improvements=tuple(imps),
                    ))
    except Exception as exc:  # noqa: BLE001
        logger.warning("benchmark diff skipped — cannot read %s: %s", bs.label, exc)
        return []
    return entries


def has_regressions(entries: list[DiffEntry]) -> bool:
    """True if any DiffEntry contains at least one regression."""
    return any(e.regressions for e in entries)


def format_diff_table(entries: list[DiffEntry]) -> str:
    """Terminal-friendly summary of a list of DiffEntry."""
    if not entries:
        return "(no diff entries — store empty or no current results)"
    lines: list[str] = []
    lines.append(
        f"  {'scenario':<32} {'model':<24} {'status':<12} notes"
    )
    lines.append("  " + "─" * 90)
    for e in entries:
        if e.baseline is None:
            status = "NEW"
            notes = "first benchmark for this pair"
        elif e.regressions:
            status = "✗ REGRESS"
            notes = "; ".join(e.regressions)
            if e.baseline_prompt_mismatch:
                notes = f"[baseline prompt_version differs] {notes}"
        elif e.improvements:
            status = "↑ IMPROVE"
            notes = "; ".join(e.improvements)
        else:
            status = "= same"
            notes = ""
            if e.baseline_prompt_mismatch:
                notes = "[baseline prompt_version differs]"
        sid = (e.scenario_id[:30] + "..") if len(e.scenario_id) > 32 else e.scenario_id
        model = (e.model[:22] + "..") if len(e.model) > 24 else e.model
        lines.append(f"  {sid:<32} {model:<24} {status:<12} {notes}")
    return "\n".join(lines)


# ── Aggregation / stats views ───────────────────────────────────────────────────

# Latest row per (scenario, model) — history is not double-counted in the
# leaderboard / difficulty views. Window functions work on DuckDB and Postgres.
_RANKED_CTE = (
    "WITH ranked AS ("
    " SELECT scenario_id, model, passed, patch_valid, patch_applies,"
    " diag_score, duration_seconds,"
    " ROW_NUMBER() OVER (PARTITION BY scenario_id, model"
    " ORDER BY recorded_at DESC) AS rn"
    " FROM benchmark_results)"
)


def _f(v: Any) -> float | None:
    return float(v) if v is not None else None


def compute_stats(store: "Path | str | BenchmarkStore", *, trend_limit: int = 14) -> dict:
    """Aggregate the store into leaderboard / difficulty / trend views.

    Leaderboard + difficulty use the LATEST row per (scenario, model). Trend
    aggregates pass-rate by calendar day across all rows. Returns empty lists
    when the store is empty or unreadable — never raises.
    """
    bs = _as_store(store)
    out: dict = {"models": [], "scenarios": [], "trend": []}
    try:
        with bs.cursor() as con:
            for r in con.execute(
                _RANKED_CTE +
                " SELECT model, COUNT(*) n,"
                " AVG(CASE WHEN passed THEN 1.0 ELSE 0.0 END),"
                " AVG(CASE WHEN patch_valid THEN 1.0 ELSE 0.0 END),"
                " AVG(CASE WHEN patch_applies THEN 1.0 ELSE 0.0 END),"
                " AVG(diag_score), AVG(duration_seconds)"
                " FROM ranked WHERE rn = 1 GROUP BY model"
                " ORDER BY 3 DESC, model"
            ).fetchall():
                out["models"].append({
                    "model": r[0], "n": int(r[1]), "pass_rate": _f(r[2]),
                    "parse_rate": _f(r[3]), "apply_rate": _f(r[4]),
                    "avg_diag": _f(r[5]), "avg_duration": _f(r[6]),
                })
            for r in con.execute(
                _RANKED_CTE +
                " SELECT scenario_id, COUNT(*) n,"
                " AVG(CASE WHEN passed THEN 1.0 ELSE 0.0 END)"
                " FROM ranked WHERE rn = 1 GROUP BY scenario_id"
                " ORDER BY 3 ASC, scenario_id"
            ).fetchall():
                out["scenarios"].append({
                    "scenario_id": r[0], "n": int(r[1]), "pass_rate": _f(r[2]),
                })
            trend = con.execute(
                " SELECT substr(recorded_at, 1, 10) d, COUNT(*) n,"
                " AVG(CASE WHEN passed THEN 1.0 ELSE 0.0 END)"
                " FROM benchmark_results GROUP BY substr(recorded_at, 1, 10)"
                " ORDER BY d DESC LIMIT ?",
                [trend_limit],
            ).fetchall()
            out["trend"] = [
                {"date": r[0], "n": int(r[1]), "pass_rate": _f(r[2])}
                for r in reversed(trend)
            ]
    except Exception as exc:  # noqa: BLE001
        logger.warning("benchmark stats unavailable — %s: %s", bs.label, exc)
        return {"models": [], "scenarios": [], "trend": []}
    return out


def format_stats(stats: dict) -> str:
    """Terminal-friendly render of :func:`compute_stats` output."""
    models = stats.get("models") or []
    scenarios = stats.get("scenarios") or []
    trend = stats.get("trend") or []
    if not models and not scenarios:
        return "(benchmark store empty — run `aqueduct benchmark` first)"

    def pct(x: float | None) -> str:
        return f"{x:.0%}" if x is not None else "—"

    def dur(x: float | None) -> str:
        return f"{x:.1f}s" if x is not None else "—"

    out: list[str] = []
    out.append("Model leaderboard  (latest row per scenario × model)")
    out.append(f"  {'model':<28} {'n':>3} {'pass':>6} {'parse':>6} {'apply':>6} {'diag':>6} {'dur':>7}")
    for m in models:
        out.append(
            f"  {m['model'][:28]:<28} {m['n']:>3} {pct(m['pass_rate']):>6} "
            f"{pct(m['parse_rate']):>6} {pct(m['apply_rate']):>6} "
            f"{pct(m['avg_diag']):>6} {dur(m['avg_duration']):>7}"
        )
    if models:
        best = models[0]
        out.append(f"  → best model: {best['model']}  ({pct(best['pass_rate'])} pass rate)")

    out.append("")
    out.append("Hardest scenarios  (lowest pass rate across models)")
    out.append(f"  {'scenario':<44} {'n':>3} {'pass':>6}")
    for s in scenarios[:10]:
        out.append(f"  {s['scenario_id'][:44]:<44} {s['n']:>3} {pct(s['pass_rate']):>6}")

    if trend:
        out.append("")
        out.append("Pass-rate trend  (by day)")
        for t in trend:
            out.append(f"  {t['date']}  {pct(t['pass_rate']):>6}   (n={t['n']})")
    return "\n".join(out)
