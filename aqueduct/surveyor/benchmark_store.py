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
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aqueduct.surveyor.scenario import ScenarioResult


logger = logging.getLogger(__name__)


_BENCHMARK_DDL = """
CREATE TABLE IF NOT EXISTS benchmark_results (
    id                VARCHAR PRIMARY KEY,
    recorded_at       VARCHAR NOT NULL,
    scenario_id       VARCHAR NOT NULL,
    model             VARCHAR NOT NULL,
    prompt_version    VARCHAR,
    provider          VARCHAR,
    base_url          VARCHAR,
    passed            BOOLEAN NOT NULL,
    patch_valid       BOOLEAN NOT NULL,
    patch_applies     BOOLEAN NOT NULL,
    confidence        DOUBLE PRECISION,
    duration_seconds  DOUBLE PRECISION,
    attempts_to_parse INTEGER,
    diag_score        DOUBLE PRECISION,
    root_cause_match  BOOLEAN,
    category_match    BOOLEAN,
    failures          JSON,
    soft_failures     JSON
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
    """Open a DuckDB connection, creating the parent dir + DDL on first use."""
    import duckdb
    store_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(store_path))
    con.execute(_BENCHMARK_DDL)
    return con


# ── Persist ───────────────────────────────────────────────────────────────────

def persist_results(
    results: dict[str, dict[str, "ScenarioResult"]],
    store_path: Path,
) -> int:
    """Insert one row per ``(scenario, model)`` ScenarioResult into the store.

    Returns the number of rows written. Errors are logged but do not raise —
    benchmark output is the primary signal; persistence is best-effort so a
    locked file or missing duckdb dep cannot fail the benchmark command.
    """
    try:
        con = _connect(store_path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("benchmark persistence skipped — cannot open %s: %s",
                       store_path, exc)
        return 0

    now = _dt.datetime.now(_dt.timezone.utc).isoformat()
    written = 0
    try:
        for sid, by_model in results.items():
            for model, r in by_model.items():
                con.execute(
                    """
                    INSERT INTO benchmark_results (
                        id, recorded_at, scenario_id, model, prompt_version,
                        provider, base_url,
                        passed, patch_valid, patch_applies,
                        confidence, duration_seconds, attempts_to_parse,
                        diag_score, root_cause_match, category_match,
                        failures, soft_failures
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    ],
                )
                written += 1
    finally:
        con.close()
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


# Thresholds that flag a metric as regressed. Tuned for low false-positive
# rate; small noise from a single rerun should NOT trigger gating.
_DIAG_DROP_THRESHOLD = 0.05         # diag_score must drop by > 5pp
_CONFIDENCE_DROP_THRESHOLD = 0.05   # confidence must drop by > 5pp


def _compare(baseline: BenchmarkRow, current: BenchmarkRow) -> tuple[list[str], list[str]]:
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

    if baseline.confidence is not None and current.confidence is not None:
        delta = current.confidence - baseline.confidence
        if delta < -_CONFIDENCE_DROP_THRESHOLD:
            regressions.append(
                f"confidence: {baseline.confidence:.2f} → {current.confidence:.2f}"
            )

    return regressions, improvements


def diff_latest(
    results: dict[str, dict[str, "ScenarioResult"]],
    store_path: Path,
) -> list[DiffEntry]:
    """Compare every ``(scenario, model)`` in ``results`` against its baseline.

    Returns one ``DiffEntry`` per pair. An entry with ``baseline is None``
    means this is the first ever benchmark for that pair — never treated as a
    regression. An entry with ``baseline_prompt_mismatch=True`` means the
    baseline was recorded under a different ``prompt_version``; comparison
    still runs but the caller may choose to soften the gate.

    Reads only — does not write. Call ``persist_results`` first so the current
    run's rows are in the store; this function then looks back ONE step.
    """
    try:
        con = _connect(store_path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("benchmark diff skipped — cannot open %s: %s", store_path, exc)
        return []

    entries: list[DiffEntry] = []
    try:
        for sid, by_model in results.items():
            for model, r in by_model.items():
                # Look up the most recent persisted row for this pair — that's
                # the "current" row we just wrote.
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
    finally:
        con.close()
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
