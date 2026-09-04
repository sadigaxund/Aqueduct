"""Perf-regression attribution for an applied heal patch — WARN-ONLY.

**The hole this closes.** A ``healed_by`` record's ``validated_on`` is
BINARY: the run after the patch either succeeded or it did not. Config-op
success is not binary. The most common outcome of naive shuffle/partition
tuning is "completes, but much slower", and ``validated_on`` records that
as an unqualified success. The patch then persists into the Blueprint and
every later run inherits it, with nothing anywhere that says the pipeline
got slower the day it was applied.

This module snapshots a BASELINE run's metrics when the patch is applied
and, on the first green run per engine afterwards, records what changed.
It never blocks, never fails a run, never changes acceptance, and never
converts an observation into a verdict.

**No threshold, deliberately.** There is no measurement behind "3x is a
regression" — the honest ratio at which a change matters depends on the
pipeline, the SLA, and the cluster, none of which this code can see.
AGENTS.md's "measure the hazard before building the guard" rule applies
directly: a number invented here would be carried forever and read as
authoritative. So this module reports ``duration_ratio`` and the two raw
durations and stops. ``status`` is ``observed`` or ``not_applicable`` —
there is no ``pass``, because nothing was judged, and no ``fail``, because
nothing can fail.

**Comparability is the real hazard, and it is stated in the note itself.**
Two runs of the same Blueprint are not interchangeable: a run over ten
times the input data is slower for reasons that have nothing to do with
the patch. Three things are done about that rather than one:

  1. *Engine match is required.* ``run_records`` has no ``engine`` column,
     but its ``module_results`` JSON carries a per-module ``engine`` (the
     fully-resolved post-compile value the Surveyor stamps on every run),
     so the set of engines a run touched is derivable from what is really
     recorded. A baseline whose engine set differs from the observing
     run's is refused outright — ``not_applicable``, never a comparison.
  2. *Volume is reported when the engine records it.* ``module_metrics``
     carries ``records_read``/``bytes_read``. Spark's executor writes a
     full row per module; DuckDB's writes rows only for Handoff modules.
     So the volume proxy is present on one engine and absent on the other
     — it is reported as ``None`` with an explicit
     ``"not recorded on this engine"`` sentence in ``caveats``, never as
     a zero and never as an omitted line that would read as "volume was
     unchanged".
  3. *Everything unresolved is written into ``caveats``*, which travels
     with the observation into the Blueprint. A note that implies
     causation it cannot support is worse than no note, so the note
     carries its own limits: co-applied patches, missing volume data,
     and the standing fact that wall-clock time is not attributable to a
     single cause.

**What is measured.** Wall-clock run duration
(``run_records.finished_at - started_at``) is the only metric every engine
records, so it is the comparison. Shuffle bytes, spill, and stage counts
are Spark concepts with no DuckDB counterpart and are not attempted here.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from aqueduct.executor.models import SUCCEEDED_STATUSES

logger = logging.getLogger(__name__)

__all__ = [
    "PerfObservation",
    "RunPerf",
    "capture_baseline_perf",
    "capture_run_perf",
    "compare_perf",
    "run_engines",
]

# Terminal run statuses that count as GREEN. `patched` is a run that
# succeeded after a patch was applied mid-run (see ExecutionStatus), so it
# is a legitimate baseline and a legitimate observation point. Aliases the
# shared tuple beside the enum (`executor/models.py`) rather than restating
# it — the handoff-spill retention rule reads the same set to decide when a
# kept-failure spill has been superseded, and the two must not drift.
_GREEN_STATUSES: tuple[str, ...] = SUCCEEDED_STATUSES

# Volume proxy is unavailable on any engine that writes no `module_metrics`
# rows. Stated in full rather than left as a None the reader has to
# interpret — see this module's docstring, point 2.
VOLUME_UNAVAILABLE = (
    "input-volume proxy not recorded on this engine, so this comparison "
    "cannot separate a patch effect from a change in input data volume"
)

WALL_CLOCK_CAVEAT = (
    "wall-clock duration is not attributable to a single cause: cluster "
    "contention, cache state, and source-system latency all move it"
)


@dataclass(frozen=True)
class RunPerf:
    """One run's engine-agnostic performance snapshot.

    ``duration_ms`` is the only field guaranteed on every engine.
    ``records_read``/``bytes_read`` are volume PROXIES, not semantic input
    counts: they sum ``module_metrics`` across the whole run, so a value is
    comparable between two runs of the SAME Blueprint and meaningless
    across different ones. ``None`` means the engine recorded nothing —
    never zero (the falsy-trap rule: a genuine zero-row run is a different
    fact from an unmeasured one).
    """

    run_id: str
    status: str
    started_at: str
    finished_at: str
    duration_ms: int
    engines: tuple[str, ...]
    records_read: int | None = None
    bytes_read: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_ms": self.duration_ms,
            "engines": list(self.engines),
            "records_read": self.records_read,
            "bytes_read": self.bytes_read,
        }


@dataclass(frozen=True)
class PerfObservation:
    """One warn-only perf note for one applied patch on one engine.

    ``status`` vocabulary follows ``aqueduct.patch.preview.LineageGateResult``
    and ``aqueduct.patch.config_delta.EngineConfigDeltaResult``:

      ``observed``        both runs' durations are known and comparable
                          (same engine set) — ``duration_ratio`` and the
                          two durations are reported. NOT ``pass``: no
                          threshold exists, so nothing was judged.
      ``not_applicable``  no comparison was possible — no baseline run, no
                          observability store, a baseline on a different
                          engine, or a missing duration. NEVER ``pass``,
                          which would claim a check that had nothing to
                          look at (the same reason those two siblings
                          carry the member).

    There is no ``fail`` member and no ``warn`` member. This is warn-only
    in the strict sense: it reports, a human judges.
    """

    status: str
    detail: str
    engine: str = ""
    observed_at: str = ""
    baseline: dict[str, Any] = field(default_factory=dict)
    current: dict[str, Any] = field(default_factory=dict)
    duration_ratio: float | None = None
    duration_delta_ms: int | None = None
    co_applied_patches: int = 1
    caveats: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serialize for the ``healed_by.perf_observations`` Blueprint block.

        Empty sub-dicts are kept out so a ``not_applicable`` note stays two
        lines of YAML rather than a skeleton of nulls.
        """
        out: dict[str, Any] = {
            "status": self.status,
            "detail": self.detail,
            "engine": self.engine,
            "observed_at": self.observed_at,
        }
        if self.baseline:
            out["baseline"] = self.baseline
        if self.current:
            out["current"] = self.current
        if self.duration_ratio is not None:
            out["duration_ratio"] = self.duration_ratio
        if self.duration_delta_ms is not None:
            out["duration_delta_ms"] = self.duration_delta_ms
        if self.co_applied_patches != 1:
            out["co_applied_patches"] = self.co_applied_patches
        if self.caveats:
            out["caveats"] = list(self.caveats)
        return out


# ── reading what is really recorded ───────────────────────────────────────


def run_engines(module_results_json: Any) -> tuple[str, ...]:
    """Engines a run touched, from ``run_records.module_results``.

    ``run_records`` carries no ``engine`` column; its ``module_results``
    JSON does, per module (``Surveyor.record`` stamps the fully-resolved
    post-compile ``Module.engine`` on every module of every run, polyglot
    or not). Deriving the set here reads what is really recorded instead of
    adding a column for a fact already present.

    An empty tuple means "this run does not say" — a row written before
    per-module engine stamping existed, or an unparseable payload. Callers
    must treat that as unknown, never as a match.
    """
    if isinstance(module_results_json, str):
        try:
            module_results_json = json.loads(module_results_json)
        except (ValueError, TypeError):
            return ()
    if not isinstance(module_results_json, list):
        return ()
    engines = {
        row.get("engine")
        for row in module_results_json
        if isinstance(row, dict) and row.get("engine")
    }
    return tuple(sorted(str(e) for e in engines))


def _parse_ts(value: Any) -> datetime | None:
    """A stored ``run_records`` timestamp as a datetime, or None.

    Both backends return a ``TIMESTAMPTZ`` column as a Python ``datetime``;
    the string branch covers a store that hands back text. Deliberately NOT
    read via ``CAST(finished_at AS VARCHAR)`` the way the display queries in
    ``stores/queries.py`` do: DuckDB renders that cast in the SESSION's
    local timezone with a space separator (``2026-08-14 13:00:50+04``), so
    the text is neither UTC nor ISO-8601 — fine to print, wrong to compare
    or to persist into a Blueprint that will be read on another machine.
    """
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _iso_utc(value: Any) -> str:
    """Normalize a stored timestamp to a UTC ISO-8601 string for the record.

    The ``perf_baseline`` block is written into the Blueprint and read
    wherever that Blueprint travels, so a session-local rendering would be
    ambiguous the moment the file leaves the machine that produced it.
    """
    parsed = _parse_ts(value)
    if parsed is None:
        return str(value or "")
    if parsed.tzinfo is None:
        return parsed.isoformat()
    return parsed.astimezone(UTC).isoformat()


def _duration_ms(started_at: Any, finished_at: Any) -> int | None:
    """Wall-clock milliseconds between two stored timestamps, or None.

    Returns None — never 0 — for anything unparseable or negative: a zero
    would read as an instant run.
    """
    start, end = _parse_ts(started_at), _parse_ts(finished_at)
    if start is None or end is None:
        return None
    try:
        delta_ms = int((end - start).total_seconds() * 1000)
    except (TypeError, ValueError, OverflowError):
        return None
    return delta_ms if delta_ms >= 0 else None


def _volume_proxy(cur: Any, run_id: str) -> tuple[int | None, int | None]:
    """``(records_read, bytes_read)`` summed over a run's ``module_metrics``.

    ``(None, None)`` when the engine wrote no rows — DuckDB writes
    ``module_metrics`` only for Handoff modules, so this is the normal
    state there and is reported as unavailable, never as zero.
    """
    try:
        cur.execute(
            "SELECT SUM(records_read), SUM(bytes_read) FROM module_metrics WHERE run_id = ?",
            [run_id],
        )
        row = cur.fetchone()
    except Exception:
        # The table does not exist until some engine writes to it — a
        # perfectly normal state for a DuckDB-only install, and the reason
        # this is a debug log rather than a warning.
        logger.debug("module_metrics volume proxy unavailable for run %s", run_id, exc_info=True)
        return (None, None)
    if not row:
        return (None, None)
    records = int(row[0]) if row[0] is not None else None
    read_bytes = int(row[1]) if row[1] is not None else None
    return (records, read_bytes)


def _row_to_run_perf(cur: Any, row: Any) -> RunPerf | None:
    """``(run_id, status, started_at, finished_at, module_results)`` -> RunPerf."""
    run_id, status, started_at, finished_at, module_results = row
    duration = _duration_ms(started_at, finished_at)
    if duration is None:
        return None
    records, read_bytes = _volume_proxy(cur, run_id)
    return RunPerf(
        run_id=str(run_id),
        status=str(status),
        started_at=_iso_utc(started_at),
        finished_at=_iso_utc(finished_at),
        duration_ms=duration,
        engines=run_engines(module_results),
        records_read=records,
        bytes_read=read_bytes,
    )


# Raw timestamp columns, deliberately NOT cast to VARCHAR — see _parse_ts.
_RUN_COLUMNS = "run_id, status, started_at, finished_at, module_results"


def capture_run_perf(obs_store: Any, run_id: str) -> RunPerf | None:
    """One specific run's perf snapshot, or None.

    Best-effort by contract: every caller is on a path that must not fail
    because observability is unavailable, so a missing store, a missing
    table, or an unfinished run all yield None rather than raising.
    """
    if obs_store is None or not run_id:
        return None
    try:
        with obs_store.connect() as cur:
            cur.execute(
                f"SELECT {_RUN_COLUMNS} FROM run_records "  # noqa: S608 - constant column list
                "WHERE run_id = ? AND finished_at IS NOT NULL",
                [run_id],
            )
            row = cur.fetchone()
            if not row:
                return None
            return _row_to_run_perf(cur, row)
    except Exception:
        logger.debug("capture_run_perf failed for run %s", run_id, exc_info=True)
        return None


def capture_baseline_perf(
    obs_store: Any, blueprint_id: str, before: str | None = None
) -> RunPerf | None:
    """The most recent GREEN run of *blueprint_id* finishing before *before*.

    This is the "what was it like before the patch" half of the
    comparison, captured at APPLY time and snapshotted into the
    ``healed_by`` record. Snapshotting rather than re-deriving later is
    deliberate: the record travels with the Blueprint, while the
    observability store is local, prunable, and may not exist at all on
    the machine that reads the provenance block.

    *before* is the patch's ``applied_at``; ``None`` means "no upper
    bound" (the newest green run). A blueprint whose every previous run
    failed — the common case when healing a pipeline that never worked —
    has no baseline, and None here is what makes the eventual note
    ``not_applicable`` rather than a comparison against nothing.
    """
    if obs_store is None or not blueprint_id:
        return None
    placeholders = ", ".join("?" for _ in _GREEN_STATUSES)
    params: list[Any] = [blueprint_id, *_GREEN_STATUSES]
    time_filter = ""
    if before:
        # Compared as a TIMESTAMPTZ, never as text: `finished_at` is a
        # TIMESTAMPTZ column and *before* is a UTC ISO string, so a string
        # comparison would be against DuckDB's session-local rendering of
        # the column — a different timezone AND a different separator, both
        # of which silently mis-order same-day runs.
        time_filter = "AND finished_at < CAST(? AS TIMESTAMPTZ) "
        params.append(before)
    try:
        with obs_store.connect() as cur:
            cur.execute(
                f"SELECT {_RUN_COLUMNS} FROM run_records "  # noqa: S608 - placeholders only
                f"WHERE blueprint_id = ? AND status IN ({placeholders}) "
                f"AND finished_at IS NOT NULL {time_filter}"
                "ORDER BY finished_at DESC LIMIT 1",
                params,
            )
            row = cur.fetchone()
            if not row:
                return None
            return _row_to_run_perf(cur, row)
    except Exception:
        logger.debug("capture_baseline_perf failed for %s", blueprint_id, exc_info=True)
        return None


# ── the comparison ─────────────────────────────────────────────────────────


def compare_perf(
    *,
    baseline: dict[str, Any] | None,
    current: RunPerf | None,
    engine: str,
    observed_at: str,
    co_applied_patches: int = 1,
) -> PerfObservation:
    """Compare a snapshotted *baseline* against the *current* green run.

    *baseline* is the plain dict stored in the ``healed_by`` record (the
    output of ``RunPerf.to_dict``), not a ``RunPerf``, because that is what
    survives a YAML round-trip.

    Every refusal path returns ``not_applicable`` with a ``detail`` saying
    which fact was missing. None of them return ``observed`` with a
    fabricated number, and none return ``pass``.
    """
    if current is None:
        return PerfObservation(
            status="not_applicable",
            detail="the current run's duration was not recorded",
            engine=engine,
            observed_at=observed_at,
        )
    if not baseline:
        return PerfObservation(
            status="not_applicable",
            detail=(
                "no green run of this blueprint was recorded before the patch "
                "was applied, so there is nothing to compare against"
            ),
            engine=engine,
            observed_at=observed_at,
            current=current.to_dict(),
        )

    base_duration = baseline.get("duration_ms")
    if not isinstance(base_duration, int | float) or base_duration <= 0:
        return PerfObservation(
            status="not_applicable",
            detail="the baseline run's duration was not recorded",
            engine=engine,
            observed_at=observed_at,
            baseline=dict(baseline),
            current=current.to_dict(),
        )

    base_engines = tuple(baseline.get("engines") or ())
    if not base_engines or not current.engines:
        return PerfObservation(
            status="not_applicable",
            detail=(
                "the engine of the baseline run or of this run was not "
                "recorded, so the two are not known to be comparable"
            ),
            engine=engine,
            observed_at=observed_at,
            baseline=dict(baseline),
            current=current.to_dict(),
        )
    if tuple(base_engines) != tuple(current.engines):
        return PerfObservation(
            status="not_applicable",
            detail=(
                f"baseline ran on {list(base_engines)} and this run ran on "
                f"{list(current.engines)} — a cross-engine duration "
                "comparison says nothing about the patch"
            ),
            engine=engine,
            observed_at=observed_at,
            baseline=dict(baseline),
            current=current.to_dict(),
        )

    ratio = round(current.duration_ms / float(base_duration), 3)
    delta = current.duration_ms - int(base_duration)

    caveats = [WALL_CLOCK_CAVEAT]
    base_records = baseline.get("records_read")
    if base_records is None or current.records_read is None:
        caveats.append(VOLUME_UNAVAILABLE)
    elif base_records != current.records_read:
        caveats.append(
            f"input volume changed between the two runs "
            f"({base_records} -> {current.records_read} records read), so the "
            "duration change is not attributable to the patch alone"
        )
    if co_applied_patches > 1:
        caveats.append(
            f"{co_applied_patches} patches were applied to this blueprint "
            "since the baseline run, so any change is shared between them"
        )

    return PerfObservation(
        status="observed",
        detail=(
            f"this run took {current.duration_ms} ms against a baseline of "
            f"{int(base_duration)} ms ({ratio}x). Reported, not judged: "
            "Aqueduct sets no regression threshold"
        ),
        engine=engine,
        observed_at=observed_at,
        baseline=dict(baseline),
        current=current.to_dict(),
        duration_ratio=ratio,
        duration_delta_ms=delta,
        co_applied_patches=co_applied_patches,
        caveats=caveats,
    )
