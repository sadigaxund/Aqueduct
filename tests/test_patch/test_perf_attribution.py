"""Warn-only perf attribution (`aqueduct/patch/perf_attribution.py`).

The properties under test are the ones the feature would be dangerous
without: that it never invents a verdict, that it refuses a comparison it
cannot justify instead of producing a number, and that an engine which
cannot report a metric is told about, not silently zeroed.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from aqueduct.executor.models import MODULE_METRICS_DDL
from aqueduct.patch.apply import stamp_perf_observation
from aqueduct.patch.perf_attribution import (
    VOLUME_UNAVAILABLE,
    RunPerf,
    capture_baseline_perf,
    capture_run_perf,
    compare_perf,
    run_engines,
)
from aqueduct.stores.duckdb_ import DuckDBObservabilityStore
from aqueduct.surveyor.ddl import _DDL

pytestmark = pytest.mark.unit

_NOW = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)


def _module_results(engine: str | None) -> str:
    return json.dumps([
        {"module_id": "src", "status": "success", "error": None, "engine": engine},
    ])


@pytest.fixture
def store(tmp_path):
    st = DuckDBObservabilityStore(tmp_path / "obs.db")
    with st.connect() as cur:
        cur.execute(_DDL)
        cur.execute(MODULE_METRICS_DDL)
    return st


def _add_run(store, run_id, *, status="success", start, duration_s,
             engine="spark", records=None):
    with store.connect() as cur:
        cur.execute(
            "INSERT INTO run_records (run_id, blueprint_id, status, started_at, "
            "finished_at, module_results, parent_run_id) VALUES (?,?,?,?,?,?,NULL)",
            [run_id, "bp", status, start.isoformat(),
             (start + timedelta(seconds=duration_s)).isoformat(),
             _module_results(engine)],
        )
        if records is not None:
            cur.execute(
                "INSERT INTO module_metrics (run_id, module_id, records_read, "
                "bytes_read, records_written, bytes_written, duration_ms, captured_at)"
                " VALUES (?,?,?,?,?,?,?,?)",
                [run_id, "src", records, records * 10, records, records * 10,
                 duration_s * 1000, _NOW.isoformat()],
            )


# ── engine derivation from what is really recorded ─────────────────────────


class TestRunEngines:
    def test_reads_engine_from_module_results_json(self):
        assert run_engines(_module_results("duckdb")) == ("duckdb",)

    def test_accepts_an_already_parsed_list(self):
        assert run_engines([{"module_id": "a", "engine": "spark"}]) == ("spark",)

    def test_polyglot_run_reports_every_engine_sorted(self):
        payload = json.dumps([
            {"module_id": "a", "engine": "spark"},
            {"module_id": "b", "engine": "duckdb"},
        ])
        assert run_engines(payload) == ("duckdb", "spark")

    def test_unstamped_row_is_unknown_not_a_default_engine(self):
        """An older row with no per-module engine must read as UNKNOWN.

        Defaulting it to any engine name would let a Spark baseline be
        compared against a DuckDB run — the exact comparison the gate
        exists to refuse.
        """
        assert run_engines(_module_results(None)) == ()
        assert run_engines("not json at all") == ()
        assert run_engines(None) == ()


# ── reading the store ──────────────────────────────────────────────────────


class TestCapture:
    def test_baseline_is_the_latest_green_run_before_the_cutoff(self, store):
        _add_run(store, "old", start=_NOW - timedelta(hours=5), duration_s=10)
        _add_run(store, "recent", start=_NOW - timedelta(hours=2), duration_s=20)
        _add_run(store, "after", start=_NOW + timedelta(hours=1), duration_s=30)
        got = capture_baseline_perf(store, "bp", before=_NOW.isoformat())
        assert got is not None
        assert got.run_id == "recent"
        assert got.duration_ms == 20_000

    def test_a_failed_run_is_never_a_baseline(self, store):
        _add_run(store, "green", start=_NOW - timedelta(hours=5), duration_s=10)
        _add_run(store, "red", status="error",
                 start=_NOW - timedelta(hours=1), duration_s=2)
        got = capture_baseline_perf(store, "bp", before=_NOW.isoformat())
        assert got is not None
        assert got.run_id == "green"

    def test_cutoff_orders_same_day_runs_correctly(self, store):
        """The cutoff is a TIMESTAMPTZ comparison, not a string one.

        DuckDB renders `CAST(finished_at AS VARCHAR)` in the session's
        local timezone with a space separator, so comparing that text
        against a UTC ISO string mis-orders every same-day run (' ' < 'T'
        makes any same-day row look earlier). Two runs an hour apart on
        one day is exactly the case that catches it.
        """
        _add_run(store, "before_cut", start=_NOW - timedelta(hours=2), duration_s=10)
        _add_run(store, "after_cut", start=_NOW + timedelta(minutes=30), duration_s=99)
        got = capture_baseline_perf(store, "bp", before=_NOW.isoformat())
        assert got is not None
        assert got.run_id == "before_cut"

    def test_timestamps_are_normalized_to_utc_iso(self, store):
        _add_run(store, "r", start=_NOW - timedelta(hours=1), duration_s=5)
        got = capture_run_perf(store, "r")
        assert got is not None
        assert got.started_at.endswith("+00:00")
        assert datetime.fromisoformat(got.started_at) == _NOW - timedelta(hours=1)

    def test_volume_proxy_is_none_not_zero_when_unrecorded(self, store):
        _add_run(store, "r", start=_NOW, duration_s=5, records=None)
        got = capture_run_perf(store, "r")
        assert got is not None
        assert got.records_read is None
        assert got.bytes_read is None

    def test_volume_proxy_is_summed_when_recorded(self, store):
        _add_run(store, "r", start=_NOW, duration_s=5, records=1234)
        got = capture_run_perf(store, "r")
        assert got is not None
        assert got.records_read == 1234

    def test_absent_module_metrics_table_degrades_to_none(self, tmp_path):
        """An install where no engine ever wrote module_metrics.

        The table does not exist at all. The duration must still be
        reported and the volume proxy must be None, not a crash and not a
        zero that would read as "no rows were read".
        """
        bare = DuckDBObservabilityStore(tmp_path / "bare.db")
        with bare.connect() as cur:
            cur.execute(_DDL)
        _add_run(bare, "r", start=_NOW, duration_s=7)
        got = capture_run_perf(bare, "r")
        assert got is not None
        assert got.duration_ms == 7_000
        assert got.records_read is None

    def test_no_store_yields_none_rather_than_raising(self):
        assert capture_baseline_perf(None, "bp") is None
        assert capture_run_perf(None, "r") is None


# ── the comparison ─────────────────────────────────────────────────────────


def _perf(run_id="cur", duration_ms=100_000, engines=("spark",), records=None):
    return RunPerf(
        run_id=run_id, status="success", started_at="2026-05-01T12:00:00+00:00",
        finished_at="2026-05-01T12:01:40+00:00", duration_ms=duration_ms,
        engines=engines, records_read=records,
    )


class TestComparePerf:
    def test_slower_run_reports_the_ratio_and_delta(self):
        obs = compare_perf(
            baseline=_perf("base", 50_000).to_dict(),
            current=_perf("cur", 160_000),
            engine="spark", observed_at="t",
        )
        assert obs.status == "observed"
        assert obs.duration_ratio == 3.2
        assert obs.duration_delta_ms == 110_000

    def test_status_is_never_pass_or_fail(self):
        """No threshold exists, so no verdict may be reported.

        `pass` would claim a judgement nothing made; `fail` would claim a
        regression nothing measured a bound for.
        """
        for baseline, current in (
            (_perf("b", 50_000).to_dict(), _perf("c", 500_000)),   # 10x slower
            (_perf("b", 50_000).to_dict(), _perf("c", 5_000)),     # 10x faster
            (None, _perf("c", 5_000)),
        ):
            obs = compare_perf(baseline=baseline, current=current,
                               engine="spark", observed_at="t")
            assert obs.status in ("observed", "not_applicable")
            assert obs.status not in ("pass", "fail", "warn")

    def test_faster_run_is_reported_too(self):
        obs = compare_perf(
            baseline=_perf("b", 100_000).to_dict(),
            current=_perf("c", 25_000),
            engine="spark", observed_at="t",
        )
        assert obs.status == "observed"
        assert obs.duration_ratio == 0.25

    def test_no_baseline_is_not_applicable_and_says_why(self):
        obs = compare_perf(baseline=None, current=_perf(),
                           engine="spark", observed_at="t")
        assert obs.status == "not_applicable"
        assert "no green run" in obs.detail
        assert obs.duration_ratio is None

    def test_cross_engine_comparison_is_refused(self):
        obs = compare_perf(
            baseline=_perf("b", 50_000, engines=("spark",)).to_dict(),
            current=_perf("c", 160_000, engines=("duckdb",)),
            engine="duckdb", observed_at="t",
        )
        assert obs.status == "not_applicable"
        assert "duckdb" in obs.detail and "spark" in obs.detail
        assert obs.duration_ratio is None

    def test_unknown_engine_on_either_side_is_refused(self):
        obs = compare_perf(
            baseline=_perf("b", 50_000, engines=()).to_dict(),
            current=_perf("c", 160_000, engines=("spark",)),
            engine="spark", observed_at="t",
        )
        assert obs.status == "not_applicable"
        assert obs.duration_ratio is None

    def test_missing_volume_is_named_in_the_caveats(self):
        obs = compare_perf(
            baseline=_perf("b", 50_000, records=None).to_dict(),
            current=_perf("c", 60_000, records=None),
            engine="duckdb", observed_at="t",
        )
        assert obs.status == "observed"
        assert VOLUME_UNAVAILABLE in obs.caveats

    def test_changed_volume_is_named_in_the_caveats(self):
        obs = compare_perf(
            baseline=_perf("b", 50_000, records=1_000).to_dict(),
            current=_perf("c", 500_000, records=10_000),
            engine="spark", observed_at="t",
        )
        assert obs.status == "observed"
        assert any("input volume changed" in c for c in obs.caveats)
        assert any("1000 -> 10000" in c for c in obs.caveats)

    def test_unchanged_volume_adds_no_volume_caveat(self):
        obs = compare_perf(
            baseline=_perf("b", 50_000, records=1_000).to_dict(),
            current=_perf("c", 60_000, records=1_000),
            engine="spark", observed_at="t",
        )
        assert VOLUME_UNAVAILABLE not in obs.caveats
        assert not any("input volume changed" in c for c in obs.caveats)

    def test_co_applied_patches_are_disclosed(self):
        obs = compare_perf(
            baseline=_perf("b", 50_000, records=1).to_dict(),
            current=_perf("c", 60_000, records=1),
            engine="spark", observed_at="t", co_applied_patches=3,
        )
        assert any("3 patches were applied" in c for c in obs.caveats)

    def test_detail_disclaims_a_verdict(self):
        obs = compare_perf(
            baseline=_perf("b", 50_000).to_dict(), current=_perf("c", 150_000),
            engine="spark", observed_at="t",
        )
        assert "no regression threshold" in obs.detail


# ── the green-run stamp ────────────────────────────────────────────────────


_BP_WITH_PROVENANCE = """
aqueduct: "1.0"
id: bp
name: BP
modules:
  - id: src
    type: Ingress
    label: Source
    config:
      format: parquet
      path: in.parquet
edges: []
healed_by:
  - patch_id: p1
    engine: spark
    classification: dialect_neutral
    applied_at: "2026-05-01T10:00:00+00:00"
    validated_on: []
    perf_baseline:
      run_id: base
      status: success
      started_at: "2026-05-01T09:00:00+00:00"
      finished_at: "2026-05-01T09:00:50+00:00"
      duration_ms: 50000
      engines: [spark]
      records_read: 1000
      bytes_read: 10000
"""


class TestStampPerfObservation:
    def test_green_run_appends_one_observation(self, tmp_path, store):
        bp = tmp_path / "bp.yml"
        bp.write_text(_BP_WITH_PROVENANCE)
        _add_run(store, "green", start=_NOW, duration_s=160, records=1000)
        written = stamp_perf_observation(bp, "spark", obs_store=store, run_id="green")
        assert len(written) == 1
        assert written[0]["status"] == "observed"
        assert written[0]["duration_ratio"] == 3.2
        import yaml
        rec = yaml.safe_load(bp.read_text())["healed_by"][0]
        assert len(rec["perf_observations"]) == 1
        assert rec["perf_observations"][0]["engine"] == "spark"

    def test_second_green_run_on_the_same_engine_adds_nothing(self, tmp_path, store):
        bp = tmp_path / "bp.yml"
        bp.write_text(_BP_WITH_PROVENANCE)
        _add_run(store, "green", start=_NOW, duration_s=160, records=1000)
        stamp_perf_observation(bp, "spark", obs_store=store, run_id="green")
        first = bp.read_text()
        assert stamp_perf_observation(bp, "spark", obs_store=store, run_id="green") == []
        assert bp.read_text() == first

    def test_a_second_engine_gets_its_own_note(self, tmp_path, store):
        bp = tmp_path / "bp.yml"
        bp.write_text(_BP_WITH_PROVENANCE)
        _add_run(store, "green", start=_NOW, duration_s=160, records=1000)
        _add_run(store, "duck", start=_NOW, duration_s=90, engine="duckdb")
        stamp_perf_observation(bp, "spark", obs_store=store, run_id="green")
        written = stamp_perf_observation(bp, "duckdb", obs_store=store, run_id="duck")
        assert len(written) == 1
        # Baseline ran on spark, this run on duckdb — refused, not compared.
        assert written[0]["status"] == "not_applicable"
        import yaml
        rec = yaml.safe_load(bp.read_text())["healed_by"][0]
        assert [o["engine"] for o in rec["perf_observations"]] == ["spark", "duckdb"]

    def test_blueprint_without_provenance_is_left_untouched(self, tmp_path, store):
        bp = tmp_path / "bp.yml"
        bp.write_text(_BP_WITH_PROVENANCE.split("healed_by:")[0])
        before = bp.read_text()
        assert stamp_perf_observation(bp, "spark", obs_store=store, run_id="x") == []
        assert bp.read_text() == before

    def test_missing_blueprint_returns_empty_and_does_not_raise(self, tmp_path, store):
        assert stamp_perf_observation(
            tmp_path / "nope.yml", "spark", obs_store=store, run_id="x"
        ) == []

    def test_broken_store_never_fails_the_run(self, tmp_path):
        """The stamp is on the run-success path — it must never raise."""
        bp = tmp_path / "bp.yml"
        bp.write_text(_BP_WITH_PROVENANCE)

        class Exploding:
            def connect(self):
                raise RuntimeError("store is down")

        written = stamp_perf_observation(bp, "spark", obs_store=Exploding(), run_id="x")
        assert len(written) == 1
        assert written[0]["status"] == "not_applicable"

    def test_apply_snapshots_the_pre_patch_baseline(self, tmp_path, store):
        """`aqueduct patch apply` records the last green run BEFORE it."""
        import yaml

        from aqueduct.patch.apply import apply_patch_file

        bp = tmp_path / "bp.yml"
        bp.write_text(_BP_WITH_PROVENANCE.split("healed_by:")[0])
        _add_run(store, "pre", start=_NOW - timedelta(days=1),
                 duration_s=42, records=999)
        patch = tmp_path / "p.json"
        patch.write_text(json.dumps({
            "patch_id": "p9",
            "rationale": "repoint the source",
            "operations": [{"op": "set_module_config_key", "module_id": "src",
                            "key": "path", "value": "in2.parquet"}],
            "_aq_meta": {"engine": "spark", "run_id": "r"},
        }))
        apply_patch_file(bp, patch, patches_dir=tmp_path / "patches",
                         obs_store=store)
        rec = yaml.safe_load(bp.read_text())["healed_by"][0]
        assert rec["perf_baseline"]["run_id"] == "pre"
        assert rec["perf_baseline"]["duration_ms"] == 42_000
        assert rec["perf_baseline"]["records_read"] == 999

    def test_apply_omits_the_baseline_when_no_green_run_preceded_it(
        self, tmp_path, store
    ):
        import yaml

        from aqueduct.patch.apply import apply_patch_file

        bp = tmp_path / "bp.yml"
        bp.write_text(_BP_WITH_PROVENANCE.split("healed_by:")[0])
        patch = tmp_path / "p.json"
        patch.write_text(json.dumps({
            "patch_id": "p9",
            "rationale": "repoint the source",
            "operations": [{"op": "set_module_config_key", "module_id": "src",
                            "key": "path", "value": "in2.parquet"}],
            "_aq_meta": {"engine": "spark", "run_id": "r"},
        }))
        apply_patch_file(bp, patch, patches_dir=tmp_path / "patches",
                         obs_store=store)
        rec = yaml.safe_load(bp.read_text())["healed_by"][0]
        assert "perf_baseline" not in rec

    def test_stamped_blueprint_still_parses(self, tmp_path, store):
        """The note lands in a schema-valid Blueprint, not a freeform blob."""
        from aqueduct.parser.parser import parse

        bp = tmp_path / "bp.yml"
        bp.write_text(_BP_WITH_PROVENANCE)
        _add_run(store, "green", start=_NOW, duration_s=160, records=1000)
        stamp_perf_observation(bp, "spark", obs_store=store, run_id="green")
        parsed = parse(str(bp))
        rec = parsed.healed_by[0]
        assert rec.perf_baseline["duration_ms"] == 50000
        assert rec.perf_observations[0]["duration_ratio"] == 3.2
