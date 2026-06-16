"""Tests for Phase 52 — BenchmarkStore backend abstraction + compute_stats/format_stats.

Covers:
- BenchmarkStore dataclass construction (duckdb / postgres)
- BenchmarkStore.from_config: duckdb + no path → default_store_path(scenarios_dir)
- BenchmarkStore.from_config: postgres + no path → ValueError
- BenchmarkStore.from_config: postgres + DSN → postgres store with correct label
- BenchmarkStore.label: "postgres:benchmark" for postgres, file path for duckdb
- persist_results / diff_latest accept BenchmarkStore OR legacy Path/str
- compute_stats: models leaderboard ordered pass_rate DESC (best model first)
- compute_stats: scenarios ordered pass_rate ASC (hardest first)
- compute_stats: uses LATEST row per (scenario, model) only
- compute_stats: empty / missing store → all-empty lists, never raises
- format_stats: renders leaderboard + "→ best model" line + hardest scenarios + trend
- format_stats: empty stats → "(benchmark store empty …)"
- Postgres backend round-trip (integration, skipped when AQ_PG_DSN unset)
"""

from __future__ import annotations

import os
import types
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

from aqueduct.surveyor.benchmark_store import (
    BenchmarkStore,
    _PG_BENCHMARK_SCHEMA,
    compute_stats,
    default_store_path,
    diff_latest,
    format_stats,
    persist_results,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _fake_result(
    *,
    passed: bool = True,
    patch_valid: bool = True,
    patch_applies: bool = True,
    confidence: float | None = 0.9,
    duration_seconds: float = 1.0,
    attempts_to_parse: int = 1,
    diag_score: float | None = None,
    root_cause_match: bool | None = None,
    category_match: bool | None = None,
    prompt_version: str | None = "v1",
    provider: str | None = "anthropic",
    base_url: str | None = None,
    failures: list | None = None,
    soft_failures: list | None = None,
    violated_guardrails: list | None = None,
    stop_reason: str | None = None,
    escalated: bool = False,
    tokens_in_total: int = 0,
    tokens_out_total: int = 0,
) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        passed=passed,
        patch_valid=patch_valid,
        patch_applies=patch_applies,
        confidence=confidence,
        duration_seconds=duration_seconds,
        attempts_to_parse=attempts_to_parse,
        diag_score=diag_score,
        root_cause_match=root_cause_match,
        category_match=category_match,
        prompt_version=prompt_version,
        provider=provider,
        base_url=base_url,
        failures=failures or [],
        soft_failures=soft_failures or [],
        violated_guardrails=violated_guardrails,
        stop_reason=stop_reason,
        escalated=escalated,
        tokens_in_total=tokens_in_total,
        tokens_out_total=tokens_out_total,
    )


def _cfg(backend: str = "duckdb", path: str | None = None) -> types.SimpleNamespace:
    """Minimal fake config block matching what from_config reads via getattr."""
    return types.SimpleNamespace(backend=backend, path=path)


# ── BenchmarkStore dataclass ───────────────────────────────────────────────────

class TestBenchmarkStoreDataclass:
    def test_default_backend_is_duckdb(self):
        bs = BenchmarkStore()
        assert bs.backend == "duckdb"

    def test_explicit_duckdb(self, tmp_path):
        bs = BenchmarkStore(backend="duckdb", location=str(tmp_path / "bench.duckdb"))
        assert bs.backend == "duckdb"
        assert "bench.duckdb" in bs.location

    def test_label_duckdb_returns_location(self, tmp_path):
        loc = str(tmp_path / "bench.duckdb")
        bs = BenchmarkStore(backend="duckdb", location=loc)
        assert bs.label == loc

    def test_label_postgres_returns_schema_label(self):
        bs = BenchmarkStore(backend="postgres", location="postgresql://user:pass@host/db")
        assert bs.label == f"postgres:{_PG_BENCHMARK_SCHEMA}"

    def test_frozen_mutation_raises(self, tmp_path):
        bs = BenchmarkStore(backend="duckdb", location=str(tmp_path / "b.duckdb"))
        with pytest.raises((AttributeError, TypeError)):
            bs.backend = "postgres"  # type: ignore[misc]


# ── BenchmarkStore.from_config ─────────────────────────────────────────────────

class TestFromConfig:
    def test_duckdb_no_path_uses_default_store_path(self, tmp_path):
        scenarios_dir = tmp_path / "scenarios"
        scenarios_dir.mkdir()
        cfg = _cfg(backend="duckdb", path=None)
        bs = BenchmarkStore.from_config(cfg, scenarios_dir)
        expected = str(default_store_path(scenarios_dir))
        assert bs.backend == "duckdb"
        assert bs.location == expected

    def test_duckdb_explicit_path_uses_provided_path(self, tmp_path):
        path = str(tmp_path / "custom.duckdb")
        cfg = _cfg(backend="duckdb", path=path)
        bs = BenchmarkStore.from_config(cfg, tmp_path)
        assert bs.location == path

    def test_postgres_no_path_raises_value_error(self, tmp_path):
        cfg = _cfg(backend="postgres", path=None)
        with pytest.raises(ValueError, match="stores.benchmark.backend=postgres requires"):
            BenchmarkStore.from_config(cfg, tmp_path)

    def test_postgres_with_dsn_returns_postgres_store(self, tmp_path):
        dsn = "postgresql://user:pass@host/db"
        cfg = _cfg(backend="postgres", path=dsn)
        bs = BenchmarkStore.from_config(cfg, tmp_path)
        assert bs.backend == "postgres"
        assert bs.location == dsn
        assert bs.label == f"postgres:{_PG_BENCHMARK_SCHEMA}"


# ── persist_results back-compat (Path / str / BenchmarkStore) ─────────────────

class TestPersistResultsBackcompat:
    def _results_one_pair(self) -> dict:
        return {"sc1": {"model-a": _fake_result(passed=True)}}

    def test_persist_with_path(self, tmp_path):
        p = tmp_path / "bench.duckdb"
        n = persist_results(self._results_one_pair(), p)
        assert n == 1

    def test_persist_with_str(self, tmp_path):
        p = str(tmp_path / "bench.duckdb")
        n = persist_results(self._results_one_pair(), p)
        assert n == 1

    def test_persist_with_benchmark_store(self, tmp_path):
        bs = BenchmarkStore(backend="duckdb", location=str(tmp_path / "bench.duckdb"))
        n = persist_results(self._results_one_pair(), bs)
        assert n == 1

    def test_persist_empty_results_writes_zero_rows(self, tmp_path):
        n = persist_results({}, tmp_path / "bench.duckdb")
        assert n == 0


# ── diff_latest back-compat ────────────────────────────────────────────────────

class TestDiffLatestBackcompat:
    def test_diff_latest_with_path(self, tmp_path):
        p = tmp_path / "bench.duckdb"
        results = {"sc1": {"model-a": _fake_result(passed=True)}}
        persist_results(results, p)
        entries = diff_latest(results, p)
        assert len(entries) == 1
        assert entries[0].baseline is None  # first run → no prior

    def test_diff_latest_with_benchmark_store(self, tmp_path):
        bs = BenchmarkStore(backend="duckdb", location=str(tmp_path / "bench.duckdb"))
        results = {"sc1": {"model-a": _fake_result(passed=False)}}
        persist_results(results, bs)
        entries = diff_latest(results, bs)
        assert len(entries) == 1


# ── compute_stats ──────────────────────────────────────────────────────────────

class TestComputeStats:
    def _seed(self, tmp_path, rows: list[tuple]) -> Path:
        """Insert rows={(scenario_id, model, passed)} into a fresh duckdb."""
        p = tmp_path / "bench.duckdb"
        for scenario_id, model, passed in rows:
            persist_results(
                {scenario_id: {model: _fake_result(passed=passed)}},
                p,
            )
        return p

    def test_empty_store_returns_empty_lists(self, tmp_path):
        p = tmp_path / "empty.duckdb"
        stats = compute_stats(p)
        assert stats == {"models": [], "scenarios": [], "trend": []}

    def test_nonexistent_store_returns_empty_lists_no_raise(self, tmp_path):
        p = tmp_path / "nonexistent.duckdb"
        # Should not raise even if the file doesn't exist yet
        stats = compute_stats(p)
        assert stats["models"] == []
        assert stats["scenarios"] == []
        assert stats["trend"] == []

    def test_models_leaderboard_ordered_pass_rate_desc(self, tmp_path):
        """Best model (highest pass_rate) first."""
        p = tmp_path / "bench.duckdb"
        # model-A: 2/2 passes → 100%
        # model-B: 0/1 passes → 0%
        persist_results({"sc1": {"model-A": _fake_result(passed=True)}}, p)
        persist_results({"sc2": {"model-A": _fake_result(passed=True)}}, p)
        persist_results({"sc1": {"model-B": _fake_result(passed=False)}}, p)

        stats = compute_stats(p)
        models = stats["models"]
        assert len(models) == 2
        # Best model must come first
        assert models[0]["model"] == "model-A"
        assert models[0]["pass_rate"] == pytest.approx(1.0)
        assert models[1]["model"] == "model-B"
        assert models[1]["pass_rate"] == pytest.approx(0.0)

    def test_scenarios_ordered_pass_rate_asc_hardest_first(self, tmp_path):
        """Hardest scenario (lowest pass_rate across models) must come first."""
        p = tmp_path / "bench.duckdb"
        # sc-easy: 1/1 → 100%  sc-hard: 0/1 → 0%
        persist_results({"sc-easy": {"m": _fake_result(passed=True)}}, p)
        persist_results({"sc-hard": {"m": _fake_result(passed=False)}}, p)

        stats = compute_stats(p)
        scenarios = stats["scenarios"]
        assert len(scenarios) == 2
        assert scenarios[0]["scenario_id"] == "sc-hard"
        assert scenarios[0]["pass_rate"] == pytest.approx(0.0)
        assert scenarios[1]["scenario_id"] == "sc-easy"
        assert scenarios[1]["pass_rate"] == pytest.approx(1.0)

    def test_latest_row_per_scenario_model_used_for_leaderboard(self, tmp_path):
        """Even if a (scenario, model) pair has two rows, only the latest counts."""
        p = tmp_path / "bench.duckdb"
        # First row: failed
        persist_results({"sc1": {"model-A": _fake_result(passed=False)}}, p)
        # Second (later) row: passed — should win
        persist_results({"sc1": {"model-A": _fake_result(passed=True)}}, p)

        stats = compute_stats(p)
        assert len(stats["models"]) == 1
        # Only 1 distinct (scenario, model) pair after dedup → n=1
        assert stats["models"][0]["n"] == 1
        # Latest row was passed=True → pass_rate=1.0
        assert stats["models"][0]["pass_rate"] == pytest.approx(1.0)

    def test_models_stat_keys_present(self, tmp_path):
        p = tmp_path / "bench.duckdb"
        persist_results({"sc1": {"m": _fake_result()}}, p)
        stats = compute_stats(p)
        m = stats["models"][0]
        for key in ("model", "n", "pass_rate", "parse_rate", "apply_rate", "avg_diag", "avg_duration"):
            assert key in m

    def test_scenarios_stat_keys_present(self, tmp_path):
        p = tmp_path / "bench.duckdb"
        persist_results({"sc1": {"m": _fake_result()}}, p)
        stats = compute_stats(p)
        s = stats["scenarios"][0]
        for key in ("scenario_id", "n", "pass_rate"):
            assert key in s

    def test_trend_entries_have_date_n_pass_rate(self, tmp_path):
        p = tmp_path / "bench.duckdb"
        persist_results({"sc1": {"m": _fake_result()}}, p)
        stats = compute_stats(p)
        if stats["trend"]:
            t = stats["trend"][0]
            assert "date" in t and "n" in t and "pass_rate" in t

    def test_compute_stats_accepts_benchmark_store(self, tmp_path):
        bs = BenchmarkStore(backend="duckdb", location=str(tmp_path / "bench.duckdb"))
        persist_results({"sc1": {"m": _fake_result(passed=True)}}, bs)
        stats = compute_stats(bs)
        assert len(stats["models"]) == 1


# ── format_stats ───────────────────────────────────────────────────────────────

class TestFormatStats:
    def test_empty_stats_returns_benchmark_store_empty_message(self):
        empty = {"models": [], "scenarios": [], "trend": []}
        out = format_stats(empty)
        assert "benchmark store empty" in out

    def test_renders_best_model_line(self, tmp_path):
        p = tmp_path / "bench.duckdb"
        persist_results({"sc1": {"my-model": _fake_result(passed=True)}}, p)
        stats = compute_stats(p)
        out = format_stats(stats)
        assert "→ best model:" in out
        assert "my-model" in out

    def test_renders_leaderboard_header(self, tmp_path):
        p = tmp_path / "bench.duckdb"
        persist_results({"sc1": {"m": _fake_result()}}, p)
        out = format_stats(compute_stats(p))
        assert "Model leaderboard" in out

    def test_renders_hardest_scenarios_section(self, tmp_path):
        p = tmp_path / "bench.duckdb"
        persist_results({"hard-sc": {"m": _fake_result(passed=False)}}, p)
        out = format_stats(compute_stats(p))
        assert "Hardest scenarios" in out
        assert "hard-sc" in out

    def test_renders_trend_section_when_rows_exist(self, tmp_path):
        p = tmp_path / "bench.duckdb"
        persist_results({"sc1": {"m": _fake_result()}}, p)
        out = format_stats(compute_stats(p))
        assert "trend" in out.lower() or "Pass-rate trend" in out

    def test_multiple_models_both_appear_in_output(self, tmp_path):
        p = tmp_path / "bench.duckdb"
        persist_results({"sc1": {"alpha": _fake_result(passed=True)}}, p)
        persist_results({"sc1": {"beta": _fake_result(passed=False)}}, p)
        out = format_stats(compute_stats(p))
        assert "alpha" in out
        assert "beta" in out


# ── Postgres integration (skipped when AQ_PG_DSN unset) ───────────────────────

@pytest.mark.integration
class TestPostgresBackendRoundTrip:
    def test_persist_and_stats_via_postgres(self, tmp_path):
        dsn = os.environ.get("AQ_PG_DSN")
        if not dsn:
            pytest.skip("AQ_PG_DSN not set — skipping postgres integration test")

        bs = BenchmarkStore(backend="postgres", location=dsn)
        results = {f"pg-sc-{uuid.uuid4().hex[:6]}": {"pg-model": _fake_result(passed=True)}}
        n = persist_results(results, bs)
        assert n == 1

        stats = compute_stats(bs)
        assert isinstance(stats["models"], list)
        assert isinstance(stats["scenarios"], list)
