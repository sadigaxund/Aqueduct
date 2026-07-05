"""Unit tests for Phase 33 Part A:
- surveyor/benchmark_store.py  (persist, diff, compare, has_regressions, default_store_path)
- surveyor/surveyor.py         (healing_outcomes.prompt_version migration)
- surveyor/scenario.py         (ScenarioResult Phase 33 defaults + run_scenario fields)
"""

from __future__ import annotations

import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from aqueduct.agent.budget import StopReason

pytestmark = pytest.mark.unit

from aqueduct.surveyor.benchmark_store import (
    BenchmarkRow,
    DiffEntry,
    _compare,
    _connect,
    default_store_path,
    diff_latest,
    has_regressions,
    persist_results,
)
from aqueduct.surveyor.scenario import ScenarioResult

# ── helpers ───────────────────────────────────────────────────────────────────

def _make_result(
    scenario_id: str = "s1",
    model: str = "m1",
    passed: bool = True,
    patch_valid: bool = True,
    patch_applies: bool = True,
    confidence: float | None = 0.9,
    diag_score: float | None = 0.8,
    prompt_version: str | None = "1.0",
    provider: str | None = "anthropic",
    base_url: str | None = None,
) -> ScenarioResult:
    return ScenarioResult(
        scenario_id=scenario_id,
        model=model,
        passed=passed,
        patch_valid=patch_valid,
        patch_applies=patch_applies,
        patch=None,
        duration_seconds=1.0,
        confidence=confidence,
        diag_score=diag_score,
        failures=[],
        soft_failures=[],
        prompt_version=prompt_version,
        provider=provider,
        base_url=base_url,
    )


def _make_row(
    passed: bool = True,
    patch_valid: bool = True,
    patch_applies: bool = True,
    confidence: float | None = 0.9,
    diag_score: float | None = 0.8,
    prompt_version: str | None = "1.0",
    recorded_at: str = "2026-01-01T00:00:00+00:00",
) -> BenchmarkRow:
    return BenchmarkRow(
        recorded_at=recorded_at,
        scenario_id="s1",
        model="m1",
        prompt_version=prompt_version,
        passed=passed,
        patch_valid=patch_valid,
        patch_applies=patch_applies,
        confidence=confidence,
        duration_seconds=1.0,
        attempts_to_parse=1,
        diag_score=diag_score,
    )


def _insert_row(store_path: Path, scenario_id: str, model: str,
                prompt_version: str | None, passed: bool, recorded_at: str) -> None:
    """Directly insert a row for baseline tests."""
    con = _connect(store_path)
    import json
    con.execute(
        """INSERT INTO benchmark_results
           (id, recorded_at, scenario_id, model, prompt_version, provider, base_url,
            passed, patch_valid, patch_applies, confidence, duration_seconds,
            attempts_to_parse, diag_score, root_cause_match, category_match,
            failures, soft_failures)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [str(uuid.uuid4()), recorded_at, scenario_id, model, prompt_version,
         None, None, passed, True, True, 0.9, 1.0, 1, 0.8, None, None,
         json.dumps([]), json.dumps([])],
    )
    con.close()


# ── default_store_path ────────────────────────────────────────────────────────

def test_default_store_path_dir(tmp_path):
    """default_store_path(<dir>) → <dir>/.aqueduct/benchmark.duckdb"""
    result = default_store_path(tmp_path)
    assert result == (tmp_path / ".aqueduct" / "benchmark.duckdb").resolve()


def test_default_store_path_file(tmp_path):
    """default_store_path(<file.aqscenario.yml>) → <file_dir>/.aqueduct/benchmark.duckdb"""
    f = tmp_path / "my.aqscenario.yml"
    f.touch()
    result = default_store_path(f)
    assert result == (tmp_path / ".aqueduct" / "benchmark.duckdb").resolve()


# ── persist_results ───────────────────────────────────────────────────────────

def test_persist_results_empty(tmp_path):
    """persist_results({}) → 0 rows written, no error."""
    store_path = tmp_path / "bench.duckdb"
    written = persist_results({}, store_path)
    assert written == 0


def test_persist_results_writes_one_row_per_pair(tmp_path):
    """persist_results(results) writes one row per (scenario, model) pair."""
    store_path = tmp_path / "bench.duckdb"
    results = {
        "s1": {
            "m1": _make_result("s1", "m1", prompt_version="1.0", provider="anthropic", base_url=None),
            "m2": _make_result("s1", "m2", prompt_version="1.0", provider="openai_compat", base_url="http://x"),
        },
        "s2": {
            "m1": _make_result("s2", "m1", passed=False, confidence=None),
        },
    }
    written = persist_results(results, store_path)
    assert written == 3

    con = duckdb.connect(str(store_path))
    rows = con.execute("SELECT scenario_id, model, prompt_version, provider, base_url FROM benchmark_results ORDER BY scenario_id, model").fetchall()
    con.close()

    assert len(rows) == 3
    assert ("s1", "m1", "1.0", "anthropic", None) in rows
    assert ("s1", "m2", "1.0", "openai_compat", "http://x") in rows
    assert ("s2", "m1", "1.0", "anthropic", None) in rows


def test_persist_results_best_effort_on_duckdb_missing(tmp_path):
    """persist_results is best-effort — duckdb import fails → returns 0, logs warning."""
    store_path = tmp_path / "bench.duckdb"
    results = {"s1": {"m1": _make_result()}}

    # Make _connect fail by patching duckdb.connect at the import level
    with patch("aqueduct.surveyor.benchmark_store._connect", side_effect=RuntimeError("no duckdb")):
        written = persist_results(results, store_path)

    assert written == 0
    assert not store_path.exists()


# ── has_regressions ───────────────────────────────────────────────────────────

def _make_entry(baseline=None, regressions=(), improvements=()):
    current = _make_row()
    return DiffEntry(
        scenario_id="s1",
        model="m1",
        baseline=baseline,
        current=current,
        baseline_prompt_mismatch=False,
        regressions=tuple(regressions),
        improvements=tuple(improvements),
    )


def test_has_regressions_empty():
    assert has_regressions([]) is False


def test_has_regressions_new_pair():
    """Entry with baseline=None (NEW pair) is NOT a regression."""
    entry = _make_entry(baseline=None, regressions=())
    assert has_regressions([entry]) is False


def test_has_regressions_with_regression():
    entry = _make_entry(baseline=_make_row(), regressions=("passed: True → False",))
    assert has_regressions([entry]) is True


# ── _compare ──────────────────────────────────────────────────────────────────

def test_compare_passed_true_to_false():
    baseline = _make_row(passed=True)
    current = _make_row(passed=False)
    regs, imps = _compare(baseline, current)
    assert any("passed" in r for r in regs)
    assert not any("passed" in i for i in imps)


def test_compare_passed_false_to_true():
    baseline = _make_row(passed=False)
    current = _make_row(passed=True)
    regs, imps = _compare(baseline, current)
    assert not any("passed" in r for r in regs)
    assert any("passed" in i for i in imps)


def test_compare_patch_applies_true_to_false():
    baseline = _make_row(patch_applies=True)
    current = _make_row(patch_applies=False)
    regs, imps = _compare(baseline, current)
    assert any("patch_applies" in r for r in regs)


def test_compare_diag_score_drop_over_threshold():
    """diag_score drop > 0.05 → regression; drop ≤ 0.05 → ignored."""
    # Drop of 0.10 (>0.05) → regression
    baseline = _make_row(diag_score=0.90)
    current = _make_row(diag_score=0.80)
    regs, _ = _compare(baseline, current)
    assert any("diag_score" in r for r in regs)

    # Drop of 0.03 (≤0.05) → no regression
    baseline2 = _make_row(diag_score=0.90)
    current2 = _make_row(diag_score=0.87)
    regs2, _ = _compare(baseline2, current2)
    assert not any("diag_score" in r for r in regs2)


def test_compare_confidence_drop_over_threshold():
    """confidence is EXCLUDED from regression detection. Drop > 0.05 must NOT cause regression."""
    baseline = _make_row(confidence=0.90)
    current = _make_row(confidence=0.80)
    regs, imps = _compare(baseline, current)
    assert not any("confidence" in r for r in regs)
    assert not any("confidence" in i for i in imps)
    
    # Increase in confidence also does nothing
    baseline2 = _make_row(confidence=0.80)
    current2 = _make_row(confidence=0.95)
    regs2, imps2 = _compare(baseline2, current2)
    assert not any("confidence" in r for r in regs2)
    assert not any("confidence" in i for i in imps2)


# ── diff_latest ───────────────────────────────────────────────────────────────

def test_diff_latest_no_prior_row(tmp_path):
    """No prior row → DiffEntry.baseline is None, status NEW, no regression."""
    store_path = tmp_path / "bench.duckdb"
    r = _make_result()
    results = {"s1": {"m1": r}}

    persist_results(results, store_path)
    entries = diff_latest(results, store_path)

    assert len(entries) == 1
    assert entries[0].baseline is None
    assert not entries[0].regressions


def test_diff_latest_baseline_exact_triple(tmp_path):
    """Baseline lookup prefers exact (scenario, model, prompt_version) triple → mismatch=False."""
    store_path = tmp_path / "bench.duckdb"
    # Insert old row with same prompt_version at an earlier time
    _insert_row(store_path, "s1", "m1", prompt_version="1.0", passed=True,
                recorded_at="2026-01-01T00:00:00+00:00")
    # Now persist a new row (prompt_version="1.0") at a later time
    r = _make_result("s1", "m1", passed=True, prompt_version="1.0")
    results = {"s1": {"m1": r}}
    persist_results(results, store_path)

    entries = diff_latest(results, store_path)

    assert len(entries) == 1
    e = entries[0]
    assert e.baseline is not None
    assert e.baseline_prompt_mismatch is False


def test_diff_latest_baseline_fallback_prompt_mismatch(tmp_path):
    """Fallback to (scenario, model) when no exact prompt_version triple → mismatch=True."""
    store_path = tmp_path / "bench.duckdb"
    # Insert old row with different prompt_version
    _insert_row(store_path, "s1", "m1", prompt_version="0.9", passed=True,
                recorded_at="2026-01-01T00:00:00+00:00")
    # Persist new row with prompt_version="1.0"
    r = _make_result("s1", "m1", passed=True, prompt_version="1.0")
    results = {"s1": {"m1": r}}
    persist_results(results, store_path)

    entries = diff_latest(results, store_path)

    assert len(entries) == 1
    e = entries[0]
    assert e.baseline is not None
    assert e.baseline_prompt_mismatch is True


# ── Surveyor healing_outcomes migration ───────────────────────────────────────

def _create_legacy_obs_db(path: Path) -> None:
    """Create a pre-1.0.3 observability.db without prompt_version in healing_outcomes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    # Minimal legacy DDL (no prompt_version column)
    con.execute("""
    CREATE TABLE IF NOT EXISTS healing_outcomes (
        id                      VARCHAR PRIMARY KEY,
        run_id                  VARCHAR NOT NULL,
        failed_module           VARCHAR,
        failure_category        VARCHAR,
        model                   VARCHAR,
        patch_id                VARCHAR,
        confidence              DOUBLE PRECISION,
        patch_applied           BOOLEAN,
        run_success_after_patch BOOLEAN,
        applied_at              VARCHAR
    )""")
    # Insert one legacy row
    con.execute(
        "INSERT INTO healing_outcomes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [str(uuid.uuid4()), "run-legacy", "mod1", "bad_path",
         "model-x", "patch-y", 0.8, True, True, "2026-01-01T00:00:00+00:00"],
    )
    con.close()


def _make_manifest():
    from aqueduct.compiler.models import Manifest
    return Manifest(
        blueprint_id="test-bp",
        modules=(),
        edges=(),
        context={},
        spark_config={},
    )


def test_surveyor_fresh_db_has_prompt_version(tmp_path):
    """Fresh DB → healing_outcomes includes prompt_version VARCHAR column from initial DDL."""
    from aqueduct.surveyor.surveyor import Surveyor

    manifest = _make_manifest()
    surveyor = Surveyor(manifest=manifest, store_dir=tmp_path)
    surveyor.start("run-fresh")
    surveyor.stop()

    obs_path = tmp_path / "observability.db"
    con = duckdb.connect(str(obs_path))
    has_col = con.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name='healing_outcomes' AND column_name='prompt_version'"
    ).fetchone()
    con.close()
    assert has_col is not None


# test_surveyor_migration_adds_prompt_version_to_legacy_db removed —
# surveyor.py no longer carries pre-1.0 ALTER TABLE migration paths
# (commit ec173e7). Fresh DBs include prompt_version from the base
# CREATE TABLE; legacy DBs are unsupported by design.


def test_surveyor_migration_idempotent(tmp_path):
    """Second Surveyor.start() on same DB does not re-issue the ALTER (idempotent)."""
    from aqueduct.surveyor.surveyor import Surveyor

    manifest = _make_manifest()
    # First start
    s1 = Surveyor(manifest=manifest, store_dir=tmp_path)
    s1.start("run-first")
    s1.stop()
    # Second start — must not raise
    s2 = Surveyor(manifest=manifest, store_dir=tmp_path)
    s2.start("run-second")
    s2.stop()

    obs_path = tmp_path / "observability.db"
    con = duckdb.connect(str(obs_path))
    cols = con.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_name='healing_outcomes' AND column_name='prompt_version'"
    ).fetchone()[0]
    con.close()
    assert cols == 1  # exactly one, not duplicated


def test_record_healing_outcome_default_prompt_version(tmp_path):
    """record_healing_outcome(prompt_version=None) → column populated from agent.PROMPT_VERSION."""
    from aqueduct.agent import PROMPT_VERSION
    from aqueduct.surveyor.surveyor import Surveyor

    manifest = _make_manifest()
    surveyor = Surveyor(manifest=manifest, store_dir=tmp_path)
    surveyor.start("run-abc")

    surveyor.record_healing_outcome(
        run_id="run-abc",
        failed_module="m1",
        failure_category="bad_path",
        model="test-model",
        patch_id="patch-123",
        confidence=0.85,
        patch_applied=True,
        run_success_after_patch=True,
        prompt_version=None,  # should fall back to agent.PROMPT_VERSION
    )
    surveyor.stop()

    obs_path = tmp_path / "observability.db"
    con = duckdb.connect(str(obs_path))
    row = con.execute(
        "SELECT prompt_version FROM healing_outcomes WHERE run_id='run-abc'"
    ).fetchone()
    con.close()

    assert row is not None
    assert row[0] == PROMPT_VERSION


def test_record_healing_outcome_explicit_prompt_version(tmp_path):
    """record_healing_outcome(prompt_version='2.0') → honors override, does NOT use constant."""
    from aqueduct.surveyor.surveyor import Surveyor

    manifest = _make_manifest()
    surveyor = Surveyor(manifest=manifest, store_dir=tmp_path)
    surveyor.start("run-xyz")

    surveyor.record_healing_outcome(
        run_id="run-xyz",
        failed_module="m1",
        failure_category=None,
        model="test-model",
        patch_id=None,
        confidence=None,
        patch_applied=False,
        run_success_after_patch=False,
        prompt_version="2.0",
    )
    surveyor.stop()

    obs_path = tmp_path / "observability.db"
    con = duckdb.connect(str(obs_path))
    row = con.execute(
        "SELECT prompt_version FROM healing_outcomes WHERE run_id='run-xyz'"
    ).fetchone()
    con.close()

    assert row is not None
    assert row[0] == "2.0"


# ── ScenarioResult Phase 33 defaults ─────────────────────────────────────────

def test_scenario_result_phase33_defaults():
    """ScenarioResult defaults: prompt_version=None, provider=None, base_url=None."""
    r = ScenarioResult(
        scenario_id="s1",
        model="m1",
        passed=True,
        patch_valid=True,
        patch_applies=True,
        patch=None,
        duration_seconds=1.0,
        failures=[],
    )
    assert r.prompt_version is None
    assert r.provider is None
    assert r.base_url is None


def test_run_scenario_populates_phase33_fields_normal_path(tmp_path):
    """run_scenario normal path → prompt_version, provider, base_url set on result."""
    from aqueduct.agent import PROMPT_VERSION
    from aqueduct.surveyor.scenario import AqScenario, run_scenario

    scenario = AqScenario(
        id="test-sc",
        description="test",
        blueprint="",
        inject_failure={"module": "m1", "error_message": "err"},
        expected_patch={},
        assertions=[],
        source_path=Path("/fake/path.aqscenario.yml"),
    )

    mock_agent_result = MagicMock()
    mock_agent_result.patch = None
    mock_agent_result.attempts = 1
    mock_agent_result.reprompt_errors = []

    with patch("aqueduct.surveyor.scenario._build_failure_ctx") as mock_ctx, \
         patch("aqueduct.agent.generate_agent_patch", return_value=mock_agent_result):
        mock_ctx.return_value = MagicMock()
        result = run_scenario(
            scenario,
            model="test-model",
            patches_dir=tmp_path,
            provider="openai_compat",
            base_url="http://localhost:11434",
        )

    assert result.prompt_version == PROMPT_VERSION
    assert result.provider == "openai_compat"
    assert result.base_url == "http://localhost:11434"


def test_run_scenario_populates_phase33_fields_early_exit(tmp_path):
    """run_scenario early-exit (FailureContext build failure) → Phase 33 fields still set."""
    from aqueduct.agent import PROMPT_VERSION
    from aqueduct.surveyor.scenario import AqScenario, run_scenario

    scenario = AqScenario(
        id="test-sc-fail",
        description="test",
        blueprint="",
        inject_failure={"module": "m1", "error_message": "err"},
        expected_patch={},
        assertions=[],
        source_path=Path("/fake/path.aqscenario.yml"),
    )

    with patch("aqueduct.surveyor.scenario._build_failure_ctx",
               side_effect=RuntimeError("blueprint not found")):
        result = run_scenario(
            scenario,
            model="test-model",
            patches_dir=tmp_path,
            provider="openai_compat",
            base_url="http://my-url",
        )

    assert result.passed is False
    assert result.prompt_version == PROMPT_VERSION
    assert result.provider == "openai_compat"
    assert result.base_url == "http://my-url"


# ── BenchmarkStore violated_guardrails migration ─────────────────────────────

def test_benchmark_fresh_db_has_violated_guardrails(tmp_path):
    """Fresh store has violated_guardrails JSON column in benchmark_results DDL."""
    store_path = tmp_path / "bench.duckdb"
    # trigger DDL
    con = _connect(store_path)
    has_col = con.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name='benchmark_results' AND column_name='violated_guardrails'"
    ).fetchone()
    con.close()
    assert has_col is not None


def test_benchmark_migration_violated_guardrails_idempotent(tmp_path):
    """Migration is idempotent — second _connect does not re-issue the ALTER."""
    store_path = tmp_path / "bench.duckdb"
    # First connect
    con1 = _connect(store_path)
    con1.close()
    # Second connect
    con2 = _connect(store_path)
    cols = con2.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_name='benchmark_results' AND column_name='violated_guardrails'"
    ).fetchone()[0]
    con2.close()
    assert cols == 1  # exactly one, not duplicated


def test_persist_results_writes_violated_guardrails(tmp_path):
    """persist_results writes violated_guardrails as JSON-serialized list when non-None; NULL when None."""
    store_path = tmp_path / "bench.duckdb"
    r_none = _make_result("s1", "m1")
    r_none.violated_guardrails = None

    r_empty = _make_result("s2", "m1")
    r_empty.violated_guardrails = []

    r_violated = _make_result("s3", "m1")
    r_violated.violated_guardrails = ["replace_module_config"]

    results = {
        "s1": {"m1": r_none},
        "s2": {"m1": r_empty},
        "s3": {"m1": r_violated},
    }
    persist_results(results, store_path)

    con = duckdb.connect(str(store_path))
    rows = con.execute("SELECT scenario_id, violated_guardrails FROM benchmark_results ORDER BY scenario_id").fetchall()
    con.close()

    assert rows[0] == ("s1", None)
    assert rows[1] == ("s2", "[]")
    assert rows[2] == ("s3", '["replace_module_config"]')


# ── Phase 34 migrations ───────────────────────────────────────────────────────

def test_benchmark_migration_phase34_new_store(tmp_path):
    """Fresh store DDL includes stop_reason, escalated, tokens_in_total, tokens_out_total."""
    store_path = tmp_path / "bench.duckdb"
    con = _connect(store_path)
    cols = con.execute("SELECT column_name FROM information_schema.columns WHERE table_name='benchmark_results'").fetchall()
    con.close()
    
    cnames = [c[0] for c in cols]
    assert "stop_reason" in cnames
    assert "escalated" in cnames
    assert "tokens_in_total" in cnames
    assert "tokens_out_total" in cnames

def test_benchmark_migration_phase34_idempotent(tmp_path):
    """Migration is idempotent — second _connect does not re-issue the ALTERs."""
    store_path = tmp_path / "bench.duckdb"
    con1 = _connect(store_path)
    con1.close()
    
    con2 = _connect(store_path)
    cols = con2.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_name='benchmark_results' AND column_name='stop_reason'"
    ).fetchone()[0]
    con2.close()
    
    assert cols == 1

def test_persist_results_writes_phase34_columns(tmp_path):
    """persist_results writes new columns from ScenarioResult; falls back to safe defaults."""
    store_path = tmp_path / "bench.duckdb"
    r_full = _make_result("s1", "m1")
    r_full.stop_reason = StopReason.STUCK_SIGNATURE
    r_full.escalated = True
    r_full.tokens_in_total = 100
    r_full.tokens_out_total = 50
    
    class OldResult:
        scenario_id = "s2"
        model = "m1"
        passed = True
        patch_valid = True
        patch_applies = True
        confidence = 0.9
        duration_seconds = 1.0
        attempts_to_parse = 1
        diag_score = 0.8
        root_cause_match = None
        category_match = None
        failures = []
        soft_failures = []
        prompt_version = "1.0"
        provider = "anthropic"
        base_url = None

    results = {
        "s1": {"m1": r_full},
        "s2": {"m1": OldResult()},  # type: ignore[dict-item]
    }
    persist_results(results, store_path)
    
    con = duckdb.connect(str(store_path))
    rows = con.execute(
        "SELECT scenario_id, stop_reason, escalated, tokens_in_total, tokens_out_total "
        "FROM benchmark_results ORDER BY scenario_id"
    ).fetchall()
    con.close()
    
    assert rows[0] == ("s1", StopReason.STUCK_SIGNATURE, True, 100, 50)
    assert rows[1] == ("s2", None, False, 0, 0)
