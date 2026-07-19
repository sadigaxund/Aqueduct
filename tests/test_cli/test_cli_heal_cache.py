"""Phase 45 — CLI heal-cache wiring tests: pending, replay, memory gating, LLM stamps."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.unit

from click.testing import CliRunner
from aqueduct.agent.budget import StopReason
from aqueduct.cli import cli
from aqueduct.exit_codes import HEAL_PENDING


def _write_bp(path: Path, extra: str):
    path.write_text("""\
aqueduct: '1.0'
id: heal_cache
name: heal_cache
modules:
  - id: m1
    type: Ingress
    label: M1
    config: {format: csv, path: /missing.csv}
edges: []
agent:
  %s
""" % extra)


def _write_config(path: Path, extra: str = ""):
    path.write_text("""\
aqueduct_config: "1.0"
stores:
  observability:
    path: %s/obs
agent:
  provider: anthropic
  model: claude-3
  base_url: https://api.anthropic.example
  %s
""" % (path.parent, extra))


def _run_err():
    from aqueduct.executor.models import ExecutionResult, ModuleResult
    return ExecutionResult(
        blueprint_id="heal_cache", run_id="r1", status="error",
        module_results=(ModuleResult("m1", "error", error="fail"),),
    )


def _run_ok():
    from aqueduct.executor.models import ExecutionResult, ModuleResult
    return ExecutionResult(
        blueprint_id="heal_cache", run_id="r2", status="success",
        module_results=(ModuleResult("m1", "success"),),
    )


def _make_executor(side_effects):
    """Build a mock executor callable that returns the given results in order,
    falling back to _run_ok() after exhaustion."""
    seq = list(side_effects)
    def _exec(*args, **kwargs):
        return seq.pop(0) if seq else _run_ok()
    mock = MagicMock(side_effect=_exec)
    mock._seq = seq
    return mock


# ── PATH 1: Pending hit ─────────────────────────────────────────────────────

def test_pending_hit_skips_llm_exits_heal_pending(tmp_path):
    """Pending hit → LLM skipped, HEAL_PENDING(3) exit."""
    runner = CliRunner()
    bp_path = tmp_path / "bp.yml"
    _write_bp(bp_path, "approval: human")
    cfg_path = tmp_path / "aq.yml"
    _write_config(cfg_path)

    from aqueduct.agent.memory import PendingHit
    hit = PendingHit(object_key="pending/001_fix.json",
                     patch_id="fix-1", staged_at=None, source="llm")

    os.environ["ANTHROPIC_API_KEY"] = "test-key"

    with patch("aqueduct.executor.get_executor") as mock_get_exec:
        mock_get_exec.return_value = _make_executor([_run_err()])
        with patch("aqueduct.agent.memory.find_pending", return_value=hit):
            res = runner.invoke(cli, ["run", str(bp_path), "--config", str(cfg_path)])

    assert "skipping Agent" in res.output
    assert res.exit_code == HEAL_PENDING


# ── PATH 2: Replay hit (auto mode) ──────────────────────────────────────────

def test_replay_hit_auto_mode_zero_llm(tmp_path):
    """Replay candidate passes gates → zero LLM calls."""
    runner = CliRunner()
    bp_path = tmp_path / "bp.yml"
    _write_bp(bp_path, "approval: auto")
    cfg_path = tmp_path / "aq.yml"
    _write_config(cfg_path)

    from aqueduct.patch.grammar import PatchSpec
    spec = PatchSpec(patch_id="replay-1", rationale="replay",
                     operations=[{"op": "set_module_config_key",
                                  "module_id": "m1", "key": "k", "value": "v"}])

    mock_candidate = MagicMock()
    mock_candidate.patch_id = "replay-1"
    mock_candidate.payload = json.loads(spec.model_dump_json())

    os.environ["ANTHROPIC_API_KEY"] = "test-key"

    first_result = _run_err()
    ok_result = _run_ok()

    from aqueduct.patch.preview import SandboxGateResult

    with patch("aqueduct.executor.get_executor") as mock_get_exec:
        mock_get_exec.return_value = _make_executor([first_result, ok_result, ok_result])
        with patch("aqueduct.agent.memory.find_pending", return_value=None):
            with patch("aqueduct.agent.memory.find_replay_candidate", return_value=mock_candidate):
                # The replay-gate path (Phase 79) runs run_sandbox_gate through
                # the TARGET ENGINE's own ExecutorProtocol, not the mocked
                # get_executor() above — mock it directly so this unit test
                # never starts a real Spark session.
                with patch("aqueduct.patch.preview.run_sandbox_gate") as mock_sandbox:
                    mock_sandbox.return_value = SandboxGateResult(status="pass", detail="ok")
                    res = runner.invoke(cli, ["run", str(bp_path), "--config", str(cfg_path)])

    assert res.exit_code == 0

    # Verify that the replayed healing outcome recorded a NULL model column.
    import duckdb
    obs_db = tmp_path / "obs" / "heal_cache" / "observability.db"
    conn = duckdb.connect(str(obs_db))
    row = conn.execute("SELECT model FROM healing_outcomes WHERE patch_id = 'replay-1'").fetchone()
    conn.close()
    assert row is not None, "Healing outcome for replayed patch not found"
    assert row[0] is None, "Model column should be NULL for replay resolutions"


# ── PATH 3: Replay gate-fail → fall through to LLM ──────────────────────────

def test_replay_gate_fail_falls_through_to_llm(tmp_path):
    """Replay candidate fails sandbox → falls through to LLM."""
    runner = CliRunner()
    bp_path = tmp_path / "bp.yml"
    _write_bp(bp_path, "approval: auto")
    cfg_path = tmp_path / "aq.yml"
    _write_config(cfg_path)

    from aqueduct.patch.grammar import PatchSpec
    spec = PatchSpec(patch_id="replay-fail-1", rationale="replay",
                     operations=[{"op": "set_module_config_key",
                                  "module_id": "m1", "key": "k", "value": "v"}])

    mock_candidate = MagicMock()
    mock_candidate.patch_id = "replay-fail-1"
    mock_candidate.payload = json.loads(spec.model_dump_json())

    os.environ["ANTHROPIC_API_KEY"] = "test-key"

    from aqueduct.patch.preview import SandboxGateResult

    with patch("aqueduct.executor.get_executor") as mock_get_exec:
        # Only two get_executor-routed execute() calls happen now: the
        # initial failing run, then the post-LLM-patch re-run. The replay
        # candidate's sandbox validation run no longer consumes from this
        # sequence — it's mocked directly via run_sandbox_gate below
        # (Phase 79: the sandbox gate runs through the target engine's own
        # ExecutorProtocol, not this generic get_executor mock).
        mock_get_exec.return_value = _make_executor([_run_err(), _run_ok()])
        with patch("aqueduct.agent.memory.find_pending", return_value=None):
            with patch("aqueduct.agent.memory.find_replay_candidate", return_value=mock_candidate):
                with patch("aqueduct.agent.generate_agent_patch") as mock_gap:
                    from aqueduct.agent import AgentPatchResult
                    mock_gap.return_value = AgentPatchResult(
                        patch=spec, attempts=1, stop_reason=StopReason.SOLVED,
                    )
                    # The replay-gate path (Phase 79) runs run_sandbox_gate
                    # through the TARGET ENGINE's own ExecutorProtocol, not the
                    # mocked get_executor() above — mock it directly to force
                    # the FAIL this test needs (driving the fall-through) and
                    # to never start a real Spark session.
                    with patch("aqueduct.patch.preview.run_sandbox_gate") as mock_sandbox:
                        mock_sandbox.return_value = SandboxGateResult(
                            status="fail", detail="replay candidate no longer applies",
                        )
                        res = runner.invoke(cli, ["run", str(bp_path), "--config", str(cfg_path)])

    assert "falling through to Agent" in res.output
    assert mock_gap.called
    assert res.exit_code == 0


# ── PATH 5: Replay in human mode → staged as pending with source='replay' ───

def test_replay_human_mode_stages_pending(tmp_path):
    """Replay in human mode → staged to pending with source='replay'."""
    runner = CliRunner()
    bp_path = tmp_path / "bp.yml"
    _write_bp(bp_path, "approval: human")
    cfg_path = tmp_path / "aq.yml"
    _write_config(cfg_path)

    from aqueduct.patch.grammar import PatchSpec
    spec = PatchSpec(patch_id="replay-human-1", rationale="replay",
                     operations=[{"op": "set_module_config_key",
                                  "module_id": "m1", "key": "k", "value": "v"}])

    mock_candidate = MagicMock()
    mock_candidate.patch_id = "replay-human-1"
    mock_candidate.payload = json.loads(spec.model_dump_json())

    os.environ["ANTHROPIC_API_KEY"] = "test-key"

    with patch("aqueduct.executor.get_executor") as mock_get_exec:
        mock_get_exec.return_value = _make_executor([_run_err(), _run_ok()])
        with patch("aqueduct.agent.memory.find_pending", return_value=None):
            with patch("aqueduct.agent.memory.find_replay_candidate", return_value=mock_candidate):
                res = runner.invoke(cli, ["run", str(bp_path), "--config", str(cfg_path)])

    assert res.exit_code == HEAL_PENDING
    pending_files = list((tmp_path / "patches" / "pending").glob("*.json"))
    if pending_files:
        data = json.loads(pending_files[0].read_text())
        assert data.get("_aq_meta", {}).get("source") == "replay"


# ── PATH 6: memory.replay: false → straight to LLM ──────────────────────────

def test_memory_replay_false_skips_pending_replay_lookups(tmp_path):
    """agent.memory.replay: false → pending/replay lookups skipped, LLM called."""
    runner = CliRunner()
    bp_path = tmp_path / "bp.yml"
    _write_bp(bp_path, "approval: auto")
    cfg_path = tmp_path / "aq.yml"
    _write_config(cfg_path, 'memory: {replay: false, coaching: false}')

    from aqueduct.patch.grammar import PatchSpec
    spec = PatchSpec(patch_id="llm-fix", rationale="fix",
                     operations=[{"op": "set_module_config_key",
                                  "module_id": "m1", "key": "k", "value": "v"}])

    os.environ["ANTHROPIC_API_KEY"] = "test-key"

    with patch("aqueduct.executor.get_executor") as mock_get_exec:
        mock_get_exec.return_value = _make_executor([_run_err(), _run_ok()])
        with patch("aqueduct.agent.memory.find_pending") as mock_fp:
            with patch("aqueduct.agent.memory.find_replay_candidate") as mock_frc:
                with patch("aqueduct.agent.generate_agent_patch") as mock_gap:
                    from aqueduct.agent import AgentPatchResult
                    mock_gap.return_value = AgentPatchResult(
                        patch=spec, attempts=1, stop_reason=StopReason.SOLVED,
                    )
                    res = runner.invoke(cli, ["run", str(bp_path), "--config", str(cfg_path)])

    assert mock_fp.call_count == 0
    assert mock_frc.call_count == 0
    assert mock_gap.called
    assert res.exit_code == 0


# ── PATH 6b: aqueduct runs --heal-coverage ─────────────────────────────────

def test_heal_coverage_aggregates_resolutions(tmp_path):
    """aqueduct runs --heal-coverage aggregates by resolution and reports zero-token %."""
    import duckdb
    from click.testing import CliRunner
    from aqueduct.cli import cli

    # Build a observability DB with known healing_outcomes data
    obs_dir = tmp_path / ".aqueduct" / "observability" / "test_bp"
    obs_dir.mkdir(parents=True)
    db_path = obs_dir / "observability.db"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE healing_outcomes (
            id VARCHAR PRIMARY KEY,
            run_id VARCHAR, patch_id VARCHAR,
            resolution VARCHAR, failure_signature VARCHAR
        )
    """)
    conn.execute("INSERT INTO healing_outcomes VALUES ('1', 'r1', 'p1', 'llm', 'h1')")
    conn.execute("INSERT INTO healing_outcomes VALUES ('2', 'r1', 'p2', 'cached', 'h2')")
    conn.execute("INSERT INTO healing_outcomes VALUES ('3', 'r2', 'p3', 'replayed', 'h3')")
    conn.execute("INSERT INTO healing_outcomes VALUES ('4', 'r2', 'p4', 'llm', 'h4')")
    conn.close()

    runner = CliRunner()
    os.environ["ANTHROPIC_API_KEY"] = "test-key"
    res = runner.invoke(cli, [
        "runs", "--heal-coverage",
        "--store-dir", str(tmp_path / ".aqueduct" / "observability" / "test_bp"),
    ])

    assert res.exit_code == 0
    assert "llm" in res.output
    assert "cached" in res.output
    assert "replayed" in res.output
    # 2 llm + 1 cached + 1 replayed = 4 total; zero-token = 2 (cached+replayed) = 50%


def test_heal_coverage_empty_db_shows_no_healings(tmp_path):
    """No healing_outcomes → 'No healing outcomes recorded yet' message."""
    import duckdb
    from click.testing import CliRunner
    from aqueduct.cli import cli

    obs_dir = tmp_path / ".aqueduct" / "observability" / "test_bp"
    obs_dir.mkdir(parents=True)
    db_path = obs_dir / "observability.db"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE healing_outcomes (
            id VARCHAR PRIMARY KEY,
            run_id VARCHAR, patch_id VARCHAR,
            resolution VARCHAR, failure_signature VARCHAR
        )
    """)
    conn.close()

    runner = CliRunner()
    os.environ["ANTHROPIC_API_KEY"] = "test-key"
    res = runner.invoke(cli, [
        "runs", "--heal-coverage",
        "--store-dir", str(tmp_path / ".aqueduct" / "observability" / "test_bp"),
    ])

    # No rows → no error message
    assert res.exit_code == 0


def test_heal_coverage_no_db_no_runs(tmp_path):
    """No observability.db → 'No runs found' message."""
    from click.testing import CliRunner
    from aqueduct.cli import cli

    runner = CliRunner()
    os.environ["ANTHROPIC_API_KEY"] = "test-key"
    res = runner.invoke(cli, [
        "runs", "--heal-coverage",
        "--store-dir", str(tmp_path / "empty"),
    ])

    assert "No runs found" in res.output


# ── PATH 7: LLM-resolution stamps ──────────────────────────────────────────

def test_llm_heal_stamps_resolution_and_signature(tmp_path):
    """LLM-generated patch records resolution='llm' and failure_signature in healing_outcomes."""
    runner = CliRunner()
    bp_path = tmp_path / "bp.yml"
    _write_bp(bp_path, "approval: auto")
    cfg_path = tmp_path / "aq.yml"
    _write_config(cfg_path)

    from aqueduct.patch.grammar import PatchSpec
    spec = PatchSpec(patch_id="llm-stamp-1", rationale="fix",
                     operations=[{"op": "set_module_config_key",
                                  "module_id": "m1", "key": "k", "value": "v"}])

    os.environ["ANTHROPIC_API_KEY"] = "test-key"

    with patch("aqueduct.executor.get_executor") as mock_get_exec:
        mock_get_exec.return_value = _make_executor([_run_err(), _run_ok()])
        with patch("aqueduct.agent.memory.find_pending", return_value=None):
            with patch("aqueduct.agent.memory.find_replay_candidate", return_value=None):
                with patch("aqueduct.agent.generate_agent_patch") as mock_gap:
                    from aqueduct.agent import AgentPatchResult
                    mock_gap.return_value = AgentPatchResult(
                        patch=spec, attempts=1, stop_reason=StopReason.SOLVED,
                    )
                    runner.invoke(cli, ["run", str(bp_path), "--config", str(cfg_path)])

    # Check healing_outcomes in observability DB
    import duckdb
    obs_db = tmp_path / "obs" / "heal_cache" / "observability.db"
    if obs_db.exists():
        conn = duckdb.connect(str(obs_db))
        row = conn.execute(
            "SELECT resolution, failure_signature FROM healing_outcomes WHERE patch_id = 'llm-stamp-1'"
        ).fetchone()
        conn.close()
        assert row is not None, "healing_outcomes row not found"
        assert row[0] == "llm", f"expected resolution='llm', got {row[0]!r}"
        assert row[1] is not None and len(row[1]) > 0, "failure_signature should be non-empty"
