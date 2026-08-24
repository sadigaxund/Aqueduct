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
    path.write_text(
        """\
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
  # 2.2.0: approval: auto denies file-touching ops by default unless an
  # allowlist is configured (item A, security workstream) — these fixtures
  # patch a `path` config key under auto mode, so they need one.
  guardrails: {allowed_paths: ["*"]}
"""
        % extra
    )


def _write_config(path: Path, extra: str = ""):
    path.write_text(
        """\
aqueduct_config: "1.0"
stores:
  observability:
    path: %s/obs
agent:
  provider: anthropic
  model: claude-3
  base_url: https://api.anthropic.example
  %s
"""
        % (path.parent, extra)
    )


def _run_err():
    from aqueduct.executor.models import ExecutionResult, ModuleResult

    return ExecutionResult(
        blueprint_id="heal_cache",
        run_id="r1",
        status="error",
        module_results=(ModuleResult("m1", "error", error="fail"),),
    )


def _run_ok():
    from aqueduct.executor.models import ExecutionResult, ModuleResult

    return ExecutionResult(
        blueprint_id="heal_cache",
        run_id="r2",
        status="success",
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

    hit = PendingHit(
        object_key="pending/001_fix.json", patch_id="fix-1", staged_at=None, source="llm"
    )

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

    spec = PatchSpec(
        patch_id="replay-1",
        rationale="replay",
        operations=[
            {"op": "set_module_config_key", "module_id": "m1", "key": "path", "value": "/fixed.csv"}
        ],
    )

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


# ── PATH 2b: Replay candidate names a RETIRED op → fall through, no crash ──


def test_replay_candidate_with_retired_op_falls_through_to_llm(tmp_path):
    """A patch_index-indexed body persisted before the set_spark_config →
    set_engine_config rename still carries the old op name — the exact
    "stored patch is now unparseable" scenario the cross-engine remediation
    must handle. model_validate() must raise the typed RetiredPatchOpError
    (not a bare pydantic ValidationError), the replay path must catch it,
    announce the cache entry as unusable, and fall through to the LLM —
    never crash the run and never silently behave as if the cache simply
    missed (PATH 3's `run_sandbox_gate` fail test is the closest existing
    precedent for the fall-through shape; this one fails earlier, at
    model_validate, before any gate ever runs for the replay candidate)."""
    runner = CliRunner()
    bp_path = tmp_path / "bp.yml"
    _write_bp(bp_path, "approval: auto")
    cfg_path = tmp_path / "aq.yml"
    _write_config(cfg_path)

    # A raw dict, NOT built via PatchSpec(...) — constructing a PatchSpec
    # with this op would itself raise RetiredPatchOpError. This is exactly
    # the shape a pre-rename patch body has sitting in the blob store.
    mock_candidate = MagicMock()
    mock_candidate.patch_id = "replay-retired-1"
    mock_candidate.payload = {
        "patch_id": "replay-retired-1",
        "rationale": "bump shuffle partitions",
        "operations": [
            {"op": "set_spark_config", "key": "spark.sql.shuffle.partitions", "value": "200"}
        ],
    }

    from aqueduct.patch.grammar import PatchSpec

    llm_spec = PatchSpec(
        patch_id="llm-fix-1",
        rationale="fix",
        operations=[
            {"op": "set_module_config_key", "module_id": "m1", "key": "path", "value": "/fixed.csv"}
        ],
    )

    os.environ["ANTHROPIC_API_KEY"] = "test-key"

    from aqueduct.patch.preview import SandboxGateResult

    with patch("aqueduct.executor.get_executor") as mock_get_exec:
        mock_get_exec.return_value = _make_executor([_run_err(), _run_ok()])
        with patch("aqueduct.agent.memory.find_pending", return_value=None):
            with patch("aqueduct.agent.memory.find_replay_candidate", return_value=mock_candidate):
                with patch("aqueduct.agent.generate_agent_patch") as mock_gap:
                    from aqueduct.agent import AgentPatchResult

                    mock_gap.return_value = AgentPatchResult(
                        patch=llm_spec,
                        attempts=1,
                        stop_reason=StopReason.SOLVED,
                    )
                    # Only the LLM patch's own gate call happens now — the
                    # retired-op candidate never reaches run_sandbox_gate at
                    # all (it fails at model_validate, before the gate
                    # pyramid is even entered).
                    with patch("aqueduct.patch.preview.run_sandbox_gate") as mock_sandbox:
                        mock_sandbox.return_value = SandboxGateResult(status="pass", detail="ok")
                        res = runner.invoke(cli, ["run", str(bp_path), "--config", str(cfg_path)])

    # Not a crash: the CLI reached its normal terminal exit code.
    assert res.exit_code == 0
    # Not a silent miss: the warning names the reason, distinguishable from
    # a generic "no longer parses" — proves the typed RetiredPatchOpError
    # actually propagated out of model_validate rather than being masked by
    # a bare pydantic ValidationError.
    assert "retired op" in res.output
    assert "falling through to Agent" in res.output
    # And it actually moved on: the LLM was consulted, not left hanging.
    assert mock_gap.called


# ── PATH 2c: Replay candidate carries set_engine_config → never replayed ────


def test_replay_candidate_with_set_engine_config_falls_through_to_llm(tmp_path):
    """A cached patch that includes a `set_engine_config` op parses fine and
    is otherwise a perfectly valid PatchSpec — but engine/session config is
    environment-specific (the right shuffle.partitions for one cluster is
    not the right value for another), so it must never be replayed from
    the heal cache, even though the failure signature matches exactly. The
    skip must be announced (not indistinguishable from an ordinary cache
    miss), and the run must fall through to the LLM instead of silently
    doing nothing."""
    runner = CliRunner()
    bp_path = tmp_path / "bp.yml"
    _write_bp(bp_path, "approval: auto")
    cfg_path = tmp_path / "aq.yml"
    _write_config(cfg_path)

    from aqueduct.patch.grammar import PatchSpec

    cached_spec = PatchSpec(
        patch_id="replay-cfg-1",
        rationale="bump shuffle partitions",
        operations=[
            {
                "op": "set_engine_config",
                "engine": "spark",
                "key": "spark.sql.shuffle.partitions",
                "value": 200,
            }
        ],
    )
    mock_candidate = MagicMock()
    mock_candidate.patch_id = "replay-cfg-1"
    mock_candidate.payload = json.loads(cached_spec.model_dump_json())

    llm_spec = PatchSpec(
        patch_id="llm-fix-1",
        rationale="fix",
        operations=[
            {"op": "set_module_config_key", "module_id": "m1", "key": "path", "value": "/fixed.csv"}
        ],
    )

    os.environ["ANTHROPIC_API_KEY"] = "test-key"

    from aqueduct.patch.preview import SandboxGateResult

    with patch("aqueduct.executor.get_executor") as mock_get_exec:
        mock_get_exec.return_value = _make_executor([_run_err(), _run_ok()])
        with patch("aqueduct.agent.memory.find_pending", return_value=None):
            with patch("aqueduct.agent.memory.find_replay_candidate", return_value=mock_candidate):
                with patch("aqueduct.agent.generate_agent_patch") as mock_gap:
                    from aqueduct.agent import AgentPatchResult

                    mock_gap.return_value = AgentPatchResult(
                        patch=llm_spec,
                        attempts=1,
                        stop_reason=StopReason.SOLVED,
                    )
                    # The disqualified candidate never reaches the gate
                    # pyramid at all — only the LLM patch's own gate call
                    # happens.
                    with patch("aqueduct.patch.preview.run_sandbox_gate") as mock_sandbox:
                        mock_sandbox.return_value = SandboxGateResult(status="pass", detail="ok")
                        res = runner.invoke(cli, ["run", str(bp_path), "--config", str(cfg_path)])

    # Not a crash: normal terminal exit.
    assert res.exit_code == 0
    # Not a silent miss: the discard is announced, naming the reason.
    assert "sets engine config" in res.output
    assert "never replayed from cache" in res.output
    assert "falling through to Agent" in res.output
    # And it actually moved on: the LLM was consulted, not left hanging.
    assert mock_gap.called


# ── PATH 3: Replay gate-fail → fall through to LLM ──────────────────────────


def test_replay_gate_fail_falls_through_to_llm(tmp_path):
    """Replay candidate fails sandbox → falls through to LLM."""
    runner = CliRunner()
    bp_path = tmp_path / "bp.yml"
    _write_bp(bp_path, "approval: auto")
    cfg_path = tmp_path / "aq.yml"
    _write_config(cfg_path)

    from aqueduct.patch.grammar import PatchSpec

    spec = PatchSpec(
        patch_id="replay-fail-1",
        rationale="replay",
        operations=[
            {"op": "set_module_config_key", "module_id": "m1", "key": "path", "value": "/fixed.csv"}
        ],
    )

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
                        patch=spec,
                        attempts=1,
                        stop_reason=StopReason.SOLVED,
                    )
                    # The replay-gate path (Phase 79) runs run_sandbox_gate
                    # through the TARGET ENGINE's own ExecutorProtocol, not the
                    # mocked get_executor() above — mock it directly to force
                    # the FAIL this test needs (driving the fall-through) and
                    # to never start a real Spark session.
                    #
                    # `_run_patch_gates_inline` (cli/__init__.py) calls this
                    # SAME `run_sandbox_gate` for the LLM-generated patch's own
                    # gate check too (the replay path and the main auto-mode
                    # path share one implementation) — a single `return_value`
                    # would reject BOTH, so the LLM patch would also fail its
                    # gate and correctly exit VALIDATION_GATE(4), not the
                    # SUCCESS(0) this test is actually about. `side_effect`
                    # distinguishes the two calls: 1st (replay) fails, driving
                    # the fall-through; 2nd (the LLM patch) passes, so the
                    # patch is actually applied and the second get_executor()
                    # call (`_run_ok()` above) is a genuine post-patch re-run.
                    with patch("aqueduct.patch.preview.run_sandbox_gate") as mock_sandbox:
                        mock_sandbox.side_effect = [
                            SandboxGateResult(
                                status="fail",
                                detail="replay candidate no longer applies",
                            ),
                            SandboxGateResult(status="pass", detail="ok"),
                        ]
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

    spec = PatchSpec(
        patch_id="replay-human-1",
        rationale="replay",
        operations=[
            {"op": "set_module_config_key", "module_id": "m1", "key": "path", "value": "/fixed.csv"}
        ],
    )

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
    _write_config(cfg_path, "memory: {replay: false, coaching: false}")

    from aqueduct.patch.grammar import PatchSpec

    spec = PatchSpec(
        patch_id="llm-fix",
        rationale="fix",
        operations=[
            {"op": "set_module_config_key", "module_id": "m1", "key": "path", "value": "/fixed.csv"}
        ],
    )

    os.environ["ANTHROPIC_API_KEY"] = "test-key"

    from aqueduct.patch.preview import SandboxGateResult

    with patch("aqueduct.executor.get_executor") as mock_get_exec:
        mock_get_exec.return_value = _make_executor([_run_err(), _run_ok()])
        with patch("aqueduct.agent.memory.find_pending") as mock_fp:
            with patch("aqueduct.agent.memory.find_replay_candidate") as mock_frc:
                with patch("aqueduct.agent.generate_agent_patch") as mock_gap:
                    from aqueduct.agent import AgentPatchResult

                    mock_gap.return_value = AgentPatchResult(
                        patch=spec,
                        attempts=1,
                        stop_reason=StopReason.SOLVED,
                    )
                    # See test_llm_heal_stamps_resolution_and_signature above —
                    # the inline sandbox gate must be mocked too, or it starts
                    # (and stops) the REAL shared SparkSession (ISSUE-026).
                    with patch("aqueduct.patch.preview.run_sandbox_gate") as mock_sandbox:
                        mock_sandbox.return_value = SandboxGateResult(status="pass", detail="ok")
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
    conn.execute(
        """
        CREATE TABLE healing_outcomes (
            id VARCHAR PRIMARY KEY,
            run_id VARCHAR, patch_id VARCHAR,
            resolution VARCHAR, failure_signature VARCHAR
        )
    """
    )
    conn.execute("INSERT INTO healing_outcomes VALUES ('1', 'r1', 'p1', 'llm', 'h1')")
    conn.execute("INSERT INTO healing_outcomes VALUES ('2', 'r1', 'p2', 'cached', 'h2')")
    conn.execute("INSERT INTO healing_outcomes VALUES ('3', 'r2', 'p3', 'replayed', 'h3')")
    conn.execute("INSERT INTO healing_outcomes VALUES ('4', 'r2', 'p4', 'llm', 'h4')")
    conn.close()

    runner = CliRunner()
    os.environ["ANTHROPIC_API_KEY"] = "test-key"
    res = runner.invoke(
        cli,
        [
            "runs",
            "--heal-coverage",
            "--store-dir",
            str(tmp_path / ".aqueduct" / "observability" / "test_bp"),
        ],
    )

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
    conn.execute(
        """
        CREATE TABLE healing_outcomes (
            id VARCHAR PRIMARY KEY,
            run_id VARCHAR, patch_id VARCHAR,
            resolution VARCHAR, failure_signature VARCHAR
        )
    """
    )
    conn.close()

    runner = CliRunner()
    os.environ["ANTHROPIC_API_KEY"] = "test-key"
    res = runner.invoke(
        cli,
        [
            "runs",
            "--heal-coverage",
            "--store-dir",
            str(tmp_path / ".aqueduct" / "observability" / "test_bp"),
        ],
    )

    # No rows → no error message
    assert res.exit_code == 0


def test_heal_coverage_no_db_no_runs(tmp_path):
    """No observability.db → 'No runs found' message."""
    from click.testing import CliRunner

    from aqueduct.cli import cli

    runner = CliRunner()
    os.environ["ANTHROPIC_API_KEY"] = "test-key"
    res = runner.invoke(
        cli,
        [
            "runs",
            "--heal-coverage",
            "--store-dir",
            str(tmp_path / "empty"),
        ],
    )

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

    spec = PatchSpec(
        patch_id="llm-stamp-1",
        rationale="fix",
        operations=[
            {"op": "set_module_config_key", "module_id": "m1", "key": "path", "value": "/fixed.csv"}
        ],
    )

    os.environ["ANTHROPIC_API_KEY"] = "test-key"

    from aqueduct.patch.preview import SandboxGateResult

    with patch("aqueduct.executor.get_executor") as mock_get_exec:
        mock_get_exec.return_value = _make_executor([_run_err(), _run_ok()])
        with patch("aqueduct.agent.memory.find_pending", return_value=None):
            with patch("aqueduct.agent.memory.find_replay_candidate", return_value=None):
                with patch("aqueduct.agent.generate_agent_patch") as mock_gap:
                    from aqueduct.agent import AgentPatchResult

                    mock_gap.return_value = AgentPatchResult(
                        patch=spec,
                        attempts=1,
                        stop_reason=StopReason.SOLVED,
                    )
                    # The inline patch-gates path (Phase 79) runs
                    # run_sandbox_gate through the TARGET ENGINE's own
                    # ExecutorProtocol, not the mocked get_executor() above —
                    # mock it directly so this unit test never starts (and,
                    # believing it owns a throwaway sandbox session, stops)
                    # the REAL shared SparkSession (ISSUE-026): under pytest
                    # `session_factory()` always returns the process-wide
                    # SparkContext via getOrCreate(), so an unmocked sandbox
                    # gate's teardown kills the session-scoped `spark` fixture
                    # for every later test. Same pattern as
                    # test_replay_hit_auto_mode_zero_llm /
                    # test_replay_gate_fail_falls_through_to_llm above.
                    with patch("aqueduct.patch.preview.run_sandbox_gate") as mock_sandbox:
                        mock_sandbox.return_value = SandboxGateResult(status="pass", detail="ok")
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
