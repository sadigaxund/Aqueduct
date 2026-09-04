"""CLI heal-loop wiring tests: the pending-patch short-circuit and LLM stamps.

Phase 92 removed the signature-keyed heal cache (`aqueduct/agent/memory.py`)
outright — pending-patch reuse, exact replay, and coaching-example retrieval
are gone. What replaces the operationally-important half of pending-reuse is
the pending-patch short-circuit tested here: a blueprint that already has an
unreviewed patch in `patches/pending/` never gets a second LLM call for the
same problem.
"""

from __future__ import annotations

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


# ── Pending-patch short-circuit ──────────────────────────────────────────────


def test_pending_patch_for_blueprint_short_circuits_before_agent(tmp_path):
    """A pending patch already exists for this blueprint's id → the run exits
    HEAL_PENDING, stages nothing new, and the Agent is never called."""
    runner = CliRunner()
    bp_path = tmp_path / "bp.yml"
    _write_bp(bp_path, "approval: human")
    cfg_path = tmp_path / "aq.yml"
    _write_config(cfg_path)

    # Seed patch_index with a pending row for this blueprint BEFORE the run
    # starts — the same table `aqueduct patch list`/`pull` read, and what the
    # guard under test queries via `list_by_status(status="pending", ...)`.
    from aqueduct.patch.index import PatchIndexRow, ensure_schema, upsert
    from aqueduct.stores.duckdb_ import DuckDBObservabilityStore

    obs_db_dir = tmp_path / "obs" / "heal_cache"
    obs_db_dir.mkdir(parents=True)
    store = DuckDBObservabilityStore(obs_db_dir / "observability.db")
    with store.connect() as cur:
        ensure_schema(cur)
        upsert(
            cur,
            PatchIndexRow(
                patch_id="existing-pending-1",
                status="pending",
                object_key="pending/existing-pending-1.json",
                blueprint_id="heal_cache",
            ),
        )

    os.environ["ANTHROPIC_API_KEY"] = "test-key"

    with patch("aqueduct.executor.get_executor") as mock_get_exec:
        mock_get_exec.return_value = _make_executor([_run_err()])
        with patch("aqueduct.agent.generate_agent_patch") as mock_gap:
            res = runner.invoke(cli, ["run", str(bp_path), "--config", str(cfg_path)])

    assert res.exit_code == HEAL_PENDING
    assert "existing-pending-1" in res.output
    assert "skipping Agent" in res.output
    assert not mock_gap.called
    # Nothing new staged to patches/pending/ — the existing patch is untouched.
    pending_files = list((tmp_path / "patches" / "pending").glob("*.json"))
    assert pending_files == []


def test_no_pending_patch_calls_agent_normally(tmp_path):
    """No pending row for this blueprint → the guard is a no-op and the
    normal LLM heal path runs."""
    runner = CliRunner()
    bp_path = tmp_path / "bp.yml"
    _write_bp(bp_path, "approval: auto")
    cfg_path = tmp_path / "aq.yml"
    _write_config(cfg_path)

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
        with patch("aqueduct.agent.generate_agent_patch") as mock_gap:
            from aqueduct.agent import AgentPatchResult

            mock_gap.return_value = AgentPatchResult(
                patch=spec,
                attempts=1,
                stop_reason=StopReason.SOLVED,
            )
            with patch("aqueduct.patch.preview.run_sandbox_gate") as mock_sandbox:
                mock_sandbox.return_value = SandboxGateResult(status="pass", detail="ok")
                res = runner.invoke(cli, ["run", str(bp_path), "--config", str(cfg_path)])

    assert mock_gap.called
    assert res.exit_code == 0


# ── aqueduct runs --heal-coverage ───────────────────────────────────────────
# `--heal-coverage` is a pure read-side aggregation over whatever
# `healing_outcomes.resolution` values are in the store — it makes no
# assumption about which code path wrote them, so it is exercised directly
# against hand-seeded rows (including the legacy `cached`/`replayed` values a
# pre-2.3.0 store may still carry) rather than through a live run.


def test_heal_coverage_aggregates_resolutions(tmp_path):
    """aqueduct runs --heal-coverage aggregates by resolution and reports zero-token %."""
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


# ── LLM-resolution stamps ────────────────────────────────────────────────────


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
        with patch("aqueduct.agent.generate_agent_patch") as mock_gap:
            from aqueduct.agent import AgentPatchResult

            mock_gap.return_value = AgentPatchResult(
                patch=spec,
                attempts=1,
                stop_reason=StopReason.SOLVED,
            )
            # The inline patch-gates path (Phase 79) runs run_sandbox_gate
            # through the TARGET ENGINE's own ExecutorProtocol, not the
            # mocked get_executor() above — mock it directly so this unit
            # test never starts (and, believing it owns a throwaway sandbox
            # session, stops) the REAL shared SparkSession (ISSUE-026).
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
