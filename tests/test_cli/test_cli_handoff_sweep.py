"""`aqueduct handoff sweep` (Phase 85 Wave 5, Task 2).

Wires `aqueduct/executor/spill.py::plan_orphan_sweep`/`sweep_orphan_spills`
(the orphan-vs-live decision rule already used by the automatic per-run
sweep) into a standalone CLI verb, since a Blueprint that fails and is
never rerun previously kept its spill forever (see `sweep_orphan_spills`'s
docstring — "STILL OPEN" became "CLOSED" in this phase).

Seeds an observability store directly with the real DDL (no Spark, no
executor import — same approach as ``test_report_prune_costs.py``) and lays
out fake spill directories by hand, since only the directory NAMES
(``<root>/<manifest_hash>/<run_id>/<edge_id>/``) and ``run_records`` rows
matter to the sweep — nothing here needs a real run.
"""

from __future__ import annotations

import json

import duckdb
import pytest
from click.testing import CliRunner

from aqueduct import exit_codes
from aqueduct.cli import cli
from aqueduct.surveyor import ddl as _ddl

pytestmark = pytest.mark.unit


def _conn(store_dir):
    store_dir.mkdir(parents=True, exist_ok=True)
    c = duckdb.connect(str(store_dir / "observability.db"))
    c.execute(_ddl._DDL)
    return c


def _run_record(c, run_id, blueprint_id, status, finished_at):
    c.execute(
        "INSERT INTO run_records (run_id, blueprint_id, status, started_at, finished_at, "
        "module_results) VALUES (?,?,?,?,?,?)",
        [run_id, blueprint_id, status, "2020-01-01T00:00:00+00:00", finished_at, "[]"],
    )


def _spill(root, manifest_hash, run_id, content=b"x"):
    d = root / manifest_hash / run_id / "edge1"
    d.mkdir(parents=True)
    (d / "part.parquet").write_bytes(content)
    return root / manifest_hash / run_id


@pytest.fixture
def project(tmp_path):
    config = tmp_path / "aqueduct.yml"
    config.write_text(
        """
aqueduct_config: "1.0"
deployment:
  engine: duckdb
  target: local
handoff:
  root: handoff_spill
  keep_on_failure: true
""",
        encoding="utf-8",
    )
    store_dir = tmp_path / "store"
    conn = _conn(store_dir)
    _run_record(conn, "run-old-fail", "bp1", "error", "2020-01-01T00:00:00+00:00")
    _run_record(conn, "run-superseding-success", "bp1", "success", "2020-01-02T00:00:00+00:00")
    _run_record(conn, "run-live", "bp1", "running", None)
    conn.close()

    root = tmp_path / "handoff_spill"
    _spill(root, "HASH1", "run-old-fail")
    _spill(root, "HASH1", "run-unknown")  # no run_records row at all
    _spill(root, "HASH1", "run-live")

    return tmp_path, config, store_dir, root


def _invoke(runner, config, store_dir, *extra):
    return runner.invoke(
        cli,
        [
            "handoff",
            "sweep",
            "--config",
            str(config),
            "--store-dir",
            str(store_dir),
            *extra,
        ],
    )


def test_dry_run_is_the_default_and_removes_nothing(project):
    tmp_path, config, store_dir, root = project
    runner = CliRunner()
    result = _invoke(runner, config, store_dir)
    assert result.exit_code == exit_codes.SUCCESS, result.output
    assert "dry run" in result.output
    assert "Pass --execute" in result.output
    # Nothing was deleted.
    assert (root / "HASH1" / "run-old-fail").exists()
    assert (root / "HASH1" / "run-unknown").exists()
    assert (root / "HASH1" / "run-live").exists()


def test_dry_run_reports_what_would_be_removed_and_why(project):
    tmp_path, config, store_dir, root = project
    runner = CliRunner()
    result = _invoke(runner, config, store_dir, "--format", "json")
    assert result.exit_code == exit_codes.SUCCESS, result.output
    payload = json.loads(result.output)
    assert payload["dry_run"] is True
    by_run = {c["run_id"]: c for c in payload["candidates"]}

    assert "run-old-fail" in by_run
    assert "superseded" in by_run["run-old-fail"]["reason"]
    assert "run-unknown" in by_run
    assert "unknown" in by_run["run-unknown"]["reason"]
    # A live (non-terminal) run is NEVER a candidate.
    assert "run-live" not in by_run


def test_live_run_spill_is_never_removed_even_with_execute(project):
    tmp_path, config, store_dir, root = project
    runner = CliRunner()
    result = _invoke(runner, config, store_dir, "--execute")
    assert result.exit_code == exit_codes.SUCCESS, result.output
    assert (root / "HASH1" / "run-live").exists()
    assert not (root / "HASH1" / "run-old-fail").exists()
    assert not (root / "HASH1" / "run-unknown").exists()


def test_execute_removes_orphans_and_reports_the_count(project):
    tmp_path, config, store_dir, root = project
    runner = CliRunner()
    result = _invoke(runner, config, store_dir, "--execute")
    assert result.exit_code == exit_codes.SUCCESS, result.output
    assert "removed 2/2" in result.output


def test_json_format_is_valid_and_ansi_free(project):
    tmp_path, config, store_dir, root = project
    runner = CliRunner()
    result = _invoke(runner, config, store_dir, "--format", "json")
    assert result.exit_code == exit_codes.SUCCESS, result.output
    assert "\x1b[" not in result.output
    json.loads(result.output)  # parses cleanly


def test_a_kept_failure_not_yet_superseded_is_never_removed(tmp_path):
    """A terminal failure under handoff.keep_on_failure is untouched until a
    LATER successful run of the same blueprint resolves it — the resume
    story `aqueduct run --resume` depends on."""
    config = tmp_path / "aqueduct.yml"
    config.write_text(
        """
aqueduct_config: "1.0"
deployment:
  engine: duckdb
  target: local
handoff:
  root: handoff_spill
  keep_on_failure: true
""",
        encoding="utf-8",
    )
    store_dir = tmp_path / "store"
    conn = _conn(store_dir)
    _run_record(conn, "run-kept-fail", "bp1", "error", "2020-01-01T00:00:00+00:00")
    conn.close()

    root = tmp_path / "handoff_spill"
    _spill(root, "HASH1", "run-kept-fail")

    runner = CliRunner()
    result = _invoke(runner, config, store_dir, "--execute")
    assert result.exit_code == exit_codes.SUCCESS, result.output
    assert (root / "HASH1" / "run-kept-fail").exists()
