"""CLI wiring for `aqueduct run --target databricks` (remote-submit).

Audit 2026-08-01 (deploy + overrides units) found two related gaps in
`cli/run.py`'s remote-submit branch:

  - `--set` overrides were silently dropped: `package()` uploads
    `blueprint.yml`/`aqueduct.yml` from disk verbatim, never the in-memory
    overridden config/blueprint, while the preamble announced the overrides
    as if they applied. Fixed by refusing `--set` loudly for remote targets.
  - `_submitter.poll(...)` had no try/except at all (unlike its `package()`/
    `submit()` siblings), so its documented `TimeoutError` (a routine "the
    remote job is slow" condition) crashed the CLI with a raw, un-styled
    traceback and no `exit_codes.*` mapping instead of the same clean
    `DATA_OR_RUNTIME` exit its neighbors get.

Both covered here with `get_submitter` mocked — no real Databricks workspace.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from aqueduct.cli import cli
from aqueduct.executor.models import ExecutionResult, ModuleResult

pytestmark = pytest.mark.unit

_BP = """\
aqueduct: '1.0'
id: remote_bp
name: Remote BP
modules:
  - id: src
    type: Ingress
    label: Src
    config: {format: csv, path: /nonexistent/data.csv}
edges: []
"""

_CFG = """\
aqueduct_config: "1.0"
deployment:
  target: databricks
  databricks:
    workspace_url: https://dbc-example.cloud.databricks.com
    cluster_id: test-cluster-id
"""


def _write_project(tmp_path: Path) -> tuple[Path, Path]:
    bp = tmp_path / "bp.yml"
    bp.write_text(_BP, encoding="utf-8")
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(_CFG, encoding="utf-8")
    return bp, cfg


def test_set_override_refused_for_remote_target(tmp_path):
    bp, cfg = _write_project(tmp_path)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["run", str(bp), "--config", str(cfg), "--set", "agent.approval=auto"],
    )
    assert result.exit_code == 1, result.output  # CONFIG_ERROR
    assert "--set is not supported for remote-submit targets" in result.output


def test_poll_timeout_error_exits_data_or_runtime_not_a_crash(tmp_path):
    bp, cfg = _write_project(tmp_path)
    mock_submitter = MagicMock()
    mock_submitter.package.return_value = MagicMock()
    mock_submitter.submit.return_value = "run-123"
    mock_submitter.poll.side_effect = TimeoutError("run-123 did not finish within 3600s")

    runner = CliRunner()
    with patch("aqueduct.deploy.get_submitter", return_value=mock_submitter):
        result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])

    assert result.exit_code == 2, result.output  # DATA_OR_RUNTIME, not an uncaught crash
    assert "remote poll failed" in result.output
    assert "Traceback" not in result.output


def test_fetch_logs_failure_does_not_mask_the_failure_report(tmp_path):
    bp, cfg = _write_project(tmp_path)
    mock_submitter = MagicMock()
    mock_submitter.package.return_value = MagicMock()
    mock_submitter.submit.return_value = "run-123"
    mock_submitter.poll.return_value = ExecutionResult(
        blueprint_id="",
        run_id="run-123",
        status="error",
        module_results=(ModuleResult(module_id="_remote_run", status="error", error="boom"),),
    )
    mock_submitter.fetch_logs.side_effect = RuntimeError("network blip")

    runner = CliRunner()
    with patch("aqueduct.deploy.get_submitter", return_value=mock_submitter):
        result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])

    assert result.exit_code == 2, result.output  # DATA_OR_RUNTIME — the real failure report
    assert "could not fetch remote logs" in result.output
    assert "remote job failed" in result.output
    assert "Traceback" not in result.output
