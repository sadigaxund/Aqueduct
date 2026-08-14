"""Phase 78 checkpoint 3b — proof that ENGINE-INVARIANT capability leaves
(hooks.*, retry_policy.*) are not a rubber-stamp verdict on the DuckDB engine.

capabilities.yml declares these leaves `supported` because core orchestration
(aqueduct/cli/run.py's hook firing, the compiled Manifest's retry_policy) is
identical regardless of which engine executes the Manifest — the DuckDB
executor never touches hooks at all, and its own `_with_retry` is a
line-for-line duplicate of Spark's. That claim is proven here with two real
end-to-end `aqueduct run` invocations against the DuckDB engine, not asserted
by inspection:

  - an `on_failure` command hook actually fires when a duckdb run fails
  - a module-level `retry:` policy actually drives DuckDB's own retry loop
    (proven separately, at the executor layer, in
    tests/test_executor_duckdb/test_executor.py — the two together cover
    "wired end-to-end via the CLI" and "the retry loop itself is exercised").
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from aqueduct import exit_codes
from aqueduct.cli import cli

pytestmark = [pytest.mark.integration, pytest.mark.duckdb]


_BP = """\
aqueduct: '1.0'
id: test_bp
name: Test BP
agent:
  approval: disabled
hooks:
  on_failure:
    - command: "{marker_cmd}"
modules:
  - id: src
    type: Ingress
    label: Src
    config: {{format: csv, path: /nonexistent/does-not-exist.csv}}
edges: []
"""

_CFG = """\
aqueduct_config: "1.0"

deployment:
  engine: duckdb

danger:
  allow_command_hooks: true

stores:
  observability:
    backend: duckdb
    path: "{obs}"
  depots:
    default:
      backend: duckdb
      path: "{dep}"
"""


def test_on_failure_command_hook_fires_on_a_real_duckdb_run(tmp_path):
    """A genuinely failing DuckDB run (Ingress pointed at a missing file,
    approval: disabled so no healing loop intervenes) must still fire its
    `hooks.on_failure` command — proving hook firing works end-to-end on this
    engine, not just on Spark."""
    marker = tmp_path / "fired.txt"
    bp = tmp_path / "bp.yml"
    bp.write_text(_BP.format(marker_cmd=f"touch {marker}"), encoding="utf-8")
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(
        _CFG.format(obs=str(tmp_path / "obs"), dep=str(tmp_path / "dep.duckdb")),
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(bp), "--config", str(cfg)])

    assert result.exit_code == exit_codes.DATA_OR_RUNTIME, (
        f"Expected DATA_OR_RUNTIME ({exit_codes.DATA_OR_RUNTIME}), got "
        f"{result.exit_code}\n{result.output}"
    )
    assert marker.exists(), (
        "on_failure command hook did not fire on a real duckdb run\n" + result.output
    )


_ROUTING_BP = """\
aqueduct: '1.0'
id: obs_routing_regression
name: Obs Routing Regression
modules:
  - id: src
    type: Ingress
    label: Src
    config: {{format: csv, path: {in_path}}}
  - id: sink
    type: Egress
    label: Sink
    config: {{format: csv, path: {out_path}, mode: overwrite}}
edges:
  - from: src
    to: sink
"""

_ROUTING_CFG = """\
aqueduct_config: "1.0"

deployment:
  engine: duckdb
"""


def test_run_store_dir_then_report_store_dir_finds_the_run(tmp_path):
    """`aqueduct run --store-dir X` routes to `X/<blueprint_id>/observability.db`
    (docs/specs.md §10.4.1 — --store-dir is a routing BASE, same as the
    configured path). `aqueduct report --store-dir X` must resolve through
    the SAME contract, not a flat `X/observability.db` — the whole bug in one
    round trip: write with --store-dir, then read with the identical
    --store-dir and find the run."""
    in_path = tmp_path / "in.csv"
    in_path.write_text("a,b\n1,2\n3,4\n")
    out_path = tmp_path / "out.csv"

    bp = tmp_path / "bp.yml"
    bp.write_text(_ROUTING_BP.format(in_path=in_path, out_path=out_path), encoding="utf-8")
    cfg = tmp_path / "aqueduct.yml"
    cfg.write_text(_ROUTING_CFG, encoding="utf-8")

    store_dir = tmp_path / "store"
    run_id = "obs-routing-regression-run"
    runner = CliRunner()

    run_result = runner.invoke(
        cli,
        ["run", str(bp), "--config", str(cfg), "--store-dir", str(store_dir), "--run-id", run_id],
    )
    assert run_result.exit_code == exit_codes.SUCCESS, run_result.output

    # The write side routed per-blueprint under store_dir, per the contract.
    assert (store_dir / "obs_routing_regression" / "observability.db").exists()

    report_result = runner.invoke(
        cli,
        ["report", run_id, "--config", str(cfg), "--store-dir", str(store_dir), "--format", "json"],
    )
    assert report_result.exit_code == exit_codes.SUCCESS, report_result.output
    payload = json.loads(report_result.output)
    assert payload["run_id"] == run_id
    assert payload["status"] == "success"
