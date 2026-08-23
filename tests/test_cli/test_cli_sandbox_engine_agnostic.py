"""``aqueduct run --sandbox`` is engine-agnostic (Phase 89 add-on).

Before this change, ``--sandbox`` was hardcoded Spark-only
(``if engine != "spark": ... sys.exit(...)``) even though the Gate 3 sandbox
machinery it reuses (``aqueduct.patch.preview.run_sandbox_gate``) already
runs through ``ExecutorProtocol`` on any registered engine. This module
proves a DuckDB blueprint now dry-runs successfully end-to-end through the
CLI ``--sandbox`` flag, and that the Spark path still works unchanged.
"""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from aqueduct.cli import cli

pytestmark = [pytest.mark.integration]

_BP = """\
aqueduct: '1.0'
id: sandbox_engine_agnostic_bp
name: Sandbox Engine Agnostic BP
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

_DUCKDB_CFG = """\
aqueduct_config: "1.0"

deployment:
  engine: duckdb
"""


@pytest.mark.duckdb
def test_sandbox_dry_run_succeeds_on_duckdb_blueprint(tmp_path):
    """``--sandbox`` on a duckdb-engine Blueprint dry-runs successfully
    through the DuckDB ``ExecutorProtocol`` session, and the Egress module
    is skipped — no output file gets written."""
    in_path = tmp_path / "in.csv"
    in_path.write_text("a,b\n1,2\n3,4\n")
    out_path = tmp_path / "out.csv"
    bp_path = tmp_path / "bp.yml"
    bp_path.write_text(_BP.format(in_path=in_path, out_path=out_path), encoding="utf-8")
    cfg_path = tmp_path / "aqueduct.yml"
    cfg_path.write_text(_DUCKDB_CFG, encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(bp_path), "--config", str(cfg_path), "--sandbox"])

    assert result.exit_code == 0, f"output={result.output!r} exc={result.exception!r}"
    assert "sandbox dry-run" in result.output
    assert "sandbox run succeeded" in result.output
    assert "skipped Egress" in result.output
    # The sandbox never writes — the Egress target must not exist.
    assert not out_path.exists()


@pytest.mark.duckdb
def test_sandbox_dry_run_polyglot_still_refused_on_duckdb(tmp_path):
    """A polyglot Blueprint's ``--sandbox`` refusal is engine-independent —
    still refused loudly (CONFIG_ERROR) regardless of which engines the
    islands resolve to."""
    in_path = tmp_path / "in.csv"
    in_path.write_text("a,b\n1,2\n3,4\n")
    out_path = tmp_path / "out.csv"
    bp_path = tmp_path / "bp.yml"
    bp_path.write_text(
        f"""\
aqueduct: '1.0'
id: sandbox_polyglot_bp
name: Sandbox Polyglot BP
modules:
  - id: src
    type: Ingress
    label: Src
    config: {{format: csv, path: {in_path}}}
  - id: sink
    type: Egress
    label: Sink
    engine: duckdb
    config: {{format: csv, path: {out_path}, mode: overwrite}}
edges:
  - from: src
    to: sink
""",
        encoding="utf-8",
    )
    cfg_path = tmp_path / "aqueduct.yml"
    cfg_path.write_text('aqueduct_config: "1.0"\n', encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(bp_path), "--config", str(cfg_path), "--sandbox"])
    assert result.exit_code != 0
    assert "does not support a polyglot Blueprint" in result.output


@pytest.mark.spark
def test_sandbox_dry_run_still_succeeds_on_spark_blueprint(spark, tmp_path):
    """The Spark path is unchanged by the engine-agnostic rewrite: a
    single-engine Spark Blueprint still dry-runs successfully through
    ``--sandbox``, now built via ``ExecutorProtocol.session_factory()``
    instead of a direct ``make_spark_session()`` call."""
    in_path = tmp_path / "in.parquet"
    out_path = tmp_path / "out"
    spark.range(5).withColumnRenamed("id", "n").write.parquet(str(in_path))

    bp_path = tmp_path / "bp.yml"
    bp_path.write_text(
        f"""\
aqueduct: '1.0'
id: sandbox_spark_bp
name: Sandbox Spark BP
modules:
  - id: src
    type: Ingress
    label: Src
    config: {{format: parquet, path: {in_path}}}
  - id: sink
    type: Egress
    label: Sink
    config: {{format: parquet, path: {out_path}, mode: overwrite}}
edges:
  - from: src
    to: sink
""",
        encoding="utf-8",
    )
    cfg_path = tmp_path / "aqueduct.yml"
    cfg_path.write_text('aqueduct_config: "1.0"\n', encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(cli, ["run", str(bp_path), "--config", str(cfg_path), "--sandbox"])

    assert result.exit_code == 0, f"output={result.output!r} exc={result.exception!r}"
    assert "sandbox dry-run" in result.output
    assert "sandbox run succeeded" in result.output
    # The sandbox never writes — the Egress target must not exist.
    assert not out_path.exists()
