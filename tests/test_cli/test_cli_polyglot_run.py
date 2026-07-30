"""End-to-end ``aqueduct run`` against a real polyglot Blueprint (Phase 81/82
batch A — wiring ``run_polyglot()`` into the CLI healing loop and rendering
it). Covers the routing decision (a >1-island Manifest reaches
``run_polyglot()``, not the single-engine ``execute()`` path) and every
rendering surface the task asked for: the run header naming every engine
involved, per-module engine tags in the transcript, first-class Handoff step
rendering (bytes transferred + duration), and ``report --json``'s new
``engines``/per-module ``engine`` fields.

Needs a real SparkSession (``local[1]``, Java 17) and DuckDB's bare
``:memory:`` connection — both engines auto-register via
``aqueduct.executor.capabilities.load_engines()``.
"""

from __future__ import annotations

import json

import pytest

pytestmark = [pytest.mark.spark, pytest.mark.integration]

from click.testing import CliRunner

from aqueduct.cli import cli


def test_polyglot_run_succeeds_and_renders_engines_and_handoff(spark, tmp_path):
    in_path = tmp_path / "in.parquet"
    out_path = tmp_path / "out"
    spark.range(5).withColumnRenamed("id", "n").write.parquet(str(in_path))

    bp_path = tmp_path / "bp.yml"
    bp_path.write_text(f"""
aqueduct: '1.0'
id: polyglot_cli_test
name: Polyglot CLI Test
modules:
  - id: in
    type: Ingress
    label: In
    config: {{format: parquet, path: {in_path}}}
  - id: out
    type: Egress
    label: Out
    engine: duckdb
    config: {{format: parquet, path: {out_path}, mode: overwrite}}
edges:
  - from: in
    to: out
""")
    config_path = tmp_path / "aqueduct.yml"
    config_path.write_text(f"""
stores:
  observability: {{path: {tmp_path / "obs"}}}
""")

    runner = CliRunner()
    # Deliberately NOT passing --store-dir here: `_render_module_summary`'s
    # in-run metrics lookup (`open_obs_read(cfg, store_dir=store_dir, ...)`)
    # uses the RAW --store-dir string, which `resolve_duckdb_obs_path`
    # treats as a literal final directory (no per-blueprint nesting) — a
    # PRE-EXISTING mismatch against where `run` actually writes the DB when
    # --store-dir is given explicitly (nested under the blueprint id
    # regardless), reproducible on a vanilla single-engine Blueprint and
    # unrelated to cross-engine routing/rendering. Routing through
    # `stores.observability.path` instead (as this test does) hits the path
    # that already resolves consistently, so it actually exercises the new
    # Handoff bytes/duration rendering rather than this unrelated gap.
    result = runner.invoke(cli, ["run", str(bp_path), "--config", str(config_path)])
    assert result.exit_code == 0, result.output

    # ── Header names every engine involved, not the single nominal default ──
    header_line = next(l for l in result.output.splitlines() if "polyglot_cli_test" in l)
    assert "spark" in header_line
    assert "duckdb" in header_line
    assert "islands" in header_line

    # ── Per-module engine tag in the transcript ──────────────────────────────
    assert "duckdb" in result.output  # 'out' module's own engine tag

    # ── Handoff step rendered first-class (distinct marker + transport info) ─
    assert "⇄" in result.output
    assert "in → out" in result.output
    assert "written" in result.output

    # Real data actually landed via the handoff + duckdb egress. DuckDB's
    # `COPY ... TO` writes a single file at exactly the given path (no
    # per-partition directory the way Spark's writer does).
    import duckdb
    written = duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{out_path}')").fetchone()[0]
    assert written == 5


def test_polyglot_run_report_json_has_engine_fields(spark, tmp_path):
    in_path = tmp_path / "in.parquet"
    out_path = tmp_path / "out"
    spark.range(3).withColumnRenamed("id", "n").write.parquet(str(in_path))

    bp_path = tmp_path / "bp.yml"
    bp_path.write_text(f"""
aqueduct: '1.0'
id: polyglot_report_test
name: Polyglot Report Test
modules:
  - id: in
    type: Ingress
    label: In
    config: {{format: parquet, path: {in_path}}}
  - id: out
    type: Egress
    label: Out
    engine: duckdb
    config: {{format: parquet, path: {out_path}, mode: overwrite}}
edges:
  - from: in
    to: out
""")
    config_path = tmp_path / "aqueduct.yml"
    store_dir = tmp_path / "store"
    config_path.write_text(f"""
stores:
  observability: {{path: {tmp_path / "obs"}}}
""")

    runner = CliRunner()
    run_id = "polyglot-report-run"
    result = runner.invoke(
        cli,
        ["run", str(bp_path), "--config", str(config_path), "--store-dir", str(store_dir), "--run-id", run_id],
    )
    assert result.exit_code == 0, result.output

    # `aqueduct run --store-dir X` treats X as a ROUTING base and nests
    # `X/<blueprint_id>/observability.db`; `report --store-dir X` (unlike
    # `run`) opens `X/observability.db` literally, no nesting — point it at
    # the nested directory `run` actually wrote to.
    report_result = runner.invoke(
        cli,
        ["report", run_id, "--config", str(config_path),
         "--store-dir", str(store_dir / "polyglot_report_test"), "--format", "json"],
    )
    assert report_result.exit_code == 0, report_result.output
    payload = json.loads(report_result.output)

    assert set(payload["engines"]) == {"spark", "duckdb"}
    by_id = {mr["module_id"]: mr for mr in payload["module_results"]}
    assert by_id["in"]["engine"] == "spark"
    assert by_id["out"]["engine"] == "duckdb"
