"""run_records per-iteration rows + parent_run_id linkage (1.1.0).

Covers TEST_MANIFEST.md ⏳ item:
  "Aggressive heal with 3 iterations produces 3 `run_records` rows. Iteration
   0 row carries `parent_run_id=NULL`, iterations 1+ carry the outer run_id."
"""
from __future__ import annotations

import duckdb
import pytest

from aqueduct.compiler.models import Manifest
try:
    from aqueduct.executor.models import ExecutionResult, ModuleResult
except ImportError:
    pytest.skip("pyspark required", allow_module_level=True)
from aqueduct.surveyor.surveyor import Surveyor

pytestmark = [pytest.mark.spark, pytest.mark.integration]


def test_three_iterations_produce_three_rows_linked_by_parent_run_id(tmp_path):
    manifest = Manifest(
        blueprint_id="agg.bp",
        modules=(),
        edges=(),
        context={},
        spark_config={},
    )

    surveyor = Surveyor(manifest, store_dir=tmp_path, engine="spark")
    outer = "outer-run"
    surveyor.start(outer)

    # Iteration 0 — reuses outer run_id; parent_run_id stays NULL (top-level).
    surveyor.record(
        ExecutionResult(
            blueprint_id="agg.bp",
            run_id=outer,
            status="error",
            module_results=(ModuleResult(module_id="m1", status="error", error="boom"),),
        )
    )

    # Iterations 1 and 2 — mint fresh inner run_ids; parent_run_id=outer.
    for inner in ("inner-1", "inner-2"):
        surveyor.register_iteration(run_id=inner, parent_run_id=outer)
        surveyor.record(
            ExecutionResult(
                blueprint_id="agg.bp",
                run_id=inner,
                status="error",
                module_results=(ModuleResult(module_id="m1", status="error", error="boom"),),
            )
        )

    conn = duckdb.connect(str(tmp_path / "observability.db"))
    rows = conn.execute(
        "SELECT run_id, parent_run_id FROM run_records "
        "WHERE COALESCE(parent_run_id, run_id) = ? "
        "ORDER BY run_id",
        [outer],
    ).fetchall()
    conn.close()
    surveyor.stop()

    # 3 rows total — one per iteration.
    assert len(rows) == 3
    by_run = dict(rows)
    assert by_run[outer] is None
    assert by_run["inner-1"] == outer
    assert by_run["inner-2"] == outer
