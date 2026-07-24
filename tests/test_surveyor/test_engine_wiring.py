"""Regression tests for Phase 78's engine-aware error extraction (Surveyor.record()).

Before this fix, `Surveyor.record()` called the Spark/Py4J-hardcoded
`_extract_structured_error` directly, ignoring `ExecutorProtocol.extract_error`
entirely. A DuckDB run's structured root-cause fields (error_class,
root_exception, suggested_columns, object_name) silently came from Spark's
extractor (which cannot recognise `duckdb.Error` subclasses) instead of
`aqueduct.executor.duckdb_.error_extraction.extract_duckdb_error`.

These tests exercise the real `Surveyor(...).record(...)` path end-to-end with
a real `duckdb.Error` and a real (best-effort-mocked) Spark path, and the
required-`engine`/signature-hash guarantees layered on top of it.
"""

from __future__ import annotations

import duckdb
import pytest

pytestmark = [pytest.mark.unit]

from aqueduct.compiler.models import Manifest
from aqueduct.executor.models import ExecutionResult, ExecutionStatus, ModuleResult
from aqueduct.surveyor.surveyor import Surveyor


@pytest.fixture
def manifest():
    return Manifest(
        blueprint_id="engine-wiring-test",
        name="n",
        modules=(),
        edges=(),
        context={},
        spark_config={},
    )


def _failing_result(exc: BaseException) -> ExecutionResult:
    mr = ModuleResult(module_id="m1", status=ExecutionStatus.ERROR, error=str(exc))
    return ExecutionResult(
        run_id="run1", blueprint_id="engine-wiring-test",
        status=ExecutionStatus.ERROR, module_results=(mr,),
    )


def test_surveyor_requires_engine_typeerror(manifest, tmp_path):
    """Surveyor(...) without `engine` is a TypeError — the required-param guarantee."""
    with pytest.raises(TypeError):
        Surveyor(manifest, store_dir=tmp_path)  # type: ignore[call-arg]


def test_duckdb_failure_routes_through_duckdb_extractor(manifest, tmp_path):
    """The regression test for the actual bug.

    A DuckDB exception routed through Surveyor.record() must produce a
    FailureContext whose error_class/root_exception/suggested_columns come
    from DuckDB's own extractor (extract_duckdb_error), NOT Spark's
    Py4J-hardcoded one. DuckDB's "Candidate bindings" suggestion format is
    NOT something Spark's backtick-based `_parse_suggested_columns` can
    parse — if this regressed to calling Spark's extractor, suggested_columns
    would come back empty instead of ("a",).
    """
    surveyor = Surveyor(manifest, store_dir=tmp_path, engine="duckdb")
    surveyor.start("run1")

    try:
        duckdb.sql('SELECT nonexistent_col FROM (SELECT 1 AS a)')
        pytest.fail("expected duckdb.Error")
    except duckdb.Error as exc:
        live_exc: BaseException = exc

    ctx = surveyor.record(_failing_result(live_exc), exc=live_exc)

    assert ctx is not None
    assert ctx.engine == "duckdb"
    assert ctx.error_class == "BinderException"
    assert ctx.root_exception is not None
    assert ctx.root_exception["type"] == "BinderException"
    # DuckDB-specific "Candidate bindings" parsing — Spark's extractor has no
    # code path that produces this for a generic non-PySpark exception.
    assert ctx.suggested_columns == ("a",)

    # Persisted row round-trips the same engine + structured fields.
    with surveyor._observability.connect() as cur:
        row = cur.execute(
            "SELECT engine, error_class, suggested_columns FROM failure_contexts WHERE run_id = ?",
            ["run1"],
        ).fetchone()
    assert row[0] == "duckdb"
    assert row[1] == "BinderException"


def test_spark_failure_extraction_unchanged(manifest, tmp_path):
    """Spark's path still produces what it produced before this refactor.

    Without a live PySparkException/Py4JJavaError available (pyspark may not
    be installed), both the pre-refactor call
    (`aqueduct.surveyor.error_extraction._extract_structured_error`) and the
    post-refactor call (`Surveyor.record()` via
    `get_protocol("spark").extract_error`) fall through to the same Python
    cause-chain fallback — this test pins that the two paths agree, i.e. the
    Phase 78 protocol indirection is a pure refactor for Spark, not a
    behaviour change.
    """
    from aqueduct.surveyor.error_extraction import _extract_structured_error

    surveyor = Surveyor(manifest, store_dir=tmp_path, engine="spark")
    surveyor.start("run1")

    live_exc = ValueError("boom: something went wrong")
    direct = _extract_structured_error(live_exc)

    ctx = surveyor.record(_failing_result(live_exc), exc=live_exc)

    assert ctx is not None
    assert ctx.engine == "spark"
    assert ctx.error_class == direct["error_class"]
    assert ctx.root_exception == direct["root_exception"]
    assert ctx.sql_state == direct["sql_state"]
    assert ctx.object_name == direct["object_name"]


def test_two_engines_same_error_text_different_signatures():
    """Two engines producing the same error text yield DIFFERENT signatures.

    Closes the exact gap Phase 78 targets: a shared-core failure (e.g. a
    plain FileNotFoundError) must not hash identically across engines, or a
    patch cached from a Spark run could be replayed into a DuckDB run.
    """
    from aqueduct.agent.signature import from_failure_context
    from aqueduct.surveyor.models import FailureContext

    common_kwargs = dict(
        run_id="r1", blueprint_id="b1", failed_module="m1",
        error_message="No such file or directory: '/data/in.csv'",
        stack_trace="", manifest_json="{}",
        started_at="2024-01-01", finished_at="2024-01-01",
    )
    ctx_spark = FailureContext(engine="spark", **common_kwargs)
    ctx_duckdb = FailureContext(engine="duckdb", **common_kwargs)

    exact_spark, coarse_spark = from_failure_context(ctx_spark)
    exact_duckdb, coarse_duckdb = from_failure_context(ctx_duckdb)

    assert exact_spark.hash != exact_duckdb.hash
    assert coarse_spark.hash != coarse_duckdb.hash
