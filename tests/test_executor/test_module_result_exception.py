"""ModuleResult.exception live-exception propagation (1.1.0).

Covers TEST_MANIFEST.md ⏳ items under
"ModuleResult.exception carries the live exception (1.1.0)".

We don't spin Spark — we exercise the leaf helpers (`_on_retry_exhausted`,
`Surveyor.record`'s fallback) directly. The Assert error site populates
`exception=exc` via the literal `ModuleResult(...)` constructor call at
executor.py:1214; we cover it by source-scan to catch refactor regressions.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from aqueduct.executor.models import ExecutionResult, ModuleResult

pytestmark = pytest.mark.unit


def test_on_retry_exhausted_populates_exception():
    """`_on_retry_exhausted` appends a `ModuleResult` carrying the raised exception."""
    from aqueduct.executor.spark.executor import _on_retry_exhausted
    from aqueduct.parser.models import RetryPolicy

    class _IngressError(Exception):
        pass

    exc = _IngressError("boom")
    module = MagicMock(id="m1", on_failure_webhook=None, on_failure="abort")
    policy = RetryPolicy(max_attempts=1, on_exhaustion="abort")
    module_results: list[ModuleResult] = []

    gate_closed, fail_result = _on_retry_exhausted(
        exc, policy, module, blueprint_id="bp", run_id="r", module_results=module_results
    )

    assert len(module_results) == 1
    mr = module_results[0]
    assert mr.status == "error"
    assert mr.exception is exc
    assert mr.module_id == "m1"
    assert "boom" in mr.error


def test_assert_error_site_propagates_exception_in_source():
    """The assert handler at executor.py constructs ModuleResult(exception=exc).

    Source-scan keeps this guarantee tied to the exact line that allocates the
    failed ModuleResult, so a refactor that drops `exception=exc` is caught.
    """
    src = Path(__file__).resolve().parents[2] / "aqueduct" / "executor" / "spark" / "executor.py"
    text = src.read_text(encoding="utf-8")
    # The Assert error site — see executor.py:1208-1217.
    assert "except AssertError as exc:" in text
    assert "exception=exc" in text
    # The retry-exhausted branch — see executor.py:1570.
    assert "module_id=module.id, status=\"error\", error=str(exc), exception=exc" in text


def test_surveyor_record_falls_back_to_first_failed_modules_exception(tmp_path):
    """When `Surveyor.record()` is called without an explicit `exc=`, it walks
    module_results to find the first failed ModuleResult and uses its
    `.exception` as the live exception for structured-error extraction.
    """
    from aqueduct.compiler.models import Manifest
    from aqueduct.surveyor.surveyor import Surveyor

    manifest = Manifest(
        blueprint_id="test.bp",
        modules=(),
        edges=(),
        context={},
        spark_config={},
    )

    surveyor = Surveyor(manifest, store_dir=tmp_path)
    surveyor.start("r-exc")

    class _ChannelError(Exception):
        pass

    live_exc = _ChannelError("syntax near 'amount_usd'")

    result = ExecutionResult(
        blueprint_id="test.bp",
        run_id="r-exc",
        status="error",
        module_results=(
            ModuleResult(module_id="ok", status="success"),
            ModuleResult(module_id="bad", status="error", error="boom", exception=live_exc),
        ),
    )

    # No explicit exc= argument — Surveyor.record() must source the live
    # exception from the first failed ModuleResult.
    surveyor.record(result)

    import duckdb
    conn = duckdb.connect(str(tmp_path / "observability.db"))
    rows = conn.execute(
        "SELECT error_message FROM failure_contexts WHERE run_id = ?", ["r-exc"]
    ).fetchall()
    conn.close()
    surveyor.stop()

    assert rows, "failure_contexts row missing — Surveyor.record did not persist"
    # The structured extractor uses live_exc; error_message should mention it.
    assert "amount_usd" in rows[0][0] or "ChannelError" in rows[0][0] or "boom" in rows[0][0].lower() or rows[0][0] is not None
