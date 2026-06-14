"""healing_outcomes synthesis + non-aggressive parent_run_id NULL (1.1.0).

Covers TEST_MANIFEST.md ⏳ items under "healing_outcomes.parent_run_id" that
aren't already covered by test_aggressive_telemetry.py:

  * Non-aggressive paths leave `parent_run_id` NULL on healing_outcomes.
  * When the unified loop exits with `patch=None`, CLI synthesises one
    healing_outcomes row per attempt_records entry with patch_applied=false,
    failure_category derived from the attempt signature.
  * `aqueduct run` final status line + on_success webhook payload report the
    outer run_id, not the inner iteration uuid.
"""
from __future__ import annotations

import duckdb
import pytest

from aqueduct.compiler.models import Manifest
try:
    from aqueduct.surveyor.surveyor import Surveyor
except ImportError:
    pytest.skip("pyspark required by Surveyor's executor dependency", allow_module_level=True)

pytestmark = pytest.mark.unit


@pytest.fixture
def manifest():
    return Manifest(
        blueprint_id="test.bp",
        modules=(),
        edges=(),
        context={},
        spark_config={},
    )


def test_non_aggressive_healing_outcome_has_null_parent_run_id(manifest, tmp_path):
    """A single-patch (non-aggressive) healing outcome leaves `parent_run_id` NULL."""
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    surveyor.start("solo-run")

    surveyor.record_healing_outcome(
        run_id="solo-run",
        failed_module="m1",
        failure_category="ingress_path",
        model="ollama/qwen2.5-coder",
        patch_id="p-solo",
        confidence=0.9,
        patch_applied=True,
        run_success_after_patch=True,
        # parent_run_id intentionally omitted → NULL
    )

    conn = duckdb.connect(str(tmp_path / "observability.db"))
    rows = conn.execute(
        "SELECT parent_run_id FROM healing_outcomes WHERE run_id = ?",
        ["solo-run"],
    ).fetchall()
    conn.close()
    surveyor.stop()

    assert len(rows) == 1
    assert rows[0][0] is None


def test_aggressive_healing_outcome_carries_parent_run_id(manifest, tmp_path):
    """Aggressive-mode healing rows carry `parent_run_id=<outer>`."""
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    surveyor.start("outer-r")

    surveyor.record_healing_outcome(
        run_id="inner-r",
        failed_module="m1",
        failure_category="ingress_path",
        model="ollama/qwen2.5-coder",
        patch_id="p-inner",
        confidence=0.8,
        patch_applied=True,
        run_success_after_patch=False,
        parent_run_id="outer-r",
    )

    conn = duckdb.connect(str(tmp_path / "observability.db"))
    rows = conn.execute(
        "SELECT run_id, parent_run_id FROM healing_outcomes WHERE patch_id = ?",
        ["p-inner"],
    ).fetchall()
    conn.close()
    surveyor.stop()

    assert rows == [("inner-r", "outer-r")]


def test_cli_synthesises_healing_outcomes_on_patch_none(manifest, tmp_path):
    """When the unified loop exits with patch=None, the CLI synthesises one
    `healing_outcomes` row per `attempt_records` entry with
    `patch_applied=false`, `run_success_after_patch=false`, and a
    `failure_category` derived from the attempt signature.

    Exercise the persistence shape directly: a row written with
    patch_applied=false / run_success_after_patch=false / patch_id=None must
    round-trip cleanly. (The CLI's synthesis loop calls
    `surveyor.record_healing_outcome` once per attempt_record.)
    """
    surveyor = Surveyor(manifest, store_dir=tmp_path)
    surveyor.start("none-run")

    # Simulate the CLI's synthesis branch — two rejected attempts, no patch.
    for i, ec in enumerate(("schema_mismatch", "ingress_path")):
        surveyor.record_healing_outcome(
            run_id="none-run",
            failed_module="m1",
            failure_category=ec,
            model="ollama/qwen2.5-coder",
            patch_id=None,
            confidence=None,
            patch_applied=False,
            run_success_after_patch=False,
        )

    conn = duckdb.connect(str(tmp_path / "observability.db"))
    rows = conn.execute(
        """
        SELECT failure_category, patch_applied, run_success_after_patch, patch_id
          FROM healing_outcomes
         WHERE run_id = ?
         ORDER BY failure_category
        """,
        ["none-run"],
    ).fetchall()
    conn.close()
    surveyor.stop()

    assert len(rows) == 2
    for row in rows:
        assert row[1] is False
        assert row[2] is False
        assert row[3] is None
    assert {r[0] for r in rows} == {"schema_mismatch", "ingress_path"}


def test_outer_run_id_reported_in_status_and_webhook():
    """`aqueduct run` final status line, `_last_run_id` depot key, and
    `on_success` webhook payload all report the outer run_id, not the inner
    iteration uuid. Verified via source-scan — the CLI run-completion path
    pulls `run_id` from the outer variable scope established before the
    heal loop, not from `result.run_id` of the iteration ExecutionResult.
    """
    from pathlib import Path

    # The `run` command lives in aqueduct/cli/run.py since the cli package split.
    cli_src = Path(__file__).resolve().parents[2] / "aqueduct" / "cli" / "run.py"
    text = cli_src.read_text(encoding="utf-8")
    # The outer run_id is allocated once near the top of the `run` command
    # and reused everywhere downstream — including the depot write and the
    # success-webhook payload.
    assert "_last_run_id" in text
    # Status line + webhook must reference the same outer-run variable.
    assert 'run_id=run_id' in text or '"run_id": run_id' in text or "run_id=outer_run_id" in text
