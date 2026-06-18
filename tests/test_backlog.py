"""The test backlog — a single, low-friction landing zone for planned tests.

This is the pytest-native replacement for the old TEST_MANIFEST.md ⏳ list. It
keeps the manifest's one real strength — *one place to append* — without its
weaknesses (prose drift, manual ✅, no enforcement):

* Recording a gap costs one ``@pytest.mark.todo`` stub here. It auto-skips, and
  ``pytest --collect-only -m todo`` (or ``pytest -rs``) prints the whole backlog
  with reasons.
* Each stub carries an ``intended:`` line — where the real test should live — and
  a ``context:`` note. The agent (or human) who implements it writes the body
  and **moves it to that path**, deleting it from here.
* A known live bug uses ``@pytest.mark.xfail(strict=True, reason=...)`` instead
  (it can live here or in place); ``xfail_strict`` flips it to a failure the
  moment the bug is fixed, forcing the marker's removal.

Do not let this file accumulate *implemented* tests — a stub leaves the instant
it has a body. See AGENTS.md → "Testing" and CONTRIBUTING.md → "Test backlog".
"""

from __future__ import annotations

import pytest


@pytest.mark.todo("iceberg + hudi read/write + maintenance run on a real SparkSession with the format jars: write a table, rewrite_data_files/expire_snapshots (iceberg) and run_compaction/run_clean (hudi) execute, maintenance_metrics rows recorded")
def test_iceberg_hudi_roundtrip_and_maintenance():
    # intended: tests/test_executor/test_executor_lakehouse.py  (mark spark integration)
    # context: Phase 59 — build_maintenance_ops + doctor check are unit-tested;
    # this needs spark.jars.packages for iceberg/hudi, so it lives behind the
    # spark marker and pulls the bundles at session build.
    ...
