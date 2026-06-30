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


@pytest.mark.todo("aqueduct run upfront agent-unreachable warning gates on agent.approval, not on the (always-defaulted) agent.model: a blueprint with NO agent block (approval=disabled) + unreachable agent prints NO warning; a blueprint with agent.approval=human/auto + no API key/base_url DOES print 'self-healing is enabled ... not reachable'")
def test_run_upfront_unreachable_warning_gated_on_approval():
    # intended: tests/test_cli/test_cli_agent_warning.py  (mock executor like test_cli_doctor_hints)
    # context: regression for the false-positive where the warning fired on every
    # run because agent.model defaults to DEFAULT_LLM_MODEL (so `model is not None`
    # was always true). Fixed to gate on manifest.agent.approval_mode != "disabled".
    ...


@pytest.mark.todo("doctor --preflight verifies cloud object existence: with a live Spark session + cloud creds, an s3a:// Ingress pointing at a missing object → CheckResult 'fail'; an existing object → 'ok'; Egress → 'ok' (parent prefix resolves). Without --preflight the same cloud URI → 'skip'. _cloud_uri_check reuses the preflight session's Hadoop FileSystem.")
def test_doctor_preflight_cloud_object_existence():
    # intended: tests/test_cli/test_cli_doctor_preflight.py  (mark spark + cloud)
    # context: _cloud_uri_check uses spark._jvm Hadoop FileSystem with the run's
    # s3a/gcs/adls creds — needs a real session + a reachable bucket (minio in CI).
    # The non-preflight skip branch is exercisable without Spark.
    ...
