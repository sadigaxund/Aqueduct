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

# ── Phase 53 — Spark-side persistence (need a real SparkSession) ──────────────


@pytest.mark.todo("incremental Channel reads+writes its watermark via the Depot only; no watermarks/*.json file is created")
def test_watermark_depot_only_no_sidecar():
    # intended: tests/test_executor/  (mark integration + spark)
    # context: Phase 53 dropped the local sidecar; verify a full incremental run
    #          persists/reads the watermark from a configured Depot exclusively.
    ...


@pytest.mark.todo("no Depot configured → post-Egress watermark update logs a warning and persists nothing")
def test_watermark_no_depot_warns():
    # intended: tests/test_executor/  (mark integration + spark; use caplog)
    ...


@pytest.mark.todo("schema_snapshot probe writes its payload only to probe_signals; no snapshots/<run_id>/*_schema.json file")
def test_schema_snapshot_no_local_sidecar():
    # intended: tests/test_surveyor/test_probe.py  (mark spark)
    # context: Phase 53 removed the local snapshots/ sidecar.
    ...


# ── Phase 53 — patch CLI lifecycle (index status + pull) ──────────────────────


@pytest.mark.todo("aqueduct patch apply flips patch_index status pending→applied (best-effort, never raises)")
def test_patch_apply_flips_index_status():
    # intended: tests/test_cli/  (needs a blueprint fixture + per-blueprint obs store)
    ...


@pytest.mark.todo("aqueduct patch reject flips patch_index status pending→rejected")
def test_patch_reject_flips_index_status():
    # intended: tests/test_cli/
    ...


@pytest.mark.todo("aqueduct patch pull <id> --blueprint resolves the index row, reads the body from stores.blob, writes patches/pending/<id>.json")
def test_patch_pull_writes_local_body():
    # intended: tests/test_cli/
    ...


@pytest.mark.todo("aqueduct patch pull exits DATA_OR_RUNTIME when the id is unknown or the body is unreadable")
def test_patch_pull_unknown_id_exits_data_or_runtime():
    # intended: tests/test_cli/
    ...


# ── Phase 53 — object store over fsspec (need a mocked object store) ──────────


@pytest.mark.todo("FsspecBackend put/get/move/list against a moto-mocked s3 bucket")
def test_fsspec_backend_s3_roundtrip():
    # intended: tests/test_stores/test_object_store.py  (needs moto / s3 mock)
    ...


@pytest.mark.todo("PatchStore over an fsspec backend: pending→applied move preserves the body")
def test_patchstore_fsspec_move():
    # intended: tests/test_stores/test_object_store.py
    ...


# ── Gallery — heavier e2e that needs external infra ───────────────────────────


@pytest.mark.todo("showcase 03-self-healing runs end to end: induced failure → staged patch → green re-run (scoped Spark + mocked agent)")
def test_showcase_self_healing_e2e():
    # intended: tests/test_gallery.py  (mark e2e)
    ...


@pytest.mark.todo("every aqscenario heals with a MOCKED agent through the full gate pyramid (deterministic, no live LLM)")
def test_aqscenarios_heal_with_mocked_agent():
    # intended: tests/test_gallery.py  (mark integration)
    ...
