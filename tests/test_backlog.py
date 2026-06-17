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

# ── Gallery — heavier e2e that needs external infra ───────────────────────────


@pytest.mark.todo("showcase 03-self-healing runs end to end: induced failure → staged patch → green re-run (scoped Spark + mocked agent)")
def test_showcase_self_healing_e2e():
    # intended: tests/test_gallery.py  (mark e2e)
    ...


@pytest.mark.todo("every aqscenario heals with a MOCKED agent through the full gate pyramid (deterministic, no live LLM)")
def test_aqscenarios_heal_with_mocked_agent():
    # intended: tests/test_gallery.py  (mark integration)
    ...


@pytest.mark.todo("sync-constants: PATHLESS_INGRESS_FORMATS, AQ_ERROR_*, PATCH_META_KEY values match their source-of-truth definitions; catch mismatch if a new format/column is added to one frozenset but not the other")
def test_sync_constants_do_not_drift():
    # intended: tests/test_sync_constants.py  (mark unit)
    # context: Phase 2 Chunk A — constants extracted in Chunk 4 of the audit can
    # independently drift if someone adds a new entry to a consuming frozenset
    # without updating the canonical definition. A single file that asserts
    # equality across all shared constants prevents lockstep-change regressions.
    ...
