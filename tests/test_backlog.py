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


@pytest.mark.todo("real multi-patch auto-mode heal with agent.regression_artifact: true "
                   "actually calls generate() at the successful-full-run point and the "
                   "resulting .aqtest.yml passes `aqueduct test` — the generation logic "
                   "itself is unit-tested in tests/test_agent/test_regression_artifact.py, "
                   "but the wiring in aqueduct/cli/run.py (real Spark run -> _write_patch_to_blueprint "
                   "-> generate()) has no end-to-end coverage")
def test_cli_heal_regression_artifact_end_to_end():
    """intended: tests/test_cli/test_cli_heal_regression_artifact.py (spark marker)

    context: needs a real local[1] Spark session, a Blueprint with a genuine
    fixable failure (e.g. wrong schema_hint), agent.regression_artifact: true,
    and approval: auto with a mocked LLM response producing a single-module
    set_module_config_key patch. Assert the emitted aqtests/*.aqtest.yml is
    written and that `aqueduct test <file>` on it passes.
    """

