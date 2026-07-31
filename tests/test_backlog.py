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

from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]


@pytest.mark.todo(
    "an unparseable SQL-bearing construct on one island forces "
    "attribute_udfs_to_islands to conservatively gate EVERY UDF against "
    "that island — verify this never over-rejects a genuinely valid "
    "polyglot Blueprint, or document it as a permanent trade-off"
)
def test_unparseable_dialect_construct_does_not_over_reject_a_valid_polyglot_blueprint():
    """
    intended: tests/test_compiler/test_udf_attribution.py

    context: `aqueduct.compiler.udf_attribution.attribute_udfs_to_islands`
        is deliberately fail-closed — a SQL-bearing construct sqlglot's
        "spark" dialect cannot parse marks its WHOLE containing island
        "uncertain" for EVERY UDF the Blueprint declares, not just ones
        plausibly related to that construct (see the module docstring).
        That is the right default when nothing can be seen inside the
        unparseable text, but it is also the residual false-positive
        surface this stub tracks: a Blueprint with one UDF used only on a
        Spark island, PLUS an unrelated Channel on a DIFFERENT island
        whose SQL sqlglot's spark dialect genuinely cannot parse (a very
        new Spark SQL syntax addition, a dialect-specific function
        sqlglot does not model, or deliberately dynamic SQL text), would
        have that unrelated island's engine gated against the UDF too —
        and could raise a CompileError even though the UDF was never
        reachable from it. Write a case that reproduces this (a
        Blueprint-legal but sqlglot-unparseable Spark SQL fragment on one
        island, a Python UDF used only on a different, unrelated island)
        and decide the fix: extend the scan surface, special-case a
        known-safe unparseable shape, or accept and document the
        over-rejection as a permanent, deliberate trade-off (in which case
        update `docs/specs.md` §10.9 to say so explicitly rather than
        leaving it untested and undocumented).
    """

