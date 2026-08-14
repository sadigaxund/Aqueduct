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


@pytest.mark.todo(
    "aqueduct.compiler.runtime._java_to_strftime only maps yyyy/MM/dd/yy/"
    "HH/mm/ss — Java SimpleDateFormat's month-name (MMM), day-name (EEE), "
    "and millisecond (SSS) patterns still pass through as literal text"
)
def test_java_to_strftime_month_day_name_and_millis_patterns():
    """
    intended: tests/test_compiler/test_runtime.py

    context: `_java_to_strftime` (aqueduct/compiler/runtime.py) is a naive
        ordered string-replace, not a real SimpleDateFormat tokenizer.
        Audit triage (2026-08) confirmed and fixed the HH/mm/ss gap (a
        `date`, having no time component, rendered "00:00:00" instead of
        the previous literal-text passthrough). MMM (month abbreviation,
        e.g. "Aug"), EEE (day-of-week abbreviation, e.g. "Sun"), and SSS
        (milliseconds — meaningless on a bare `date` object, which is all
        every @aq.date.* function here operates on) were deliberately left
        out of this pass: MMM/EEE need a locale-aware name table (Python's
        %b/%a exist but Java's default locale for SimpleDateFormat may not
        match the process locale %b/%a resolve against — verify before
        wiring), and SSS has no meaningful value on a date-only type at
        all (worth a compile-time warning or CompileError rather than a
        silent "000"). Write the case (`@aq.date.today(format='MMM yyyy')`
        etc.) and decide: extend the map with %b/%a (verifying locale
        parity), or reject those three patterns at compile time with a
        clear "not supported, requires a time-of-day value" CompileError
        rather than leaving them as silent literal passthrough.
    """
