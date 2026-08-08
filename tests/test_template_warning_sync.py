"""Template <-> warning-registry sync guard.

`aqueduct/templates/default/aqueduct.yml.template` documents, in prose
comments, the suppressible compile-time / session-startup warning rule_ids
so a user who hits ``AQ-WARN [some_id] ...`` can find it in the template
they scaffolded and learn how to suppress it (``warnings.suppress: [...]``).
That list is hand-written text -- nothing forced it to stay in sync with the
two real registries it claims to describe
(``aqueduct.compiler.warnings.RULES``, ``aqueduct.executor.spark.warnings.
RULES``), and it rotted: ``custom_probe_driver_code`` and
``spillway_port_mismatch`` are both registered, active, and suppressible,
but appeared in neither template list (0 grep hits) before this guard.

This test parses the template's two labeled comment blocks and asserts they
equal the live registries exactly, in both directions (missing AND stale),
so an 8th rule landing without a template update fails the build here
instead of rotting silently again -- rather than just adding the two
missing ids as a one-off fix that rots the same way the moment a ninth rule
lands.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from aqueduct.compiler.warnings import RULES as COMPILER_RULES
from aqueduct.executor.spark.warnings import RULES as SPARK_STARTUP_RULES

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[1]
_TEMPLATE = _REPO / "aqueduct" / "templates" / "default" / "aqueduct.yml.template"

# A continuation line of a rule-id list: `#   id_one, id_two,` -- comma-
# separated lowercase identifiers only. A section header line ("# Legacy
# compile-time rules...", "# Session-startup rules (registry: ...):") always
# contains a character outside this set (a hyphen, colon, parenthesis, or
# capital letter), so it can never be mistaken for a continuation line --
# that is what stops the scan without hardcoding the next header's text.
_ID_LIST_LINE = re.compile(r"^#\s+([a-z0-9_]+(?:,\s*[a-z0-9_]+)*,?)\s*$")


def _ids_after(lines: list[str], header_substring: str) -> set[str]:
    """Collect the comma-separated identifiers from every comment line
    immediately following the line containing ``header_substring``, stopping
    at the first line that is not a bare identifier list."""
    start = next(i for i, line in enumerate(lines) if header_substring in line)
    ids: set[str] = set()
    for line in lines[start + 1 :]:
        m = _ID_LIST_LINE.match(line)
        if not m:
            break
        ids.update(part.strip() for part in m.group(1).split(",") if part.strip())
    return ids


def _template_lines() -> list[str]:
    return _TEMPLATE.read_text(encoding="utf-8").splitlines()


def test_template_documents_every_compile_time_rule_id():
    """Every rule_id in ``aqueduct.compiler.warnings.RULES`` must be
    documented in the template's 'Available compile-time rule IDs' block --
    and every id documented there must be a real, still-registered rule."""
    documented = _ids_after(_template_lines(), "Available compile-time rule IDs")
    registered = {rule_id for rule_id, _ in COMPILER_RULES}

    missing = registered - documented
    assert not missing, (
        f"{sorted(missing)} registered in aqueduct/compiler/warnings/__init__.py"
        f"::RULES but not documented in {_TEMPLATE} -- a user hitting one of "
        "these warnings cannot find it in the template to learn how to "
        "suppress it. Add it to the 'Available compile-time rule IDs' comment "
        "block."
    )
    stale = documented - registered
    assert not stale, (
        f"{sorted(stale)} documented in {_TEMPLATE} as a compile-time rule id "
        "but not present in aqueduct/compiler/warnings/__init__.py::RULES -- "
        "renamed or removed without updating the template."
    )


def test_template_documents_every_session_startup_rule_id():
    """Same guard for ``aqueduct.executor.spark.warnings.RULES`` (session-
    startup rules, e.g. ``jar_availability``)."""
    documented = _ids_after(_template_lines(), "Session-startup rules")
    registered = {rule_id for rule_id, _ in SPARK_STARTUP_RULES}

    missing = registered - documented
    assert not missing, (
        f"{sorted(missing)} registered in "
        "aqueduct/executor/spark/warnings/__init__.py::RULES but not "
        f"documented in {_TEMPLATE}'s 'Session-startup rules' block."
    )
    stale = documented - registered
    assert not stale, (
        f"{sorted(stale)} documented in {_TEMPLATE} as a session-startup rule "
        "id but not present in aqueduct/executor/spark/warnings/__init__.py::"
        "RULES."
    )


def test_parser_actually_finds_the_known_rule_ids():
    """Falsifiability floor for the parser itself: if the comment format ever
    changes shape enough that `_ids_after` returns nothing, the two tests
    above would pass vacuously (empty == empty is never true here, since
    RULES is non-empty, but guard the parser directly anyway so a shape
    change is diagnosed as a parser break, not misread as 'fully synced')."""
    documented_compile = _ids_after(_template_lines(), "Available compile-time rule IDs")
    documented_startup = _ids_after(_template_lines(), "Session-startup rules")
    assert documented_compile, "parser found zero compile-time rule ids -- template format changed?"
    assert documented_startup, "parser found zero session-startup rule ids -- template format changed?"
