"""The shared gate-status vocabulary and the single auto-apply decision.

`aqueduct/patch/gate_status.py` exists because the vocabulary previously
lived as bare string literals in four modules, each with a comment claiming
to "follow" another's — a prose cross-reference, not a single source of
truth. It drifted into one word (`skip`) doing two opposite jobs: "no check
was owed" and "a check was owed and could not run". These tests pin the
partition and the one place it decides anything.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from aqueduct.patch.gate_status import (
    AUTO_APPLY_PERMITTING_SANDBOX_STATUSES,
    GATE_STATUSES,
    GateStatus,
    sandbox_gate_permits_auto_apply,
)

pytestmark = pytest.mark.unit


def _result(status: str) -> SimpleNamespace:
    return SimpleNamespace(status=status, detail="")


def test_skip_is_gone_from_the_vocabulary():
    """The whole point of the split: `skip` must not be a value any gate can
    write, because it cannot distinguish the two facts it used to cover."""
    assert "skip" not in GATE_STATUSES


def test_the_two_non_verdict_statuses_are_distinct_values():
    """`not_applicable` and `unavailable` are opposite facts. If they ever
    collapse to the same string the split has been undone."""
    assert GateStatus.NOT_APPLICABLE != GateStatus.UNAVAILABLE
    assert GateStatus.NOT_APPLICABLE in GATE_STATUSES
    assert GateStatus.UNAVAILABLE in GATE_STATUSES


def test_unavailable_blocks_auto_apply():
    """The owner's ruling, and the availability cost that comes with it: a
    replay that was owed and did not happen stops for a human."""
    assert not sandbox_gate_permits_auto_apply(_result(GateStatus.UNAVAILABLE))


def test_not_applicable_does_not_block():
    """No check was owed, so there is nothing to stall on. A patch whose ops
    have no sandbox surface, or a run under `sandbox_mode: off`, must still
    auto-apply."""
    assert sandbox_gate_permits_auto_apply(_result(GateStatus.NOT_APPLICABLE))


def test_pass_permits_and_fail_does_not():
    """Positive and negative controls: without these, the two assertions
    above would also hold for a helper that ignored its argument."""
    assert sandbox_gate_permits_auto_apply(_result(GateStatus.PASS))
    assert not sandbox_gate_permits_auto_apply(_result(GateStatus.FAIL))


def test_a_none_result_permits():
    """`None` is a caller-level fact, not a gate verdict — the heal-cache
    replay short-circuit passes it after the gates already ran on the
    candidate. Treating it as blocking would stall every cached heal."""
    assert sandbox_gate_permits_auto_apply(None)


def test_an_unknown_status_blocks_rather_than_permits():
    """The permitting set is written as the small closed set that ALLOWS,
    not the growing set that blocks (AGENTS.md: classify by exclusion), so a
    status added later without updating this rule falls into "a human
    decides" — the loud, recoverable direction."""
    assert not sandbox_gate_permits_auto_apply(_result("some_future_status"))


def test_the_permitting_set_is_exactly_pass_and_not_applicable():
    """Pinned as a set rather than probed one value at a time: a widening of
    this set is the single change that could silently let an unverified patch
    through, so it must be impossible to make without editing this line."""
    assert AUTO_APPLY_PERMITTING_SANDBOX_STATUSES == frozenset(
        {GateStatus.PASS, GateStatus.NOT_APPLICABLE}
    )


def test_observed_is_not_a_gate_verdict_but_is_in_the_vocabulary():
    """Perf attribution records a quantity and deliberately renders no
    judgement, so it has no pass/fail. It is listed because it is a status a
    consumer can encounter, but it must never permit auto-apply as if it
    were a verdict."""
    assert GateStatus.OBSERVED in GATE_STATUSES
    assert not sandbox_gate_permits_auto_apply(_result(GateStatus.OBSERVED))
