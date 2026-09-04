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
    AUTO_APPLY_PERMITTING_RESOLVABILITY_STATUSES,
    AUTO_APPLY_PERMITTING_SANDBOX_STATUSES,
    GATE_STATUSES,
    PREVIEW_NON_BLOCKING_SANDBOX_STATUSES,
    GateStatus,
    resolvability_gate_permits_auto_apply,
    sandbox_gate_blocks_preview,
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


def test_a_none_result_blocks():
    """`None` is fail-CLOSED, not fail-open: a caller that forgot to run the
    gate, or a code path that passes `None` by accident, must not silently
    auto-apply a patch nothing ever replayed. A caller that legitimately
    owes no fresh replay (e.g. a heal-cache replay short-circuit that ran
    the gates one step earlier) must pass an explicit
    `GateStatus.NOT_APPLICABLE` result instead of `None`."""
    assert not sandbox_gate_permits_auto_apply(None)


def test_not_requested_does_not_permit():
    """`NOT_REQUESTED` means the gate was never asked to run at all — a
    caller-level fact, not proof the patch is safe. It must not permit
    auto-apply any more than `UNAVAILABLE` does."""
    assert not sandbox_gate_permits_auto_apply(_result(GateStatus.NOT_REQUESTED))
    assert GateStatus.NOT_REQUESTED in GATE_STATUSES
    assert GateStatus.NOT_REQUESTED not in AUTO_APPLY_PERMITTING_SANDBOX_STATUSES


def test_explicit_not_applicable_replaces_none_for_legitimate_skips():
    """The one legitimate case that used to pass `None` (gates already ran
    on this exact candidate one step earlier) now passes an explicit
    `NOT_APPLICABLE` result and still permits auto-apply."""
    assert sandbox_gate_permits_auto_apply(_result(GateStatus.NOT_APPLICABLE))


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


def test_not_requested_blocks_auto_apply_but_not_preview():
    """The two predicates answer different questions and must not be fused.

    Auto-apply asks "may a machine apply this with nobody watching", where
    "nothing replayed it" is a reason to stop. Preview asks "did a gate that
    actually RAN object", and a gate never asked to run objected to nothing.
    Fusing them would make `aqueduct patch preview` exit non-zero for every
    patch previewed without `--sandbox` — its documented default invocation —
    which is defaulting `--sandbox` on by another name.
    """
    not_requested = _result(GateStatus.NOT_REQUESTED)
    assert not sandbox_gate_permits_auto_apply(not_requested)
    assert not sandbox_gate_blocks_preview(not_requested)


def test_preview_still_blocks_on_a_gate_that_ran_and_objected():
    """`NOT_REQUESTED` being non-blocking must not soften the statuses that
    represent a gate which ran and could not clear the patch."""
    assert sandbox_gate_blocks_preview(_result(GateStatus.FAIL))
    assert sandbox_gate_blocks_preview(_result(GateStatus.UNAVAILABLE))
    assert not sandbox_gate_blocks_preview(_result(GateStatus.PASS))
    assert not sandbox_gate_blocks_preview(_result(GateStatus.NOT_APPLICABLE))


def test_preview_blocks_on_none_and_on_an_unknown_status():
    """Same fail-closed direction as its sibling: no result object at all is
    a bug rather than a verdict, and a status added later blocks until
    someone deliberately lists it."""
    assert sandbox_gate_blocks_preview(None)
    assert sandbox_gate_blocks_preview(_result("some_future_status"))


def test_the_preview_non_blocking_set_is_pinned():
    """Pinned as a set for the same reason as the auto-apply set: widening it
    is the one edit that could let a genuinely failing gate stop failing the
    command, so it must be impossible to do without editing this line."""
    assert PREVIEW_NON_BLOCKING_SANDBOX_STATUSES == frozenset(
        {GateStatus.PASS, GateStatus.NOT_APPLICABLE, GateStatus.NOT_REQUESTED}
    )


# ── Phase 88: Gate 4 (resolvability) ────────────────────────────────────────


def test_resolvability_none_does_not_permit():
    assert not resolvability_gate_permits_auto_apply(None)


@pytest.mark.parametrize(
    "status,permits",
    [
        (GateStatus.PASS, True),
        (GateStatus.NOT_APPLICABLE, True),
        (GateStatus.WARN, False),
        (GateStatus.FAIL, False),
        (GateStatus.UNAVAILABLE, False),
    ],
)
def test_resolvability_permits_by_status(status, permits):
    assert resolvability_gate_permits_auto_apply(_result(status)) is permits


def test_resolvability_permitting_set_is_pinned():
    assert AUTO_APPLY_PERMITTING_RESOLVABILITY_STATUSES == frozenset(
        {GateStatus.PASS, GateStatus.NOT_APPLICABLE}
    )
