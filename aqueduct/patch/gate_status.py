"""The gate-status vocabulary, and the one place a status decides auto-apply.

Every validation gate in `aqueduct/patch/` reports a status string, and those
strings are persisted (`patch_simulation.status`), rendered (`aqueduct patch
preview`, the run/heal output), counted (`stores/queries.py::
gate_rejection_rates`) and — for Gate 3 — acted on. Before this module the
vocabulary lived as bare literals in four modules with a comment in each
saying it "follows" another module's; a prose cross-reference is not a single
source of truth, and the vocabulary drifted into one word doing two jobs.

**The partition that defines the two non-verdict members.** The question is
whether a check was OWED:

- `NOT_APPLICABLE` — no check was owed. Either the patch has no surface this
  gate looks at (a `set_engine_config` op carries no module for the lineage
  diff), or the operator declared that no check is owed here
  (`agent.sandbox_mode: off`, itself gated on `danger.allow_skip_sandbox`).
  Informational, never an alarm, never blocking.
- `UNAVAILABLE` — a check was owed and the environment prevented it. The
  target engine's dependencies are not installed, its session would not
  start, the Blueprint is polyglot and the sandbox can replay only one
  engine, plan capture does not exist on this session. **Nothing about the
  patch was verified.**

The two are opposite facts. Gate 3 used to report both as `skip`, so a patch
that was never verified rendered identically to one that needed no
verification, and auto-approval accepted both. `skip` is gone from the
vocabulary; see the CHANGELOG BREAKING entry for what that means for rows
written before the split.

`NOT_APPLICABLE` and a hypothetical `skipped` are the SAME concept under two
names, so no `skipped` member exists — `not_applicable` was already the
incumbent name in three of the four gates (lineage, engine-config delta, perf
attribution) and in the docs, and a synonym would have been a fifth word for
four facts.

**Blocking lives here, once.** `sandbox_gate_permits_auto_apply()` is the
only place Gate 3's status answers "may this patch be applied without a
human?". `cli/__init__.py::_run_patch_gates_inline` (the heal loop's
`gates_passed`) and `cli/patch.py`'s `patch preview` exit code both call it
rather than re-listing statuses; a call site that re-lists them is how the
rule drifts apart between the loop that heals and the command that reviews.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "AUTO_APPLY_PERMITTING_SANDBOX_STATUSES",
    "GATE_STATUSES",
    "PREVIEW_NON_BLOCKING_SANDBOX_STATUSES",
    "GateStatus",
    "sandbox_gate_blocks_preview",
    "sandbox_gate_permits_auto_apply",
]


class GateStatus:
    """Status values a validation gate may report.

    Plain `str` constants rather than a `StrEnum`: these values are written
    into DuckDB/Postgres rows, JSON reports and (for `PerfObservation`) a
    Blueprint's own YAML, and a `str` subclass round-trips through none of
    those three predictably.
    """

    #: Checked, nothing wrong.
    PASS = "pass"
    #: Checked, findings that a human should read. Non-blocking by default;
    #: Gate 4's `warn` blocks only under `agent.block_on_explain_regression`.
    WARN = "warn"
    #: Checked, the patch is refused.
    FAIL = "fail"
    #: No check was owed — see the module docstring. Never blocking.
    NOT_APPLICABLE = "not_applicable"
    #: A check was owed and could not be performed. Blocking for Gate 3.
    UNAVAILABLE = "unavailable"
    #: A quantity was recorded and deliberately not judged — perf
    #: attribution's only positive member (`patch/perf_attribution.py`),
    #: which ships no threshold and therefore no `pass`/`fail`. Never
    #: written to `patch_simulation`: `PerfObservation` is a record in the
    #: Blueprint's own `healed_by:` block, not a gate, and nothing branches
    #: on it.
    OBSERVED = "observed"
    #: The sandbox gate was deliberately never asked to run — a caller-level
    #: fact about THIS invocation, not a verdict about the patch. Distinct
    #: from both non-verdict members above: `NOT_APPLICABLE` means "asked,
    #: and nothing here needed checking"; `UNAVAILABLE` means "asked, and
    #: the environment could not answer"; `NOT_REQUESTED` means "never
    #: asked" (e.g. `aqueduct patch preview` invoked without `--sandbox`).
    #: Synthesized by a caller that skips the gate outright — `run_sandbox_gate`
    #: itself never returns it — so it is not currently written to
    #: `patch_simulation` (nothing calls `record_patch_simulation` for a
    #: gate it chose not to run). Never blocking in the sense of `fail`, but
    #: it does NOT permit auto-apply: a patch nothing ever replayed must not
    #: auto-apply just because replay wasn't asked for.
    NOT_REQUESTED = "not_requested"


#: Every value any gate may write. `patch_simulation.status` is unconstrained
#: at the DDL level, so this tuple is what the docs enumerate and what the
#: tests pin.
GATE_STATUSES: tuple[str, ...] = (
    GateStatus.PASS,
    GateStatus.WARN,
    GateStatus.FAIL,
    GateStatus.NOT_APPLICABLE,
    GateStatus.UNAVAILABLE,
    GateStatus.OBSERVED,
    GateStatus.NOT_REQUESTED,
)


#: Gate 3 statuses under which a patch may be applied with no human in the
#: loop. Written as the small closed set of statuses that PERMIT rather than
#: the growing set that blocks (AGENTS.md "classify by what you EXCLUDE"):
#: a status added later falls into "a human decides", which is the loud,
#: recoverable direction.
AUTO_APPLY_PERMITTING_SANDBOX_STATUSES: frozenset[str] = frozenset(
    {GateStatus.PASS, GateStatus.NOT_APPLICABLE}
)


def sandbox_gate_permits_auto_apply(sandbox_result: Any) -> bool:
    """Return whether Gate 3's result permits applying without a human.

    `None` does NOT permit — this is fail-CLOSED. A caller that forgets to
    run the gate, or a code path that passes `None` by accident, must not
    silently auto-apply a patch nothing ever replayed; `None` now reads as
    "no verdict", which blocks like any other status outside the permitting
    set. A caller that legitimately owes no fresh replay (the heal-cache
    replay short-circuit, where the gates already ran on this exact
    candidate one step earlier) must say so explicitly by passing a result
    object with `status=GateStatus.NOT_APPLICABLE` — never `None`.

    `NOT_REQUESTED` does NOT permit either: the gate being skipped is a
    caller-level fact, not proof the patch is safe to apply unattended.

    An `UNAVAILABLE` result does NOT permit. That is the availability cost of
    the split and it is deliberate: a machine without the target engine
    installed now stalls the heal for a human instead of auto-applying a
    patch nothing ever replayed.
    """
    if sandbox_result is None:
        return False
    return getattr(sandbox_result, "status", None) in AUTO_APPLY_PERMITTING_SANDBOX_STATUSES


#: Gate 3 statuses that do NOT make `aqueduct patch preview` exit non-zero.
#: Same closed-set-of-permitting-values shape as the auto-apply set above,
#: for the same reason: a status added later blocks, which is the loud
#: direction.
PREVIEW_NON_BLOCKING_SANDBOX_STATUSES: frozenset[str] = frozenset(
    {GateStatus.PASS, GateStatus.NOT_APPLICABLE, GateStatus.NOT_REQUESTED}
)


def sandbox_gate_blocks_preview(sandbox_result: Any) -> bool:
    """Return whether Gate 3's result should make `patch preview` exit non-zero.

    This is deliberately NOT `sandbox_gate_permits_auto_apply`, because the
    two answer different questions. Auto-apply asks "may a machine apply this
    with no human present", where "nobody replayed it" is a reason to stop.
    Preview asks "did a gate that actually RAN object to this patch", and a
    gate that was never asked to run has objected to nothing.

    The distinction is load-bearing rather than academic: running without
    `--sandbox` is `patch preview`'s documented default invocation (the
    command "always runs the guardrails gate and the lineage gate. With
    `--sandbox`, also runs the sandbox gate"). Routing preview's exit code
    through the auto-apply predicate would make that default invocation exit
    non-zero for every patch ever previewed, breaking every CI job that runs
    the command as a check — and amounting to defaulting `--sandbox` on by
    making every other invocation look like a failure, which Phase 85's F-2
    ruling explicitly refused.

    `None` blocks, matching the fail-closed direction of its sibling: no
    result object at all is a bug, not a verdict.
    """
    if sandbox_result is None:
        return True
    return getattr(sandbox_result, "status", None) not in PREVIEW_NON_BLOCKING_SANDBOX_STATUSES
