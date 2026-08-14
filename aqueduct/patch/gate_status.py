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
    "GateStatus",
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

    `None` permits: the gate did not run at all on this path (the heal-cache
    replay short-circuit passes `None` after the gates already ran on the
    candidate), which is a caller-level fact, not a gate verdict.

    An `UNAVAILABLE` result does NOT permit. That is the availability cost of
    the split and it is deliberate: a machine without the target engine
    installed now stalls the heal for a human instead of auto-applying a
    patch nothing ever replayed.
    """
    if sandbox_result is None:
        return True
    return getattr(sandbox_result, "status", None) in AUTO_APPLY_PERMITTING_SANDBOX_STATUSES
