"""Multi-axis budget for the unified reprompt loop (Phase 34).

Replaces the single ``max_reprompts`` knob with a set of orthogonal axes;
the loop terminates on the FIRST axis that trips and records which one as
the ``stop_reason``. Same config is shared by production heal AND benchmark
— divergence between the two would silently invalidate the leaderboard
(see Phase 34 item #7 in TODOs.md).

This module ships the config dataclass, the reason vocabulary, and the
driver (``BudgetTracker``); integration lives in ``generate_agent_patch``.

Defaults
--------
``max_reprompts``           hard ceiling on LLM round-trips per heal call.
``max_seconds``             wall-clock cap per heal call.
``max_tokens_total``        sum of prompt+completion tokens (cost cap).
``same_error_consecutive``  ≥N identical signatures in a row → likely stuck.
``same_signature_overall``  ≥N occurrences of one signature across the run.
``progress_stalled_window`` distinct-signature-set must shrink within N turns.

The two ``same_*`` axes feed Task 87 (stuck-detection escalation BEFORE
abort) — when ``same_error_consecutive`` trips, the loop bumps temperature
+ swaps reprompt template for ONE more attempt before honouring the abort.

Stop reasons
------------
``solved``                  LLM returned a parseable PatchSpec, loop exits clean.
                            NOTE: this describes loop termination only — it does
                            NOT mean the patch fixed the pipeline. The patch may
                            still fail the apply / lineage / sandbox / explain
                            gates downstream. To check whether the heal actually
                            worked, query ``healing_outcomes.run_success_after_patch``
                            for the same ``run_id``.
``exhausted_attempts``      ``max_reprompts`` reached.
``budget_seconds_exceeded`` ``max_seconds`` reached.
``budget_tokens_exceeded``  ``max_tokens_total`` reached.
``stuck_signature``         same-signature axis tripped (post-escalation).
``progress_stalled``        no new distinct signatures across the window.
``api_error``               provider call raised (non-recoverable).
``deferred``                LLM deferred to human — failure not healable at the
                            Blueprint level (Phase 41).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Literal

from aqueduct.agent.signature import ErrorSignature

__all__ = [
    "BudgetConfig",
    "BudgetTracker",
    "DEFAULT_BUDGET",
    "StopReason",
    "STOP_REASONS",
    "AttemptRecord",
]


StopReason = Literal[
    "solved",
    "exhausted_attempts",
    "budget_seconds_exceeded",
    "budget_tokens_exceeded",
    "stuck_signature",
    "progress_stalled",
    "api_error",
    "deferred",
]

STOP_REASONS: tuple[StopReason, ...] = (
    "solved",
    "exhausted_attempts",
    "budget_seconds_exceeded",
    "budget_tokens_exceeded",
    "stuck_signature",
    "progress_stalled",
    "api_error",
    "deferred",
)


@dataclass(frozen=True)
class BudgetConfig:
    """Frozen multi-axis budget. Same instance used by heal AND benchmark.

    Set an axis to ``None`` (where the type allows) to disable that axis;
    integer axes default to enabled with safe ceilings. Validation lives on
    the constructor so misconfiguration fails at config-load, not at heal
    time.
    """

    max_reprompts: int = 5
    max_seconds: float = 120.0
    max_tokens_total: int | None = 50_000
    same_error_consecutive: int = 2
    same_signature_overall: int = 3
    progress_stalled_window: int = 3

    def __post_init__(self) -> None:
        if self.max_reprompts < 1:
            raise ValueError("BudgetConfig.max_reprompts must be >= 1")
        if self.max_seconds <= 0:
            raise ValueError("BudgetConfig.max_seconds must be > 0")
        if self.max_tokens_total is not None and self.max_tokens_total < 1:
            raise ValueError("BudgetConfig.max_tokens_total must be >= 1 or None")
        if self.same_error_consecutive < 2:
            raise ValueError(
                "BudgetConfig.same_error_consecutive must be >= 2 — a single "
                "occurrence is not yet evidence of being stuck."
            )
        if self.same_signature_overall < self.same_error_consecutive:
            raise ValueError(
                "BudgetConfig.same_signature_overall must be >= "
                "same_error_consecutive — overall ceiling cannot be stricter "
                "than the consecutive one."
            )
        if self.progress_stalled_window < 2:
            raise ValueError(
                "BudgetConfig.progress_stalled_window must be >= 2 — need at "
                "least two turns to detect non-progress."
            )

    def to_dict(self) -> dict[str, float | int | None]:
        return {
            "max_reprompts": self.max_reprompts,
            "max_seconds": self.max_seconds,
            "max_tokens_total": self.max_tokens_total,
            "same_error_consecutive": self.same_error_consecutive,
            "same_signature_overall": self.same_signature_overall,
            "progress_stalled_window": self.progress_stalled_window,
        }


DEFAULT_BUDGET = BudgetConfig()


@dataclass
class AttemptRecord:
    """One row in the per-heal attempt log; persisted to ``heal_attempts``."""

    attempt_num: int
    signature: ErrorSignature | None        # None when the attempt SUCCEEDED
    tokens_in: int = 0
    tokens_out: int = 0
    latency_ms: int = 0
    gate_that_rejected: str | None = None    # 'schema' | 'apply' | 'validate' | 'provider' | 'budget' | 'defer_rejected' | None on success
    escalated: bool = False                   # was Task 87 escalation applied on this attempt?
    model_cascade_position: int | None = None  # Phase 44: tier index (0-based) in multi-model cascade

    def to_dict(self) -> dict:
        return {
            "attempt_num": self.attempt_num,
            "signature": self.signature.to_dict() if self.signature else None,
            "tokens_in": self.tokens_in,
            "tokens_out": self.tokens_out,
            "latency_ms": self.latency_ms,
            "gate_that_rejected": self.gate_that_rejected,
            "escalated": self.escalated,
            "model_cascade_position": self.model_cascade_position,
        }


@dataclass
class BudgetTracker:
    """Drives the unified reprompt loop. Checks every axis after each attempt.

    Lifecycle:
        tracker = BudgetTracker(config)
        while True:
            tracker.begin_attempt()
            ...call LLM, maybe apply...
            tracker.record(signature_or_None, tokens_in, tokens_out, latency_ms, gate)
            reason = tracker.check_stop()
            if reason: break          # 'solved' set internally when signature is None
            if tracker.should_escalate(): ...bump temperature, swap template...
    """

    config: BudgetConfig
    started_at: float = field(default_factory=time.monotonic)
    attempts: list[AttemptRecord] = field(default_factory=list)
    tokens_in_total: int = 0
    tokens_out_total: int = 0
    escalated_once: bool = False             # one escalation per heal, never two
    _current_attempt: int = 0
    _stop_reason: str | None = None

    # ── public API ─────────────────────────────────────────────────────────

    def begin_attempt(self) -> int:
        self._current_attempt += 1
        return self._current_attempt

    @property
    def current_attempt(self) -> int:
        return self._current_attempt

    @property
    def stop_reason(self) -> str | None:
        return self._stop_reason

    def record(
        self,
        signature: ErrorSignature | None,
        *,
        tokens_in: int = 0,
        tokens_out: int = 0,
        latency_ms: int = 0,
        gate_that_rejected: str | None = None,
        escalated: bool = False,
        model_cascade_position: int | None = None,
    ) -> AttemptRecord:
        rec = AttemptRecord(
            attempt_num=self._current_attempt,
            signature=signature,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            latency_ms=latency_ms,
            gate_that_rejected=gate_that_rejected,
            escalated=escalated,
            model_cascade_position=model_cascade_position,
        )
        self.attempts.append(rec)
        self.tokens_in_total += tokens_in
        self.tokens_out_total += tokens_out
        return rec

    def check_stop(self) -> str | None:
        """Return a stop reason if any axis tripped, else None. Sticky once set."""
        if self._stop_reason is not None:
            return self._stop_reason
        last = self.attempts[-1] if self.attempts else None
        if last and last.signature is None:
            self._stop_reason = "solved"
            return self._stop_reason
        if self._current_attempt >= self.config.max_reprompts:
            self._stop_reason = "exhausted_attempts"
            return self._stop_reason
        if time.monotonic() - self.started_at >= self.config.max_seconds:
            self._stop_reason = "budget_seconds_exceeded"
            return self._stop_reason
        if (
            self.config.max_tokens_total is not None
            and (self.tokens_in_total + self.tokens_out_total) >= self.config.max_tokens_total
        ):
            self._stop_reason = "budget_tokens_exceeded"
            return self._stop_reason
        if self._same_signature_overall_tripped():
            # Overall-occurrence axis is unconditional — N repeats of the same
            # signature anywhere in the run is "looping" regardless of
            # escalation state. Maps to the public ``stuck_signature`` reason.
            self._stop_reason = "stuck_signature"
            return self._stop_reason
        if self._progress_stalled_tripped():
            self._stop_reason = "progress_stalled"
            return self._stop_reason
        # Stuck-consecutive: only abort with `stuck_signature` AFTER one
        # escalation has been spent (Task 87). Until then it's a signal to
        # escalate, not abort — the caller checks should_escalate().
        if self._same_error_consecutive_tripped() and self.escalated_once:
            self._stop_reason = "stuck_signature"
            return self._stop_reason
        return None

    def should_escalate(self) -> bool:
        """True iff stuck-consecutive tripped AND we haven't escalated yet."""
        if self.escalated_once:
            return False
        return self._same_error_consecutive_tripped()

    def mark_escalated(self) -> None:
        self.escalated_once = True

    def mark_api_error(self) -> None:
        """Caller invokes when the provider raises — terminates with api_error."""
        self._stop_reason = "api_error"

    def mark_budget_seconds_exceeded(self) -> None:
        """Caller invokes when the mid-call budget deadline fires — terminates
        with ``budget_seconds_exceeded`` (Phase 40)."""
        self._stop_reason = "budget_seconds_exceeded"

    def mark_deferred(self) -> None:
        """Caller invokes when the LLM defers to a human (``defer_to_human``
        with ``allow_defer=True``) — terminates with ``deferred``."""
        self._stop_reason = "deferred"

    def remaining_seconds(self) -> float:
        """Seconds remaining in the wall-clock budget, floored at 0 (Phase 40).

        Used by the orchestration loop to compute a per-call HTTP deadline
        so that ``max_seconds`` is enforced mid-call, not just at iteration
        boundaries.
        """
        elapsed = time.monotonic() - self.started_at
        return max(0.0, self.config.max_seconds - elapsed)

    def signatures(self) -> list[ErrorSignature]:
        return [a.signature for a in self.attempts if a.signature is not None]

    def summary(self) -> dict:
        return {
            "attempts": self._current_attempt,
            "stop_reason": self._stop_reason,
            "tokens_in_total": self.tokens_in_total,
            "tokens_out_total": self.tokens_out_total,
            "elapsed_seconds": round(time.monotonic() - self.started_at, 3),
            "escalated_once": self.escalated_once,
            "signatures": [s.to_dict() for s in self.signatures()],
        }

    # ── axis evaluators (private) ──────────────────────────────────────────

    def _same_error_consecutive_tripped(self) -> bool:
        sigs = self.signatures()
        need = self.config.same_error_consecutive
        if len(sigs) < need:
            return False
        tail = sigs[-need:]
        return all(s.hash == tail[0].hash for s in tail)

    def _same_signature_overall_tripped(self) -> bool:
        sigs = self.signatures()
        need = self.config.same_signature_overall
        if not sigs:
            return False
        counts: dict[str, int] = {}
        for s in sigs:
            counts[s.hash] = counts.get(s.hash, 0) + 1
            if counts[s.hash] >= need:
                return True
        return False

    def _progress_stalled_tripped(self) -> bool:
        sigs = self.signatures()
        window = self.config.progress_stalled_window
        if len(sigs) < window:
            return False
        tail = sigs[-window:]
        # Stalled = distinct-signature-set across the window is exactly 1 OR
        # has not grown vs the previous window. Use "all same in window" as
        # the conservative trigger — broader stalls are caught by
        # same_signature_overall.
        unique = {s.hash for s in tail}
        return len(unique) == 1
