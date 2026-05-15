"""Stable exit-code contract for the Aqueduct CLI — Phase 30b (v1.0 stamp).

Downstream tooling (Airflow operators, CI runners, shell wrappers) keys off
these codes to decide whether to retry, page, or fail-the-pipeline. They are
part of the v1.0 stability contract — semver applies.

Codes:
    0   SUCCESS              run / command completed without error
    1   CONFIG_ERROR         malformed aqueduct.yml or Blueprint schema error
    2   DATA_OR_RUNTIME      execution failed at runtime — Spark error,
                             Assert failure, missing file, network, etc.
    3   HEAL_PENDING         healing produced a patch staged for human
                             review (only emitted by `aqueduct run` /
                             `aqueduct heal` in `human` / `ci` mode)
    4   VALIDATION_GATE      patch validation pyramid (Gate 1–4) rejected
                             a patch in non-interactive mode
    5   USAGE_ERROR          CLI invoked with invalid flag / missing arg.
                             Click already exits with this category; the
                             constant is here for completeness.

Add a code by appending to this module — never reuse a retired number.
"""

from __future__ import annotations

SUCCESS: int = 0
CONFIG_ERROR: int = 1
DATA_OR_RUNTIME: int = 2
HEAL_PENDING: int = 3
VALIDATION_GATE: int = 4
USAGE_ERROR: int = 5

__all__ = [
    "SUCCESS",
    "CONFIG_ERROR",
    "DATA_OR_RUNTIME",
    "HEAL_PENDING",
    "VALIDATION_GATE",
    "USAGE_ERROR",
]
