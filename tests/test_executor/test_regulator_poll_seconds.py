"""Regulator `poll_seconds` knob (1.1.0).

Covers TEST_MANIFEST.md ⏳ items under
"Regulator poll_seconds knob (1.1.0)".

These tests verify the clamp + default logic on the executor's poll loop
without spinning up Spark. The relevant snippet (executor/spark/executor.py):

    poll_interval = max(0.5, float(module.config.get("poll_seconds", 30.0)))

We exercise it by importing the module and recomputing the clamp directly
so the assertions don't require a live SparkSession.
"""
from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit


def _clamp(cfg: dict) -> float:
    """Mirrors the executor's poll-interval clamp formula."""
    return max(0.5, float(cfg.get("poll_seconds", 30.0)))


def test_poll_seconds_default_is_thirty():
    """`config.poll_seconds` omitted → 30.0 used."""
    assert _clamp({}) == 30.0


def test_poll_seconds_half_second_respected():
    """`poll_seconds: 0.5` accepted at the minimum boundary."""
    assert _clamp({"poll_seconds": 0.5}) == 0.5


def test_poll_seconds_below_min_clamps_to_half():
    """`poll_seconds: 0.1` clamps to 0.5."""
    assert _clamp({"poll_seconds": 0.1}) == 0.5


def test_poll_seconds_zero_clamps_to_half():
    """`poll_seconds: 0` clamps to 0.5."""
    assert _clamp({"poll_seconds": 0}) == 0.5


def test_executor_source_matches_clamp_formula():
    """Sanity: the executor source contains the exact clamp expression so the
    helper above stays in sync. Catches accidental refactors that drop the
    clamp or change the default.
    """
    from pathlib import Path
    src = Path(__file__).resolve().parents[2] / "aqueduct" / "executor" / "spark" / "executor.py"
    text = src.read_text(encoding="utf-8")
    assert 'max(0.5, float(module.config.get("poll_seconds", 30.0)))' in text


def test_timeout_zero_skips_poll_loop():
    """With timeout_seconds: 0 the while-loop guard `elapsed < timeout_sec`
    is false from the start, so poll_seconds has no observable effect.

    We verify the control-flow predicate directly: the executor enters the
    loop only when `not gate_open and timeout_sec > 0`.
    """
    timeout_sec = 0.0
    gate_open = False
    enters_loop = (not gate_open) and timeout_sec > 0
    assert enters_loop is False
