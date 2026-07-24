"""Unit test for the Spark-Connect honesty fix in
aqueduct/executor/spark/channel.py::_apply_metrics_boundary.

No real SparkSession/pyspark import needed for the guard itself — the guard
only wraps `df.rdd.getNumPartitions()` in try/except, so a stub DataFrame
whose `.rdd` raises is enough to exercise the failure path deterministically
(equivalent to what a Spark Connect DataFrame does).
"""

from __future__ import annotations

import logging

import pytest

from aqueduct.executor.spark.channel import _apply_metrics_boundary

pytestmark = pytest.mark.unit


class _ConnectLikeDf:
    """Stub mimicking a Spark Connect DataFrame: `.rdd` raises."""

    @property
    def rdd(self):
        raise AttributeError("'DataFrame' object has no attribute 'rdd' on Connect")

    def repartition(self, n):  # pragma: no cover - must not be called
        raise AssertionError("repartition should not be called when .rdd raises")


def test_metrics_boundary_skips_on_rdd_failure_without_raising(caplog):
    df = _ConnectLikeDf()
    with caplog.at_level(logging.WARNING):
        result = _apply_metrics_boundary(df, {"metrics_boundary": True}, "m1")
    # Transform result flows through unchanged — no exception propagated.
    assert result is df
    assert any("runtime_metrics_boundary_skipped" in rec.getMessage() for rec in caplog.records)
    assert any("m1" in rec.getMessage() for rec in caplog.records)


def test_metrics_boundary_noop_when_disabled():
    df = _ConnectLikeDf()
    # metrics_boundary not set → returns df unchanged without touching .rdd at all.
    result = _apply_metrics_boundary(df, {}, "m1")
    assert result is df
