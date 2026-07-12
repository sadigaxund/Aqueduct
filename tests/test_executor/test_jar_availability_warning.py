"""Unit tests for aqueduct/executor/spark/warnings/jar_availability.py.

No real SparkSession needed — `_loaded_jar_names`/`check` take any object
duck-typed as a session; these tests use MagicMock/plain objects. Covers the
Spark-Connect honesty fix: JVM-introspection failure must be distinguishable
from "genuinely zero JARs loaded" so the missing-driver warning doesn't
silently become a permanent no-op.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from aqueduct.executor.spark.warnings import jar_availability
from aqueduct.models import ModuleType

pytestmark = pytest.mark.unit


def _module(module_id: str, mtype: ModuleType, config: dict):
    return SimpleNamespace(id=module_id, type=mtype, config=config)


def _manifest(modules):
    return SimpleNamespace(modules=modules)


def test_loaded_jar_names_returns_none_when_jsc_missing():
    """Connect-like session: sparkContext has no _jsc → inspection unavailable."""
    spark = SimpleNamespace(sparkContext=SimpleNamespace())
    assert jar_availability._loaded_jar_names(spark) is None


def test_loaded_jar_names_returns_none_when_sparkcontext_raises():
    class _NoSparkContext:
        @property
        def sparkContext(self):
            raise AttributeError("sparkContext not available on Connect sessions")

    assert jar_availability._loaded_jar_names(_NoSparkContext()) is None


def test_loaded_jar_names_returns_empty_list_when_genuinely_no_jars():
    it = MagicMock()
    it.hasNext.return_value = False
    jars_seq = MagicMock()
    jars_seq.iterator.return_value = it
    jsc = MagicMock()
    jsc.sc.return_value.listJars.return_value = jars_seq
    spark = SimpleNamespace(sparkContext=SimpleNamespace(_jsc=jsc))

    result = jar_availability._loaded_jar_names(spark)
    assert result == []


def test_check_emits_soft_note_when_inspection_unavailable():
    """check() must not claim all-clear when jars couldn't be inspected —
    it should surface a distinct 'could not verify' message, not silence."""
    spark = SimpleNamespace(sparkContext=SimpleNamespace())  # no _jsc
    manifest = _manifest([
        _module("m1", ModuleType.Ingress, {"format": "delta"}),
    ])

    messages = jar_availability.check(manifest, spark)
    assert len(messages) == 1
    assert "could not verify" in messages[0].lower()
    assert "delta" in messages[0]


def test_check_silent_when_inspection_unavailable_and_no_jar_needed_formats():
    """No format requiring a JAR check is declared → no message, even if
    inspection failed (nothing to warn honestly about)."""
    spark = SimpleNamespace(sparkContext=SimpleNamespace())
    manifest = _manifest([
        _module("m1", ModuleType.Ingress, {"format": "parquet"}),
    ])
    assert jar_availability.check(manifest, spark) == []


def test_check_still_warns_on_genuinely_missing_jar():
    it = MagicMock()
    it.hasNext.return_value = False
    jars_seq = MagicMock()
    jars_seq.iterator.return_value = it
    jsc = MagicMock()
    jsc.sc.return_value.listJars.return_value = jars_seq
    spark = SimpleNamespace(sparkContext=SimpleNamespace(_jsc=jsc))

    manifest = _manifest([
        _module("m1", ModuleType.Ingress, {"format": "delta"}),
    ])
    messages = jar_availability.check(manifest, spark)
    assert len(messages) == 1
    assert "delta" in messages[0]
    assert "could not verify" not in messages[0].lower()
