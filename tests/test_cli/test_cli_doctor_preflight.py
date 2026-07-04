"""Test ``doctor --preflight`` cloud object existence check (``_cloud_uri_check``).

Three branches:
1. Without ``--preflight`` → always returns ``"skip"``.
2. With ``--preflight`` + Ingress → ``"ok"`` if object exists, ``"fail"`` if not.
3. With ``--preflight`` + Egress → ``"ok"`` if parent prefix resolves.
"""
from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from aqueduct.doctor import _cloud_uri_check
from aqueduct.parser.models import ModuleType

pytestmark = pytest.mark.unit


# ── Non-preflight (no Spark needed) ────────────────────────────────────────────


@pytest.mark.parametrize("module_type", [ModuleType.Ingress, ModuleType.Egress])
def test_skip_without_preflight_flag(module_type):
    result = _cloud_uri_check(
        "ingress:in" if module_type == ModuleType.Ingress else "egress:out",
        "s3a://bucket/path/to/file.parquet",
        module_type,
        time.monotonic(),
        preflight=False,
    )
    assert result.status == "skip"
    assert "cloud URI" in result.detail
    assert "--preflight" in result.detail


# ── Preflight + Ingress ───────────────────────────────────────────────────────


def _mock_spark_session(jpath_return=None, jpath_side_effect=None):
    """Build a mock SparkSession with the JVM Path mock wired."""
    mock_spark = MagicMock()
    mock_jvm = MagicMock()
    mock_jvm.org.apache.hadoop.fs.Path = MagicMock(
        return_value=jpath_return,
        side_effect=jpath_side_effect,
    )
    mock_spark._jvm = mock_jvm
    mock_spark._jsc.hadoopConfiguration.return_value = MagicMock()
    return mock_spark


def test_preflight_ingress_object_exists():
    mock_fs = MagicMock()
    mock_fs.exists.return_value = True
    mock_jpath = MagicMock()
    mock_jpath.getFileSystem.return_value = mock_fs

    mock_spark = _mock_spark_session(jpath_return=mock_jpath)

    with patch("pyspark.sql.SparkSession", return_value=mock_spark) as MockSS:
        MockSS.builder.getOrCreate.return_value = mock_spark
        result = _cloud_uri_check(
            "ingress:in", "s3a://bucket/obj.parquet",
            ModuleType.Ingress, time.monotonic(), preflight=True,
        )

    assert result.status == "ok"
    assert result.name == "ingress:in"
    assert "s3a://bucket/obj.parquet" in result.detail


def test_preflight_ingress_object_missing():
    mock_fs = MagicMock()
    mock_fs.exists.return_value = False
    mock_jpath = MagicMock()
    mock_jpath.getFileSystem.return_value = mock_fs

    mock_spark = _mock_spark_session(jpath_return=mock_jpath)

    with patch("pyspark.sql.SparkSession", return_value=mock_spark) as MockSS:
        MockSS.builder.getOrCreate.return_value = mock_spark
        result = _cloud_uri_check(
            "ingress:in", "s3a://bucket/missing.parquet",
            ModuleType.Ingress, time.monotonic(), preflight=True,
        )

    assert result.status == "fail"
    assert "not found" in result.detail


def test_preflight_ingress_fs_throws_warning():
    mock_spark = _mock_spark_session(jpath_side_effect=RuntimeError("Bad creds"))

    with patch("pyspark.sql.SparkSession", return_value=mock_spark) as MockSS:
        MockSS.builder.getOrCreate.return_value = mock_spark
        result = _cloud_uri_check(
            "ingress:in", "s3a://bucket/obj.parquet",
            ModuleType.Ingress, time.monotonic(), preflight=True,
        )

    assert result.status == "warn"
    assert "Bad creds" in result.detail


# ── Preflight + Egress ────────────────────────────────────────────────────────


def test_preflight_egress_parent_prefix_resolves():
    mock_fs = MagicMock()
    mock_jpath_parent = MagicMock()
    mock_jpath = MagicMock()
    mock_jpath.getFileSystem.return_value = mock_fs
    mock_jpath.getParent.return_value = mock_jpath_parent

    mock_spark = _mock_spark_session(jpath_return=mock_jpath)

    with patch("pyspark.sql.SparkSession", return_value=mock_spark) as MockSS:
        MockSS.builder.getOrCreate.return_value = mock_spark
        result = _cloud_uri_check(
            "egress:out", "s3a://bucket/prefix/",
            ModuleType.Egress, time.monotonic(), preflight=True,
        )

    assert result.status == "ok"
    assert "target reachable" in result.detail
    mock_fs.exists.assert_called_once_with(mock_jpath_parent)
