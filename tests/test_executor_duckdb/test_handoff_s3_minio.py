"""Cross-engine handoff (`handoff.root`) against a real `s3a://` MinIO root —
closes the untested claim behind httpfs work: the transport is parquet
files at `handoff.root`, and the case that made non-AWS S3 config matter is
exactly this one — Spark writes the spill FROM ITS EXECUTORS, the next
engine reads it back, so BOTH sides must reach the SAME URI with their OWN
credentials/config (Spark: S3A Hadoop conf + `hadoop-aws`; DuckDB: httpfs +
`CREATE SECRET` + the `engine.duckdb.s3_endpoint`/`s3_url_style`/`s3_use_ssl`
non-AWS escape hatch added alongside this test).

Both directions verified against the repo owner's local docker-compose MinIO
(``tmp/04-dashboard-showcase/docker-compose.yml``).

**Why a subprocess, not a normal in-process test — read before "simplifying"
this.** Spark needs `hadoop-aws` on its JVM classpath, settable ONLY at the
FIRST `SparkSession.builder...getOrCreate()` in a process (`spark.jars.
packages` is a static conf). Measured: `tests/conftest.py` unconditionally
builds a PLAIN SparkSession (no hadoop-aws) as a side effect of the
*module-level* `_spark_is_healthy()` call that defines
`requires_healthy_spark` — this fires on `conftest.py` import, before any
test in this file runs, for ANY pytest invocation that collects this
directory at all. There is therefore no way to be "first" from inside a
pytest process. Also measured and ruled out: patching an ALREADY-RUNNING
session (`SparkContext._jsc.addJar()` + mutating `hadoopConfiguration()`
directly) — `addJar` only ships jars to executors for task closures, it
does not extend the driver JVM's own classloader; the write still raises
`ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem`. A fresh
subprocess (`_handoff_s3_minio_worker.py`, not collected by pytest — no
`test_` prefix) is the only way to get an uncontaminated JVM, so that is
what these tests shell out to.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from tests.conftest import (
    _minio_access_key,
    _minio_endpoint,
    _minio_secret_key,
    ensure_minio_bucket,
    requires_minio,
)

pytest.importorskip("pyspark", reason="pyspark required for the Spark side of the handoff")

pytestmark = [pytest.mark.spark, pytest.mark.integration, requires_minio]

_BUCKET = "aqueduct-test"
_WORKER = Path(__file__).parent / "_handoff_s3_minio_worker.py"


@pytest.fixture(autouse=True, scope="module")
def _bucket():
    ensure_minio_bucket(_BUCKET)


def _run_worker(direction: str, tmp_path: Path) -> subprocess.CompletedProcess:
    endpoint_hostport = _minio_endpoint().split("://", 1)[-1]
    return subprocess.run(
        [
            sys.executable,
            str(_WORKER),
            "--direction",
            direction,
            "--tmp-dir",
            str(tmp_path),
            "--endpoint",
            endpoint_hostport,
            "--access-key",
            _minio_access_key(),
            "--secret-key",
            _minio_secret_key(),
        ],
        capture_output=True,
        text=True,
        timeout=300,
    )


def test_spark_to_duckdb_handoff_over_s3_minio(tmp_path):
    proc = _run_worker("forward", tmp_path)
    if proc.returncode != 0 or "HANDOFF_S3_FORWARD_OK" not in proc.stdout:
        pytest.fail(
            f"worker failed (exit={proc.returncode}):\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )


def test_duckdb_to_spark_handoff_over_s3_minio(tmp_path):
    proc = _run_worker("reverse", tmp_path)
    if proc.returncode != 0 or "HANDOFF_S3_REVERSE_OK" not in proc.stdout:
        pytest.fail(
            f"worker failed (exit={proc.returncode}):\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
