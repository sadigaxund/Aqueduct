"""Spark metrics helpers — zero-extra-action collection for module I/O.

Row counts use ``DataFrame.observe()`` (Spark 3.3+).  On older versions the
function returns the original DataFrame unchanged and ``None`` for the
observation; callers treat ``None`` as zero rows.

Byte counts use filesystem inspection post-write.  Cloud paths (s3://, hdfs://,
gs://, etc.) return 0 — Spark doesn't expose byte totals without an extra job.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


_CLOUD_SCHEMES = ("s3://", "s3a://", "s3n://", "hdfs://", "gs://", "abfs://", "wasb://", "dbfs:/")


def observe_df(
    df: "DataFrame",
    obs_name: str,
    alias: str = "records_written",
    enabled: bool = True,
) -> "tuple[DataFrame, Any]":
    """Wrap df with an Observation for row counting during the next action.

    Args:
        df:       DataFrame to observe.
        obs_name: Unique name for the Observation (scoped to a SparkSession).
        alias:    Metric name in the Observation result dict.
        enabled:  When False, short-circuit and return (df, None) without
                  inserting an Observation node. Used to honour
                  `metrics.use_observe: false` for high-throughput pipelines
                  where the codegen-break overhead is not worth the accuracy.

    Returns:
        (observed_df, observation) on Spark 3.3+ with enabled=True.
        (df, None)                 when disabled, on Spark < 3.3, or any failure.
    """
    if not enabled:
        return df, None
    try:
        from pyspark.sql import Observation
        from pyspark.sql.functions import count, lit

        obs = Observation(obs_name)
        return df.observe(obs, count(lit(1)).alias(alias)), obs
    except Exception:
        # ImportError (Spark < 3.3), AttributeError (mock df), or anything else
        return df, None


def get_observation(obs: Any, alias: str, timeout: float = 2.0) -> "int | None":
    """Safely read a completed Observation with a timeout.

    Returns:
        int   — observation fired; value is the metric (may be 0 for empty dataset).
        None  — observation did not fire within timeout (DF never consumed by a write
                action, or Spark < 3.3). Callers should treat None as "not collected"
                and leave the DB column NULL rather than writing 0.

    obs.get blocks until the observation fires. If the DF was never consumed
    by a write action (skipped downstream, gate closed, Probe-only path),
    the observation never fires and obs.get hangs forever. A daemon thread
    with join(timeout) guards against this.
    """
    if obs is None:
        return None
    import threading
    result: list[int | None] = [None]

    def _get() -> None:
        try:
            result[0] = int(obs.get.get(alias, 0))
        except Exception:
            pass

    t = threading.Thread(target=_get, daemon=True)
    t.start()
    t.join(timeout=timeout)
    # If thread is still alive, observation never fired — return None (not collected)
    # If thread finished, result[0] holds the actual count (could be 0 = empty dataset)
    return None if t.is_alive() else result[0]


def _hadoop_fs_bytes(path_str: str) -> "int | None":
    """Query Hadoop FileSystem via py4j for remote/HDFS/cloud path size.

    Requires an active SparkSession with the matching FileSystem jars on the
    classpath (S3A, GCS, ABFS, etc.).  Returns None on any failure so callers
    can fall back gracefully.
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is None:
            return None
        jvm = spark._jvm
        conf = spark._jsc.hadoopConfiguration()
        uri = jvm.java.net.URI(path_str)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
        summary = fs.getContentSummary(jvm.org.apache.hadoop.fs.Path(path_str))
        return int(summary.getLength())
    except Exception:
        return None


def dir_bytes(path_str: str) -> "int | None":
    """Return total byte size of a path — local or remote.

    For local paths: uses os.stat() directly.
    For cloud/HDFS paths (s3a://, gs://, hdfs://, etc.): queries Hadoop
    FileSystem via py4j (requires active SparkSession with matching jars).

    Returns:
        int   — measured size in bytes (0 = path exists but is empty).
        None  — size could not be determined: missing path, no active session,
                missing jars, or OS error. Stored as NULL, not 0.
    """
    if not path_str:
        return None
    for scheme in _CLOUD_SCHEMES:
        if path_str.startswith(scheme):
            return _hadoop_fs_bytes(path_str)
    try:
        import glob as _glob
        p = Path(path_str)
        if p.is_file():
            return p.stat().st_size
        if p.is_dir():
            return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())
        # Try glob expansion (handles patterns like data/green/*.parquet)
        total = 0
        matched = False
        for match in _glob.glob(path_str, recursive=True):
            matched = True
            mp = Path(match)
            if mp.is_file():
                total += mp.stat().st_size
            elif mp.is_dir():
                total += sum(f.stat().st_size for f in mp.rglob("*") if f.is_file())
        return total if matched else None  # no glob matches → path not found
    except OSError:
        return None


def null_metrics() -> "dict[str, Any]":
    """Return a metrics dict with all collection fields set to None (unknown/not-yet-collected)."""
    return {
        "records_read": None,
        "bytes_read": None,
        "records_written": None,
        "bytes_written": None,
        "duration_ms": 0,
    }
