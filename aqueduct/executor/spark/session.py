"""SparkSession factory.

Creates (or reuses) a SparkSession and applies pipeline-level spark_config.
The factory is the only place in the codebase that calls SparkSession.builder.
"""

from __future__ import annotations

import contextlib
import os
import sys
from typing import Any

from pyspark.sql import SparkSession

_DEFAULT_MASTER = "local[*]"

# log4j/log4j2 suppress flags injected into driver JVM before session start
_LOG4J_QUIET_OPTS = (
    "-Dlog4j.rootCategory=ERROR,console"
    " -Dlog4j2.rootLogger.level=ERROR"
    " -Dlog4j2.rootLogger.appenderRef.console.ref=console"
)


@contextlib.contextmanager
def _suppress_stderr():
    """Redirect fd 2 (JVM writes here) and sys.stderr to /dev/null."""
    import logging
    logging.getLogger("py4j").setLevel(logging.ERROR)
    devnull_fd = os.open(os.devnull, os.O_WRONLY)
    saved_fd2 = os.dup(2)
    saved_stderr = sys.stderr
    os.dup2(devnull_fd, 2)
    sys.stderr = open(os.devnull, "w")
    try:
        yield
    finally:
        sys.stderr.close()
        sys.stderr = saved_stderr
        os.dup2(saved_fd2, 2)
        os.close(saved_fd2)
        os.close(devnull_fd)


def make_spark_session(
    pipeline_id: str,
    spark_config: dict[str, Any],
    master_url: str = _DEFAULT_MASTER,
    quiet: bool = False,
) -> SparkSession:
    """Build or reuse a SparkSession for the given pipeline.

    Applies every key in spark_config as a Spark conf property.  If a
    SparkSession already exists (e.g. on a running Spark cluster) the existing
    session is returned and the supplied config is applied on top.

    Args:
        pipeline_id:  Used as the Spark app name when building a fresh session.
        spark_config: Flat dict of Spark conf key → value strings.
        master_url:   Spark master URL.  Examples:
                        ``"local[*]"``          — local mode (default)
                        ``"spark://host:7077"`` — standalone cluster
                        ``"yarn"``              — YARN resource manager
                        ``"k8s://https://..."`` — Kubernetes
                      Passed verbatim to ``SparkSession.builder.master()``.
        quiet:        Suppress all Spark/JVM log output during and after session
                      startup. Use for health-check commands (doctor). Leave
                      False for pipeline runs so Spark warnings remain visible.

    Returns:
        An active SparkSession.
    """
    builder = SparkSession.builder.master(master_url).appName(pipeline_id)

    if quiet:
        # Inject log4j suppress flags before JVM init so startup messages are
        # silenced. Prepend so user-supplied extraJavaOptions still take effect.
        existing = str(spark_config.get("spark.driver.extraJavaOptions", ""))
        builder = builder.config(
            "spark.driver.extraJavaOptions",
            f"{_LOG4J_QUIET_OPTS} {existing}".strip(),
        )

    for key, value in spark_config.items():
        builder = builder.config(key, str(value))

    if quiet:
        with _suppress_stderr():
            session = builder.getOrCreate()
        session.sparkContext.setLogLevel("ERROR")
    else:
        session = builder.getOrCreate()

    from aqueduct.executor.spark.listener import AqueductMetricsListener
    listener = AqueductMetricsListener()
    listener.register(session)
    session._aq_metrics_listener = listener  # type: ignore[attr-defined]

    return session
