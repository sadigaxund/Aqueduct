"""SparkSession factory.

Creates (or reuses) a SparkSession and applies pipeline-level spark_config.
The factory is the only place in the codebase that calls SparkSession.builder.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession

_DEFAULT_MASTER = "local[*]"


def make_spark_session(
    pipeline_id: str,
    spark_config: dict[str, Any],
    master_url: str = _DEFAULT_MASTER,
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

    Returns:
        An active SparkSession.
    """
    builder = SparkSession.builder.master(master_url).appName(pipeline_id)

    for key, value in spark_config.items():
        builder = builder.config(key, str(value))

    session = builder.getOrCreate()

    from aqueduct.executor.spark.listener import AqueductMetricsListener
    listener = AqueductMetricsListener()
    listener.register(session)
    session._aq_metrics_listener = listener  # type: ignore[attr-defined]

    return session
