"""SparkSession factory.

Creates (or reuses) a SparkSession and applies pipeline-level spark_config.
The factory is the only place in the codebase that calls SparkSession.builder.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import SparkSession


def make_spark_session(pipeline_id: str, spark_config: dict[str, Any]) -> SparkSession:
    """Build or reuse a SparkSession for the given pipeline.

    Applies every key in spark_config as a Spark conf property.  If a
    SparkSession already exists (e.g. on a running Spark cluster) the existing
    session is returned and the supplied config is applied on top.

    Args:
        pipeline_id:  Used as the Spark app name when building a fresh session.
        spark_config: Flat dict of Spark conf key → value strings.

    Returns:
        An active SparkSession.
    """
    builder = SparkSession.builder.appName(pipeline_id)

    for key, value in spark_config.items():
        builder = builder.config(key, str(value))

    return builder.getOrCreate()
