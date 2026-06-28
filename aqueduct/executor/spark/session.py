"""SparkSession factory.

Creates (or reuses) a SparkSession and applies blueprint-level spark_config.
The factory is the only place in the codebase that calls SparkSession.builder.
"""

from __future__ import annotations

import contextlib
import logging
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
    blueprint_id: str,
    spark_config: dict[str, Any],
    master_url: str = _DEFAULT_MASTER,
    quiet: bool = False,
    quiet_startup: bool = False,
) -> SparkSession:
    """Build or reuse a SparkSession for the given blueprint.

    Applies every key in spark_config as a Spark conf property.  If a
    SparkSession already exists (e.g. on a running Spark cluster) the existing
    session is returned and the supplied config is applied on top.

    Args:
        blueprint_id: Used as the Spark app name when building a fresh session.
        spark_config: Flat dict of Spark conf key → value strings.
        master_url:   Spark master URL.  Examples:
                        ``"local[*]"``          — local mode (default)
                        ``"spark://host:7077"`` — standalone cluster
                        ``"yarn"``              — YARN resource manager
                        ``"k8s://https://..."`` — Kubernetes
                      Passed verbatim to ``SparkSession.builder.master()``.
        quiet:        Suppress all Spark/JVM log output during and after session
                      startup. Use for health-check commands (doctor).
        quiet_startup: Suppress only the JVM/Spark *startup banner* (incubator
                      notice, log4j profile lines, NativeCodeLoader warning) by
                      muting stderr around session creation — but leave the
                      runtime log level at WARN so genuine Spark warnings during
                      execution still print. The clean default for `aqueduct run`.

    Returns:
        An active SparkSession.
    """
    # Localized per import discipline: compat patch lives behind the factory
    # entrypoint, not at module load (ISSUE-025).
    from aqueduct.executor.spark.udf import _patch_pyspark_cloudpickle

    _patch_pyspark_cloudpickle()
    builder = SparkSession.builder.master(master_url).appName(blueprint_id)

    # Spark 4.0 defaults to structured (JSON) logging: its SQLQueryContextLogger
    # dumps the whole Catalyst plan tree as JSON to stderr on every
    # AnalysisException (ISSUE-046), fighting Aqueduct's own --log-format. Revert
    # to plain text unless the user explicitly opts in (their spark_config below
    # still wins).
    if "spark.log.structuredLogging.enabled" not in spark_config:
        builder = builder.config("spark.log.structuredLogging.enabled", "false")

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
    elif quiet_startup:
        # Mute only the startup banner; keep the default (WARN) runtime level.
        with _suppress_stderr():
            session = builder.getOrCreate()
    else:
        session = builder.getOrCreate()

    _mute_query_context_loggers()
    return session


def _mute_query_context_loggers() -> None:
    """Silence pyspark's per-error query-context JSON dump (ISSUE-046).

    On every ``AnalysisException`` pyspark 4.x logs the full query context (the
    entire Catalyst plan tree) as one JSON line to stderr. The crucial fact —
    confirmed against a live ``local[1]`` Spark 4.1 — is that this is emitted
    **python-side, not by the JVM**: ``pyspark/errors/exceptions/base.py`` calls
    ``PySparkLogger.getLogger("SQLQueryContextLogger").exception(...)`` (and a
    ``DataFrameQueryContextLogger`` sibling). That is why no JVM/log4j lever
    touches it — ``Configurator.setLevel`` (default + Spark-classloader context),
    per-instance ``core.Logger.setLevel``, ``setRootLevel(OFF)`` and
    ``sparkContext.setLogLevel("OFF")`` were all verified to do nothing. The dump
    duplicates the root cause Aqueduct already reports concisely on the module
    status line and ignores ``--log-format``.

    The fix is plain Python ``logging``: these are ``logging.Logger`` instances
    (own ``StreamHandler``, ``propagate=False``). We create them via pyspark's
    own ``PySparkLogger.getLogger`` (so the class/handler match) and raise the
    level above ERROR — ``logger.exception()`` logs at ERROR, so it is filtered.
    ``logging`` caches loggers by name, so pyspark's later ``getLogger`` at error
    time returns this same muted instance. The ``AnalysisException`` itself still
    propagates to Aqueduct untouched (the heal path is unaffected).
    """
    try:
        from pyspark.logger import PySparkLogger
        for name in ("SQLQueryContextLogger", "DataFrameQueryContextLogger"):
            PySparkLogger.getLogger(name).setLevel(logging.CRITICAL)
    except Exception:
        pass  # best-effort — pyspark logging internals may differ across versions


def stop_spark_session(spark: SparkSession) -> None:
    """Stop a SparkSession created by a short-lived CLI command.

    `make_spark_session` uses `getOrCreate()`, so the returned session may be a
    pre-existing global one (e.g. the session-scoped pytest fixture, or a
    long-lived cluster driver session). Eagerly calling `.stop()` there tears
    down a `SparkContext` other code still depends on — under pytest this kills
    every subsequent test with `'NoneType' object has no attribute 'sc'`
    (ISSUE-026). When `AQ_TESTING` is set we skip the stop and let the fixture
    own the session lifecycle. In a real one-shot CLI run the process exits
    immediately after, so the OS reclaims the JVM regardless.
    """
    if os.environ.get("AQ_TESTING"):
        return
    spark.stop()
