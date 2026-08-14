"""Executor layer — runs compiled manifests against a Spark cluster.

``execute`` and ``ExecuteError`` are resolved lazily via ``__getattr__``
so that ``aqueduct.executor.path_keys`` (imported by the parser) and
``aqueduct.executor.models`` (imported by the surveyor) can be used
without a Spark installation.

``ExecuteError`` is engine-agnostic (``aqueduct.errors.ExecuteError``) and
is resolved DIRECTLY from there, never via ``aqueduct.executor.spark``.
Every registered engine (Spark, DuckDB, ...) raises this same type, so a
caller can ``from aqueduct.executor import ExecuteError`` to catch any
engine's setup failure without pulling in ``pyspark`` — the bug this
comment guards against: this used to resolve through
``aqueduct.executor.spark.executor``, so a DuckDB-only install crashed with
a bare ``ImportError: No module named pyspark`` on ANY ``aqueduct run``,
regardless of ``deployment.engine``.
"""

from __future__ import annotations


def get_executor(engine: str = "spark"):
    """Return the ``execute()`` function for the requested engine.

    Phase 78 Step 2: resolves through the ``aqueduct.engines`` entry-point
    registry + ``ExecutorProtocol`` (``aqueduct/executor/protocol.py``)
    instead of a hardcoded Spark-only branch — the same fail-closed
    registration seam ``aqueduct.executor.capabilities.get_capabilities()``
    already uses, so a future engine (e.g. DuckDB) needs no edit here.

    Args:
        engine: Execution engine name — must be registered via the
            ``aqueduct.engines`` entry-point group.

    Raises:
        UnknownEngineError: Engine has no registered ``ExecutorProtocol``
            (unknown name, or nothing registered at all — see
            ``aqueduct.executor.protocol.get_protocol``).
    """
    from aqueduct.executor.protocol import get_protocol

    return get_protocol(engine).execute


def __getattr__(name: str):
    if name == "execute":
        from aqueduct.executor.spark.executor import execute

        return execute
    if name == "ExecuteError":
        from aqueduct.errors import ExecuteError

        return ExecuteError
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["execute", "ExecuteError", "get_executor"]
