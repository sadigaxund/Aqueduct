"""Executor layer — runs compiled manifests against a Spark cluster.

``execute`` and ``ExecuteError`` are resolved lazily via ``__getattr__``
so that ``aqueduct.executor.path_keys`` (imported by the parser) and
``aqueduct.executor.models`` (imported by the surveyor) can be used
without a Spark installation.
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
        from aqueduct.executor.spark.executor import ExecuteError
        return ExecuteError
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["execute", "ExecuteError", "get_executor"]
