"""Executor layer — runs compiled manifests against a Spark cluster.

``execute`` and ``ExecuteError`` are resolved lazily via ``__getattr__``
so that ``aqueduct.executor.path_keys`` (imported by the parser) and
``aqueduct.executor.models`` (imported by the surveyor) can be used
without a Spark installation.
"""

from __future__ import annotations

_SUPPORTED_ENGINES = ("spark",)


def get_executor(engine: str = "spark"):
    """Return the execute() function for the requested engine.

    Args:
        engine: Execution engine name.  Currently only ``"spark"`` is supported.

    Raises:
        ValueError: Engine is unknown.
    """
    if engine == "spark":
        from aqueduct.executor.spark.executor import execute as spark_execute
        return spark_execute
    raise ValueError(
        f"Unknown execution engine: {engine!r}. "
        f"Supported: {', '.join(_SUPPORTED_ENGINES)}"
    )


def __getattr__(name: str):
    if name == "execute":
        from aqueduct.executor.spark.executor import execute
        return execute
    if name == "ExecuteError":
        from aqueduct.executor.spark.executor import ExecuteError
        return ExecuteError
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["execute", "ExecuteError", "get_executor"]
