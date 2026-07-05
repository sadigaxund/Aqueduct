"""``execute``, ``ExecuteError``, ``AssertError`` are resolved lazily via
``__getattr__`` so that importing any OTHER submodule of this package
(``aqueduct.executor.spark.udf``, ``.probe``, ``.error_columns``, ...) does
not unconditionally require pyspark to be installed — matching the parent
package's (``aqueduct/executor/__init__.py``) already-lazy ``get_executor``
factory. Only actually touching one of these three names pulls in
``executor.py`` (a real pyspark dependency)."""

from __future__ import annotations

__all__ = ["execute", "ExecuteError", "AssertError"]


def __getattr__(name: str):
    if name == "AssertError":
        from aqueduct.executor.spark.assert_ import AssertError
        return AssertError
    if name in ("execute", "ExecuteError"):
        from aqueduct.executor.spark.executor import ExecuteError, execute
        return {"execute": execute, "ExecuteError": ExecuteError}[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
