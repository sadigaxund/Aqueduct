"""Custom Python DataSource support (Phase 61) — Spark 4.0+.

Lets a Blueprint read from / write to a user-supplied
``pyspark.sql.datasource.DataSource`` subclass via ``format: custom`` + a
fully-qualified ``class:`` pointer. The class is imported, validated as a
``DataSource`` subclass, registered with the active session, and then used by its
own ``name()``.

Same trust/precedent as UDFs and custom probe plugins: the Blueprint carries a
*pointer* to importable code, never an inline body.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

from aqueduct.errors import ConfigError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def import_datasource_class(class_path: str):
    """Import ``module.Class`` and verify it subclasses ``DataSource``.

    Raises:
        ValueError: not fully qualified, not found, or not a DataSource subclass.
        ModuleNotFoundError: pyspark (or the pointed-at module) is not installed.
    """
    from pyspark.sql.datasource import DataSource

    if "." not in class_path:
        raise ConfigError(
            f"custom DataSource 'class' must be fully qualified (module.Class), got {class_path!r}"
        )
    module_name, _, cls_name = class_path.rpartition(".")
    mod = importlib.import_module(module_name)
    cls = getattr(mod, cls_name, None)
    if cls is None:
        raise ConfigError(f"custom DataSource class {class_path!r} not found")
    if not (isinstance(cls, type) and issubclass(cls, DataSource)):
        raise ConfigError(
            f"custom DataSource {class_path!r} must subclass pyspark.sql.datasource.DataSource"
        )
    return cls


def register_custom_source(spark: SparkSession, class_path: str) -> str:
    """Import, validate, and register a custom DataSource. Returns its format name.

    Raises:
        RuntimeError: the running Spark is < 4.0 (no ``spark.dataSource`` registry).
    """
    if not hasattr(spark, "dataSource"):
        raise ConfigError(
            "custom Python DataSource requires Spark 4.0+ "
            "(spark.dataSource registry is unavailable on this Spark)"
        )
    cls = import_datasource_class(class_path)
    spark.dataSource.register(cls)
    return cls.name()
