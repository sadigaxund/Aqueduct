"""UDF registration — registers all UDFs from Manifest with SparkSession.

Called once at blueprint start before any module executes.  Registration
failure raises ExecuteError immediately (a UDF that cannot be loaded makes
any Channel that calls it incorrect).

Supported languages:
  python  — importlib loads the module; spark.udf.register() wires it in.
  sql     — registered as a Spark SQL function via spark.sql("CREATE FUNCTION …").
             Not supported yet; raises ExecuteError with a clear message.
  scala   — not supported; raises ExecuteError.
  java    — not supported; raises ExecuteError.

UDF registry entry shape (from Blueprint / Manifest):
  id:          SQL function name (e.g. "validate_email")
  lang:        "python" | "sql" | "scala" | "java"
  module:      dotted Python module path (python only, e.g. "my_pkg.udfs")
  entry:       callable name within the module (python only, e.g. "validate_email_fn")
  return_type: Spark SQL type string (python only, e.g. "boolean", "StringType()")
"""

from __future__ import annotations

import importlib
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class UDFError(Exception):
    """Raised when UDF registration fails."""


def register_udfs(
    udf_registry: tuple[dict[str, Any], ...],
    spark: SparkSession,
) -> None:
    """Register all UDFs from manifest.udf_registry with SparkSession.

    Raises:
        UDFError: On import failure, missing entry, or unsupported language.
    """
    if not udf_registry:
        return

    for entry in udf_registry:
        udf_id: str = entry.get("id", "")
        lang: str = entry.get("lang", "python")

        if not udf_id:
            raise UDFError("UDF registry entry missing required 'id' field")

        if lang != "python":
            raise UDFError(
                f"UDF {udf_id!r}: language {lang!r} is not supported. "
                "Only 'python' UDFs can be registered in this version."
            )

        _register_python_udf(udf_id, entry, spark)
        logger.info("Registered UDF %r (%s)", udf_id, lang)


def _register_python_udf(
    udf_id: str,
    entry: dict[str, Any],
    spark: SparkSession,
) -> None:
    module_path: str | None = entry.get("module")
    entry_name: str | None = entry.get("entry") or udf_id
    return_type: str | None = entry.get("return_type", "string")

    if not module_path:
        raise UDFError(f"UDF {udf_id!r}: 'module' is required for python UDFs")

    try:
        mod = importlib.import_module(module_path)
    except ImportError as exc:
        raise UDFError(
            f"UDF {udf_id!r}: cannot import module {module_path!r}: {exc}"
        ) from exc

    fn = getattr(mod, entry_name, None)
    if fn is None:
        raise UDFError(
            f"UDF {udf_id!r}: function {entry_name!r} not found in {module_path!r}"
        )

    try:
        spark.udf.register(udf_id, fn, return_type)
    except Exception as exc:
        raise UDFError(
            f"UDF {udf_id!r}: spark.udf.register() failed: {exc}"
        ) from exc
