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
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any
from pyspark.sql.functions import UserDefinedFunction

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def _ensure_project_root_on_path() -> None:
    """Insert CWD into sys.path[0] so project-relative UDF modules are importable.

    The CLI sets CWD to the project root before execute() is called, but installed
    CLI tools don't get CWD on sys.path automatically — only interactive Python does.
    """
    cwd = str(Path.cwd())
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

# FIXME: this is a TEMPORARY hack until new Spark release supports newer python version
def _patch_pyspark_cloudpickle() -> None:
    """Replace pyspark's bundled cloudpickle 2.x with system cloudpickle 3.x.

    pyspark.serializers does ``from pyspark import cloudpickle`` then calls
    ``cloudpickle.dumps(obj)`` — it holds the module object, so replacing
    attributes on that module affects all subsequent calls in the same process.

    Needed on Python 3.13+ where cloudpickle 2.2.1 (bundled with PySpark 3.5)
    segfaults during function serialization. Install ``cloudpickle>=3.0`` to
    enable. The patch is **defensive** — every failure mode emits a clear
    warning rather than silently leaving UDFs to crash with cryptic recursion
    errors at runtime.

    Failure modes that the warning covers:

    * Python 3.13+ but ``cloudpickle`` not installed system-wide.
    * PySpark version changed the cloudpickle module path (renamed to
      ``cloudpickle_fast``, moved under ``pyspark._cloudpickle``, removed).
    * Bundled module exists but is missing the expected attributes
      (``dumps`` / ``loads`` / ``CloudPickler``) — future PySpark may
      restructure internals.
    """
    if sys.version_info < (3, 13):
        return

    py_label = f"Python {sys.version_info.major}.{sys.version_info.minor}"

    try:
        import cloudpickle as system_cp  # system-installed (pip install cloudpickle)
    except ImportError:
        logger.warning(
            "%s detected but system cloudpickle is not installed. "
            "Python UDFs will fail with infinite recursion / segfault during "
            "serialization. Fix: `pip install cloudpickle` (or run on Python ≤ 3.12).",
            py_label,
        )
        return

    # Locate the bundled module — PySpark has moved this around historically;
    # try every known import path before giving up.
    bundled_cp = None
    bundled_path: str | None = None
    for candidate in ("pyspark.cloudpickle", "pyspark.cloudpickle_fast", "pyspark._cloudpickle"):
        try:
            bundled_cp = importlib.import_module(candidate)
            bundled_path = candidate
            break
        except ImportError:
            continue

    if bundled_cp is None or bundled_path is None:
        logger.warning(
            "%s detected and `pyspark.cloudpickle` is not importable under any "
            "known path (tried: pyspark.cloudpickle, pyspark.cloudpickle_fast, "
            "pyspark._cloudpickle). Skipping the cloudpickle compatibility "
            "patch — Python UDFs may fail with cryptic recursion errors. "
            "Pin pyspark to a version that bundles cloudpickle at the expected "
            "path, or run on Python ≤ 3.12.",
            py_label,
        )
        return

    # Verify the module has the attributes we need to patch. If PySpark
    # restructures internals (e.g. renames `dumps` to `cp_dumps`), do not
    # silently no-op — surface the mismatch so it can be diagnosed.
    required_attrs = ("dumps", "loads", "CloudPickler")
    missing = [a for a in required_attrs if not hasattr(bundled_cp, a)]
    if missing:
        logger.warning(
            "%s detected; `%s` was imported but lacks expected attributes %s. "
            "Aqueduct cannot patch cloudpickle — Python UDFs may fail with "
            "recursion errors. Likely a future PySpark restructured the "
            "module; report this with the installed pyspark version.",
            py_label, bundled_path, missing,
        )
        return

    try:
        system_ver = tuple(int(x) for x in system_cp.__version__.split(".")[:2])
        bundled_ver_str = getattr(bundled_cp, "__version__", "0.0")
        bundled_ver = tuple(int(x) for x in str(bundled_ver_str).split(".")[:2])
    except (AttributeError, ValueError) as exc:
        logger.warning(
            "%s detected; could not compare cloudpickle versions "
            "(system=%r, bundled=%r): %s. Skipping patch.",
            py_label, getattr(system_cp, "__version__", "?"),
            getattr(bundled_cp, "__version__", "?"), exc,
        )
        return

    if system_ver <= bundled_ver:
        return  # bundled is already newer or equal, no patch needed

    # Replace key symbols on the module object — serializers.py holds a reference
    # to the module itself so attribute-level replacement propagates to all callers.
    bundled_cp.dumps = system_cp.dumps
    bundled_cp.loads = system_cp.loads
    bundled_cp.CloudPickler = system_cp.CloudPickler
    logger.info(
        "Patched %s %s.%s → system cloudpickle %s.%s",
        bundled_path, *bundled_ver, *system_ver,
    )


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

    _ensure_project_root_on_path()

    for entry in udf_registry:
        udf_id: str = entry.get("id", "")
        lang: str = entry.get("lang", "python")

        if not udf_id:
            raise UDFError("UDF registry entry missing required 'id' field")

        if lang == "python":
            _register_python_udf(udf_id, entry, spark)
        elif lang in ("java", "scala"):
            _register_java_udf(udf_id, entry, spark)
        else:
            raise UDFError(
                f"UDF {udf_id!r}: language {lang!r} is not supported. "
                "Supported: 'python', 'java', 'scala'."
            )

        logger.info("Registered UDF %r (%s)", udf_id, lang)


def _register_java_udf(
    udf_id: str,
    entry: dict[str, Any],
    spark: SparkSession,
) -> None:
    """Register a Java or Scala UDF from a JAR file.

    Adds the JAR to SparkContext and registers the class as a SQL function.
    No cloudpickle involved — pure JVM bytecode.
    """
    from pyspark.sql.types import _parse_datatype_string

    jar_path: str | None = entry.get("jar") or entry.get("path")
    class_name: str | None = entry.get("entry") or entry.get("class_name")
    return_type_str: str | None = entry.get("return_type", "string")

    if not jar_path:
        raise UDFError(f"UDF {udf_id!r}: 'jar' is required for java/scala UDFs")
    if not class_name:
        raise UDFError(f"UDF {udf_id!r}: 'entry' (fully-qualified class name) is required for java/scala UDFs")

    jar_abs = str(Path(jar_path).resolve())
    if not Path(jar_abs).exists():
        raise UDFError(f"UDF {udf_id!r}: JAR not found at {jar_abs!r}")

    try:
        spark.sparkContext.addJar(jar_abs)
    except Exception as exc:
        raise UDFError(f"UDF {udf_id!r}: failed to add JAR {jar_abs!r}: {exc}") from exc

    try:
        return_type = _parse_datatype_string(return_type_str) if return_type_str else None
        if return_type is not None:
            spark.udf.registerJavaFunction(udf_id, class_name, return_type)
        else:
            spark.udf.registerJavaFunction(udf_id, class_name)
    except Exception as exc:
        raise UDFError(
            f"UDF {udf_id!r}: registerJavaFunction({class_name!r}) failed: {exc}"
        ) from exc


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
        # If fn is already a Spark UDF object (class-based or duck-typed with returnType),
        # don't pass the redundant return_type string.
        if isinstance(fn, UserDefinedFunction) or hasattr(fn, "returnType"):
            spark.udf.register(udf_id, fn)
        else:
            spark.udf.register(udf_id, fn, return_type)
    except Exception as exc:
        msg = str(exc)
        if "serialize" in msg.lower() or "recursion" in msg.lower() or "stack overflow" in msg.lower():
            import sys as _sys
            v = _sys.version_info
            hint = (
                f" Python {v.major}.{v.minor} detected — cloudpickle bundled with PySpark 3.5 "
                "does not support Python 3.14+. Use Python ≤ 3.12, or replace the UDF with a "
                "native Spark SQL expression."
            ) if v >= (3, 14) else ""
            raise UDFError(
                f"UDF {udf_id!r}: spark.udf.register() failed — could not serialize the function.{hint}"
            ) from exc
        raise UDFError(
            f"UDF {udf_id!r}: spark.udf.register() failed: {exc}"
        ) from exc
