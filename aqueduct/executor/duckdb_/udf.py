"""UDF registration — registers Python UDFs from Manifest with a DuckDB connection.

Called once at blueprint start before any module executes — same placement as
``aqueduct/executor/spark/udf.py::register_udfs``. Registration failure raises
``UDFError`` immediately (a UDF that cannot be loaded makes any Channel that
calls it incorrect).

Supported languages this stage:
  python  — ``aqueduct.infra.module_loading.load_module`` loads the module;
            ``conn.create_function()`` wires it into the DuckDB catalog.
  java / scala — UNSUPPORTED (``feature.java_udf``) — DuckDB is not on the
            JVM; there is no JAR-loading, bytecode-execution runtime to
            register a Java/Scala UDF against. Raises a clear ``UDFError``
            rather than silently skipping.

Why ``conn.create_function()`` is a safe, portable mirror of Spark's Python
UDF semantics (measured, not assumed — see the DuckDB Python API docs and
this module's own tests):

  * ``type="native"`` (the default — never overridden here) calls the
    Python function ROW-AT-A-TIME, one call per input row. The vectorised
    Arrow-batch mode (``type="arrow"``) is opt-in and deliberately never
    used: it would hand the function a whole batch object instead of one
    value per call, breaking every UDF body written against Spark's
    row-at-a-time contract.
  * ``null_handling="special"`` is REQUIRED (not the default): DuckDB's
    default null handling short-circuits a NULL argument to a NULL result
    WITHOUT calling the Python function at all — measured behavior, not
    documentation. Spark's Python UDFs always invoke the function, NULL
    argument or not, and let the function body decide (this is exactly
    the ``mask_email(email: str | None)`` pattern gallery snippet
    ``05_channel_sql_udf`` exercises). ``special`` restores that: the
    function is called on every row, including NULL ones.
  * Exception propagation is DuckDB's default (``exception_handling=
    "default"``) — an uncaught Python exception aborts the query, same as
    an uncaught exception in a Spark Python UDF aborts the Spark action.
    Aqueduct's own Spillway pattern (try/except inside the UDF body,
    AGENTS.md) is how a Blueprint author opts out of that on either engine
    — this module does not swallow anything on their behalf.
  * ``parameters=None`` (deliberate — the Blueprint grammar has no
    per-parameter type declaration for a Python UDF, only ``return_type``;
    Spark's ``spark.udf.register`` has the exact same shape, duck-typed
    arguments, one declared return type) lets DuckDB accept untyped
    positional arguments without requiring Python type hints on the UDF
    body, matching the un-hinted style every existing gallery UDF (e.g.
    ``logic.py::mask_email``) is already written in.

``deterministic`` (default ``true``) maps onto DuckDB's ``side_effects``
(inverted: ``side_effects = not deterministic``) so a UDF marked
``deterministic: false`` is not incorrectly constant-folded/cached by
DuckDB's optimizer — the engine-native equivalent of Spark's own
``UserDefinedFunction.asNondeterministic()`` (``spark/udf.py``, Pass G1;
before that pass Spark's ``udf.py`` did not read this field at all, so the
same Blueprint field behaved differently per engine — worse than uniformly
dead. Both engines now honour it for real.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from aqueduct.errors import AqueductError

if TYPE_CHECKING:
    import duckdb

logger = logging.getLogger(__name__)


class UDFError(AqueductError):
    """Raised when UDF registration fails."""


def _render_udf_return_type(udf_id: str, return_type_str: str) -> str:
    """Render a UDF ``return_type`` spelling through the Arrow type hub
    (same seam as ``duckdb_/ingress.py``'s ``schema_hint``/
    ``duckdb_/assert_.py``'s ``schema_match``) to DuckDB's own SQL type
    spelling — the ONE type-mapping path, never a second hand-rolled one.
    """
    from aqueduct.errors import EnginePluginError
    from aqueduct.executor.duckdb_.type_render import normalize_type_spelling

    try:
        return normalize_type_spelling(return_type_str)
    except EnginePluginError as exc:
        raise UDFError(f"UDF {udf_id!r}: return_type {return_type_str!r}: {exc}") from exc


def _apply_udf_params(
    udf_id: str,
    entry_name: str,
    module_path: str,
    fn: Any,
    params: dict[str, Any],
) -> Any:
    """Resolve a parameterized UDF: invoke ``fn`` as a factory with ``params``.

    Pure Python — mirrors ``aqueduct/executor/spark/udf.py::_apply_udf_params``
    exactly (duplicated by hand rather than imported across the Spark/DuckDB
    package boundary, same precedent as ``duckdb_/assert_.py``'s module
    docstring "Layer note"). When ``params`` is empty, ``fn`` is returned
    untouched (back-compat with static UDFs).
    """
    if not params:
        return fn
    if not callable(fn):
        raise UDFError(
            f"UDF {udf_id!r}: 'params' was given but {entry_name!r} in "
            f"{module_path!r} is not callable — it must be a factory "
            f"`{entry_name}(**params) -> callable`."
        )
    try:
        produced = fn(**params)
    except Exception as exc:
        raise UDFError(f"UDF {udf_id!r}: factory {entry_name}(**params) raised: {exc}") from exc
    if not callable(produced):
        raise UDFError(
            f"UDF {udf_id!r}: factory {entry_name!r} must return a callable, "
            f"got {type(produced).__name__}."
        )
    return produced


def register_udfs(
    udf_registry: tuple[dict[str, Any], ...],
    con: duckdb.DuckDBPyConnection,
    base_dir: str | None = None,
) -> None:
    """Register all Python UDFs from manifest.udf_registry with a DuckDB connection.

    ``base_dir`` (Manifest.base_dir) lets a python UDF's ``module:`` resolve a
    sibling .py file next to the blueprint — see
    ``aqueduct/infra/module_loading.py``.

    Raises:
        UDFError: On import failure, missing entry, or unsupported language
                  (java/scala — see ``feature.java_udf``).
    """
    if not udf_registry:
        return

    for entry in udf_registry:
        udf_id: str = entry.get("id", "")
        lang: str = entry.get("lang", "python")

        if not udf_id:
            raise UDFError("UDF registry entry missing required 'id' field")

        if lang == "python":
            _register_python_udf(udf_id, entry, con, base_dir=base_dir)
        elif lang in ("java", "scala"):
            raise UDFError(
                f"UDF {udf_id!r}: lang={lang!r} is not supported on the DuckDB "
                "engine — DuckDB is not on the JVM, so there is no JAR-loading, "
                "bytecode-execution runtime to register a Java/Scala UDF "
                "against. See feature.java_udf in docs/compatibility.md."
            )
        else:
            raise UDFError(
                f"UDF {udf_id!r}: language {lang!r} is not supported. "
                "Supported on this engine: 'python'."
            )

        logger.info("Registered UDF %r (%s)", udf_id, lang)


def _register_python_udf(
    udf_id: str,
    entry: dict[str, Any],
    con: duckdb.DuckDBPyConnection,
    base_dir: str | None = None,
) -> None:
    module_path: str | None = entry.get("module")
    entry_name: str | None = entry.get("entry") or udf_id
    return_type_str: str | None = entry.get("return_type", "string")
    return_type = _render_udf_return_type(udf_id, return_type_str) if return_type_str else "VARCHAR"

    if not module_path:
        raise UDFError(f"UDF {udf_id!r}: 'module' is required for python UDFs")

    from aqueduct.infra.module_loading import load_module

    try:
        mod = load_module(module_path, base_dir)
    except ImportError as exc:
        raise UDFError(f"UDF {udf_id!r}: cannot import module {module_path!r}: {exc}") from exc

    fn = getattr(mod, entry_name, None)
    if fn is None:
        raise UDFError(f"UDF {udf_id!r}: function {entry_name!r} not found in {module_path!r}")

    # Parameterized UDF: `entry` is a factory — call it with the resolved params
    # to obtain the actual callable to register. Params are already
    # context/secret-resolved by the compiler before they reach here.
    fn = _apply_udf_params(udf_id, entry_name, module_path, fn, entry.get("params") or {})

    # `deterministic` (default True) inverts onto DuckDB's `side_effects` —
    # see module docstring's "What is honestly NOT honoured" section for why
    # this is a strict improvement over Spark's own non-consumption of the
    # field, not a divergence from real Spark behavior.
    deterministic = bool(entry.get("deterministic", True))

    try:
        con.create_function(
            udf_id,
            fn,
            None,  # parameters — see module docstring; no per-arg type in the grammar
            return_type,
            null_handling="special",
            side_effects=not deterministic,
        )
    except Exception as exc:
        raise UDFError(f"UDF {udf_id!r}: con.create_function() failed: {exc}") from exc


__all__ = ["UDFError", "register_udfs"]
