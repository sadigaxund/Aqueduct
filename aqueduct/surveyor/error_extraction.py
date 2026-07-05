"""Structured Spark/Py4J error extraction for the Surveyor.

Extracted from ``surveyor.py``. ``surveyor.py`` re-imports these names, so
``aqueduct.surveyor.surveyor._extract_structured_error`` (and friends) keep
working for callers and tests.

``pyspark``/``py4j`` are imported lazily INSIDE ``_extract_structured_error`` so
top-level ``import aqueduct.surveyor`` stays ``pyspark``-free — do not promote
those to module-level imports.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

_PY4J_CAUSE_HOP_LIMIT = 10
# Spark 4.0 error-class names we recognise as carrying column-suggestion data.
_COLUMN_SUGGEST_CLASSES = frozenset({
    "UNRESOLVED_COLUMN.WITH_SUGGESTION",
    "UNRESOLVED_FIELD.WITH_SUGGESTION",
    "UNRESOLVED_MAP_KEY.WITH_SUGGESTION",
})


def _parse_suggested_columns(blob: str) -> tuple[str, ...]:
    """Parse Spark 'Did you mean one of the following? [`a`, `b`]' segment.

    Spark's UNRESOLVED_COLUMN.WITH_SUGGESTION message embeds the suggestion
    list as backtick-quoted identifiers separated by commas. Extracting them
    explicitly lets the prompt show "actual columns: …" without asking the
    LLM to parse the trace.
    """
    import re as _re
    if not blob:
        return ()
    out: list[str] = []
    for m in _re.finditer(r"`([^`]+)`", blob):
        name = m.group(1).strip()
        if name and name not in out:
            out.append(name)
    return tuple(out)


def _extract_structured_error(exc: BaseException | None) -> dict[str, Any] | None:
    """Return a dict of structured error fields, or None if extraction fails.

    Best-effort: lazy-imports pyspark/py4j and swallows any failure so that a
    bug in extraction can never block self-heal. Used by Surveyor.record()
    before the exception is stringified into FailureContext.error_message.

    Resolution order:
      1. PySparkException (Spark 4.0): getCondition / getErrorClass +
         getMessageParameters + getSqlState. Highest-fidelity path.
      2. Py4JJavaError: walk .java_exception.getCause() up to
         _PY4J_CAUSE_HOP_LIMIT to find innermost Java throwable.
      3. Python cause chain: traceback.TracebackException root.

    Returns mapping with keys: error_class, root_exception, sql_state,
    suggested_columns, object_name. Any field may be None / empty tuple.
    """
    if exc is None:
        return None
    out: dict[str, Any] = {
        "error_class": None,
        "root_exception": None,
        "sql_state": None,
        "suggested_columns": (),
        "object_name": None,
    }
    try:
        # --- 1. Spark 4.0 PySparkException ---------------------------------
        try:
            from pyspark.errors import PySparkException  # type: ignore
        except Exception:
            PySparkException = None  # type: ignore[assignment]

        spark_exc = None
        if PySparkException is not None:
            cur = exc
            for _ in range(_PY4J_CAUSE_HOP_LIMIT):
                if isinstance(cur, PySparkException):
                    spark_exc = cur
                    break
                cur = getattr(cur, "__cause__", None) or getattr(cur, "__context__", None)
                if cur is None:
                    break

        if spark_exc is not None:
            try:
                if hasattr(spark_exc, "getCondition"):
                    out["error_class"] = spark_exc.getCondition()
                elif hasattr(spark_exc, "getErrorClass"):
                    out["error_class"] = spark_exc.getErrorClass()
            except Exception:
                pass  # structured-error enrichment is best-effort; fall back to raw trace
            try:
                if hasattr(spark_exc, "getSqlState"):
                    out["sql_state"] = spark_exc.getSqlState()
            except Exception:
                pass  # sql-state inspection is diagnostic only; missing sql_state is not an error
            params: dict[str, Any] = {}
            try:
                if hasattr(spark_exc, "getMessageParameters"):
                    raw = spark_exc.getMessageParameters() or {}
                    params = {str(k): str(v) for k, v in dict(raw).items()}
            except Exception:
                params = {}
            if params:
                for k in ("objectName", "fieldName", "tableName", "relationName", "columnName"):
                    if k in params:
                        out["object_name"] = params[k]
                        break
                if out["error_class"] in _COLUMN_SUGGEST_CLASSES:
                    for k in ("proposal", "suggestion", "suggestions"):
                        if k in params:
                            out["suggested_columns"] = _parse_suggested_columns(params[k])
                            break

        # --- 2. Py4JJavaError cause chain ----------------------------------
        if out["error_class"] is None:
            try:
                from py4j.protocol import Py4JJavaError  # type: ignore
            except Exception:
                Py4JJavaError = None  # type: ignore[assignment]

            if Py4JJavaError is not None:
                cur = exc
                for _ in range(_PY4J_CAUSE_HOP_LIMIT):
                    if isinstance(cur, Py4JJavaError):
                        java_exc = getattr(cur, "java_exception", None)
                        if java_exc is not None:
                            try:
                                innermost = java_exc
                                for _ in range(_PY4J_CAUSE_HOP_LIMIT):
                                    nxt = innermost.getCause()
                                    if nxt is None or nxt is innermost:
                                        break
                                    innermost = nxt
                                jclass = innermost.getClass().getName()
                                jmsg = innermost.getMessage() or ""
                                out["error_class"] = out["error_class"] or jclass
                                out["root_exception"] = {"type": jclass, "message": jmsg}
                            except Exception:
                                pass  # getCause() chain walk is best-effort; root cause extraction is diagnostic only
                        break
                    cur = getattr(cur, "__cause__", None) or getattr(cur, "__context__", None)
                    if cur is None:
                        break

        # --- 3. Python cause chain (root fallback) -------------------------
        if out["root_exception"] is None:
            root = exc
            for _ in range(_PY4J_CAUSE_HOP_LIMIT):
                nxt = getattr(root, "__cause__", None) or getattr(root, "__context__", None)
                if nxt is None or nxt is root:
                    break
                root = nxt
            out["root_exception"] = {
                "type": type(root).__name__,
                "message": str(root),
            }
            if out["error_class"] is None:
                out["error_class"] = type(root).__name__

        if not any((out["error_class"], out["root_exception"], out["sql_state"],
                    out["suggested_columns"], out["object_name"])):
            return None
        return out
    except Exception:
        logger.debug("_extract_structured_error failed", exc_info=True)
        return None
