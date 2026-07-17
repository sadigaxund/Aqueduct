"""Structured DuckDB error extraction — DuckDB's ``ExecutorProtocol.extract_error``.

Mirrors ``aqueduct.surveyor.error_extraction._extract_structured_error`` (the
Spark/Py4J extractor) field-for-field, but reads DuckDB's own exception
vocabulary instead: ``duckdb.Error`` subclasses (``BinderException``,
``CatalogException``, ``ConstraintException``, ``ConversionException``,
``IOException``, ``ParserException``, ``SyntaxException``,
``TransactionException``, ...) and DuckDB's plain-text "Candidate bindings"
/ "Did you mean" suggestion format, rather than Spark's structured
``getMessageParameters()`` API.

``duckdb`` is imported lazily INSIDE ``extract_duckdb_error`` so this module —
and therefore ``aqueduct.executor.duckdb_.engine``'s ``ExecutorProtocol``
construction — stays importable without ``duckdb`` installed. In practice
``duckdb`` is a base dependency of aqueduct-core (the observability/depot
stores need it), so this is defensive rather than load-bearing, but the same
discipline as the Spark extractor's lazy ``pyspark`` import is kept regardless.
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

_CAUSE_HOP_LIMIT = 10

# "Candidate bindings:" / "Binder Error: ... column" -> the offending name.
_REFERENCED_COLUMN_RE = re.compile(r'[Rr]eferenced column "([^"]+)"')
_TABLE_NOT_FOUND_RE = re.compile(r"Table with name (\S+) does not exist")
_UNKNOWN_ANY_QUOTED_RE = re.compile(r'"([^"]+)" does not exist')
# DuckDB's suggestion segments: `Candidate bindings: "a", "b"` or `Did you mean "x"?`
_CANDIDATE_BINDINGS_RE = re.compile(r"Candidate bindings?:\s*(.+)")
_DID_YOU_MEAN_RE = re.compile(r'Did you mean\s+"([^"]+)"')
_QUOTED_RE = re.compile(r'"([^"]+)"')


def _parse_object_name(message: str) -> str | None:
    for pattern in (_REFERENCED_COLUMN_RE, _TABLE_NOT_FOUND_RE, _UNKNOWN_ANY_QUOTED_RE):
        m = pattern.search(message)
        if m:
            return m.group(1).strip("\"'")
    return None


def _parse_suggested_columns(message: str) -> tuple[str, ...]:
    """Parse DuckDB's 'Candidate bindings: "a", "b"' / 'Did you mean "x"?' segments."""
    out: list[str] = []
    m = _CANDIDATE_BINDINGS_RE.search(message)
    if m:
        for name in _QUOTED_RE.findall(m.group(1)):
            if name and name not in out:
                out.append(name)
    if not out:
        m = _DID_YOU_MEAN_RE.search(message)
        if m:
            out.append(m.group(1))
    return tuple(out)


def extract_duckdb_error(exc: BaseException | None) -> dict[str, Any] | None:
    """Return a dict of structured error fields, or None if extraction fails.

    Best-effort: swallows any internal failure so a bug in extraction can
    never block self-heal. Returns the same field shape as Spark's extractor
    (``error_class``, ``root_exception``, ``sql_state``, ``suggested_columns``,
    ``object_name``) so ``FailureContext`` stays engine-agnostic downstream.

    Resolution order:
      1. A ``duckdb.Error`` anywhere in the ``__cause__``/``__context__`` chain
         (DuckDB's own exception hierarchy — ``BinderException``,
         ``CatalogException``, ``ConstraintException``, ``ConversionException``,
         ``IOException``, ``ParserException``, ``SyntaxException``,
         ``TransactionException``, ...): ``error_class`` = the concrete
         exception class name; ``object_name`` / ``suggested_columns`` parsed
         out of DuckDB's plain-text message (it has no structured
         ``getMessageParameters()`` API the way Spark 4's ``PySparkException``
         does — the message IS the only structured data DuckDB exposes).
      2. Python cause chain fallback: same as Spark's — walk to the root
         exception for ``root_exception``/``error_class`` when no
         ``duckdb.Error`` was found.
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
        try:
            import duckdb as _duckdb  # noqa: PLC0415
        except Exception:
            _duckdb = None  # type: ignore[assignment]

        duck_exc = None
        if _duckdb is not None:
            cur = exc
            for _ in range(_CAUSE_HOP_LIMIT):
                if isinstance(cur, _duckdb.Error):
                    duck_exc = cur
                    break
                cur = getattr(cur, "__cause__", None) or getattr(cur, "__context__", None)
                if cur is None:
                    break

        if duck_exc is not None:
            out["error_class"] = type(duck_exc).__name__
            message = str(duck_exc)
            out["object_name"] = _parse_object_name(message)
            out["suggested_columns"] = _parse_suggested_columns(message)
            out["root_exception"] = {"type": type(duck_exc).__name__, "message": message}
            # DuckDB's Python API does not expose a SQLSTATE code (unlike
            # ODBC/JDBC drivers) — left None deliberately, not a missed field.

        # --- Python cause chain (root fallback) -----------------------------
        if out["root_exception"] is None:
            root = exc
            for _ in range(_CAUSE_HOP_LIMIT):
                nxt = getattr(root, "__cause__", None) or getattr(root, "__context__", None)
                if nxt is None or nxt is root:
                    break
                root = nxt
            out["root_exception"] = {"type": type(root).__name__, "message": str(root)}
            if out["error_class"] is None:
                out["error_class"] = type(root).__name__

        if not any(
            (
                out["error_class"],
                out["root_exception"],
                out["sql_state"],
                out["suggested_columns"],
                out["object_name"],
            )
        ):
            return None
        return out
    except Exception:
        logger.debug("extract_duckdb_error failed", exc_info=True)
        return None


__all__ = ["extract_duckdb_error"]
