"""Per-connection registry of run-scoped temp tables created by Channel/Funnel.

``channel.py::_run_sql`` and ``funnel.py``'s union/coalesce/zip path both
materialize their result into a uniquely-named ``CREATE TEMP TABLE`` (see the
long docstrings there for why: ``con.register()`` is a mutable catalog
binding, not a value capture, so the result of a multi-input SQL/Funnel op
must be a real table before the caller can safely consume it later). Those
temp tables were never dropped, so a long multi-module run accumulates one
per Channel/Funnel module execution on the connection for the life of the
connection.

This module tracks the names created for a given connection and drops them
in one best-effort pass once the caller (``executor.py::execute()``) is done
with them for the run. It is deliberately dumb: a name -> set-of-names map
keyed by ``id(con)`` would leak if a connection object were garbage
collected without ever calling ``drop_tracked_temp_tables`` again, so this
uses a ``WeakKeyDictionary`` keyed by the connection object itself — the
registry entry is collected automatically when the connection is, with no
explicit close-time bookkeeping required.

Safe to call from anywhere: never raises, and a no-op if nothing was ever
tracked for a given connection.
"""

from __future__ import annotations

import weakref
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

_registry: weakref.WeakKeyDictionary[object, set[str]] = weakref.WeakKeyDictionary()


def track_temp_table(con: duckdb.DuckDBPyConnection, name: str) -> None:
    """Record that temp table ``name`` was created on ``con``.

    Call this right after a successful ``CREATE TEMP TABLE`` — never before,
    so a failed CREATE never registers a name that does not exist.
    """
    names = _registry.get(con)
    if names is None:
        names = set()
        _registry[con] = names
    names.add(name)


def drop_tracked_temp_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Drop every temp table tracked for ``con`` and clear its tracked set.

    Best-effort: a table already gone (e.g. connection reset) or any other
    per-table failure is swallowed, matching the existing
    ``except Exception: pass  # best-effort cleanup`` style used throughout
    the DuckDB executor for non-essential teardown. Never raises. A no-op
    when nothing was ever tracked for ``con``.
    """
    names = _registry.pop(con, None)
    if not names:
        return
    for name in names:
        try:
            con.execute(f'DROP TABLE IF EXISTS "{name}"')
        except Exception:
            pass  # best-effort cleanup — tracked temp tables must never block run cleanup
