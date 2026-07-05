"""`aqueduct studio` — interactive terminal UI (Phase 67).

Split in two so the query logic is testable without a UI or a Spark install:

- ``data.py`` — read-only DuckDB + config query helpers. NO ``textual``, NO
  ``pyspark``. Unit-tested directly.
- ``app.py`` — the ``textual`` application that renders what ``data.py`` returns.
  Imports ``textual`` (an optional ``tui`` extra); only loaded once the CLI has
  confirmed the dependency is installed.

This module's ``__init__`` must stay import-light (no ``textual``) so that
``import aqueduct.tui.data`` works on a base install.
"""
