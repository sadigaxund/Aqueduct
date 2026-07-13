"""Spark engine registration — the ``aqueduct.engines`` entry point target.

Phase 78 Step 1 wired the registration seam; Phase 78 Step 2 fills in the
``ExecutorProtocol`` (``aqueduct/executor/protocol.py``). This module is what
``[project.entry-points."aqueduct.engines"]``'s ``spark = ...`` line resolves
to (see ``pyproject.toml``). ``aqueduct.executor.capabilities.load_engines()``
imports it once per process; importing it registers, as import side effects:

  - Spark's capability declaration (``aqueduct.executor.spark.capabilities.SPARK``)
    into ``CAPABILITY_REGISTRY`` (Step 1, unchanged).
  - Spark's ``ExecutorProtocol`` into ``aqueduct.executor.protocol.PROTOCOL_REGISTRY``
    (Step 2, this module).

Must stay importable without ``pyspark`` installed — same constraint as
``aqueduct/executor/spark/capabilities.py`` — because ``load_engines()`` runs
eagerly at compile time and at ``aqueduct.yml`` load time, long before any
Spark session exists. ``execute``/``extract_error`` below are thin wrappers
that defer their real (``pyspark``-importing) implementation to call time;
constructing the ``ExecutorProtocol`` object itself never touches ``pyspark``.

The dependency direction is one-way: this module imports from the executor's
protocol seam and from ``aqueduct.surveyor`` (lazily, inside
``_extract_error``), and imports NOTHING from ``aqueduct.agent``. Spark's
healing prompt-rules pack lives beside this file
(``aqueduct/executor/spark/prompt_rules.py``); the agent layer composes it in
by pulling it through ``ExecutorProtocol.prompt_rules``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

# Side effect: registers "spark" into aqueduct.executor.capabilities.CAPABILITY_REGISTRY.
# Pure data (pyspark-free) — see that module's docstring.
import aqueduct.executor.spark.capabilities  # noqa: F401
from aqueduct.executor.protocol import ExecutorProtocol, register_protocol
from aqueduct.executor.spark.prompt_rules import SPARK_PROMPT_RULES

if TYPE_CHECKING:
    from aqueduct.executor.models import ExecutionResult


def load_executor() -> Callable:
    """Return the Spark engine's ``execute()`` function.

    Kept for backward compatibility with Step 1 callers; equivalent to
    ``get_protocol("spark").execute``. Imports
    ``aqueduct.executor.spark.executor`` (and therefore ``pyspark``) lazily,
    only when actually called.
    """
    from aqueduct.executor.spark.executor import execute

    return execute


def _execute(*args: Any, **kwargs: Any) -> ExecutionResult:
    """``ExecutorProtocol.execute`` for Spark — lazy ``pyspark`` import.

    Thin pass-through to ``aqueduct.executor.spark.executor.execute``: same
    signature, same behavior, zero change. The lazy import (inside the
    function body, not at module scope) is what lets this module — and
    therefore the ``ExecutorProtocol`` object built below — stay importable
    without ``pyspark`` installed.
    """
    from aqueduct.executor.spark.executor import execute

    return execute(*args, **kwargs)


def _extract_error(exc: BaseException | None) -> dict[str, Any] | None:
    """``ExecutorProtocol.extract_error`` for Spark.

    Delegates to the existing
    ``aqueduct.surveyor.error_extraction._extract_structured_error`` — this is
    a refactor (naming today's Spark/Py4J error extraction as the engine's
    contribution through the new seam), not new behavior. That function
    already lazily imports ``pyspark.errors``/``py4j`` inside its own body, so
    this wrapper is safe to reference at module scope: importing
    ``aqueduct.surveyor.error_extraction`` does not require ``pyspark``.
    """
    from aqueduct.surveyor.error_extraction import _extract_structured_error

    return _extract_structured_error(exc)


SPARK = ExecutorProtocol(
    engine="spark",
    execute=_execute,
    extract_error=_extract_error,
    prompt_rules=SPARK_PROMPT_RULES,
)
register_protocol(SPARK)


__all__ = ["load_executor", "SPARK"]
