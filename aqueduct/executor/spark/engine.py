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
from aqueduct.executor.protocol import ExecutorProtocol, SessionSpec, register_protocol
from aqueduct.executor.spark.prompt_rules import SPARK_PROMPT_RULES
from aqueduct.executor.spark.type_render import render_spark_type

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


def _make_session(spec: SessionSpec) -> Any:
    """``ExecutorProtocol.make_session`` for Spark — lazy ``pyspark`` import.

    Thin translation from the engine-agnostic ``SessionSpec`` to the existing
    ``make_spark_session(...)`` call the CLI used to make inline. Behaviour is
    unchanged: the CLI's old
    ``make_spark_session(manifest.blueprint_id, merged_spark_config,
    master_url=master_url, quiet_startup=not verbose)`` maps field-for-field
    onto the spec. The lazy import (inside the body) keeps this module —
    and the ``ExecutorProtocol`` object below — importable without ``pyspark``.
    ``make_spark_session`` is imported by name so a test patching
    ``aqueduct.executor.spark.session.make_spark_session`` still intercepts it.
    """
    from aqueduct.executor.spark.session import make_spark_session

    return make_spark_session(
        spec.blueprint_id,
        spec.engine_config,
        master_url=spec.master_url or "local[*]",
        quiet=spec.quiet,
        quiet_startup=spec.quiet_startup,
    )


def _close_session(session: Any) -> None:
    """``ExecutorProtocol.close_session`` for Spark.

    Preserves the CLI's prior teardown exactly: it registered ``session.stop``
    with ``atexit``, a raw stop (NOT ``stop_spark_session``'s ``AQ_TESTING``
    guard, which belongs to short-lived commands like ``doctor``/``test`` that
    may share the fixture session). Keeping the raw stop here means the
    ``aqueduct run`` teardown is byte-for-byte what it was.
    """
    session.stop()


def _read_source_schema(module: Any, session: Any) -> dict[str, str]:
    """``ExecutorProtocol.read_source_schema`` for Spark — lazy ``pyspark`` import.

    Thin pass-through to ``aqueduct.executor.spark.ingress.read_source_schema``
    — a refactor (naming the existing reader as the engine's contribution
    through this seam), not new behavior. Previously the healing agent's
    ToolBox (``aqueduct/agent/toolbox.py``) imported this function by name
    directly, so a heal on a non-Spark engine still read source schemas
    through Spark; routing it through the protocol closes that coupling.
    """
    from aqueduct.executor.spark.ingress import read_source_schema

    return read_source_schema(module, session)


def _sample_source_rows(
    module: Any, session: Any, n: int = 10, base_dir: str | None = None,
) -> list[dict[str, Any]]:
    """``ExecutorProtocol.sample_source_rows`` for Spark — lazy ``pyspark`` import.

    Same bounded-sample contract the ToolBox's ``sample_rows`` tool used to
    implement inline against ``read_ingress`` directly (``limit(n).collect()``
    — a LIMIT push-down, never a full scan, so this is exempt from the
    ``block_full_actions`` gate the same way ``executor/spark/probe.py``'s
    ``sample_rows`` signal is). Moved here so the ToolBox no longer imports
    ``aqueduct.executor.spark.ingress`` by name.
    """
    from aqueduct.executor.spark.ingress import read_ingress

    df = read_ingress(module, session, base_dir=base_dir)
    rows = df.limit(int(n)).collect()
    return [row.asDict(recursive=True) for row in rows]


SPARK = ExecutorProtocol(
    engine="spark",
    execute=_execute,
    extract_error=_extract_error,
    prompt_rules=SPARK_PROMPT_RULES,
    make_session=_make_session,
    close_session=_close_session,
    read_source_schema=_read_source_schema,
    sample_source_rows=_sample_source_rows,
    render_type=render_spark_type,
    # execute_kwargs=None (the default) — Spark's real execute() has a
    # parameter for every OPTIONAL_EXECUTE_KWARGS name
    # (aqueduct/executor/protocol.py), so call_execute() applies no
    # filtering/warning here; every kwarg a caller passes goes straight
    # through to the real function.
)
register_protocol(SPARK)


__all__ = ["load_executor", "SPARK"]
