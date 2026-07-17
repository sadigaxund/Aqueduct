"""DuckDB engine registration — the ``aqueduct.engines`` entry point target.

Mirrors ``aqueduct/executor/spark/engine.py``. This module is what
``[project.entry-points."aqueduct.engines"]``'s ``duckdb = ...`` line
resolves to (see ``pyproject.toml``). ``aqueduct.executor.capabilities.load_engines()``
imports it once per process; importing it registers, as import side effects:

  - DuckDB's capability declaration (``aqueduct.executor.duckdb_.capabilities.DUCKDB``)
    into ``CAPABILITY_REGISTRY``.
  - DuckDB's ``ExecutorProtocol`` into ``aqueduct.executor.protocol.PROTOCOL_REGISTRY``.

``duckdb`` is a base dependency of aqueduct-core (the observability/depot
stores need it — see ``aqueduct/stores/duckdb_``), so unlike Spark's module
this one does not strictly need lazy imports to stay installable. The lazy
imports below are kept anyway, for the same reason Spark's are: constructing
the ``ExecutorProtocol`` object at ``load_engines()`` time must never require
opening an actual DuckDB connection, and consistency with the sibling engine
module makes the seam easier to read.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

# Side effect: registers "duckdb" into aqueduct.executor.capabilities.CAPABILITY_REGISTRY.
import aqueduct.executor.duckdb_.capabilities  # noqa: F401
from aqueduct.executor.duckdb_.prompt_rules import DUCKDB_PROMPT_RULES
from aqueduct.executor.protocol import ExecutorProtocol, SessionSpec, register_protocol

if TYPE_CHECKING:
    from aqueduct.executor.models import ExecutionResult

# Execution kwargs the DuckDB engine's `execute()` genuinely accepts. The
# shared run path (`aqueduct/cli/run.py`) calls the protocol's `execute` with a
# superset that also carries Spark-only run options — `parallel`, `use_observe`,
# `sampling`, `observability_store`, `explain_capture`. Those are the "clearly
# optional capabilities an engine may ignore" half of the ExecutorProtocol
# contract: DuckDB does not implement them (see capabilities.yml —
# `feature.parallel_mode`, `config.metrics.use_observe`, Probe sampling, and the
# Spark explain-gate are all UNSUPPORTED/unimplemented this stage), so the
# adapter DROPS them here rather than forwarding them into
# `duckdb_.executor.execute`, whose signature never mentions them. The engine's
# real execute() is therefore never handed an option it cannot honour.
_DUCKDB_EXECUTE_KWARGS: frozenset[str] = frozenset({
    "run_id", "store_dir", "checkpoint_root", "surveyor", "depot",
    "resume_run_id", "from_module", "to_module", "block_full_actions",
    "warnings_suppress", "warnings_silence_all",
})


def load_executor() -> Callable:
    """Return the DuckDB engine's ``execute()`` function.

    Equivalent to ``get_protocol("duckdb").execute``. Imports
    ``aqueduct.executor.duckdb_.executor`` lazily, only when actually called.
    """
    from aqueduct.executor.duckdb_.executor import execute

    return execute


def _execute(*args: Any, **kwargs: Any) -> ExecutionResult:
    """``ExecutorProtocol.execute`` for DuckDB — filters Spark-only run options.

    Positional args (``manifest``, the DuckDB connection) pass through; keyword
    run options are filtered to ``_DUCKDB_EXECUTE_KWARGS`` so a Spark-only
    option the shared CLI passes (``parallel``/``use_observe``/``sampling``/…)
    is dropped rather than raising ``TypeError`` from the real execute. Lazy
    import keeps this module importable without ``duckdb`` at module load.
    """
    from aqueduct.executor.duckdb_.executor import execute

    filtered = {k: v for k, v in kwargs.items() if k in _DUCKDB_EXECUTE_KWARGS}
    return execute(*args, **filtered)


def _extract_error(exc: BaseException | None) -> dict[str, Any] | None:
    """``ExecutorProtocol.extract_error`` for DuckDB."""
    from aqueduct.executor.duckdb_.error_extraction import extract_duckdb_error

    return extract_duckdb_error(exc)


def _make_session(spec: SessionSpec) -> Any:
    """``ExecutorProtocol.make_session`` for DuckDB — an in-memory connection.

    Stage A always opens a fresh ``:memory:`` connection (see the engine's
    capability table: ``config.spark_config``/``master_url`` are
    ``ignored_with_warning`` and there is no ``duckdb_config`` knob yet). The
    ``SessionSpec``'s ``engine_config``/``master_url``/``quiet*`` fields are
    accepted for cross-engine parity and ignored here. ``duckdb`` is imported
    inside the body so constructing the protocol object never requires it.
    """
    import duckdb

    return duckdb.connect(":memory:")


def _close_session(session: Any) -> None:
    """``ExecutorProtocol.close_session`` for DuckDB — close the connection."""
    session.close()


DUCKDB = ExecutorProtocol(
    engine="duckdb",
    execute=_execute,
    extract_error=_extract_error,
    prompt_rules=DUCKDB_PROMPT_RULES,
    make_session=_make_session,
    close_session=_close_session,
)
register_protocol(DUCKDB)


__all__ = ["load_executor", "DUCKDB"]
