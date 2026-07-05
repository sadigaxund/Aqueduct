"""Execution result models — frozen dataclasses returned by the Executor."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from enum import StrEnum


class ExecutionStatus(StrEnum):
    """Module/blueprint execution outcome. Subclasses ``str`` so stored/serialized
    values (manifests, DDL rows, JSON output) are unchanged — ``ExecutionStatus.SUCCESS
    == "success"`` and ``str(ExecutionStatus.SUCCESS) == "success"``. Comparison sites
    should use the enum members; ``ModuleResult.status`` / ``ExecutionResult.status``
    stay typed ``str`` since callers may still hand in plain strings."""

    SUCCESS = "success"
    ERROR = "error"
    SKIPPED = "skipped"
    PATCHED = "patched"  # run-level effective status (succeeded after an applied patch); never set on a ModuleResult


def concise_error(text: str | None, *, limit: int = 300) -> str:
    """First line of an error string, capped — for **display** only.

    Spark `AnalysisException` messages put the root cause on the first line and
    then dump the whole Catalyst plan tree on the following lines. Logs and the
    CLI status line want only the root cause; the full text is still stored
    verbatim in `ModuleResult.error` (the obs store + the heal agent need it).
    """
    s = (text or "").strip().split("\n", 1)[0].strip()
    return s if len(s) <= limit else s[: limit - 1] + "…"


@dataclass(frozen=True)
class ModuleResult:
    """Outcome of executing a single module."""

    module_id: str
    status: str          # "success" | "error" | "skipped"
    error: str | None = None
    error_type: str | None = None  # user-defined label from Assert rule's error_type field
    exception: BaseException | None = None
    """Live exception object when status == "error" — preserves the original
    PySparkException / Py4JJavaError / `__cause__` chain so
    ``Surveyor.record()`` can hand it to ``_extract_structured_error`` for
    Phase 35 structured fields (error_class, object_name, suggested_columns,
    sql_state, root_exception). Stringified ``error`` alone loses the chain.
    Not serialized to JSON — see ``ExecutionResult.to_dict``."""
    warnings: tuple[tuple[str, str], ...] = ()
    """Per-module runtime warnings collected during execution.
    Each entry is (rule_id, message).  Engine-agnostic — no pyspark."""
    notes: tuple[str, ...] = ()
    """Informational output lines (NOT warnings — never counted in the
    runtime warning roll-up). Today: Probe `report: stdout` signal results,
    printed dim under the module's summary row (capped unless -v)."""


@dataclass(frozen=True)
class ExecutionResult:
    """Aggregate outcome of a full blueprint run."""

    blueprint_id: str
    run_id: str
    status: str                          # "success" | "error"
    module_results: tuple[ModuleResult, ...]
    trigger_agent: bool = False          # LLM loop should fire even if approval=disabled

    def to_dict(self) -> dict:
        return {
            "blueprint_id": self.blueprint_id,
            "run_id": self.run_id,
            "status": self.status,
            "trigger_agent": self.trigger_agent,
            "module_results": [
                {
                    "module_id": r.module_id,
                    "status": r.status,
                    "error": r.error,
                    "error_type": r.error_type,
                    "warnings": [list(w) for w in r.warnings],
                    "notes": list(r.notes),
                }
                for r in self.module_results
            ],
        }


# ── Thread-safe per-module warning collector ──────────────────────────────
# probe / assert code runs inside a module execution context and appends
# (rule_id, message) pairs here.  The executor clears this before each module
# and gathers the accumulated warnings when constructing ModuleResult.
# Using threading.local() keeps parallel-component threads isolated.

_collector = threading.local()


def _add_module_warning(rule_id: str, message: str) -> None:
    """Append a runtime warning to the current module's warning list."""
    if not hasattr(_collector, "warnings"):
        _collector.warnings = []
    _collector.warnings.append((rule_id, message))


def _collect_module_warnings() -> tuple[tuple[str, str], ...]:
    """Gather and reset the per-module warning list."""
    ws: list[tuple[str, str]] = getattr(_collector, "warnings", []) or []
    _collector.warnings = []
    return tuple(ws)
