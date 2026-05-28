"""Surveyor data models — frozen dataclasses for run records and failure context.

FailureContext is the primary artifact consumed by the LLM agent in later phases.
It is serialised to JSON and embedded in the DuckDB observability store.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class RunRecord:
    """Persisted record of a single blueprint run."""

    run_id: str
    blueprint_id: str
    status: str                     # "running" | "success" | "error"
    started_at: str                 # ISO-8601 UTC
    finished_at: str | None         # None while running
    module_results: tuple[dict, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "blueprint_id": self.blueprint_id,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "module_results": list(self.module_results),
        }


@dataclass(frozen=True)
class FailureContext:
    """Structured failure payload — logged to DuckDB and sent to webhook / LLM.

    All fields required by the LLM in later phases are included here so that
    the context document is self-contained and can be passed without additional
    Manifest lookups.
    """

    run_id: str
    blueprint_id: str
    failed_module: str          # module_id of the first failing module, or "_executor"
    error_message: str
    stack_trace: str | None
    manifest_json: str          # JSON-serialised Manifest.to_dict()
    started_at: str             # ISO-8601 UTC
    finished_at: str            # ISO-8601 UTC
    provenance_json: str | None = None        # JSON-serialized ProvenanceMap slice for failed module + context
    blueprint_source_yaml: str | None = None  # raw uncompiled YAML text of the blueprint file
    doctor_hints: tuple[str, ...] = field(default_factory=tuple)  # warn/fail results from check_blueprint_sources
    error_type: str | None = None             # user-defined label from Assert rule's error_type field; None for infra errors
    # Structured Spark/Py4J error extraction. Populated by
    # surveyor._extract_structured_error when the raised exception carries
    # Spark 4.0 condition metadata. All optional; legacy callers see None.
    error_class: str | None = None            # Spark condition name (e.g. UNRESOLVED_COLUMN.WITH_SUGGESTION) or innermost exception type
    root_exception: dict[str, Any] | None = None  # {"type": str, "message": str} — innermost Python or Java throwable
    sql_state: str | None = None              # Spark getSqlState() when available
    suggested_columns: tuple[str, ...] = field(default_factory=tuple)  # extracted "did you mean" suggestions
    object_name: str | None = None            # offending column/table/relation name from messageParameters

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "blueprint_id": self.blueprint_id,
            "failed_module": self.failed_module,
            "error_message": self.error_message,
            "stack_trace": self.stack_trace,
            "manifest_json": self.manifest_json,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "provenance_json": self.provenance_json,
            "blueprint_source_yaml": self.blueprint_source_yaml,
            "doctor_hints": list(self.doctor_hints),
            "error_type": self.error_type,
            "error_class": self.error_class,
            "root_exception": self.root_exception,
            "sql_state": self.sql_state,
            "suggested_columns": list(self.suggested_columns),
            "object_name": self.object_name,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)
