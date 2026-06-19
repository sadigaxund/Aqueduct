"""Synthetic FailureContext for predicted (drift) failures — pure, no pyspark.

A breaking schema drift is turned into a FailureContext that *looks like* a real
failure to the agent + gate pipeline, so the existing heal machinery can run
unchanged. The object is built in memory and driven through the agent; it is
**not** persisted to the ``failure_contexts`` table (which means "a real run
failed") — the drift audit lands in ``drift_checks`` instead.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from aqueduct.drift.classifier import DriftResult
from aqueduct.surveyor.models import FailureContext

#: error_class marker so post-mortem can tell a predicted heal from a real one.
PREDICTED_DRIFT_ERROR_CLASS = "PREDICTED_SCHEMA_DRIFT"


def build_synthetic_failure_context(
    blueprint_id: str,
    module_id: str,
    drift: DriftResult,
    manifest_json: str,
) -> FailureContext:
    """Construct an in-memory FailureContext describing a predicted drift failure.

    The dropped column is surfaced as ``object_name`` and the added columns as
    ``suggested_columns`` — so a rename (drop ``amount`` + add ``amount_usd``)
    gives the agent both the missing name and the rename candidates.
    """
    now = datetime.now(tz=timezone.utc).isoformat()
    breaking = drift.breaking
    summary = "; ".join(c.describe() for c in breaking)
    message = (
        f"Predicted schema drift on Ingress {module_id!r}: {summary}. "
        f"Downstream modules referencing the changed column(s) will fail on the next run."
    )
    object_name = drift.dropped_columns[0] if drift.dropped_columns else None

    return FailureContext(
        run_id=f"drift-{uuid.uuid4().hex[:8]}",
        blueprint_id=blueprint_id,
        failed_module=module_id,
        error_message=message,
        stack_trace=None,
        manifest_json=manifest_json,
        started_at=now,
        finished_at=now,
        error_class=PREDICTED_DRIFT_ERROR_CLASS,
        object_name=object_name,
        suggested_columns=drift.added_columns,
    )
