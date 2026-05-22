"""``AqueductPatchSensor`` — standalone deferrable sensor.

The ``AqueductOperator`` already defers internally when it sees
``HEAL_PENDING``, so most DAGs do not need an explicit sensor. This class
exists for the split-task pattern where the operator runs in ``ci`` mode and
exits cleanly with ``HEAL_PENDING``, and a downstream sensor waits for
approval before triggering a rerun task.
"""
from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator

from .trigger import AqueductPatchTrigger


class AqueductPatchSensor(BaseSensorOperator):
    """Deferrable sensor that waits for a patch to be approved or rejected."""

    template_fields = ("run_id", "blueprint")

    def __init__(
        self,
        *,
        run_id: str,
        blueprint: str,
        patches_dir: str | None = None,
        aqueduct_cmd: list[str] | None = None,
        poll_interval: float = 30.0,
        patch_timeout: float | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.run_id = run_id
        self.blueprint = blueprint
        self.patches_dir = patches_dir
        self.aqueduct_cmd = aqueduct_cmd or ["aqueduct"]
        self.poll_interval = poll_interval
        self.patch_timeout = patch_timeout

    # ------------------------------------------------------------------
    def execute(self, context: dict[str, Any]) -> None:
        patches_dir = self.patches_dir or str(
            Path(self.blueprint).resolve().parent / "patches"
        )
        timeout = (
            timedelta(seconds=self.patch_timeout) if self.patch_timeout is not None else None
        )
        self.defer(
            trigger=AqueductPatchTrigger(
                run_id=self.run_id,
                blueprint=self.blueprint,
                patches_dir=patches_dir,
                aqueduct_cmd=self.aqueduct_cmd,
                poll_interval=self.poll_interval,
            ),
            method_name="resume_from_patch",
            timeout=timeout,
        )

    def resume_from_patch(
        self, context: dict[str, Any], event: dict[str, Any]
    ) -> dict[str, Any]:
        status = event.get("status")
        if status == "approved":
            return event
        if status == "rejected":
            raise AirflowException(
                f"Patch rejected for run_id={self.run_id!r}: "
                f"{event.get('reason') or 'no reason given'}"
            )
        raise AirflowException(
            f"AqueductPatchTrigger fired with unexpected status: {event!r}"
        )
