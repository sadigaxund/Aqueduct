"""``AqueductPatchTrigger`` — async polling for patch approval.

Runs inside the Airflow ``triggerer`` process. Polls
``aqueduct patch list --status all --format json`` until the patch produced
for ``run_id`` lands in ``applied/`` or ``rejected/``, then emits a
``TriggerEvent``. The operator resumes on any worker via
``resume_from_patch``.

Polling uses a subprocess call so the triggerer node only needs the
``aqueduct`` binary on ``$PATH`` — no pyspark / blueprint imports.
"""
from __future__ import annotations

import asyncio
import json
import subprocess
from collections.abc import AsyncIterator
from typing import Any

from airflow.triggers.base import BaseTrigger, TriggerEvent


class AqueductPatchTrigger(BaseTrigger):
    """Async trigger that polls the patch CLI for approval."""

    def __init__(
        self,
        *,
        run_id: str,
        blueprint: str,
        patches_dir: str,
        aqueduct_cmd: list[str] | None = None,
        poll_interval: float = 30.0,
    ) -> None:
        super().__init__()
        self.run_id = run_id
        self.blueprint = blueprint
        self.patches_dir = patches_dir
        self.aqueduct_cmd = aqueduct_cmd or ["aqueduct"]
        self.poll_interval = poll_interval

    # ------------------------------------------------------------------
    # Serialization (Airflow Trigger contract)
    # ------------------------------------------------------------------
    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "aqueduct.integrations.airflow.trigger.AqueductPatchTrigger",
            {
                "run_id": self.run_id,
                "blueprint": self.blueprint,
                "patches_dir": self.patches_dir,
                "aqueduct_cmd": self.aqueduct_cmd,
                "poll_interval": self.poll_interval,
            },
        )

    # ------------------------------------------------------------------
    # Polling loop
    # ------------------------------------------------------------------
    async def run(self) -> AsyncIterator[TriggerEvent]:
        while True:
            status, patch_id, reason = await asyncio.to_thread(self._check_once)
            if status == "approved":
                yield TriggerEvent(
                    {"status": "approved", "patch_id": patch_id, "run_id": self.run_id}
                )
                return
            if status == "rejected":
                yield TriggerEvent(
                    {
                        "status": "rejected",
                        "patch_id": patch_id,
                        "run_id": self.run_id,
                        "reason": reason,
                    }
                )
                return
            await asyncio.sleep(self.poll_interval)

    # ------------------------------------------------------------------
    # Synchronous one-shot status check (called via ``to_thread``)
    # ------------------------------------------------------------------
    def _check_once(self) -> tuple[str, str | None, str | None]:
        """Return ``(status, patch_id, reason)`` for this trigger's ``run_id``.

        ``status`` is one of ``approved`` / ``rejected`` / ``pending``.
        """
        cmd = [
            *self.aqueduct_cmd,
            "patch",
            "list",
            "--status",
            "all",
            "--format",
            "json",
            "--patches-dir",
            self.patches_dir,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            return "pending", None, None
        try:
            payload = json.loads(result.stdout or "[]")
        except json.JSONDecodeError:
            return "pending", None, None

        for entry in payload:
            if not self._matches_run(entry):
                continue
            status_label = entry.get("status")
            if status_label == "applied":
                return "approved", entry.get("patch_id"), None
            if status_label == "rejected":
                return "rejected", entry.get("patch_id"), entry.get("rationale")
        return "pending", None, None

    def _matches_run(self, entry: dict[str, Any]) -> bool:
        """A patch belongs to this run if its ``run_id`` matches.

        Primary match: the CLI's JSON exposes ``run_id`` from the patch's
        ``_aq_meta`` block. Falls back to filename / rationale substring so
        older patches (pre-1.0.1, no ``_aq_meta.run_id`` in JSON output)
        still resolve.
        """
        if not self.run_id:
            return True
        entry_run_id = entry.get("run_id")
        if entry_run_id:
            return entry_run_id == self.run_id
        file_path = entry.get("file") or ""
        rationale = entry.get("rationale") or ""
        return self.run_id in file_path or self.run_id in rationale
