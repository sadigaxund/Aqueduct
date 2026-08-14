"""``AqueductPatchTrigger`` — async polling for patch approval.

Runs inside the Airflow ``triggerer`` process. Polls
``aqueduct patch list --status all --format json`` until the patch produced
for ``run_id`` lands in ``applied/`` or ``rejected/``, then emits a
``TriggerEvent``. The operator resumes on any worker via
``resume_from_patch``.

Polling uses a subprocess call so the triggerer node only needs the
``aqueduct`` binary on ``$PATH`` — no pyspark / blueprint imports.

``patches_dir`` is an explicit-override-only field. When unset (the common
case), no ``--patches-dir`` flag is sent, so ``aqueduct patch list`` resolves
the CONFIGURED patch store itself (local **or** an object-store backend —
``stores.blob.backend: s3``/``gcs``/``adls``) instead of being forced onto
the legacy local-directory scan, which is empty (and silently, permanently
"pending") whenever the store lives remotely. Passing ``--patches-dir``
unconditionally was Phase 74's original shape and is the exact bug this
docstring update fixes — see ``aqueduct/cli/patch.py::patch_list``'s own
comment ("``--patches-dir`` forces the legacy local scan").
"""

from __future__ import annotations

import asyncio
import json
import logging
import subprocess
from collections.abc import AsyncIterator
from typing import Any

from airflow.triggers.base import BaseTrigger, TriggerEvent

# Module-level logger (not `self.log`): BaseTrigger's LoggingMixin `.log` is a
# real-Airflow-only guarantee this module can't rely on being present in
# every host process the same way BaseOperator's is (see AqueductOperator,
# which does use `self.log` — different base class, different contract).
logger = logging.getLogger(__name__)


class AqueductPatchTrigger(BaseTrigger):
    """Async trigger that polls the patch CLI for approval."""

    def __init__(
        self,
        *,
        run_id: str,
        blueprint: str,
        patches_dir: str | None = None,
        config: str | None = None,
        aqueduct_cmd: list[str] | None = None,
        poll_interval: float = 30.0,
    ) -> None:
        super().__init__()
        self.run_id = run_id
        self.blueprint = blueprint
        self.patches_dir = patches_dir
        self.config = config
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
                "config": self.config,
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
    def _build_command(self) -> list[str]:
        # `--blueprint` anchors the legacy local-scan fallback to the SAME
        # directory the operator resolves by default (`<blueprint-dir>/patches`)
        # — required for the local-backend default to keep working when
        # `--patches-dir` is omitted (see module docstring). For an
        # object-store backend it is read but structurally ignored by
        # `make_patch_store` (only `stores.blob.path` matters there), so it
        # never changes which backend gets used — the one cost is that the
        # blueprint file itself must be reachable from the triggerer node,
        # same filesystem assumption the legacy scan already made for
        # `patches/`.
        cmd = [
            *self.aqueduct_cmd,
            "patch",
            "list",
            "--status",
            "all",
            "--format",
            "json",
            "--blueprint",
            self.blueprint,
        ]
        if self.config:
            cmd += ["--config", self.config]
        if self.patches_dir:
            cmd += ["--patches-dir", self.patches_dir]
        return cmd

    def _check_once(self) -> tuple[str, str | None, str | None]:
        """Return ``(status, patch_id, reason)`` for this trigger's ``run_id``.

        ``status`` is one of ``approved`` / ``rejected`` / ``pending``.
        """
        cmd = self._build_command()
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            logger.warning(
                "aqueduct patch list failed (rc=%s) for run_id=%r: %s",
                result.returncode,
                self.run_id,
                (result.stderr or "").strip()[:2000],
            )
            return "pending", None, None
        try:
            payload = json.loads(result.stdout or "[]")
        except json.JSONDecodeError as exc:
            logger.warning(
                "aqueduct patch list returned unparseable JSON for run_id=%r: %s "
                "(stdout head: %r)",
                self.run_id,
                exc,
                (result.stdout or "")[:200],
            )
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

        An empty/unset ``run_id`` is a construction error, not "match
        anything" — a trigger built without one would otherwise approve the
        FIRST applied patch for ANY run sharing the store.
        """
        if not self.run_id:
            return False
        entry_run_id = entry.get("run_id")
        if entry_run_id:
            return entry_run_id == self.run_id
        file_path = entry.get("file") or ""
        rationale = entry.get("rationale") or ""
        return self.run_id in file_path or self.run_id in rationale
