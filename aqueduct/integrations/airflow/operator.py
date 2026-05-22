"""``AqueductOperator`` — runs a blueprint via the ``aqueduct`` CLI.

The operator is a thin wrapper over ``subprocess.run`` that:

1. Invokes ``aqueduct run <blueprint> --run-id <id>``.
2. Maps the CLI's stable exit codes onto Airflow outcomes:

   - ``0``  SUCCESS         → task success, XCom push.
   - ``2``  DATA_OR_RUNTIME → ``AirflowException`` (Airflow retry policy applies).
   - ``3``  HEAL_PENDING    → ``self.defer(...)`` to the patch trigger.
   - anything else          → ``AirflowException``.

Airflow is imported at module top — the package-level ``__getattr__`` in
``aqueduct.integrations.airflow.__init__`` only loads this submodule when
``AqueductOperator`` is explicitly requested, so the engine itself can be
imported without Airflow installed.

The deferrable resume path requires Airflow 2.7+.
"""
from __future__ import annotations

import os
import shlex
import subprocess
from datetime import timedelta
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from aqueduct import exit_codes


class AqueductOperator(BaseOperator):
    """Run an Aqueduct blueprint as an Airflow task.

    Parameters
    ----------
    blueprint:
        Path to the blueprint YAML, resolved on the worker.
    run_id:
        Stable run identifier propagated to the engine. Templated with the
        Jinja context (``{{ run_id }}`` etc.); the engine uses it as the
        primary key for run/lineage/observability records.
    config:
        Optional path to an ``aqueduct.yml`` (overrides auto-discovery).
    aqueduct_cmd:
        CLI invocation (defaults to ``["aqueduct"]``). Override for venv /
        container paths.
    extra_args:
        Extra positional arguments appended to ``aqueduct run``.
    env:
        Extra env vars merged into the subprocess environment. Use this to
        forward Airflow ``Connection`` / ``Variable`` values as
        ``${env:NAME}`` resolver targets (Phase 32).
    poll_interval:
        Seconds between trigger polls while in HEAL_PENDING (forwarded to
        ``AqueductPatchTrigger``).
    patch_timeout:
        Max wall-clock seconds the trigger waits for approval before failing
        the task. ``None`` means wait forever.
    patches_dir:
        Optional override for the patches root directory used by
        ``aqueduct patch list --json``. Defaults to the blueprint's resolved
        ``patches/`` directory.
    """

    template_fields = ("blueprint", "run_id", "extra_args", "env")

    def __init__(
        self,
        *,
        blueprint: str,
        run_id: str = "{{ run_id }}",
        config: str | None = None,
        aqueduct_cmd: list[str] | None = None,
        extra_args: list[str] | None = None,
        env: dict[str, str] | None = None,
        poll_interval: float = 30.0,
        patch_timeout: float | None = None,
        patches_dir: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.blueprint = blueprint
        self.run_id = run_id
        self.config = config
        self.aqueduct_cmd = aqueduct_cmd or ["aqueduct"]
        self.extra_args = list(extra_args or [])
        self.env = dict(env or {})
        self.poll_interval = poll_interval
        self.patch_timeout = patch_timeout
        self.patches_dir = patches_dir

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------
    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """Run the blueprint. See module docstring for exit-code mapping."""
        cmd = self._build_command()
        env = {**os.environ, **self.env}

        self.log.info("aqueduct invocation: %s", " ".join(shlex.quote(c) for c in cmd))
        result = subprocess.run(cmd, env=env, check=False)
        rc = result.returncode

        if rc == exit_codes.SUCCESS:
            return {"run_id": self.run_id, "exit_code": 0}

        if rc == exit_codes.HEAL_PENDING:
            self.log.info("HEAL_PENDING — deferring to AqueductPatchTrigger")
            self._defer_to_trigger()
            return {}  # unreachable — defer() raises TaskDeferred

        if rc == exit_codes.DATA_OR_RUNTIME:
            raise AirflowException(
                f"aqueduct run failed (exit code {rc}, DATA_OR_RUNTIME). "
                f"See blueprint logs for run_id={self.run_id!r}."
            )

        raise AirflowException(
            f"aqueduct run returned unexpected exit code {rc} for run_id={self.run_id!r}."
        )

    # ------------------------------------------------------------------
    # Deferral / resume
    # ------------------------------------------------------------------
    def _defer_to_trigger(self) -> None:
        from .trigger import AqueductPatchTrigger

        timeout = (
            timedelta(seconds=self.patch_timeout) if self.patch_timeout is not None else None
        )
        self.defer(
            trigger=AqueductPatchTrigger(
                run_id=self.run_id,
                blueprint=self.blueprint,
                patches_dir=self._resolved_patches_dir(),
                aqueduct_cmd=self.aqueduct_cmd,
                poll_interval=self.poll_interval,
            ),
            method_name="resume_from_patch",
            timeout=timeout,
        )

    def resume_from_patch(
        self, context: dict[str, Any], event: dict[str, Any]
    ) -> dict[str, Any]:
        """Trigger fired — handle the approval outcome."""
        status = event.get("status")
        if status == "approved":
            self.log.info(
                "Patch approved (patch_id=%s) — rerunning blueprint", event.get("patch_id")
            )
            return self.execute(context)
        if status == "rejected":
            raise AirflowException(
                f"Patch rejected for run_id={self.run_id!r}: "
                f"{event.get('reason') or 'no reason given'}"
            )
        raise AirflowException(
            f"AqueductPatchTrigger fired with unexpected status: {event!r}"
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _build_command(self) -> list[str]:
        cmd = [*self.aqueduct_cmd, "run", self.blueprint, "--run-id", str(self.run_id)]
        if self.config:
            cmd += ["--config", self.config]
        cmd += self.extra_args
        return cmd

    def _resolved_patches_dir(self) -> str:
        if self.patches_dir:
            return self.patches_dir
        return str(Path(self.blueprint).resolve().parent / "patches")
