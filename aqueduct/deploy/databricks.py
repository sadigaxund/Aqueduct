"""Databricks Jobs API 2.x remote submitter.

Uploads blueprint + config + bootstrap to DBFS, creates a one-shot
``spark_python_task`` job run, and polls its outcome.

Uses ``httpx`` (already a base dep) — no ``databricks-sdk`` needed for
the v1 thin implementation.  SDK availability (``[databricks]`` extra)
is checked lazily for a nicer error message; the fallback is raw httpx
calls.
"""

from __future__ import annotations

import base64
import logging
import os
import time
import uuid
from typing import TYPE_CHECKING

import httpx

from aqueduct.deploy.base import PackagedBlueprint, Submitter
from aqueduct.errors import AqueductError, ConfigError
from aqueduct.executor.models import ExecutionResult, ModuleResult

if TYPE_CHECKING:
    from aqueduct.config import AqueductConfig

logger = logging.getLogger(__name__)


class DeployError(AqueductError):
    """Raised for deployment/runtime failures during remote submission."""


_BOOTSTRAP_SCRIPT = """\
import json
import subprocess
import sys

blueprint_path = sys.argv[1]
config_path = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else None
outcome_path = sys.argv[3] if len(sys.argv) > 3 else "/tmp/aqueduct_result.json"

cmd = ["aqueduct", "run", blueprint_path]
if config_path:
    cmd.extend(["--config", config_path])

try:
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
except subprocess.TimeoutExpired:
    result_json = {
        "returncode": 124,
        "stdout": "",
        "stderr": "aqueduct run timed out after 2 hours",
    }
else:
    result_json = {
        "returncode": result.returncode,
        "stdout": result.stdout[-8000:] if result.stdout else "",
        "stderr": result.stderr[-8000:] if result.stderr else "",
    }

try:
    with open(outcome_path, "w") as f:
        json.dump(result_json, f)
except Exception:
    pass  # bootstrap outcome write is best-effort; the enclosing process is about to `sys.exit`

sys.exit(result_json["returncode"])
"""

_DEFAULT_POLL_INTERVAL = 5.0
_BACKOFF_CAP = 60.0
_BACKOFF_FACTOR = 1.5


def _normalize_url(url: str) -> str:
    url = url.rstrip("/")
    if not url.startswith("https://"):
        if url.startswith("http://"):
            raise ConfigError(f"Databricks workspace URL must use https: {url!r}")
        url = f"https://{url}"
    return url


class DatabricksSubmitter(Submitter):
    """Upload to DBFS, launch a one-shot run, poll to completion."""

    def _api_url(self, cfg: AqueductConfig, endpoint: str) -> str:
        databricks = cfg.deployment.databricks
        if databricks is None:
            raise ConfigError(
                "deployment.databricks block is required for target=databricks"
            )
        if not databricks.workspace_url:
            raise ConfigError(
                "deployment.databricks.workspace_url is required"
            )
        return f"{_normalize_url(databricks.workspace_url)}{endpoint}"

    def _auth_header(self) -> dict[str, str]:
        token = os.environ.get("DATABRICKS_TOKEN")
        if not token:
            raise ConfigError(
                "DATABRICKS_TOKEN environment variable is required for "
                "Databricks remote-submit. Set it or reference it via "
                "@aq.secret('DATABRICKS_TOKEN') in aqueduct.yml."
            )
        return {"Authorization": f"Bearer {token}"}

    def _db_put(self, dbfs_path: str, content: bytes, cfg: AqueductConfig) -> None:
        handle = None
        try:
            r_create = httpx.post(
                self._api_url(cfg, "/api/2.0/dbfs/create"),
                headers=self._auth_header(),
                json={"path": dbfs_path, "overwrite": True},
                timeout=30,
            )
            r_create.raise_for_status()
            handle = r_create.json().get("handle")
        except httpx.HTTPStatusError as exc:
            raise DeployError(
                f"DBFS create failed for {dbfs_path!r}: {exc.response.status_code}"
            ) from exc

        if handle is None:
            raise DeployError(f"DBFS create returned no handle for {dbfs_path!r}")

        chunk_size = 1024 * 1024
        offset = 0
        while offset < len(content):
            chunk = content[offset : offset + chunk_size]
            try:
                enc = base64.b64encode(chunk).decode("ascii")
            except Exception as exc:
                raise DeployError(
                    f"Failed to encode chunk for {dbfs_path!r} at offset {offset}: {exc}"
                ) from exc
            try:
                r_block = httpx.post(
                    self._api_url(cfg, "/api/2.0/dbfs/add-block"),
                    headers=self._auth_header(),
                    json={"handle": handle, "data": enc},
                    timeout=60,
                )
                r_block.raise_for_status()
            except httpx.HTTPStatusError as exc:
                try:
                    httpx.post(
                        self._api_url(cfg, "/api/2.0/dbfs/close"),
                        headers=self._auth_header(),
                        json={"handle": handle},
                        timeout=10,
                    )
                except Exception:
                    pass  # DBFS close-handle is best-effort cleanup on error; the real failure is the block upload
                raise DeployError(
                    f"DBFS add-block failed for {dbfs_path!r} at offset {offset}: "
                    f"{exc.response.status_code}"
                ) from exc
            offset += chunk_size

        try:
            r_close = httpx.post(
                self._api_url(cfg, "/api/2.0/dbfs/close"),
                headers=self._auth_header(),
                json={"handle": handle},
                timeout=30,
            )
            r_close.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise DeployError(
                f"DBFS close failed for {dbfs_path!r}: {exc.response.status_code}"
            ) from exc

    def _db_read(self, dbfs_path: str, cfg: AqueductConfig) -> str:
        offset = 0
        chunks: list[str] = []
        while True:
            r = httpx.get(
                self._api_url(cfg, "/api/2.0/dbfs/read"),
                headers=self._auth_header(),
                params={"path": dbfs_path, "offset": offset, "length": 1024 * 1024},
                timeout=30,
            )
            if r.status_code == 404:
                break
            r.raise_for_status()
            body = r.json()
            data = body.get("data", "")
            if not data:
                break
            try:
                chunks.append(base64.b64decode(data).decode("utf-8", errors="replace"))
            except Exception:
                chunks.append(data)
            bytes_read = body.get("bytes_read", 0)
            if bytes_read == 0:
                break
            offset += bytes_read

        return "".join(chunks)

    def _dbfs_base(self, run_id: str) -> str:
        return f"dbfs:/aqueduct/jobs/{run_id}"

    # ── Submitter ABC ──────────────────────────────────────────────────────

    def package(self, blueprint_path: str, cfg: AqueductConfig) -> PackagedBlueprint:
        from pathlib import Path

        bp_path = Path(blueprint_path)
        run_id = uuid.uuid4().hex[:12]
        dbfs_base = self._dbfs_base(run_id)

        # Blueprint file
        bp_content = bp_path.read_bytes()
        bp_dbfs = f"{dbfs_base}/blueprint.yml"
        self._db_put(bp_dbfs, bp_content, cfg)

        # Config file (aqueduct.yml) — upload if present in project root
        config_dbfs = ""
        project_root = bp_path.parent
        for _ in range(4):
            candidate = project_root / "aqueduct.yml"
            if candidate.exists():
                cfg_content = candidate.read_bytes()
                config_dbfs = f"{dbfs_base}/aqueduct.yml"
                self._db_put(config_dbfs, cfg_content, cfg)
                break
            if project_root.parent == project_root:
                break
            project_root = project_root.parent

        # Bootstrap script
        bootstrap_dbfs = f"{dbfs_base}/bootstrap.py"
        self._db_put(bootstrap_dbfs, _BOOTSTRAP_SCRIPT.encode("utf-8"), cfg)

        return PackagedBlueprint(
            blueprint_path=bp_dbfs,
            config_path=config_dbfs,
            bootstrap_path=bootstrap_dbfs,
            run_id=run_id,
        )

    def submit(self, packaged: PackagedBlueprint, cfg: AqueductConfig) -> str:
        databricks = cfg.deployment.databricks
        if databricks is None:
            raise ConfigError("deployment.databricks block is required for target=databricks")

        task: dict = {
            "task_key": "aqueduct-run",
            "spark_python_task": {
                "python_file": packaged.bootstrap_path,
                "parameters": [packaged.blueprint_path],
            },
        }
        if packaged.config_path:
            task["spark_python_task"]["parameters"].append(packaged.config_path)

        if databricks.cluster_id:
            task["existing_cluster_id"] = databricks.cluster_id
        elif databricks.new_cluster:
            task["new_cluster"] = databricks.new_cluster
        else:
            raise ConfigError(
                "deployment.databricks: one of cluster_id or new_cluster "
                "is required"
            )

        if databricks.libraries:
            task["libraries"] = databricks.libraries

        payload: dict = {
            "run_name": f"aqueduct-{packaged.run_id}",
            "tasks": [task],
        }

        if databricks.max_concurrent_runs is not None:
            payload["max_concurrent_runs"] = databricks.max_concurrent_runs

        try:
            r = httpx.post(
                self._api_url(cfg, "/api/2.1/jobs/runs/submit"),
                headers=self._auth_header(),
                json=payload,
                timeout=30,
            )
            r.raise_for_status()
        except httpx.HTTPStatusError as exc:
            detail = ""
            try:
                detail = f": {exc.response.text}"
            except Exception:
                pass  # response-body read is diagnostic best-effort; the error is surfaced regardless
            raise DeployError(
                f"Job submit failed "
                f"(status {exc.response.status_code}){detail}"
            ) from exc

        run_id = r.json()["run_id"]
        logger.info("Submitted Databricks job run_id=%s", run_id)
        return str(run_id)

    def poll(
        self,
        job_id: str,
        cfg: AqueductConfig,
        timeout_seconds: float = 3600.0,
    ) -> ExecutionResult:
        deadline = time.monotonic() + timeout_seconds
        interval = _DEFAULT_POLL_INTERVAL

        terminal_states = frozenset({
            "TERMINATED",
            "INTERNAL_ERROR",
            "SKIPPED",
        })

        while True:
            elapsed = timeout_seconds - (deadline - time.monotonic())
            logger.debug("Polling run_id=%s elapsed=%.0fs", job_id, elapsed)

            try:
                r = httpx.get(
                    self._api_url(cfg, "/api/2.1/jobs/runs/get"),
                    headers=self._auth_header(),
                    params={"run_id": job_id},
                    timeout=15,
                )
                r.raise_for_status()
            except httpx.HTTPStatusError as exc:
                return ExecutionResult(
                    blueprint_id="",
                    run_id=job_id,
                    status="error",
                    module_results=(
                        ModuleResult(
                            module_id="_submitter",
                            status="error",
                            error=f"Poll failed: HTTP {exc.response.status_code}",
                        ),
                    ),
                )

            body = r.json()
            run_state = body.get("state", {})
            life_cycle_state = run_state.get("life_cycle_state", "UNKNOWN")
            state_message = run_state.get("state_message", "")

            if life_cycle_state in terminal_states:
                result_state = run_state.get("result_state", "FAILED")
                error_msg = state_message or (
                    f"Run ended: life_cycle_state={life_cycle_state}, "
                    f"result_state={result_state}"
                )

                if result_state == "SUCCESS" or life_cycle_state == "SKIPPED":
                    return ExecutionResult(
                        blueprint_id="",
                        run_id=job_id,
                        status="success",
                        module_results=(
                            ModuleResult(
                                module_id="_remote_run",
                                status="success",
                            ),
                        ),
                    )

                return ExecutionResult(
                    blueprint_id="",
                    run_id=job_id,
                    status="error",
                    module_results=(
                        ModuleResult(
                            module_id="_remote_run",
                            status="error",
                            error=error_msg,
                        ),
                    ),
                )

            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Databricks run_id={job_id} did not finish within "
                    f"{timeout_seconds:.0f}s. Last state: {life_cycle_state}"
                )

            time.sleep(interval)
            interval = min(interval * _BACKOFF_FACTOR, _BACKOFF_CAP)

    def fetch_logs(self, job_id: str, cfg: AqueductConfig) -> str:
        """Read the outcome file written by the bootstrap script on DBFS."""
        outcome_path = f"{self._dbfs_base(job_id)}/../result.json"
        try:
            raw = self._db_read(outcome_path, cfg)
            if raw:
                import json
                outcome = json.loads(raw)
                parts = []
                if outcome.get("stdout"):
                    parts.append(outcome["stdout"])
                if outcome.get("stderr"):
                    parts.append(f"── stderr ──\n{outcome['stderr']}")
                return "\n".join(parts)
        except Exception:
            pass  # outcome JSON parse is best-effort log enrichment; empty-string fallback is harmless
        return ""