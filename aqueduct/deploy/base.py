"""Deploy submitter interface — engine-agnostic, pyspark-free.

Each remote target (Databricks, EMR, Dataproc) implements this ABC. The
contract models the packaging→submit→poll→logs lifecycle of a remote
batch job whose entrypoint is ``aqueduct run <blueprint>`` running on
the cluster.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

from aqueduct.executor.models import ExecutionResult

if TYPE_CHECKING:
    from aqueduct.config import AqueductConfig


@dataclass(frozen=True)
class PackagedBlueprint:
    """Paths to uploaded artefacts on the target's storage system."""

    blueprint_path: str
    config_path: str
    bootstrap_path: str
    run_id: str


class RemoteHealNotSupported(NotImplementedError):
    """Self-healing is not yet supported for remote-submit targets.

    The failure must be handled by the orchestrator — fetch the remote
    driver logs and retry locally.
    """


class Submitter(ABC):
    """Upload, launch, and monitor a remote batch job."""

    @abstractmethod
    def package(self, blueprint_path: str, cfg: "AqueductConfig") -> PackagedBlueprint:
        """Upload blueprint + config + bootstrap artefacts to the target's
        storage layer (DBFS / S3 / GCS).

        Returns the remote paths needed by ``submit()``.
        """

    @abstractmethod
    def submit(self, packaged: PackagedBlueprint, cfg: "AqueductConfig") -> str:
        """Create and start the remote job.  Returns a job identifier."""

    @abstractmethod
    def poll(
        self, job_id: str, cfg: "AqueductConfig", timeout_seconds: float = 3600.0
    ) -> ExecutionResult:
        """Poll until the job finishes and map its outcome to an ExecutionResult.

        Raises ``TimeoutError`` if the job does not finish within
        *timeout_seconds*.
        """

    @abstractmethod
    def fetch_logs(self, job_id: str, cfg: "AqueductConfig") -> str:
        """Retrieve the remote driver / stdout logs after completion."""

    def fetch_failure_context(self, job_id: str, cfg: "AqueductConfig") -> None:
        """Stub — self-healing is not supported for remote targets in v1.

        Designed to accept a remote ``FailureContext`` from the cluster
        (logs + structured error) so it can be fed to the local agent
        loop in a future release.  Always raises ``RemoteHealNotSupported``
        for now.
        """
        raise RemoteHealNotSupported(
            "Remote FailureContext fetch is not yet supported. "
            "Fetch the driver logs manually and re-run the pipeline via "
            "your orchestrator (Airflow / cron / CI)."
        )