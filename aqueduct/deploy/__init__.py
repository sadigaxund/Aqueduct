"""Remote-submit dispatch — factory for execution-target submitters.

``get_submitter(target, cfg)`` returns a ``Submitter`` instance for the
given deployment target, mirroring ``aqueduct.executor.get_executor()``.

All code in this package is engine-agnostic and pyspark-free — the whole
point of remote-submit is that it runs on a laptop / CI runner with no
Spark installation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from aqueduct.deploy.base import (
    DeployError,
    PackagedBlueprint,
    RemoteHealNotSupported,
    Submitter,
)

if TYPE_CHECKING:
    from aqueduct.config import AqueductConfig

_SUBMITTERS: dict[str, type[Submitter]] = {}


def _register(target: str, cls: type[Submitter]) -> type[Submitter]:
    _SUBMITTERS[target] = cls
    return cls


def get_submitter(target: str, cfg: AqueductConfig) -> Submitter:
    """Return a ``Submitter`` instance for *target*.

    Raises ``DeployError`` for targets that are not yet wired. In practice
    this is dead code on the `aqueduct run` path — `config.py`'s
    `DeploymentConfig` validator already rejects `emr`/`dataproc` and any
    non-Literal `target` at config-load, before `cfg.deployment.target` ever
    reaches here — but `get_submitter` is a public dispatch point any future
    caller (or a config-validation change) could reach directly, so it still
    fails as an `AqueductError` rather than a bare builtin.
    """
    if target == "databricks":
        from aqueduct.deploy.databricks import DatabricksSubmitter

        return DatabricksSubmitter()

    if target in ("emr", "dataproc"):
        raise DeployError(
            f"Remote-submit for target {target!r} is not yet implemented. "
            f"Databricks is the only supported remote target in this release."
        )

    raise DeployError(
        f"No submitter for target {target!r}. "
        f"Supported remote targets: databricks."
    )


__all__ = [
    "DeployError",
    "PackagedBlueprint",
    "RemoteHealNotSupported",
    "Submitter",
    "get_submitter",
]