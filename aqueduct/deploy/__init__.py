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

    Raises ``NotImplementedError`` for targets that are not yet wired.
    """
    if target == "databricks":
        from aqueduct.deploy.databricks import DatabricksSubmitter

        return DatabricksSubmitter()

    if target in ("emr", "dataproc"):
        raise NotImplementedError(
            f"Remote-submit for target {target!r} is not yet implemented. "
            f"Databricks is the only supported remote target in this release."
        )

    raise NotImplementedError(
        f"No submitter for target {target!r}. "
        f"Supported remote targets: databricks."
    )


__all__ = [
    "PackagedBlueprint",
    "RemoteHealNotSupported",
    "Submitter",
    "get_submitter",
]