"""Apache Airflow integration for Aqueduct (Phase 31).

Provides a drop-in ``AqueductOperator`` plus a deferrable
``AqueductPatchSensor`` / ``AqueductPatchTrigger`` pair so that Airflow DAGs
can run Aqueduct blueprints and pause on healing approval without holding a
worker slot.

Install::

    pip install aqueduct-core[airflow]

Usage::

    from aqueduct.integrations.airflow import AqueductOperator

    run_etl = AqueductOperator(
        task_id="run_etl",
        blueprint="dags/etl.blueprint.yml",
        run_id="{{ run_id }}",
    )

See ``aqueduct/integrations/airflow/README.md`` for the full DAG example and
the HEAL_PENDING approval flow.
"""
from __future__ import annotations

__all__ = [
    "AqueductOperator",
    "AqueductPatchSensor",
    "AqueductPatchTrigger",
]


def __getattr__(name: str):
    if name == "AqueductOperator":
        from .operator import AqueductOperator

        return AqueductOperator
    if name == "AqueductPatchSensor":
        from .sensor import AqueductPatchSensor

        return AqueductPatchSensor
    if name == "AqueductPatchTrigger":
        from .trigger import AqueductPatchTrigger

        return AqueductPatchTrigger
    raise AttributeError(f"module 'aqueduct.integrations.airflow' has no attribute {name!r}")
