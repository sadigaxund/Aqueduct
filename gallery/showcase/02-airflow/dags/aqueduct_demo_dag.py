"""Aqueduct + Airflow showcase DAGs.

Two DAGs ship in this file:

* ``aqueduct_happy``  — runs a clean blueprint end-to-end. Proves the
  ``AqueductOperator`` -> Airflow wiring works without needing an LLM.
* ``aqueduct_drift``  — runs a blueprint with a deliberate
  ``UNRESOLVED_COLUMN`` defect. The Surveyor stages a patch under
  ``patches/pending/`` and exits with ``HEAL_PENDING`` (exit code 3).
  The operator calls ``self.defer(...)`` releasing the worker slot;
  the triggerer process polls ``aqueduct patch list --format json``
  until a reviewer approves the patch, then the task resumes and the
  pipeline finishes green.

Approval (from the host)::

    docker compose exec airflow-scheduler \\
        aqueduct patch apply patches/pending/<id>.json \\
        --blueprint /opt/airflow/blueprints/etl_drift.yml

See the showcase README for the full walkthrough.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG

from aqueduct.integrations.airflow import AqueductOperator

DEFAULT_ARGS = {
    "owner": "aqueduct",
    "retries": 0,
}

with DAG(
    dag_id="aqueduct_happy",
    description="Aqueduct ETL — happy path. No LLM required.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["aqueduct", "showcase"],
):
    AqueductOperator(
        task_id="run_happy",
        blueprint="/opt/airflow/blueprints/etl_happy.yml",
        config="/opt/airflow/aqueduct.yml",
        run_id="{{ run_id }}",
    )


with DAG(
    dag_id="aqueduct_drift",
    description="Aqueduct ETL — schema-drift defect. Defers on HEAL_PENDING, "
                "waits for human approval, then resumes.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["aqueduct", "showcase", "healing"],
):
    AqueductOperator(
        task_id="run_drift",
        blueprint="/opt/airflow/blueprints/etl_drift.yml",
        config="/opt/airflow/aqueduct.yml",
        run_id="{{ run_id }}",
        patches_dir="/opt/airflow/patches",
        poll_interval=10.0,             # snappy demo; production = 30s+
        patch_timeout=60 * 60 * 6,      # 6h max wait for human approval
    )
