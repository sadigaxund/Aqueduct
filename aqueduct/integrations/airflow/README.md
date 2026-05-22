# Aqueduct ⨯ Airflow Integration

Drop-in operator + deferrable sensor for running Aqueduct blueprints from
Apache Airflow DAGs. Honors the engine's stable exit-code contract
(see `docs/specs.md §10.7`) and pauses on `HEAL_PENDING` without holding a
worker slot.

## Install

```bash
pip install aqueduct-core[airflow]
```

Requires Airflow 2.7+ (deferrable triggers stable). The triggerer process
must be running on the Airflow cluster.

## Quickstart

```python
from datetime import datetime

from airflow import DAG
from aqueduct.integrations.airflow import AqueductOperator

with DAG(
    dag_id="aqueduct_etl",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
):
    AqueductOperator(
        task_id="run_etl",
        blueprint="/opt/airflow/dags/etl.blueprint.yml",
        run_id="{{ run_id }}",
        # Optional: forward Airflow Connections / Variables as env vars
        # so blueprint `${env:DB_PASSWORD}` resolves on the worker.
        env={
            "DB_PASSWORD": "{{ var.value.db_password }}",
        },
        poll_interval=30.0,
        patch_timeout=60 * 60 * 24,  # 24h max wait for human approval
    )
```

## Exit-code mapping

| `aqueduct run` exit | Airflow outcome |
| :- | :- |
| `0` SUCCESS | task success, XCom push `{run_id, exit_code}` |
| `2` DATA_OR_RUNTIME | `AirflowException` — Airflow retry policy applies |
| `3` HEAL_PENDING | `self.defer(...)` — worker slot released; trigger polls for approval |
| anything else | `AirflowException` (`CONFIG_ERROR`, `VALIDATION_GATE`, `USAGE_ERROR`) |

## HEAL_PENDING flow

1. `aqueduct run` produces a patch under `patches/pending/<patch_id>.json`
   and exits `3`.
2. Operator calls `self.defer(trigger=AqueductPatchTrigger(...))`. Worker
   slot is freed.
3. Triggerer runs `AqueductPatchTrigger.run()` — polls
   `aqueduct patch list --status all --format json` every
   `poll_interval` seconds.
4. Reviewer approves on any node:
   ```bash
   aqueduct patch apply patches/pending/<patch_id>.json --blueprint etl.yml
   ```
   (or rejects with `aqueduct patch reject ... --reason "..."`).
5. Trigger fires a `TriggerEvent`. Operator's `resume_from_patch` runs:
   - approved → re-invokes the blueprint (which now picks up the applied
     patch).
   - rejected → task fails with the rejection reason.

## Split-task pattern (optional)

If you want patch approval to gate downstream tasks instead of resuming the
same task, use `AqueductPatchSensor` between a `ci`-mode `AqueductOperator`
and a rerun task:

```python
from aqueduct.integrations.airflow import AqueductOperator, AqueductPatchSensor

run = AqueductOperator(
    task_id="run",
    blueprint="etl.yml",
    extra_args=["--mode", "ci"],  # exits cleanly on HEAL_PENDING
)
wait = AqueductPatchSensor(
    task_id="wait_for_patch",
    run_id="{{ run_id }}",
    blueprint="etl.yml",
)
rerun = AqueductOperator(task_id="rerun", blueprint="etl.yml")

run >> wait >> rerun
```

## What the operator does NOT do

* It does **not** explode a blueprint into per-module Airflow tasks. The
  engine's internal DAG is opaque to Airflow — one operator = one blueprint
  run. Per-module visibility (TaskGroup mode) is a future feature.
* It does **not** manage Spark sessions. The engine starts/stops Spark
  inside the subprocess; Airflow only sees the CLI invocation.
* It does **not** read patch sidecar files directly. The patch CLI is the
  source of truth.

## Troubleshooting

* **`ModuleNotFoundError: aqueduct`** on the worker — the worker needs
  `pip install aqueduct-core[spark]`; the scheduler / triggerer can run on
  the slim install.
* **Trigger never fires** — confirm the triggerer process is running
  (`airflow triggerer`). The deferrable model needs it; without it the
  task sits forever.
* **Wrong patch matched** — the trigger pairs patches to runs by
  substring-matching `run_id` against the patch file path and rationale.
  If your `run_id` template is non-unique, override `run_id=` with
  something stable per task instance.
