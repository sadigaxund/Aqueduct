# Showcase 02 — Aqueduct + Apache Airflow

End-to-end demo of Aqueduct's Airflow integration. Pure
`docker compose` — no local build, no `make`, no pip install.

| DAG | What it shows | LLM required? |
| :- | :- | :- |
| `aqueduct_happy` | Operator wiring — blueprint runs green inside an Airflow task | **No** |
| `aqueduct_drift` | `HEAL_PENDING` flow — task defers on `UNRESOLVED_COLUMN`, releases worker slot, resumes after human approval | Yes |

> Wake up to a pending patch awaiting your approval — not a wall of error
> logs. This is what that looks like in practice.

## Layout

```
02-airflow/
├── docker-compose.yml          # Airflow 2.10 + Postgres + triggerer (LocalExecutor)
├── aqueduct.yml                # engine config (DuckDB stores, no agent)
├── .env.example                # optional LLM env vars (only the drift demo reads them)
├── dags/aqueduct_demo_dag.py   # two DAGs using AqueductOperator
├── blueprints/
│   ├── etl_happy.yml           # green path (no agent block → no LLM)
│   └── etl_drift.yml           # `total` -> UNRESOLVED_COLUMN -> HEAL_PENDING
├── data/orders.csv             # 10-row sample input
└── patches/                    # bind-mounted; reviewer + worker share state here
```

The whole DAG file is **two `AqueductOperator(...)` calls** — one blueprint
per task, the engine owns Spark/lineage/healing.

## Demo 1 — happy path (no LLM needed)

```bash
docker compose up -d
# wait ~60s for boot, then open http://localhost:8080  (login: airflow / airflow)
docker compose exec airflow-scheduler airflow dags trigger aqueduct_happy
```

Watch the `aqueduct_happy` DAG in the UI. One task, green, ~30s. The CSV
under `data/orders.csv` is filtered to completed orders and written as
parquet to `data/output/etl_happy/`.

Inspect the Aqueduct run record:

```bash
docker compose exec airflow-scheduler aqueduct runs --last 5
```

## Demo 2 — HEAL_PENDING round-trip

Needs Ollama running on the host. Pull a small coder model first:

```bash
ollama pull qwen2.5-coder:7b
```

(Other models or remote endpoints: copy `.env.example` to `.env` and edit.)

Trigger the drift DAG:

```bash
docker compose exec airflow-scheduler airflow dags trigger aqueduct_drift
```

In the Airflow UI the task transitions to **deferred** (purple). At that
point the worker slot has been released — the triggerer is polling for
patch approval. Why: the blueprint queries a column `total` that does not
exist; Spark raises `UNRESOLVED_COLUMN.WITH_SUGGESTION` with
`proposal: ["amount"]`, the Surveyor stages a `set_module_config_key`
patch under `patches/pending/`, exits with code `3` (`HEAL_PENDING`), and
the operator calls `self.defer(...)`.

Inspect the pending patch:

```bash
docker compose exec airflow-scheduler \
    aqueduct patch list --patches-dir /opt/airflow/patches --status pending
```

Approve it (grab the patch file path from the list above):

```bash
docker compose exec airflow-scheduler \
    aqueduct patch apply /opt/airflow/patches/pending/<id>.json \
    --blueprint /opt/airflow/blueprints/etl_drift.yml
```

Within ~10s the triggerer fires a `TriggerEvent`, the operator's
`resume_from_patch` reruns the blueprint against the patched config, and
the task flips to **green**. Output ends up under `data/output/etl_drift/`.

To see the rejection path instead:

```bash
docker compose exec airflow-scheduler \
    aqueduct patch reject /opt/airflow/patches/pending/<id>.json --reason "demo reject"
```

The task fails with the rejection reason surfaced in the Airflow UI.

## Reset between demos

```bash
rm -rf data/output patches/pending/* patches/applied/* patches/rejected/*
docker compose exec airflow-scheduler airflow dags trigger aqueduct_drift
```

## Tear down

```bash
docker compose down            # keep volumes (next `up` is fast)
docker compose down -v         # delete volumes (full reset)
```

## What this showcase deliberately does NOT do

* **Per-module Airflow tasks.** One blueprint = one Airflow task.
* **Cluster-shared filesystem.** Bind-mounted `patches/` works because
  everything runs on one host. Multi-node Airflow needs a shared FS
  (NFS, S3 + sidecar sync) or the upcoming Postgres patch store.
* **Production secrets.** Fernet key and DB credentials are hardcoded
  in `docker-compose.yml`. Do not copy verbatim into production.

## Troubleshooting

* **`pull access denied for sakhund/aqueduct-airflow:latest`** — image
  hasn't been pushed yet. The maintainer needs to run
  `tmp/aqueduct-airflow-image/build_and_push.sh` (not part of the
  public repo) before this showcase works for outside users.
* **Containers `(unhealthy)` for 30-60s after `docker compose up`** —
  normal cold-boot (Postgres init + Airflow `db migrate`).
* **`Permission denied` / `Mkdirs failed` on bind-mounted dirs** —
  The init container `chown`s `data/output/` and `patches/` to the
  container's airflow user (UID 50000) at every `up`. SELinux `:z`
  relabel + chown should cover Fedora/RHEL/CentOS too. If you still see
  permission errors (rare, usually means SELinux is in `enforcing` mode
  with a custom policy), match the host UID instead:
  ```bash
  echo "AIRFLOW_UID=$(id -u)" >> .env
  docker compose down -v && docker compose up -d
  ```
  After this, output files under `data/output/` will be owned by UID
  50000 on the host — readable but not editable by your host user. Use
  `docker compose exec airflow-scheduler ls /opt/airflow/data/output/`
  if you need to inspect them from inside the container.
* **Drift task hangs in deferred forever** — confirm `airflow-triggerer`
  is up (`docker compose ps`). Without it, deferred tasks never resume.
* **`patch list` empty after triggering drift** — the drift task may
  not have failed yet (~20s to hit the Spark error + Surveyor). Watch
  `docker compose logs airflow-scheduler | grep aqueduct`.
* **LLM unreachable** — `aqueduct.yml` resolves `${AQ_OLLAMA_URL}` to
  `http://host.docker.internal:11434` by default. On Linux, the
  `extra_hosts` block in `docker-compose.yml` maps that to the host
  gateway. Confirm `ollama serve` is running on the host.
