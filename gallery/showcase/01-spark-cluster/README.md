# Showcase 01 — Spark Cluster + S3A (guided walkthrough)

A hands-on, do-it-yourself tour of deploying Aqueduct against a **real
distributed Spark Standalone cluster** (2 workers) with **MinIO** object
storage and a **Postgres**-backed observability/lineage/depot. You will
stand up the stack, learn the two networking gotchas that bite everyone,
understand the S3A jar story, run the pipeline, then read back what the
engine recorded.

---

## What you'll learn

- **Where Aqueduct sits**: the engine runs on the driver as a thin
  orchestration layer and dispatches the actual work to a remote cluster
  it does not own — and the one networking contract that makes that work.
- **How Aqueduct stays out of the data path**: it declares `s3a://`
  sources/sinks and lets the cluster do the I/O — so the runtime
  dependency (the S3A jars) is a cluster property, not something the
  engine ships.
- Spillway error routing + an inline Assert quality gate, on a cluster.
- Reading the run back from a **Postgres** observability + lineage store.

```
MinIO  s3a://showcase/input/transactions.parquet   ← you seed this first
        │
   raw_transactions      (Ingress)
        │
   clean_transactions    (Channel — filter completed, normalize region)
        ├── spillway ─→ rejected_output   (Egress → s3a://showcase/output/rejected/)
        │
   quality_gate          (Assert — min_rows, null_rate, freshness)
        │
   transactions_output   (Egress → s3a://showcase/output/clean/  partitioned by region)
```

---

## Concept 1 — the two IPs (read this, it's the #1 failure)

This is a genuinely distributed setup: two roles, two hosts, two
addresses. The **cluster host** runs the Spark master/workers + MinIO;
the **driver host** runs `aqueduct run`. Spark is bidirectional — the
driver dials the cluster, and the workers dial **back** to the driver —
so each side needs the other's reachable LAN IP.

This walkthrough uses worked example IPs throughout:

| Var           | Role        | Example         | What it must be                                            |
| ------------- | ----------- | --------------- | ---------------------------------------------------------- |
| `HOST_IP`     | cluster host | `192.168.1.20` | LAN IP of the box running `docker compose` (cluster+MinIO) |
| `DRIVER_HOST` | driver host  | `192.168.1.10` | LAN IP of the box running `aqueduct run`                   |

Substitute your own LAN IPs (`hostname -I | awk '{print $1}'` on Linux,
`ipconfig getifaddr en0` on macOS). Never `127.0.0.1`/`localhost` — the
workers run inside Docker containers; loopback there is the container,
not your host, and the job hangs forever on *"Initial job has not
accepted any resources"*. (Both roles on one machine? Then both IPs are
that machine's single LAN IP — same value, two variables.)

Export the cluster IP now — Docker Compose and the seed script read it
(`aqueduct run` gets `DRIVER_HOST` explicitly in Step 5):

```bash
export HOST_IP=192.168.1.20
```

---

## Concept 2 — the S3A jar story (why a custom Spark image)

Spark 4.0 bundles Hadoop 3.4 but **not** the S3A connector — out of the
box it cannot read `s3a://`. S3A needs two jars on the classpath:

- `hadoop-aws-3.4.0.jar` — the `S3AFileSystem` itself; **must** match
  Spark's bundled Hadoop (3.4.x for Spark 4.0; a 3.3.x jar throws the
  infamous `NumberFormatException: For input string: "60s"`).
- `bundle-2.26.25.jar` — AWS SDK **v2**, what `hadoop-aws` 3.4 calls into.


Here's the part people miss: **both sides need the jars, by different
mechanisms**, because they do different work:

| Side         | Does                                | How it gets the jars                                                                 |
| ------------ | ----------------------------------- | ------------------------------------------------------------------------------------ |
| **Driver**   | plans the job, resolves `s3a://`    | `aqueduct.yml` → `spark.driver.extraClassPath: ${PWD}/jars/*` — i.e. **local `./jars/`** |
| **Executors**| do the actual S3A read/write I/O    | **baked into the cluster image** `sakhund/aqueduct-spark:4.0.0-python3.12-showcase01` |

> So this example uses a **custom image** — an official Spark 4.0 build
> with the S3A jars pre-installed on the Spark classpath.
> The driver side is the only thing you provision locally.
> If you point `aqueduct.yml` at a *vanilla* `apache/spark` image instead,
> executors would fail with `ClassNotFoundException: ...S3AFileSystem`.


Provision the **driver-side** jars now:

```bash
mkdir -p jars && cd jars
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar
wget https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.26.25/bundle-2.26.25.jar
cd ..
```

---

## Prerequisites

- Docker + Docker Compose
- Python 3.11+ (tested 3.12) with a virtualenv
- `HOST_IP` exported (Concept 1) and `./jars/` populated (Concept 2)

---

## Step 1 — Install the driver

```bash
pip install -r requirements.txt
```

> **Why the pin:** `requirements.txt` forces `pyspark==4.0.0` to match
> the cluster image exactly. A driver/cluster Spark mismatch fails with
> an opaque `serialVersionUID` RPC error — same major.minor.patch on
> both sides is non-negotiable.

---

## Step 2 — Start the stack

```bash
docker compose up -d
```

First run pulls the custom image plus Postgres + MinIO. Give
the workers ~15 s to register, then **interactively verify** before
moving on:

```bash
docker compose ps                       # all services "running"/"healthy"
```

- **Spark UI** → `http://$HOST_IP:8080` — expect **2 workers ALIVE**
  with free cores/memory under "Workers".
- **MinIO console** → `http://$HOST_IP:9001` — login `minioadmin` /
  `minioadmin`.

> Don't continue until the Spark UI shows 2 live workers. A pipeline run
> against a master with 0 workers hangs, it does not error.

---

## Step 3 — Seed MinIO (do this **before** running)

The pipeline's Ingress reads `s3a://showcase/input/transactions.parquet`.
**That object does not exist yet** — nothing has created the bucket or
the data. Seed it now (this is the single most common "why does Ingress
say path not found" mistake):

```bash
python seed.py        # uses the exported HOST_IP
```

Expected:

```
Connecting to MinIO at http://<HOST_IP>:9000 ...
  create  s3://showcase/
  seed    s3://showcase/input/transactions.parquet
          1000 rows  |  ~630 completed  |  ~38 null amounts

MinIO console: http://<HOST_IP>:9001  (minioadmin / minioadmin)
Spark UI:      http://<HOST_IP>:8080
```

Confirm in the MinIO console: bucket `showcase` → `input/` →
`transactions.parquet`.

---

## Step 4 — Pre-flight the config

```bash
aqueduct doctor aqueduct.yml --preflight -e DRIVER_HOST=192.168.1.10
```

`192.168.1.10` is the driver host (Concept 1). `HOST_IP` isn't passed —
it's exported, so `aqueduct.yml`'s `${HOST_IP}` already resolves.

> **Why `--preflight`:** plain `doctor` does a fast TCP reachability
> probe (no Spark session). `--preflight` builds a real session against
> the cluster with your `spark_config` — the true "will my run connect"
> test.

Expect `spark`, `observability`, `lineage`, `depot` (all Postgres) green.

---

## Step 5 — Run

No `.env`, no `source`. `HOST_IP` is exported; pass the driver host
explicitly:

```bash
aqueduct run blueprint.yml --config aqueduct.yml -e DRIVER_HOST=192.168.1.10
```

Expected:

```
▶ showcase.spark.cluster  (5 modules)  run=<uuid>  engine=spark  master=spark://<HOST_IP>:7077
  ✓ raw_transactions
  ✓ clean_transactions
  ✓ quality_gate
  ✓ transactions_output
  ✓ rejected_output

✓ blueprint complete  run_id=<uuid>
```

---

## Step 6 — Read back what happened

This is the educational payoff — the run left three trails.

**1. The data, in MinIO** (`http://$HOST_IP:9001` → `showcase`):

- `output/clean/region=US/`, `region=EU/`, `region=APAC/` — partitioned
  Parquet (only `status='completed'` rows).
- `output/rejected/` — rows the spillway diverted (null `amount` or
  non-completed `status`) — the pipeline did **not** abort on them.

**2. The execution, in Postgres.** Stores are Postgres here (not local
DuckDB) — query them directly:

```bash
docker compose exec -T postgres psql -U aqueduct -d aqueduct_db -c \
  "SELECT module_id, status, records_written, duration_ms
     FROM module_metrics ORDER BY started_at;"
```

You'll see one row per module, with `records_written` showing the
clean/rejected split.

**3. Column lineage, in Postgres:**

```bash
docker compose exec -T postgres psql -U aqueduct -d aqueduct_db -c \
  "SELECT downstream_column, upstream_column, transform
     FROM column_lineage LIMIT 20;"
```

This traces `region`/`amount` from the source Parquet through the SQL
Channel into both Egress sinks — captured automatically, no annotations.

**4. The run record / any failures:**

```bash
docker compose exec -T postgres psql -U aqueduct -d aqueduct_db -c \
  "SELECT run_id, status, started_at, finished_at FROM run_records;"
```

> **Try it:** drop `min: 100` to `min: 100000` in `blueprint.yml`'s
> `quality_gate`, re-run, and watch the Assert abort the pipeline — then
> see the failure captured in `failure_contexts`.

---

## Spark UI tour

`http://$HOST_IP:8080` → **Completed Applications** → your run:

- 2 executors (one per worker), tasks spread across them.
- Stages for the Parquet scan, the SQL filter, and the two writes.
- This is genuine distributed execution — the driver on your machine
  orchestrated work that ran in the worker containers.

---

## Troubleshooting

| Symptom                                              | Cause / fix                                                                                          |
| ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| Ingress: path / object not found                     | You skipped Step 3. Run `python seed.py` — the input object isn't created until then.                |
| Run hangs on *"has not accepted any resources"*      | `DRIVER_HOST`/`HOST_IP` is loopback, or 0 workers. Use a real LAN IP; confirm 2 ALIVE in Spark UI.   |
| `Cannot assign requested address` on driver bind     | `DRIVER_HOST` is not a local interface on the driver machine. Check `ip -4 addr`.                     |
| `serialVersionUID` RPC error                         | Driver pyspark ≠ cluster Spark. Both must be `4.0.0` (the `requirements.txt` pin handles the driver). |
| `ClassNotFoundException: ...S3AFileSystem`           | Driver side: `./jars/` missing the two jars (Concept 2). Executor side: not the `-showcase01` image.  |
| `NumberFormatException: For input string: "60s"`     | Wrong `hadoop-aws` — Spark 4.0 needs `hadoop-aws-3.4.x`, not 3.3.x.                                   |
| MinIO 403 / auth on doctor's storage probe           | Expected if the probe bucket is absent — not your creds. A real run is the true S3 auth test.        |
| `psycopg2 ... could not connect`                     | Postgres not up/healthy yet. `docker compose ps`; wait for the `postgres` healthcheck.               |

---

## Tear down

```bash
docker compose down -v          # stops stack + drops Postgres/MinIO volumes
```

> Stores are in Postgres (a dropped volume), so there is **no local
> `.aqueduct/` to clean** — that's the point of the Postgres backend on a
> cluster: nothing important lives in the ephemeral driver CWD.
