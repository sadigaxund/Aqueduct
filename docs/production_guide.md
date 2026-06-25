# Aqueduct Production Guide

**Deploying Aqueduct pipelines on Spark clusters — configuration, security, maintenance.**

---

## Deployment Environments

Aqueduct supports three deployment environments, declared under the `deployment:` block in `aqueduct.yml`:

```yaml
deployment:
  env: local        # default — relative paths, local Spark, local DuckDB
  # env: cluster    # Spark standalone / YARN — shared FS, driver is persistent
  # env: cloud      # K8s / EMR / Dataproc — ephemeral driver, object storage
```

The value affects:
- Path validation in `aqueduct doctor` (errors on relative store paths in `cluster`/`cloud` mode)
- Self-healing patch lifecycle guidance
- Observability state persistence guidance

---

## Scheduling

Aqueduct has no built-in scheduler. `aqueduct run` is a one-shot CLI command designed to be invoked by an orchestrator:

- **Simple cron**: Any OS-level cron, systemd timer, or cloud scheduler (AWS EventBridge, GCP Cloud Scheduler) invoking `aqueduct run blueprint.yml`.
- **Complex orchestration**: An Airflow `AqueductOperator` (wrapping `aqueduct run` via the deferrable trigger pattern) for dependency management, backfill, and SLA tracking.
- **On-demand**: Manual invocation from CI/CD pipelines or by the LLM agent via `aqueduct run` after patch application.

---

## Spark Cluster Configuration

Aqueduct creates a `SparkSession` on the driver. Cluster connection is controlled via the `deployment:` and `spark_config:` blocks in `aqueduct.yml`.

The `target` field is validated against `master_url` at config-load. A
mismatch raises a `ConfigError` naming both values and the expected shape.
`databricks` is a fully wired **remote‑submit** target — the engine packages
the blueprint, uploads to DBFS, submits via the Databricks Jobs API, and
polls to completion. `emr` and `dataproc` are rejected — they require a
packaging/submit layer planned for a future release.

```yaml
deployment:
  env: cluster
  target: standalone                   # local | standalone | yarn | kubernetes | databricks | emr | dataproc
  master_url: "spark://master:7077"    # consumed by SparkSession.builder.master()

spark_config:
  spark.sql.shuffle.partitions: "200"
  spark.hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
  spark.hadoop.fs.s3a.aws.credentials.provider: "com.amazonaws.auth.InstanceProfileCredentialsProvider"
```

`spark_config` keys are forwarded verbatim to `SparkSession.builder.config()`. Blueprint-level `spark_config` overrides engine-level keys on conflict.

**Catalog wiring (`table:` addressing).** When Blueprint modules use `table:` instead of `path:`, Spark resolves the `catalog.schema.table` identifier through the session's configured catalog. The catalog connection lives entirely in `spark_config` — standard Spark properties like `spark.sql.catalog.*`, no Aqueduct-specific config. See the [Spark Guide](spark_guide.md#catalog-wiring) for examples.

`table:` and `path:` are **mutually exclusive** on both Ingress and Egress — setting both raises a parse error.

Environment variable substitution works inside config values:

```yaml
deployment:
  master_url: "${SPARK_MASTER_URL:-local[*]}"   # falls back to local if unset
```

Running on a cluster:
```bash
aqueduct run pipeline.yml --config aqueduct.cluster.yml
```

In K8s, mount a PersistentVolumeClaim at `.aqueduct/` and `patches/` so observability state and patch history survive pod restarts.

### Per‑target configuration

#### local

```yaml
deployment:
  target: local
  master_url: "local[*]"              # or local[4], local[8], …
```

The session runs in‑process inside the driver JVM. No external connectivity
needed — `aqueduct doctor` always reports `ok`.

#### standalone

```yaml
deployment:
  target: standalone
  master_url: "spark://spark-master.internal:7077"
```

`master_url` must start with `spark://`. The doctor TCP‑probes `<host>:<port>`
from the master URL (default port 7077) to verify the Spark master is reachable
before a run.

#### yarn

```yaml
deployment:
  target: yarn
  master_url: "yarn"
  env: cluster
```

`master_url` must be exactly `"yarn"`. The driver uses the Hadoop configuration
on the node to locate the ResourceManager. Set `HADOOP_CONF_DIR` (or
`YARN_CONF_DIR`) to the directory containing `yarn-site.xml`:

```bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
```

`aqueduct doctor` warns if neither env var is set — the session will fail at
startup without them. The doctor does **not** build a SparkSession (that would
trigger a full YARN negotiation); use `--preflight` for a real session test.

#### kubernetes

```yaml
deployment:
  target: kubernetes
  master_url: "k8s://https://k8s-api.internal:6443"
  env: cluster

spark_config:
  spark.kubernetes.namespace: "aqueduct"
  spark.kubernetes.container.image: "my-registry/aqueduct:latest"
  spark.kubernetes.authenticate.driver.serviceAccountName: "aqueduct-sa"
```

`master_url` must start with `k8s://`. The doctor parses the API server
host:port from the URL and TCP‑probes it (port defaults to 443). It also warns
if no `spark.kubernetes.*` keys are present in `spark_config` — at minimum a
namespace and container image are normally required.

Credentials come from the pod's service account (no `@aq.secret()` needed when
using in‑cluster `spark.kubernetes.authenticate.*` config). For out‑of‑cluster
submission, add a kubeconfig path under `spark_config`.

### Write-mode features

**`mode: overwrite_partitions`** — two strategies for atomically replacing a subset
of a table:

| Strategy | Config | Behaviour |
|---|---|---|
| `replaceWhere` | `replace_where: "<predicate>"` (e.g. `"event_date = '2025-01-01'"`) | Delta `replaceWhere` — atomically replaces rows matching the predicate |
| Dynamic partition overwrite | `partition_by: ["col"]` + `mode: overwrite_partitions` | Spark dynamic partition overwrite — replaces only partitions in the output DataFrame |

When neither `replace_where` nor `partition_by` is set, falls back to plain `overwrite`.

**`format: custom` + `class:`** (Spark 4.0+). User-defined Python DataSources
(implements `DataSourceRegister`) on Ingress and Egress. The class must be
importable on the driver; `aqueduct doctor` verifies importability. As with
UDFs, the class body is opaque to Aqueduct.

**`on_new_columns`** — schema-drift contract (`allow` | `fail` | `alert`) on
Ingress and Egress (see the [Spark Guide](spark_guide.md) for the policy table).

**Iceberg / Hudi / Delta maintenance.** The Egress `maintenance:` block runs
format-aware post-write ops (`delta`: `OPTIMIZE` + `VACUUM`, `iceberg`:
`rewrite_data_files` + `expire_snapshots`, `hudi`: `run_compaction` +
`run_clean`). Timing lands in `maintenance_metrics`. `aqueduct doctor` emits an
`iceberg_catalog` warning when a `format: iceberg` module has no catalog key in
the blueprint `spark_config`.

---

## Path Conventions

**Rule: all I/O paths in production Blueprints must be cloud/HDFS URIs, never local filesystem paths.**

Local paths (`data/yellow/*.parquet`) are relative to the working directory of the driver process. In a container or remote driver, that directory contains no data.

**Correct pattern — context refs resolved from environment variables:**

```yaml
context:
  paths:
    yellow_input: "${YELLOW_DATA_PATH}"    # e.g. s3a://my-bucket/yellow/
    output:       "${OUTPUT_PATH}"         # e.g. s3a://my-bucket/output/

modules:
  - id: yellow_ingress
    type: Ingress
    config:
      path: "${ctx.paths.yellow_input}"
      format: parquet
```

**`allowed_paths` guardrail for production:**

```yaml
agent:
  guardrails:
    allowed_paths:
      - "s3a://my-bucket/**"
      - "hdfs://namenode/**"
      - "gs://my-gcs-bucket/**"
    forbidden_ops:
      - remove_module
      - insert_module
```

In `cluster` or `cloud` mode, `aqueduct doctor` warns when a Blueprint contains paths without a URI scheme.

---

## Observability State Persistence

`observability.db` (DuckDB) runs on the driver — always correct. Spark workers never access it.

| Deployment | Driver persistence | Action required |
|---|---|---|
| Local / dev | Permanent | None |
| YARN client mode | Permanent (edge node) | None |
| Spark standalone | Permanent (driver host) | None |
| Kubernetes | **Ephemeral** (pod) | Mount PVC at `.aqueduct/`, or use external stores (below) |

Each store has its own path under the `stores:` block in `aqueduct.yml`. In ephemeral environments, point the stores at a persistent mount:

```yaml
stores:
  observability:
    path: "/mnt/aqueduct-state/observability.db"
  depot:
    path: "/mnt/aqueduct-state/depot.db"
```

### Pod-native: external stores, zero local artefacts

Instead of a PVC, point every store at an external backend so the pod writes **nothing** to its cwd:

```yaml
stores:
  observability: { backend: postgres, path: "postgresql://aq@host/aqueduct_db" }
  depot:         { backend: postgres, path: "postgresql://aq@host/aqueduct_db" }
  blob:          { backend: s3, path: "s3://my-bucket/aqueduct" }   # needs [object-store]
```

The `blob` object store carries the two opaque artefact families that are not relational rows — observability blobs (fat `manifest_json` / `stack_trace` / `provenance_json`) and the patch lifecycle (`pending`/`applied`/`rejected`). With `backend: s3` (or `gcs` / `adls`) they land in object storage; the patch *bodies* sit there while their status lives in the `patch_index` table, so the heal cache works without a local `patches/` directory. **Incremental Channels** persist their watermark to the Depot only (the local sidecar was removed) — a Depot is now required for incremental state.

> The `stores.lineage` config option has been **removed** — `column_lineage` lives in `observability.db`. A legacy `stores.lineage` block in `aqueduct.yml` is ignored with a warning.

The `aqueduct run --store-dir <path>` CLI flag overrides the parent directory for a single invocation (useful for per-run isolation in CI / Kubernetes Jobs).

### Growing up: when to switch observability to Postgres

Per-pipeline DuckDB files are the right default: zero setup, single-writer is fine because one pipeline runs at a time, and the file sits next to the project. You have outgrown them when any of these become true:

- **Fleet questions** — "which of my N pipelines healed last night", "heal-rate trend across all blueprints" require querying N separate `.db` files; with Postgres every pipeline writes to one database (the engine creates `observability` / `lineage` / `depot` schemas per store automatically).
- **Many pipelines** — dozens of per-pipeline files under `.aqueduct/observability/*/` get awkward to back up, retain, and dashboard.
- **Ephemeral drivers** — Kubernetes Jobs / CI runners lose local files on every restart; a network DSN removes the PVC requirement above entirely.
- **Concurrent access** — a dashboard or `aqueduct runs` polling while a run is writing hits DuckDB's single-writer lock; Postgres MVCC does not care.

The switch is one config change (plus `pip install aqueduct-core[postgres]`):

```yaml
stores:
  observability:
    backend: postgres
    path: "postgresql://aq:${PGPASSWORD}@pg.internal:5432/aqueduct"
  depot:
    backend: postgres
    path: "postgresql://aq:${PGPASSWORD}@pg.internal:5432/aqueduct"
```

Tables are created on the first run — no migration step is required to start. Old per-pipeline DuckDB history is not imported automatically; it stays queryable in place (`duckdb .aqueduct/observability/<blueprint_id>/observability.db`). If you want the history in Postgres, DuckDB's `postgres` extension can copy it table-by-table:

```sql
-- inside `duckdb .aqueduct/observability/<blueprint_id>/observability.db`
INSTALL postgres; LOAD postgres;
ATTACH 'dbname=aqueduct user=aq host=pg.internal' AS pg (TYPE postgres);
INSERT INTO pg.observability.run_records       SELECT * FROM run_records;
INSERT INTO pg.observability.healing_outcomes  SELECT * FROM healing_outcomes;
-- repeat for failure_contexts, heal_attempts, probe_signals, module_metrics, …
```

Run the new-backend pipeline once first so the target tables exist, and mind schema drift: columns added by newer Aqueduct versions (e.g. `healing_outcomes.failure_signature`) may not exist in old DuckDB files — list columns explicitly if the `SELECT *` shapes differ.

---

## LLM Service Configuration

In production, LLM inference runs as a remote HTTP service.

**OpenAI-compatible endpoint (vLLM, Azure OpenAI, together.ai, etc.):**

```yaml
agent:
  provider: openai_compat
  base_url: "${LLM_BASE_URL}"
  model: "${LLM_MODEL}"
  timeout: 60
  approval: human
```

**Anthropic API:**

```yaml
agent:
  provider: anthropic
  model: claude-opus-4-7-20251001
  approval: human
```

Inject API keys (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`) via Kubernetes Secrets or your secrets manager. Never commit keys to Blueprint YAML or `aqueduct.yml`. As of 1.9, `agent.api_key` can be configured per blueprint or per cascade tier — always use `@aq.secret('KEY')` or `${ENV_VAR}`, never a plaintext literal (which triggers an `insecure_api_key` warning and is redacted from logs/LLM payloads). A project-wide cascade default can live in `aqueduct.yml` under `agent.cascade` (useful for fleets of blueprints sharing a heal policy); each Blueprint's own `agent.cascade` overrides it.

---

## Danger Settings

Certain features are disabled by default because they have destructive or expensive side-effects. They are grouped under a `danger:` block in `aqueduct.yml`. All keys default to `false`. If any key is set to `true`, Aqueduct prints a startup warning.

```yaml
danger:
  allow_multi_patch: false          # if true, approval: auto + max_patches > 1 is allowed
  allow_full_probe_actions: false   # if true, Probes may run expensive Spark actions
  allow_full_preflight: false       # if true, sandbox_mode: preflight is allowed
  allow_skip_sandbox: false         # if true, sandbox_mode: off is allowed (no pre-validation)
```

**Never set `danger.*: true` in a production `aqueduct.yml` checked into git.** Use environment-specific config overrides or per-run CLI flags.

---

## Patch Lifecycle in Production

The patch lifecycle differs between development and production.

**Development (local):**

```
pipeline fails → LLM generates patch → patch written to patches/pending/
→ human reviews → aqueduct patch apply → Blueprint updated → re-run
```

`approval: auto` is acceptable locally — git provides rollback.

**Production (recommended):**

**Rule: in production, use `approval: human`. The LLM must never autonomously modify a Blueprint managed by CI/CD.**

Recommended flow:

```
pipeline fails → webhook fires (alert to Slack/PagerDuty)
→ LLM generates patch → staged to patches/pending/
→ operator reviews patch JSON
→ aqueduct patch apply on a git branch → PR opened
→ CI/CD validates → human approves → merge → redeploy
```

**`approval: ci`:** When set, instead of writing to `patches/pending/`, Aqueduct stages the patch and fires a POST to `agent.ci_webhook_url`. The receiving CI system creates the branch and PR. Aqueduct does not couple to any git provider and ships no versioned GitHub Action — you wire a short workflow you own.

**CI webhook payload schema.** The `on_patch_pending` POST body carries:

| Key | Type | Notes |
|---|---|---|
| `patch_id` | string | Stable patch identifier (non-empty) |
| `run_id` | string | The failing run |
| `blueprint_id` | string | Blueprint that failed |
| `failed_module` | string \| null | Module id, or null when no single module |
| `source` | string | Origin of the patch (e.g. `llm`) |
| `root_cause`, `rationale`, `category`, `confidence` | string | Diagnostic extras (optional to consume) |
| `patch_path` | string | Where the body was staged |

The patch **body** (a PatchSpec JSON) additionally carries an `_aq_meta` block (`run_id`, `blueprint_id`, `failed_module`, `applied_at`, `approval_mode`, `prompt_version`, `failure_signature`). The receiver obtains the body — from a run artifact or `aqueduct patch pull <patch_id> --blueprint <bp>` — then applies + commits it:

```bash
aqueduct patch import received-patch.json --blueprint pipeline.yml
# → applies the patch, then `git commit` with a structured ---aqueduct--- trailer
```

`patch import` is the **one CI command**: it is `patch apply` + `patch commit` in a single atomic step (use `--no-commit` to stage only). A copy-paste example workflow wiring `import` + `gh pr create` lives at [`docs/templates/ci-heal-workflow.yml`](templates/ci-heal-workflow.yml) — a snippet you own, not a maintained Action.

**`on_patch_pending` webhook:** When a patch is staged (`approval: human`), Aqueduct fires `agent.webhooks.on_patch_pending` so teams receive a Slack/PagerDuty notification.

### `patches/rules.md`

`patches/rules.md` is a freeform Markdown file committed alongside Blueprints. Its content is appended verbatim after `agent.prompt_context` in the LLM system prompt. Use it to encode project-specific repair rules:

```markdown
## Project rules
- All I/O paths must be `s3a://my-bucket/**` globs — never local paths.
- Never change `mode: overwrite` on any Egress — data loss risk.
```

No code change needed to add a rule. Edit the file, commit it, rules apply on next run.

### `patches/` directory in production

| Directory | Commit to git? | Notes |
|---|---|---|
| `patches/applied/` | **Yes** | Audit trail |
| `patches/pending/` | No (`.gitignore`) | Ephemeral — human review staging area |
| `patches/rejected/` | No (`.gitignore`) | Ephemeral |
| `patches/rules.md` | **Yes** | Project-specific LLM repair rules |

### Git rollback

`aqueduct patch rollback` is a **development tool only**. In production: rollback = revert the commit in git → CI/CD redeployment.

---

## Security Considerations

| Concern | Mitigation |
|---|---|
| API keys in Blueprint YAML | Never. Use `${ENV_VAR}` refs. Inject via K8s Secrets / Vault. |
| LLM modifying paths beyond guardrails | Set `agent.guardrails.allowed_paths` to cloud URI patterns. |
| `auto` mode with `max_patches > 1` in production | Requires `danger.allow_multi_patch: true` — do not set in prod config. |
| `observability.db` exposure | Contains resolved config and error details. Restrict filesystem access. |
| Patch tampering | Planned: patch signature verification for `auto` multi-patch mode. |
| `danger.*` settings in shared config | Never commit `danger.*: true` to a shared/prod `aqueduct.yml`. |

---

## Delta Lake Operational Notes

When using `format: delta` Egress modules in production, Aqueduct handles reads and writes but does **not** run Delta maintenance operations. These must be scheduled externally:

| Operation | Why it matters | When to run |
|---|---|---|
| `OPTIMIZE` | Compacts small files written by incremental runs into larger files | After each incremental batch, or daily |
| `VACUUM` | Removes old file versions; controls storage growth | Daily or weekly (default retention: 7 days) |
| `ZORDER BY (col)` | Co-locates related data for data-skipping on filter columns | Run with `OPTIMIZE` when query patterns are known |

Example (run via `spark.sql()` in an external maintenance job):

```sql
OPTIMIZE delta.`s3://my-bucket/output/orders` ZORDER BY (event_date, region);
VACUUM delta.`s3://my-bucket/output/orders` RETAIN 168 HOURS;
```

Without `OPTIMIZE`, incremental pipelines using `mode: append` or `mode: merge` will accumulate small files over time, degrading read performance significantly.

---

## Remote-Submit Targets

`emr` and `dataproc` are **rejected at config‑load** in the current
release. Setting `deployment.target` to either of these two values raises
a `ConfigError`. `databricks` is the sole supported remote‑submit target
(see below).

The sections below (Databricks E2E, EMR, Dataproc) are forward‑looking
reference for EMR/Dataproc; the Databricks sections are fully wired.

### Databricks

**Prerequisites.** `aqueduct-core[databricks]` (optional SDK) or plain `httpx` (already a base dep). The Databricks cluster must have `aqueduct-core[spark]` installed as a library (pypi or init script).

**Configuration.** Add a `deployment.databricks` block to `aqueduct.yml`:

```yaml
deployment:
  target: databricks
  databricks:
    workspace_url: "https://dbc-xxxx.cloud.databricks.com"
    cluster_id: "0123-456789-abcdefgh"          # existing cluster — mutually exclusive with new_cluster
    max_concurrent_runs: 1                      # max parallel job runs submitted by this pipeline (default 1)
    # new_cluster:                               # one-shot cluster spec per Jobs API
    #   spark_version: "15.3.x-scala2.12"
    #   node_type_id: "i3.xlarge"
    #   num_workers: 4
    # libraries:                                 # optional — pypi libraries installed on the job cluster
    #   - pypi:
    #       package: "aqueduct-core[spark]"
    #   - pypi:
    #       package: "delta-spark"
```

**Credentials.** Set `DATABRICKS_TOKEN` in the submitting environment or reference it via `@aq.secret('DATABRICKS_TOKEN')` in the Blueprint context. The token is never placed in `aqeduct.yml` plaintext.

**Execution flow.**
1. `aqueduct run blueprint.yml` on the submitting machine
2. Blueprint + `aqeduct.yml` + bootstrap script uploaded to `dbfs:/aqueduct/jobs/<run_id>/`
3. `POST /api/2.1/jobs/runs/submit` with a `spark_python_task` running `aqueduct run dbfs:/.../blueprint.yml`
4. The local CLI polls `GET /api/2.1/jobs/runs/get` with exponential backoff (5s → 60s cap)
5. On success, exit `0`; on failure, exit `DATA_OR_RUNTIME(2)` with remote driver logs printed

**Doctor check.** `aqueduct doctor` includes a `remote-target` check — it verifies the workspace URL is reachable and a `DATABRICKS_TOKEN` is set. Status: `fail` (non-blocking — `run_doctor` continues to subsequent checks).

### EMR / Dataproc

Deferred. These targets raise `NotImplementedError` at runtime. They will reuse the existing `aws` / `gcp` extras for their submit clients when implemented.

## Production Readiness Checklist

Before promoting a Blueprint to production:

- [ ] All I/O paths use `${ctx.*}` refs resolved from environment variables (no hardcoded local paths)
- [ ] `aqueduct doctor pipeline.yml` passes with `deployment.env: cluster` or `cloud`
- [ ] `agent.guardrails.allowed_paths` set to cloud URI patterns
- [ ] `agent.approval: human` or `ci` (not `auto`)
- [ ] No `danger.*: true` in production `aqueduct.yml`
- [ ] API keys injected via environment (`@aq.secret()` reads `os.environ`)
- [ ] Store directories point to persistent paths (PVC in K8s, edge node in YARN)
- [ ] `patches/pending/` and `patches/rejected/` in `.gitignore`
- [ ] `patches/applied/` and `patches/rules.md` committed to git
- [ ] `agent.webhooks.on_patch_pending` configured if using `approval: human` in a team setting
- [ ] If using `format: delta`: external OPTIMIZE / VACUUM jobs scheduled