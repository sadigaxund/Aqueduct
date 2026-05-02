# Aqueduct

**Intelligent, self-healing Spark pipelines. Declarative. Observable. Autonomous.**

[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)

Aqueduct is a control plane for Apache Spark. You write pipelines as YAML *Blueprints*. Aqueduct validates, compiles, and executes them while monitoring every step. When something breaks, Aqueduct can **autonomously patch the pipeline** using an LLM agent, applying structured, auditable fixes.

---

## What Makes Aqueduct Different?

- **Declarative YAML Blueprints** - No DAG wiring in code. Version-control your entire pipeline.
- **Any Spark Connector** - Pass any format Spark supports (JDBC, Kafka, Avro, ORC, Delta, Parquet, CSV…) directly in config. Aqueduct adds no format restrictions.
- **LLM-First Observability** - Every failure ships with a complete `FailureContext` JSON, ready for an agent (or you) to diagnose without digging through logs.
- **Patch Grammar, Not Codegen** - The LLM operates inside a structured `PatchSpec` schema. Patches are auditable, reversible, and never hallucinate invalid YAML.
- **Zero-Cost Observability** - Probes capture schema snapshots, null rates, value distributions, distinct counts, freshness, and sample rows using lazy Spark operations and sampling. No full scans in production mode (`block_full_actions_in_prod`).
- **Spillway Error Routing** - Bad rows route to a separate error Egress with `_aq_error_*` metadata columns. Good rows flow uninterrupted.
- **Depot KV Store** - Cross-run pipeline state (watermarks, counters) backed by DuckDB. Read at compile time via `@aq.depot.get()`, write at runtime via `format: depot` Egress.
- **Passive-by-Default Gates** - Regulators (data quality gates) compile away entirely unless wired to a Probe signal. Zero overhead for unused features.

---

## Open-Core Model

**Aqueduct Core is Apache 2.0 licensed and always will be.**

You can run it locally, in CI, or on a production Spark cluster - free, forever.

A commercial frontend, **Aqueduct Platform**, adds:
- Centralized dashboards for all your pipelines
- Team collaboration and RBAC
- Managed Depot (persistent KV store)
- Audit logs of every LLM patch

The Core engine emits a documented webhook event stream so you can integrate it with any frontend: ours, yours, or a third-party.

**This repository contains the full engine. No telemetry. No proprietary code.**

---

## Installation

```bash
# Core CLI + parser + compiler + LLM self-healing (no Spark dependency)
pip install aqueduct-core

# With Spark execution (required for aqueduct run)
pip install aqueduct-core[spark]

# Everything
pip install aqueduct-core[all]
```

The base package installs the CLI, parser, compiler, and LLM self-healing. All LLM providers (Anthropic, OpenAI-compatible, Ollama) use `httpx` which is a core dependency - no extra install needed. Spark execution requires the `[spark]` extra (`pyspark`, `delta-spark`).

Requires Python 3.11+ and Java 17 (for local Spark).

---

## Quick Start

```bash
mkdir my-pipeline && cd my-pipeline
aqueduct init --name my-pipeline
# Creates: blueprints/example.yml, aqueduct.yml, .gitignore, patches/, arcades/
# Runs git init + initial commit automatically
```

Or manually:

### 1. Write a Blueprint

```yaml
# pipeline.yml
aqueduct: "1.0"
id: my.first.pipeline
name: "Order Processing"

context:
  today: "@aq.date.today()"

modules:
  - id: raw_orders
    type: Ingress
    label: "Load orders"
    config:
      format: parquet
      path: "s3a://data/orders/"

  - id: clean_orders
    type: Channel
    label: "Filter invalid rows"
    config:
      op: sql
      query: |
        SELECT *
        FROM raw_orders
        WHERE amount IS NOT NULL
          AND order_date <= CURRENT_TIMESTAMP()
      spillway_condition: "amount IS NULL OR order_date > CURRENT_TIMESTAMP()"

  - id: good_output
    type: Egress
    label: "Write clean orders"
    config:
      format: parquet
      path: "s3a://processed/orders/date=${ctx.today}/"
      mode: overwrite

  - id: error_output
    type: Egress
    label: "Write rejected rows"
    config:
      format: parquet
      path: "s3a://errors/orders/date=${ctx.today}/"
      mode: overwrite

edges:
  - from: raw_orders
    to: clean_orders
  - from: clean_orders
    to: good_output
  - from: clean_orders
    to: error_output
    port: spillway
```

### 2. Configure the engine

```yaml
# aqueduct.yml
aqueduct_config: "1.0"

deployment:
  master_url: "spark://your-cluster:7077"

spark_config:
  spark.hadoop.fs.s3a.endpoint: "http://minio:9000"
  spark.hadoop.fs.s3a.access.key: "@aq.secret('S3_KEY')"
  spark.hadoop.fs.s3a.secret.key: "@aq.secret('S3_SECRET')"
  spark.jars.packages: "org.apache.hadoop:hadoop-aws:3.3.4"

stores:
  obs:
    path: ".aqueduct/obs.db"
  lineage:
    path: ".aqueduct/lineage.db"
  depot:
    path: ".aqueduct/depot.db"
```

### 3. Run

```bash
aqueduct run pipeline.yml --config aqueduct.yml
```

```
▶ my.first.pipeline  (4 modules)  run=abc123  master=spark://your-cluster:7077
  ✓ raw_orders
  ✓ clean_orders
  ✓ good_output
  ✓ error_output

✓ pipeline complete  run_id=abc123
```

---

## Core Concepts

### Blueprint

A YAML file describing your pipeline. Contains modules (data sources, transforms, sinks) and edges (data flow).

### Manifest

The compiled, fully-resolved form of a Blueprint. All `@aq.*` runtime tokens resolved, Arcades expanded, passive Regulators compiled away. This is what the Executor runs.

### Module Types

| Type | Role |
|---|---|
| `Ingress` | Load data from any Spark-supported source |
| `Egress` | Write data to any Spark-supported sink, or `format: depot` for KV state |
| `Channel` | SQL transform; optional `spillway_condition` routes bad rows |
| `Junction` | Split one DataFrame into named branches (conditional / broadcast / partition) |
| `Funnel` | Merge multiple DataFrames (union_all / union / coalesce / zip) |
| `Probe` | Capture observability signals (schema, null rates, value distribution, distinct counts, freshness, partition stats, sample rows) — never halts pipeline |
| `Regulator` | Data quality gate; evaluates Probe signals; blocks or skips downstream on failure |
| `Assert` | Inline data quality rules (schema, row counts, null rates, freshness, SQL, custom); failing rows route to spillway |
| `Arcade` | Reusable sub-Blueprint; namespaced and inlined at compile time |

### Runtime Functions (`@aq.*`)

Resolved at compile time on the driver:

```yaml
path: "s3a://data/date=@aq.date.today()/"
path: "s3a://data/from=@aq.depot.get('last_run_date', '2020-01-01')/"
run_id: "@aq.runtime.run_id()"
prev_run: "@aq.runtime.prev_run_id()"
key: "@aq.secret('MY_SECRET')"
```

### Channel `op: join`

Higher-level join syntax — no raw SQL needed for common join patterns:

```yaml
- id: enrich_orders
  type: Channel
  config:
    op: join
    left: orders_ingress       # upstream module ID (registered as temp view)
    right: customers_ingress   # upstream module ID
    join_type: left            # inner | left | right | full | semi | anti | cross
    condition: "orders_ingress.customer_id = customers_ingress.id"
    broadcast_side: right      # optional: BROADCAST hint for smaller side
```

For complex multi-table joins or window functions, use `op: sql` directly.

### SQL Macros

Define reusable SQL fragments at compile time — resolved before Spark runs, no runtime overhead:

```yaml
macros:
  active: "status = 'active' AND deleted_at IS NULL"
  trunc:  "DATE_TRUNC('{{ period }}', {{ col }})"

modules:
  - id: clean
    type: Channel
    config:
      op: sql
      query: |
        SELECT * FROM src
        WHERE {{ macros.active }}
          AND {{ macros.trunc(period='day', col=event_ts) }} >= '2026-01-01'
```

Macros are static — no loops, no conditionals. The Manifest always contains plain SQL.

### Spillway

Route bad rows to a separate sink without stopping the pipeline:

```yaml
- id: clean
  type: Channel
  config:
    op: sql
    query: "SELECT * FROM __input__"
    spillway_condition: "amount IS NULL"  # rows matching this → spillway port

edges:
  - from: clean
    to: good_sink           # clean rows
  - from: clean
    to: error_sink
    port: spillway          # null-amount rows with _aq_error_* columns
```

### Assert — Inline Data Quality Gates

Enforce explicit business rules against the flowing DataFrame. Unlike Probe + Regulator (which evaluate signals from a prior run), Assert evaluates rules inline and can quarantine bad rows to a spillway:

```yaml
- id: orders_quality
  type: Assert
  label: "Quality gate"
  config:
    rules:
      - type: schema_match
        expected: {order_id: long, amount: double}
        on_fail: abort

      - type: min_rows
        min: 1000
        on_fail: abort

      - type: null_rate
        column: order_id
        fraction: 0.1      # sample 10% for speed
        max: 0.001
        on_fail: abort

      - type: freshness
        column: event_time
        max_age_hours: 4
        on_fail: warn

      - type: sql
        expr: "SUM(amount) > 0"
        on_fail:
          action: webhook
          url: "${ALERT_WEBHOOK}"

      - type: sql_row
        expr: "amount > 0 AND order_id IS NOT NULL"
        on_fail: quarantine    # bad rows → spillway port

edges:
  - from: clean_orders
    to: orders_quality
  - from: orders_quality
    to: good_egress
  - from: orders_quality
    port: spillway
    to: quarantine_egress
```

**Performance:** All aggregate rules (`min_rows`, `freshness`, `sql`) batch into one `df.agg()`. All `null_rate` rules share one `df.sample().agg()`. Schema match is zero-action. Row-level rules (`sql_row`, `custom`) are lazy filters. **At most 2 Spark actions** per Assert module.

**vs Spillway:** Spillway catches UDF runtime exceptions (bad rows discovered during execution). Assert enforces explicit business rules you declare up front. They solve different problems and compose naturally.

### Depot KV Store

Persist state across pipeline runs:

```yaml
# Read at compile time
path: "s3a://data/from=@aq.depot.get('last_processed_date', '2020-01-01')/"

# Write at runtime (Egress module)
- id: save_watermark
  type: Egress
  config:
    format: depot
    key: last_processed_date
    value_expr: "MAX(order_date)"   # single Spark aggregate
```

### `aqueduct test` — Isolated Module Testing

Test Channel, Junction, Funnel, and Assert modules against inline data — no real sources, no external I/O:

```yaml
# pipeline.aqtest.yml
aqueduct_test: "1.0"
blueprint: pipeline.yml

tests:
  - id: test_filter_nulls
    description: "Null amounts must be removed"
    module: clean_orders

    inputs:
      raw_orders:
        schema:
          order_id: long
          amount: double
        rows:
          - [1, 10.0]
          - [2, null]

    assertions:
      - type: row_count
        expected: 1
      - type: contains
        rows:
          - {order_id: 1, amount: 10.0}
      - type: sql
        expr: "SELECT count(*) = 1 FROM __output__"
```

```bash
aqueduct test pipeline.aqtest.yml
aqueduct test pipeline.aqtest.yml --blueprint pipeline.yml --quiet
```

**Assertion types:** `row_count` (exact count), `contains` (rows must appear in output), `sql` (SQL expression over `__output__` view returns truthy).

### Patch Dry-Run (`validate_patch`)

Enable pre-validation before the LLM agent applies patches in `aggressive` mode. If the patched Blueprint doesn't compile, the patch is staged for human review instead of applied:

```yaml
agent:
  approval_mode: aggressive
  validate_patch: true     # compile-check patch before writing to disk
  max_patches_per_run: 5
```

With `validate_patch: true`: the patched Blueprint is compiled in memory first. Only if compilation succeeds does Aqueduct write it to disk and continue the self-healing loop. Invalid patches are staged in `patches/pending/` for human review.

---

## CLI Reference

```bash
aqueduct init                               # Scaffold new project in current directory
aqueduct init --name my-pipeline           # Set project name (used for blueprint ID)

aqueduct validate pipeline.yml              # Parse and validate only
aqueduct compile  pipeline.yml              # Output resolved Manifest JSON
aqueduct compile  pipeline.yml --execution-date 2026-01-15  # backfill: pin @aq.date.* to date

aqueduct run      pipeline.yml              # Compile and execute
aqueduct run      pipeline.yml \
  --config aqueduct.yml \
  --store-dir .aqueduct \
  --run-id my-run-001 \
  --ctx env=prod

# Sub-DAG execution — run only a slice of the pipeline
aqueduct run pipeline.yml --from clean_orders              # from module onwards
aqueduct run pipeline.yml --from clean_orders --to egress  # inclusive range
aqueduct run pipeline.yml --to egress                      # up to and including module

# Backfill / logical execution date — pins @aq.date.today() and @aq.runtime.timestamp()
aqueduct run pipeline.yml --execution-date 2026-01-15

aqueduct check-config                       # Validate aqueduct.yml schema; print resolved summary
aqueduct check-config --config path/to/aqueduct.yml

aqueduct doctor                             # Probe all resources end-to-end (config, stores, secrets, webhook, Spark, storage)
aqueduct doctor --skip-spark               # Skip JVM startup — fast CI health check
aqueduct doctor --config path/to/aqueduct.yml

aqueduct test     pipeline.aqtest.yml       # Run isolated module tests (no Spark I/O)
aqueduct test     pipeline.aqtest.yml --quiet

aqueduct report   <run_id>                  # Flow Report for a completed run (--format table|json|csv)
aqueduct lineage  <blueprint_id>             # Column lineage graph (--from <table>, --column <col>)

# Persistent gate override — affects ALL future runs until cleared
aqueduct signal   <signal_id> --value false           # close gate (block downstream)
aqueduct signal   <signal_id> --error "Stale source"  # close with reason
aqueduct signal   <signal_id> --value true            # clear override (resume normal evaluation)
aqueduct signal   <signal_id>                         # show current override status

aqueduct heal     <run_id>                  # manually trigger LLM self-healing for a failed run
aqueduct heal     <run_id> --module <id>   # scope healing to a specific module

aqueduct runs                               # list recent runs (all blueprints)
aqueduct runs --blueprint blueprint.yml     # filter by blueprint
aqueduct runs --failed                      # show only failed runs
aqueduct runs --last 20                     # show last N runs

aqueduct patch apply patch.json --blueprint pipeline.yml
aqueduct patch reject <patch-id> --reason "Incorrect column name"
aqueduct patch rollback <patch-id> --blueprint pipeline.yml  # restore Blueprint from backup
```

### Key `aqueduct run` flags

| Flag | Description |
|---|---|
| `--from <module_id>` | Start execution from this module (inclusive); skips all upstream modules |
| `--to <module_id>` | Stop execution at this module (inclusive); skips all downstream modules |
| `--execution-date YYYY-MM-DD` | Pin logical date for `@aq.date.*` and `@aq.runtime.timestamp()` — enables backfill idempotency |
| `--resume <run_id>` | Resume from checkpoints written by a previous run with `checkpoint: true` |
| `--run-id <uuid>` | Override auto-generated run UUID (useful for idempotent reruns) |
| `--ctx key=value` | Override a Blueprint `context:` variable |
| `--profile <name>` | Activate a `context_profiles:` entry |

---

## Observability

Aqueduct writes all observability data to DuckDB files under `.aqueduct/`:

```
.aqueduct/
  obs.db          — run records, probe signals, module metrics, signal overrides
  lineage.db      — column-level lineage
  depot.db        — depot KV store (@aq.depot.*)
  snapshots/      — schema_snapshot JSON files (one per probe per run)
```

```bash
# Run records and probe signals (all in obs.db)
duckdb .aqueduct/obs.db
SELECT run_id, blueprint_id, status, started_at, finished_at FROM run_records;
SELECT probe_id, signal_type, payload FROM probe_signals ORDER BY captured_at DESC;

# Depot state
duckdb .aqueduct/depot.db
SELECT key, value, updated_at FROM depot_kv;

# Column lineage
duckdb .aqueduct/lineage.db
SELECT channel_id, output_column, source_table, source_column FROM column_lineage;
```

Or use the CLI:

```bash
aqueduct runs                        # list recent runs
aqueduct runs --failed --last 10     # last 10 failed runs
aqueduct report <run_id>             # detailed flow report
aqueduct lineage <blueprint_id>      # column lineage graph
```

---

## Architecture

```
Blueprint (YAML)
    │
    ▼
┌─────────┐
│ Parser  │  Validate schema, resolve ${ctx.*}, detect cycles
└────┬────┘
     │ Blueprint AST
     ▼
┌──────────┐
│ Compiler │  Resolve @aq.* tokens, expand Arcades, compile-away passive Regulators
└────┬─────┘
     │ Manifest (JSON)
     ▼
┌──────────┐
│ Executor │  Topo-sort → frame_store → Spark actions
└────┬─────┘
     │ ExecutionResult
     ▼
┌──────────┐
│ Surveyor │  Write run record, FailureContext, fire webhooks
└──────────┘
```

---

## Development

```bash
git clone https://github.com/sadigaxund/aqueduct
cd aqueduct
pip install -e ".[spark,dev]"

# Run tests (requires Java 17)
source ~/.bashrc && use_java17
pytest tests/
```

Tests include unit tests and blueprint integration tests that run real Spark (`local[*]`). Coverage target: 80%.

---

## License

Apache 2.0. See [LICENSE](LICENSE).
