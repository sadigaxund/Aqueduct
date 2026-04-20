# Aqueduct

**Intelligent, self-healing Spark pipelines. Declarative. Observable. Autonomous.**

[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)

Aqueduct is a control plane for Apache Spark. You write pipelines as YAML *Blueprints*. Aqueduct validates, compiles, and executes them—monitoring every step. When something breaks, Aqueduct can **autonomously patch the pipeline** using an LLM agent, applying structured, auditable fixes.

> **Vibe-Transform-Load (VTL) for Apache Spark.**

---

## What Makes Aqueduct Different?

- **Declarative YAML Blueprints** — No DAG wiring in code. Version-control your entire pipeline.
- **Any Spark Connector** — Pass any format Spark supports (JDBC, Kafka, Avro, ORC, Delta, Parquet, CSV…) directly in config. Aqueduct adds no format restrictions.
- **LLM-First Observability** — Every failure ships with a complete `FailureContext` JSON, ready for an agent (or you) to diagnose without digging through logs.
- **Patch Grammar, Not Codegen** — The LLM operates inside a structured `PatchSpec` schema. Patches are auditable, reversible, and never hallucinate invalid YAML.
- **Zero-Cost Observability** — Probes capture schema snapshots, null rates, and sample rows using lazy Spark operations and sampling. No full scans.
- **Spillway Error Routing** — Bad rows route to a separate error Egress with `_aq_error_*` metadata columns. Good rows flow uninterrupted.
- **Depot KV Store** — Cross-run pipeline state (watermarks, counters) backed by DuckDB. Read at compile time via `@aq.depot.get()`, write at runtime via `format: depot` Egress.
- **Passive-by-Default Gates** — Regulators (data quality gates) compile away entirely unless wired to a Probe signal. Zero overhead for unused features.

---

## Open-Core Model

**Aqueduct Core is Apache 2.0 licensed and always will be.**

You can run it locally, in CI, or on a production Spark cluster — free, forever.

A commercial frontend, **Aqueduct Platform**, adds:
- Centralized dashboards for all your pipelines
- Team collaboration and RBAC
- Managed Depot (persistent KV store)
- Audit logs of every LLM patch

The Core engine emits a documented webhook event stream so you can integrate it with any frontend — ours, yours, or a third-party.

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

The base package installs the CLI, parser, compiler, and LLM self-healing. All LLM providers (Anthropic, OpenAI-compatible, Ollama) use `httpx` which is a core dependency — no extra install needed. Spark execution requires the `[spark]` extra (`pyspark`, `delta-spark`).

Requires Python 3.11+ and Java 17 (for local Spark).

---

## Quick Start

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
  observability:
    path: ".aqueduct/signals"
  depot:
    path: ".aqueduct/depot.duckdb"
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
| `Probe` | Capture observability signals (schema, null rates, sample rows) — never halts pipeline |
| `Regulator` | Data quality gate; evaluates Probe signals; blocks or skips downstream on failure |
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

---

## CLI Reference

```bash
aqueduct validate pipeline.yml              # Parse and validate only
aqueduct compile  pipeline.yml              # Output resolved Manifest JSON
aqueduct run      pipeline.yml              # Compile and execute
aqueduct run      pipeline.yml \
  --config aqueduct.yml \
  --store-dir .aqueduct/signals \
  --run-id my-run-001 \
  --ctx env=prod

aqueduct patch apply patch.json --blueprint pipeline.yml
aqueduct patch reject <patch-id> --reason "Incorrect column name"
```

---

## Observability

Aqueduct writes all observability data to DuckDB files:

```bash
# Run records
duckdb .aqueduct/signals/runs.db
SELECT run_id, pipeline_id, status, started_at, finished_at FROM run_records;

# Probe signals
duckdb .aqueduct/signals/signals.db
SELECT probe_id, signal_type, payload FROM probe_signals ORDER BY captured_at DESC;

# Depot state
duckdb .aqueduct/depot.duckdb
SELECT key, value, updated_at FROM depot_kv;
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
git clone https://github.com/your-org/aqueduct
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
