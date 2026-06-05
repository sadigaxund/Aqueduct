# Aqueduct — Blueprint & Engine Reference

**Version 1.1 — Reference Document**

*Self-healing LLM-integrated pipelines for Apache Spark*
*Declarative · Observable · Autonomous · Self-healing*

Blueprint · Module · Ingress · Channel · Egress · Junction · Funnel · Probe · Regulator · Spillway · Arcade · Surveyor

> **Document map:** This document covers the Blueprint format, engine architecture, module types, and self-healing agent. Companion docs cover specific areas:
> - **[CLI Reference](cli_reference.md)** — All commands and flags
> - **[Spark Guide](spark_guide.md)** — Compiler warnings, performance, tuning
> - **[Observability Guide](observability_guide.md)** — Store schemas and diagnostic query cookbook
> - **[Production Guide](production_guide.md)** — Cluster deployment, security, Delta operations
> - **[Roadmap](roadmap.md)** — Deferred features and future plans

---

# **1. Introduction**

## **1.1 Purpose of This Document**

This document is the complete system design and implementation reference for Aqueduct — an intelligent, declarative Spark pipeline engine with integrated LLM-driven self-healing. It covers every component, design decision, data contract, configuration schema, and runtime behaviour.

## **1.2 What Aqueduct Is**

Aqueduct is a control plane for Apache Spark. It does not replace Spark — it wraps it. Engineers and LLM agents author pipelines as YAML Blueprint files. Aqueduct parses, validates, compiles, plans, and executes those Blueprints as Spark jobs, monitoring them continuously and autonomously patching failures when they occur.

The name is deliberate: a Roman aqueduct is precision-engineered infrastructure for carrying flow reliably across vast distances, planned on actual blueprints (forma), and built to strict tolerances. Aqueduct the software carries data flow with the same philosophy — structured, observable, and resilient.

## **1.3 Primary Users**

- Data engineers (code-first) — author and maintain Blueprints, review patches, configure retry policies.
- LLM agents — primary runtime operators; diagnose failures, propose and apply Patches autonomously.
- Platform operators — deploy and configure the engine, manage deployment targets and credentials.

## **1.4 Design Principles**

These principles govern every design decision in the system. When two requirements conflict, the higher principle wins.

| Principle | Description |
| :- | :- |
| **P1 — LLM-first observability** | Every failure must carry enough structured context for an agent to diagnose and patch without additional queries. |
| **P2 — Blueprint as truth** | The Blueprint is the single source of truth. Nothing about a pipeline exists outside the Blueprint and its derived Manifest. |
| **P3 — Performance non-regression** | Aqueduct adds no hidden Spark actions. Any action beyond the pipeline's own Egress writes is the result of a user-configured Probe, Assert, or incremental watermark. |
| **P4 — Static resolution first** | Any value that can be resolved at parse time must be. Runtime resolution is explicit, opt-in, and visually distinct in Blueprint syntax. |
| **P5 — Patch grammar over codegen** | The LLM agent operates within a structured Patch grammar, not free-form code generation. Every patch is schema-valid, auditable, and reversible. |
| **P6 — Passive-by-default gates** | Flow control constructs (Regulators, Spillways) do not exist in the execution path unless explicitly wired. Unwired gates compile away entirely. |
| **P7 — Pure Spark type system** | Aqueduct owns no type system. All types are Spark DDL strings. Semantic constraints are annotations, not types. |
| **P8 — Provenance-aware LLM context** | Every value the LLM reasons about is backed by a compile-time provenance index. The agent never receives raw Blueprint YAML to reverse-engineer — it receives resolved values tagged with their origin. |

---

# **2. Naming Glossary**

These names are canonical and used consistently throughout the codebase, documentation, logs, and LLM prompts.

| Term | Definition |
| :- | :- |
| **Aqueduct** | The engine itself. The full system described in this document. |
| **Module** | The smallest indivisible unit of a pipeline. Every step in a Blueprint is a Module. Typed: Ingress, Channel, Egress, Junction, Funnel, Probe, Regulator, Arcade, Assert. |
| **Blueprint** | The YAML file authored by an engineer or agent. Defines a complete pipeline: its Modules, edges, Context Registry, retry policy, and agent config. |
| **Manifest** | The compiled, fully-resolved JSON form of a Blueprint after all Context Registry substitution and Arcade expansion. What Aqueduct actually executes. |
| **Context Registry** | The variable system for Blueprints. Tier 0 (static) values resolved at parse time. Tier 1 (@aq.*) values resolved before Spark jobs start. |
| **Depot** | Aqueduct's persistent key-value state store. Pipelines read and write named keys across runs. |
| **Ingress** | Module type: reads data from an external source into the pipeline. |
| **Channel** | Module type: applies a transformation to one or more upstream DataFrames. No Spark actions. |
| **Egress** | Module type: writes data to an external target or triggers a collection action. The only Module type that materialises results and costs a Spark action. |
| **Junction** | Module type: splits one incoming DataFrame into multiple downstream branches (fan-out). |
| **Funnel** | Module type: merges multiple upstream DataFrames into one (fan-in). |
| **Probe** | Module type: non-blocking observability tap attached to a Module's output edge. Zero Spark actions by default. |
| **Regulator** | Module type: trigger gate. Passive by default — if nothing is wired to it, it does not exist in the execution path. |
| **Spillway** | The error output port present on every Module. Routes row-level errors to a designated downstream Module. |
| **Arcade** | Module type: an encapsulated, reusable sub-pipeline embedded as a single Module in a parent Blueprint. Expanded at compile time. |
| **Surveyor** | The runtime supervisor process. Monitors pipeline execution, evaluates health signals, manages retry policy, triggers LLM self-healing. |
| **Patch** | A structured diff to a Blueprint proposed by the LLM agent or a human. Expressed as a PatchSpec JSON. |
| **Flow Report** | The post-run column-level quality report. Shows per-column status (OK / Degraded / Error) across each Module. |
| **FailureContext** | The structured failure document assembled by the Surveyor when a pipeline run ends in error. Passed to the LLM self-healing loop. |
| **PatchSpec** | The JSON document that describes a set of operations to apply to a Blueprint. Produced by the LLM agent or authored by hand. |
| **ProvenanceMap** | A compile-time index of every resolved config value: where it came from (literal, context ref, env var, Arcade inheritance), the original expression, and the resolved value. |

---

# **3. System Architecture**

## **3.1 High-Level Overview**

Aqueduct has four processing layers and three persistent stores. Each layer has a defined input/output contract and can be developed and tested independently.

| Layer | Input | Output | Responsibility |
| :- | :- | :- | :- |
| 1 — Parser | Blueprint YAML | Validated AST | Schema validation, Context Tier 0 resolution, cycle detection, Arcade loading |
| 2 — Compiler | AST + Context map | Manifest (JSON) | Interpolate `${ctx.*}` refs, resolve `@aq.*` functions, expand Arcades, wire Probes and Spillways |
| 3 — Executor | Manifest | RunRecord + metrics | Topological sort, submit Spark jobs, attach SparkListener, stream events to Observability Store |
| 4 — Surveyor | Live run signals | HealthEvents + Patches | Monitor health, apply retry policy, invoke LLM loop, apply approved Patches |

## **3.2 Persistent Stores**

| Store | Description |
| :- | :- |
| **Observability Store** | Append-only log of all runtime signals: Probe readings, stage metrics, errors. Per-pipeline routing (1.1.0+): `.aqueduct/observability/<blueprint_id>/observability.db`. |
| **Lineage Store** | Column lineage graphs and Flow Reports. Stored in `column_lineage` table inside `observability.db` (1.1.2+ — previously a separate `lineage.db`). The `stores.lineage` config block is inert. |
| **Depot (KV Store)** | Persistent key-value store for pipeline state across runs: watermarks, last-run metadata. Project-wide (not per-pipeline) so blueprints can read each other's watermarks. |

## **3.3 Component Interaction Flow**

```
Blueprint.yml → Parser → Validated AST → Compiler → Manifest (JSON) → Executor → RunRecord → Surveyor → Flow Report
                                                                              ↓ failure
                                                                       Agent loop → PatchSpec → Gates → apply
```

On the happy path the flow is linear: Parser → Compiler → Executor → Surveyor.

1. **Parse + Compile.** Parser validates YAML against the JSON Schema, builds the AST, resolves Tier 0 context refs. Compiler resolves Tier 1 (`@aq.*`) calls — secrets, Depot reads, dates — expands Arcades into a flat module list, and emits the Manifest plus `provenance_map` and `inputs_fingerprint`.
2. **Execute.** The Executor topologically sorts the modules, inserts Probes after their `attach_to` targets, identifies independent connected components for `--parallel` mode, and runs each module through its handler. Per-module metrics and Probe signals are written to the observability store as they fire.
3. **Surveyor + Agent loop (when triggered).** On failure, the Surveyor packages a FailureContext (error trace + provenance slice + recent signals + lineage), calls the configured LLM, and receives a PatchSpec. Patches are validated through gates (guardrails → compile-check → lineage → sandbox). The `apply_callback` writes the patch to disk, recompiles the Manifest, and the executor re-runs the pipeline.

### **Why a Manifest? Why not run the YAML directly?**

The compile step is not cosmetic — the Executor consumes the Manifest, never the raw YAML. Several Blueprint constructs cannot be resolved at execution time:

| Blueprint construct | Why resolve at compile, not run |
| :- | :- |
| `${ctx.foo}` Tier 0 context refs | Substitution must happen before any module sees its config to ensure consistency. |
| `@aq.date.today()`, `@aq.runtime.timestamp()` Tier 1 calls | Resolving at execution time would tie the value to the moment each module ran — two modules calling `today()` at different stages of a 4-hour pipeline could see different dates. |
| `@aq.secret('KEY')` | One network round-trip per run, not one per module per worker thread. |
| `@aq.depot.get('watermark')` | A single DuckDB read at compile time prevents race conditions with runtime writes. |
| Arcade `ref: arcades/foo.yml` | Sub-Blueprints are expanded inline so the executor sees a single flat module list. |
| Macros `{{ macros.* }}` | Spark SQL cannot parse `{{ }}` placeholders — expansion must happen before Spark sees the query. |
| Passive Regulators | Regulators with no wired signal input are compiled away entirely. |

The Manifest also carries the **ProvenanceMap** (per-config-key audit trail recording where every value came from) and **`inputs_fingerprint`** (compile-time snapshot of Ingress file metadata for the LLM to distinguish data-drift bugs from code bugs).

---

# **4. Blueprint Format**

## **4.1 File Format & Versioning**

Blueprints are YAML files. The format is versioned — the `aqueduct` field selects the JSON Schema version. Unknown fields at any level are hard errors. This guarantees Blueprints are always valid input for LLM patch generation.

## **4.2 Top-Level Structure**

```yaml
aqueduct: "1.0"                        # schema version — required
id: pipeline.orders.daily_aggregate    # globally unique pipeline ID
name: "Daily Orders Aggregation"       # human display name

description: |
  Reads raw orders, deduplicates by order_id,
  aggregates by region, writes to Delta.

context:                               # Context Registry
  env: ${AQUEDUCT_ENV:-dev}
  tables:
    orders_raw: "s3://data/${ctx.env}/orders/raw"
    orders_out: "s3://data/${ctx.env}/orders/daily"

context_profiles:                      # environment promotion
  dev:
    tables.orders_raw: "s3://dev/orders/raw"
  prod:
    tables.orders_raw: "s3://prod/orders/raw"

modules:                               # Module list
  - id: read_orders
    type: Ingress

edges:                                 # explicit edge definitions
  - from: read_orders
    to:   dedup_orders
    port: main                         # main (default) or spillway

spark_config:                          # merged with Aqueduct defaults
  spark.sql.shuffle.partitions: 200
  spark.sql.adaptive.enabled: true

retry_policy:                          # per-pipeline retry config
  max_attempts: 3
```

## **4.3 Module Schema — Common Fields**

Every Module regardless of type shares these fields:

| Field | Description |
| :- | :- |
| **id** | Required. Unique string within the Blueprint. Must be filesystem-safe. |
| **label** | Required. Human-readable display name. |
| **type** | Required. One of: Ingress, Channel, Egress, Junction, Funnel, Probe, Regulator, Arcade, Assert. |
| **description** | Optional. Free-text explanation. Used in LLM context and UI. |
| **tags** | Optional list of strings. Used for filtering and scoped search. |
| **config** | Type-specific configuration block. |
| **spillway** | Optional downstream Module ID to receive error-port output. |
| **depends_on** | Optional explicit upstream dependency list. |
| **checkpoint** | Optional boolean. When true, output DataFrame is saved as Parquet for `--resume`. |

### Ports

| Port | Carries | Where it's produced | Where it's consumed |
| :- | :- | :- | :- |
| `main` (default) | Successful DataFrame | Every module type | Every module type |
| `spillway` | Row-level error DataFrame | Channel, Assert | Egress (quarantine sink) |
| `signal` | Control signal, not a DataFrame | Probe (threshold signal) | Regulator (gate evaluation) |
| `<branch_id>` | One subset of the upstream Junction's branches | Junction | Any downstream module |

## **4.4 Module Types — Full Specification**

### Ingress

```yaml
- id: read_orders
  type: Ingress
  label: "Read raw orders from S3 Parquet"
  config:
    format: parquet              # parquet | delta | csv | json | jdbc | kafka | custom
    path: ${ctx.tables.orders_raw}
    partition_filters: "event_date >= '${ctx.start_date}'"
    schema_hint:                 # optional — enforced at read time
      order_id: STRING
      amount: DECIMAL(18,2)
    options:
      mergeSchema: true
```

| Config field | Description |
| :- | :- |
| **format** | Spark data source format. Supports: parquet, delta, csv, json, orc, avro, jdbc, kafka. Custom formats use the fully qualified DataSource class name. Special format `dataframe` reads from a parent pipeline by Module ID inside Arcades only. |
| **path** | Source path or URL. Context Registry references allowed. |
| **partition_filters** | Optional SQL predicate for manual partition pruning. |
| **schema_hint** | Optional. Flat dict `{col: type}` or nested `{mode: strict\|additive\|subset, columns: [{name, type}]}`. |
| **options** | Passed directly to Spark DataFrameReader.option(k,v). |

**Cloud credentials:** There is no per-Ingress `credentials:` field. Credentials live at the engine level in `spark_config:`, keyed by standard Hadoop/Spark property names. Use `@aq.secret('KEY')` or `${ENV_VAR}` inside those values.

### Channel

```yaml
- id: dedup_orders
  type: Channel
  label: "Deduplicate by order_id, keep latest event"
  config:
    op: deduplicate
    key: ${ctx.params.dedup_key}
    order_by: "event_ts DESC"
```

For SQL transformations:

```yaml
- id: cast_and_clean
  type: Channel
  config:
    op: sql
    udfs: [clean_phone, parse_currency]
    query: |
      SELECT CAST(amount AS DECIMAL(18,2)) AS amount, ...
      FROM dedup_orders
```

Upstream Modules are referenced by their id directly in SQL FROM clauses. Aqueduct registers each upstream DataFrame as a temp view using its Module id. For single-input Channels, the upstream is auto-registered as `__input__`.

| Config field | Description |
| :- | :- |
| **op** | Operation type. Built-in ops: `sql` \| `deduplicate` \| `filter` \| `select` \| `rename` \| `cast` \| `join` \| `union` \| `sort` \| `repartition` \| `coalesce` \| `cache`. |
| **query** | SQL string (`op: sql` only). Upstream Module IDs available as temp views. |
| **udfs** | List of UDF IDs to register before executing this Channel. |
| **key** | Column name or list of column names. Used by `deduplicate`. |
| **order_by** | Sort expression. Used by `deduplicate` and `sort`. |
| **condition** | Filter expression (`op: filter`). Standard Spark SQL boolean expression. |
| **columns** | Column mapping or list. Semantics depend on op. |
| **num_partitions** | Target partition count. Used by `repartition` and `coalesce`. |
| **spillway_condition** | Optional SQL boolean expression. Matching rows are routed to the spillway port. |
| **materialize** | Set to `incremental` for watermark-based incremental processing. |
| **watermark_column** | Required when `materialize: incremental`. Column used to track the high-water mark. |

**Op reference:**

| Op | Spark action? | Single input | Notes |
| :- | :-: | :-: | :- |
| `sql` | No | No | Full SQL; upstreams as temp views |
| `join` | No | No | Sugar over SQL JOIN with broadcast hint |
| `deduplicate` | No | Yes | `dropDuplicates()` or Window+rank with `order_by` |
| `filter` | No | Yes | `df.filter(condition)` |
| `select` | No | Yes | `df.select(*columns)` |
| `rename` | No | Yes | `df.withColumnRenamed()` per column |
| `cast` | No | Yes | `df.withColumn(col, col.cast(type))` |
| `sort` | No | Yes | `df.orderBy(*exprs)` — deferred until action |
| `union` | No | No (multi) | `unionByName` across all upstreams |
| `repartition` | No | Yes | Full shuffle — increase partitions or rebalance |
| `coalesce` | No | Yes | No shuffle — shrink partition count |
| `cache` | Yes | Yes | `df.persist(StorageLevel)` — triggers materialisation |

### Egress

```yaml
- id: save_orders
  type: Egress
  config:
    format: parquet
    mode: overwrite                # overwrite | append | error | ignore | merge
    path: "${ctx.tables.orders_out}"
    partition_by: [event_date, region]
    options: { compression: snappy }
```

| Config field | Description |
| :- | :- |
| **format** | Spark write format. Standard: parquet, delta, csv, json, orc, avro, jdbc. |
| **mode** | Write mode: `overwrite`, `append`, `error`, `ignore`, `merge` (Delta `MERGE INTO`, requires `key`). |
| **path** | Output path or URL. |
| **partition_by** | Columns to partition the output by. |
| **key** | Required for `mode: merge`. Column(s) for upsert match. |
| **options** | Passed directly to Spark DataFrameWriter.option(). |

### Junction (Fan-out)

```yaml
- id: split_by_action
  type: Junction
  config:
    mode: conditional              # conditional | broadcast | partition
    branches:
      - id: high_value
        condition: "amount > 1000"
      - id: low_value
        condition: "amount <= 1000"
```

| Config field | Description |
| :- | :- |
| **mode** | Junction mode: `conditional` (filter-based), `broadcast` (zero-shuffle, same data to all branches), `partition` (key-based hash split). |
| **branches** | List of branch definitions. Each has `id` and optional `condition`. |

### Funnel (Fan-in)

```yaml
- id: merge_all
  type: Funnel
  config:
    mode: union_all                # union_all | union | coalesce | zip
```

| Config field | Description |
| :- | :- |
| **mode** | Funnel mode: `union_all` (zero-shuffle), `union` (distinct), `coalesce` (aligned), `zip` (monotonically increasing ID join). |

### Probe

```yaml
- id: schema_check
  type: Probe
  config:
    attach_to: dedup_orders
    signal: schema_snapshot        # schema_snapshot | null_rates | value_distribution | row_count_estimate | distinct_count | freshness | partition_stats
```

Probes are non-blocking observability taps. They do not execute on the Spark critical path. Default signals are zero-cost (SparkListener). Sample-based signals (`null_rates`, `value_distribution`, `distinct_count`) require explicit opt-in via `danger.allow_full_probe_actions`.

See the [Observability Guide](observability_guide.md) for full signal reference and cost model.

### Regulator

```yaml
- id: quality_gate
  type: Regulator
  config:
    on_block: skip                 # skip | abort | trigger_agent
```

Regulators are passive — they compile away entirely if no signal edge is wired to them.

### Arcade (Sub-pipeline)

```yaml
- id: process_region
  type: Arcade
  config:
    ref: arcades/region_processor.yml
    context_override:
      env: ${ctx.env}
      data_dir: "/data/regions/${ctx.region}"
```

Arcades are expanded at compile time into a flat module list. Module IDs are namespaced (`{arcade_id}__{child_id}`). Blueprint module IDs must not contain `__` (reserved for Arcade expansion).

### Assert

```yaml
- id: orders_quality_gate
  type: Assert
  config:
    rules:
      - type: schema_match
        expected: {order_id: STRING, amount: "DECIMAL(18,4)", order_ts: TIMESTAMP}
        on_fail: abort
      - type: min_rows
        min: 1000
        on_fail: abort
      - type: null_rate
        column: order_id
        max: 0.0
        on_fail: abort
      - type: freshness
        column: order_ts
        max_age_hours: 26
        on_fail: webhook
      - type: sql_row
        expr: "amount > 0 AND order_id IS NOT NULL"
        on_fail: quarantine
```

Assert rules are batched into 1-2 Spark actions. Rule types: `schema_match` (zero action), `min_rows`, `null_rate`, `freshness`, `sql`, `sql_row`, `custom`.

---

# **5. Context Registry**

## **5.1 Three-Tier Resolution Model**

| Tier | Syntax | Resolved at | Performance cost |
| :- | :- | :- | :- |
| Tier 0 — Static | `${ctx.namespace.key}` | Parse time | Zero — substituted before Manifest is written |
| Tier 1 — Runtime function | `@aq.fn(args)` | Pre-job (Compiler) | Driver-only, milliseconds |
| Tier 2 — UDF | `udf_id` in `udfs:` list | Spark execution | Distributed — operates on DataFrame columns |

## **5.2 Tier 0 — Static Context**

```yaml
context:
  env:    ${AQUEDUCT_ENV:-dev}
  tables:
    orders: "s3://data/${ctx.env}/orders"
  params:
    dedup_key:  "order_id"
    batch_size: 10000
```

Resolution order (highest priority wins):

1. CLI flags: `aqueduct run --ctx env=prod`
2. Environment variables matching `AQUEDUCT_CTX_*` prefix
3. `context_profiles` block for the active profile (`--profile` flag)
4. `context:` block static defaults

## **5.3 Tier 1 — Runtime Functions (`@aq.*`)**

| Function | Description |
| :- | :- |
| `@aq.date.today(format="%Y-%m-%d")` | Current date (UTC). Pinned by `--execution-date` for idempotent backfills. |
| `@aq.date.yesterday(format="%Y-%m-%d")` | Date - 1. |
| `@aq.date.offset(base, days)` | Offset a date string by N days. Useful for backfill windows: `@aq.date.offset(base=@aq.date.today(), days=-7)`. |
| `@aq.date.month_start(format="%Y-%m-%d")` | First day of the current month. |
| `@aq.date.format(date_str, pattern)` | Reformat an ISO date string into a custom pattern. |
| `@aq.runtime.run_id()` | Auto-generated UUID for this pipeline run. |
| `@aq.runtime.timestamp()` | ISO-8601 timestamp of compilation. |
| `@aq.runtime.prev_run_id()` | Run ID of the previous pipeline execution (reads `_last_run_id` from Depot). |
| `@aq.env('KEY')` | Read environment variable. Fails fast when absent — unlike `${VAR:-default}` which supports a fallback. |
| `@aq.secret('KEY')` | Read from AWS/GCP/Azure secrets manager or environment fallback. |
| `@aq.depot.get('key')` | Read from Depot KV store at compile time. |

## **5.4 UDF Registry**

```yaml
udf_registry:
  - id: clean_phone
    lang: python                   # python (default) | pandas | spark_sql
    fn: |
      import re
      def clean_phone(p: str) -> str:
          return re.sub(r'[^\d+]', '', p) if p else p
    return_type: STRING
```

Three UDF execution models: Python (row-at-a-time via JVM bridge), Pandas (vectorized via Arrow), Spark SQL (inline expression, zero serialization).

---

# **6. Observability, Probes & Flow Report**

See the dedicated **[Observability Guide](observability_guide.md)** for:

- Full schema reference for `observability.db` and `benchmark.duckdb`
- Diagnostic query cookbook (run post-mortem, heal-loop forensics, cost analysis)
- Probe signal reference and cost model
- Store backend configuration (DuckDB / Postgres / Redis)

### Key design constraint

All observability is governed by one rule: **no Spark actions may be added to the critical execution path** beyond what the Blueprint configures. Default Probe signals (`schema_snapshot`, `partition_stats`, `row_count_estimate` via SparkListener) are zero-cost. Sample-based signals (`null_rates`, `value_distribution`) require explicit opt-in.

---

# **7. Lineage**

## **7.1 Two Lineage Layers**

| Layer | Computed at | Purpose |
| :- | :- | :- |
| **Structural Lineage** | Parse time (static) | Column-level DAG of the Blueprint. Used by the lineage gate and the LLM. |
| **Runtime Flow Report** | Post-run | Per-column quality metrics from Probe signals. |

## **7.2 Structural Lineage**

Structural lineage is computed at parse time by `sqlglot` analysis of Channel SQL queries. It maps each output column to its upstream source module and column. Stored in the `column_lineage` table inside `observability.db`. Used by:

- **Lineage gate:** Before a patch is applied, the lineage of the patched Blueprint is compared to the original. Lost columns or broken references are flagged.
- **LLM context:** The structural lineage for the failed module's neighbourhood is included in the FailureContext, allowing the agent to trace column origins without accessing the original Spark session.

## **7.3 Runtime Flow Report**

Generated post-run from Probe signals. Shows per-column, per-Module status (OK / Degraded / Error) with null rates, row estimates, schema snapshots, and thresholds.

---

# **8. Self-Healing & LLM Agent Loop**

## **8.1 Design Philosophy**

The LLM agent operates within a grammar, not in free-form code generation mode. It can only propose structured PatchSpec operations — valid, schema-checked modifications to the Blueprint. This constraint makes every agent action auditable, reversible, Git-diffable, and explainable to a human reviewer.

## **8.2 The Healing Flow**

```
Pipeline failure → Capture → Prune → Generate → Reprompt → Gate → Confirm and write
```

### 1. Capture

Transient errors retry first (per `retry_policy.max_attempts`). Non-transient failures — schema drift, missing columns, bad paths, OOM — trigger the agent. The Surveyor assembles a self-contained failure package:

- Compiled module config
- ProvenanceMap (where every config value came from)
- Sliced lineage neighbourhood
- Structured root-cause block (offending column + Spark suggestions)
- `inputs_fingerprint` (file metadata to distinguish data-drift from code bugs)

### 2. Prune

A ContextPruner trims the package to the failure's blast radius. Pruning rules:

| Error class | Manifest scope |
| :- | :- |
| `ColumnNotFound`, `TypeMismatch`, `AnalysisException` | Failed module + 2 upstream + 2 downstream |
| `SparkException` with OOM/shuffle | Full manifest |
| All other errors | Failed module + direct upstream |

### 3. Generate

The LLM responds with a structured PatchSpec — a list of typed operations that map one-to-one to Blueprint edits. Anything else is rejected.

### 4. Reprompt

Schema errors, guardrail violations, and gate rejections feed back into the same conversation as annotated, field-level corrections. The loop is bounded by a multi-axis budget:

| Axis | Default | What it guards against |
| :- | :- | :- |
| `max_reprompts` | 5 | Hard ceiling on LLM round-trips |
| `max_seconds` | 120 | Wall-clock cap per heal call |
| `max_tokens_total` | 50,000 | Sum of prompt + completion tokens |
| `same_error_consecutive` | 2 | Stuck on identical error signatures |
| `same_signature_overall` | 3 | Same error signature across the run |
| `progress_stalled_window` | 3 | No new distinct signatures |

When `same_error_consecutive` trips, the loop escalates: temperature is bumped and a skeleton reprompt template is used for one more attempt before honouring the abort.

### 5. Gate

Before a patch touches the Blueprint, four gates run in order:

1. **Guardrails** — path and operation policy (deterministic, enforced before the LLM response is parsed)
2. **Compile-check** — the patched dict must produce a valid Manifest
3. **Lineage gate** — column-level diff catches broken references before Spark sees them
4. **Sandbox gate** — sampled or full replay catches "parsed but produces nothing"

### 6. Confirm and write

Only after every gate passes does the patch run against the real pipeline. The on-disk Blueprint is rewritten only if the full re-run succeeds. Failed patches stage to `patches/pending/` for inspection.

## **8.3 Approval Modes**

| Mode | Who applies the patch | When it changes the Blueprint |
| :- | :- | :- |
| `disabled` | LLM never fires | Never |
| `human` | Engineer reviews and applies | Only after human accepts |
| `ci` | External CI receives patch and opens a PR | Only after merge |
| `auto` | Aqueduct applies in-memory, re-validates, writes only if the re-run succeeds | Only on a successful re-run |

Low-confidence patches and any guardrail violation auto-escalate to human review.

## **8.4 Sandbox Modes**

| Mode | Sample size | Egress writes | Danger gate |
| :- | :- | :- | :- |
| `sample` (default) | 1000 rows per Ingress | dropped | — |
| `preflight` | full dataset | dropped | `danger.allow_full_preflight: true` |
| `off` | no replay | writes for real | `danger.allow_skip_sandbox: true` |

## **8.5 Patch Grammar**

A PatchSpec is a JSON document with the following structure:

```json
{
  "patch_id": "fix-yellow-taxi-path",
  "description": "One sentence: what was wrong and what the fix does.",
  "confidence": 0.9,
  "category": "schema_drift | bad_path | format_mismatch | oom_config | sql_column_not_found | type_mismatch | missing_context | permission_error | other",
  "root_cause": "One sentence: root cause.",
  "operations": [
    { "op": "set_module_config_key", "module_id": "my_ingress", "key": "format", "value": "csv" },
    { "op": "replace_context_value", "key": "paths.yellow_path", "value": "data/yellow/*.parquet" }
  ]
}
```

Supported operations: `set_module_config_key`, `replace_module_config`, `replace_context_value`, `replace_module_label`, `insert_module`, `remove_module`, `add_probe`, `replace_edge`, `set_module_on_failure`, `replace_retry_policy`, `add_arcade_ref`, `defer_to_human`, `set_spark_config`.

`defer_to_human` signals an unhealable failure. It makes zero Blueprint changes and terminates the loop with `stop_reason='deferred'`. The payload carries `diagnosis`, `suggestions`, and `confidence_reason` for human review. Opt-in via `agent.allow_defer: true` — when false (default), the op is hidden from the LLM prompt.

`set_spark_config` sets a single key in the Blueprint `spark_config:` block. Covers OOM, shuffle fetch failures, Kryo buffer overflow, dynamic allocation thrashing, GC issues, and driver MaxResultSize — seven of the 20 most common Spark errors. Auto-creates the `spark_config` block if absent. Recommended default: add to `guardrails.forbidden_ops` to require human review before auto-apply. **Phase 43 `deep_loop`:** when `agent.deep_loop: true`, sandbox/lineage/explain gates run inside the LLM conversation so the model sees rejection feedback and retries in-context before `apply_callback` runs. Default false preserves the current post-hoc gate behavior.

### Metadata field tolerance

PatchSpec is **strict on operations, lenient on metadata.** Operation-level fields (`op`, `module_id`, `key`, `value`, `config`, …) mutate the Blueprint, so each Op model enforces `extra="forbid"` — a typo there bounces the patch. Top-level metadata fields (`rationale`, `root_cause`, `confidence`, `category`, `patch_id`) are descriptive only; the parser tolerates casing variants and synonym aliases so cheap models don't burn reprompt budget on cosmetics:

| Common LLM variant | Normalised to |
|---|---|
| `rootCause`, `rootcause`, `cause`, `rootCauseAnalysis` | `root_cause` |
| `reasoning`, `reason`, `description`, `summary`, `explanation` | `rationale` |
| `patchId`, `patchID` | `patch_id` |
| `runId`, `runID` | `run_id` |
| `Confidence`, `score` | `confidence` |
| `Category`, `failure_category`, `failureCategory` | `category` |

Anything else that doesn't fit a known top-level field is moved into `misc: dict[str, Any]` rather than rejected — the LLM's stray `"examples"`, `"notes"`, or `"verified_by"` field is preserved for post-mortem visibility but does not participate in mutation. The `misc` field is persisted alongside the patch in `patches/applied/*.json`.

## **8.6 FailureContext Structure**

```json
{
  "run_id": "run_20240412_143022_a3f9",
  "blueprint_id": "pipeline.orders.daily_aggregate",
  "failed_module": "cast_and_clean",
  "failure_type": "AnalysisException",
  "error_message": "Cannot resolve column 'event_ts' ...",
  "manifest_snapshot": { /* pruned manifest */ },
  "structural_lineage": { /* ColumnLineageGraph for failed Module */ },
  "probe_signals": [ ... ],
  "retry_history": [ ... ],
  "previous_patches": [ ... ],
  "inputs_fingerprint": { ... }
}
```

## **8.7 Why it is reliable**

- **No silent mutations.** Every patch is a structured diff with a rationale and a confidence score. Low confidence escalates to human review.
- **No production data corruption.** The sandbox validates patches against representative data before they reach live writes.
- **No runaway loops.** Budgets bound wall-clock, tokens, and stuck-signature counts. A rolling rate-limit caps healing attempts per hour per blueprint.
- **No black-box decisions.** Every LLM turn persists with the gate that rejected it, a stable error signature, and the prompt version.

---

# **9. Type System**

## **9.1 Principle: Aqueduct Owns No Types**

All column types throughout Aqueduct — in `schema_hint` declarations, UDF `return_type` fields, column assertions, and the Flow Report — use Spark DDL type strings verbatim. Aqueduct does not define its own type system and does not wrap or alias Spark types.

This guarantees complete compatibility and eliminates any translation layer between Aqueduct's type references and what Spark executes.

## **9.2 Accepted Aliases**

For `schema_hint` and assertions, common aliases are normalized:

| Alias | Canonical |
| :- | :- |
| `STRING` | `string` |
| `LONG` | `bigint` |
| `INTEGER` | `int` |
| `BOOL` | `boolean` |
| `SHORT` | `smallint` |
| `BYTE` | `tinyint` |
| `FLOAT` | `float` |
| `DOUBLE` | `double` |

---

# **10. Deployment & Spark Integration**

## **10.1 Engine Configuration File**

Aqueduct reads a project-level `aqueduct.yml` configuration file from the working directory (or path specified by `--config` flag). This file sets deployment target, store backends, agent config, and engine defaults.

## **10.2 Environment Variables & .env**

- Aqueduct automatically loads `.env` from the directory of the config or blueprint file.
- Override with `-e KEY=VAL` (highest precedence) or `--env-file <path>`.
- Disable entirely with `AQ_NO_ENV_FILE=1`.

## **10.3 SparkSession Lifecycle**

- The Executor creates one SparkSession per pipeline run.
- Session configuration from the Blueprint `spark_config` block is merged with engine defaults (Blueprint takes precedence).
- On self-healing patch and resume: the SparkSession is preserved if the failure was application-level; recycled if JVM/network-level.
- On run completion or abort, `session.stop()` is called.

## **10.4 Path Resolution (1.1.0+)**

Every relative path inside a YAML file resolves to **that YAML file's parent directory**, never the CWD of the `aqueduct` command. See [CLI Reference](cli_reference.md) for details.

## **10.5 Deployment Targets**

| Target | Description |
| :- | :- |
| **local** | `spark://local[*]`. Default for development. |
| **standalone** | Spark standalone cluster. Requires `master_url`. |
| **yarn** | YARN resource manager. Requires `hadoop_conf_dir`. |
| **kubernetes** | Driver pod via `spark-submit --master k8s://`. |
| **databricks** | Databricks Connect or Jobs API. |
| **emr** | AWS EMR via YARN. |
| **dataproc** | GCP Dataproc via YARN. |

See the **[Production Guide](production_guide.md)** for cluster setup, path conventions, security, and the production readiness checklist.

## **10.6 `aqueduct test` — Isolated Module Testing**

`aqueduct test <test_file.yml>` runs Channel, Junction, Funnel, and Assert modules against inline data with no external I/O. Ingress and Egress are never executed. The session always runs on `local[*]` — `deployment.master_url` is deliberately ignored for cluster-pointed configs.

## **10.7 Orchestrator Integration Contract**

Aqueduct stays orchestrator-agnostic. Schedulers (Airflow, Dagster, Prefect) wrap `aqueduct run` and consume two stable surfaces: the **exit-code contract** and the **patch CLI JSON**. Both are part of the v1.0 stability guarantee.

| Exit code | Name | Meaning |
| :- | :- | :- |
| 0 | SUCCESS | Command completed successfully |
| 1 | CONFIG_ERROR | Configuration or schema error |
| 2 | DATA_OR_RUNTIME | Runtime / Spark / data error |
| 3 | HEAL_PENDING | Patch staged for human review |
| 4 | VALIDATION_GATE | Patch rejected by validation |
| 5 | USAGE_ERROR | Invalid command usage |

---

# **11. Engine Scope & Boundaries**

## **11.1 What Aqueduct Is**

- A **batch processing engine** for Apache Spark. Every pipeline run is finite.
- A **declarative control plane**. Engineers describe *what* the pipeline does, not *how* Spark executes it.
- An **LLM-integrated operations tool**. Self-healing, patch lifecycle, and FailureContext are core.

## **11.2 What Aqueduct Is Not**

| Out of scope | Recommended alternative |
| :- | :- |
| **Streaming (Spark Structured Streaming, Kafka)** | Deferred. Requires continuous process lifecycle management. |
| **Native ML training pipelines** | MLflow Pipelines, Vertex AI, or Kubeflow. |
| **Visual graph editor / UI** | The Blueprint YAML is always the source of truth. |
| **Multi-pipeline orchestration (native)** | Use Airflow, Prefect, or cron to trigger `aqueduct run`. |
| **Built-in scheduler** | Aqueduct has no scheduler. `aqueduct run` is designed to be invoked by an orchestrator. |

## **11.3 Scheduling**

Aqueduct has no built-in scheduler. `aqueduct run` is a one-shot CLI command designed to be invoked by an orchestrator:

- **Simple cron:** Any OS-level cron, systemd timer, or cloud scheduler invoking `aqueduct run blueprint.yml`.
- **Complex orchestration:** Airflow `AqueductOperator` for dependency management, backfill, and SLA tracking.
- **On-demand:** Manual invocation from CI/CD or by the LLM agent.

The Airflow integration (`aqueduct-core[airflow]`) provides `AqueductOperator` with a deferrable `AqueductPatchSensor`/`AqueductPatchTrigger` pair for the HEAL_PENDING approval flow. This is the recommended production scheduler.