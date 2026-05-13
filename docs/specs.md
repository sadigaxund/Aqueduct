**AQUEDUCT**  —  System Design & Implementation Reference

**AQUEDUCT**

Intelligent Spark Blueprint Engine

*System Design & Implementation Reference*

|<p>*Self-healing LLM-integrated pipelines for Apache Spark*</p><p>**Declarative  ·  Observable  ·  Autonomous  ·  Self-healing**</p><p>Version 1.0 — Implementation Reference</p>|
| :-: |

*Blueprint · Module · Ingress · Channel · Egress · Junction · Funnel · Probe · Regulator · Spillway · Arcade · Surveyor*


# **1. Introduction**
## **1.1 Purpose of This Document**
This document is the complete system design and implementation reference for Aqueduct — an intelligent, declarative Spark pipeline engine with integrated LLM-driven self-healing. It covers every component, design decision, data contract, configuration schema, and runtime behaviour required to implement the system from scratch. Anyone reading this document should have no ambiguous points remaining regarding what Aqueduct does, how it works internally, or how to build it.

## **1.2 What Aqueduct Is**
Aqueduct is a control plane for Apache Spark. It does not replace Spark — it wraps it. Engineers and LLM agents author pipelines as YAML Blueprint files. Aqueduct parses, validates, compiles, plans, and executes those Blueprints as Spark jobs, monitoring them continuously and autonomously patching failures when they occur.

The name is deliberate: a Roman aqueduct is precision-engineered infrastructure for carrying flow reliably across vast distances, planned on actual blueprints (forma), and built to strict tolerances. Aqueduct the software carries data flow with the same philosophy — structured, observable, and resilient.

## **1.3 Primary Users**
- Data engineers (code-first) — author and maintain Blueprints, review patches, configure retry policies.
- LLM agents — primary runtime operators; diagnose failures, propose and apply Patches autonomously.
- Platform operators — deploy and configure the engine, manage deployment targets and credentials.

## **1.4 Design Principles**
These principles govern every design decision in the system. When two requirements conflict, the higher principle wins.

|**Principle**|**Description**|
| :- | :- |
|**P1 — LLM-first observability**|Every failure must carry enough structured context for an agent to diagnose and patch without additional queries. Observability is not optional.|
|**P2 — Blueprint as truth**|The Blueprint is the single source of truth. Nothing about a pipeline exists outside the Blueprint and its derived Manifest. No hidden state.|
|**P3 — Performance non-regression**|Aqueduct adds no hidden Spark actions. Any action beyond the pipeline's own Egress writes is the direct result of a user-configured Probe, Assert rule, or incremental watermark — each with its cost fully described in the signal cost model (§6.2). Zero-cost options are always preferred and are the default.|
|**P4 — Static resolution first**|Any value that can be resolved at parse time must be. Runtime resolution is explicit, opt-in, and visually distinct in Blueprint syntax.|
|**P5 — Patch grammar over codegen**|The LLM agent operates within a structured Patch grammar, not free-form code generation. Every patch is schema-valid, auditable, and reversible.|
|**P6 — Passive-by-default gates**|Flow control constructs (Regulators, Spillways) do not exist in the execution path unless explicitly wired. Unwired gates compile away entirely.|
|**P7 — Pure Spark type system**|Aqueduct owns no type system. All types are Spark DDL strings. Semantic constraints are annotations, not types.|
|**P8 — Provenance-aware LLM context**|Every value the LLM reasons about is backed by a compile-time provenance index. The agent never receives raw Blueprint YAML to reverse-engineer — it receives resolved values tagged with their origin (`literal`, `context_ref`, `env_ref`, `arcade_inherited`). Arcade-expanded modules, context refs, and env vars are all correctly attributed so patch ops target the right layer of the Blueprint.|


# **2. Naming Glossary**
These names are canonical and used consistently throughout the codebase, documentation, logs, and LLM prompts. They must not be substituted for synonyms.

|**Term**|**Definition**|
| :- | :- |
|**Aqueduct**|The engine itself. The full system described in this document.|
|**Module**|The smallest indivisible unit of a pipeline. Every step in a Blueprint is a Module. Typed: Ingress, Channel, Egress, Junction, Funnel, Probe, Regulator, Arcade, Assert.|
|**Blueprint**|The YAML file authored by an engineer or agent. Defines a complete pipeline: its Modules, edges, Context Registry, retry policy, and agent config. Human-editable, version-controllable.|
|**Manifest**|The compiled, fully-resolved JSON form of a Blueprint after all Context Registry substitution and Arcade expansion. What Aqueduct actually executes. Machine-readable, not hand-edited.|
|**Context Registry**|The variable system for Blueprints. Namespaced key-value store. Tier 0 (static) values are resolved at parse time. Tier 1 (@aq.\*) values are resolved at execution time before Spark jobs start.|
|**Depot**|Aqueduct's persistent key-value state store. Pipelines read and write named keys across runs. Values can be flagged read-only or writable per-key.|
|**Ingress**|Module type: reads data from an external source into the pipeline. Produces a named DataFrame. Equivalent concept to "Source" in other tools.|
|**Channel**|Module type: applies a transformation to one or more upstream DataFrames. Produces one output DataFrame. No Spark actions. Pure lazy transformation.|
|**Egress**|Module type: writes data to an external target or triggers a collection action. The only Module type that materialises results and costs a Spark action.|
|**Junction**|Module type: splits one incoming DataFrame into multiple downstream branches. Fan-out. Supports partition, broadcast, and conditional routing modes.|
|**Funnel**|Module type: merges multiple upstream DataFrames into one. Fan-in. Supports union\_all, union, coalesce, and zip merge modes.|
|**Probe**|Module type: non-blocking observability tap attached to a Module's output edge. Emits signals (schema, null rates, row estimates) without adding Spark actions to the critical path.|
|**Regulator**|Module type: trigger gate. Passive by default — if nothing is wired to it, it does not exist in the execution path. Blocks downstream when it receives False or an error signal.|
|**Spillway**|The error output port present on every Module. When a Module produces row-level errors, they are routed via the Spillway to a designated downstream Module. Named by analogy to a hydraulic spillway — a designed relief path for overflow.|
|**Arcade**|Module type: an encapsulated, reusable sub-pipeline embedded as a single Module in a parent Blueprint. Parameterised via Context override. Expanded at Manifest compile time — Spark sees a flat plan.|
|**Surveyor**|The runtime supervisor process. Monitors live pipeline execution, evaluates health signals, manages the retry policy, and triggers the LLM self-healing loop on failure.|
|**Patch**|A structured diff to a Blueprint proposed by the LLM agent or a human. Expressed as a PatchSpec JSON. Applied atomically. Version-controlled in patches/.|
|**Flow Report**|The post-run column-level quality report. Shows per-column status (OK / Degraded / Error) across each Module, sourced from Probe signals.|
|**Flow Graph**|The directed acyclic graph (DAG) of Modules extracted from a Blueprint and used for execution planning.|
|**FailureContext**|The structured failure document assembled by the Surveyor when a pipeline run ends in error. Contains: run metadata, compiled Manifest JSON, ProvenanceMap slice, error message, stack trace, and doctor pre-flight hints. Passed to the LLM self-healing loop.|
|**PatchSpec**|The JSON document that describes a set of operations to apply to a Blueprint. Produced by the LLM agent or authored by hand. Validated by Pydantic before application. Stored in `patches/pending/` (awaiting review) or `patches/applied/` (committed).|
|**ProvenanceMap**|A compile-time index of every resolved config value in the Manifest: where it came from (literal, context ref, env var, Arcade inheritance), the original Blueprint expression, and the resolved value. Attached to the Manifest. Used by the LLM prompt (eliminates need to send raw Blueprint YAML), guardrails (resolves `${ctx.*}` before pattern matching), and doctor checks.|


# **3. System Architecture**
## **3.1 High-Level Overview**
Aqueduct has five processing layers and three persistent stores. Each layer has a defined input/output contract and can be developed and tested independently.

|**Layer**|**Input**|**Output**|**Responsibility**|
| :- | :- | :- | :- |
|1 — Parser|Blueprint YAML|Validated AST|Schema validation, Context Tier 0 resolution, cycle detection, Arcade loading|
|2 — Compiler|AST + Context map|Manifest (JSON)|Interpolate all ${ctx.\*} refs, resolve @aq.\* functions, expand Arcades, wire Probes and Spillways|
|3 — Planner|Manifest|Execution Plan|Partition into Spark JobSpecs, assign stage order, identify parallel branches|
|4 — Executor|Execution Plan|RunRecord + raw metrics|Submit Spark jobs, attach SparkListener, stream events to Observability Store|
|5 — Surveyor|Live run signals|HealthEvents + Patches|Monitor health, apply retry policy, invoke LLM loop, apply approved Patches|

## **3.2 Persistent Stores**

|**Store**|**Description**|
| :- | :- |
|**Observability Store**|Append-only log of all runtime signals: Probe readings, stage metrics, errors. Queryable by run\_id and module\_id. Backend: DuckDB embedded database (`.aqueduct/obs.db`).|
|**Lineage Store**|Stores structural column lineage graphs (computed at parse time) and runtime ColumnQualityReport entries per run. Backend: DuckDB embedded database (`.aqueduct/lineage.db`).|
|**Depot (KV Store)**|Persistent key-value store for pipeline state across runs: watermarks, last-run metadata, cross-pipeline coordination values. Keyed by blueprint\_id + key name. Per-key read/write access flags. Backend: DuckDB embedded database (`.aqueduct/depot.db`).|

## **3.3 Component Interaction Flow**
The following sequence describes a complete pipeline run from Blueprint file to completion:

1. Engineer or agent writes Blueprint.yml and commits it to version control.
1. aqueduct run Blueprint.yml [--profile prod] [--ctx key=value] is invoked.
1. Parser reads and validates Blueprint.yml against the versioned JSON Schema. Fails fast on any unknown field or schema violation.
1. Context Registry is built: static values resolved, @aq.\* functions queued for Tier 1 resolution.
1. Compiler resolves all @aq.\* runtime functions (date functions, secret fetches, Depot reads). All ${ctx.\*} tokens are substituted. Arcades are expanded. Manifest JSON is written to runs/<run\_id>/manifest.json.
1. Executor topologically sorts the Module list and inserts Probes immediately after their `attach_to` targets. When `--parallel` is set, independent connected components are identified via Union-Find and each component is submitted to a separate Python thread.
1. Executor acquires or creates a SparkSession for the target deployment. RunRecord is opened in the Observability Store.
1. Executor submits each JobSpec. SparkListener is attached, streaming task metrics to the Observability Store in real time.
1. Surveyor runs concurrently, consuming the Observability Store event stream. It evaluates health signals and Regulator states continuously.
1. On job completion, Executor finalises the RunRecord. Surveyor generates the Flow Report from Probe signals.
1. On any failure: Surveyor applies the retry policy. On exhaustion (or non-transient error), it packages the FailureContext and invokes the LLM agent loop.
1. Approved Patch is applied to the Blueprint. Manifest is recompiled. Execution resumes from the failed Module forward.


# **4. Blueprint Format**
## **4.1 File Format & Versioning**
Blueprints are YAML files. The format is versioned — the `aqueduct` field at the top selects the JSON Schema version used for validation. Unknown fields at any level are hard errors, not warnings. This strictness is intentional: it guarantees Blueprints are always valid input for LLM patch generation.

Internally, Aqueduct maintains two representations: the YAML Blueprint (human-authored, version-controlled) and the JSON Manifest (machine-compiled, never hand-edited). Engineers write YAML. The LLM agent reads JSON. The Parser bridges them.

|**Dual-format contract:**  The Manifest is always complete and self-contained. The LLM agent must never need to fetch external state to understand what ran. Every resolved value, every expanded Arcade, every wired Spillway appears explicitly in the Manifest.|
| :- |

## **4.2 Top-Level Structure**
```yaml
aqueduct: "1.0"                        # schema version — required
id: pipeline.orders.daily_aggregate    # globally unique pipeline ID
name: "Daily Orders Aggregation"       # human display name

description: |
  Reads raw orders, deduplicates by order_id,
  aggregates by region, writes to Delta.

context:                               # Context Registry (see Section 5)
  env: ${AQUEDUCT_ENV:-dev}
  tables:
    orders_raw: "s3://data/${ctx.env}/orders/raw"
    orders_out: "s3://data/${ctx.env}/orders/daily"
  params:
    dedup_key: "order_id"
    agg_col: "region"

context_profiles:                      # environment promotion (see Section 5.3)
  dev:
    tables.orders_raw: "s3://dev/orders/raw"
  prod:
    tables.orders_raw: "s3://prod/orders/raw"

modules:                               # ordered or graph-connected Module list
  - id: read_orders
    type: Ingress
  # ... more modules

edges:                                 # explicit edge definitions
  - from: read_orders
    to:   dedup_orders
    port: main                         # main (default) or spillway

spark_config:                          # merged with Aqueduct defaults
  spark.sql.shuffle.partitions: 200
  spark.sql.adaptive.enabled: true

retry_policy:                          # see Section 8.2
  max_attempts: 3
  # ... other fields

checkpoint: false                      # if true, write run checkpoints for --resume

agent:                                 # LLM self-healing config (see Section 8)
  approval_mode: auto                  # disabled | human | auto | aggressive
  model: claude-sonnet-4-6
  aggressive_max_patches: 5
  # Guardrails — deterministically enforced at patch-apply time (recommended for auto/aggressive)
  guardrails:
    allowed_paths: ["s3://company-data/*"]   # fnmatch patterns; empty = unrestricted
    forbidden_ops: ["insert_module"]         # PatchSpec op names blocked; empty = all ops allowed
```

## **4.3 Module Schema — Common Fields**
Every Module regardless of type shares these fields:

|**Field**|**Description**|
| :- | :- |
|**id**|Required. Unique string within the Blueprint. Auto-generated as slug + short hash if omitted. Used in edges, logs, patches, and file paths. Must be filesystem-safe (auto-slugified from label if not set).|
|**label**|Required. Human-readable display name. Completely unconstrained — spaces, unicode, punctuation all permitted. Shown in UI, logs, Flow Report, and LLM FailureContext. The id is derived from this if not explicitly set.|
|**type**|Required. One of: Ingress | Channel | Egress | Junction | Funnel | Probe | Regulator | Arcade | Assert.|
|**description**|Optional. Free-text explanation of what this Module does. Used in LLM context and UI tooltips.|
|**tags**|Optional list of strings. Used for filtering, grouping, and agent scoped search.|
|**config**|Type-specific configuration block. Defined per Module type in Section 4.4.|
|**on\_failure**|Optional. Overrides the pipeline-level retry\_policy for this specific Module.|
|**spillway**|Optional. Specifies the downstream Module ID to receive error-port output. If omitted and errors occur, they propagate to the Surveyor as a pipeline failure.|
|**depends\_on**|Optional explicit upstream dependency list. Normally inferred from edges but can be stated for clarity or to force ordering between non-connected Modules.|
|**checkpoint**|Optional boolean, default `false`. When `true`, the module's output DataFrame is saved as Parquet to `.aqueduct/checkpoints/<run_id>/<module_id>/` after successful execution. Used with `aqueduct run --resume <run_id>` to skip completed modules on re-run.|

## **4.4 Module Types — Full Specification**

|**INGRESS**|**Source of data — reads from an external system into the pipeline**|
| :-: | :- |

```yaml
- id: read_orders
  type: Ingress
  label: "Read raw orders from S3 Parquet"
  config:
    format: parquet              # parquet | delta | csv | json | jdbc | kafka | custom
    path: ${ctx.tables.orders_raw}
    schema_hint:                 # optional — enforced at read time
      order_id: STRING
      amount: DECIMAL(18,2)
      event_ts: TIMESTAMP
    options:
      mergeSchema: true
      basePath: ${ctx.tables.orders_raw}
```

|**Config field**|**Description**|
| :- | :- |
|**format**|Spark data source format string. Standard formats: parquet, delta, csv, json, orc, avro, jdbc, kafka. Custom formats use the fully qualified DataSource class name. Special Aqueduct-only format: `dataframe` — used inside Arcade sub-blueprints to read a DataFrame from the parent pipeline by Module ID (set `ref` to the parent Module id); not valid in standalone Blueprints.|
|**path**|Source path or URL. Context Registry references allowed. For JDBC: the full connection URL.|
|**schema\_hint**|Optional. Flat dict `{col: type}` checks all listed columns with strict matching. Nested form `{mode: strict\|additive\|subset, columns: [{name, type}]}` selects check mode: strict (all must match), additive (extra upstream cols allowed), subset (missing cols allowed). Common type aliases accepted: `STRING`→`string`, `LONG`→`bigint`, `INTEGER`→`int`, `BOOL`→`boolean`, `SHORT`→`smallint`, `BYTE`→`tinyint`. Mismatch raises IngressError.|
|**options**|Passed directly to Spark DataFrameReader.option(k,v). Aqueduct does not validate these — Spark handles unknown options.|
|**credentials**|Optional. Reference to a Depot key or @aq.secret() call resolving to a credentials map. Injected as Spark config before session creation.|

|**CHANNEL**|**Transformation — shapes, filters, enriches, or restructures data**|
| :-: | :- |

```yaml
- id: dedup_orders
  type: Channel
  label: "Deduplicate by order_id, keep latest event"
  config:
    op: deduplicate
    key: ${ctx.params.dedup_key}
    order_by: "event_ts DESC"
```

For SQL-based transformations:

```yaml
- id: cast_and_clean
  type: Channel
  label: "Cast types and strip whitespace"
  config:
    op: sql
    udfs: [clean_phone, parse_currency]   # registered UDF IDs
    query: |
      SELECT
        order_id,
        CAST(amount AS DECIMAL(18,2))          AS amount,
        clean_phone(phone_raw)                 AS phone,
        TRIM(region)                           AS region
      FROM dedup_orders
```

|**SQL reference convention:**  Upstream Modules are referenced by their id directly in SQL FROM clauses. Aqueduct registers each upstream DataFrame as a temp view using its Module id before executing the query. No special tokens required. For single-input Channels with no explicit FROM, the upstream DataFrame is auto-registered as \_\_input\_\_.|
| :- |

|**Config field**|**Description**|
| :- | :- |
|**op**|Operation type. Built-in ops: `sql` \| `deduplicate` \| `filter` \| `select` \| `rename` \| `cast` \| `join` \| `union` \| `sort` \| `repartition` \| `coalesce` \| `cache`.|
|**query**|SQL string (`op: sql` only). Upstream Module IDs are available as temp views. `${ctx.*}` references are substituted before the query reaches Spark.|
|**udfs**|List of UDF IDs to register before executing this Channel. UDFs are defined in the `udf_registry` block (see Section 5.4).|
|**key**|Column name or list of column names. Used by `deduplicate` (partition key) and key-based ops.|
|**order\_by**|Sort expression string. Used by `deduplicate` (determines which row to keep — e.g. `"event_ts DESC"` keeps the latest) and `sort`.|
|**condition**|Filter expression (`op: filter`). Standard Spark SQL boolean expression.|
|**columns**|Column mapping or list. Semantics depend on op: `select` = list of column names; `rename` = `{old: new}` dict or `[{from, to}]` list; `cast` = `{col: spark_type}` dict or `[{column, type}]` list.|
|**num\_partitions**|Target partition count. Used by `repartition` and `coalesce`.|
|**column**|Optional column name for `repartition` — partitions by hash of this column.|
|**allow\_missing\_columns**|Boolean (default `true`). Used by `union` — when `true`, missing columns are filled with `null` (`unionByName` semantics).|
|**storage\_level**|Storage level for `op: cache`. Default: `MEMORY_AND_DISK`. Valid: `MEMORY_AND_DISK`, `MEMORY_AND_DISK_SER`, `MEMORY_ONLY`, `MEMORY_ONLY_SER`, `DISK_ONLY`, `DISK_ONLY_2`, `OFF_HEAP`.|
|**spillway\_condition**|Optional SQL boolean expression. Rows matching the expression are routed to the spillway port (appended with `_aq_error_*` columns); non-matching rows flow to the main port. Requires a spillway edge — warns if set without one. When omitted and a spillway edge exists, the spillway DataFrame is empty.|
|**metrics\_boundary**|Optional boolean (default `false`). When `true`, appends `df.repartition(n)` after the op result to force a Spark stage cut. Gives accurate per-module `recordsWritten` metrics from SparkListener — without this, Spark's stage fusion groups consecutive logical modules into one physical stage, making per-module attribution inaccurate. `n` is the current planned partition count (`df.rdd.getNumPartitions()`, driver-side — no Spark action). Pairs well with LLM self-healing, which uses `recordsWritten` to detect empty-output failures.|
|**materialize**|Optional. Set to `incremental` to enable watermark-based incremental processing. On each run the last watermark is loaded from Depot and substituted into the query as `${ctx._watermark}`. After successful execution the new watermark (MAX of `watermark_column`) is persisted back. First run uses sentinel `'1900-01-01 00:00:00'` so the full table is scanned. Only valid for `op: sql`. Warns if downstream Egress uses `mode: overwrite`.|
|**watermark\_column**|Required when `materialize: incremental`. Column name used to track the high-water mark (typically a timestamp or monotonic integer). The executor runs `MAX(watermark_column)` on the output DataFrame after each successful run to advance the watermark. **Cost:** this is an extra Spark action — if the output DataFrame is not cached, Spark re-executes the full DAG to compute the max (a second scan of the output data). Add a Checkpoint upstream or cache the Channel output to avoid the double scan. See [docs/SPARK_GUIDE.md#incremental-watermark-scan](SPARK_GUIDE.md#incremental-watermark-scan).|

**Incremental Channel (`materialize: incremental`):**

Enables watermark-based incremental batch processing without a streaming engine. The watermark is stored in the Depot KV store under the key `"{blueprint_id}:{module_id}:_watermark"` and advanced after each successful run.

```yaml
- id: new_orders
  type: Channel
  config:
    op: sql
    materialize: incremental
    watermark_column: event_ts
    query: |
      SELECT *
      FROM source_orders
      WHERE event_ts > CAST('${ctx._watermark}' AS TIMESTAMP)
```

- First run: `${ctx._watermark}` resolves to `'1900-01-01 00:00:00'` → full scan.
- Subsequent runs: resolves to the `MAX(event_ts)` from the previous run's output.
- `${ctx._watermark}` substitution happens at **runtime** (not compile time) — it is not a Tier 0 context ref.
- Downstream Egress should use `mode: append`. Using `mode: overwrite` triggers a startup warning.
- Watermark is not advanced if the Channel fails or an exception is raised.

**Op reference — when to use which:**

| Op | Spark action? | Single input | Notes |
| :- | :-: | :-: | :- |
| `sql` | No | No | Full SQL; upstreams as temp views |
| `join` | No | No | Sugar over SQL JOIN with broadcast hint support |
| `deduplicate` | No¹ | Yes | `dropDuplicates()` or Window+rank when `order_by` set |
| `filter` | No | Yes | `df.filter(condition)` |
| `select` | No | Yes | `df.select(*columns)` |
| `rename` | No | Yes | `df.withColumnRenamed()` per column |
| `cast` | No | Yes | `df.withColumn(col, col.cast(type))` per column |
| `sort` | No² | Yes | `df.orderBy(*exprs)` — deferred until action |
| `union` | No | No (multi) | `unionByName` across all upstreams |
| `repartition` | No³ | Yes | Full shuffle — use to increase partitions or rebalance |
| `coalesce` | No | Yes | No shuffle — use to shrink partition count (e.g. before single-file Egress) |
| `cache` | Yes⁴ | Yes | `df.persist(StorageLevel)` — triggers materialisation |

¹ `deduplicate` with `order_by` uses `row_number()` window function — lazy.
² `sort` adds an exchange to the plan; the shuffle fires when downstream action triggers.
³ `repartition` also adds an exchange; shuffle fires at action time.
⁴ `cache`/`persist` fires a Spark action immediately to materialise the DataFrame into memory/disk.

**Channel op: join — config reference:**

```yaml
- id: enrich_orders
  type: Channel
  label: "Join orders with customer data"
  config:
    op: join
    left: orders            # upstream Module ID (registered as temp view)
    right: customers        # upstream Module ID (registered as temp view)
    join_type: left         # inner | left | right | full | semi | anti | cross
    condition: "orders.customer_id = customers.id"
    broadcast_side: right   # optional Spark broadcast hint for the smaller side
```

|**Join config field**|**Description**|
| :- | :- |
|**left**|Module ID of the left-side DataFrame. Must be registered as a temp view.|
|**right**|Module ID of the right-side DataFrame. Must be registered as a temp view.|
|**join\_type**|Spark join type: inner, left, right, full, semi, anti, cross. Default: inner.|
|**condition**|SQL join predicate string. Both sides accessible by their module IDs.|
|**broadcast\_side**|Optional: left or right. Adds a `BROADCAST` hint to the smaller side to avoid shuffle. **When to use:** Spark's `spark.sql.autoBroadcastJoinThreshold` (default 10 MiB) broadcasts small tables automatically — `broadcast_side` is only needed when the table exceeds the threshold but is known to be small enough to broadcast safely, or when AQE's runtime decision must be overridden. With AQE enabled, `broadcast_side` is advisory and may be overridden at runtime.|

> **Note:** `op: join` is sugar over `op: sql`. It is translated to `SELECT * FROM left JOIN right ON condition` before reaching Spark. For complex multi-table joins, multi-way expressions, or window functions, use `op: sql` directly.

**Channel op examples — DataFrame API ops:**

```yaml
# deduplicate — keep latest row per order_id
- id: dedup_orders
  type: Channel
  config:
    op: deduplicate
    key: order_id
    order_by: "event_ts DESC"

# deduplicate — keyless (all columns)
- id: dedup_all
  type: Channel
  config:
    op: deduplicate

# filter
- id: active_only
  type: Channel
  config:
    op: filter
    condition: "status = 'active' AND amount > 0"

# select
- id: slim_orders
  type: Channel
  config:
    op: select
    columns: [order_id, amount, region, event_ts]

# rename — dict form
- id: rename_cols
  type: Channel
  config:
    op: rename
    columns:
      ReviewDate: review_date
      CustomerID: customer_id

# cast — dict form
- id: cast_types
  type: Channel
  config:
    op: cast
    columns:
      amount: "decimal(18,2)"
      event_ts: timestamp

# sort
- id: sorted_orders
  type: Channel
  config:
    op: sort
    order_by: ["event_ts DESC", "order_id ASC"]

# union — merges all upstream DataFrames
- id: all_regions
  type: Channel
  config:
    op: union
    allow_missing_columns: true   # default true

# repartition — increase parallelism before heavy transform
- id: repart
  type: Channel
  config:
    op: repartition
    num_partitions: 200
    column: region   # optional — partition by hash of this column

# coalesce — shrink to single file before Egress (no shuffle)
- id: single_file
  type: Channel
  config:
    op: coalesce
    num_partitions: 1

# cache — materialise for reuse across multiple downstream paths
- id: cached_base
  type: Channel
  config:
    op: cache
    storage_level: MEMORY_AND_DISK   # default; omit to use default
```

**SQL Macros — static compile-time fragments:**

Define reusable SQL snippets in the top-level `macros:` block. Macros are resolved at compile time; the Manifest always contains plain SQL. No loops, no conditionals, no runtime evaluation.

```yaml
macros:
  active_filter: "status = 'active' AND deleted_at IS NULL"
  date_trunc: "DATE_TRUNC('{{ period }}', {{ column }})"

modules:
  - id: clean_orders
    type: Channel
    config:
      op: sql
      query: |
        SELECT *
        FROM orders
        WHERE {{ macros.active_filter }}
          AND {{ macros.date_trunc(period='day', column=event_ts) }} >= '2026-01-01'
```

| Macro syntax | Description |
| :- | :- |
| `{{ macros.name }}` | Simple substitution — inserts the macro body verbatim |
| `{{ macros.name(key=value) }}` | Parameterized — substitutes `{{ key }}` placeholders in the macro body |

Macro bodies may not reference other macros. All `{{ param }}` placeholders in a parameterized macro body must be supplied at the call site or a `CompileError` is raised.

|**EGRESS**|**Sink — writes data to an external target or triggers a Spark action**|
| :-: | :- |

```yaml
- id: write_daily_orders
  type: Egress
  label: "Write aggregated orders to Delta Lake"
  config:
    format: delta
    path: ${ctx.tables.orders_out}
    mode: overwrite              # overwrite | append | merge | error | ignore
    partition_by: [region, date]
    merge_key: [order_id]          # for mode: merge (Delta MERGE INTO)
    options:
      overwriteSchema: true
```

|**Config field**|**Description**|
| :- | :- |
|**format**|Target format. Same values as Ingress format.|
|**path**|Target path or URL.|
|**mode**|Write mode. overwrite replaces. append adds. merge uses Delta MERGE INTO semantics with merge\_key. error fails if data exists. ignore skips if data exists.|
|**partition\_by**|List of column names to partition output by.|
|**merge\_key**|List of columns forming the merge condition for mode: merge.|
|**collect**|Boolean. If true, collects result to driver (small datasets only). Outputs to RunRecord as collected\_result. Not for production large datasets.|
|**register\_as\_table**|Optional string. After writing, registers the output path as an external table with this name in the active Spark catalog. Non-fatal: registration failure logs a warning but does not fail the pipeline.|

|**JUNCTION**|**Fan-out — splits one flow into multiple downstream branches**|
| :-: | :- |

```yaml
- id: split_by_region
  type: Junction
  label: "Route orders to regional processing branches"
  config:
    mode: conditional              # conditional | partition | broadcast
    branches:
      - id: branch_eu
        label: "EU orders"
        condition: "region IN ('DE','FR','NL','IT')"
      - id: branch_us
        label: "US orders"
        condition: "region = 'US'"
      - id: branch_other
        label: "All other regions"
        condition: "_else_"        # catches unmatched rows
```

|**Mode**|**Behaviour**|
| :- | :- |
|**mode: conditional**|Each branch receives a filtered subset of rows matching its condition. Rows matching no branch go to the \_else\_ branch if defined, otherwise dropped. Implemented as DataFrame.filter() per branch — one lazy plan per branch, no shuffle.|
|**mode: partition**|Splits by a key column into N branches. Each branch receives rows where key = branch value. High-cardinality keys should use mode: conditional instead.|
|**mode: broadcast**|Sends the same full DataFrame to all branches unchanged. Zero shuffle — all branches reference the same lazy plan. Used for parallel independent processing of the same data.|

|**Performance note:**  All Junction modes produce lazy DataFrame references — no Spark action is triggered by a Junction itself. Branches are evaluated lazily when their downstream Egress modules trigger actions. Aqueduct submits parallel branches as concurrent Spark jobs when the Planner determines they have no shared dependencies.|
| :- |

|**FUNNEL**|**Fan-in — merges multiple upstream flows into one**|
| :-: | :- |

```yaml
- id: merge_regional_results
  type: Funnel
  label: "Reunite processed regional streams"
  config:
    mode: union_all                # union_all | union | coalesce | zip
    inputs: [branch_eu, branch_us, branch_other]
    schema_check: strict           # strict | permissive
```

|**Mode**|**Behaviour**|
| :- | :- |
|**mode: union\_all**|Concatenates all inputs preserving duplicates. Equivalent to UNION ALL in SQL. Zero shuffle. Requires all inputs to have the same schema (unless schema\_check: permissive).|
|**mode: union**|Concatenates and deduplicates. Equivalent to UNION DISTINCT. Triggers a shuffle for deduplication.|
|**mode: coalesce**|Returns the first non-null value per row across inputs. Inputs must be aligned (same row count and order). Used for filling nulls from fallback sources.|
|**mode: zip**|Row-by-row lateral join of all inputs. Inputs must be aligned. Implemented as DataFrame.join() on a monotonically\_increasing\_id column.|

|**PROBE**|**Observability tap — emits signals without adding Spark actions**|
| :-: | :- |

```yaml
- id: probe_after_dedup
  type: Probe
  label: "Capture observability signals after deduplication"
  attach_to: dedup_orders          # emits signals after this Module completes
  config:
    signals:
      - type: schema_snapshot          # always free — metadata only

      - type: row_count_estimate       # two methods:
        method: spark_listener         #   spark_listener = zero cost (from stage metrics)
                                       #   sample = count() on a fraction

      - type: null_rates
        columns: [order_id, region, amount]
        fraction: 0.01                 # 1% sample

      - type: sample_rows
        n: 20

      - type: value_distribution
        columns: [amount, fare]        # omit → all numeric columns
        fraction: 0.1
        percentiles: [0.25, 0.5, 0.75]

      - type: distinct_count
        columns: [region, status]      # omit → all columns
        fraction: 0.1

      - type: data_freshness
        column: event_time             # required
        # allow_sample: true           # use fraction below instead of full scan
        # fraction: 0.1

      - type: partition_stats          # zero Spark action — always free

      - type: threshold                 # evaluates a SQL aggregate; writes {"passed": bool}
        expr: "MAX(amount) > 0"        # required — must resolve to a truthy/falsy scalar
```

|**Signal type**|**I/O Cost**|**Spark Actions**|**Implementation**|
| :- | :- | :- | :- |
|**schema\_snapshot**|Zero|0|`df.schema` is always available on a lazy DataFrame. Writes DuckDB row + JSON file.|
|**row\_count\_estimate** (spark\_listener)|Zero|0|Reads `recordsWritten` from SparkListener stage metrics in `obs.db`. No Spark action.|
|**row\_count\_estimate** (sample)|**Full dataset scan**|1|`df.sample(fraction).count()` — `sample()` is a row-level filter, not partition prune: all data is read.|
|**null\_rates**|**Full dataset scan**|1|`df.sample(fraction).agg(...)` — despite the name, `sample()` reads 100% of the data before discarding rows. A 1% sample on a 10 TB table reads 10 TB.|
|**sample\_rows**|First partition(s) only|1|`df.limit(n).collect()` — stops after N rows from the first partition(s).|
|**value\_distribution**|**Full dataset scan**|1|`df.sample(fraction).agg(min, max, mean, stddev, count)`. Percentiles via `approxQuantile`. Numeric columns only by default.|
|**distinct\_count**|**Full dataset scan**|1|`df.sample(fraction).agg(approx_count_distinct(c) for each col)`. Approximate; error < 5%.|
|**data\_freshness**|**Full dataset scan**|1|`df.select(max(column)).collect()`. Set `allow_sample: true` to use a fraction instead (trades accuracy for speed in production).|
|**partition\_stats**|Zero|0|`df.rdd.getNumPartitions()`. Purely a driver-side call.|
|**threshold**|Depends on expr|1|`df.selectExpr(expr).collect()[0][0]`. Evaluates any SQL aggregate expression. Writes `{"passed": bool, "value": <result>, "expr": "<expr>"}`. **This is the only signal type that produces a `passed` key** — required to close a Regulator gate based on data.|

|**Production guard:**  `row_count_estimate` (sample method), `null_rates`, `value_distribution`, and `distinct_count` are suppressed when `danger.allow_full_probe_actions: false` is set in `aqueduct.yml`. `data_freshness` is also suppressed unless `allow_sample: true`. `schema_snapshot`, `sample_rows`, `partition_stats`, and `threshold` are never suppressed.|
| :- |

|**REGULATOR**|**Trigger gate — passive by default, blocks downstream on False or error signal**|
| :-: | :- |

```yaml
- id: quality_gate
  type: Regulator
  label: "Block downstream if dedup removed too many rows"
  config:
    on_block: skip                 # skip | abort | trigger_agent
    timeout_seconds: 0             # 0 = no timeout (default); polls every 2s until signal or timeout
```

Wiring a Regulator — the gate is passive until something is connected to its input port:

edges:

```yaml
# Wire a boolean signal to the Regulator
- from: probe_after_dedup
  to: quality_gate
  port: signal                  # signal port — carries boolean or error

# Downstream only executes if quality_gate passes
- from: quality_gate
  to: write_daily_orders
```

|**State**|**Behaviour**|
| :- | :- |
|**Passive (nothing wired to signal port)**|Regulator does not exist in the compiled Manifest. It is compiled away entirely by the Compiler. Zero runtime overhead.|
|**Signal = True**|Gate opens. Downstream executes normally.|
|**Signal = False**|Gate closes. Downstream is blocked. on\_block determines behaviour: skip (mark downstream as skipped in RunRecord), abort (fail the pipeline), trigger\_agent (invoke LLM loop with gate context).|
|**Signal = error type / exception**|Treated identically to False. Gate closes. on\_block applies.|
|**Signal = None / null**|Treated as True. Gate opens. This preserves passive-by-default behaviour for Probes that do not emit a signal on a given run.|

|**Implementation note:**  The Regulator is evaluated by the Surveyor by reading `probe_signals` from `obs.db`. It looks for a `passed` key in the signal payload — only the `threshold` signal type writes this key. All other Probe signal types (`schema_snapshot`, `null_rates`, etc.) write raw metrics and cannot directly close a gate. To wire a Probe to a Regulator, include at least one `threshold` signal. When `timeout_seconds > 0` and the gate is closed, the executor polls every 2s until the gate opens or the timeout expires; on timeout, `on_block` applies (same as if the gate had been closed immediately).|
| :- |

|**SPILLWAY**|**Error output port — present on every Module, routes row-level failures**|
| :-: | :- |

Every Module implicitly has a main output port and a spillway port. By default, row-level errors propagate to the Surveyor as pipeline failures. When a spillway is wired, error rows are diverted to a designated downstream Module instead of failing the pipeline.

edges:

```yaml
- from: cast_and_clean
  to: write_clean_orders
  port: main

- from: cast_and_clean
  to: write_quarantine
  port: spillway
  error_types:                   # optional filter — only route these error types
    - CastException
    - NullValueException
```

The spillway DataFrame received by write\_quarantine contains all original columns plus two appended system columns:

|**System column**|**Description**|
| :- | :- |
|**\_aq\_error\_module**|The id of the Module that produced the error.|
|**\_aq\_error\_msg**|The exception message or error description string.|
|**\_aq\_error\_type**|The exception class name (e.g. CastException).|
|**\_aq\_error\_ts**|Timestamp when the error was recorded.|

|**Implementation:**  Spillway routing uses SQL `filter()` on a `spillway_condition` expression — a pure Spark SQL operation, fully vectorized, no Python boundary. The `_aq_error_*` columns are appended via `withColumn(F.lit(...))` — also native. This is a single-pass operation; no separate Spark job or action is triggered.|
| :- |

> **Spillway cannot catch UDF runtime exceptions.** A Python (or Java) UDF that **raises an exception** causes the entire Spark task to abort — not the row to be routed to the spillway. Spark provides no mechanism to catch an exception inside a UDF and divert that row to an alternate output. The spillway `spillway_condition` is evaluated *after* the transformation succeeds; it cannot handle a transformation that throws.
>
> **Consequence:** Any pipeline where a Channel calls a UDF that may throw must handle errors *inside the UDF* before they propagate to Spark. The correct pattern:
>
> ```python
> # Defensive UDF: never raises — returns None on error
> @udf(returnType=StructType([
>     StructField("result", DoubleType(), nullable=True),
>     StructField("error",  StringType(), nullable=True),
> ]))
> def safe_parse_amount(raw):
>     try:
>         return (float(raw.replace(",", "")), None)
>     except Exception as e:
>         return (None, str(e))
> ```
>
> Then route error rows via `spillway_condition`:
>
> ```yaml
> spillway_condition: "safe_parse_amount_result.error IS NOT NULL"
> ```
>
> This keeps Spark fully vectorized (the UDF is still row-at-a-time, but it never raises) and lets the spillway do its job.

|**ARCADE**|**Encapsulated sub-pipeline — reusable Blueprint embedded as a single Module**|
| :-: | :- |

```yaml
- id: enrich_with_customer
  type: Arcade
  label: "Standard customer data enrichment"
  ref: arcades/customer_enrichment.yml   # path to sub-Blueprint
  context_override:                       # local context passed to Arcade
    input_table: dedup_orders
    lookup_table: ${ctx.tables.customers}
  spillway: quarantine_enrichment_errors  # Arcade-level spillway target
```

Arcade sub-Blueprints declare their required context keys:

\# arcades/customer\_enrichment.yml

```yaml
aqueduct: "1.0"

id: arcade.customer_enrichment

required_context:                  # validated at parse time — error if missing
  - input_table
  - lookup_table

modules:
  - id: read_input
    type: Ingress
    config:
      format: dataframe             # special format: reads from parent pipeline
      ref: ${ctx.input_table}       # resolves to the parent Module id
```

|**Expansion contract:**  Arcades are fully expanded at Manifest compile time. The Manifest always contains a flat Module list. Spark sees a single flat execution plan with no nesting. Module IDs within Arcades are namespaced (`{arcade_id}__{child_id}`) to prevent collision. The `__` separator is safe for Spark `createTempView` (dots would be parsed as multi-part identifiers). If expansion produces a duplicate ID (e.g. an existing module is already named `arcade__child`), a `CompileError` is raised with a clear message.|

> **Naming constraint:** Blueprint module IDs must not contain `__` (double underscore). Any ID with `__` collides with the Arcade expansion namespace and will cause a `ParseError` at validation time with message: `"Module ID '{id}' contains '__' which is reserved for Arcade expansion. Use a single underscore or hyphen instead."` This is enforced by the Parser before compilation.|
| :- |

|**ASSERT**|**Inline data quality gate — evaluates rules on a flowing DataFrame, routes failures to spillway or aborts**|
| :-: | :- |

Assert is an inline module (wired via main-port edges, not `attach_to`). It intercepts the data flow, evaluates a set of rules, and either passes the DataFrame downstream or quarantines/aborts depending on the per-rule `on_fail` action. Unlike Probe+Regulator (passive tap + gate), Assert can quarantine individual failing rows via its spillway port.

```yaml
- id: orders_quality_gate
  type: Assert
  label: "Data quality gate"
  config:
    rules:
      # schema_match — zero Spark action; raises AssertError if schema differs
      - type: schema_match
        expected: {order_id: STRING, amount: "DECIMAL(18,4)", order_ts: TIMESTAMP}
        on_fail: abort

      # min_rows — batched into shared df.agg()
      - type: min_rows
        min: 1000
        on_fail: abort

      # null_rate — one shared df.sample().agg() for all null_rate rules
      - type: null_rate
        column: order_id
        fraction: 0.1       # sample fraction for the null check
        max: 0.0
        on_fail: abort

      # freshness — max(column) in batched agg()
      - type: freshness
        column: order_ts
        max_age_hours: 26
        on_fail:
          action: webhook
          url: "${ctx.alert_webhook}"

      # sql — arbitrary aggregate expression, TRUE = pass
      - type: sql
        expr: "SUM(amount) > 0"
        on_fail: warn

      # sql_row — lazy filter; failing rows routed to spillway port
      - type: sql_row
        expr: "amount > 0 AND order_id IS NOT NULL"
        on_fail: quarantine

      # custom — Python callable: fn(df) -> {passed, message, quarantine_df}
      - type: custom
        fn: "my_project.quality.validate_orders"
        on_fail: quarantine

edges:
  - from: upstream_module
    to: orders_quality_gate       # main-port data flow in
  - from: orders_quality_gate
    to: good_egress               # passing rows out
  - from: orders_quality_gate
    to: quarantine_egress
    port: spillway                # quarantine rows out (sql_row / custom rules)
```

**Rule types:**

| Rule | Spark cost | Description |
| :- | :- | :- |
| `schema_match` | Zero | Compares `df.schema` to expected dict. No scan. |
| `min_rows` | Batched into one `df.agg()` | Asserts `COUNT(*) >= min`. |
| `max_rows` | Batched into one `df.agg()` | Asserts `COUNT(*) <= max`. Useful for detecting unexpected data explosions. |
| `null_rate` | One shared `df.sample().agg()` for all null_rate rules | Asserts null fraction ≤ max on a sampled column. |
| `freshness` | Batched into one `df.agg()` | Asserts `MAX(column) >= NOW() - max_age_hours`. |
| `sql` | Batched into one `df.agg()` | Evaluates an aggregate SQL expression; result must be truthy. |
| `sql_row` | Lazy filter (no action) | Rows failing `expr` go to spillway; passing rows continue. |
| `spillway_rate` | 2 Spark actions post-row-level (`df.count` + `quarantine.count`) | Asserts fraction of rows reaching spillway ≤ `max`. Prevents "silent data loss" where the Spark job succeeds but most data was quarantined. |
| `custom` | Determined by callable | Python fn receives full DataFrame; returns `{passed, message, quarantine_df}`. |

**Performance model:** All aggregate rules (`min_rows`, `freshness`, `sql`) are collected into a single `df.agg()` call. All `null_rate` rules share a single `df.sample(fraction).agg()` call. Schema match is zero-action. Row-level rules (`sql_row`, `custom`) are lazy filters — no Spark action. `spillway_rate` adds 2 Spark actions (`df.count` + `quarantine_df.count`) and is evaluated after row-level rules. Net: **at most 2 Spark actions** without `spillway_rate`; **4 actions maximum** when `spillway_rate` is configured.

**`spillway_rate` rule example:**
```yaml
- type: spillway_rate
  max: 0.05          # fail if > 5% of input rows are quarantined
  on_fail: abort     # or trigger_agent to invoke self-healing
```
Use this whenever you have `sql_row` or `custom` quarantine rules to prevent the pipeline from "succeeding" with 90% of data silently dropped.

**`on_fail` actions (per rule):**

| Action | Behaviour |
| :- | :- |
| `abort` | Raises `AssertError`; pipeline stops and enters Surveyor failure handling. |
| `warn` | Logs a warning; pipeline continues. |
| `webhook` | POSTs an alert to the configured URL; pipeline continues. Combined as `{action: webhook, url: ...}`. |
| `quarantine` | Failing rows routed to the module's spillway port. Valid for `sql_row`, `custom`, and `freshness` rules only — see note below. |
| `trigger_agent` | Raises `AssertError(trigger_agent=True)`; Surveyor invokes the LLM self-healing loop. |

**`on_fail: quarantine` validity:**

`quarantine` requires a per-row predicate. Only rule types that can derive one support it:

| Rule type | Quarantine supported | Row predicate |
|---|---|---|
| `sql_row` | Yes | The rule's `expr` directly |
| `custom` | Yes | Callable returns `quarantine_df` |
| `freshness` | Yes | `column >= now() - INTERVAL max_age_hours HOURS` (auto-derived) |
| `min_rows`, `max_rows`, `sql`, `null_rate` | **No — compile error** | No derivable per-row predicate |

Using `on_fail: quarantine` on `min_rows`, `max_rows`, `sql`, or `null_rate` raises a `CompileError`. Previously these silently degraded to warn (and passed bad data through) — this was removed as a footgun.

**`error_type` field (typed label for guardrail matching):**

Every rule accepts an optional `error_type:` string. When the rule fires with `on_fail: abort` or `on_fail: trigger_agent`, the label is attached to the `AssertError` and propagated through `ModuleResult.error_type` → `FailureContext.error_type`. The LLM pre-trigger guardrails (`heal_on_errors`, `never_heal_errors`) match against this label.

```yaml
rules:
  - type: min_rows
    min: 1000
    on_fail: abort
    error_type: EmptyDataset        # matched by never_heal_errors: [EmptyDataset]

  - type: null_rate
    column: order_id
    max: 0.0
    on_fail: trigger_agent
    error_type: DataQualityViolation   # LLM fires only for this class of error
```

Infrastructure errors (Spark exceptions, network errors) that are not raised by Assert rules have `error_type: null`. The guardrail check falls back to matching the exception class name from the stack trace (e.g. `SparkException`, `AnalysisException`).

**`freshness` + `quarantine` pattern** — eliminates the need for a paired `sql_row` rule:
```yaml
rules:
  # Abort if entire table is dead (no data within 24h)
  - type: freshness
    column: event_time
    max_age_hours: 24
    on_fail: abort

  # Quarantine individual stale rows (>12h) — predicate auto-derived
  - type: freshness
    column: event_time
    max_age_hours: 12
    on_fail: quarantine
```
A spillway edge must be present when `freshness` uses `on_fail: quarantine` — the compiler raises `CompileError` if not, to prevent silent row discard.

**Implementation note:** Assert is executed by `aqueduct.executor.spark.assert_.execute_assert()`. It returns `(passing_df, quarantine_df | None)`. The executor wires `quarantine_df` into `frame_store["{module_id}.spillway"]` if a spillway edge exists. Using `on_fail: quarantine` without a connected spillway edge is a `CompileError` — the compiler catches this before any Spark job runs.


# **5. Context Registry**
## **5.1 Overview & Design Contract**
The Context Registry is the variable system for Blueprints. It has one inviolable contract: any value that can be resolved at parse time must be resolved at parse time, before the Manifest is written and before any Spark job starts. This guarantees the Manifest is always fully concrete — no deferred resolution, no hidden state.

## **5.2 Three-Tier Resolution Model**

|**Tier**|**Syntax**|**Resolved at**|**Performance cost**|
| :- | :- | :- | :- |
|Tier 0 — Static|${ctx.namespace.key}|Parse time (Parser layer)|Zero — substituted before Manifest is written. Spark never sees the token.|
|Tier 1 — Runtime function|@aq.fn(args)|Pre-job (Compiler layer)|Driver-only, milliseconds. Resolved after Tier 0 but before any Spark job starts.|
|Tier 2 — UDF|udf\_id in udfs: list|Spark execution (Channel op)|Executor cost — distributed. Operates on DataFrame columns, not scalar config values.|

The syntax difference is the hard visual boundary. ${ctx.\*} is Tier 0. @aq.\* is Tier 1. A UDF name in a udfs: list is Tier 2. Engineers and LLM agents can determine resolution tier at a glance without domain knowledge.

## **5.3 Tier 0 — Static Context**
Defined in the context: block. Supports namespacing with dot notation. Supports shell variable passthrough with default values.

```yaml
context:
  env:    ${AQUEDUCT_ENV:-dev}       # reads shell env var, defaults to "dev"
  tables:
    orders: "s3://data/${ctx.env}/orders"
  params:
    dedup_key:  "order_id"
    batch_size: 10000
```

Resolution order (highest priority wins):

1. CLI flags: aqueduct run --ctx env=prod --ctx tables.orders=s3://override/path
1. Environment variables matching AQUEDUCT\_CTX\_\* prefix (e.g. AQUEDUCT\_CTX\_ENV=prod)
1. context\_profiles block for the active profile (--profile flag)
1. context: block static defaults

Profile overrides for environment promotion:

```yaml
context_profiles:
  dev:
    params.batch_size: 100          # small batches in dev
    tables.orders: "s3://dev/orders"
  prod:
    params.batch_size: 50000
    tables.orders: "s3://prod/orders"
```

## **5.4 Tier 1 — Runtime Functions (@aq.\*)**
A curated, versioned set of built-in functions resolved on the driver before any Spark job starts. All return scalar values that substitute into the Blueprint exactly as Tier 0 values do.

**Logical Execution Date:** All `@aq.date.*` functions and `@aq.runtime.timestamp()` evaluate relative to the logical execution date, not the system clock, when `--execution-date YYYY-MM-DD` is passed via the CLI. This enables idempotent backfills: rerunning a pipeline for a historical date yields the same resolved values regardless of when the run actually occurs.

```bash
# Backfill for a specific date — @aq.date.today() resolves to 2026-03-01
aqueduct run pipeline.yml --execution-date 2026-03-01
```

|**Function**|**Description**|
| :- | :- |
|**@aq.date.today()**|Current date as string (or logical execution date if `--execution-date` set). Optional format arg: @aq.date.today(format="yyyy/MM/dd"). Default: yyyy-MM-dd.|
|**@aq.date.yesterday()**|Yesterday relative to logical execution date. Same format arg as today().|
|**@aq.date.offset(base, days)**|Date offset. base is a date string or another @aq.date.\*. days is integer (negative for past). E.g. @aq.date.offset(base=@aq.date.today(), days=-7).|
|**@aq.date.month\_start()**|First day of the current month.|
|**@aq.date.format(date, pattern)**|Reformats a date string using Java SimpleDateFormat pattern.|
|**@aq.runtime.timestamp()**|Current UTC timestamp as ISO-8601 string.|
|**@aq.runtime.run\_id()**|The current run UUID. Useful for writing run-partitioned output.|
|**@aq.runtime.prev\_run\_id()**|The run UUID of the last successful run of this pipeline. Null if no previous run exists.|
|**@aq.secret(key)**|Fetches a secret by key. Provider configured in engine config: AWS Secrets Manager | GCP Secret Manager | Azure Key Vault | HashiCorp Vault | env variable fallback.|
|**@aq.env(var)**|Reads an environment variable. Fails fast at compile time if the variable is not set (unlike ${AQUEDUCT\_ENV:-default} which has a fallback).|
|**@aq.depot.get(key)**|Reads a value from the Depot KV store. Returns null if key does not exist. Useful for watermarks and last-run state.|
|**@aq.depot.get(key, default)**|Reads a value from the Depot with a fallback default if the key does not exist.|

> **⚠ Stale-read footgun:** `@aq.depot.get()` is a Tier 1 function — it resolves at compile time, before any Spark job starts. If Pipeline A compiles with `@aq.depot.get('watermarks.orders')` at T=0, and Pipeline B writes a new watermark value at T=1 during A's execution, A has already compiled with the stale T=0 value. This is by design (Manifest is immutable once compiled), but it means overlapping pipeline runs that share a Depot key will each see the value from their own compile time. Design accordingly: write watermarks at Egress time, read them at the next run's compile time. Do not use `@aq.depot.get()` for values that must reflect within-run writes.

> **⚠ `${ctx.*}` and shell env substitution:** The `${ctx.namespace.key}` syntax resembles POSIX shell variable syntax. If your CI system runs `envsubst`, `sed`, or other substitution tools on YAML files before invoking Aqueduct, `${ctx.*}` tokens will be mangled or silently dropped. **Never run env substitution tools on Blueprint YAML files.** Aqueduct reads the file directly; inject runtime values via `--ctx key=value` flags or `AQUEDUCT_CTX_*` environment variables instead.

## **5.5 UDF Registry (Tier 2)**
UDFs are not Context values — they operate on DataFrame columns during Spark execution, not on scalar config values. They are registered in a udf\_registry block at the top level of the Blueprint or in a shared udf\_registry.yml file referenced by multiple Blueprints.

```yaml
udf_registry:
  - id: clean_phone
    label: "Normalise phone to E.164 format"
    lang: python
    module: udfs.clean_phone
    entry: clean_phone_fn          # function name within the module
    return_type: STRING            # Spark DDL type string
    deterministic: true
  - id: haversine_distance
    label: "Great-circle distance between two lat/lon pairs"
    lang: scala
    jar:  udfs/geo-udfs.jar
    class: com.myco.udfs.Haversine
    return_type: DOUBLE
    deterministic: true
```

UDFs are registered with the SparkSession before any Channel that references them executes. Spillway routing is automatically applied to UDF calls — if a UDF raises an exception on a row, the row is routed to the Channel's spillway port with the exception captured in `_aq_error_msg`.

**Python UDFs — module layout**

Place UDF code in a `udfs/` directory at the project root. Either a single `.py` file or a package with `__init__.py` works — both import the same way:

```
udfs/
  taxi_math.py          # → module: udfs.taxi_math
  geo/
    __init__.py
    haversine.py        # → module: udfs.geo.haversine
```

The `entry` field names the callable inside the module. If omitted, defaults to `id`.

**Decorated UDFs (recommended):**

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

@udf(returnType=DoubleType())
def calc_duration_min(pickup_dt, dropoff_dt):
    if pickup_dt is None or dropoff_dt is None:
        return 0.0
    return float((dropoff_dt - pickup_dt).total_seconds() / 60.0)
```

When the function carries a `returnType` attribute (set by `@udf` or `@pandas_udf`), Aqueduct skips the `return_type` field in Blueprint — the type is already encoded in the function.

**Python version constraint:** PySpark 3.5 bundles cloudpickle 2.2.1, which does not support Python 3.13+. On Python 3.13+, install `cloudpickle` separately (`pip install cloudpickle`) — Aqueduct will automatically patch PySpark's bundled version at UDF registration time. Python ≤ 3.12 requires no extra steps.

**Java/Scala UDFs**

Java/Scala UDFs bypass Python serialisation entirely — they run as JVM bytecode with no cloudpickle dependency:

1. Write a class implementing `org.apache.spark.sql.api.java.UDF1<T, R>` (or `UDF2`, `UDF3`, ...) and package it as a JAR.
2. Reference it in the Blueprint:

```yaml
udf_registry:
  - id: haversine_distance
    lang: java                              # or scala
    jar: udfs/geo-udfs.jar                  # path relative to project root
    entry: com.myco.udfs.Haversine          # fully-qualified class name
    return_type: DOUBLE
```

Aqueduct calls `spark.sparkContext.addJar(jar_path)` and `spark.udf.registerJavaFunction(id, class_name, return_type)`. No additional Spark configuration required for local mode.

**UDAFs (User-Defined Aggregate Functions)**

UDAFs aggregate across rows (custom `SUM`, `AVG`, windowed stats). In PySpark, use `@pandas_udf` with `PandasUDFType.GROUPED_AGG`:

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType

@pandas_udf("double", PandasUDFType.GROUPED_AGG)
def weighted_avg(value: pd.Series, weight: pd.Series) -> float:
    return (value * weight).sum() / weight.sum()
```

Register in Blueprint identically to regular Python UDFs. Aqueduct detects the `returnType` attribute and skips the redundant `return_type` Blueprint field. UDAFs are only valid in grouped SQL queries — call them in a Channel `query` with a `GROUP BY` clause.

## **5.6 Depot — Persistent KV Store**
The Depot is Aqueduct's built-in state store for cross-run persistence. It supports watermarking, incremental load patterns, and cross-pipeline coordination.

\# Reading from Depot in Context (Tier 1)

context:

```yaml
context:
  watermarks:
    last_orders_ts: "@aq.depot.get('watermarks.orders', '1970-01-01')"

# Writing to Depot via Egress Module

- id: update_watermark
  type: Egress
  label: "Update orders watermark after successful run"
  config:
    format: depot
    key:    watermarks.orders
    value:  "@aq.runtime.timestamp()"
    access: readwrite              # readwrite | readonly (default: readwrite for Egress)
```

|**Flag**|**Behaviour**|
| :- | :- |
|**access: readonly**|The key can be read via @aq.depot.get() but cannot be written by any Egress in this pipeline. Default for all keys unless explicitly overridden.|
|**access: readwrite**|The key can be read and written. Must be explicitly declared on the Egress Module config. Any attempt to write a readonly key raises a compile-time error.|
|**access: writeonly**|The key can be written but not read within this pipeline. Useful for keys consumed by other pipelines.|


# **6. Observability, Probes & Flow Report**
## **6.1 Design Constraint**
All observability in Aqueduct is governed by one constraint: no Spark actions may be added to the critical execution path. Every signal source has an explicit cost model documented in Section 6.2. Engineers selecting Probe signals must understand the cost they are accepting.

## **6.2 Signal Cost Model**

|**Signal**|**Mechanism**|**Performance cost**|**Accuracy**|
| :- | :- | :- | :- |
|schema\_snapshot|DataFrame.schema property|Zero — metadata read, no scan|Exact|
|row\_count\_estimate (spark\_listener)|SparkListener task metrics (recordsWritten)|Zero — taps internal event bus|Exact for completed stages|
|stage\_duration|SparkListener onStageCompleted event|Zero — taps internal event bus|Exact|
|shuffle\_size|SparkListener task metrics (shuffleWriteBytes)|Zero — taps internal event bus|Exact|
|partition\_stats|df.rdd.getNumPartitions()|Zero — driver-side call|Exact|
|column\_type\_drift|Schema comparison at parse time (static)|Zero — compile time only|Exact|
|sample\_rows|df.limit(N).collect()|Low — Spark satisfies LIMIT from first partition(s)|Exact for N rows|
|null\_rates|sample(frac).agg(...)|**Full dataset scan** — sample() is a row-level filter, not a partition prune. All data read; (1−frac) rows discarded after I/O.|Approximate (±sampling error)|
|row\_count\_estimate (sample)|df.sample(frac).count()|**Full dataset scan** — see null\_rates note.|Approximate|
|value\_distribution|df.sample(frac) + approx\_count\_distinct()|**Full dataset scan** — see null\_rates note.|Approximate|
|distinct\_count|df.sample(frac) + approx\_count\_distinct()|**Full dataset scan** — see null\_rates note.|Approximate|
|data\_freshness|df.select(max(column)).collect()|**Full dataset scan** (fraction=0 default)|Exact (or approx if allow\_sample=true)|

> **Note:** `null_rates`, `row_count_estimate` (sample method), `value_distribution`, and `distinct_count` all use `df.sample()` — a row-level random filter that reads every partition. The compiler emits a warning for each. Use `method: spark_listener` for zero-cost row counts. See [docs/SPARK_GUIDE.md#probe-sample-cost](SPARK_GUIDE.md#probe-sample-cost).

**Auto-collected vs. user-configurable:** `stage_duration`, `shuffle_size`, and `column_type_drift` are collected automatically by the executor via SparkListener and schema comparison — they are not user-configurable Probe signals. All other signals in §4.4 (`schema_snapshot`, `row_count_estimate`, `null_rates`, `sample_rows`, `value_distribution`, `distinct_count`, `data_freshness`, `partition_stats`) are Probe config options.

## **6.3 Observability Store Schema**
All signals are written to the Observability Store as structured records. Signals are appended as newline-delimited JSON (or Parquet) files partitioned by run\_id. DuckDB queries these files directly — no import step required. The logical schema below describes the shape of each record; there is no DDL to manage.

**Why DuckDB over SQLite:** SQLite serialises all writes through a single writer lock. The Surveyor reads while the Executor writes concurrently, which reliably produces `database is locked` errors under load. DuckDB handles concurrent reads with MVCC and can query Parquet files on disk with zero ETL overhead, making it the correct embedded backend for this access pattern.

```text
Table: signals
  run_id        TEXT        -- UUID for this pipeline invocation
  module_id     TEXT        -- ID of the Module that emitted the signal
  signal_type   TEXT        -- schema_snapshot | row_count | null_rates | ...
  spark_stage   INTEGER     -- Spark stage ID from SparkListener (null if N/A)
  emitted_at    TIMESTAMP   -- UTC timestamp of emission
  payload       JSON        -- signal-type-specific structured data
  run_phase     TEXT        -- executing | retrying | patching | completed
```

```text
Table: run_records
  run_id        TEXT PRIMARY KEY
  blueprint_id   TEXT
  blueprint_sha TEXT        -- git SHA of Blueprint at run time
  manifest_path TEXT        -- path to compiled Manifest JSON
  started_at    TIMESTAMP
  completed_at  TIMESTAMP
  status        TEXT        -- running | success | error | patched | skipped
  patch_count   INTEGER     -- number of patches applied in this run
```

## **6.4 Module Metrics Collection**
Aqueduct collects per-module I/O metrics using `DataFrame.observe()` (Spark 3.3+) and filesystem inspection. No extra Spark actions are triggered.

**Minimum recommended Spark version: 3.3** for row count collection. On Spark < 3.3, `observe()` is unavailable — `records_read` and `records_written` in `module_metrics` will be `NULL` (not `0` — `0` would imply a measured result of zero records, which is incorrect). `bytes_read`, `bytes_written`, and `duration_ms` are still collected.

Metrics collected per module:

| Metric | Source | Spark 3.3+ | Spark < 3.3 |
|---|---|---|---|
| `records_written` | `df.observe()` on Egress write | ✓ exact | NULL |
| `bytes_written` | filesystem inspection post-write | ✓ local paths | ✓ local paths |
| `records_read` | not collected (no write action at Ingress) | NULL | NULL |
| `bytes_read` | filesystem inspection at Ingress read | ✓ local paths | ✓ local paths |
| `duration_ms` | wall-clock around module action | ✓ | ✓ |

Cloud paths (`s3://`, `hdfs://`, `gs://`, `abfs://`) return `NULL` for byte metrics — filesystem inspection requires local path access. `NULL` in any metric field means "not collected", never "zero records processed."

## **6.5 Flow Report**
The Flow Report is generated by the Surveyor after each run. It is a per-column, per-Module quality summary sourced from Probe signals. It is stored in the Lineage Store and accessible via aqueduct report <run\_id>.

Each row in the Flow Report represents one column at one Module's output:

|**Field**|**Description**|
| :- | :- |
|**module\_id**|The Module whose output was measured.|
|**column\_name**|Column name as it appears in the output schema.|
|**spark\_type**|Spark DDL type string at this Module's output. Compared to the previous run to detect drift.|
|**status**|OK | Degraded | Error. OK: all thresholds passed. Degraded: one or more soft thresholds breached. Error: hard error (type cast failure, exception, missing column).|
|**null\_rate**|Null rate from Probe sample. Null if no null\_rates Probe is attached.|
|**row\_estimate**|Row count estimate from SparkListener. Always present for completed stages.|
|**detail**|Human-readable description of any issue. Used in LLM FailureContext and UI display.|
|**type\_drift**|Boolean. True if the Spark type changed compared to the previous successful run.|


# **7. Lineage**
## **7.1 Two Lineage Layers**
Aqueduct maintains two distinct lineage layers with different costs, consumers, and update frequencies. They must not be conflated.

|**Layer**|**Description**|
| :- | :- |
|**Structural Lineage (static)**|Derived from the Blueprint at parse time. For every output column in every Module, traces which upstream columns contributed to it by walking the AST. Zero runtime cost. Available before the pipeline runs. Stored as a ColumnLineageGraph JSON in the Lineage Store. Updated whenever the Blueprint changes.|
|**Runtime Quality Lineage**|Populated after each run from Probe signals and Flow Report data. Records per-column quality status, type drift, null rate trends, and error locations at each Module. Appended to the Lineage Store per run\_id. Enables historical quality trend analysis.|

## **7.2 Structural Lineage — ColumnLineageGraph**
The Parser builds a ColumnLineageGraph as a by-product of AST construction. The graph is a directed acyclic graph where:

- Nodes are (module\_id, column\_name) pairs.
- Edges represent data flow: an edge from (module\_A, col\_x) to (module\_B, col\_y) means col\_y in module\_B was derived from col\_x in module\_A.
- Transformation edges carry a transform\_op annotation (the Channel operation that produced the derivation).

For SQL Channels, column lineage is derived using **sqlglot** — an open-source SQL parser and transpiler with native Apache Spark dialect support. sqlglot's `lineage.lineage()` function walks the AST of a SELECT expression and returns a lineage graph of source-column → output-column relationships in approximately 10 lines of Python. Complex expressions (CASE, UDF calls) are annotated as derived\_from: [list of input columns] with transform\_op: expression. This replaces any requirement to build or maintain a custom SQL parser.

The ColumnLineageGraph supports two query patterns used in practice:

- Impact analysis: "if I change column X in Module A, which downstream columns and Modules are affected?" — traverses forward edges.
- Root cause tracing: "where did this null value in column Y at Module B come from?" — traverses backward edges to find the earliest Module where nulls were introduced.

## **7.3 Lineage Store Schema**

```text
Table: column_lineage
  blueprint_sha   TEXT        -- graph is keyed to a specific Blueprint version
  blueprint_id     TEXT
  from_module     TEXT
  from_column     TEXT
  to_module       TEXT
  to_column       TEXT
  transform_op    TEXT        -- e.g. "sql:CAST", "deduplicate", "join:left"
```

```text
Table: quality_lineage
  run_id          TEXT
  blueprint_id     TEXT
  module_id       TEXT
  column_name     TEXT
  spark_type      TEXT
  status          TEXT        -- OK | Degraded | Error
  null_rate       REAL
  row_estimate    INTEGER
  type_drift      BOOLEAN
  detail          TEXT
  recorded_at     TIMESTAMP
```


# **8. Self-Healing & LLM Agent Loop**
## **8.1 Design Philosophy**
The LLM agent operates within a grammar, not in free-form code generation mode. It can only propose structured PatchSpec operations — valid, schema-checked modifications to the Blueprint. This constraint is not a limitation: it makes every agent action auditable, reversible, Git-diffable, and explainable to a human reviewer. The agent cannot produce an invalid Blueprint.

**On LLM cost and context window size:** The default FailureContext is intentionally generous. Even a full, unpruned context package of approximately 40,000 tokens costs less than $0.50 per patch attempt with current frontier models. Over a month of pipeline operations, this is orders of magnitude cheaper than maintaining on-call engineering coverage. The ContextPruner (Section 8.4) exists to improve model accuracy and reduce latency — not primarily to reduce cost. Engineers should size `aggressive_max_patches` based on risk tolerance for auto-applied patches, not on token cost.

## **8.2 Retry Policy**
The retry\_policy block defines behaviour before the LLM agent is invoked. The Surveyor applies this policy on failure before escalating to the agent loop.

```text
retry_policy:
  max_attempts: 3
  backoff:
    strategy: exponential      # linear | exponential | fixed
    base_seconds: 30
    max_seconds: 600
    jitter: true               # adds random ±20% to backoff interval
  transient_errors:            # retried without agent involvement
    - SparkException: ".*Connection reset.*"
    - SparkException: ".*Task not serializable.*"
    - Py4JNetworkError
  non_transient_errors:        # go directly to agent loop, no retry
    - AnalysisException
    - SchemaIncompatibleException
    - ParseException
  on_exhaustion: trigger_agent # trigger_agent | abort | alert_only
```

## **8.3 Trigger Conditions**
The Surveyor triggers the agent loop on any of the following:

|**Trigger**|**Description**|
| :- | :- |
|**Pipeline exception**|An unhandled exception at any Module after retry policy is exhausted.|
|**Non-transient error**|Any error matching the non\_transient\_errors list triggers the agent immediately, bypassing retry.|
|**Regulator block with trigger**|A Regulator with on\_block: trigger\_agent fires when its gate closes.|
|**Probe threshold breach**|A Probe alert\_threshold is breached, generating a HealthEvent with severity: error.|
|**Manual invocation**|Engineer runs: aqueduct heal <run\_id> [--module <module\_id>]|

## **8.4 FailureContext Package**
Before invoking the LLM, the Surveyor assembles a FailureContext — a complete, self-contained JSON document. The agent must never need to make additional queries to understand what happened.

### Provenance Layer

Every resolved config value in the Manifest carries **provenance metadata** — a record of where it came from in the Blueprint source. This is built during compilation and stored as `ProvenanceMap` on the Manifest. On failure, a slice of the ProvenanceMap (failed module + full context block) is serialised as `provenance_json` in the FailureContext and passed to the LLM instead of the raw Blueprint YAML source.

**Why:** The LLM previously received both the compiled Manifest (resolved values) and the raw Blueprint YAML (template expressions), and had to mentally reverse-engineer the compilation to generate correct patch ops. This breaks for Arcade-expanded modules (their IDs don't exist in the Blueprint YAML) and context refs (`${ctx.*}` values in patch ops confuse guardrails). The provenance layer eliminates the impedance mismatch.

**`ValueProvenance` source types:**

| source_type | Meaning | Correct patch op |
|---|---|---|
| `literal` | Hardcoded value in Blueprint YAML | `set_module_config_key` on the module |
| `context_ref` | `${ctx.key}` reference | `replace_context_value(key=...)` |
| `env_ref` | `${ENV_VAR}` environment variable | Change env var — do not patch Blueprint |
| `tier1` | `@aq.*` function call | `set_module_config_key` with literal replacement |
| `arcade_inherited` | Value injected via arcade `context_override` | `replace_context_value` on parent context key — **module ID does not exist in Blueprint YAML** |

**Arcade-expanded modules** (IDs containing `__`) are compile-time artefacts. The LLM is explicitly instructed never to use `set_module_config_key` on such IDs. The provenance section in the LLM prompt shows:
- Which parent arcade the module came from
- Which `context_override` key injected each value
- Which parent context key maps to the resolved value
- A warning that the module does not exist in Blueprint YAML

**Guardrails** use the ProvenanceMap to resolve `${ctx.*}` path values before matching against `allowed_paths` patterns, eliminating false positives when the LLM references a context key instead of a literal path.

### ContextPruner

Before the FailureContext is sent to the model, it passes through a **ContextPruner** component. The pruner trims irrelevant sections to improve model accuracy and response latency. Pruning rules:

| **Error class** | **Manifest scope included** | **spark\_config included** |
| :- | :- | :- |
| `ColumnNotFound`, `TypeMismatch`, `AnalysisException` | Failed module + 2 upstream modules (via lineage graph) + 2 immediate downstream modules | No |
| `SparkException` containing "OutOfMemory" or "shuffle" | Full manifest | Yes |
| All other errors | Failed module + direct upstream modules | No |

The unpruned full manifest is always written to disk at `runs/<run_id>/failure_context_full.json` for post-mortem inspection. The LLM receives only the pruned version.

> **Cost note:** Context pruning is applied to improve model accuracy and response latency. From a cost perspective, even unpruned FailureContext packages (approx. 40k tokens) result in a per-incident cost of < $0.50. Over a month of pipeline operations, this is orders of magnitude less expensive than maintaining on-call engineering coverage. The pruning logic exists to improve accuracy and reduce latency, not primarily to reduce cost.

```JSON
{
  "run_id":             "run_20240412_143022_a3f9",
  "blueprint_id":        "pipeline.orders.daily_aggregate",
  "failed_module":      "cast_and_clean",
  "failed_module_label":"Cast types and strip whitespace",
  "failure_type":       "AnalysisException",
  "error_message":      "Cannot resolve column 'event_ts' ...",
  "stack_trace":        "...",
  "manifest_snapshot":  { /* pruned: failed module + 2 upstream + 2 downstream */ },
  "structural_lineage": { /* ColumnLineageGraph for failed Module and pruned scope */ },
  "probe_signals": [
    { "module": "read_orders",  "signal": "schema_snapshot", "payload": {...} },
    { "module": "dedup_orders", "signal": "null_rates",      "payload": {...} }
  ],
  "flow_report_partial": [ /* Flow Report rows up to point of failure */ ],
  "retry_history": [
    { "attempt": 1, "error": "...", "duration_seconds": 34 }
  ],
  "previous_patches": [  /* all patches applied to this pipeline historically */ ],
  "depot_state": {       /* relevant Depot keys read/written by this pipeline */ },
  "inputs_fingerprint": {
    "read_orders": {
      "path": "data/orders.parquet",
      "size_bytes": 104857600,
      "last_modified": "2025-05-11T08:00:00+00:00"
    }
  }
}
```

### Input Fingerprinting

Every compiled Manifest includes an **`inputs_fingerprint`** — a compile-time snapshot of each local Ingress module's file metadata:

| Field | Type | Description |
|---|---|---|
| `path` | string | Resolved file path as it appears in config |
| `size_bytes` | int \| null | File size in bytes at compile time. `null` for remote paths or if stat fails. |
| `last_modified` | ISO-8601 string \| null | File mtime in UTC at compile time. `null` for remote or unresolvable paths. |

Fingerprinting is zero-cost (no Spark action — only a Python `os.stat()` call on the driver). Remote paths (`s3://`, `s3a://`, `gs://`, `hdfs://`, `abfs://`, `wasbs://`) and non-file formats (`jdbc`, `kafka`, `depot`, `dataframe`) produce `null` fields rather than being skipped, so the key is always present for each Ingress.

**Use in post-mortems:** When the LLM receives a FailureContext, `inputs_fingerprint` allows distinguishing data-drift bugs (file grew or changed since last run) from code bugs (same file, different pipeline). The LLM prompt surfaces this as: *"Input file `data/orders.parquet` was 100 MB and last modified 08:00 UTC."*

## **8.5 Patch Grammar — PatchSpec Operations**
The agent responds exclusively with a PatchSpec JSON object. Any response that is not a valid PatchSpec is rejected, and the agent is re-prompted with the validation error. The agent may include multiple operations in one PatchSpec — there is no limit on the number of operations per patch, enabling full pipeline restructuring.

|**Operation**|**Description**|
| :- | :- |
|**replace\_module\_config**|Replace the **entire** config block of a named Module. Use when multiple config keys must change together, or when restructuring the module type. The whole `config:` dict is replaced.|
|**set\_module\_config\_key**|Set a **single** key inside a module's config block, leaving all other keys intact. Prefer over `replace_module_config` for targeted fixes (path typo, one param, one column name). The key may be a dotted path (e.g. `"schema_hint.columns.order_id"`) for nested values.|
|**replace\_module\_label**|Update the label of a Module. Used when the agent renames a Module for clarity after restructuring.|
|**insert\_module**|Insert a new Module at a specified position in the graph. Requires specifying upstream and downstream edges. Used to add a cast step, filter, Probe, or Regulator.|
|**remove\_module**|Remove a Module and rewire its edges. The agent must specify how to reconnect the graph. Used to eliminate a broken step.|
|**replace\_context\_value**|Update a Tier 0 or Tier 1 value in the Context Registry. Used when a path, key, threshold, or parameter is wrong.|
|**add\_probe**|Attach a new Probe to a Module's output. Commonly added by the agent before a retry to gather more signal.|
|**replace\_edge**|Rewire an existing edge. Used when the agent determines data should flow through a different path.|
|**set\_module\_on\_failure**|Change the retry or spillway policy for a specific Module.|
|**replace\_retry\_policy**|Replace the pipeline-level retry policy. Used when the agent determines the current policy is causing repeated failures.|
|**add\_arcade\_ref**|Reference a new or existing Arcade sub-Blueprint. Used when the agent restructures repeated logic into a reusable Arcade.|

**Patch metadata fields** (returned by the LLM alongside operations):

| Field | Type | Description |
|---|---|---|
| `confidence` | float 0.0–1.0 | LLM-estimated probability this patch fixes the failure. If < 0.7, patch is auto-escalated to human review regardless of `approval_mode`. |
| `category` | string | Failure category: `schema_drift`, `bad_path`, `format_mismatch`, `oom_config`, `sql_column_not_found`, `type_mismatch`, `missing_context`, `permission_error`, `other`. Used in `healing_outcomes` analytics. |
| `root_cause` | string | One-sentence root cause diagnosis. Included in patch rationale. |

Example PatchSpec (single operation):

```json
{
  "patch_id":   "patch_20240412_143155",
  "run_id":     "run_20240412_143022_a3f9",
  "rationale":  "Column event_ts is STRING but cast to TIMESTAMP without explicit format. Replacing with explicit format string cast.",
  "confidence": 0.92,
  "category":   "type_mismatch",
  "root_cause": "event_ts column is STRING type but used directly in TIMESTAMP cast without format argument.",
  "operations": [
    {
      "op":        "set_module_config_key",
      "module_id": "cast_and_clean",
      "key":       "query",
      "value":     "SELECT *, TO_TIMESTAMP(event_ts, 'yyyy-MM-dd HH:mm:ss') AS event_ts FROM dedup_orders"
    }
  ]
}
```

## **8.6 Agent Guardrails**

When `approval_mode` is `auto` or `aggressive`, the LLM applies patches without human review. Guardrails are **deterministically enforced at patch-apply time** — the code rejects violations regardless of what the LLM generated, not dependent on LLM compliance:

```yaml
agent:
  approval_mode: auto
  guardrails:
    allowed_paths:
      - "s3://company-data/*"
      - "gs://prod-bucket/*"
    forbidden_ops:
      - insert_module
      - remove_module
```

| Field | Type | Description |
| :- | :- | :- |
| `guardrails.allowed_paths` | list[str] | fnmatch patterns. Any `path`/`output_path` config value in a `set_module_config_key` op must match at least one pattern. Empty list = unrestricted. |
| `guardrails.forbidden_ops` | list[str] | PatchSpec operation names that are always blocked. Empty list = all ops permitted. |
| `guardrails.heal_on_errors` | list[str] | **Pre-trigger guard.** LLM only fires when the failure's `error_type` matches. Empty list = no restriction (LLM fires on any error). |
| `guardrails.never_heal_errors` | list[str] | **Pre-trigger guard.** LLM never fires when the failure's `error_type` matches. Takes priority over `heal_on_errors`. |

`heal_on_errors` / `never_heal_errors` match against two sources:
- **Assert rule label** — the `error_type:` field on an Assert rule that fired (e.g. `SLABreach`, `DataQualityViolation`).
- **Infrastructure exception class** — the Python exception class name extracted from the stack trace last line (e.g. `SparkException`, `AnalysisException`).

`aqueduct doctor` warns if a guardrail entry does not match any `error_type` declared in the blueprint's Assert rules (potential typo).

If a patch violates `allowed_paths` or `forbidden_ops`, `apply_patch` raises `PatchError` — the Blueprint is never written. A clear error message is emitted. This is enforced before any Blueprint mutation occurs.

**Recommended guardrail policy for production:**

```yaml
agent:
  approval_mode: auto
  guardrails:
    allowed_paths: ["s3://your-company-bucket/*"]
    forbidden_ops:
      - insert_module
      - remove_module  # structural changes require human review
```

## **8.7 Patch Lifecycle**

### Step-by-step flow

1. Module fails. Executor exhausts its retry policy (if configured). Failure bubbles to `cli.py`.
2. If `approval_mode: disabled` — LLM never fires. Run ends with `status=error`. **All other steps skipped.**
3. If unreviewed patches exist in `patches/pending/` and `on_pending_patches: block` — LLM skipped, run ends.
4. **Assembles FailureContext:** compiled module config with resolved values, **provenance map** (shows which context key or literal each config value came from), error message, truncated stack trace, doctor pre-flight hints.
5. **Calls LLM** with FailureContext + PatchSpec JSON schema + optional `prompt_context` injections (engine-level, then blueprint-level appended after).
6. **Validates response.** If LLM returns invalid JSON or schema mismatch, Aqueduct sends a reprompt showing the model its exact previous output alongside field-level error annotations (e.g. "line 8, column 3: missing comma — your output near the error: ..."). This is a real multi-turn conversation. Repeats up to `llm_max_reprompts` times (default 3, overridable per-blueprint). On network timeout the attempt fails immediately without retry. Common field name aliases (`ops`→`operations`, `description`→`rationale`, `type`→`op` in operations) are silently normalized before validation, so minor LLM naming deviations do not consume reprompt budget.
7. **Confidence check.** If `confidence < 0.7`, the patch is forced to `human` review regardless of `approval_mode`. Low-confidence patches are never auto-applied.
8. **Guardrails check.** If the patch touches a path outside `allowed_paths` or uses a `forbidden_ops` operation, it is staged in `patches/pending/` for human review regardless of `approval_mode`.
9. **Approval mode routing** (after confidence + guardrail checks pass):

### Approval modes

| Mode | What happens | Blueprint file changed? | Safe for prod? |
| :- | :- | :- | :- |
| `disabled` | LLM never fires | No | N/A |
| `human` | Patch staged to `patches/pending/`. Engineer reviews via `aqueduct patch apply` or `aqueduct patch reject`. Run ends. | Only after human apply | Yes |
| `auto` | **Fully automatic, single-shot.** Patch applied in-memory → pipeline re-runs. Blueprint written to disk only if re-run succeeds. If re-run fails, `on_heal_failure` policy applies. One patch attempt per run. | Only on successful re-run | Yes |
| `aggressive` | **Fully automatic, looping.** Same as `auto` but loops up to `aggressive_max_patches` attempts. Each patch is tested in-memory; Blueprint only written when re-run succeeds. If a patch fails, `on_heal_failure` policy applies and the loop continues. Requires `danger.allow_aggressive_patching: true`. | Only on successful re-run | Dev/trusted |
| `ci` | Patch staged to `patches/pending/`. Full PatchSpec JSON + metadata POSTed to `agent.ci_webhook_url` (or `webhooks.on_ci_patch`). External CI creates branch and PR. Pipeline not re-run. | Only after external merge | Yes |

> **`auto` vs `aggressive`:** Both are safe — Blueprint is only written when the in-memory re-run confirms the fix works. `auto` makes one attempt per run. `aggressive` keeps trying up to `aggressive_max_patches` times, generating a new patch for each remaining failure. Use `auto` in production for a single healing attempt per failure. Use `aggressive` in dev or trusted environments for hands-free multi-step repair.

> **`on_heal_failure`** controls what happens when a patch is generated but fails to fix the pipeline: `stage` (default) — the failed patch is written to `patches/pending/` for human inspection. `discard` — silently thrown away. `abort` — loop stops immediately. Applies in `auto` and `aggressive` modes.

10. Applied patch moves from `patches/pending/<ts>_<slug>.json` to `patches/applied/<ts>_<slug>.json` with `applied_at`, `run_id`, and `rationale` preserved. `patches/pending/` and `patches/rejected/` are gitignored; `patches/applied/` is committed alongside the Blueprint.
11. If `aggressive_max_patches` is reached without a successful run, pipeline aborts.

### Per-blueprint LLM overrides

All connection fields in the Blueprint `agent:` block override the engine defaults from `aqueduct.yml`:

| Blueprint field | Engine default field | Description |
| :- | :- | :- |
| `provider` | `agent.provider` | `"anthropic"` or `"openai_compat"` |
| `model` | `agent.model` | Model name string |
| `base_url` | `agent.base_url` | Endpoint URL |
| `ollama_options` | `agent.ollama_options` | Ollama-specific options dict |
| `llm_timeout` | `agent.llm_timeout` | HTTP timeout in seconds (default 120) |
| `llm_max_reprompts` | `agent.llm_max_reprompts` | Reprompt retries on invalid PatchSpec (default 3) |
| `confidence_threshold` | — (blueprint-only) | Minimum confidence to auto-apply patch (default 0.7; below → human review) |
| `on_heal_failure` | — (blueprint-only) | `stage` \| `discard` \| `abort` — what to do when patch fails to fix pipeline (default `stage`) |
| `prompt_context` | `agent.prompt_context` | Extra text appended to LLM system prompt (blueprint appended after engine) |

Blueprint values win on conflict. `null` (unset) means inherit from engine.

**`healing_outcomes` table:** Every healing attempt — whether the patch was applied, whether the re-run succeeded — is recorded in `obs.db`'s `healing_outcomes` table:

| Column | Description |
|---|---|
| `run_id` | The original failed run |
| `failed_module` | Module that triggered healing |
| `failure_category` | LLM-assigned category string |
| `model` | LLM model used |
| `patch_id` | PatchSpec identifier |
| `confidence` | LLM confidence score |
| `patch_applied` | Whether the patch was written to Blueprint |
| `run_success_after_patch` | Whether re-run succeeded |
| `applied_at` | ISO-8601 timestamp |

Query with: `SELECT * FROM healing_outcomes WHERE blueprint_id = '...' ORDER BY applied_at DESC;`

**Patch file naming:** `{YYYYMMDDTHHmmss}_{slug}.json` — e.g. `20260502T143022_fix-green-path.json`. Timestamp prefix enables chronological sort without reading file contents.

**LLM template expression preservation:** The LLM receives both the compiled module config (with resolved values) and the raw Blueprint YAML source. The system prompt instructs it to write patches using template expressions from the raw YAML (e.g. `${ctx.paths.input}`) rather than hardcoded resolved values. Patches are applied to the raw Blueprint, not the compiled Manifest.

**Custom prompt context:** `agent.prompt_context` in `aqueduct.yml` (engine-wide) and `agent.prompt_context` in the Blueprint `agent:` block (per-blueprint) append domain-specific instructions to the LLM system prompt. Blueprint-level context appends after engine-level. Use for cluster constraints, naming conventions, schema hints, or forbidden patterns.

## **8.8 Resume-From Semantics** *(Deferred — Advanced Feature)*

> **V1 behaviour:** When a patch is applied, Aqueduct re-runs the entire pipeline from the beginning. This is less efficient than partial resume but is 100% reliable and trivial to implement. Partial resume is deferred to a post-MVP release.

**Why deferred:** Partial resume requires the Executor to cache intermediate DataFrames across a JVM session boundary, maintain a mapping of which Modules completed, and handle invalidation when a patch modifies an upstream Module. The correctness surface area is large. The V1 trade-off is to accept the re-run cost in exchange for implementation simplicity.

**Middle-ground path (optional, not required for MVP):** If the Blueprint explicitly uses `df.checkpoint()` or sets `spark.sql.streaming.checkpointLocation` at a Module boundary, the Planner may restart from that checkpoint location after a patch. This is opt-in and driven entirely by the Blueprint author — Aqueduct does not insert checkpoints automatically.

**Future full implementation (post-MVP):**

- Any Module whose output was consumed by a completed Egress is not re-executed. Its output is considered finalised.
- Any Module whose output was not yet consumed is re-executed from the first un-cached ancestor.
- The Surveyor records which Modules completed successfully in the RunRecord before failure, enabling precise resume targeting.
- If the patch modifies a Module that ran successfully before the failure, Aqueduct forces re-execution of that Module and all its descendants.

## **8.9 Debugging a Failed Pipeline**

Standard debugging workflow after a pipeline failure:

**Step 1 — Identify the failure**
```bash
aqueduct runs blueprints/pipeline.yml --failed --last 5
```
Shows recent failed runs with `run_id`, `failed_module`, and `started_at`. Pick the `run_id`.

**Step 2 — Check sources and config**
```bash
aqueduct doctor --blueprint blueprints/pipeline.yml
```
Checks Ingress/Egress paths exist, formats match file extensions, LLM is reachable, Spark is healthy. Warnings here often explain the failure.

**Step 3 — Inspect the failure context**

The failure context is stored in `obs.db` (table: `failure_contexts`). The full context — including provenance JSON, stack trace, and probe signals — is what the LLM receives:
```bash
# Query directly
duckdb .aqueduct/obs/<blueprint_id>/obs.db \
  "SELECT error_message, failed_module FROM failure_contexts WHERE run_id='<run_id>'"
```

**Step 4 — Check the module output trail**
```bash
aqueduct report <run_id>
```
Shows which modules succeeded, which failed, and byte/row counts from SparkListener.

**Step 5 — Check lineage for column-level impact**
```bash
aqueduct lineage blueprints/pipeline.yml --from <failed_module>
```
Shows which columns flow into the failed module and from which sources.

**Step 6 — Manually trigger healing**
```bash
aqueduct heal blueprints/pipeline.yml
```
Generates a patch for the most recent failed run. Useful when `approval_mode: disabled` but you want to try the LLM on demand. The patch is always staged to `patches/pending/` — never auto-applied by `heal`.

**Step 6b — Inspect the prompt before calling the LLM**
```bash
# Human-readable (system block then user block)
aqueduct heal <run_id> --print-prompt

# Machine-readable JSON {"system": "...", "user": "..."}
aqueduct heal <run_id> --print-prompt --print-prompt-format json

# Works with scenarios too
aqueduct heal --scenario path/to/scenario.aqscenario.yml --print-prompt
```
Prints the exact system and user prompt that would be sent, then exits without calling the model. Does not require `agent.model` to be configured. Useful for debugging prompt construction, estimating token cost, or comparing prompt changes across scenarios.

**Step 7 — Inspect the patch**
```bash
aqueduct patch list --blueprint blueprints/pipeline.yml --status pending
cat patches/pending/<patch_file>.json
```
Review `rationale`, `confidence`, `category`, `root_cause`, and `operations`.

**Step 8 — Apply and re-run**
```bash
aqueduct patch apply patches/pending/<file> --blueprint blueprints/pipeline.yml
aqueduct run blueprints/pipeline.yml
```

**Step 9 — Check healing analytics**
```bash
duckdb .aqueduct/obs/<blueprint_id>/obs.db \
  "SELECT patch_id, confidence, failure_category, patch_applied, run_success_after_patch FROM healing_outcomes ORDER BY applied_at DESC LIMIT 10"
```

**Common failure patterns and fixes:**

| Error | Likely cause | Patch op |
|---|---|---|
| `AnalysisException: Column 'X' not found` | Schema drift — upstream renamed column | `set_module_config_key` on `query` |
| `IngressError: source not found` | Wrong path, missing file | `replace_context_value` on path key |
| `IngressError: schema_hint mismatch` | Upstream schema changed | `set_module_config_key` on `schema_hint` or change `mode: additive` |
| `AssertError: min_rows` | Empty dataset — data quality issue | Check source data; may not be a Blueprint bug |
| `OutOfMemoryError` | Spark executor memory too low | `set_module_config_key` on `spark_config.spark.executor.memory` |
| `UDFError: cannot import module` | Module not on `sys.path` / cloudpickle incompatibility | Check `udfs/` layout; run `pip install cloudpickle` on Python 3.13+ |


## **8.10 Scenario-Based LLM Benchmark**

Scenarios are `.aqscenario.yml` files that define a synthetic failure + expected LLM response. They test the self-healing agent without running a real Spark pipeline, enabling prompt regression testing and cross-model comparisons.

**Scenario format:**
```yaml
aqueduct_scenario: "1.0"
id: schema_drift_column_rename
description: "Column renamed from event_ts to event_time upstream"
blueprint: pipelines/orders.yml    # compiled to get manifest_json (no Spark)
inject_failure:
  module: cast_and_clean           # failed_module
  error_message: "AnalysisException: Column 'event_ts' does not exist"
  stack_trace: |                   # optional
    ...
expected_patch:
  ops:                             # ALL must match at least one generated op
    - op: set_module_config_key
      module_id: cast_and_clean
      key: query
      value_contains: "event_time" # substring match on the 'value' field
  forbidden_ops:                   # NONE may appear in the generated patch
    - replace_module_config
assertions:
  - patch_is_valid: true           # PatchSpec parses without schema error
  - patch_applies: true            # patch can be applied to the blueprint
```

**Running scenarios:**
```bash
# Single scenario test (PASS/FAIL with assertion details)
aqueduct heal --scenario tests/scenario_format_mismatch.aqscenario.yml

# Compare models across all scenarios in a directory
aqueduct benchmark --scenarios tests/ --model claude-opus-4-7 --model llama3

# JSON output for CI integration
aqueduct benchmark --scenarios tests/ --model claude-opus-4-7 --output json
```

**Benchmark output:**
```
Scenario                          | claude-opus-4-7  | llama3
-----------------------------------+------------------+----------
schema_drift_column_rename        |  PASS 0.94 1.2s  | FAIL 2.1s
format_mismatch_parquet_as_csv    |  PASS 0.99 0.8s  | PASS 0.88 1.5s
-----------------------------------+------------------+----------
Parse rate                        |      100%        |   92%
Apply rate                        |       91%        |   85%
Pass rate                         |       95%        |   80%
Avg confidence                    |      0.91        |  0.82
```

**Prompt versioning:** `PROMPT_VERSION` constant in `surveyor/llm.py` is stored in patch `_aq_meta`. Bump it manually when the system prompt changes. This correlates benchmark scores to prompt versions over time.


# **9. Type System**
## **9.1 Principle: Aqueduct Owns No Types**
All column types throughout Aqueduct — in schema\_hint declarations, UDF return\_type fields, column assertions, and the Flow Report — use Spark DDL type strings verbatim. Aqueduct does not define its own type system and does not wrap or alias Spark types. This guarantees complete compatibility and eliminates any translation layer between Aqueduct's type references and what Spark executes.

## **9.2 Supported Spark DDL Types**
Any valid Spark DDL type string is accepted wherever Aqueduct expects a type. Examples:

```text
# Scalar types
STRING, BOOLEAN, BYTE, SHORT, INT, INTEGER, LONG, BIGINT
FLOAT, DOUBLE, DECIMAL(precision, scale)
DATE, TIMESTAMP, TIMESTAMP_NTZ, BINARY

# Complex types
ARRAY<STRING>
MAP<STRING, DECIMAL(18,2)>
STRUCT<name: STRING, age: INT, address: STRUCT<city: STRING, zip: STRING>>

# Used in schema_hint and UDF return_type
schema_hint:
  order_id:   STRING
  amount:     DECIMAL(18,2)
  tags:       ARRAY<STRING>
  metadata:   MAP<STRING, STRING>
  location:   STRUCT<lat: DOUBLE, lon: DOUBLE>
```

## **9.3 Semantic Constraints (Not Types)**
For domain-specific validation (email format, URL format, phone number format, enumerated values), Aqueduct uses constraint annotations on columns within Probe modules. The underlying Spark type remains STRING or the appropriate primitive. The constraint is a validation rule, not a type.

```text
-- id: validate_contacts
type: Probe
attach_to: clean_contacts
config:
  column_constraints:
    email:
      - format: email             # built-in regex rule
      - not_null
    phone:
      - format: e164_phone        # built-in E.164 format rule
      - udf: clean_phone          # auto-applies UDF and checks result
    status:
      - enum: [active, inactive, pending]
    website:
      - format: url
    age:
      - range: { min: 0, max: 150 }
```

Constraint violations are recorded in the Flow Report as Degraded or Error status depending on violation rate vs alert thresholds. They do not cause pipeline failure unless a Regulator is wired to the Probe's output signal.


# **10. Deployment & Spark Integration**
## **10.1 Engine Configuration File**
Aqueduct reads a project-level aqueduct.yml configuration file from the working directory (or path specified by --config flag). This file sets deployment target, store backends, agent config, and engine defaults. It is separate from Blueprints.

```yaml
aqueduct_config: "1.0"
deployment:
  target: local                    # local | standalone | yarn | kubernetes | databricks
  env: local                       # local | cluster | cloud  — doctor warns on local paths in cluster/cloud
  master_url: "local[*]"           # overridden per target type
stores:
  # Root store directory. All runtime DB files are created inside it:
  #   obs.db      — run records, failure contexts, probe signals, module metrics, signal overrides
  #   lineage.db  — column-level lineage
  #   snapshots/  — schema_snapshot JSON files (one file per probe per run)
  observability:
    backend: duckdb                # duckdb (default) | s3 | gcs | adls
    path: ".aqueduct"              # root dir; obs.db created here
  lineage:
    backend: duckdb
    path: ".aqueduct"              # must match observability.path; lineage.db created here
  depot:
    backend: duckdb
    path: ".aqueduct/depot.db"     # single DuckDB file for KV state
agent:
  provider: "anthropic"            # "anthropic" | "openai_compat"
  model: "claude-sonnet-4-6"
  llm_timeout: 120.0               # HTTP timeout per LLM call; increase for slow local models
  llm_max_reprompts: 3             # max retries when LLM returns invalid PatchSpec JSON
  prompt_context: |                # optional: appended to LLM system prompt for all blueprints
    This cluster runs Databricks. All paths use s3a:// or dbfs://.
  aggressive_max_patches: 5
probes:
  max_sample_rows: 100
  default_sample_fraction: 0.01
danger:
  allow_full_probe_actions: false  # if true, Probes may run full Spark actions (adds latency)
  allow_aggressive_patching: false # if true, approval_mode: aggressive is allowed
secrets:
  provider: env                    # env | aws | gcp | azure | vault
                                   # Controls @aq.secret() resolution inside Blueprints.
                                   # Only "env" (os.environ) is fully implemented today.

# Simple form — plain URL, sends full context JSON via POST
webhooks:
  on_failure: "https://hooks.slack.com/services/T.../B.../"
  on_success: "https://hooks.example.com/pipeline-complete"

# Full form — custom method, headers, templated payload
webhooks:
  on_failure:
    url: "${SLACK_WEBHOOK_URL}"
    method: POST                   # POST | PUT | PATCH
    headers:
      Authorization: "Bearer ${API_TOKEN}"   # ${ENV_VAR} reads os.environ
    payload:                       # null = send full FailureContext JSON
      text: "Pipeline *${blueprint_id}* failed on `${failed_module}`"
      run_id: "${run_id}"
    timeout: 10
  on_success:
    url: "${SLACK_WEBHOOK_URL}"
    method: POST
    payload:
      text: "Pipeline *${blueprint_id}* complete — ${module_count} modules  run_id=${run_id}"
```

**`.env` file auto-loading**

`aqueduct run` auto-discovers a `.env` file in the **project root** (the directory containing `aqueduct.yml`) and loads it into `os.environ` before any config parsing. This means `${VAR}`, `${VAR:-default}`, and `@aq.secret('KEY')` in both `aqueduct.yml` and Blueprints all see the loaded values.

- Existing env vars are never overwritten (env always wins over `.env`).
- Drop a `.env` next to the blueprint for local dev — no manual `export` needed.
- `--env-file <path>` — explicit override; loads that file instead of auto-discovery.
- `--no-env-file` — disables auto-discovery entirely (for CI where vars are injected externally).

```bash
# .env next to blueprint — loaded automatically
AQUEDUCT_LLM_MODEL=qwen3:32b
AQUEDUCT_LLM_BASE_URL=http://localhost:11434/v1
S3_SECRET=mysecret

aqueduct run pipeline.yml                        # auto-loads .env
aqueduct run pipeline.yml --env-file prod/.env   # explicit path
aqueduct run pipeline.yml --no-env-file          # skip (CI)
```

**Environment variable substitution in `aqueduct.yml`**

`aqueduct.yml` is expanded before YAML parsing. Two syntaxes are supported:

| Syntax | Behavior |
| :- | :- |
| `${VAR}` | Replaced with `os.environ["VAR"]`; error if unset and no default |
| `${VAR:-default}` or `${VAR:default}` | Replaced with env var if set, otherwise `default` (dash optional) |
| `@aq.secret('KEY')` | Resolved via `os.environ["KEY"]` (always — see note below) |

> **Note on `@aq.secret()` in `aqueduct.yml`:** The secrets provider (e.g. Vault, AWS SSM) is initialized *after* `aqueduct.yml` is loaded. So `@aq.secret()` inside `aqueduct.yml` always resolves via `os.environ`, ignoring `secrets.provider`. Use `@aq.secret()` in Blueprints (where the provider is live) for Vault/AWS/GCP keys. In `aqueduct.yml`, use `${VAR:-default}` for optional fields and plain env vars for credentials.

```yaml
# Example — LLM agent config with env-var fallback and secret lookup:
agent:
  provider: ${AQUEDUCT_LLM_PROVIDER:-openai_compat}
  model: "@aq.secret('AQUEDUCT_LLM_MODEL')"        # reads os.environ["AQUEDUCT_LLM_MODEL"]
  base_url: "@aq.secret('AQUEDUCT_LLM_BASE_URL')"  # reads os.environ["AQUEDUCT_LLM_BASE_URL"]
```

**Three webhook scopes:**

| Scope | Config location | Fires when |
| :- | :- | :- |
| Pipeline failure | `aqueduct.yml webhooks.on_failure` | Run ends with `status=error` |
| Pipeline success | `aqueduct.yml webhooks.on_success` | Run ends with `status=success` |
| Per-module failure | Blueprint `on_failure_webhook` on any module | That module's retry is exhausted (fires regardless of `on_exhaustion` — even `alert_only`) |
| Per-Assert-rule | Blueprint Assert rule `on_fail: {action: webhook, url: ...}` | That specific rule fails |
| Patch staged | `aqueduct.yml webhooks.on_patch_pending` | A patch is written to `patches/pending/` (fires in `human` and `ci` modes) |
| CI patch | `aqueduct.yml webhooks.on_ci_patch` | `approval_mode: ci` — fires with full patch JSON for external PR creation |

All webhooks are best-effort (daemon thread), non-blocking, and do not affect pipeline outcome.

**`on_failure` / `on_success` template variables:**

| Variable | Value |
| :- | :- |
| `${run_id}` | UUID of the pipeline run |
| `${blueprint_id}` | Pipeline identifier string |
| `${blueprint_name}` | Pipeline display name |
| `${failed_module}` | Module ID of first failure (`on_failure` only) |
| `${error_message}` | Human-readable error string (`on_failure` only) |
| `${error_type}` | Exception class name (`on_failure` only) |
| `${started_at}` | ISO-8601 run start timestamp (`on_failure` only) |
| `${attempt}` | Number of failed modules (`on_failure` only) |
| `${module_count}` | Number of modules executed (`on_success` only) |
| `${ANY_ENV_VAR}` | Falls back to `os.environ` — useful for auth tokens in headers |

**`on_failure_webhook` (per-module) template variables:**

| Variable | Value |
| :- | :- |
| `${run_id}` | UUID of the pipeline run |
| `${blueprint_id}` | Pipeline identifier string |
| `${module_id}` | ID of the module that exhausted retries |
| `${error_message}` | Error description |
| `${error_type}` | Exception class name |

If `payload` is omitted, the full context JSON is sent unchanged.

**Per-module webhook YAML:**
```yaml
modules:
  - id: critical_ingress
    type: Ingress
    config:
      format: parquet
      path: "s3a://bucket/critical/"
    on_failure:
      max_attempts: 3
      on_exhaustion: alert_only     # pipeline continues
    on_failure_webhook: "https://hooks.slack.com/services/T.../B.../"
    # or full form:
    # on_failure_webhook:
    #   url: "${SLACK_WEBHOOK_URL}"
    #   payload:
    #     text: "Module `${module_id}` failed — ${error_message}"
```

## **10.2 Deployment Targets**

|**Target**|**Description**|
| :- | :- |
|**local**|spark://local[\*]. For development and CI. No cluster required. SparkSession created with local master. Default when AQUEDUCT\_ENV=dev or no config is present.|
|**standalone**|Connects to a running Spark standalone cluster. Requires master\_url: "spark://host:7077". SparkSession uses spark.master = master\_url.|
|**yarn**|Submits via spark-submit to a YARN resource manager. Requires hadoop\_conf\_dir set in deployment config. Supports client and cluster deploy modes.|
|**kubernetes**|Submits driver pod via spark-submit --master k8s://. Requires k8s\_master\_url and spark.kubernetes.container.image in spark\_config. Supports executor pod template overrides.|
|**databricks**|Uses Databricks Connect (interactive cluster reuse) or Databricks Jobs API (job cluster provisioning). Requires databricks\_host and databricks\_token in deployment config or via @aq.secret().|
|**emr**|AWS EMR via YARN target with EMR-specific credential handling. Requires emr\_cluster\_id or uses YARN submission to EMR master.|
|**dataproc**|GCP Dataproc via YARN target with GCP credential handling. Requires dataproc\_cluster and gcp\_project.|

## **10.3 SparkSession Lifecycle**
- The Executor creates or acquires one SparkSession per pipeline run.
- Multiple JobSpecs within a run share the session — this preserves cached DataFrames and avoids session startup overhead between jobs.
- Session configuration from the Blueprint spark\_config block is merged with engine defaults. Blueprint config takes precedence.
- On self-healing patch and resume: the SparkSession is preserved if the failure was application-level (bad SQL, schema error, wrong path). It is recycled if the failure was JVM/network-level (OutOfMemoryError, Py4JNetworkError).
- On run completion or abort, the Executor calls session.stop() and records the session duration in the RunRecord.

## **10.4 Performance Commitments**
These are explicit guarantees that Aqueduct makes regarding its performance overhead:

- No additional Spark actions beyond those defined by Egress modules in the Blueprint, unless a Probe explicitly uses sample-based signals (documented in Section 6.2).
- Probe signals sourced from SparkListener are zero-cost and run on the driver thread without touching the Spark execution thread.
- Context Registry Tier 0 resolution is fully static. Spark receives concrete values, never interpolation tokens.
- Arcade expansion is compile-time. Spark sees a flat execution plan with no nesting overhead.
- Regulator modules with no wired signal input are compiled away entirely — they add zero overhead.
- The Parser → Compiler → Planner pipeline is designed to complete in under 500ms for Blueprints containing up to 500 Modules on standard driver hardware (4 cores, 8GB RAM).

**Job Fusion:** Channels, Junctions, and Funnels produce lazy DataFrame references — no Spark action is triggered by these modules themselves. Aqueduct does not submit a separate Spark job per Module. The entire lazy computation graph is fused by Spark's Catalyst optimizer and materialised only when an Egress (or a sample-based Probe) triggers an action. The number of Spark jobs is bounded by the number of Egress modules in the Blueprint, not by the number of Modules.

## **10.5 Depot Concurrency Limitations**

The Depot KV store uses DuckDB in embedded mode. DuckDB's embedded mode supports multiple concurrent readers but only one writer at a time per database file. **Two `aqueduct run` processes writing to the same `depot.db` file simultaneously may produce write errors or data corruption.**

**Safe patterns:**
- Sequential pipeline runs (even different pipelines) sharing the same depot file: fully safe.
- Parallel runs from different machines pointing to a shared depot file over a network filesystem: not supported — use a centralized backend (future: `depot.backend: postgres`).
- Parallel runs on the same machine: use a separate depot file per pipeline (`depot.path: .aqueduct/<blueprint_id>.duckdb`), or serialize invocations through an orchestrator.

**Scheduler integration pattern:** When using Airflow, Prefect, or similar tools, pass `--execution-date {{ ds }}` to make `@aq.date.*` functions idempotent. Use the depot watermark pattern to track last-processed state. Query `run_records` in the DuckDB observability store to detect and backfill missed runs.


# **10.6 `aqueduct test` — Isolated Module Testing**

`aqueduct test <test_file.yml>` runs Channel, Junction, Funnel, and Assert modules against inline data with no external I/O. Ingress and Egress are never executed. The same SparkSession configuration from `aqueduct.yml` is used (`make_spark_session()`), defaulting `quiet=True`.

## **Test file format**

```yaml
aqueduct_test: "1.0"
blueprint: pipeline.yml         # relative to test file

tests:
  - id: test_filter_nulls
    description: "Null amounts removed"
    module: clean_orders         # module ID in blueprint (Channel/Junction/Funnel/Assert)

    inputs:
      raw_orders:                # upstream module ID
        schema:
          order_id: long
          amount: double
          order_date: string
        rows:
          - [1, 10.0, "2026-01-01"]
          - [2, null, "2026-01-01"]

    assertions:
      - type: row_count
        expected: 1
      - type: contains
        rows:
          - {order_id: 1, amount: 10.0}
      - type: sql
        expr: "SELECT count(*) = 1 FROM __output__"
```

## **Assertion types**

| Type | Description |
|---|---|
| `row_count` | `expected: N` — exact row count match. Triggers one `count()` action. |
| `contains` | `rows: [...]` — each dict must appear as a row in the output. One `filter().count()` per expected row. |
| `sql` | `expr: "SELECT ..."` — arbitrary SQL over `__output__` view; must return a single truthy value. |

## **Constraints**

- Only Channel, Junction, Funnel, Assert can be tested. Ingress/Egress → error with clear message.
- If a Channel SQL references an undeclared table, Spark raises "table not found" — correct behavior that surfaces bad dependencies.
- Junction output: first branch used by default. Specify `branch: <name>` to target a specific branch.
- Uses inline schema type mapping: `long`, `int`, `double`, `float`, `string`, `boolean`, `timestamp`, `date`.

## **CLI**

```bash
aqueduct test pipeline.aqtest.yml
aqueduct test pipeline.aqtest.yml --blueprint pipeline.yml  # override blueprint path
aqueduct test pipeline.aqtest.yml --quiet                   # suppress Spark progress
aqueduct test pipeline.aqtest.yml --config aqueduct.yml
```

Exit code 0 = all tests passed. Exit code 1 = any test failed or test file error.

# **11. CLI Reference**
## **11.1 Core Commands**

**Global flags (placed before subcommand):**

| Flag | Description |
| :- | :- |
| `-v`, `--verbose` | Enable DEBUG logging. Prints LLM raw responses, resolver steps, and Spark plan annotations. Use to diagnose healing failures or config resolution issues. |

|**Command**|**Description**|**Key Flags**|
| :- | :- | :- |
|`aqueduct init <name>`|Scaffold a new project in the current directory.|`--name`|
|`aqueduct validate <blueprint.yml>`|Validate Blueprint schema and logic.|(none)|
|`aqueduct compile <blueprint.yml>`|Compile to a resolved Manifest JSON.|`-p/--profile`, `--ctx`, `--execution-date`, `-o/--output`|
|`aqueduct run <blueprint.yml>`|Execute a Blueprint on Spark.|(see Run Flags below)|
|`aqueduct test <test_file.yml>`|Run isolated module tests.|`--blueprint`, `--config`, `--quiet`|

## **11.2 Observability Commands**

|**Command**|**Description**|**Key Flags**|
| :- | :- | :- |
|`aqueduct runs [blueprint]`|List recent run history from `obs.db`.|`--blueprint`, `--failed`, `--last <N>`, `--config`|
|`aqueduct report <run_id>`|Print Flow Report for a run.|`--format <table|json|csv>`, `--config`, `--store-dir`|
|`aqueduct lineage <blueprint>`|Print column-level lineage graph.|`--from <table_id>`, `--column <col>`, `--format <table|json>`|
|`aqueduct log <blueprint.yml>`|Show git history with patch metadata.|`--format <table|json>`|

## **11.3 Self-Healing & Signal Commands**

|**Command**|**Description**|**Key Flags**|
| :- | :- | :- |
|`aqueduct heal <run_id>`|Manually trigger LLM healing for a failure.|`--module <module_id>`, `--scenario <path>`, `--config`, `--patches-dir`|
|`aqueduct heal --scenario <path>`|Run scenario-based healing (no Spark). Builds FailureContext from a `.aqscenario.yml` file, calls the LLM, validates against expected assertions.||
|`aqueduct benchmark`|Run scenario suite against one or more models and compare results.|`--scenarios <dir>`, `--model <model>` (repeatable), `--output <table\|json>`, `--config`|
|`aqueduct signal <signal_id>`|Set/clear persistent gate overrides.|`--value <true|false>`, `--error <msg>`, `--config`|

## **11.4 Patch Management Commands**

|**Command**|**Description**|**Key Flags**|
| :- | :- | :- |
|`aqueduct patch list`|List patches in the lifecycle.|`--blueprint`, `--status <pending|applied|rejected|all>`|
|`aqueduct patch apply <file>`|Apply a pending patch to a Blueprint.|`--blueprint` (required), `--patches-dir`|
|`aqueduct patch reject <ref>`|Reject a pending patch with a reason.|`--reason` (required), `--patches-dir`|
|`aqueduct patch commit`|Commit applied patches to git.|`--blueprint` (required), `--patches-dir`|
|`aqueduct patch discard`|Revert Blueprint to git HEAD.|`--blueprint` (required), `--patches-dir`|
|`aqueduct rollback <blueprint>`|Restore blueprint file(s) to pre-patch state via a new forward commit.|`--to <patch_id>`|

## **11.5 System Integrity Commands**

|**Command**|**Description**|**Key Flags**|
| :- | :- | :- |
|`aqueduct check-config`|Validate `aqueduct.yml` schema.|`--config`|
|`aqueduct doctor`|Probe all resources (Spark, DBs, Cloud).|`--config`, `--skip-spark`, `--blueprint`|

## **11.6 Key Flags for `aqueduct run`**

Detailed reference for the most common command:

**Key flags for `aqueduct run`:**

| Flag | Description |
| :- | :- |
| `--config <path>` | Path to the engine configuration file. Default: `aqueduct.yml` in the current directory. |
| `--profile <name>` | Activate a specific block in `context_profiles:`. Overrides default `context:` values. |
| `--ctx <key=val>` | Override a Context Registry variable. Repeatable. Higher priority than profiles or YAML defaults. |
| `--execution-date <ISO>` | Pin logical date for `@aq.date.*` and `@aq.runtime.timestamp()`. Format: `YYYY-MM-DD`. Enables idempotent backfills. |
| `--from <module_id>` | Start execution at this module (inclusive). Upstream modules are skipped. Requires their output data to already exist. |
| `--to <module_id>` | Stop execution after this module (inclusive). Downstream modules are skipped. |
| `--resume <run_id>` | Resume a failed run from its last successful checkpoints. Skips all modules marked as `_aq_done` in the previous attempt. |
| `--run-id <uuid>` | Force a specific UUID for this run. Useful for tracking runs across external systems. |
| `--store-dir <path>` | Override the default `.aqueduct` directory for observability and state storage. |
| `--allow-aggressive` | Allow `approval_mode: aggressive` for this run, overriding `danger.allow_aggressive_patching: false` in config. Single-use — does not modify `aqueduct.yml`. |
| `--env-file <path>` | Load environment variables from a `.env` file before running. If omitted, Aqueduct auto-discovers `.env` in the blueprint's directory. Existing env vars are never overwritten. |
| `--no-env-file` | Disable `.env` auto-discovery. Use in CI where variables are injected externally and a local `.env` might cause conflicts. |
| `--parallel` | Execute independent DAG branches concurrently. Aqueduct identifies fully-independent connected components in the data-flow graph (Union-Find) and submits each to a separate Python thread. Only beneficial when the Blueprint contains multiple independent source trees (e.g. two separate Ingress→Egress chains with no shared edges). Junction fan-out is already parallel via Spark's lazy evaluation — `--parallel` adds nothing for single-tree blueprints. First component failure cancels remaining components. |

**Sub-DAG execution example:**

```bash
# Test only the transform step without re-running expensive Ingress
aqueduct run pipeline.yml --from clean_orders --to write_output

# Backfill for a missed date
aqueduct run pipeline.yml --execution-date 2026-04-28
```


# **12. Deferred Topics & Open Items**
The following areas are intentionally deferred from this version of the specification. They are not out of scope permanently — they are staged for future revision to avoid premature design decisions.

### **Streaming (Spark Structured Streaming)**
Architecturally compatible with Aqueduct's Module model — a streaming Ingress and streaming Egress bookend the same Channel chain. The Probe model requires adaptation since SparkListener signals differ for continuous streams (microbatch vs. continuous processing). Regulator gates require a re-evaluation model for streaming contexts. Deferred to specification version 1.1.

### **MLOps Integration**
A Channel module wrapping a model inference call (MLflow, SageMaker, Vertex AI endpoint) is architecturally straightforward. Feature store reads as Ingress modules are natural. The open question is whether Aqueduct should own training pipeline orchestration or defer to MLflow Pipelines / Vertex AI Pipelines. Recommendation: ML inference as a built-in Channel op type in v1.1. Training orchestration out of scope for v1.

### **Multi-pipeline Orchestration**
Aqueduct currently runs one pipeline per invocation. Cross-pipeline dependencies (pipeline A must complete before pipeline B starts) are handled externally via Depot watermarks and standard orchestrators (Airflow, Prefect, etc.) triggering aqueduct run commands. A native Aqueduct workflow layer (a Blueprint of Blueprints) is a potential v1.2 feature.

### **LLM Model Benchmarking**
Scenario-based LLM benchmarking is implemented via `.aqscenario.yml` files and the `aqueduct benchmark` command. See §11.3 for command reference. Scenarios define a simulated failure, expected patch ops, and pass/fail assertions — no Spark required. Run `aqueduct benchmark --scenarios <dir> --model A --model B` to compare models.

### **MCP (Model Context Protocol) Readiness**
Aqueduct's LLM loop is architected to be exposed as an **MCP Server**. This will allow any MCP-compatible agent (Claude Desktop, Cursor, etc.) to discover and invoke Aqueduct capabilities directly as tools — moving from prompt engineering to structured tool use for greater reliability and composability.

Candidate MCP tools:

| **Tool name** | **Description** |
| :- | :- |
| `patch_blueprint` | Accepts a run\_id and optional module scope. Assembles the FailureContext, invokes the LLM loop, and returns the applied PatchSpec. |
| `get_lineage` | Accepts a pipeline\_id, module\_id, and column name. Returns the upstream and downstream ColumnLineageGraph for that column. |
| `get_flow_report` | Returns the Flow Report for a given run\_id in structured JSON. |
| `run_pipeline` | Submits a Blueprint for execution and streams RunRecord status events. |

When Aqueduct operates as an MCP server, the `approval_mode` in the agent config applies to the tool caller — `auto` approves patches immediately, `human` holds them for the user to confirm in the MCP client UI.


# **13. Engine Scope & Boundaries**

This section explicitly defines what Aqueduct is and is not, to prevent scope creep and set clear expectations for integrators.

## **13.1 What Aqueduct Is**

- A **batch processing engine** for Apache Spark. Every pipeline run is finite — it starts, processes a bounded dataset, and completes.
- A **declarative control plane**. Engineers describe *what* the pipeline does (YAML Blueprint), not *how* Spark executes it.
- A **data preparation layer**. Its output is clean, validated, enriched DataFrames written to storage or databases.
- An **LLM-integrated operations tool**. Self-healing, patch lifecycle, and FailureContext are core, not optional add-ons.

## **13.2 What Aqueduct Is Not**

| **Out of scope** | **Rationale / Recommended alternative** |
| :- | :- |
| **Streaming (Spark Structured Streaming, Kafka)** | Streaming requires continuous process lifecycle management, microbatch Probe semantics, and Regulator re-evaluation models that are fundamentally different from batch. Deferred to spec v1.1. |
| **Native ML training pipelines** | Training orchestration belongs to MLflow Pipelines, Vertex AI Pipelines, or Kubeflow. Aqueduct prepares the data that feeds these systems. |
| **ML inference as a built-in Channel op** | Planned as a Channel op type (`op: infer`, wrapping MLflow/SageMaker endpoints) in v1.1. Not in v1. |
| **Flink execution engine** | The `executor/__init__.py` factory has a `flink` stub that raises `NotImplementedError`. Flink support is planned but not started. |
| **Multi-pipeline orchestration (native)** | Cross-pipeline deps use Depot watermarks + external orchestrators (Airflow, Prefect, Dagster). A native Aqueduct workflow layer is a v1.2 candidate. |
| **Visual graph editor / UI** | The Blueprint YAML is always the source of truth. A UI is a separate product layer — it reads and writes valid Blueprint YAML. |
| **Real-time alerting / monitoring daemon** | Aqueduct's Surveyor is a run-scoped supervisor, not a persistent monitoring service. Long-running monitoring integrates via webhook signals to PagerDuty, Slack, etc. |
| **Data catalogue / discovery** | Lineage data in `lineage.db` is queryable but Aqueduct does not provide a data catalogue UI. Integrate with DataHub, OpenMetadata, or Amundsen via export. |

## **13.3 Scheduling**

Aqueduct has no built-in scheduler. `aqueduct run` is a one-shot CLI command designed to be invoked by an orchestrator:

- **Simple cron**: Any OS-level cron, systemd timer, or cloud scheduler (AWS EventBridge, GCP Cloud Scheduler) invoking `aqueduct run blueprint.yml`.
- **Complex orchestration**: An Airflow `AqueductOperator` (wrapping `aqueduct run` as a `BashOperator` or SDK call) for dependency management, backfill, and SLA tracking.
- **On-demand**: Manual invocation from CI/CD pipelines or by the LLM agent via `aqueduct run` after patch application.


# **14. Production Deployment**

> This section describes what changes when moving from a local development setup to a production Spark cluster. It covers configuration, path conventions, state persistence, and the LLM self-healing lifecycle in distributed environments.

---

## **14.1 Deployment Environments**

Aqueduct supports three deployment environments, declared in `aqueduct.yml`:

```yaml
deployment_env: local        # default — relative paths, local Spark, local DuckDB
# deployment_env: cluster    # Spark standalone / YARN — shared FS, driver is persistent
# deployment_env: cloud      # K8s / EMR / Dataproc — ephemeral driver, object storage
```

The value affects:
- Path validation in `aqueduct doctor` (warns on local paths in `cluster`/`cloud` mode)
- Self-healing patch lifecycle guidance
- Observability state persistence guidance

---

## **14.2 Spark Cluster Configuration**

Aqueduct creates a `SparkSession` on the driver. Cluster connection is fully controlled via environment variables and config — no code changes required.

**Environment variables:**

| Variable | Purpose | Example |
|---|---|---|
| `AQ_SPARK_MASTER` | Spark master URL | `yarn`, `spark://host:7077`, `k8s://https://...` |
| `AQ_SPARK_EXECUTOR_CORES` | Cores per executor | `4` |
| `AQ_SPARK_EXECUTOR_MEMORY` | Memory per executor | `8g` |
| `AQ_SPARK_DRIVER_MEMORY` | Driver memory | `4g` |

**`aqueduct.yml` Spark config block:**
```yaml
spark:
  master: "${AQ_SPARK_MASTER}"
  config:
    spark.sql.shuffle.partitions: "200"
    spark.hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
    spark.hadoop.fs.s3a.aws.credentials.provider: "com.amazonaws.auth.InstanceProfileCredentialsProvider"
```

**Running on a cluster:**
```bash
# YARN client mode (driver on edge node)
AQ_SPARK_MASTER=yarn aqueduct run pipeline.yml

# Spark standalone
AQ_SPARK_MASTER=spark://master:7077 aqueduct run pipeline.yml

# Kubernetes — driver runs as a pod
AQ_SPARK_MASTER="k8s://https://cluster-endpoint" aqueduct run pipeline.yml
```

In K8s, mount a PersistentVolumeClaim at `.aqueduct/` and `patches/` so observability state and patch history survive pod restarts.

---

## **14.3 Path Conventions for Production**

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

In `cluster` or `cloud` deployment environments, `aqueduct doctor` warns when a Blueprint contains paths without a URI scheme.

---

## **14.4 Observability State Persistence**

`obs.db` (DuckDB) runs on the driver — always correct. Spark workers never access it.

| Deployment | Driver persistence | Action required |
|---|---|---|
| Local / dev | Permanent | None |
| YARN client mode | Permanent (edge node) | None |
| Spark standalone | Permanent (driver host) | None |
| Kubernetes | **Ephemeral** (pod) | Mount PVC at `.aqueduct/` |

**`store_dir` config:**
```yaml
# aqueduct.yml
store_dir: "/mnt/aqueduct-state"   # PVC mount in K8s, or shared NFS path
```

---

## **14.5 LLM Service Configuration**

In production, LLM inference runs as a remote HTTP service.

**OpenAI-compatible endpoint (vLLM, Bedrock proxy, Azure OpenAI, together.ai, etc.):**
```yaml
agent:
  provider: openai_compat
  base_url: "${LLM_BASE_URL}"
  model: "${LLM_MODEL}"
  llm_timeout: 60
  approval_mode: human
```

**Anthropic API:**
```yaml
agent:
  provider: anthropic
  model: claude-opus-4-7-20251001
  approval_mode: human
```

Inject API keys (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`) via Kubernetes Secrets or your secrets manager. Never commit keys to Blueprint YAML or `aqueduct.yml`.

---

## **14.6 Danger Settings**

Certain features are disabled by default because they have destructive or expensive side-effects. They are grouped under a `danger:` block in `aqueduct.yml`. All keys default to `false` (safe). If any key is set to `true`, Aqueduct prints a startup warning to stderr listing which danger settings are active.

```yaml
danger:
  allow_aggressive_patching: false  # if true, agent permanently modifies Blueprint without human review
  allow_full_probe_actions: false   # if true, Probes may run expensive Spark actions (null_rates, value_distribution, etc.)
```

**`allow_aggressive_patching`:** Gates `approval_mode: aggressive`. If this is `false` (default) and `approval_mode: aggressive` is set, Aqueduct raises a `ConfigError` at startup with an explanation. Can also be overridden per-run via CLI flag `--allow-aggressive` without touching config.

**`allow_full_probe_actions`:** Probes default to zero-cost observability (schema snapshots, SparkListener metrics). Opt-in signals (`null_rates`, `value_distribution`, `distinct_count`, `data_freshness`) require Spark sampling actions — `count()`, `agg()`, `collect()` on a sample. These are safe in dev but add latency in prod. This flag gates those signals globally. Even when `false`, Probes still emit schema snapshots and SparkListener row/byte counts.

**Never set `danger.*: true` in a production `aqueduct.yml` checked into git.** Use environment-specific config overrides or per-run CLI flags.

---

## **14.7 Patch Lifecycle in Production**

The patch lifecycle differs significantly between development and production.

### Development (local)
```
pipeline fails → LLM generates patch → patch written to patches/pending/
→ human reviews → aqueduct patch apply → Blueprint updated → re-run
```
`approval_mode: auto` or `aggressive` is acceptable locally — git provides rollback.

### Production (recommended)

**Rule: in production, use `approval_mode: human`. The LLM must never autonomously modify a Blueprint managed by CI/CD.**

Recommended flow:
```
pipeline fails → webhook fires (alert to Slack/PagerDuty)
→ LLM generates patch → staged to patches/pending/
→ operator reviews patch JSON
→ aqueduct patch apply on a git branch → PR opened
→ CI/CD validates (aqueduct validate) → human approves → merge → redeploy
```

**`approval_mode: ci`:**
When set, instead of writing to `patches/pending/`, Aqueduct fires a `POST` request to `agent.ci_webhook_url` with:
```json
{
  "run_id": "...",
  "blueprint_id": "...",
  "patch": { /* PatchSpec JSON */ },
  "patch_diff": "--- before\n+++ after\n...",
  "rationale": "LLM root cause + fix description",
  "failed_module": "...",
  "confidence": 0.87
}
```
The receiving CI system (GitHub Actions, GitLab CI, Jenkins, custom script) creates the branch and PR. Aqueduct does not couple to any git provider — a reference GitHub Actions handler ships in `examples/ci/`. This is the recommended production pattern.

**`on_patch_pending` webhook:** When a patch is staged to `patches/pending/` (`approval_mode: human`), Aqueduct fires `agent.webhooks.on_patch_pending` so teams receive a Slack/PagerDuty notification. Without this, `approval_mode: human` is impractical in team settings.

### `patches/rules.md` — project-specific LLM rules

`patches/rules.md` is a freeform Markdown file committed alongside Blueprints. Its content is appended verbatim after `agent.prompt_context` in the LLM system prompt on every self-healing call. Use it to encode project-specific repair rules the LLM should always follow:

```markdown
## Project rules
- All I/O paths must be `s3a://my-bucket/**` globs — never local paths.
- Never change `mode: overwrite` on any Egress — data loss risk.
- The context key `paths.yellow_path` must always end in `*.parquet`.
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

`aqueduct rollback` is a **development tool only**.

It restores only the blueprint file(s) touched by the target patch commit by checking out their pre-patch content from git history and creating a new forward commit. No history is rewritten; other files in the repository are never touched. Arcade blueprints that were part of the same patch commit are automatically included.

In production: rollback = revert the commit in git → CI/CD redeployment. Do not run `git` commands on production driver nodes.

---

## **14.8 Security Considerations**

| Concern | Mitigation |
|---|---|
| API keys in Blueprint YAML | Never. Use `${ENV_VAR}` refs. Inject via K8s Secrets / Vault. |
| LLM modifying paths beyond guardrails | Set `agent.guardrails.allowed_paths` to cloud URI patterns. |
| `aggressive` mode in production | Requires `danger.allow_aggressive_patching: true` — do not set in prod config. |
| `obs.db` exposure | Contains resolved config and error details. Restrict filesystem access. |
| Patch tampering | Phase 25 (planned): patch signature verification for `aggressive` mode. |
| `danger.*` settings in shared config | Never commit `danger.*: true` to a shared/prod `aqueduct.yml`. Use env-specific overrides or per-run CLI flags. |

---

## **14.9 Delta Lake Operational Notes**

When using `format: delta` Egress modules in production, Aqueduct handles reads and writes but does **not** run Delta maintenance operations. These must be scheduled externally:

| Operation | Why it matters | When to run |
|---|---|---|
| `OPTIMIZE` | Compacts small files written by incremental runs into larger files; dramatically improves read performance | After each incremental batch, or daily |
| `VACUUM` | Removes old file versions no longer reachable by the current snapshot; controls storage growth | Daily or weekly (default retention: 7 days) |
| `ZORDER BY (col)` | Co-locates related data within files for data-skipping on filter columns | Run with `OPTIMIZE` when query patterns are known |

Example (run via `spark.sql()` in an external maintenance job or notebook):

```sql
OPTIMIZE delta.`s3://my-bucket/output/orders` ZORDER BY (event_date, region);
VACUUM delta.`s3://my-bucket/output/orders` RETAIN 168 HOURS;
```

Without `OPTIMIZE`, incremental pipelines using `mode: append` or `mode: merge` will accumulate small files over time, degrading read performance significantly.

---

## **14.10 Production Readiness Checklist**

Before promoting a Blueprint to production:

- [ ] All I/O paths use `${ctx.*}` refs resolved from environment variables (no hardcoded local paths)
- [ ] `aqueduct doctor --blueprint pipeline.yml` passes with no errors in `cluster`/`cloud` deployment_env
- [ ] `agent.guardrails.allowed_paths` set to cloud URI patterns
- [ ] `agent.approval_mode: human` or `ci` (not `auto` or `aggressive`)
- [ ] No `danger.*: true` in production `aqueduct.yml`
- [ ] API keys injected via environment (`@aq.secret()` reads `os.environ` only — no external provider)
- [ ] `store_dir` points to a persistent path (PVC in K8s, persistent edge node in YARN)
- [ ] `patches/pending/` and `patches/rejected/` in `.gitignore`
- [ ] `patches/applied/` and `patches/rules.md` committed to git
- [ ] `agent.webhooks.on_patch_pending` configured if using `approval_mode: human` in a team setting
- [ ] If using `format: delta`: external OPTIMIZE / VACUUM jobs scheduled (see §14.9)

---

*AQUEDUCT — System Design & Implementation Reference — v1.0*
v1.0  —  Blueprint · Module · Ingress · Channel · Egress · Junction · Funnel · Probe · Regulator · Spillway · Arcade · Surveyor
