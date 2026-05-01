**AQUEDUCT**  —  System Design & Implementation Reference

**AQUEDUCT**

Intelligent Spark Pipeline Engine

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
|**P3 — Performance non-regression**|Aqueduct must not add Spark actions that were not in the original pipeline. Every observability feature has a defined cost model; zero-cost options are preferred.|
|**P4 — Static resolution first**|Any value that can be resolved at parse time must be. Runtime resolution is explicit, opt-in, and visually distinct in Blueprint syntax.|
|**P5 — Patch grammar over codegen**|The LLM agent operates within a structured Patch grammar, not free-form code generation. Every patch is schema-valid, auditable, and reversible.|
|**P6 — Passive-by-default gates**|Flow control constructs (Regulators, Spillways) do not exist in the execution path unless explicitly wired. Unwired gates compile away entirely.|
|**P7 — Pure Spark type system**|Aqueduct owns no type system. All types are Spark DDL strings. Semantic constraints are annotations, not types.|


# **2. Naming Glossary**
These names are canonical and used consistently throughout the codebase, documentation, logs, and LLM prompts. They must not be substituted for synonyms.

|**Term**|**Definition**|
| :- | :- |
|**Aqueduct**|The engine itself. The full system described in this document.|
|**Module**|The smallest indivisible unit of a pipeline. Every step in a Blueprint is a Module. Typed: Ingress, Channel, Egress, Junction, Funnel, Probe, Regulator, Arcade.|
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
|**Observability Store**|Append-only log of all runtime signals: Probe readings, stage metrics, errors. Queryable by run\_id and module\_id. Backend: raw Parquet/JSON files written to disk, queried on-demand via DuckDB.|
|**Lineage Store**|Stores structural column lineage graphs (computed at parse time) and runtime ColumnQualityReport entries per run. Backend: Parquet/JSON files on disk, queried via DuckDB.|
|**Depot (KV Store)**|Persistent key-value store for pipeline state across runs: watermarks, last-run metadata, cross-pipeline coordination values. Keyed by pipeline\_id + key name. Per-key read/write access flags. Backend: DuckDB embedded database.|

## **3.3 Component Interaction Flow**
The following sequence describes a complete pipeline run from Blueprint file to completion:

1. Engineer or agent writes Blueprint.yml and commits it to version control.
1. aqueduct run Blueprint.yml [--profile prod] [--ctx key=value] is invoked.
1. Parser reads and validates Blueprint.yml against the versioned JSON Schema. Fails fast on any unknown field or schema violation.
1. Context Registry is built: static values resolved, @aq.\* functions queued for Tier 1 resolution.
1. Compiler resolves all @aq.\* runtime functions (date functions, secret fetches, Depot reads). All ${ctx.\*} tokens are substituted. Arcades are expanded. Manifest JSON is written to runs/<run\_id>/manifest.json.
1. Planner partitions the flat Module list into ordered JobSpecs. Parallel branches are identified. Probe attachment points are annotated.
1. Executor acquires or creates a SparkSession for the target deployment. RunRecord is opened in the Observability Store.
1. Executor submits each JobSpec. SparkListener is attached, streaming task metrics to the Observability Store in real time.
1. Surveyor runs concurrently, consuming the Observability Store event stream. It evaluates health signals and Regulator states continuously.
1. On job completion, Executor finalises the RunRecord. Surveyor generates the Flow Report from Probe signals.
1. On any failure: Surveyor applies the retry policy. On exhaustion (or non-transient error), it packages the FailureContext and invokes the LLM agent loop.
1. Approved Patch is applied to the Blueprint. Manifest is recompiled. Execution resumes from the failed Module forward.


# **4. Blueprint Format**
## **4.1 File Format & Versioning**
Blueprints are YAML files. The format is versioned — the flux field at the top selects the JSON Schema version used for validation. Unknown fields at any level are hard errors, not warnings. This strictness is intentional: it guarantees Blueprints are always valid input for LLM patch generation.

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

agent:                                 # LLM self-healing config (see Section 8)
  approval_mode: auto                  # disabled | human | auto | aggressive
  model: claude-sonnet-4-20250514
  max_patches_per_run: 5
  # Guardrails (optional — recommended for auto/aggressive modes)
  allowed_paths: ["s3://company-data/*"]   # fnmatch patterns; empty = unrestricted
  forbidden_ops: ["insert_module"]         # PatchSpec op names blocked from auto-apply
```

## **4.3 Module Schema — Common Fields**
Every Module regardless of type shares these fields:

|**Field**|**Description**|
| :- | :- |
|**id**|Required. Unique string within the Blueprint. Auto-generated as slug + short hash if omitted. Used in edges, logs, patches, and file paths. Must be filesystem-safe (auto-slugified from label if not set).|
|**label**|Required. Human-readable display name. Completely unconstrained — spaces, unicode, punctuation all permitted. Shown in UI, logs, Flow Report, and LLM FailureContext. The id is derived from this if not explicitly set.|
|**type**|Required. One of: Ingress | Channel | Egress | Junction | Funnel | Probe | Regulator | Arcade.|
|**description**|Optional. Free-text explanation of what this Module does. Used in LLM context and UI tooltips.|
|**tags**|Optional list of strings. Used for filtering, grouping, and agent scoped search.|
|**config**|Type-specific configuration block. Defined per Module type in Section 4.4.|
|**on\_failure**|Optional. Overrides the pipeline-level retry\_policy for this specific Module.|
|**spillway**|Optional. Specifies the downstream Module ID to receive error-port output. If omitted and errors occur, they propagate to the Surveyor as a pipeline failure.|
|**depends\_on**|Optional explicit upstream dependency list. Normally inferred from edges but can be stated for clarity or to force ordering between non-connected Modules.|

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
|**format**|Spark data source format string. Standard formats: parquet, delta, csv, json, orc, avro, jdbc, kafka. Custom formats use the fully qualified DataSource class name.|
|**path**|Source path or URL. Context Registry references allowed. For JDBC: the full connection URL.|
|**schema\_hint**|Optional. Map of column\_name: SparkDDLType. Aqueduct enforces this at read time by calling .schema() and comparing. Mismatch raises SchemaIncompatibleException and triggers the Surveyor.|
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
|**op**|Operation type. Built-in ops: sql | deduplicate | filter | select | rename | cast | join | union | sort | repartition | cache. Custom ops reference a registered Python function by ID.|
|**query**|SQL string (op: sql only). Upstream Module IDs are available as temp views. ${ctx.\*} references are substituted before the query reaches Spark.|
|**udfs**|List of UDF IDs to register before executing this Channel. UDFs are defined in the udf\_registry block (see Section 5.4).|
|**key**|Column name or list. Used by deduplicate, join, and other key-based ops.|
|**order\_by**|Sort expression string. Used by deduplicate (to pick which row to keep) and sort ops.|
|**condition**|Filter expression string (op: filter). Standard Spark SQL boolean expression.|

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
|**broadcast\_side**|Optional: left or right. Adds a `BROADCAST` hint to the smaller side to avoid shuffle.|

> **Note:** `op: join` is sugar over `op: sql`. It is translated to `SELECT * FROM left JOIN right ON condition` before reaching Spark. For complex multi-table joins, multi-way expressions, or window functions, use `op: sql` directly.

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
```

|**Signal type**|**Spark cost**|**Implementation**|
| :- | :- | :- |
|**schema\_snapshot**|Zero — metadata only|`df.schema` is always available on a lazy DataFrame. Writes DuckDB row + JSON file.|
|**row\_count\_estimate**|Zero (spark\_listener) or 1 action on fraction (sample)|`spark_listener`: reads `recordsWritten` from SparkListener stage metrics. `sample`: `df.sample(fraction).count()`.|
|**null\_rates**|1 action on sample|`df.sample(fraction).agg(sum(isNull(c)) for each col)`. Never touches full DataFrame.|
|**sample\_rows**|1 bounded action|`df.take(n)` — short-circuits after N rows. Not considered a full scan.|
|**value\_distribution**|1 agg() + 1 approxQuantile per column on sample|`df.sample(fraction).agg(min, max, mean, stddev, count)`. Percentiles via `approxQuantile`. Numeric columns only by default.|
|**distinct\_count**|1 agg() on sample|`df.sample(fraction).agg(approx_count_distinct(c) for each col)`. Approximate; error < 5%.|
|**data\_freshness**|1 action (full scan by default)|`df.select(max(column)).collect()`. Set `allow_sample: true` to use a fraction instead (trades accuracy for speed in production).|
|**partition\_stats**|Zero — no Spark action|`df.rdd.getNumPartitions()`. Purely a driver-side call.|

|**Production guard:**  `row_count_estimate` (sample method), `null_rates`, `value_distribution`, and `distinct_count` are suppressed when `probes.block_full_actions_in_prod: true` is set in `aqueduct.yml`. `data_freshness` is also suppressed unless `allow_sample: true`. `schema_snapshot`, `sample_rows`, and `partition_stats` are never suppressed.|
| :- |

|**REGULATOR**|**Trigger gate — passive by default, blocks downstream on False or error signal**|
| :-: | :- |

```yaml
- id: quality_gate
  type: Regulator
  label: "Block downstream if dedup removed too many rows"
  config:
    on_block: skip                 # skip | abort | trigger_agent
    timeout_seconds: 0             # 0 = no timeout (default)
    on_timeout: proceed            # proceed | skip | abort
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

|**Implementation note:**  The Regulator is evaluated by the Surveyor, not by Spark. Signal values are read from the Observability Store after the upstream Probe completes. The Surveyor holds the downstream JobSpec in a pending state until the Regulator resolves. No Spark resources are held during this wait.|
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

|**Implementation:**  Spillway routing for SQL Channels uses Spark's built-in try\_cast(), try\_divide(), and try() functions to capture row-level failures without aborting the job. For custom transform functions, Aqueduct wraps the UDF in a try/except that populates the \_aq\_error\_\* columns on failure rows and routes them to the spillway DataFrame. This is a single-pass operation — no separate Spark job or action is triggered.|
| :- |

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

|**Expansion contract:**  Arcades are fully expanded at Manifest compile time. The Manifest always contains a flat Module list. Spark sees a single flat execution plan with no nesting. Module IDs within Arcades are namespaced (parent\_module\_id.child\_module\_id) to prevent collision.|
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
| `null_rate` | One shared `df.sample().agg()` for all null_rate rules | Asserts null fraction ≤ max on a sampled column. |
| `freshness` | Batched into one `df.agg()` | Asserts `MAX(column) >= NOW() - max_age_hours`. |
| `sql` | Batched into one `df.agg()` | Evaluates an aggregate SQL expression; result must be truthy. |
| `sql_row` | Lazy filter (no action) | Rows failing `expr` go to spillway; passing rows continue. |
| `custom` | Determined by callable | Python fn receives full DataFrame; returns `{passed, message, quarantine_df}`. |

**Performance model:** All aggregate rules (`min_rows`, `freshness`, `sql`) are collected into a single `df.agg()` call. All `null_rate` rules share a single `df.sample(fraction).agg()` call. Schema match is zero-action. Row-level rules (`sql_row`, `custom`) are lazy filters — no Spark action. Net: **at most 2 Spark actions** per Assert module, regardless of rule count.

**`on_fail` actions (per rule):**

| Action | Behaviour |
| :- | :- |
| `abort` | Raises `AssertError`; pipeline stops and enters Surveyor failure handling. |
| `warn` | Logs a warning; pipeline continues. |
| `webhook` | POSTs an alert to the configured URL; pipeline continues. Combined as `{action: webhook, url: ...}`. |
| `quarantine` | Row-level rules only — failing rows are collected into `quarantine_df` and routed to the module's spillway port. |
| `trigger_agent` | Raises `AssertError(trigger_agent=True)`; Surveyor invokes the LLM self-healing loop. |

**Implementation note:** Assert is executed by `aqueduct.executor.spark.assert_.execute_assert()`. It returns `(passing_df, quarantine_df | None)`. The executor wires `quarantine_df` into `frame_store["{module_id}.spillway"]` if a spillway edge exists; otherwise logs a warning and discards quarantine rows.


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

## **5.5 UDF Registry (Tier 2)**
UDFs are not Context values — they operate on DataFrame columns during Spark execution, not on scalar config values. They are registered in a udf\_registry block at the top level of the Blueprint or in a shared udf\_registry.yml file referenced by multiple Blueprints.

```yaml
udf_registry:
  - id: clean_phone
    label: "Normalise phone to E.164 format"
    lang: python
    path: udfs/clean_phone.py
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

UDFs are registered with the SparkSession before any Channel that references them executes. Spillway routing is automatically applied to UDF calls — if a UDF raises an exception on a row, the row is routed to the Channel's spillway port with the exception captured in \_aq\_error\_msg.

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
|row\_count\_estimate|SparkListener task metrics (recordsWritten)|Zero — taps internal event bus|Exact for completed stages|
|stage\_duration|SparkListener onStageCompleted event|Zero — taps internal event bus|Exact|
|shuffle\_size|SparkListener task metrics (shuffleWriteBytes)|Zero — taps internal event bus|Exact|
|null\_rates|sample(frac).agg(count(when(isNull)))|Low — one action on sample partition|Approximate (±sampling error)|
|value\_distribution|sample(frac) + approx\_count\_distinct()|Low — approximate Spark functions|Approximate|
|sample\_rows|DataFrame.take(N), N ≤ max\_sample\_rows|Low — short-circuit scan|Exact for N rows|
|column\_type\_drift|Schema comparison at parse time (static)|Zero — compile time only|Exact|

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
  pipeline_id   TEXT
  blueprint_sha TEXT        -- git SHA of Blueprint at run time
  manifest_path TEXT        -- path to compiled Manifest JSON
  started_at    TIMESTAMP
  completed_at  TIMESTAMP
  status        TEXT        -- running | success | failed | patched | skipped
  patch_count   INTEGER     -- number of patches applied in this run
```

## **6.4 SparkListener Integration**
Aqueduct registers a custom SparkListener with the SparkSession before any job is submitted. The listener runs on the driver in a separate thread and writes events directly to the Observability Store without touching the Spark execution thread.

Events captured:

- onStageCompleted — records stage ID, duration, input/output row count, shuffle read/write bytes, spill bytes. This is the primary source for zero-cost row count estimates.
- onTaskEnd — records per-task metrics for identifying skew (tasks with outlier durations).
- onJobEnd — records job-level success/failure and total duration.
- onApplicationEnd — final flush of any buffered events before session close.

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
  pipeline_id     TEXT
  from_module     TEXT
  from_column     TEXT
  to_module       TEXT
  to_column       TEXT
  transform_op    TEXT        -- e.g. "sql:CAST", "deduplicate", "join:left"
```

```text
Table: quality_lineage
  run_id          TEXT
  pipeline_id     TEXT
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

**On LLM cost and context window size:** The default FailureContext is intentionally generous. Even a full, unpruned context package of approximately 40,000 tokens costs less than $0.50 per patch attempt with current frontier models. Over a month of pipeline operations, this is orders of magnitude cheaper than maintaining on-call engineering coverage. The ContextPruner (Section 8.4) exists to improve model accuracy and reduce latency — not primarily to reduce cost. Engineers should size `max_patches_per_run` based on risk tolerance for auto-applied patches, not on token cost.

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
  "pipeline_id":        "pipeline.orders.daily_aggregate",
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
  "depot_state": {       /* relevant Depot keys read/written by this pipeline */ }
}
```

## **8.5 Patch Grammar — PatchSpec Operations**
The agent responds exclusively with a PatchSpec JSON object. Any response that is not a valid PatchSpec is rejected, and the agent is re-prompted with the validation error. The agent may include multiple operations in one PatchSpec — there is no limit on the number of operations per patch, enabling full pipeline restructuring.

|**Operation**|**Description**|
| :- | :- |
|**replace\_module\_config**|Replace the config block of a named Module. The most common operation. Used to fix wrong paths, bad SQL, incorrect params, wrong format strings.|
|**replace\_module\_label**|Update the label of a Module. Used when the agent renames a Module for clarity after restructuring.|
|**insert\_module**|Insert a new Module at a specified position in the graph. Requires specifying upstream and downstream edges. Used to add a cast step, filter, Probe, or Regulator.|
|**remove\_module**|Remove a Module and rewire its edges. The agent must specify how to reconnect the graph. Used to eliminate a broken step.|
|**replace\_context\_value**|Update a Tier 0 or Tier 1 value in the Context Registry. Used when a path, key, threshold, or parameter is wrong.|
|**add\_probe**|Attach a new Probe to a Module's output. Commonly added by the agent before a retry to gather more signal.|
|**replace\_edge**|Rewire an existing edge. Used when the agent determines data should flow through a different path.|
|**set\_module\_on\_failure**|Change the retry or spillway policy for a specific Module.|
|**replace\_retry\_policy**|Replace the pipeline-level retry policy. Used when the agent determines the current policy is causing repeated failures.|
|**add\_arcade\_ref**|Reference a new or existing Arcade sub-Blueprint. Used when the agent restructures repeated logic into a reusable Arcade.|

Example PatchSpec (single operation):

```JSON
{
  "patch_id":   "patch_20240412_143155",
  "run_id":     "run_20240412_143022_a3f9",
  "rationale":  "Column event_ts is STRING but cast to TIMESTAMP without explicit
                 format. Replacing with explicit format string cast.",
  "operations": [
    {
      "op":        "replace_module_config",
      "module_id": "cast_and_clean",
      "config": {
        "op": "sql",
        "query": "SELECT *, TO_TIMESTAMP(event_ts, 'yyyy-MM-dd HH:mm:ss')
                  AS event_ts FROM dedup_orders"
      }
    }
  ]
}
```

## **8.6 Agent Guardrails**

When `approval_mode` is `auto` or `aggressive`, the LLM applies patches without human review. Guardrails limit what can be autonomously changed:

```yaml
agent:
  approval_mode: auto
  allowed_paths: ["s3://company-data/*", "gs://prod-bucket/*"]
  forbidden_ops: ["insert_module", "remove_module"]
```

| Field | Type | Description |
| :- | :- | :- |
| `allowed_paths` | list[str] | fnmatch patterns. Any `path` field in a patch operation must match at least one pattern. Empty list = unrestricted. |
| `forbidden_ops` | list[str] | PatchSpec operation names that are always blocked. Blocked patches are automatically staged for human review instead of auto-applied. |

If a patch violates either guardrail, it is moved to `patches/pending/` for human review regardless of `approval_mode`. A clear error message is emitted.

**`validate_patch: true` — Patch Dry-Run (aggressive mode)**

When enabled, the patched Blueprint is compiled in memory before being written to disk in `aggressive` mode. If compilation fails (parse error, compile error), the patch is staged for human review and the self-healing loop stops — the on-disk Blueprint is never touched:

```yaml
agent:
  approval_mode: aggressive
  validate_patch: true
  max_patches_per_run: 5
```

Provides a safety net for aggressive mode: schema errors, invalid references, and compile-time violations are caught before the Blueprint is mutated. Has no effect on `auto` or `human` modes (which already validate before writing or never write autonomously).

**Recommended guardrail policy for production:**

```yaml
agent:
  approval_mode: auto
  allowed_paths: ["s3://your-company-bucket/*"]
  forbidden_ops: ["insert_module", "remove_module"]  # structural changes require human review
```

## **8.7 Patch Lifecycle**
1. Surveyor detects failure, exhausts retry policy if applicable, assembles FailureContext.
1. FailureContext is posted to the configured LLM endpoint (model specified in agent: block).
1. LLM responds with a PatchSpec JSON. Aqueduct validates the PatchSpec against its schema. If invalid, the LLM is re-prompted with the validation error and its response. Maximum 3 re-prompt attempts before escalating to human approval regardless of approval\_mode.
1. Valid PatchSpec is checked against guardrails (allowed\_paths, forbidden\_ops). If any guardrail fires, patch is staged in patches/pending/ for human review regardless of approval\_mode.
1. If approval\_mode: disabled — LLM never fires.
1. If approval\_mode: human — Patch is held in patches/pending/. Engineer reviews and runs: aqueduct patch apply <patch\_id> or aqueduct patch reject <patch\_id>.
1. If approval\_mode: auto — Patch is applied in-memory and validated by re-running the pipeline. Only if the re-run succeeds is the Blueprint updated on disk. If the re-run fails, the Blueprint is unchanged. One patch attempt per run.
1. If approval\_mode: aggressive — If `validate_patch: true`: patch is compiled in memory first; if invalid, staged for human review and loop stops. Otherwise: patch is applied to the Blueprint immediately and the pipeline re-runs. Loops up to max\_patches\_per\_run times. ⚠ Blueprint may be modified multiple times autonomously.
1. Applied patch is moved to patches/applied/<patch\_id>.json with applied\_at timestamp, run\_id, and agent rationale preserved.
1. If max\_patches\_per\_run is reached without a successful run, the pipeline is aborted.
1. To undo an applied patch: aqueduct patch rollback <patch\_id>. This atomically restores the Blueprint from the backup in patches/backups/ and moves the patch record to patches/rolled\_back/.

## **8.7 Resume-From Semantics** *(Deferred — Advanced Feature)*

> **V1 behaviour:** When a patch is applied, Aqueduct re-runs the entire pipeline from the beginning. This is less efficient than partial resume but is 100% reliable and trivial to implement. Partial resume is deferred to a post-MVP release.

**Why deferred:** Partial resume requires the Executor to cache intermediate DataFrames across a JVM session boundary, maintain a mapping of which Modules completed, and handle invalidation when a patch modifies an upstream Module. The correctness surface area is large. The V1 trade-off is to accept the re-run cost in exchange for implementation simplicity.

**Middle-ground path (optional, not required for MVP):** If the Blueprint explicitly uses `df.checkpoint()` or sets `spark.sql.streaming.checkpointLocation` at a Module boundary, the Planner may restart from that checkpoint location after a patch. This is opt-in and driven entirely by the Blueprint author — Aqueduct does not insert checkpoints automatically.

**Future full implementation (post-MVP):**

- Any Module whose output was consumed by a completed Egress is not re-executed. Its output is considered finalised.
- Any Module whose output was not yet consumed is re-executed from the first un-cached ancestor.
- The Surveyor records which Modules completed successfully in the RunRecord before failure, enabling precise resume targeting.
- If the patch modifies a Module that ran successfully before the failure, Aqueduct forces re-execution of that Module and all its descendants.


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
  master_url: "local[*]"]           # overridden per target type
stores:
  observability:
    backend: duckdb                # duckdb (default) | s3 | gcs | adls
    path: ".aqueduct/signals"      # directory; signals written as Parquet files
  lineage:
    backend: duckdb
    path: ".aqueduct/lineage"      # directory; lineage records written as Parquet files
  depot:
    backend: duckdb
    path: ".aqueduct/depot.duckdb" # single DuckDB file for KV state
agent:
  default_model: claude-sonnet-4-20250514
  api_endpoint: https://api.anthropic.com/v1/messages
  max_tokens: 4096
  max_patches_per_run: 5
  default_approval_mode: auto
probes:
  max_sample_rows: 100
  default_sample_fraction: 0.01
  block_full_actions_in_prod: true
secrets:
  provider: env                    # env | aws | gcp | azure | vault

# Simple form — plain URL, sends full FailureContext JSON via POST
webhooks:
  on_failure: "https://hooks.slack.com/services/T.../B.../"

# Full form — custom method, headers, templated payload
webhooks:
  on_failure:
    url: "${SLACK_WEBHOOK_URL}"
    method: POST                   # POST | PUT | PATCH
    headers:
      Authorization: "Bearer ${API_TOKEN}"   # ${ENV_VAR} reads os.environ
    payload:                       # null = send full FailureContext JSON
      text: "Pipeline *${pipeline_id}* failed on `${failed_module}`"
      run_id: "${run_id}"
    timeout: 10
```

**Webhook template variables** (available in `payload` values and `headers` values):

| Variable | Value |
| :- | :- |
| `${run_id}` | UUID of the pipeline run |
| `${pipeline_id}` | Pipeline identifier string |
| `${pipeline_name}` | Pipeline display name |
| `${failed_module}` | Module ID of first failure |
| `${error_message}` | Human-readable error string |
| `${error_type}` | Exception class name |
| `${started_at}` | ISO-8601 run start timestamp |
| `${attempt}` | Number of failed modules in this run |
| `${ANY_ENV_VAR}` | Falls back to `os.environ` if not a built-in var — useful for auth tokens in headers |

If `payload` is omitted, the complete `FailureContext` JSON document is sent (run metadata, failed module, stack trace, probe signals, manifest snapshot).

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

The Depot KV store uses DuckDB in embedded mode. DuckDB's embedded mode supports multiple concurrent readers but only one writer at a time per database file. **Two `aqueduct run` processes writing to the same `depot.duckdb` file simultaneously may produce write errors or data corruption.**

**Safe patterns:**
- Sequential pipeline runs (even different pipelines) sharing the same depot file: fully safe.
- Parallel runs from different machines pointing to a shared depot file over a network filesystem: not supported — use a centralized backend (future: `depot.backend: postgres`).
- Parallel runs on the same machine: use a separate depot file per pipeline (`depot.path: .aqueduct/<pipeline_id>.duckdb`), or serialize invocations through an orchestrator.

**Scheduler integration pattern:** When using Airflow, Prefect, or similar tools, pass `--execution-date {{ ds }}` to make `@aq.date.*` functions idempotent. Use the depot watermark pattern to track last-processed state. Query `run_records` in the DuckDB observability store to detect and backfill missed runs.


# **10.5 `aqueduct test` — Isolated Module Testing**

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

|**Command**|**Description**|
| :- | :- |
|**aqueduct new \<name\>**|Scaffold a minimal valid Blueprint file named \<name\>.yml in the current directory. Generates a hello-world Ingress → Channel → Egress pipeline with default context variables and local Spark config. Designed so the user can run it immediately to verify their Spark connection.|
|**aqueduct run <blueprint.yml>**|Parse, compile, plan, and execute a Blueprint. Accepts --profile, --ctx key=val, --config, --run-id, --store-dir, --webhook, --resume, --from, --to, --execution-date.|
|**aqueduct validate <blueprint.yml>**|Parse and validate a Blueprint without compiling or running. Reports all schema errors. Exit code 0 = valid.|
|**aqueduct compile <blueprint.yml>**|Parse, compile, and write the Manifest to stdout or --output path. Accepts --profile, --ctx, --execution-date. Useful for inspecting the resolved Manifest before running.|
|**aqueduct test <test\_file.yml>**|Run isolated module tests from a YAML test file. Executes Channel/Junction/Funnel/Assert against inline data. No Ingress/Egress. Uses same SparkSession config as `aqueduct run`. Accepts --blueprint, --config, --quiet.|
|**aqueduct heal <run\_id>**|Manually trigger the LLM agent loop for a failed run. Optionally scoped with --module <module\_id> to focus the agent on a specific Module.|
|**aqueduct patch apply <patch\_id>**|Apply a pending Patch. Recompiles the Manifest and resumes execution from the patched Module.|
|**aqueduct patch reject <patch\_id>**|Reject a pending Patch. Records rejection reason (--reason flag). Pipeline remains in failed state.|
|**aqueduct patch rollback <patch\_id>**|Roll back an applied patch. Atomically restores the Blueprint from patches/backups/ and moves the patch record to patches/rolled\_back/. Only works for patches applied via Aqueduct (which create automatic backups).|
|**aqueduct report <run\_id>**|Print the Flow Report for a completed or failed run. Accepts --format table\|json\|csv.|
|**aqueduct lineage <pipeline\_id>**|Print the ColumnLineageGraph for a pipeline. Accepts --from <source\_table> --column <col> for targeted queries.|
|**aqueduct signal <signal\_id>**|Set or clear a persistent gate override for a Probe signal (identified by Probe module ID). `--value false` closes the gate for all future runs. `--error "msg"` closes with a reason. `--value true` clears the override, resuming normal Probe evaluation. Omitting all flags prints the current override status. Override stored in `signal_overrides` table in signals.db; Regulator checks it before any run-scoped Probe data.|
|**aqueduct depot get <key>**|Read a value from the Depot KV store.|
|**aqueduct depot set <key> <value>**|Write a value to the Depot KV store. Requires the key to have access: readwrite.|
|**aqueduct depot list [prefix]**|List all Depot keys, optionally filtered by prefix.|
|**aqueduct check-config**|Validate aqueduct.yml schema without running a pipeline. Prints resolved engine, stores, webhook, and secrets summary. Exit 0 = valid, 1 = invalid. Accepts --config to specify a non-default path.|
|**aqueduct doctor**|Probe all configured resources end-to-end: config schema, DuckDB stores (depot + observability), secrets provider, LLM endpoint reachability, webhook reachability, Spark connectivity, and object storage auth. Each check is independent. Accepts --config and --skip-spark (skips JVM startup for fast CI use). Exit 0 = all ok/warn/skip, 1 = any check failed.|

**Key flags for `aqueduct run`:**

| Flag | Description |
| :- | :- |
| `--from <module_id>` | Start execution at this module. Modules before it in the DAG are skipped (status=skipped). Their upstream data must already exist (e.g., from a prior run or external write). |
| `--to <module_id>` | Stop execution after this module. All its ancestors run normally; modules after are skipped. |
| `--execution-date YYYY-MM-DD` | Logical execution date. All `@aq.date.*` functions evaluate relative to this date instead of the system clock. Required for idempotent backfills. |
| `--resume <run_id>` | Resume from checkpoints written by a prior run. Modules with a `_aq_done` checkpoint marker are skipped. |

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

### **Visual Graph Editor (UI)**
The Blueprint YAML is always the source of truth. The UI is a visualisation and editing layer that reads and writes valid Blueprint YAML — never a separate representation. Module labels are shown in the UI; Module IDs are managed internally. The UI must enforce the same JSON Schema validation as the Parser. Full UI specification is a separate document.

### **Multi-pipeline Orchestration**
Aqueduct currently runs one pipeline per invocation. Cross-pipeline dependencies (pipeline A must complete before pipeline B starts) are handled externally via Depot watermarks and standard orchestrators (Airflow, Prefect, etc.) triggering aqueduct run commands. A native Aqueduct workflow layer (a Blueprint of Blueprints) is a potential v1.2 feature.

### **LLM Model Benchmarking**
Patch quality varies by LLM model. A benchmark suite of representative FailureContext scenarios with known correct PatchSpec responses is needed to evaluate and compare models. This is a product/eval concern and does not affect the architecture. Aqueduct's pluggable model configuration means model selection can be changed without code changes.

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

## **13.2 Scheduling**

Aqueduct has no built-in scheduler. `aqueduct run` is a one-shot CLI command designed to be invoked by an orchestrator:

- **Simple cron**: Any OS-level cron, systemd timer, or cloud scheduler (AWS EventBridge, GCP Cloud Scheduler) invoking `aqueduct run blueprint.yml`.
- **Complex orchestration**: An Airflow `AqueductOperator` (wrapping `aqueduct run` as a `BashOperator` or SDK call) for dependency management, backfill, and SLA tracking.
- **On-demand**: Manual invocation from CI/CD pipelines or by the LLM agent via `aqueduct run` after patch application.


# **14. Implementation Phases** *(Implementer Reference)*

> This section is a guide for phased implementation. It is not part of the public specification.

The recommended build order prioritises fast feedback loops and defers complex subsystems until the core is proven.

| **Phase** | **Deliverable** | **Done when** |
| :- | :- | :- |
| **Phase 1 — Parser & Manifest** | YAML → AST → Manifest JSON. Tier 0 context substitution only. No Spark. | `aqueduct compile hello.yml` produces valid Manifest JSON with all `${ctx.*}` tokens substituted. |
| **Phase 2 — Ingress / Egress** | Executor reads a Parquet file (Ingress) and writes it back out (Egress). Minimal SparkSession management. | `aqueduct run hello.yml` reads a CSV and writes a Parquet file on local Spark. |
| **Phase 3 — Channel: sql** | Single `op: sql` Channel executing a Spark SQL query. Upstream Module ID registered as temp view. | A Blueprint with `Ingress → Channel (op: sql) → Egress` runs end-to-end. |
| **Phase 4 — Mock Surveyor** | Surveyor detects job failure and emits a webhook payload. No LLM yet. | A broken SQL query triggers a webhook with the error details. |
| **Phase 5 — Patch Grammar (manual)** | PatchSpec JSON schema defined. `aqueduct patch apply` reads a hand-written PatchSpec and updates the Blueprint. | A human-authored PatchSpec fixes the broken SQL from Phase 4 when applied via CLI. |
| **Phase 6 — LLM Integration** | Surveyor packages FailureContext, calls the LLM, validates the PatchSpec response, applies it. | An end-to-end self-healing run: pipeline breaks, LLM proposes a patch, patch is applied, pipeline retries successfully. |



*AQUEDUCT — System Design & Implementation Reference — v1.0*
v1.0  —  Blueprint · Module · Ingress · Channel · Egress · Junction · Funnel · Probe · Regulator · Spillway · Arcade · Surveyor
