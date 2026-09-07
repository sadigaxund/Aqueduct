# 4. Blueprint Format

# **4. Blueprint format**

## **4.1 File format & versioning**

Blueprints are YAML files. The format is versioned, the `aqueduct` field selects the JSON Schema version. Unknown fields at any level are hard errors. This guarantees Blueprints are always valid input for LLM patch generation.

## **4.2 Top-level structure**

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
    port: main                         # main (default), spillway, signal, or a Junction branch id
    as:   orders                       # optional: the name this frame has inside the target Channel

engine:                                 # per-engine settings, namespaced by engine name
  spark:
    conf:                               # merged with aqueduct.yml's engine.spark.conf
      spark.sql.shuffle.partitions: 200
      spark.sql.adaptive.enabled: true
  duckdb:                               # merged with aqueduct.yml's engine.duckdb.*
    memory_limit: "8GB"                 # resource/tuning knobs only — see §10.1

retry_policy:                          # per-pipeline retry config
  max_attempts: 3

warnings:                              # optional — per-Blueprint compile-warning suppression
  suppress:
    - perf_python_udf_row_at_a_time

hooks:                                 # optional — lifecycle actions after terminal state / heal milestones
  on_success:
    - blueprint: blueprints/downstream.yml   # chain another Blueprint (fresh subprocess)
    - webhook: https://hooks.example/notify  # bare URL, or a map with url/method/headers/payload
    - command: "scripts/commit_outputs.sh ${run.id}"   # gated by danger.allow_command_hooks
      timeout: 120
  on_failure:
    - command: "scripts/cleanup_partial.sh ${run.id}"
      when_error: ["EmptyDataset"]           # optional — only fire for this error_type
  on_patch_pending:
    - webhook: https://hooks.example/patch-review
  on_healed:
    - blueprint: blueprints/notify_healed.yml
      in_process: true                       # reuse this run's live SparkSession
```

**Per-Blueprint compile-warning suppression (`warnings:`).** `warnings.suppress` is a list of compiler-warning `rule_id`s (or the sentinel `"*"`) to silence for THIS Blueprint only, it covers all **compile-time** diagnostics: the modular rule registry (e.g. `file_format_no_repartition`, `jdbc_missing_partition`, `kafka_checkpoint_stale`) and the inline compiler checks (e.g. `perf_python_udf_row_at_a_time`, `perf_multi_consumer_no_cache`, `delivery_append_retry_dupes`). It is unioned with the engine-level `warnings.suppress` from `aqueduct.yml` (+ `--suppress-warning` flags), either side suppressing a rule silences it. It does **not** affect engine/session-startup warnings, runtime (Probe/Assert) warnings, or the process-global default used by other Blueprints, a rule suppressed here stays visible everywhere else. For an **Arcade** sub-Blueprint, the parent Blueprint's `warnings.suppress` covers the whole expanded compilation unit (including the sub-Blueprint's modules); the sub-Blueprint's own `warnings:` block is valid YAML (it parses standalone) but is not consulted during expansion.

**Lifecycle hooks (`hooks:`).** Four events: `on_success` / `on_failure` run sequentially AFTER the pipeline reaches its terminal state; `on_patch_pending` / `on_healed` fire MID-RUN at heal milestones, mirroring the engine-level `webhooks:` `on_patch_pending` vocabulary one level up, at the Blueprint. `on_patch_pending` fires every time a heal stages a patch for human review (guardrail-blocked staging and `approval: human`, both staging sites). `on_healed` fires once a heal's re-run succeeds, patch applied AND the pipeline green again, and always runs BEFORE the outer run's terminal `on_success` hooks (it fires mid-loop, before the loop breaks out to the terminal report). No event **ever changes the run's exit code** (a failing hook emits `⚠ [hook_failed]` and skips the event's remaining hooks). Each entry sets **exactly one** action:

| Entry | Semantics | Gate |
| :- | :- | :- |
| `blueprint: <path>` | Chains another Blueprint. By default a fresh `aqueduct run` subprocess, own session, run_id, and report. Loose coupling by design: the child's failure is one `hook_failed` warning, not a parent failure. Tightly-coupled work belongs in ONE Blueprint (Arcades / `--parallel` / `enabled:`). `in_process: true` opts into parsing+compiling+executing the target in the SAME process, reusing the caller's live session, no self-healing loop for the chained target (a failure is still just `[hook_failed]`); falls back to the subprocess path with an info message when the target Blueprint sets its own `engine.spark.conf` (merging two Blueprints' Spark configs into one live session isn't generally safe). It also falls back (this time with a `[hook_in_process_unavailable]` warning, since there is no other signal the requested execution model didn't happen) on a polyglot run: every island's session is already closed by the time hooks fire (there was never one session for the whole run to reuse), so `in_process: true` on a polyglot Blueprint's hooks always runs the target as a fresh subprocess instead. | none (declarative) |
| `webhook: <url \| map>` | Fires the same endpoint model as the engine-level `webhooks:` block, bare URL shorthand or full `{url, method, headers, payload}` with `${run_id}`/`${blueprint_id}` payload templating. Fire-and-forget, background thread. | none (declarative) |
| `command: "<argv>"` | Arbitrary subprocess: shlex argv, **no shell**; only `${run.id}` / `${run.status}` / `${blueprint.id}` are interpolated. Per-entry `timeout:` (default 300 s). | `danger.allow_command_hooks: true` in **aqueduct.yml**: the gate is operator-owned engine config, so a Blueprint cannot self-authorize; ungated entries are skipped with `[hook_command_disabled]`. |

**Per-entry error filter (`when_error:`)**: optional, on `on_failure` / `on_patch_pending` / `on_healed` entries only (these three events carry a failure context; `on_success` does not, and setting `when_error` there is a **schema error** at parse time). A list of error-type names matched against `FailureContext.error_type` (the Assert rule's `error_type` label) or the exception class name extracted from the stack trace, the exact same candidate set and exact-match semantics as `agent.guardrails.heal_on_errors`. Unset (the default) fires unconditionally, fully backward compatible. A non-matching entry is silently skipped (not a `[hook_failed]`) and does not stop the remaining entries of that event.

Placement nuance: **`webhooks:` (aqueduct.yml) vs `hooks:` (Blueprint)**: the engine-level `webhooks:` block is ops-owned alerting that fires regardless of what any Blueprint declares (and includes the heal-loop event `on_patch_pending`); `hooks:` travel with the pipeline, are versioned and code-reviewed with it, and add `blueprint:`/`command:` actions plus the `when_error:` filter and `in_process:` execution mode. Both use the same endpoint model for webhooks. Safety: no patch-grammar operation can address `hooks:`, so the LLM self-healer cannot inject or alter them. **Cycle guard**: chained `blueprint:` hooks carry ancestry in `AQUEDUCT_HOOK_CHAIN` (subprocess mode) or an explicit in-memory chain (`in_process: true`, the env var is process-scoped and does not propagate in-process), a hook targeting an ancestor (or itself) is refused with `[hook_cycle]`, chain depth caps at 8 (`[hook_depth]`), and `aqueduct doctor` performs the same walk statically across all four events. When a hooks section ran, the CLI closes with a final `✓ run complete` footer after the per-hook `✓/⚠` lines. For an Arcade sub-Blueprint, `hooks:` parses but is ignored, only the top-level Blueprint's hooks fire.

**Linear-edge sugar.** `edges:` may be omitted entirely. When it is, and every module is a single-input/single-output type (Ingress, Channel, Egress, Assert), the Compiler chains the modules in declaration order, injecting `main`-port edges marked `injected: true` in the Manifest. If the Blueprint omits `edges:` while using a fan-out (Junction), fan-in (Funnel), sub-pipeline (Arcade), tap (Probe), or gate (Regulator) module, compilation fails with an error: those ports are ambiguous in a flat chain, so they must be wired explicitly. A single-module Blueprint needs no edges.

## **4.3 Module schema: common fields**

Every Module regardless of type shares these fields:

| Field | Description |
| :- | :- |
| **id** | Required. Unique string within the Blueprint. Must be filesystem-safe. |
| **label** | Required. Human-readable display name. |
| **type** | Required. One of: Ingress, Channel, Egress, Junction, Funnel, Probe, Regulator, Arcade, Assert. |
| **description** | Optional. Free-text explanation. Used in LLM context and UI. |
| **tags** | Optional list of strings. Used for filtering and scoped search. |
| **config** | Type-specific configuration block. |
| **spillway** | Optional downstream Module ID to receive error-port output: authoring SUGAR for a `port: spillway` edge from this module (see below); not a second runtime mechanism. |
| **depends_on** | Optional explicit upstream dependency list. |
| **checkpoint** | Optional boolean. When true, output DataFrame is saved as Parquet for `--resume`. |
| **enabled** | Optional boolean (default `true`); accepts `${ctx.*}` / `${ENV}` so context profiles can toggle it (coerced from true/false/1/0/yes/no/on/off). A disabled module still compiles but is skipped (⏭) at run time, and the disable **cascades**: every module consuming its output, via edges, `depends_on`, or Probe `attach_to`, is disabled too, transitively and uniformly (a join or union missing one input does not run partially). A disabled Arcade disables all its expanded children. Disabled modules are excluded from compile-time warnings. If the cascade disables every module, compilation fails. |
| **retry** | Optional. Per-module override of the top-level `retry_policy:` block, see below. |
| **engine** | Optional. A scalar execution-engine NAME (`spark`, `duckdb`) selecting which engine runs THIS module, see below. Distinct from the blueprint-level `engine:` BLOCK (§4.2, per-engine settings namespaced by engine name); same word, two levels. |

**`spillway:` field sugar.** `spillway: <target>` is authoring sugar for `edges: [{from: <this module>, to: <target>, port: spillway}]`; the Compiler expands it into that real edge at compile time (right after Arcade expansion, so a `spillway:` field set inside an Arcade's own sub-Blueprint is correctly namespaced first), the SAME and ONLY mechanism §4.4's spillway routing already documents. There is no behavioral difference between the two authoring forms; use whichever reads better for a given Blueprint. `Module.spillway` is validated at parse time (the target must exist) and is `None` on every module in the compiled Manifest once desugared: the edge is the sole runtime encoding. Conflict rule: a module carrying BOTH the `spillway:` field and an explicit `port: spillway` edge is fine when they name the SAME target (idempotent; no duplicate edge); naming a DIFFERENT target is a `CompileError` (never silently pick one).

### `config:` is a typed, per-module-type union

Every module type declares its own `config:` shape; a pydantic discriminated union on `type`, one member per module type (`Ingress`/`Channel`/`Egress`/`Junction`/`Funnel`/`Probe`/`Regulator`/`Arcade`/`Assert`), each absorbing that type's real keys with `extra="forbid"`. An Ingress's `config:` accepts `format`/`path`/`table`/`schema_hint`/`options`/…; an Egress's accepts `format`/`mode`/`maintenance`/…; a key that belongs to a DIFFERENT type (or belongs to no type at all) is a structural rejection at parse time naming the offending key; not a silent accept.

The one deliberate exception is `options:` (Ingress/Egress); a freeform passthrough dict forwarded verbatim to the engine's reader/writer `.option(k, v)` calls, since enumerating every Spark/DuckDB option is out of scope and the wrong target. A couple of genuinely polymorphic fields (Ingress `schema_hint`, Channel `columns`) are similarly kept as an untyped container with the accepted shapes documented on the field, rather than modeled as a second-level union.

**Every module type's `config:` is strictly typed.** A key that no code reads (a typo, a stale synonym, a dead knob) is a `ParseError` naming the key, never a silent no-op. This is by design: a freeform dict is invisible to the capability framework by construction (a leaf is derived by introspecting a pydantic model), so a wrong key inside `config:` could never surface as a "this engine doesn't support X" capability gate; it would just silently do nothing on every engine. When a Blueprint hits this: the error names the exact key and module; check that type's `config:` fields against this section (or `SKILL.md`) for the correct spelling.

Capability leaves follow the same split: fields common to every type (`id`, `label`, `engine`, `retry`, …) keep the `module.field.<name>` leaf id; every type-specific field (both the ones already living at the module's top level (`attach_to`, `ref`, `materialize`) and every field inside a typed `config:`) gets a `<type_lower>.field.<name>` leaf (e.g. `egress.field.maintenance`, `channel.field.query`), so every engine must give it a real verdict (`aqueduct/executor/capability_leaves.py`).

### Per-module retry override (`retry:`)

`retry_policy:` (§10.1-adjacent top-level block) sets the blueprint-wide default retry behaviour. A module's own `retry:` block overrides it **field-by-field**, any field left unset inherits the blueprint-level value for that field (same per-field inheritance shape as agent cascade tiers, §8):

```yaml
retry_policy:
  max_attempts: 3
  on_exhaustion: trigger_agent

modules:
  - id: flaky_jdbc_source
    type: Ingress
    label: Flaky Source
    config: { format: jdbc, ... }
    retry:
      max_attempts: 6        # override — this module gets more attempts
      # on_exhaustion inherits "trigger_agent" from retry_policy above
```

Fields: `max_attempts`, `backoff` (whole-block override: set every backoff sub-field or omit the block entirely; a module `backoff:` does NOT merge field-by-field against the blueprint's `backoff:`), `transient_errors`, `non_transient_errors`, `on_exhaustion`, `deadline_seconds`. One caveat: `deadline_seconds: null`/omitted at module level always means "inherit", there is no module-level way to explicitly clear a blueprint-level deadline back to "no deadline."

This is distinct from `on_failure` (an internal field the self-healing agent writes via the `set_module_on_failure` / `replace_retry_policy` patch ops, a full RetryPolicy replacement, not merged against the blueprint policy). When both are present at runtime, `on_failure` (heal-time) wins over `retry:` (authoring-time) wins over the blueprint-level `retry_policy:`.

### Cross-engine handoff: per-module `engine:` and islands

A Blueprint may span more than one execution engine. Every module carries an optional `engine:` field, a scalar engine NAME (`spark`, `duckdb`, …); deliberately the same word as the blueprint-level `engine:` BLOCK (§4.2, per-engine session SETTINGS namespaced by engine name), but a different level: the block configures an engine's session behaviour, the field picks which engine runs one module. The two never conflict because they live at different keys with different shapes (a block, keyed by engine name; a scalar, on a module), and neither error message ever mentions the other.

Every module resolves to exactly one engine, following four rules in this precedence:

1. **An explicit `engine:` on the module wins.** The Blueprint's own pin is never overridden.
2. **Unset → inherit the SINGLE upstream parent's (already-resolved) engine.** "Parent" means a module feeding this one over a `main`/`spillway` data edge, a `depends_on` entry, or (for a Probe specifically) its `attach_to` target (a Probe has no incoming data edge, so `attach_to` is its one inheritance parent; this is what makes an unpinned Probe land on its target's engine by default).
3. **Unset + multiple parents resolved to DIFFERING engines → `CompileError`** demanding an explicit `engine:` on the module; the compiler will not guess which upstream to follow.
4. **Unset + no parents (an Ingress, typically) → `deployment.engine`** (or `--set deployment.engine`), the configured default.

Precedence against config: `deployment.engine` only moves rule 4's DEFAULT. An explicit per-module pin (rule 1) always wins over it: the Blueprint expresses semantics (which engine this transform needs), the config expresses environment (which engine to default to). A Blueprint with no `engine:` field anywhere is fully portable across every registered engine; a pinned `engine:` is a declared engine dependency for that module's island, enforced by the capability gate below.

> **EXPERIMENTAL: cross-engine islands and handoff.** Everything in this subsection about a
> Blueprint spanning MORE THAN ONE engine (islands with a boundary edge, the synthetic Handoff
> module, and the spill that carries data between them) is experimental and receives no further
> investment. It works and is tested, but the shape may change and a new engine is NOT required
> to support handoff to take part in Aqueduct: an engine that runs whole single-engine Blueprints
> is a complete engine. Single-engine Blueprints, which is what almost every Blueprint is, are
> unaffected and fully supported.

**Islands** are derived, never declared: there is no user-facing island syntax. An island is a connected subgraph of modules that share one resolved engine (connectivity follows the same `main`/`spillway` data-edge basis used elsewhere for parallel-component detection, plus a Probe's mandatory bond to its `attach_to` target). A **boundary edge** is a data edge whose two endpoints resolve to different islands: the compiler splices a synthetic Handoff module in at each one (`A -> B` becomes `A -> handoff -> B`; see §10.9). Disjoint components pinned to different engines produce **zero** boundary edges: two independent single-engine flows run side by side in one Blueprint with no handoff at all.

Two structural rules keep v1 from claiming more than it can run:

- **A Probe or Assert must colocate with its target's island.** Neither module type may introduce an engine boundary: a Probe's target is its `attach_to` module, an Assert's target is its upstream data parent(s). A mismatch (almost always an explicit `engine:` pin on the Probe/Assert that disagrees with its target) is a `CompileError`.
- **A spillway edge may not cross islands in v1.** Cross-engine quarantine routing isn't wired yet: route a spillway to a module on its source's own engine.

**The capability gate is per island.** Each island is checked against its OWN engine (§10.9): a module-type/op/mode leaf on one island is never checked against a different island's engine. An island whose engine has no registered capability declaration is a `CompileError` (the same fail-closed `UnknownEngineError` §10.9 already raises for an unregistered `deployment.engine`). For a single-engine Blueprint (no module pins any `engine:`) there is exactly one island, so this degenerates to the pre-2.34 single-engine gate exactly.

### Ports

| Port | Carries | Where it's produced | Where it's consumed |
| :- | :- | :- | :- |
| `main` (default) | Successful DataFrame | Every module type | Every module type |
| `spillway` | Row-level error DataFrame | Channel, Assert | Egress / Funnel (quarantine sink) |
| `signal` | Control signal, not a DataFrame | Probe (threshold signal) | Regulator (gate evaluation) |
| `<branch_id>` | One subset of the upstream Junction's branches | Junction | Any downstream module |

### Naming an input (`as`)

An edge may carry an `as:` key. It sets the name that edge's frame has inside the module the edge points at, which is the name a Channel writes in its `query:` (or in `left:` / `right:` on `op: join`).

Without `as`, a frame is named after the module that produced it, except on a Junction branch port, where the name is `<junction_id>.<branch_id>`. That dotted form is not a name SQL can reference: Spark rejects it as a temp view outright, and neither engine registers it. A single-input Channel does not need one, because it can always say `__input__`. A Channel with more than one input has no such fallback, so a Junction branch reaching one must be named:

```yaml
edges:
  - from: split_by_region
    to: compare_regions            # a Channel with op: sql
    port: us
    as: us_rows                    # the query then says `FROM us_rows`
  - from: split_by_region
    to: compare_regions
    port: eu
    as: eu_rows
```

The rules, all enforced at parse time:

- a Junction branch edge into a Channel with more than one input and `op: sql` or `op: join` must carry `as`
- an `as` may not repeat a module id, nor another `as` on an edge into the same module
- `as` on a single-input Channel is allowed, and names the frame alongside `__input__`
- `as` on anything but a Channel is an error, since no other module type resolves an upstream by name
- the value must be a bare identifier: a letter or underscore, then letters, digits or underscores

### Typed spillway routing (`error_types`)

A spillway edge may declare an `error_types` filter, a typed catch block. Only quarantined rows whose `_aq_error_type` label matches flow down that edge:

```yaml
edges:
  - from: orders_quality_gate
    to: write_quarantine
    port: spillway
    error_types:                   # optional filter — only route these error types
      - DataQualityViolation
      - SchemaError
```

The label comes from the Assert rule's `error_type` field (falling back to the rule name, `freshness`, `sql_row`, `custom`) or `SpillwayCondition` for Channel `spillway_condition` rows. Multiple spillway edges from one module act as separate catch blocks; an edge without `error_types` is a catch-all; rows matching no edge are dropped. The filter is a lazy Spark transformation, zero extra actions. `error_types` on a non-spillway edge is a parse error, and `aqueduct doctor` warns when a filter entry matches no label declared in the Blueprint.

> **⚠ `spillway_condition` without a spillway edge is dead code.** If a Channel sets `spillway_condition` but has no corresponding edge with `port: spillway` (and no `spillway:` field sugar, §4.3, which desugars into exactly that edge) the condition is silently ignored, all rows (including those matching the condition) flow to the main stream. The executor logs a warning at run time (the compiler's `spillway_port_mismatch` warning catches the common case earlier). This is not a compile error because the config alone is valid; it only becomes meaningful once wired, by either authoring form.

Every spillway row carries the system columns `_aq_error_module`, `_aq_error_type`, `_aq_error_msg`, `_aq_error_ts` (Assert rows additionally `_aq_error_rule`).

## **4.4 Module types: full specification**

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
| **format** | Spark data source format. Supports: parquet, delta, iceberg, hudi, csv, json, orc, avro, jdbc, kafka. `iceberg`/`hudi` require the matching `spark.jars.packages` and (Iceberg) a `spark.sql.catalog.*` in `engine.spark.conf`, see the Spark Guide. `format: custom` + `class:` registers a user Python DataSource (Spark 4.0+, see below). The `dataframe` format (Arcade cross-pipeline reference) is not yet implemented. Not required when `table:` is set and mutual-exclusive with `table:` (set one or the other). |
| **table** | Catalog table identifier (`catalog.schema.table`): passthrough to an external catalog. Read via `spark.read.table(table)`. The catalog is configured entirely through `engine.spark.conf` (e.g. `spark.sql.catalog.*` keys), external to Aqueduct. Mutually exclusive with `path:`, if both are set the engine raises an error. When `table:` is set, `format:` is not required. On DuckDB, resolved via `con.table(table)` against that engine's own catalog (`memory.main` for an unqualified name, unless a prior step in the same session changed the current catalog/schema, or `ATTACH`ed for a three-part name: Aqueduct never performs an implicit `ATTACH`); an unresolvable name raises naming the module and the table (see §10.9's `feature.table_addressing`). |
| **path** | Source path or URL. Context Registry references allowed. Optional for `format: custom` and the pathless formats (jdbc/kafka/depot). Mutually exclusive with `table:`. |
| **class** | For `format: custom`. Fully-qualified `module.Class` pointing at a `pyspark.sql.datasource.DataSource` subclass. |
| **partition_filters** | Optional SQL predicate for manual partition pruning. |
| **schema_hint** | Optional. Flat dict `{col: type}` or nested `{mode: strict\|additive\|subset, columns: [{name, type}]}`. |
| **time_travel** | Optional (Delta/Iceberg). Pin a historical snapshot: `{version: N}` (`versionAsOf`) or `{timestamp: "..."}` (`timestampAsOf`). Mutually exclusive. Metadata-only, no Spark action. Only supported with `path:`-based reads (format-based DataFrameReader options). For `table:`-addressed reads, use a Channel with `TIMESTAMP AS OF` SQL syntax instead. |
| **on_new_columns** | Optional schema-drift contract: `allow` (default behaviour, explicit), `fail` (raise if the source has columns outside the baseline), `alert` (warn, then proceed). Baseline = `known_columns` or, failing that, `schema_hint` names; with neither it is skipped. |
| **known_columns** | Optional explicit baseline column list for `on_new_columns`. |
| **options** | Passed directly to Spark DataFrameReader.option(k,v). |

**`schema_hint` type comparison goes through the type hub (§9), with numeric widening.** A hinted type is resolved as a hub type and compared against the engine's own inferred type for that column, not compared as a literal string. For the fixed-width numeric families (`tinyint`/`smallint`/`int`/`bigint`, `float`/`double`), a hint SATISFIES an actual column at least as wide in the same family: `quantity: integer` validates against a DuckDB-inferred `BIGINT` column, because DuckDB's CSV sniffer only ever infers `BIGINT` for whole numbers regardless of value range, while Spark's own inference picks the narrowest candidate that fits the data. The reverse (a hint wider than the actual type, e.g. `bigint` against an actual `int`) is NOT satisfied: widening is one-directional. This is a type-name resolution rule, not a value cast: no data is coerced, and a non-numeric mismatch (a string column hinted as an int) still raises exactly as before.

**Cloud credentials:** There is no per-Ingress `credentials:` field. Credentials live at the engine level in `engine.spark.conf:`, keyed by standard Hadoop/Spark property names. Use `@aq.secret('KEY')` or `${ENV_VAR}` inside those values.

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
    # clean_phone / parse_currency are udf_registry entries (§5.4): called
    # by name directly in SQL, no per-Channel scoping key. Every entry
    # registers session-wide, so any Channel's SQL may call any of them.
    query: |
      SELECT parse_currency(amount) AS amount, clean_phone(phone) AS phone
      FROM dedup_orders
```

Upstream Modules are referenced by their id directly in SQL FROM clauses. Aqueduct registers each upstream DataFrame as a temp view using its Module id. For single-input Channels, the upstream is auto-registered as `__input__`.

| Config field | Description |
| :- | :- |
| **op** | Operation type. Built-in ops: `sql` \| `deduplicate` \| `filter` \| `select` \| `rename` \| `cast` \| `join` \| `union` \| `sort` \| `repartition` \| `coalesce` \| `cache`. |
| **query** | SQL string (`op: sql` only). Upstream Module IDs available as temp views. |
| **key** | Column name or list of column names. Used by `deduplicate`. |
| **order_by** | Sort expression. Used by `deduplicate` and `sort`. |
| **condition** | Filter expression (`op: filter`). Standard Spark SQL boolean expression. |
| **columns** | Column mapping or list. Semantics depend on op. |
| **num_partitions** | Target partition count. Used by `repartition` and `coalesce`. |
| **spillway_condition** | Optional SQL boolean expression. Matching rows are routed to the spillway port. |

**Incremental watermark (`materialize:` / `watermark_column:`).** Declared MODULE-level fields, siblings of `config:`; NOT config keys (same shape as Probe's `attach_to`, §4.4). These are outside the freeform `config:` dict so the capability framework can see them (a freeform key is invisible to it by construction); every engine must declare a verdict for the `channel.field.materialize` / `channel.field.watermark_column` capability leaves (see §9's capability-leaf note below). `op: sql` only:

```yaml
- id: new_events
  type: Channel
  materialize: incremental          # module-level field, NOT inside config
  watermark_column: event_ts        # module-level field, NOT inside config
  config:
    op: sql
    query: |
      SELECT * FROM events
      WHERE event_ts > CAST(${ctx._watermark} AS TIMESTAMP)
```

| Field | Description |
| :- | :- |
| **materialize** | Optional. Set to `incremental` to opt this Channel into watermark-based incremental processing. On each run, the Depot's persisted `MAX(watermark_column)` (or the sentinel `1900-01-01 00:00:00` on the first run, so the first run is a full scan) is substituted, quoted, for the literal token `${ctx._watermark}` in the `config.query` string. After the run succeeds, the new `MAX(watermark_column)` (computed from the WRITTEN downstream Egress output, not the upstream DAG) is persisted back to the Depot. Requires a configured Depot (`stores.depots`); without one the watermark is lost between runs and every run re-scans all source data. |
| **watermark_column** | Required when `materialize: incremental`. Column used to track the high-water mark (typically a timestamp or monotonic integer). |

`materialize`/`watermark_column` are module-level fields, siblings of `config:` (like `attach_to`), not keys inside `config:`. A Blueprint that nests them inside `config:` parses (the dict stays freeform) but neither field is read from there; the incremental behaviour silently stops.

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
| `sort` | No | Yes | `df.orderBy(*exprs)`: deferred until action |
| `union` | No | No (multi) | `unionByName` across all upstreams |
| `repartition` | No | Yes | Full shuffle: increase partitions or rebalance |
| `coalesce` | No | Yes | No shuffle: shrink partition count |
| `cache` | Yes | Yes | `df.persist(StorageLevel)`: triggers materialisation |

### Egress

```yaml
- id: save_orders
  type: Egress
  config:
    format: parquet
    mode: overwrite                # overwrite | append | error | ignore | merge | overwrite_partitions
    path: "${ctx.tables.orders_out}"
    partition_by: [event_date, region]
    options: { compression: snappy }
```

| Config field | Description |
| :- | :- |
| **format** | Spark write format. Standard: parquet, delta, iceberg, hudi, csv, json, orc, avro, jdbc. `iceberg`/`hudi` need the matching `spark.jars.packages` (and an Iceberg catalog), see the Spark Guide. Pseudo-format `depot` writes a KV entry to the Depot instead of data (requires `key` + `value` or `value_expr`). |
| **mode** | Write mode: `overwrite`, `append`, `error` (default; alias `errorifexists`), `ignore`, `merge` (Delta `MERGE INTO`, requires `merge_key`), `overwrite_partitions` (idempotent partition-scoped overwrite, see below). |
| **table** | Catalog table identifier (`catalog.schema.table`): passthrough to an external catalog. When set, writes via `df.write.<mode>.saveAsTable(table)` instead of `.save(path)`. Supported for all write modes including `overwrite`, `append`, `error`, `overwrite_partitions`. Mutually exclusive with `path:`, if both are set the engine raises an error. For `mode: merge`, `table` is the Delta merge target (takes precedence over `path`, existing behaviour). `register_as_table` is meaningless when `table:` is set (the catalog table is already the direct write target). On DuckDB, `table:` writes directly into that engine's own catalog (`CREATE OR REPLACE`/`CREATE`/`INSERT INTO ... BY NAME`, mode-mapped onto DuckDB's own DDL guards: see §10.9's `feature.table_addressing`) rather than through a Delta/Iceberg-style external catalog; `overwrite_partitions`/`merge` stay unsupported there (no partition-directory pruning / no transaction log). |
| **path** | Output path or URL. Mutually exclusive with `table:`. For `mode: merge`, `table` may be used instead of `path`. |
| **partition_by** | Columns to partition the output by. |
| **repartition** | Optional. Full shuffle before the write: an integer targets exactly N output partitions/files (can raise or lower the file count, rebalances skew); `true` is shorthand for `1`. On Spark, applied via `df.repartition(n)` before the writer runs: the fix the `file_format_no_repartition`/`perf_delta_append_no_partition` compiler warnings' own suggested `repartition: N` now actually performs. On DuckDB, honestly `unsupported`: this engine has no shuffle/partition-count concept for a `COPY` target (see `coalesce` below). |
| **coalesce** | Optional. Merge to N output partitions/files with no shuffle (cheaper than `repartition`, can leave skewed partitions); `true` is shorthand for `1`. On Spark, applied via `df.coalesce(n)`. On DuckDB, maps onto "the fewest files this engine's `COPY` can produce for the write shape": a non-partitioned write already writes exactly one file, pinned explicitly via `PER_THREAD_OUTPUT false` rather than left to an undocumented default; does not target an exact N on this engine. |
| **merge_key** | Required for `mode: merge`. Column name or list of columns for the upsert match. |
| **class** | For `format: custom`. Fully-qualified `module.Class` pointing at a `pyspark.sql.datasource.DataSource` subclass (Spark 4.0+). |
| **replace_where** | For `mode: overwrite_partitions` (Delta). A predicate that is atomically replaced (Delta `replaceWhere`). Resolved at compile time, so it may embed `@aq.date.*` / `${ctx.*}` for `--execution-date` backfills. |
| **merge_schema** | Optional (Delta/Iceberg). `true` sets `mergeSchema`: new DataFrame columns are added to the target schema instead of failing the write. |
| **overwrite_schema** | Optional (Delta). `true` sets `overwriteSchema`: replaces the target schema entirely (`mode: overwrite` only). |
| **on_new_columns** | Optional schema-drift contract comparing the incoming DataFrame against the existing target: `allow` (absorb new columns via `mergeSchema`), `fail` (raise if the data adds columns the target lacks), `alert` (warn, then absorb). No-op on first write or `mode: merge`. |
| **options** | Passed directly to Spark DataFrameWriter.option(). |
| **register_as_table** | Optional. After a `path:`-based write, registers the location as an external table in the active catalog (`CREATE EXTERNAL TABLE IF NOT EXISTS`, best-effort, non-fatal on failure). On DuckDB, registers a `CREATE OR REPLACE VIEW` over the written file (`read_parquet`/`read_csv`) instead (a live pointer to the file's current contents, not a snapshot copy), under the same non-fatal-on-failure contract. Ignored (with a warning) when `table:` is set; the catalog table is already the direct write target. |
| **maintenance** | Optional. Post-write compaction/cleanup, format-aware (`delta`: `optimize`/`zorder_by`/`vacuum`; `iceberg`: `rewrite_data_files`/`expire_snapshots`; `hudi`: `compaction`/`clean`); full key reference in `docs/spark_guide.md`'s maintenance table. Runs synchronously after the write, non-fatal on failure. |
| **header** | CSV only, whether to write a header row (default `true`). Read directly by the DuckDB engine's writer; on Spark, set `options: {header: "true"}` instead (Spark's writer has no dedicated top-level `header:` read). |
| **key** / **value** / **value_expr** | `format: depot` only. `key` (required) names the Depot KV entry; exactly one of `value` (a literal string) or `value_expr` (a Spark aggregate expression, evaluated with one `.collect()`) supplies it. |
| **watermark_key** | Optional, `mode: append` only (never legal on `format: depot`). Names the Depot key a `format: depot` Egress writes to gate the next run's incremental read range; that depot Egress must be topologically AFTER this one in the blueprint's edge graph (reachable from it), not merely present somewhere in the Blueprint. See **§10.4.5 Watermark crash-consistency** below. |

**`mode: overwrite_partitions`** is the idempotent-backfill primitive: re-running for the same logical date replaces only that date's data instead of the whole table. Two strategies:

- **`replace_where: <predicate>`** (Delta): atomically replaces exactly the rows matching the predicate. The cleanest backfill: `replace_where: "event_date = '@aq.date.today()'"` with `--execution-date 2026-06-01` rewrites only that day.
- **no `replace_where`**: Spark **dynamic** partition overwrite (`partitionOverwriteMode=dynamic`): only partitions present in the written DataFrame are replaced; untouched partitions are preserved. **Requires `partition_by`**, without it the engine refuses (a plain `overwrite` would wipe the whole table).

**Custom Python DataSource (`format: custom`, Spark 4.0+).** Both Ingress and Egress accept `format: custom` with a `class:` pointer to an importable `pyspark.sql.datasource.DataSource` subclass. `class:` resolves against the Manifest's `base_dir` first (a sibling `.py` file next to the Blueprint, see **§3, `base_dir`**), falling back to a normal import. The class is imported, validated, registered with the session, then used by its own `name()`. `aqueduct doctor` verifies the class is importable and a valid subclass before a run. As with UDFs and custom probes, the Blueprint carries only a pointer, never an inline code body. Requires Spark 4.0+ (the `spark.dataSource` registry); the engine raises a clear error on older Spark.

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
| **branches** | List of branch definitions. Each has `id`, an optional `condition` (required for `mode: conditional`; the sentinel `"_else_"` catches rows no other branch's condition matched), and an optional `value` (`mode: partition` only; the value to match against `partition_key`; falls back to the branch's `id` when omitted). |
| **partition_key** | Required for `mode: partition`. Column whose value is matched against each branch's `value` (`{partition_key} = '{value}'`). |

```yaml
- id: split_by_region
  type: Junction
  config:
    mode: partition
    partition_key: region
    branches:
      - id: eu
        value: "EU"          # optional; defaults to the branch id
      - id: us
        value: "US"
```

### Funnel (Fan-in)

```yaml
- id: merge_all
  type: Funnel
  config:
    mode: union_all                # union_all | union | coalesce | zip
    inputs: [ingress_a, ingress_b]
```

| Config field | Description |
| :- | :- |
| **mode** | Funnel mode: `union_all` (zero-shuffle), `union` (distinct), `coalesce` (aligned), `zip` (monotonically increasing ID join). |
| **inputs** | Required. List of at least two upstream module IDs, in the order they are merged. |
| **schema_check** | `union_all`/`union` only. `strict` (default) requires identical schemas; `permissive` allows missing columns (filled with null). |

### Probe

```yaml
- id: schema_check
  type: Probe
  attach_to: dedup_orders          # module-level field, NOT inside config
  config:
    report: stdout                 # optional — also print signal results in the run summary
    signals:
      - type: schema_snapshot      # schema_snapshot | row_count_estimate | null_rates | sample_rows | value_distribution | distinct_count | data_freshness | execution_partitions | threshold | custom
```

Probes are non-blocking observability taps, implemented on both engines. They do not execute on the engine's critical path. `attach_to` is a module-level field (Probes attach by reference, not by edges); `config.signals` is a list, one entry per signal, each with a `type` and type-specific options. Default signals are zero-cost (SparkListener on Spark; a metadata-only relation/schema read on DuckDB; see `row_count_estimate` below for the one place the two engines' cost model genuinely diverges). Sample-based signals (`null_rates`, `value_distribution`, `distinct_count`, `data_freshness`) require explicit opt-in via `danger.allow_full_probe_actions`. `execution_partitions` is Spark-only: DuckDB is single-process with no partition concept to report. Each built-in signal type is its own capability leaf (`probe.signal.<type>`, alongside `channel.op.*`/`egress.mode.*`), so a Blueprint using `execution_partitions` on DuckDB is refused at compile time (a clean `CompileError`, same as any other unsupported leaf) rather than silently degrading; the engine's dedicated runtime warning stays as a backstop for a programmatic caller that reaches the executor directly, bypassing the compile-time gate. A Probe is not a data-flow node, an `edges:` entry with a Probe as `from:` on any port other than `signal` is a `CompileError` (Probes are excluded from the executor's topo-sort node set, so such an edge has no module to route data to).

**`row_count_estimate` is EXACT on DuckDB, not an estimate.** Spark's version samples to dodge an expensive distributed action. That rationale does not transfer to a single-node engine: DuckDB reads the row count straight from a parquet file's footer metadata (`parquet_file_metadata()`, zero rows scanned) when the Probe attaches directly to a `format: parquet` Ingress, or runs a plain `COUNT(*)` otherwise; both measured sub-millisecond even on a 1,000,000-row file. Neither path is gated by `danger.allow_full_probe_actions` on this engine. The signal's `method`/`fraction` keys are accepted for Blueprint parity but not consulted.

**`report: stdout`.** Per-Probe opt-in terminal output: each signal's result also prints under the Probe's row in the post-run summary, dim `↳` lines, single-value payloads on one line, dict/tabular payloads one line per entry, the block capped at 10 lines unless `-v`. Purely additive: every signal is still persisted to `probe_signals` exactly as without it, and the printed lines are informational notes, never counted in the runtime warning roll-up. Sampling governance and `danger.allow_full_probe_actions` gating apply unchanged, `report` changes where results are shown, not what is collected.

**Sampling governance.** The `probes:` block in `aqueduct.yml` controls how much data sample-based signals read:

| Key | Default | Role | Effect |
|---|---|---|---|
| `max_sample_rows` | `100` | **Cap** | Ceiling on `sample_rows` `n`: a per-probe `n:` above the cap is clamped; below the cap is honoured. |
| `default_sample_fraction` | `0.1` | **Default** | Fleet-wide default for signals that use `fraction` (`null_rates`, `value_distribution`, `distinct_count`, `data_freshness`, `row_count_estimate: sample`). A per-probe `fraction:` in the Blueprint overrides this. |

These sit alongside `danger.allow_full_probe_actions` (whether full actions are allowed at all) and `metrics.use_observe` (observe overhead) as the three-part probe-cost-governance family.

**Custom signals (`type: custom`).** User-defined signals extend observability without forking the engine. Exactly one of three forms:

```yaml
signals:
  - type: custom
    sql: "percentile(amount, 0.99)"   # inline SQL → "estimate" (a Spark expression)
    passed_when: "MAX(amount) < 1e6"  # optional boolean → "passed" (Regulator gate, like threshold)
  - type: custom
    module: myorg.aq_probes           # importable module + callable (mirrors the UDF pointer contract)
    entry: p99_latency
  - type: custom
    plugin: p99_latency               # setuptools entry-point group "aqueduct.probe_signals"
```

The callable forms resolve to `fn(df, sig_cfg) -> {"estimate", "metadata", "passed"}` (`fn(rel, sig_cfg)` on DuckDB: same contract, engine-native object). Like all signals the payload lands in `probe_signals` (`signal_type = custom`); a `passed` verdict is read by a downstream Regulator exactly like `threshold`. **The blueprint only carries a pointer, never an inline code body** (same rule as UDFs), so custom code stays in a packaged, importable module and is never surfaced to the healing LLM. Callables run as trusted code on the process that owns the DataFrame/relation (the Spark driver; DuckDB's single process): the engine cannot enforce zero-cost observability for them, so a callable that materializes the full dataset (`.collect()`/`.count()` on Spark, `.fetchall()`/`.df()` on DuckDB) is the author's cost to own; the compiler emits an engine-neutral `custom_probe_driver_code` warning for pointer/plugin signals on either engine (inline SQL is exempt). The `module:` pointer resolves against the Manifest's `base_dir` before falling back to a normal import, see **§3, `base_dir`**.

**Inline-SQL form: `sql` vs `passed_when`:** the two keys play different roles, so they are named differently. `sql` computes a **scalar metric** (any single-value SQL aggregate over the probed DataFrame/relation) and stores it as `estimate` for trending (`report --trend`/`--profile`). `passed_when` is an **optional boolean** that becomes the `passed` gate verdict (like `threshold`). Provide either or both:
- **`sql` only** → record-only: captures the metric every run, never gates (a Regulator reading it stays open, an absent `passed` key is treated as open).
- **`passed_when` only** → gate-only: one action (one Spark action; one DuckDB query), no recorded metric.
- **both** → records *and* gates; note these are **two separate actions/queries**, so the aggregate is scanned twice.

Each is evaluated verbatim as a SQL expression (`df.selectExpr(...)` on Spark; a transpiled aggregate query on DuckDB; a Blueprint authored against Spark's SQL dialect runs unmodified on either engine), so any single-scalar expression works (`percentile`, `approx_count_distinct`, `SUM(CASE WHEN …)/COUNT(*)`, etc.). For multi-value output, cross-table joins, or non-SQL logic, use the callable form. **Avoid duplicating a shared subquery** across `sql` and `passed_when` with a macro, macros expand inside probe config at compile time:

```yaml
macros:
  error_rate: "SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) / COUNT(*)"
signals:
  - type: custom
    sql: "{{ macros.error_rate }}"
    passed_when: "{{ macros.error_rate }} < 0.01"
```
(Macros dedupe the authored text, not the two runtime scans.)

See the [Observability Guide](../observability_guide.md) for full signal reference and cost model.

### Regulator

```yaml
- id: quality_gate
  type: Regulator
  config:
    on_block: skip                 # skip | abort | trigger_agent
```

Regulators are passive: they compile away entirely if no signal edge is wired to them.

| Config field | Description |
| :- | :- |
| **on_block** | Action when the wired signal is not `passed`: `skip` (default; downstream modules are skipped), `abort`, `trigger_agent`. |
| **timeout_seconds** | Optional. Maximum time to poll a not-yet-available signal before giving up (default `0`: no polling wait). |
| **poll_seconds** | Optional. Polling interval while waiting on `timeout_seconds` (default `30.0`, floored at `0.5`). |

### Arcade (Sub-pipeline)

```yaml
- id: process_region
  type: Arcade
  ref: arcades/region_processor.yml   # module-level field, NOT inside config
  context_override:                   # module-level field, NOT inside config
    env: ${ctx.env}
    data_dir: "/data/regions/${ctx.region}"
```

`ref` and `context_override` are MODULE-level fields, siblings of `config:`; NOT config keys (same shape as Probe's `attach_to` and Channel's `materialize`; Arcade has no legal `config:` keys at all). Arcades are expanded at compile time into a flat module list. Module IDs are namespaced (`{arcade_id}__{child_id}`). Blueprint module IDs must not contain `__` (reserved for Arcade expansion).

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
      - type: not_null
        column: order_id
        on_fail: quarantine   # routes null rows to spillway; needs spillway edge
      - type: sql_row
        expr: "amount > 0 AND order_id IS NOT NULL"
        min_pass_rate: 0.99   # optional — additionally fail if the pass rate drops below this
        on_fail: quarantine
      - type: custom
        fn: my_rules.check_completed_max   # importable module.callable — see below
        on_fail: quarantine
```

Assert rules are batched into 1-2 Spark actions (on DuckDB: one `rel.aggregate()` query plus one sampled query for `null_rate`). Rule types: `schema_match` (zero action), `not_null`, `min_rows`, `max_rows`, `null_rate`, `freshness`, `sql`, `sql_row`, `spillway_rate`, `custom`. Every rule accepts an optional `id:`; a human-readable label carried through for authoring clarity; no rule-type handler reads it.

**`schema_match`'s `expected` types resolve through the type hub (§9), with the same numeric widening `schema_hint` uses** (§4.4 Ingress): `order_id: int` validates against an engine-inferred `bigint` column, since a narrower expectation is satisfied by an actual column at least as wide in the same fixed-width family; see the Ingress `schema_hint` note above for the full reasoning and the one-directional caveat (an expectation wider than the actual type still fails).

**`sql_row`'s `min_pass_rate`** (optional) additionally fails the rule when the fraction of rows satisfying `expr` drops below the given threshold: one extra aggregate action (`count(*)` + `count_if(expr)`) beyond the row-level filter itself.

**`type: custom`** points `fn:` at an importable `module.callable`, `fn(df) -> {"passed": bool, "message"?: str, "quarantine_df"?: DataFrame}`. Same pointer-only rule as UDFs/custom probes: no inline code body. `fn`'s module resolves against the Manifest's `base_dir` first (a sibling `.py` file next to the Blueprint, see **§3, `base_dir`**), falling back to a normal import.

**A `custom` rule that cannot be evaluated is a failure of that rule, on both engines.** Two situations (no `fn:` configured, or `fn(df)` itself raising (a bug, a bad import, an API from the wrong engine)) are routed through the rule's own `on_fail`, exactly like a rule that evaluated and failed: `abort` aborts, `warn` warns and continues, `webhook` fires the webhook, `trigger_agent` defers to the healing loop, and `quarantine` falls back to the same "aggregate rule, no row filter available" warn behavior a genuinely-failed `custom` rule with no `quarantine_df` already gets (there is nothing to quarantine when the rule never ran). A quality gate whose own code is broken must not silently let the data through.

#### Quarantine eligibility

`on_fail: quarantine` routes failing rows to a spillway edge.  A rule is quarantine-able **iff it clears three gates**:

| Gate | Requirement | Why |
|------|-------------|-----|
| 1. Logical | Failure is per-row attributable: ∃ boolean predicate `P(row)` with `bad ⟺ P(row)` | Quarantine splits rows; aggregate rules have no per-row split |
| 2. Semantic | Removing `P`-rows makes the rule pass AND serves its intent (a per-row contract) | A population-gate breach IS the signal; quarantining nulls when `null_rate` trips masks what you're measuring |
| 3. Performance | `P` is already computed in a row-wise pass | No extra Spark action: the zero-cost-observability rule |

**Verdict table:**

| Rule | Quarantine? | Why blocked |
|------|------------|-------------|
| `not_null` | ✅ | Per-row `col IS NULL`; the rule's contract; row-wise pass |
| `sql_row` | ✅ | Per-row SQL expression; semantic contract; row-wise pass |
| `custom` | ✅ | User-supplied predicate; any contract; row-wise pass |
| `freshness` | ✅ | Per-row `col >= cutoff`; freshness contract; row-wise pass |
| `null_rate` | ❌ | Gate 1 passes but Gate 2: population-gate, quarantining all nulls at 25% > 20% masks the signal. Gate 3: today it uses `df.sample().agg()`, not a full scan, deriving quarantine would force a full scan + filter/split. For per-row null filtering use `not_null`. |
| `min_rows` | ❌ | Gate 1: aggregate: no per-row `P(row)` exists |
| `max_rows` | ❌ | Gate 1: aggregate |
| `sql` | ❌ | Gate 1: aggregate |
| `spillway_rate` | ❌ | Not a row rule: it measures the quarantine rate itself |
| `schema_match` | ❌ | Gate 1: metadata check, not row-level |

`not_null` and `freshness` additionally require a `spillway` edge when `on_fail: quarantine` (compiler-enforced), same as `sql_row` and `custom`.

---

