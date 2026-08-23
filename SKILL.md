---
name: aqueduct-blueprint-authoring
description: >
  Author Aqueduct Blueprints (declarative Spark pipeline YAML) and the engine
  config (aqueduct.yml). Use when asked to write, edit, fix, or review an
  Aqueduct Blueprint, wire modules/edges, add Channels/Asserts/Probes, configure
  stores/targets, or set up the self-healing LLM agent. The authoring loop is:
  write YAML → `aqueduct validate` / `aqueduct lint` → fix → repeat. No server
  needed; validation is a local CLI call.
---

# Aqueduct Blueprint Authoring

Aqueduct runs **declarative Spark pipelines** ("Blueprints" — YAML) and
self-heals them with an LLM on failure. You write *what* the pipeline does; the
engine compiles it to a Manifest and executes it on Spark. This guide teaches an
LLM to **author** Blueprints. The full reference is `docs/specs.md`; this is the
distilled, token-efficient subset.

## The authoring loop (no server)

1. Write the Blueprint YAML.
2. `aqueduct validate blueprint.yml` — static schema + graph check (no Spark, no run).
3. `aqueduct lint blueprint.yml` — style/anti-pattern warnings.
4. Fix and repeat until clean. Optionally `aqueduct run blueprint.yml --sandbox` for a dry execution.

**Hard rule:** unknown fields at ANY level are errors (`extra="forbid"`). This is
deliberate — it makes every Blueprint valid input for the healing LLM. If
validate rejects a field, the field name is wrong, not optional.

## Top-level structure

```yaml
aqueduct: "1.0"                       # schema version — REQUIRED
id: pipeline.orders.daily             # globally unique pipeline id — REQUIRED
name: "Daily Orders"                  # display name — REQUIRED
description: |                        # optional, fed to the healing LLM + UI
  Reads raw orders, dedups, aggregates by region, writes Delta.

context:                              # optional — the Context Registry (see below)
  env: ${AQUEDUCT_ENV:-dev}
  tables:
    orders_raw: "s3://data/${ctx.env}/orders/raw"
    orders_out: "s3://data/${ctx.env}/orders/daily"

modules:                              # REQUIRED — the module list
  - id: read_orders
    type: Ingress
    label: "Read raw orders"
    config: { format: parquet, path: "${ctx.tables.orders_raw}" }
  # ...

edges:                                # optional (see linear-edge sugar)
  - { from: read_orders, to: dedup, port: main }

engine:                                # optional — per-engine settings, namespaced by name
  spark:
    conf:                             # merged with engine-level engine.spark.conf
      spark.sql.shuffle.partitions: 200
  duckdb:                              # merged with engine-level engine.duckdb.*
    memory_limit: "8GB"                # resource/tuning knobs only (memory_limit/threads) —
    threads: 4                         # database_path/s3_*/extension_repository stay aqueduct.yml-only

retry_policy: { max_attempts: 3 }     # optional

warnings:                             # optional — per-blueprint compile-warning suppression
  suppress: [perf_python_udf_row_at_a_time]   # rule_ids from AQ-WARN output, or "*" for all

hooks:                                # optional — lifecycle actions / heal-milestone actions
  on_success:
    - blueprint: blueprints/next.yml  # chain another blueprint (fresh subprocess) — ungated
    - webhook: https://hooks.example  # url shorthand or {url, method, headers, payload} — ungated
    - command: "scripts/commit.sh ${run.id}"  # subprocess — NEEDS danger.allow_command_hooks
      timeout: 120                    # per-entry seconds (default 300)
  on_failure:
    - command: "scripts/cleanup.sh ${run.id}"
      when_error: ["EmptyDataset"]    # optional — only fire for this error_type
  on_patch_pending:                   # fires when a heal stages a patch for review
    - webhook: https://hooks.example/patch-review
  on_healed:                          # fires when a heal's re-run succeeds
    - blueprint: blueprints/notify_healed.yml
      in_process: true                # reuse this run's live SparkSession, no subprocess
```

**`warnings:` (1.2)** silences compile-time warnings (e.g.
`file_format_no_repartition`, `perf_python_udf_row_at_a_time`) for THIS
blueprint only — unioned with the engine-level `warnings.suppress` in
`aqueduct.yml`.
Compile-time only: never touches session/runtime warnings or other
blueprints. On an Arcade sub-blueprint, its own `warnings:` block parses fine
but is ignored — only the parent blueprint's suppress list applies to the
expanded compilation unit.

**`hooks:` (2.5)** — four events. `on_success`/`on_failure` run sequentially
after the run's terminal state; `on_patch_pending`/`on_healed` fire MID-RUN
at heal milestones (mirrors the engine-level `webhooks:` `on_patch_pending`/
`on_ci_patch` vocabulary). `on_healed` always fires before the outer run's
terminal `on_success` hooks. NEVER changes the exit code (a failing hook
warns + skips the event's remaining hooks). Exactly one of
`blueprint:`/`webhook:`/`command:` per entry. `command:` interpolates only
`${run.id}`/`${run.status}`/`${blueprint.id}` (shlex argv, no shell) and
requires `danger.allow_command_hooks: true` in aqueduct.yml — the blueprint
cannot self-authorize it. `blueprint:` entries may set `in_process: true` to
parse+compile+execute the target in-process, reusing the caller's live
SparkSession, instead of spawning an `aqueduct run` subprocess — falls back
to subprocess (with an info message) when the target sets its own
`engine.spark.conf`. `when_error: [ErrorType, ...]` on `on_failure`/
`on_patch_pending`/`on_healed` entries filters by the run's error_type /
stack-trace exception class (same matching as `agent.guardrails.
heal_on_errors`); unset = fires unconditionally; setting it on `on_success`
is a schema error (no failure context there). Chained `blueprint:` hooks are
cycle-guarded (`AQUEDUCT_HOOK_CHAIN` for subprocess mode, an explicit
in-memory chain for `in_process: true`; depth cap 8; `aqueduct doctor` checks
the chain statically). Engine-level `webhooks:` in aqueduct.yml stays
separate — ops-owned alerting that fires regardless of blueprint hooks. On an
Arcade sub-blueprint, `hooks:` is ignored — only the top-level blueprint's fire.

**`healed_by:`** — machine-written only, never hand-authored. `aqueduct patch
apply` appends one provenance record per applied self-heal patch (engine,
classification, applied_at, validated_on, `engine_config_delta` when the patch
changed effective engine/session config, and a warn-only `perf_baseline` /
`perf_observations` pair reporting how the pipeline's wall-clock duration
compares to the last green run before the patch — reported, never judged:
Aqueduct sets no regression threshold). Purely compiler-consumed metadata
— no engine executes it, and it never affects the compiled Manifest. See
docs/specs.md §8.14 for the cross-engine heal-patch gate it feeds.
`aqueduct patch revert <patch_id>` undoes one record's engine-config writes
and stamps it `reverted_at:` — the record is kept, and every consumer of the
block (the cross-engine gate, the green-run `validated_on` /
`perf_observations` stamps) skips a reverted one.

**Linear-edge sugar:** omit `edges:` entirely and the compiler chains modules in
declaration order — BUT only if every module is single-in/single-out (Ingress,
Channel, Egress, Assert). The moment you use a Junction (fan-out), Funnel
(fan-in), Arcade, Probe, or Regulator, you MUST declare `edges:` (those ports are
ambiguous in a flat chain). A single-module Blueprint needs no edges.

## Module common fields

Every module: `id` (required, unique, fs-safe, **no `__`** — reserved for Arcade
expansion), `label` (REQUIRED — human name), `type` (required), `config`
(type-specific). Optional: `description`, `tags`, `spillway` (downstream id for
error rows — sugar for `{from: this, to: <id>, port: spillway}`; same edge, either
authoring form, never both to a different target), `depends_on` (explicit upstream
list), `checkpoint` (bool, for `--resume`),
`enabled` (bool, default true; takes `${ctx.*}` so profiles can toggle it — a disabled
module is skipped ⏭ at run time and the disable cascades to every downstream consumer),
`retry` (2.8 — per-module override of the top-level `retry_policy:`; see below),
`engine` (2.34 — cross-engine handoff; see below).

**`engine:` (2.34)** — a scalar engine NAME (`spark`, `duckdb`) picking which
engine runs THIS module. NOT the blueprint-level `engine:` BLOCK (per-engine
session settings, namespaced by name — same word, different level, see the
Top-level structure section above). Four rules, in order: (1) an explicit
`engine:` on the module wins; (2) unset → inherit the SINGLE upstream
parent's resolved engine (a Probe's `attach_to` target counts as its
parent); (3) unset + multiple parents on DIFFERING engines → `CompileError`
demanding an explicit pin; (4) unset + no parents (an Ingress) →
`deployment.engine`. A Blueprint with no `engine:` field anywhere is fully
portable. The compiler partitions the module graph into engine islands at
the boundaries this creates (derived, never declared) and gates each island
against its OWN engine's capability table. Probe/Assert must colocate with
their target's island (`CompileError` on a mismatched pin); a spillway edge
may not cross islands (v1). See docs/specs.md §4.3.

**When to pin an `engine:` at all (docs/specs.md §11.4).** A boundary edge
costs a full materialise-to-parquet plus a re-read on the other side —
roughly the order of a shuffle, paid on every run whether or not the split
was worth it. Don't pin for speed: if a stage can run in the engine you're
already in, same-engine is normally faster. Pin only for one of three
reasons, none of them performance: (1) **capability** — the other engine has
a format/function/extension this one lacks and there's no way around it; (2)
**scale mismatch** — a large reduce on Spark, then a small result finished
where Spark's per-task overhead stops paying for itself; (3) **incremental
migration** — moving a pipeline engine-by-engine instead of rewriting it in
one shot. Every boundary this creates emits a compile-time
`cross_engine_handoff_io` warning naming it before the run — if you're
looking at that warning wondering whether your split is justified, this is
the answer; if you're adding an `engine:` pin, expect that warning to
appear. Zero `engine:` fields anywhere means a fully portable Blueprint; add
one and you've declared a dependency on that engine. One hard ceiling if the
receiving island is DuckDB: it opens a bare `:memory:` connection with no
persistent-file option, so its capacity is bounded by RAM, not disk.

**`retry:` (2.8)** overrides the blueprint's `retry_policy:` **per field** — any
field left unset inherits the blueprint value (same shape as `agent.cascade`
tier inheritance):

```yaml
retry_policy: { max_attempts: 3, on_exhaustion: trigger_agent }
modules:
  - id: flaky_source
    type: Ingress
    label: "Flaky source"
    config: { format: jdbc, ... }
    retry: { max_attempts: 6 }   # on_exhaustion inherits "trigger_agent"
```

Fields: `max_attempts`, `backoff` (whole-block override — set every sub-field
or omit the block, never merges field-by-field), `transient_errors`,
`non_transient_errors`, `on_exhaustion`, `deadline_seconds`.

> The single most common authoring error: **forgetting `label:`**. It is required on every module.

## Module types (9)

`Ingress | Channel | Egress | Junction | Funnel | Probe | Regulator | Arcade | Assert`

`Handoff` is a 10th type name that exists ONLY as compiler-synthesized output (2.35, cross-engine handoff — see `engine:` above) — the compiler inserts one at each engine-boundary edge in a polyglot Blueprint. `type: Handoff` is never legal in authored YAML; `aqueduct validate` rejects it by name.

### Ingress — read data
```yaml
- id: read_orders
  type: Ingress
  label: "Read orders"
  config:
    format: parquet        # parquet|delta|iceberg|hudi|csv|json|orc|avro|jdbc|kafka|custom
    path: "${ctx.tables.orders_raw}"
    # OR address an external-catalog table (mutually exclusive with path):
    # table: "catalog.schema.orders"   # spark.read.table(); catalog wired in engine.spark.conf
    schema_hint: { order_id: STRING, amount: "DECIMAL(18,2)" }   # optional
    partition_filters: "event_date >= '${ctx.start_date}'"        # optional
    on_new_columns: fail   # allow(default)|fail|alert — schema-drift contract
    options: { mergeSchema: true }
```
- `path:` and `table:` are **mutually exclusive** (engine errors if both). `format:` not required with `table:`.
- Credentials are NEVER per-Ingress — they live in `engine.spark.conf:` (Hadoop/Spark keys), values may use `@aq.secret('KEY')` / `${ENV}`.
- `time_travel: {version: N}` or `{timestamp: "..."}` only with `path:` (Delta/Iceberg). For `table:`, use a Channel with `TIMESTAMP AS OF` SQL.

### Channel — transform
```yaml
- id: dedup
  type: Channel
  label: "Dedup by order_id"
  config:
    op: deduplicate        # sql|deduplicate|filter|select|rename|cast|join|union|sort|repartition|coalesce|cache
    key: order_id
    order_by: "event_ts DESC"
```
SQL form (upstream module ids are temp views; single-input upstream is also `__input__`):
```yaml
- id: clean
  type: Channel
  label: "Cast + clean"
  config:
    op: sql
    # clean_phone is a udf_registry entry (see UDF Registry) — called by
    # name directly in SQL; no per-Channel list, it's registered session-wide.
    query: |
      SELECT CAST(amount AS DECIMAL(18,2)) AS amount, clean_phone(phone) AS phone
      FROM dedup
    spillway_condition: "amount IS NULL"   # optional — matching rows go to spillway port
```
Most ops are lazy (no Spark action) except `cache`. `join` is sugar over SQL JOIN.
Incremental: `materialize: incremental` + `watermark_column: <col>` (needs a Depot) — both are
MODULE-level fields, siblings of `config:`, NOT config keys:
```yaml
- id: new_events
  type: Channel
  materialize: incremental        # module-level, NOT inside config
  watermark_column: event_ts      # module-level, NOT inside config
  config:
    op: sql
    query: "SELECT * FROM events WHERE event_ts > CAST(${ctx._watermark} AS TIMESTAMP)"
```

### Egress — write data
```yaml
- id: save
  type: Egress
  label: "Save Delta"
  config:
    format: delta          # +iceberg|hudi|parquet|csv|json|orc|avro|jdbc; pseudo: depot
    mode: overwrite        # overwrite|append|error(default)|ignore|merge|overwrite_partitions
    path: "${ctx.tables.orders_out}"      # OR table: "catalog.schema.t" (mutually exclusive)
    partition_by: [event_date, region]
    merge_key: order_id    # required for mode: merge (Delta MERGE INTO)
    options: { compression: snappy }
```
- `mode: merge` needs `merge_key` + Delta. `mode: overwrite_partitions` is the idempotent-backfill primitive: with `replace_where: "event_date='@aq.date.today()'"` (Delta) OR dynamic mode (requires `partition_by:`).
- Schema drift on write: `on_new_columns: allow|fail|alert`, `merge_schema: true`, `overwrite_schema: true`.

### Junction — fan-out (REQUIRES explicit edges per branch)
```yaml
- id: split
  type: Junction
  label: "Split by value"
  config:
    mode: conditional      # conditional|broadcast|partition
    branches:
      - { id: high, condition: "amount > 1000" }
      - { id: low,  condition: "amount <= 1000" }
      # - { id: other, condition: "_else_" }         # catches unmatched rows
```
Downstream edges use `port: high` / `port: low`. `mode: partition` needs a top-level `partition_key:` (column name) plus each branch's `value:` (defaults to the branch `id`) — rows route where `partition_key = value`.

### Funnel — fan-in
```yaml
- id: merge_all
  type: Funnel
  label: "Union"
  config: { mode: union_all, inputs: [a, b] }   # mode: union_all|union|coalesce|zip; inputs required, >=2
```
`schema_check: strict|permissive` (default strict) — `union_all`/`union` only.

### Assert — data-quality gate
```yaml
- id: gate
  type: Assert
  label: "Quality gate"
  config:
    rules:
      - { type: schema_match, expected: {order_id: STRING, amount: "DECIMAL(18,4)"}, on_fail: abort }
      - { type: min_rows, min: 1000, on_fail: abort }
      - { type: null_rate, column: order_id, max: 0.0, on_fail: abort }
      - { type: freshness, column: order_ts, max_age_hours: 26, on_fail: webhook }
      - { type: sql_row, expr: "amount > 0", on_fail: quarantine }   # bad rows → spillway
      - { type: not_null, column: order_id, on_fail: quarantine }  # per-row null → spillway (not population gate)
```
Rule types: `schema_match | not_null | min_rows | max_rows | null_rate | freshness | sql | sql_row | spillway_rate | custom`. `on_fail`: `abort | warn | webhook | quarantine`.  Quarantine-eligible rule types: `not_null`, `sql_row`, `custom`, `freshness` (per-row predicates).  The rest are aggregate / population gates — quarantining is rejected at compile time with a clear pointer. `type: custom` → `fn: module.callable`, `fn(df) -> {"passed": bool, ...}`, pointer-only (no inline body); resolves against a sibling `.py` file next to the blueprint before falling back to a normal import (same rule as UDFs/probes/custom DataSource below).

### Probe — non-blocking observability tap
```yaml
- id: probe
  type: Probe
  attach_to: dedup          # MODULE-LEVEL field, NOT inside config
  config:
    report: stdout          # optional — also print results under the module in the run summary
    signals:
      - { type: schema_snapshot }   # zero-cost (SparkListener)
      - { type: row_count_estimate }
```
Signal types: `schema_snapshot | row_count_estimate | null_rates | sample_rows | value_distribution | distinct_count | data_freshness | execution_partitions | threshold | custom`. Runs on both engines except `execution_partitions` (Spark-only — no partition concept on DuckDB). Sample-based signals (`null_rates`, `value_distribution`, `distinct_count`, `data_freshness`) need `danger.allow_full_probe_actions: true` in `aqueduct.yml` on either engine. `row_count_estimate` is sampled on Spark but an EXACT count on DuckDB (parquet footer or `COUNT(*)`, never gated by `allow_full_probe_actions` there). Probes attach by `attach_to`, not edges — a `from:` edge off a Probe on any port but `signal` is a `CompileError`. `type: custom` → `module:`+`entry:` pointer (mirrors the UDF contract) resolves against a sibling `.py` file next to the blueprint before falling back to a normal import; never inline code.

### Regulator — gate driven by a Probe `signal` edge
```yaml
- id: gate
  type: Regulator
  label: "Hold on bad signal"
  config: { on_block: skip }   # skip|abort|trigger_agent
```
Wire a Probe's `signal` port to the Regulator via edges. Regulators with no signal edge compile away.

### Arcade — reusable sub-pipeline
```yaml
- id: process_region
  type: Arcade
  label: "Region processor"
  ref: arcades/region_processor.yml          # module-level field, NOT inside config
  context_override: { region: "${ctx.region}" }   # module-level field, NOT inside config
```
Expanded at compile time; child ids namespaced `arcade_id__child_id`. Arcade has no legal `config:` keys at all — `ref`/`context_override` are siblings of `config:`, same shape as Probe's `attach_to`.

## Edges & ports
```yaml
edges:
  - { from: a, to: b, port: main }            # main (default) | spillway | signal | <branch_id>
  - from: gate
    to: quarantine_sink
    port: spillway
    error_types: [DataQualityViolation]       # optional typed catch — only these error types
```
Ports: `main` (default DataFrame), `spillway` (error rows — from Channel/Assert), `signal` (Probe→Regulator), `<branch_id>` (Junction branch). Spillway rows carry `_aq_error_module/_type/_msg/_ts`.

## Context Registry (3 tiers)
- **Tier 0 static** `${ctx.ns.key}` — substituted at parse time. Define under `context:`. Override order: CLI `--ctx k=v` > `AQUEDUCT_CTX_*` env > `context_profiles` (`--profile`) > `context:` defaults. Env interpolation: `${ENV_VAR:-default}`.
- **Tier 1 runtime** `@aq.fn(...)` — resolved pre-job on the driver: `@aq.date.today()/yesterday()/offset(base,days)/month_start()/format(s,p)`, `@aq.run.id()/timestamp()/prev_run_id()`, `@aq.env('K')`, `@aq.secret('K')`, `@aq.depot.get('k')` (or `@aq.depot.<name>.get('k')` for a named mount), `@aq.blueprint.id()/name()/dir()/path()`, `@aq.deployment.env()/target()/engine()`, `@aq.version()`. Use `@aq.blueprint.dir()` (not cwd) as the pipeline-relative path anchor.
- **Tier 2 UDFs** — distributed column functions (below).

`context_profiles:` promote envs: `dev: { tables.orders_raw: "s3://dev/..." }`.

## UDF Registry (pointers, never inline code)
```yaml
udf_registry:
  - id: clean_phone
    module: my_project.udfs     # importable (on PYTHONPATH)
    entry: clean_phone          # defaults to id
    return_type: STRING
  - id: mask_pii                # parameterized: entry is a factory entry(**params)->callable
    module: my_project.udfs
    entry: make_masker
    return_type: STRING
    params: { keep_last: 4, salt: "@aq.secret('PII_SALT')" }
  - id: geohash                 # JVM
    lang: java                  # python(default)|java|scala
    jar: libs/geo.jar
    class: com.example.GeoHashUDF
    return_type: STRING
```
Call a registered UDF by name directly in Channel SQL (e.g. `clean_phone(phone)`) — every `udf_registry` entry registers session-wide, so any Channel's SQL may call any of them; there is no per-Channel scoping key. **Bodies are never inline** — always a module/jar pointer (so the healing LLM never sees code). Python `module:` resolves against a sibling `.py` file next to the blueprint before falling back to a normal import/PYTHONPATH lookup — same rule applies to Assert `custom` `fn:`, Probe `custom` `module:`, and `format: custom` DataSource `class:`.

## Dependencies (compile-time preflight, never an installer)
```yaml
dependencies:
  - holidays>=0.40
  - geopy[extra]>=2.3,<3
```
Flat list of PEP 508-lite requirement strings (`name`, `name>=1.2`, `name[extra]>=1.2,<2`; environment markers like `; python_version < "3.12"` are rejected, not silently ignored). Top-level, sibling of `udf_registry:`, not engine-scoped, no allowlist. Checked against the installed environment at compile time via `importlib.metadata` — missing or version-conflicting requirements raise `DependencyError` naming the failing requirements and a copy-pasteable `pip install` command. **Aqueduct never installs anything** — this block only declares what must already be true of the runtime.

## Type spellings (Ingress `schema_hint`, Channel `op: cast`, UDF `return_type`)
Every column-type string is Aqueduct's own hub vocabulary (Arrow-borrowed semantics), not a raw engine DDL string — validated at compile time, not runtime.

| `boolean` | `tinyint` | `smallint` | `int` | `bigint` | `float` | `double` |
|---|---|---|---|---|---|---|
| `string` | `binary` | `date` | `decimal(p,s)` | `timestamp_tz` | `timestamp_ntz` | `duration(unit)` |

Composites: `array<T>`, `map<K,V>`, `struct<name:type,...>` (nest freely: `array<map<string,int>>`). Familiar aliases canonicalize silently (`long`→`bigint`, `integer`→`int`, `varchar`/`char`→`string`).

`duration(unit)` (`unit` one of `s`/`ms`/`us`/`ns`) is a span of time, integer-backed — it renders as a plain `bigint`/`BIGINT` on every engine, never either engine's native `INTERVAL` type, so it round-trips across a cross-engine handoff with no ambiguity. Use it instead of a native interval spelling unless you specifically need that engine's own calendar-arithmetic semantics.

**Bare `timestamp` is REJECTED at compile time — write `timestamp_tz` (instant) or `timestamp_ntz` (naive wall-clock) explicitly.** There is no deprecation window and no suppress: Spark's `timestamp` is an instant, DuckDB's `TIMESTAMP` is naive, so the bare spelling means a different value per engine — it never parses.

**Native escape hatch:** `<engine>:<spelling>` (e.g. `duckdb:HUGEINT`, `spark:interval day to second`) names a type in one engine's own vocabulary directly, for spellings the hub has no equivalent for. It is capability-gated per engine — `duckdb:HUGEINT` in a Blueprint compiled for `spark` is a compile-time error, not a runtime crash. Prefer a portable hub spelling whenever one exists; reach for the native hatch only when the Blueprint is intentionally single-engine.

```yaml
schema_hint: { order_id: bigint, amount: "decimal(18,2)", placed_at: timestamp_tz }
# ...
- id: cast_amount
  type: Channel
  config: { op: cast, columns: { amount: "decimal(18,2)", tags: "array<string>" } }
```

## Macros (compile-time text dedup)
```yaml
macros:
  error_rate: "SUM(CASE WHEN status='error' THEN 1 ELSE 0 END)/COUNT(*)"
# use as {{ macros.error_rate }} inside SQL / probe config
```

## Self-healing agent (per-Blueprint POLICY — connection lives in aqueduct.yml only)
```yaml
agent:
  approval: auto              # disabled|human|auto|ci  (controls if/how patches apply)
  on_pending_patches: warn    # ignore|warn|block
  max_patches: 1              # >1 = multi-patch loop (also needs danger.allow_multi_patch / --allow-multi-patch)
  prompt_context: "Amounts are cents; never cast to INT."   # author hints to the healer
  guardrails:
    allowed_paths: ["s3a://my-bucket/**"]
    deny_patterns: ["s3a://my-bucket/prod/**"]   # evaluated AFTER allowed_paths; subtract-only
    forbidden_ops: [remove_module, insert_module]
  sandbox_mode: sample        # sample|preflight|off — how patches are pre-validated
  mode: oneshot                # oneshot (default) | agentic — agentic lets the model call read-only diagnostic tools before answering
  max_tool_calls: 8            # agentic mode only — hard cap on tool calls per heal attempt
  supports_tools: auto         # auto|true|false — tool-use capability; anthropic resolves true w/o probing, openai_compat probes
  progressive: false           # true = chained multi-patch healing — a candidate that fixes bug #1 but leaves bug #2 failing
                                # folds into an accumulating patch instead of being discarded; requires sandbox_mode != off
  max_chain: 3                 # hard cap on links in a progressive chain (independent of max_reprompts)
```
`approval` values: `disabled` (never heal) · `human` (stage patch for review) · `auto` (apply validated patch; with `max_patches > 1` enables the multi-patch loop) · `ci` (stage + webhook). `mode`/`max_tool_calls`/`supports_tools`/`progressive`/`max_chain`/`prompt_context` (above) override the `aqueduct.yml`-level default when set; unset inherits it. **CONNECTION fields — `provider`, `base_url`, `model`, `api_key`, `provider_options`, `timeout`, `cascade` — are NOT legal in a Blueprint's `agent:` block.** `extra="forbid"` rejects one by name if you write it here; they configure ONLY in `aqueduct.yml` (next section) — a Blueprint cannot choose its own LLM endpoint, since the healing loop ships `FailureContext` (pruned manifest, provenance, error text, and, in agentic mode, sampled data rows) to whatever endpoint is configured, and letting a pipeline author redirect that is a data-exfiltration hole, not a convenience. `progressive: true` (`agent.approval: auto`, non-cascade path) chains multi-patch healing across DIFFERENT-module failures instead of re-diagnosing the same first bug every attempt — see `docs/specs.md` §8.13; `max_patches` semantics are unchanged.

## Engine config (`aqueduct.yml`) — NOT the Blueprint
Separate file. Configures deployment target, Spark, stores, secrets, webhooks, and the agent's connection settings. Author it only when asked; Blueprints reference its results. Key blocks: `deployment` (engine/target/env), `engine` (per-engine settings namespaced by name — `engine.spark.master_url`, `engine.spark.conf`, `engine.duckdb`), `stores` (observability/depots/blob/benchmark + backend), `agent` (`provider`/`base_url`/`model`/`api_key`/`provider_options`/`timeout`/`cascade` — CONNECTION, engine-only; plus the same policy defaults the Blueprint can override), `danger` (allow_multi_patch, allow_full_probe_actions), `handoff` (`root` — cross-engine spill location, any engine-reachable URI, default `.aqueduct/handoff`; `keep_on_failure` — keep a boundary's spill after a failed run so a rerun skips recomputing it, default true), `timezone` (top-level, e.g. `"UTC"` — applied to EVERY engine's session at creation; only worth setting once a Blueprint spans more than one engine — an explicit `engine.spark.conf.spark.sql.session.timeZone` override still wins for Spark, with a warning naming the divergence). `agent.cascade` is engine-only — a Blueprint cannot declare or override a cascade (no `model: [list]` shorthand either; both were removed as Blueprint features — write an explicit list of tiers here). See `aqueduct/templates/default/aqueduct.yml.template`.

## Path resolution
Relative paths in a Blueprint anchor to **the Blueprint file's directory**, never the cwd (portable across run locations). `s3://`, `postgresql://`, absolute paths pass through unchanged. Same rule for `aqueduct.yml` (anchors to the config file dir).

## Common gotchas (author checklist)
- `label:` on every module (required).
- Unknown field = hard error. Check spelling against this guide / `aqueduct validate`.
- `table:` ⊻ `path:` (never both) on Ingress/Egress.
- Junction/Funnel/Arcade/Probe/Regulator ⇒ explicit `edges:` (no linear sugar).
- Probe `attach_to` is module-level, not in `config`.
- Module ids: no `__` (Arcade reserved).
- `mode: merge` needs `merge_key`; `overwrite_partitions` (dynamic) needs `partition_by`.
- Sample-based Probe signals need `danger.allow_full_probe_actions`.
- UDF/custom-probe/custom-datasource = importable pointer, never inline code.
- Cloud creds go in `engine.spark.conf`, not in modules.

## Worked example (end to end)
```yaml
aqueduct: "1.0"
id: pipeline.orders.daily
name: "Daily Orders"
context:
  tables: { raw: "s3a://bkt/orders/raw", out: "s3a://bkt/orders/daily" }
modules:
  - { id: read, type: Ingress, label: "Read", config: { format: parquet, path: "${ctx.tables.raw}" } }
  - { id: dedup, type: Channel, label: "Dedup", config: { op: deduplicate, key: order_id, order_by: "ts DESC" } }
  - id: gate
    type: Assert
    label: "Quality"
    config:
      rules:
        - { type: min_rows, min: 1, on_fail: abort }
        - { type: sql_row, expr: "amount > 0", on_fail: quarantine }
  - { id: save, type: Egress, label: "Save", config: { format: delta, mode: overwrite, path: "${ctx.tables.out}", partition_by: [region] } }
  - { id: bad, type: Egress, label: "Quarantine", config: { format: delta, mode: append, path: "s3a://bkt/orders/bad" } }
edges:
  - { from: read, to: dedup }
  - { from: dedup, to: gate }
  - { from: gate, to: save }
  - { from: gate, to: bad, port: spillway }
```
Validate it: `aqueduct validate orders.yml && aqueduct lint orders.yml`.

## LLM provider base_urls (OpenAI-compatible)
Aqueduct talks to **Anthropic natively** OR **any OpenAI-compatible endpoint**
(`provider: openai_compat` + `base_url`). Known-good base_urls:

| Provider | `provider` | `base_url` |
| :- | :- | :- |
| Anthropic (native) | `anthropic` | — (native Messages API) |
| OpenAI | `openai_compat` | `https://api.openai.com/v1` |
| OpenRouter | `openai_compat` | `https://openrouter.ai/api/v1` |
| DeepSeek | `openai_compat` | `https://api.deepseek.com/v1` |
| Groq | `openai_compat` | `https://api.groq.com/openai/v1` |
| Google (OpenAI-compat) | `openai_compat` | `https://generativelanguage.googleapis.com/v1beta/openai/` |
| Ollama (local) | `openai_compat` | `http://localhost:11434/v1` |
| LM Studio (local) | `openai_compat` | `http://localhost:1234/v1` |

**Auth (important):** only two code paths exist. `provider: anthropic` reads
**`ANTHROPIC_API_KEY`**. `provider: openai_compat` reads **`OPENAI_API_KEY`** for
*every* endpoint above — set that one env var to the chosen provider's key (e.g.
your OpenRouter/DeepSeek/Groq key goes in `OPENAI_API_KEY`). Keyless local servers
(Ollama / LM Studio) need nothing (it defaults to a dummy value). Set `model:` to
the provider's model id. These env vars are the fallback; configure `agent.api_key`
(via `@aq.secret()` or literal) in `aqueduct.yml` — CONNECTION fields, `api_key`
included, are engine-only and NOT legal in a Blueprint's `agent:` block — or per
cascade tier for finer control.

A single `agent.model:` in `aqueduct.yml` heals **solo** (one model, the flat
`agent.*` connection). Adding `agent.cascade:` (also `aqueduct.yml`-only — a
list of tiers, tried in the order you list them) switches to **cascade** mode.
A tier inherits a flat `agent.*` field only when it leaves that field unset; a field the
tier sets is its own key (so `--set agent.timeout` raises the solo/flat default and every
inheriting tier, but not a tier that declares its own `timeout:`).

## Diagnostics

A failing run isn't a dead end — it's inspectable through the same read-only
diagnostics tools the self-healer itself uses. Two access paths: MCP clients
(Claude Desktop, an IDE) can query it directly via `aqueduct mcp serve`;
otherwise the CLI equivalents cover the same ground — `aqueduct report`,
`aqueduct runs`, `aqueduct lineage`, `aqueduct blueprint history`, and
`aqueduct doctor`. See `docs/specs.md` §8.10 for the full tool registry.
