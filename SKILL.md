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

spark_config:                         # optional — merged with engine defaults
  spark.sql.shuffle.partitions: 200

retry_policy: { max_attempts: 3 }     # optional

warnings:                             # optional — per-blueprint compile-warning suppression
  suppress: [perf_python_udf_row_at_a_time]   # rule_ids from AQ-WARN output, or "*" for all

hooks:                                # optional — lifecycle actions after the run ends
  on_success:
    - blueprint: blueprints/next.yml  # chain another blueprint (fresh subprocess) — ungated
    - webhook: https://hooks.example  # url shorthand or {url, method, headers, payload} — ungated
    - command: "scripts/commit.sh ${run.id}"  # subprocess — NEEDS danger.allow_command_hooks
      timeout: 120                    # per-entry seconds (default 300)
  on_failure:
    - command: "scripts/cleanup.sh ${run.id}"
```

**`warnings:` (1.2)** silences compile-time warnings (e.g.
`file_format_no_repartition`, `perf_python_udf_row_at_a_time`) for THIS
blueprint only — unioned with the engine-level `warnings.suppress` in
`aqueduct.yml`.
Compile-time only: never touches session/runtime warnings or other
blueprints. On an Arcade sub-blueprint, its own `warnings:` block parses fine
but is ignored — only the parent blueprint's suppress list applies to the
expanded compilation unit.

**`hooks:` (2.5)** run sequentially after the run's terminal state and NEVER
change the exit code (a failing hook warns + skips the event's remaining
hooks). Exactly one of `blueprint:`/`webhook:`/`command:` per entry.
`command:` interpolates only `${run.id}`/`${run.status}`/`${blueprint.id}`
(shlex argv, no shell) and requires `danger.allow_command_hooks: true` in
aqueduct.yml — the blueprint cannot self-authorize it. Chained `blueprint:`
hooks are cycle-guarded (`AQUEDUCT_HOOK_CHAIN`, depth cap 8; `aqueduct doctor`
checks the chain statically). Engine-level `webhooks:` in aqueduct.yml stays
separate — ops-owned alerting that fires regardless of blueprint hooks. On an
Arcade sub-blueprint, `hooks:` is ignored — only the top-level blueprint's fire.

**Linear-edge sugar:** omit `edges:` entirely and the compiler chains modules in
declaration order — BUT only if every module is single-in/single-out (Ingress,
Channel, Egress, Assert). The moment you use a Junction (fan-out), Funnel
(fan-in), Arcade, Probe, or Regulator, you MUST declare `edges:` (those ports are
ambiguous in a flat chain). A single-module Blueprint needs no edges.

## Module common fields

Every module: `id` (required, unique, fs-safe, **no `__`** — reserved for Arcade
expansion), `label` (REQUIRED — human name), `type` (required), `config`
(type-specific). Optional: `description`, `tags`, `spillway` (downstream id for
error rows), `depends_on` (explicit upstream list), `checkpoint` (bool, for `--resume`),
`enabled` (bool, default true; takes `${ctx.*}` so profiles can toggle it — a disabled
module is skipped ⏭ at run time and the disable cascades to every downstream consumer).

> The single most common authoring error: **forgetting `label:`**. It is required on every module.

## Module types (9)

`Ingress | Channel | Egress | Junction | Funnel | Probe | Regulator | Arcade | Assert`

### Ingress — read data
```yaml
- id: read_orders
  type: Ingress
  label: "Read orders"
  config:
    format: parquet        # parquet|delta|iceberg|hudi|csv|json|orc|avro|jdbc|kafka|custom
    path: "${ctx.tables.orders_raw}"
    # OR address an external-catalog table (mutually exclusive with path):
    # table: "catalog.schema.orders"   # spark.read.table(); catalog wired in spark_config
    schema_hint: { order_id: STRING, amount: "DECIMAL(18,2)" }   # optional
    partition_filters: "event_date >= '${ctx.start_date}'"        # optional
    on_new_columns: fail   # allow(default)|fail|alert — schema-drift contract
    options: { mergeSchema: true }
```
- `path:` and `table:` are **mutually exclusive** (engine errors if both). `format:` not required with `table:`.
- Credentials are NEVER per-Ingress — they live in `spark_config:` (Hadoop/Spark keys), values may use `@aq.secret('KEY')` / `${ENV}`.
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
    udfs: [clean_phone]    # UDF ids to register first (see UDF Registry)
    query: |
      SELECT CAST(amount AS DECIMAL(18,2)) AS amount, clean_phone(phone) AS phone
      FROM dedup
    spillway_condition: "amount IS NULL"   # optional — matching rows go to spillway port
```
Most ops are lazy (no Spark action) except `cache`. `join` is sugar over SQL JOIN.
Incremental: `materialize: incremental` + `watermark_column: <col>` (needs a Depot).

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
```
Downstream edges use `port: high` / `port: low`.

### Funnel — fan-in
```yaml
- id: merge_all
  type: Funnel
  label: "Union"
  config: { mode: union_all }   # union_all|union|coalesce|zip
```

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
Rule types: `schema_match | not_null | min_rows | max_rows | null_rate | freshness | sql | sql_row | spillway_rate | custom`. `on_fail`: `abort | warn | webhook | quarantine`.  Quarantine-eligible rule types: `not_null`, `sql_row`, `custom`, `freshness` (per-row predicates).  The rest are aggregate / population gates — quarantining is rejected at compile time with a clear pointer.

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
Signal types: `schema_snapshot | row_count_estimate | null_rates | sample_rows | value_distribution | distinct_count | data_freshness | partition_stats | threshold | custom`. Sample-based signals (`null_rates`, `value_distribution`, `distinct_count`, `data_freshness`) need `danger.allow_full_probe_actions: true` in `aqueduct.yml` (they add Spark actions). Probes attach by `attach_to`, not edges.

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
  config:
    ref: arcades/region_processor.yml
    context_override: { region: "${ctx.region}" }
```
Expanded at compile time; child ids namespaced `arcade_id__child_id`.

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
- **Tier 1 runtime** `@aq.fn(...)` — resolved pre-job on the driver: `@aq.date.today()/yesterday()/offset(base,days)/month_start()/format(s,p)`, `@aq.run.id()/timestamp()/prev_run_id()`, `@aq.env('K')`, `@aq.secret('K')`, `@aq.depot.get('k')` (or `@aq.depot.<name>.get('k')` for a named mount), `@aq.blueprint.id()/name()/dir()/path()`, `@aq.deployment.env()/target()`, `@aq.version()`. Use `@aq.blueprint.dir()` (not cwd) as the pipeline-relative path anchor.
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
Reference UDFs in a Channel via `udfs: [clean_phone]` and call them in SQL. **Bodies are never inline** — always a module/jar pointer (so the healing LLM never sees code).

## Macros (compile-time text dedup)
```yaml
macros:
  error_rate: "SUM(CASE WHEN status='error' THEN 1 ELSE 0 END)/COUNT(*)"
# use as {{ macros.error_rate }} inside SQL / probe config
```

## Self-healing agent (per-Blueprint policy)
```yaml
agent:
  approval: auto              # disabled|human|auto|ci  (controls if/how patches apply)
  on_pending_patches: warn    # ignore|warn|block
  max_patches: 1              # >1 = multi-patch loop (also needs danger.allow_multi_patch / --allow-multi-patch)
  prompt_context: "Amounts are cents; never cast to INT."   # author hints to the healer
  guardrails:
    allowed_paths: ["s3a://my-bucket/**"]
    forbidden_ops: [remove_module, insert_module]
  sandbox_mode: sample        # sample|preflight|off — how patches are pre-validated
  # connection (overrides engine aqueduct.yml defaults):
  provider: openai_compat     # anthropic | openai_compat
  base_url: "https://openrouter.ai/api/v1"
  model: "anthropic/claude-sonnet-4-6"
  api_key: "@aq.secret('OPENAI_API_KEY')"  # optional; per-tier cascade override also supported
```
`approval` values: `disabled` (never heal) · `human` (stage patch for review) · `auto` (apply validated patch; with `max_patches > 1` enables the multi-patch loop) · `ci` (stage + webhook). Engine-level defaults for provider/model/base_url/api_key live in `aqueduct.yml`; the Blueprint `agent:` block overrides them. API key precedence (highest first): per-cascade-tier `api_key` → blueprint `agent.api_key` → engine `agent.api_key` → env var (`ANTHROPIC_API_KEY`/`OPENAI_API_KEY`). Prefer `@aq.secret('NAME')` or `${ENV_VAR}` over a plaintext literal in any config file.

## Engine config (`aqueduct.yml`) — NOT the Blueprint
Separate file. Configures deployment target, Spark, stores, secrets, webhooks, and engine-level agent connection. Author it only when asked; Blueprints reference its results. Key blocks: `deployment` (engine/target/master_url/env), `spark_config`, `stores` (observability/depots/blob/benchmark + backend), `agent` (provider/base_url/model/api_key/cascade defaults), `danger` (allow_multi_patch, allow_full_probe_actions). Engine `agent.cascade` provides a project-wide default; a Blueprint's `agent.cascade` (or `model: [list]` shorthand) overrides it. See `aqueduct/templates/default/aqueduct.yml.template`.

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
- Cloud creds go in `spark_config`, not in modules.

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
(via `@aq.secret()` or literal) in `aqueduct.yml`, the Blueprint `agent:` block, or
per cascade tier for finer control.

A single `agent.model:` heals **solo** (one model, the flat `agent.*` connection).
Adding `agent.cascade:` (a list of tiers, tried in the order you list them) switches to **cascade** mode.
A tier inherits a flat `agent.*` field only when it leaves that field unset; a field the
tier sets is its own key (so `--set agent.timeout` raises the solo/flat default and every
inheriting tier, but not a tier that declares its own `timeout:`).
