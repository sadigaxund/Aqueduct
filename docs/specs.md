# Aqueduct: Blueprint & Engine Reference

**Version 2.81: Reference Document**

*Self-healing LLM-integrated data pipelines*
*Declarative · Observable · Autonomous · Self-healing*

Blueprint · Module · Ingress · Channel · Egress · Junction · Funnel · Probe · Regulator · Spillway · Arcade · Surveyor

> **Document map:** This document covers the Blueprint format, engine architecture, module types, and self-healing agent. Companion docs cover specific areas:
> - **[CLI Reference](cli_reference.md)**: All commands and flags
> - **[Spark Guide](spark_guide.md)**: Compiler warnings, performance, tuning
> - **[Observability Guide](observability_guide.md)**: Store schemas and diagnostic query cookbook
> - **[Production Guide](production_guide.md)**: Cluster deployment, security, Delta operations

**Contents:** [1. Introduction](#1-introduction) · [2. Naming Glossary](#2-naming-glossary) · [3. System Architecture](#3-system-architecture) · [4. Blueprint Format](#4-blueprint-format) · [5. Context Registry](#5-context-registry) · [6. Observability, Probes & Flow Report](#6-observability-probes--flow-report) · [7. Lineage](#7-lineage) · [8. Self-Healing & LLM Agent Loop](#8-self-healing--llm-agent-loop) · [9. Type System](#9-type-system) · [10. Deployment & Engine Integration](#10-deployment--engine-integration) · [11. Engine Scope & Boundaries](#11-engine-scope--boundaries)

---

# **1. Introduction**

## **1.1 Purpose of this document**

This document is the complete system design and implementation reference for Aqueduct, a declarative data-pipeline engine with integrated LLM-driven self-healing. It covers every component, design decision, data contract, configuration schema, and runtime behaviour.

A parenthesised version such as `(2.8)` after a heading or a field name marks the Aqueduct release in which that behaviour first shipped. It is a compatibility note for readers on an older version, not part of the feature's name.

## **1.2 What Aqueduct is**

Aqueduct is a control plane for a data-processing engine. It does not replace the engine, it wraps it. Engineers and LLM agents author pipelines as YAML Blueprint files. Aqueduct parses, validates, compiles, plans, and executes those Blueprints on the engine named by `deployment.engine`, monitoring them continuously and autonomously patching failures when they occur.

Two engines ship today: Apache Spark (the reference engine, distributed) and DuckDB (single-node, in-process). The Blueprint grammar is the same on both. The engines are not interchangeable, and Aqueduct does not pretend otherwise: each engine declares exactly which parts of the grammar it runs, and the compiler refuses a Blueprint that asks its target engine for something the engine has not declared. §10.9 describes that contract.

The name is deliberate: a Roman aqueduct is precision-engineered infrastructure for carrying flow reliably across vast distances, planned on actual blueprints (forma), and built to strict tolerances. Aqueduct the software carries data flow with the same philosophy, structured, observable, and resilient.

## **1.3 Primary users**

- Data engineers (code-first): author and maintain Blueprints, review patches, configure retry policies.
- LLM agents: primary runtime operators; diagnose failures, propose and apply Patches autonomously.
- Platform operators: deploy and configure the engine, manage deployment targets and credentials.

## **1.4 Design principles**

These principles govern every design decision in the system. When two requirements conflict, the higher principle wins.

| Principle | Description |
| :- | :- |
| **P1: LLM-first observability** | Every failure must carry enough structured context for an agent to diagnose and patch without additional queries. |
| **P2: Blueprint as truth** | The Blueprint is the single source of truth. Nothing about a pipeline exists outside the Blueprint and its derived Manifest. |
| **P3: Performance non-regression** | Aqueduct adds no hidden Spark actions. Any action beyond the pipeline's own Egress writes is the result of a user-configured Probe, Assert, or incremental watermark. |
| **P4: Static resolution first** | Any value that can be resolved at parse time must be. Runtime resolution is explicit, opt-in, and visually distinct in Blueprint syntax. |
| **P5: Patch grammar over codegen** | The LLM agent operates within a structured Patch grammar, not free-form code generation. Every patch is schema-valid, auditable, and reversible. |
| **P6: Passive-by-default gates** | Flow control constructs (Regulators, Spillways) do not exist in the execution path unless explicitly wired. Unwired gates compile away entirely. |
| **P7: Adopt Arrow, invent nothing** | Aqueduct invents no types of its own. It adopts Apache Arrow's type model as the interchange vocabulary (a deliberately chosen subset, not a mirror) and each engine declares how it maps to and from it. Semantic constraints are annotations, not types. See §9. |
| **P8: Provenance-aware LLM context** | Every value the LLM reasons about is backed by a compile-time provenance index. The agent never receives raw Blueprint YAML to reverse-engineer, it receives resolved values tagged with their origin. |

---

# **2. Naming glossary**

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
| **Regulator** | Module type: trigger gate. Passive by default, if nothing is wired to it, it does not exist in the execution path. |
| **Spillway** | The error output port present on every Module. Routes row-level errors to a designated downstream Module. |
| **Arcade** | Module type: an encapsulated, reusable sub-pipeline embedded as a single Module in a parent Blueprint. Expanded at compile time. |
| **Surveyor** | The runtime supervisor process. Monitors pipeline execution, evaluates health signals, manages retry policy, triggers LLM self-healing. |
| **Patch** | A structured diff to a Blueprint proposed by the LLM agent or a human. Expressed as a PatchSpec JSON. |
| **Flow Report** | The post-run column-level quality report. Shows per-column status (OK / Degraded / Error) across each Module. |
| **FailureContext** | The structured failure document assembled by the Surveyor when a pipeline run ends in error. Passed to the LLM self-healing loop. |
| **PatchSpec** | The JSON document that describes a set of operations to apply to a Blueprint. Produced by the LLM agent or authored by hand. |
| **ProvenanceMap** | A compile-time index of every resolved config value: where it came from (literal, context ref, env var, Arcade inheritance), the original expression, and the resolved value. |

> **Three version spaces: do not conflate.** This document tracks three independent numbers: the **Blueprint grammar version** (`aqueduct: "1.0"` at the top of every Blueprint YAML, the schema contract a Blueprint declares against, currently frozen at 1.0), the **specs.md document version** (the `Version X.Y` header at the top of this file, bumped whenever a documented contract changes, currently tracking this section), and the **package version** (`aqueduct-core`'s PyPI release, in `pyproject.toml`, SemVer, independent release cadence). A specs.md version bump does not imply a package release, and a Blueprint's `aqueduct: "1.0"` does not change even when specs.md or the package version does.

---

# **3. System architecture**

## **3.1 High-level overview**

Aqueduct has four processing layers and three persistent stores. Each layer has a defined input/output contract and can be developed and tested independently.

| Layer | Input | Output | Responsibility |
| :- | :- | :- | :- |
| 1: Parser | Blueprint YAML | Validated AST | Schema validation, Context Tier 0 resolution, cycle detection, Arcade loading |
| 2: Compiler | AST + Context map | Manifest (JSON) | Interpolate `${ctx.*}` refs, resolve `@aq.*` functions, expand Arcades, wire Probes and Spillways |
| 3: Executor | Manifest | RunRecord + metrics | Topological sort, submit Spark jobs, attach SparkListener, stream events to Observability Store |
| 4: Surveyor | Live run signals | HealthEvents + Patches | Monitor health, apply retry policy, invoke LLM loop, apply approved Patches |

## **3.2 Persistent stores**

| Store | Description |
| :- | :- |
| **Observability Store** | Append-only log of all runtime signals: Probe readings, stage metrics, errors. Per-pipeline routing (1.1.0+): `.aqueduct/observability/<blueprint_id>/observability.db`. Grows unbounded; pruning it is the operator's responsibility (no built-in retention feature). |
| **Column Lineage** | Column lineage graphs and Flow Reports live in the `column_lineage` table **inside the observability store** (no separate store). The former `stores.lineage` config option has been **removed**; a legacy block in `aqueduct.yml` raises a `ConfigError` (2.0). |
| **Depot (KV Store)** | Persistent key-value store for pipeline state across runs: watermarks, last-run metadata. Configured under `stores.depots` (a name-keyed map of mounts; a `default` mount always exists). Every mount is **per-blueprint isolated** by default, by one of two mechanisms (§10.4.4): a mount with no `path` gets its own file at `.aqueduct/observability/<blueprint_id>/depot.db`, and a mount with an explicit `path` shares that file with transparent `<blueprint_id>:` key prefixing. Opt a mount into cross-blueprint sharing with `shared: true`, which requires an explicit `path` (read via `@aq.depot.<name>.get`). Incremental Channels persist their watermark to the Depot (if configured); without a Depot the watermark is lost between runs and every run re-scans all source data. The compiler emits `perf_incremental_watermark_scan` when an incremental Channel has no upstream cache/checkpoint, because computing `MAX(watermark_column)` on the output requires a second full scan. |
| **Object Store** (1.3+) | Transport for driver-side **blobs** and the **patch lifecycle**, configured under `stores.blob`. A single backend (`local` default, or `s3` / `gcs` / `adls` via one `fsspec` handle, the `object-store` extra, folded into `[stores]`) serves two semantic stores: a **BlobStore** (zstd-externalised `manifest_json` / `stack_trace` / `provenance_json`) and a **PatchStore** (the `pending` / `applied` / `rejected` patch directories). The `local` backend is byte-identical to the historical on-disk layout, so the git-diff review workflow is unchanged; the cloud backends let a run on an ephemeral pod leave no local-FS artefacts under its cwd. |
| **Benchmark Store** (1.3+) | Stores scenario benchmark results (`benchmark_results` table), leaderboard aggregates, and regression gate history. Configurable under `stores.benchmark` with a `local` DuckDB default or `postgres` backend in a dedicated `benchmark` schema. Separate from the observability store, rows are not tied to a real `run_id`. |

> **Storage-integrity warning:** When `stores.observability.backend` is remote
> (Postgres/Redis) but `stores.blob.backend` is left at its default (`local`,
> unset), Aqueduct emits a non-suppressible `AqueductWarning`, externalised
> blobs (manifests, stack traces, provenance) will be written to the driver's
> local disk instead of the remote backend. Set `stores.blob.backend` explicitly
> to silence it (either to `local` to acknowledge, or to a cloud backend like
> `s3`/`gcs`/`adls`).

## **3.3 Component interaction flow**

```
Blueprint.yml → Parser → Validated AST → Compiler → Manifest (JSON) → Executor → RunRecord → Surveyor → Flow Report
                                                                              ↓ failure
                                                                       Agent loop → PatchSpec → Gates → apply
```

On the happy path the flow is linear: Parser → Compiler → Executor → Surveyor.

1. **Parse + Compile.** Parser validates YAML against the JSON Schema, builds the AST, resolves Tier 0 context refs. Compiler resolves Tier 1 (`@aq.*`) calls, secrets, Depot reads, dates, expands Arcades into a flat module list, and emits the Manifest plus `provenance_map` and `inputs_fingerprint`.
2. **Execute.** The Executor topologically sorts the modules, inserts Probes after their `attach_to` targets, identifies independent connected components for `--parallel` mode, and runs each module through its handler. Per-module metrics and Probe signals are written to the observability store as they fire.
3. **Surveyor + Agent loop (when triggered).** On failure, the Surveyor packages a FailureContext (error trace + provenance slice + recent signals + lineage), calls the configured LLM, and receives a PatchSpec. Patches are validated through gates (guardrails → compile-check → lineage → sandbox → resolvability). The `apply_callback` writes the patch to disk, recompiles the Manifest, and the executor re-runs the pipeline.

### **Why a Manifest? Why not run the YAML directly?**

The compile step is not cosmetic: the Executor consumes the Manifest, never the raw YAML. Several Blueprint constructs cannot be resolved at execution time:

| Blueprint construct | Why resolve at compile, not run |
| :- | :- |
| `${ctx.foo}` Tier 0 context refs | Substitution must happen before any module sees its config to ensure consistency. |
| `@aq.date.today()`, `@aq.run.timestamp()` Tier 1 calls | Resolving at execution time would tie the value to the moment each module ran, two modules calling `today()` at different stages of a 4-hour pipeline could see different dates. |
| `@aq.secret('KEY')` | One network round-trip per run, not one per module per worker thread. |
| `@aq.depot.get('watermark')` | A single DuckDB read at compile time prevents race conditions with runtime writes. |
| Arcade `ref: arcades/foo.yml` | Sub-Blueprints are expanded inline so the executor sees a single flat module list. |
| Macros `{{ macros.* }}` | Spark SQL cannot parse `{{ }}` placeholders, expansion must happen before Spark sees the query. |
| Passive Regulators | Regulators with no wired signal input are compiled away entirely. |

The Manifest also carries the **ProvenanceMap** (per-config-key audit trail recording where every value came from) and **`inputs_fingerprint`** (compile-time snapshot of Ingress file metadata for the LLM to distinguish data-drift bugs from code bugs).

**`base_dir`**: the top-level Blueprint file's own directory (empty string when compiled from an in-memory dict with no file). Every executor-side user-code import site, Assert `type: custom`'s `fn:`, Probe `type: custom`'s `module:`/`entry:`, `udf_registry`'s Python `module:`, and Egress/Ingress `format: custom`'s `class:`, resolves its dotted path against `base_dir` first (a sibling `.py` file next to the Blueprint), falling back to a normal `import` for installed packages. This exists because the `aqueduct` console-script entry point never puts the Blueprint's directory on `sys.path` (unlike `python -m`/`python -c`), so a bare `import` of a sibling file used to fail unless the user manually mutated `sys.path`. For an Arcade sub-Blueprint, callable refs still resolve against the **top-level** Blueprint's `base_dir`, not the arcade's own directory (one Manifest per compilation unit).

---

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
    port: main                         # main (default) or spillway

engine:                                 # per-engine settings, namespaced by engine name (2.0)
  spark:
    conf:                               # merged with aqueduct.yml's engine.spark.conf
      spark.sql.shuffle.partitions: 200
      spark.sql.adaptive.enabled: true
  duckdb:                               # merged with aqueduct.yml's engine.duckdb.* (2.54)
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

**Per-Blueprint compile-warning suppression (`warnings:`, 1.2).** `warnings.suppress` is a list of compiler-warning `rule_id`s (or the sentinel `"*"`) to silence for THIS Blueprint only, it covers all **compile-time** diagnostics: the modular rule registry (e.g. `file_format_no_repartition`, `jdbc_missing_partition`, `kafka_checkpoint_stale`) and the inline compiler checks (e.g. `perf_python_udf_row_at_a_time`, `perf_multi_consumer_no_cache`, `delivery_append_retry_dupes`). It is unioned with the engine-level `warnings.suppress` from `aqueduct.yml` (+ `--suppress-warning` flags), either side suppressing a rule silences it. It does **not** affect engine/session-startup warnings, runtime (Probe/Assert) warnings, or the process-global default used by other Blueprints, a rule suppressed here stays visible everywhere else. For an **Arcade** sub-Blueprint, the parent Blueprint's `warnings.suppress` covers the whole expanded compilation unit (including the sub-Blueprint's modules); the sub-Blueprint's own `warnings:` block is valid YAML (it parses standalone) but is not consulted during expansion.

**Lifecycle hooks (`hooks:`, 2.5).** Four events: `on_success` / `on_failure` run sequentially AFTER the pipeline reaches its terminal state; `on_patch_pending` / `on_healed` fire MID-RUN at heal milestones, mirroring the engine-level `webhooks:` `on_patch_pending` vocabulary one level up, at the Blueprint. `on_patch_pending` fires every time a heal stages a patch for human review (guardrail-blocked staging and `approval: human`, both staging sites). `on_healed` fires once a heal's re-run succeeds, patch applied AND the pipeline green again, and always runs BEFORE the outer run's terminal `on_success` hooks (it fires mid-loop, before the loop breaks out to the terminal report). No event **ever changes the run's exit code** (a failing hook emits `⚠ [hook_failed]` and skips the event's remaining hooks). Each entry sets **exactly one** action:

| Entry | Semantics | Gate |
| :- | :- | :- |
| `blueprint: <path>` | Chains another Blueprint. By default a fresh `aqueduct run` subprocess, own session, run_id, and report. Loose coupling by design: the child's failure is one `hook_failed` warning, not a parent failure. Tightly-coupled work belongs in ONE Blueprint (Arcades / `--parallel` / `enabled:`). `in_process: true` opts into parsing+compiling+executing the target in the SAME process, reusing the caller's live session, no self-healing loop for the chained target (a failure is still just `[hook_failed]`); falls back to the subprocess path with an info message when the target Blueprint sets its own `engine.spark.conf` (merging two Blueprints' Spark configs into one live session isn't generally safe). It also falls back (this time with a `[hook_in_process_unavailable]` warning, since there is no other signal the requested execution model didn't happen) on a polyglot run (2.37): every island's session is already closed by the time hooks fire (there was never one session for the whole run to reuse), so `in_process: true` on a polyglot Blueprint's hooks always runs the target as a fresh subprocess instead. | none (declarative) |
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
| **retry** | Optional. Per-module override of the top-level `retry_policy:` block (2.8), see below. |
| **engine** | Optional. A scalar execution-engine NAME (`spark`, `duckdb`) selecting which engine runs THIS module (2.34), see below. Distinct from the blueprint-level `engine:` BLOCK (§4.2, per-engine settings namespaced by engine name); same word, two levels. |

**`spillway:` field sugar (2.51).** `spillway: <target>` is authoring sugar for `edges: [{from: <this module>, to: <target>, port: spillway}]`; the Compiler expands it into that real edge at compile time (right after Arcade expansion, so a `spillway:` field set inside an Arcade's own sub-Blueprint is correctly namespaced first), the SAME and ONLY mechanism §4.4's spillway routing already documents. There is no behavioral difference between the two authoring forms; use whichever reads better for a given Blueprint. `Module.spillway` is validated at parse time (the target must exist) and is `None` on every module in the compiled Manifest once desugared: the edge is the sole runtime encoding. Conflict rule: a module carrying BOTH the `spillway:` field and an explicit `port: spillway` edge is fine when they name the SAME target (idempotent; no duplicate edge); naming a DIFFERENT target is a `CompileError` (never silently pick one).

### `config:` is a typed, per-module-type union (2.42)

Every module type declares its own `config:` shape; a pydantic discriminated union on `type`, one member per module type (`Ingress`/`Channel`/`Egress`/`Junction`/`Funnel`/`Probe`/`Regulator`/`Arcade`/`Assert`), each absorbing that type's real keys with `extra="forbid"`. An Ingress's `config:` accepts `format`/`path`/`table`/`schema_hint`/`options`/…; an Egress's accepts `format`/`mode`/`maintenance`/…; a key that belongs to a DIFFERENT type (or belongs to no type at all) is a structural rejection at parse time naming the offending key; not a silent accept.

The one deliberate exception is `options:` (Ingress/Egress); a freeform passthrough dict forwarded verbatim to the engine's reader/writer `.option(k, v)` calls, since enumerating every Spark/DuckDB option is out of scope and the wrong target. A couple of genuinely polymorphic fields (Ingress `schema_hint`, Channel `columns`) are similarly kept as an untyped container with the accepted shapes documented on the field, rather than modeled as a second-level union.

**BREAKING (2.42):** before this, ALL nine module types shared one flat schema with a freeform `config: dict[str, Any]`; any key parsed, whether or not any executor ever read it. A key that no code reads (a typo, a stale synonym, a dead knob) previously parsed, compiled, and ran as a silent no-op; it is now a `ParseError` naming the key. This is by design: a freeform dict is invisible to the capability framework by construction (a leaf is derived by introspecting a pydantic model), so a wrong key inside `config:` could never surface as a "this engine doesn't support X" capability gate; it just silently did nothing on every engine. Migrating a Blueprint that hit this: the error names the exact key and module; check that type's `config:` fields against this section (or `SKILL.md`) for the correct spelling. No accept-both-shapes reader was added: see `CHANGELOG.md`.

Capability leaves follow the same split: fields common to every type (`id`, `label`, `engine`, `retry`, …) keep the `module.field.<name>` leaf id; every type-specific field (both the ones already living at the module's top level (`attach_to`, `ref`, `materialize`) and every field inside a typed `config:`) gets a `<type_lower>.field.<name>` leaf (e.g. `egress.field.maintenance`, `channel.field.query`), so every engine must give it a real verdict (`aqueduct/executor/capability_leaves.py`).

### Per-module retry override (`retry:`, 2.8)

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

### Cross-engine handoff: per-module `engine:` and islands (2.34)

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

**Incremental watermark (`materialize:` / `watermark_column:`, 2.40).** Declared MODULE-level fields, siblings of `config:`; NOT config keys (same shape as Probe's `attach_to`, §4.4). Promoted out of the freeform `config:` dict in 2.40 so the capability framework can see them (a freeform key is invisible to it by construction; every engine must declare a verdict for the `channel.field.materialize` / `channel.field.watermark_column` capability leaves; renamed from the flat `module.field.*` id when 2.42 split the single Blueprint module schema into one discriminated-union member per module type, each with a typed `config:`; see §9's capability-leaf note below). `op: sql` only:

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

**BREAKING (2.40):** `materialize`/`watermark_column` used to live inside a Channel's `config:` block, a freeform dict that never validated their spelling or gated them by capability. Move both out of `config:` up to the module (siblings of `config:`, like `attach_to`). A Blueprint that still nests them inside `config:` parses (the dict stays freeform) but neither field is read from there anymore; the incremental behaviour silently stops. See `CHANGELOG.md`.

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

**`report: stdout` (2.4).** Per-Probe opt-in terminal output: each signal's result also prints under the Probe's row in the post-run summary, dim `↳` lines, single-value payloads on one line, dict/tabular payloads one line per entry, the block capped at 10 lines unless `-v`. Purely additive: every signal is still persisted to `probe_signals` exactly as without it, and the printed lines are informational notes, never counted in the runtime warning roll-up. Sampling governance and `danger.allow_full_probe_actions` gating apply unchanged, `report` changes where results are shown, not what is collected.

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

See the [Observability Guide](observability_guide.md) for full signal reference and cost model.

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

**A `custom` rule that cannot be evaluated is a failure of that rule, on both engines.** Two situations (no `fn:` configured, or `fn(df)` itself raising (a bug, a bad import, an API from the wrong engine)) are routed through the rule's own `on_fail`, exactly like a rule that evaluated and failed: `abort` aborts, `warn` warns and continues, `webhook` fires the webhook, `trigger_agent` defers to the healing loop, and `quarantine` falls back to the same "aggregate rule, no row filter available" warn behavior a genuinely-failed `custom` rule with no `quarantine_df` already gets (there is nothing to quarantine when the rule never ran). A quality gate whose own code is broken must not silently let the data through: this was previously the case regardless of `on_fail`, unconditionally logging a warning and passing the data through.

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

# **5. Context Registry**

## **5.1 Three-Tier Resolution Model**

| Tier | Syntax | Resolved at | Performance cost |
| :- | :- | :- | :- |
| Tier 0: Static | `${ctx.namespace.key}` | Parse time | Zero: substituted before Manifest is written |
| Tier 1: Runtime function | `@aq.fn(args)` | Pre-job (Compiler) | Driver-only, milliseconds |
| Tier 2: UDF | `udf_id` called by name in Channel SQL (§5.4 `udf_registry`) | Engine execution | Distributed: operates on DataFrame columns |

## **5.2 Tier 0: Static Context**

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

### Env-var overrides (`AQUEDUCT_CTX_*`)

Any environment variable prefixed `AQUEDUCT_CTX_` overrides the context key
obtained by stripping the prefix and **lowercasing the rest**, so
`AQUEDUCT_CTX_ENV=prod aqueduct run blueprint.yml` is equivalent to
`--ctx env=prod`, and `AQUEDUCT_CTX_BATCH_SIZE=500` overrides a top-level
`batch_size` key. This is the override hook for CI pipelines, Airflow, and
schedulers that can set environment variables but cannot manipulate CLI
arguments.

Two rules to keep in mind:

- **Only top-level (dot-free) keys are addressable.** Nested context keys
  flatten to dot-notation (`params.batch_size`), and environment-variable
  names cannot contain dots: underscores are *not* translated to dots, so
  `AQUEDUCT_CTX_PARAMS_BATCH_SIZE` defines a new `params_batch_size` key
  rather than overriding `params.batch_size`. Keys you want overridable from
  the environment should live at the top level of `context:` (underscores in
  the key name itself are fine). Nested keys remain overridable via `--ctx
  params.batch_size=500`.
- **This is override, not substitution.** `${AQUEDUCT_ENV:-dev}` *inside* the
  `context:` block is env-var *substitution* (the value is read into a
  context key you named); `AQUEDUCT_CTX_*` is env-var *override* (the
  variable name selects which context key to replace). The two are
  independent mechanisms and compose: a substituted default is still
  replaceable by an `AQUEDUCT_CTX_*` override or `--ctx`.

## **5.3 Tier 1: Runtime Functions (`@aq.*`)**

| Function | Description |
| :- | :- |
| `@aq.date.today(format="%Y-%m-%d")` | Current date (UTC). Pinned by `--execution-date` for idempotent backfills. |
| `@aq.date.yesterday(format="%Y-%m-%d")` | Date - 1. |
| `@aq.date.offset(base, days)` | Offset a date string by N days. Useful for backfill windows: `@aq.date.offset(base=@aq.date.today(), days=-7)`. |
| `@aq.date.month_start(format="%Y-%m-%d")` | First day of the current month. |
| `@aq.date.format(date_str, pattern)` | Reformat an ISO date string into a custom pattern. |
| `@aq.run.id()` | Auto-generated UUID for this pipeline run. |
| `@aq.run.timestamp()` | ISO-8601 timestamp of compilation. |
| `@aq.run.prev_id()` | Run ID of the previous pipeline execution (reads `_last_run_id` from Depot). Fails compilation (2.68) if no depot backend is configured — see `@aq.depot.get` below. |
| `@aq.env('KEY')` | Read environment variable. Fails fast when absent, unlike `${VAR:-default}` which supports a fallback. |
| `@aq.secret('KEY')` | Read from AWS/GCP/Azure secrets manager or environment fallback. |
| `@aq.depot.get('key')` | Read from the default Depot KV store at compile time. `@aq.depot.<name>.get('key')` reads a named mount (see the Depot glossary entry + Observability Guide). **Fails compilation (`CompileError`, 2.68)** if no depot backend is configured at all — a Blueprint that references a depot read needs a real mount, or the read would silently fall back to the default and mask the pipeline going incremental-in-name-only. A configured depot with the key simply absent is unaffected: that still returns the default, unchanged. |
| `@aq.blueprint.id()` | This Blueprint's `id`. |
| `@aq.blueprint.name()` | This Blueprint's `name`. |
| `@aq.blueprint.dir()` | Absolute directory of the Blueprint file: the safe "relative-to-this-pipeline" anchor for output paths (e.g. `path: @aq.blueprint.dir()/out`). |
| `@aq.blueprint.path()` | Absolute path of the Blueprint file. |
| `@aq.deployment.env()` | `deployment.env` (e.g. `dev` / `cluster` / `cloud`), branch paths/behaviour by environment. |
| `@aq.deployment.target()` | `deployment.target` (e.g. `local` / `standalone`). |
| `@aq.deployment.engine()` | The execution engine this Manifest is compiled for (`deployment.engine`, e.g. `spark`): stamp it into an output path or a tag when the same Blueprint runs on more than one engine. |
| `@aq.version()` | The Aqueduct engine version: useful for stamping outputs. |

> `@aq.blueprint.* / @aq.deployment.*` exposes **pipeline identity + deployment context** known at compile time. Note what is **deliberately absent**: `cwd` / user / host, those differ across laptop ↔ CI ↔ Spark driver ↔ cluster, so they would make a Blueprint non-reproducible. Use `@aq.blueprint.dir()` as the stable anchor instead.

### 5.3.1 Resolution scopes: *where* each `@aq.*` resolves

The config and Blueprints resolve at **different times**, and a scope is usable
only where it exists:

| Resolution point | Allowed syntax | Why |
| :- | :- | :- |
| **`aqueduct.yml`** (engine config) | `${ENV}`, `${VAR:-default}`, `@aq.secret('KEY')` only | Loaded first, standalone: no Blueprint and no run exist yet, so per-pipeline / per-run scopes have nothing to resolve against. |
| **Blueprint compile** (per run: `context`, module `config`, blueprint-level `agent:` / `retry_policy:` / `engine:`, …) | **All `@aq.*`**: `date`, `run`, `blueprint`, `deployment`, `depot`, `secret`, `env`, `version` | The single point where the whole stack is in scope: deployment (from the loaded config) ⊃ blueprint (id/path) ⊃ run (run_id), plus the depot built from config. |

The model is **override-downstream, not propagate-uphill**: one config is shared
by many Blueprints, and one Blueprint by many runs; values flow config → blueprint
→ run, and each lower layer overrides what it inherits. There is no path back
*up*: a Blueprint cannot inject per-run values into `aqueduct.yml`, because the
config is fully resolved *before* any Blueprint is parsed.

Consequently, a non-secret `@aq.*` (e.g. `@aq.run.id()`, `@aq.blueprint.id()`) in
`aqueduct.yml` is a **hard error**: those scopes do not exist at config-load
time; use them inside the Blueprint. Per-pipeline store isolation (a depot / obs
store per Blueprint) needs no `@aq` in config, the backend handles it
automatically, keyed on `blueprint_id`.

## **5.4 UDF Registry**

UDFs are registered from importable code, not inline source. Two execution
models: `lang: python` (default) and `lang: java`/`scala` (JAR-backed).

**Python UDFs** point at an importable module + function:

```yaml
udf_registry:
  - id: clean_phone
    lang: python                   # default
    module: my_project.udfs        # importable module (must be on PYTHONPATH)
    entry: clean_phone             # function name in that module (defaults to `id`)
    return_type: STRING
```

The driver imports `module` and looks up `entry` (a plain Python callable),
registering it with `spark.udf.register`. `module` resolves against the Manifest's
`base_dir` first (a sibling `.py` file next to the Blueprint, see **§3, `base_dir`**),
falling back to a normal import from `PYTHONPATH` / an installed package. Python UDFs
execute row-at-a-time via the JVM bridge: for high-volume Channels prefer native Spark SQL.

**Parameterized (context-aware) Python UDFs.** Add a `params:` map and `entry`
becomes a **factory** (`entry(**params) -> callable`), so one importable
function is reused across blueprints and environments with different settings:

```yaml
udf_registry:
  - id: mask_pii
    module: my_project.udfs
    entry: make_masker             # factory: make_masker(char, keep_last, salt) -> callable
    return_type: STRING
    params:
      char: "*"
      keep_last: 4
      salt: "@aq.secret('PII_SALT')"   # resolved before the factory is called
```

Param values support `${ctx.*}`/`${ENV}` (Tier 0) and `@aq.*` including
`@aq.secret()` (Tier 1): they are fully resolved at compile time, so the
factory receives concrete values, never tokens. The factory must return a plain
callable (or a Spark UDF object). Omitting `params:` keeps the static behaviour
above (no factory call). UDF **bodies** remain out of scope for self-healing,
`params` change *configuration*, not code.

**Java/Scala UDFs** point at a JAR + class, pure JVM bytecode, no Python
serialization:

```yaml
udf_registry:
  - id: geohash
    lang: java                     # or scala
    jar: libs/geo-udfs.jar         # JAR path (relative paths anchor to the Blueprint dir)
    class: com.example.GeoHashUDF  # fully-qualified class name
    return_type: STRING
```

| Field | Applies to | Description |
| :- | :- | :- |
| `id` | all | UDF name: called by this name directly in Channel SQL. Required. |
| `lang` | all | `python` (default), `java`, or `scala`. |
| `return_type` | all | Hub type spelling (§9): Aqueduct's own portable vocabulary, not raw engine DDL (default `string`). |
| `module` | python | Importable module path. Required for python. |
| `entry` | python | Function name in `module` (defaults to `id`). With `params`, treated as a factory `entry(**params) -> callable`. |
| `params` | python | Optional keyword map passed to the `entry` factory. Values resolve `${ctx.*}`/`${ENV}` and `@aq.*` (incl. `@aq.secret()`) at compile time. |
| `deterministic` | python | Default `true`. `false` marks the UDF nondeterministic on both engines: Spark builds it via `asNondeterministic()` before registration, so the optimiser does not constant-fold, cache, or re-order calls; DuckDB inverts it onto `side_effects` (`side_effects = not deterministic`) for the same optimizer-safety reason. |
| `jar` | java/scala | JAR file path (relative paths anchor to the Blueprint dir). |
| `class` | java/scala | Fully-qualified class name. |

## **5.5 Dependencies (`dependencies:`, 2.66)**

A top-level Blueprint block, sibling of `udf_registry:`; not engine-scoped, no capability leaf, no `aqueduct.yml` allowlist surface. A flat list of PEP 508-lite requirement strings the Blueprint author declares the runtime environment must already satisfy:

```yaml
dependencies:
  - holidays>=0.40
  - geopy[extra]>=2.3,<3
```

`name`, `name>=1.2`, `name[extra1,extra2]>=1.2,<2` are accepted; environment markers (`; python_version < "3.12"`) are rejected rather than silently ignored, and a malformed string is a `ParseError` naming it at parse time.

This is a **compile-time preflight, not an installer**: Aqueduct never installs anything. Each declared requirement is checked against the installed environment via `importlib.metadata`; anything missing or version-conflicting raises `DependencyError` (a `CompileError` subclass), naming every failing requirement and the copy-pasteable `pip install` command, instead of the author meeting a mid-run `ImportError`. A package whose installed version string the PEP 440-lite comparator cannot read reports `unknown_version` and passes: the preflight exists to catch definitely-unsatisfied requirements, never to reject an install it merely does not understand. Satisfied requirements are silent.

See `aqueduct/dependencies.py` for the parser/comparator and §8.5 for `declare_dependency`, the healing-time counterpart that appends to this block.

> **FROZEN: declare-and-check only.** `dependencies:` and its healing-time counterpart
> `declare_dependency` are feature-complete and closed to extension. They declare what the
> environment must already provide and check whether it does. Neither will ever install,
> resolve, pin, or vendor a package, and no flag will be added to make them do so: installing
> into the environment a pipeline runs in is the operator's job, not the engine's.

---

# **6. Observability, Probes & Flow Report**

See the dedicated **[Observability Guide](observability_guide.md)** for:

- Full schema reference for `observability.db` and `benchmark.duckdb`
- Diagnostic query cookbook (run post-mortem, heal-loop forensics, cost analysis)
- Probe signal reference and cost model
- Store backend configuration (DuckDB / Postgres / Redis)

### Key design constraint

All observability is governed by one rule: **no Spark actions may be added to the critical execution path** beyond what the Blueprint configures. Default Probe signals (`schema_snapshot`, `execution_partitions`, `row_count_estimate` via SparkListener) are zero-cost. Sample-based signals (`null_rates`, `value_distribution`) require explicit opt-in.

---

# **7. Lineage**

## **7.1 Two lineage layers**

| Layer | Computed at | Purpose |
| :- | :- | :- |
| **Structural Lineage** | Compile time (static) | Column-level DAG of the Blueprint. Used by the lineage gate and the LLM. |
| **Runtime Flow Report** | Post-run | Per-column quality metrics from Probe signals. |

## **7.2 Structural Lineage**

Structural lineage is computed at compile time (in `aqueduct/compiler/lineage.py`, zero Spark actions) by `sqlglot` analysis of Channel SQL queries. It maps each output column to its upstream source module and column. Stored in the `column_lineage` table inside `observability.db`. Used by:

- **Lineage gate:** Before a patch is applied, the lineage of the patched Blueprint is compared to the original. Lost columns or broken references are flagged.
- **LLM context:** The structural lineage for the failed module's neighbourhood is included in the FailureContext. The agent can trace column origins from it without accessing the original Spark session.

### Channel SQL fingerprints

Each `op: sql` Channel also gets a **normalised AST fingerprint**, sqlglot
canonicalises the query (formatting/comment/keyword-case insensitive) and the
SHA-256 of the canonical form is recorded in `channel_fingerprints`. The table
is a *changelog*: a new row appears only when a Channel's SQL changes
*semantically* (a reformat does not), so it answers "did this transform change,
and when" without storing one row per run.

### Type-tracked column chains

`aqueduct lineage <bp.yml> --chain <column> --types` traces a single column
source→output, annotating each hop with an sqlglot-inferred SQL `output_type`
and a `transform_op` (`passthrough` | `rename` | `CAST` | `CONCAT` | a function
name | `literal` | `expression`) and flagging type changes. It is computed **on
demand** from the compiled Manifest: nothing extra is persisted and no Spark
action runs. This is a human debugging tool ("why is this column a string now",
rename-impact, type-drift); it is **not** part of the healing loop, which
already reads full SQL from the manifest. sqlglot resolves ~90% of SparkSQL
expressions; the rest fall back to `output_type=UNKNOWN`.

## **7.3 Runtime Flow Report**

Generated post-run from Probe signals. Shows per-column, per-Module status (OK / Degraded / Error) with null rates, row estimates, schema snapshots, and thresholds. `aqueduct report --trend <column> --blueprint <id>` adds a **cross-run** view of one column's null-rate and type history, a read-side aggregate over `probe_signals` (no extra table).

# **8. Self-healing & LLM agent loop**

## **8.1 Design philosophy**

The LLM agent operates within a grammar, not in free-form code generation mode. It can only propose structured PatchSpec operations, valid, schema-checked modifications to the Blueprint. This constraint makes every agent action auditable, reversible, Git-diffable, and explainable to a human reviewer.

**Model-agnostic design.** The PatchSpec grammar is deliberately narrow, 14 schema-checked operations with no code generation, so the agent works reliably across model sizes. A 7B parameter local model handles ~70% of production failures (path typos, format mismatches, column renames, simple SQL fixes) in a single attempt. Larger models unlock `agent.deep_loop` (in-conversation sandbox feedback) and multi-model cascading for complex cases like OOM tuning and multi-module restructures. The deterministic guardrails, gate pyramid, and structured prompt apply the same safety guarantees regardless of model size.

**The `agent:` block is split by kind, at both levels (2.59).** A Blueprint's `agent:` block is **POLICY-ONLY**: risk decisions about THIS pipeline; `approval`, `on_pending_patches`, `max_patches`, `guardrails`, `confidence_threshold`, `on_heal_failure`, `allow_defer`, `deep_loop`, `sandbox_mode`, plus a shared subset (`max_reprompts`, `prompt_context`, `max_heal_attempts_per_hour`, `patch_validation`) that overrides the engine's own default when set. `aqueduct.yml`'s `agent:` block (`AgentConnectionConfig`) is the **only** place CONNECTION settings live (`provider`, `base_url`, `api_key`, `model`, `provider_options`, `timeout`, `cascade`) an endpoint fact about the deployment, not a per-pipeline decision. A Blueprint cannot set or override any connection field; writing one into a Blueprint's `agent:` block is a schema-level rejection naming the field (`AgentSchema` uses `extra="forbid"`), not a silent no-op. This is the same split already applied to `engine:` (§10.1): `engine.spark`'s Blueprint-level block omits `master_url`, `engine.duckdb`'s omits `database_path`/`s3_*`; a Blueprint does not get to decide deployment/connection concerns. The security reasoning is sharper here: the healing loop ships `FailureContext` (pruned manifest, provenance, error text) to whichever endpoint is configured, so if a Blueprint could pick that endpoint, any pipeline failure would be an exfiltration opportunity to a host the pipeline's author (not the operator) controls.

**Solo vs cascade.** With a single `agent.model:` in `aqueduct.yml`, healing runs **solo**, one model, the flat `agent.*` connection (`model`, `base_url`, `timeout`, `budget`, …). Configuring `agent.cascade:` (also `aqueduct.yml` only) switches to **cascade** mode: a list of tiers tried in the order you define them.

**Multi-model cascade.** `agent.cascade:` is configured at engine level (`aqueduct.yml`) only; a Blueprint cannot declare or override a cascade (cascade tiers are entirely connection settings: each tier carries its own model/provider/base_url/api_key). Aqueduct tries the tiers strictly in the order you define them, it does **not** reorder by price or capability. The usual convention is to list the cheapest/fastest model first and escalate to a stronger one, but that ordering is the author's responsibility, not the engine's. Escalation triggers on `stuck_signature`, `exhausted_attempts`, or `deferred`; a tier whose provider is simply unreachable (`api_error`) escalates to the next tier and only aborts on the final tier. Each tier has its own budget (`max_reprompts`, `max_seconds`) and can override `provider`, `base_url`, `api_key`, `timeout`, `deep_loop`, and `allow_defer`. **A tier's own fields override the flat `agent.*` only by inheritance, not merge:** a field the tier leaves unset inherits the solo/flat value; a field the tier sets is an independent key that wins for that tier (so `--set agent.timeout` raises the flat default + every inheriting tier, but does **not** reach a tier that declares its own `timeout:`). A tier's budget reuses the top-level `agent.budget` axes with `max_reprompts` / `max_seconds` swapped for the tier's own values. `max_tokens_total` spans the WHOLE cascade: each tier receives the remaining allowance, and the cascade stops with `budget_tokens_exceeded` when it is spent. A defer on a non-final tier escalates (its diagnosis is discarded); a defer on the final tier is staged for human review. The producing tier's model and 0-based index are persisted on `healing_outcomes.model` / `model_cascade_position`. `aqueduct doctor <blueprint>` checks each tier's credentials/endpoint ahead of time.

**Pending-patch short-circuit.** Aqueduct never queues a second unreviewed patch for the same problem. Before any LLM call, `aqueduct run` checks `patch_index` for a `pending` row on the current `blueprint_id` (`patch/index.py::list_by_status`); if one exists, the run stops immediately without calling the model — the message names the existing patch id and how to review it (`aqueduct patch pull <id>` / `aqueduct patch list`), and the run exits `HEAL_PENDING`. No `heal_attempts` row is written (there was no LLM attempt); the failure itself is already recorded by the normal per-iteration `Surveyor.record()` call that runs before this check. Every failure still hashes into a stable signature, `(error_class, failed_module, normalized_message)` plus a coarse variant that drops the module (`aqueduct/agent/signature.py::from_failure_context`), and is still stamped onto `healing_outcomes.failure_signature[_coarse]` and `patch_index.signature[_coarse]` for observability and the reprompt-loop's budget axes (`same_signature_overall`, `progress_stalled_window`) — it no longer keys any lookup. `aqueduct patch list`/`pull` resolve through SQL queries against the **`patch_index`** observability table (status + body `object_key`) rather than scanning the `patches/` directory, backend-blind, so they work when patch bodies live on s3/gcs/adls. Patch bodies are written through the PatchStore (`pending` / `applied` / `rejected`) and every status transition is recorded in `patch_index`; local-checkout commands (`patch apply` / `patch reject`) stay on the filesystem but flip the index status so the two stay consistent.

## **8.2 The healing flow**

```
Pipeline failure → Capture → Pending check → Prune → Generate → Reprompt → Gate → Confirm and write
```

### 1. Capture

Transient errors retry first (per `retry_policy.max_attempts`). Non-transient failures, schema drift, missing columns, bad paths, OOM, trigger the agent. The Surveyor assembles a self-contained failure package:

- Compiled module config
- ProvenanceMap (where every config value came from)
- Sliced lineage neighbourhood
- Structured root-cause block (offending column + Spark suggestions)
- `inputs_fingerprint` (file metadata to distinguish data-drift from code bugs)

### 2. Pending check (zero tokens)

Before any LLM call, `patch_index` is checked for a `pending` row on this blueprint: if one exists, the run ends immediately, exit `HEAL_PENDING`, no LLM call. Otherwise → continue below.

### 3. Prune

A ContextPruner trims the package to the failure's blast radius. Pruning rules:

| Error class | Manifest scope |
| :- | :- |
| `ColumnNotFound`, `TypeMismatch`, `AnalysisException` | Failed module + 2 upstream + 2 downstream |
| `SparkException` with OOM/shuffle | Full manifest |
| All other errors | Failed module + direct upstream |

### 4. Generate

The LLM responds with a structured PatchSpec, a list of typed operations that map one-to-one to Blueprint edits. Anything else is rejected.

### 5. Reprompt

Schema errors, guardrail violations, and gate rejections feed back into the same conversation as annotated, field-level corrections. The loop is bounded by a multi-axis budget:

| Axis | Default | What it guards against |
| :- | :- | :- |
| `max_reprompts` | 5 | Hard ceiling on LLM round-trips |
| `max_seconds` | 120 | Wall-clock cap on LLM-conversation time per heal call |
| `max_tokens_total` | 50,000 | Sum of prompt + completion tokens |
| `same_error_consecutive` | 2 | Stuck on identical error signatures |
| `same_signature_overall` | 3 | Same error signature across the run |
| `progress_stalled_window` | 3 | No new distinct signatures |

When `same_error_consecutive` trips, the loop escalates: temperature is bumped and a skeleton reprompt template is used for one more attempt before honouring the abort.

`max_seconds` counts LLM time only: validation-gate work (deep-loop sandbox replay, lineage) is excluded from the clock, so a slow sandbox cannot exhaust the heal budget. Transient provider errors (HTTP 429/503/529) are retried per `agent.retry` (default 2 retries, exponential backoff with jitter, server `Retry-After` honored); retry sleeps count as LLM time and are always capped by the remaining per-call deadline.

### 6. Gate

Before a patch touches the Blueprint, four numbered gates plus an unnumbered compile-check run in order. The numbering is load-bearing: it matches the module docstrings (`patch/preview.py`, `patch/resolvability_gate.py`) and the test filenames (`tests/test_patch/test_patch_preview_gate3.py` is the SANDBOX gate). The compile-check is a step inside `patch/apply.py::apply_patch_file`, not a gate of its own, which is why it carries no number.

1. **Gate 1, guardrails**: path and operation policy (deterministic, enforced before the LLM response is parsed); including `allowed_paths` and, evaluated after it over the same resolved value, `deny_patterns` (subtract-only)
2. *Compile-check* (unnumbered): the patched dict must re-parse into a valid Blueprint. Runs immediately after Gate 1.
3. **Gate 2, lineage**: column-level diff catches broken references before the engine sees them
4. **Gate 3, sandbox**: sampled or full replay catches "parsed but produces nothing"
5. **Gate 4, resolvability** (2.66, `patch/resolvability_gate.py`): asks whether every `declare_dependency` op in the patch names a requirement that is at least resolvable; never whether Aqueduct can or should install it (Aqueduct never installs anything). Five statuses: `not_applicable` (no `declare_dependency` op in the patch; no check owed), `pass` (already installed; auto-apply eligible), `warn` (resolves on PyPI but is not installed; **a deliberate defer to a human**: install it, then `aqueduct patch apply <id>`; a `warn` here **never auto-applies**), `fail` (no such package on PyPI, or no published version satisfies the specifier; rejection, feeds the reprompt loop), `unavailable` (the PyPI check itself could not run; fail-closed, same posture as Gate 3's `unavailable`). Multiple `declare_dependency` ops in one patch: every requirement is checked and the WORST verdict wins, ranked `fail` > `unavailable` > `warn` > `pass`.

§8.7 below describes the same sequence in full detail. If the two ever disagree, the code wins: `_check_guardrails` is Gate 1, `run_lineage_gate` is Gate 2, `run_sandbox_gate` is Gate 3, `run_resolvability_gate` is Gate 4.

**Depot staleness notice (2.69).** A failing run may itself have written to the Depot (an Egress with `format: depot`) before it failed, so by the time Gate 3 recompiles the patched Blueprint for its sandbox replay, a depot-derived value the failure saw may have moved. When the caller passes the depot reads resolved at failure time (`_CompileResult.depot_reads`, threaded from `aqueduct run`'s own compile), `run_sandbox_gate` recompiles with its own depot-read sink and, for every key present in BOTH maps whose value differs, prints `depot key 'X' changed since failure: 'old' → 'new'` to stderr and folds the same line(s) into a `pass` result's `detail`. A key present on only one side is not a staleness signal (the patch may have added or removed a depot reference) and is skipped. This is purely informational: it never changes the gate's `status`, never blocks auto-apply, and is not built on any new snapshot/versioned-depot store — it is a diff of two already-resolved Tier 1 reads.

**Sandbox gate on a polyglot Blueprint (2.37).** The sandbox gate replays through one target engine's own `ExecutorProtocol`: a single session, a single engine. Against a Blueprint compiled to more than one island (§4.3's cross-engine handoff, §10.9), that shape can only ever validate ONE of the Blueprint's engines, which would look like a real pre-apply check while actually covering nothing about the rest. So it does not attempt a partial or single-engine-shaped replay: it returns `unavailable` immediately, and **that blocks auto-apply** (2.63); like a missing engine dependency, this is a replay that was owed and could not happen, so the patch stops for a human rather than going through unverified. The run prints the reason at the moment it happens (not only into `patch_simulation`) because a user who expects every patch to be sandbox-replayed before it touches their Blueprint needs to be told this one wasn't. `--sandbox`'s whole-Blueprint dry-run refuses a polyglot Manifest outright for the same reason (`CONFIG_ERROR`); it runs on any single-engine `deployment.engine` that declares the `tooling.sandbox_dry_run` capability leaf (both shipped engines do), refusing loudly with the leaf's hint for one that does not. A genuine multi-session polyglot replay is future work, not this release.

### 7. Confirm and write

Only after every gate passes does the patch run against the real pipeline. The on-disk Blueprint is rewritten only if the full re-run succeeds. Failed patches stage to `patches/pending/` for inspection.

### 8. Chained multi-patch healing

`agent.max_patches` (default `1`, Blueprint-only) is the ONE counter for the whole heal: a single-attempt heal at the default, or, with `max_patches: N` (N > 1, `agent.approval: auto`, non-cascade path), chained multi-patch healing — the standard (and only) behavior of the multi-patch loop as of 2.78. There is no separate opt-in flag.

**Motivation.** A naive "N independent retries of the same failure" loop re-diagnoses the SAME first failure on every attempt: it applies a candidate patch in memory, re-runs the pipeline, and, when the re-run still fails, discards the candidate and re-executes against the *original, unpatched* Blueprint. With two or more independent bugs, the model can diagnose bug #1 correctly on every single attempt and it is thrown away every time, because the pipeline still fails downstream at bug #2 and nothing carries bug #1's fix forward. Chaining fixes this.

**Chain semantics.** On a candidate patch that validates in memory but still leaves the pipeline failing, the loop checks *where* the new failure surfaced:

- **Different module than the one just patched** → the candidate was right: it is folded into an accumulating multi-op `PatchSpec` (operations concatenated in link order) and the loop advances to diagnose the new failure.
- **Same module again** → the candidate was wrong: only THAT candidate is discarded (the already-proven accumulated patches are kept) and the SAME failure is retried.

Every LLM diagnosis call spends one unit of `max_patches`, regardless of outcome — advance, same-module discard, or gate rejection. The loop ends when the pipeline is fully solved, `max_patches` is exhausted, the model returns no patch, or `agent.on_heal_failure: abort` fires on a same-module retry.

Each attempt's diagnosis and the pending-patch check (§8.2 step 2) operate independently: an attempt's failure has its own error signature (§8.6) and its own `blueprint_id` check.

**Disk invariant.** Nothing is written to the Blueprint until the FULL accumulated patch passes the pipeline end-to-end. There is never a partial/half-correct Blueprint on disk mid-chain: every intermediate apply is the existing in-memory apply path (`_apply_patch_in_memory`), re-applied against the *original* on-disk Blueprint with the growing operation list, never against a previously-written file. Exactly ONE combined `PatchSpec` is ever staged or written for a given heal — never a chain of separate staged patches.

**Sandbox requirement.** `agent.max_patches > 1` requires `agent.sandbox_mode` other than `"off"` — refused at run-start (`CONFIG_ERROR`) otherwise, since each attempt's advancement test IS the sandbox gate. A single-attempt heal (`max_patches: 1`, the default) is unaffected — `sandbox_mode: off` stays legal there.

**Gates.** Each attempt's validation IS its advancement test: the existing in-memory apply + full pipeline re-run (the same gate `full_run` patch validation already performs for a single patch, just invoked once per attempt against the growing accumulated patch). The final combined multi-op patch that solves the pipeline still runs the standard gate pyramid before being written: no gate is skipped, only the *per-bug* diagnosis loop is new.

**Approval composes once.** Because nothing hits disk mid-chain, `agent.approval: auto` applies the combined patch after the final full-run pass, one write, not N. `human`/`ci` modes stage exactly ONE combined patch (with each attempt's rationale folded into the staged patch's `rationale` field) instead of cycling through N separate pending-patch reviews.

**Cascade scope.** Multi-model cascade (§8's cascade model) never chains — each cascade tier still produces at most one patch per attempt, bounded by `max_patches`, and a rejected cascade-tier patch is not folded into an accumulating multi-op patch. Chaining is exclusive to the single-model (non-cascade) path described above.

## **8.3 Approval modes**

| Mode | Who applies the patch | When it changes the Blueprint |
| :- | :- | :- |
| `disabled` | LLM never fires | Never |
| `human` | Engineer reviews and applies | Only after human accepts |
| `ci` | External CI receives patch and opens a PR | Only after merge |
| `auto` | Aqueduct applies in-memory, re-validates, writes only if the re-run succeeds | Only on a successful re-run |

Low-confidence patches and any guardrail violation auto-escalate to human review.

**`auto` requires an explicit path allowlist (2.2.0, breaking).** Because `auto` is the only mode where a patch writes to the Blueprint with zero human review, Gate 1 refuses every file-touching patch operation when `agent.guardrails.allowed_paths` is unset, instead of allowing any path. Set `allowed_paths` to fnmatch patterns naming where a heal may write, or use `human` so a person reviews the patch first. See §8.7's Gate 1 paragraph and `docs/threat_model.md`.

**Config key.** `agent.approval` is the config key. Values: `disabled`, `human`, `auto`.

**CI hand-off via `on_patch_pending`.** In `human` mode the patch is staged to `patches/pending/` and the `on_patch_pending` webhook fires. The engine ships **no** long-running receiver and **no** versioned GitHub Action, a CI runner you own receives the payload, obtains the patch body (a run artefact, or `aqueduct patch pull`), and applies + commits it in one step:

```bash
aqueduct patch import received-patch.json --blueprint pipeline.yml
```

`patch import` is `patch apply` + `patch commit` atomically (`--no-commit` stages only), writing a structured `---aqueduct---` commit trailer that `aqueduct patch log` / `rollback` read back. The webhook payload schema (envelope keys `patch_id` / `run_id` / `blueprint_id` / `failed_module` / `source` plus the body's `_aq_meta`) and a copy-paste example workflow wiring `import` + `gh pr create` are documented in the **[Production Guide](production_guide.md)**.

**Heal-as-PR (2.2.0).** `aqueduct patch pr <patch_ref>` branches, applies, commits, pushes, and opens a PR in one command, approval-mode-agnostic, as an alternative to the manual webhook-plus-import flow above. See [CLI Reference](cli_reference.md#5-patch-management).

## **8.4 Sandbox modes**

| Mode | Sample size | Egress writes | Danger gate |
| :- | :- | :- | :- |
| `sample` (default) | 1000 rows per Ingress | dropped | n/a |
| `preflight` | full dataset | dropped | `danger.allow_full_preflight: true` |
| `off` | no replay | writes for real | `danger.allow_skip_sandbox: true` |

## **8.5 Patch grammar**

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

Supported operations: `set_module_config_key`, `replace_module_config`, `replace_context_value`, `replace_module_label`, `insert_module`, `remove_module`, `add_probe`, `replace_edge`, `set_module_on_failure`, `replace_retry_policy`, `add_arcade_ref`, `defer_to_human`, `set_engine_config`, `replace_macro`, `declare_dependency`.

`defer_to_human` signals an unhealable failure. It makes zero Blueprint changes and terminates the loop with `stop_reason='deferred'`. The payload carries `diagnosis`, `suggestions`, and `confidence_reason` for human review, plus a required `defer_reason` enum: a queryable bucket for WHY the failure was deferred, distinct from the free-prose fields: `infrastructure` | `upstream_schema_change` | `data_shape_change` | `insufficient_context` | `other`. An invalid or absent value is a pydantic `ValidationError`, which feeds the normal reprompt loop (not a hard failure). `defer_reason` round-trips into `heal_attempts.defer_reason` (see the [Observability Guide](observability_guide.md)) and, when `webhooks.on_defer` is configured, into that webhook's payload alongside `confidence_reason`: a dedicated event so defers stop overloading `on_patch_pending`; unset falls back to firing `on_patch_pending` unchanged. Opt-in via `agent.allow_defer: true`, when false (default), the op is hidden from the LLM prompt.

In `agent.approval: auto`, a **defer-only** patch (every operation is `defer_to_human`) short-circuits straight to the pending/defer staging path, skipping the sandbox replay, the gate ladder, and the apply step, since a defer makes zero Blueprint changes and running the full validation pyramid on it is a pure no-op. A **mixed** patch (a Blueprint-mutating op alongside a defer) still runs the full gate ladder.

`set_engine_config` sets a single key in one engine's Blueprint-level `engine.<engine>:` block (**BREAKING, replaces the engine-named `set_spark_config`; removed, no back-compat alias**). It carries an `engine` field plus `key`/`value`, and addresses BOTH shapes an `engine.<name>:` block can take, using the same structural rule the parser applies when reading the block back (`_resolve_engine_block_raw`): does that engine's block schema declare a `conf` field? Spark's does (a free-form bag, e.g. `spark.sql.shuffle.partitions`): `key` is an opaque vendor config name written into `engine.spark.conf.<key>`, auto-created if absent; covers OOM, shuffle fetch failures, Kryo buffer overflow, dynamic allocation thrashing, GC issues, and driver MaxResultSize, seven of the 20 most common Spark errors. DuckDB's does not (its block declares typed fields directly (`memory_limit`, `threads`)) so `key` must name one of those fields exactly; an unrecognised key is rejected rather than silently writing a field nothing reads. A third engine is addressed correctly the moment its own block schema exists, with no change to the apply path. A stored patch body still carrying the retired `set_spark_config` tag raises `RetiredPatchOpError` (an `AqueductError` subclass) when re-parsed by `aqueduct patch apply`, a typed, distinguishable failure rather than a generic parse error.

**Permission model.** `set_engine_config` is **allowlist-gated at Gate 1**, in every approval mode including `auto`: nothing in the compiler or capability framework constrains `engine.<name>.conf`/typed fields otherwise (`engine.spark.conf` in particular carries no capability leaf at all), so the allowlist is the only thing standing between a heal and an arbitrary engine key. A write of `(engine, key, value)` is permitted iff: **(1)** no core deny entry matches `key`, or the value, for a `deny_values` entry; the deny layer ships in each engine's `engine_config_allowlist.yml` inside the wheel, and no configuration surface (Blueprint, `aqueduct.yml`, a future `danger.` flag) may extend, shrink, or override it; **AND (2)** `key` matches that engine's core allowlist (or, when built, a `danger.`-gated operator extension; not yet implemented); **AND (3)** `key` survives operator narrowing (reserved for a future `aqueduct.yml` surface, not yet built); **AND (4)** the Blueprint's own `agent.guardrails` permit the op (`forbidden_ops` can still block `set_engine_config` outright, independent of allowlist membership). Every layer below core may only *subtract* permission from what core allows: nothing but the core allowlist and, once built, the operator extension, ever *adds* to it. Ownership is tiered: **core** owns the envelope and the deny families (engine semantics; cluster placement, credentials, TLS, arbitrary code loading); the **operator** may extend or narrow further in `aqueduct.yml` (a Blueprint must never grant itself power beyond what the operator installed; the same reasoning as `danger.allow_command_hooks`); the **Blueprint author** controls `forbidden_ops`, `allowed_paths`, `deny_patterns` (evaluated after `allowed_paths`, over the same resolved value; subtract-only; applies even when `allowed_paths` is empty), error filters, approval mode, and confidence thresholds; i.e. may only restrict further, never expand what core/operator already permit. Violations raise `PatchError` (a patch problem: the fix is a different patch); a malformed or missing shipped `engine_config_allowlist.yml` raises the distinct `EngineConfigAllowlistError` (a data problem; the fix is repairing/shipping the file, never retried as a patch). See `aqueduct/executor/engine_config_allowlist.py` and `aqueduct/patch/apply.py::_check_guardrails`. The policy this paragraph describes ships inside the wheel with nothing printing it: `aqueduct patch policy [--engine <name>] [--format text|json]` reads the same allowlist Gate 1 evaluates against and prints the allowed key patterns (with type/enum/range) and the denied families (with their `reason`), per engine; a Gate 1 rejection names this command so the policy is always one command away, not a rule a user has to go read the wheel to find. Since operator extension/narrowing of this policy is not yet built (item **(3)** above), the command's output is the complete policy, not a filtered view of one. The same policy is also disclosed to the healing model itself: the composed system prompt carries an "Engine/session config (`set_engine_config`)" section rendering the TARGET engine's whole allowlist (every allowed key with its type and any `enum`/`range`, every denied family with its `reason`) read from the same file Gate 1 evaluates against, so a rejection means the model made a genuine error rather than guessed at a list nobody showed it. An engine shipping no `engine_config_allowlist.yml` (or an explicitly empty one) is told in the prompt that the op is unavailable for it, because no `set_engine_config` write can clear Gate 1 there; rendering an empty table instead would invite a write that is always refused.

**Efficacy check: an inert config write is refused.** Clearing the permission model above says a write is *allowed*, not that it *does* anything. The config an engine runs with is a merge (`aqueduct/executor/session_config.py::resolve_session_engine_config`): that engine's `aqueduct.yml` `engine.<name>` block, with the Blueprint's own `engine.<name>` entry layered on top, so the Blueprint wins on a key both set, and this invocation's `-s/--set` wins over both (§10.4). A write whose value is already what resolves therefore applies cleanly and changes nothing: schema-valid, allowlist-clean, lineage and sandbox gates green, one heal attempt spent, engine behaviour identical. Gate 1 now also resolves the effective session config before and after the patch and **refuses** a patch that writes engine config but produces an empty delta, raising `PatchError` (the same class as an allowlist violation, and for the same reason: the fix is a different patch, with a different key or a different value). Values are compared as an engine session sees them, so re-spelling `400` as `"400"` counts as no change. The check runs on every apply path (`aqueduct run` self-heal, `aqueduct heal`, `aqueduct patch preview`, `aqueduct patch apply`/`import`, benchmark scenario replay) because it lives in the same shared Gate 1 function they all call. **When the nullifier is the user's own `-s/--set`** the refusal says so explicitly, naming the flag path and the value it pins, and does not offer the ordinary "write a different value" advice: no Blueprint value can outrank a `--set`, so the resolution belongs to the user, not to the model. A patch that moves some keys and writes others the invocation pins still passes on the strength of the ones that move, with the pinned ones reported in the gate's detail and its `cli_pinned` field rather than folded into a clean verdict. `aqueduct patch preview` accepts no `--set`, so its engine-config verdict is always measured with no pins (`cli_pinned` is emitted as `{}` rather than omitted, so a consumer can tell that apart from an older report).

Applicability is derived from what the patch writes, never from a list of operation names: the patch's operations are re-applied to a copy of the Blueprint whose `engine:` block has been removed, and whatever appears in that block afterwards is exactly the set of engine-config keys the patch writes. A patch that writes none of them reports `not_applicable`: the same first-class status the lineage gate uses (`aqueduct/patch/preview.py::LineageGateResult`) for the same reason: reporting `pass` for a check that had nothing to look at is a lie. `aqueduct patch preview` renders this gate next to the lineage/sandbox gates, and `--format json` carries it as `engine_config` (`status`, `detail`, `delta`, `write_targets`).

**Where the delta is recorded.** When the effective config does change, the diff is recorded in the `patch_index` table of the observability store, keyed by `patch_id`, shaped `{engine: {key: {before, after}}}`. It is built by `aqueduct/patch/provenance.py::build_heal_provenance` and written by `aqueduct/patch/apply.py::record_heal_facts`. It is recorded there, and not in the patch's own `_aq_meta`, because it is an apply-time fact rather than a generation-time one: the same patch applied against a different `aqueduct.yml` produces a different delta, and the model that wrote the patch saw neither. It is recorded in the patch index rather than the Blueprint because the Blueprint carries only what a travelling artifact's compile-time gate must read, while a before/after config dict is store data that a growing history should not force into the artifact itself. It is surfaced with `aqueduct doctor` (the `healed-config:<patch_id>` rows) instead. No row is written for a patch that writes no engine config, so a pipeline-only heal leaves the patch index's engine-config columns unset.

`replace_macro` replaces the body of an **existing** macro in the Blueprint `macros:` block, the one place bad SQL was previously unreachable, since the agent is told to preserve `{{ macros.* }}` references rather than inline them. Replace-only: unknown macro names are rejected at apply time (also catches name hallucinations). Re-expansion runs through the normal compile + lineage gates, so parameter mismatches and broken columns in *any* consuming module are caught before the patch lands. Because one macro change affects every module referencing it, the recommended default is to add `replace_macro` to `guardrails.forbidden_ops` so it always gets human review.

`declare_dependency` (2.66) carries one PEP 508-lite `requirement` string, validated at construction with the same parser `dependencies:` uses (§5.5); a malformed string is a pydantic `ValidationError`, never something that reaches the gate or apply path. Applying it appends the requirement to the Blueprint's top-level `dependencies:` list; append-stable, deduped on exact string match, creating the block when absent. It writes ONLY that dict key: never `requirements.txt`, never `pyproject.toml`, never the running environment, and never shells out to `pip`; the same declare-and-check story as `dependencies:` itself, at healing time instead of authoring time. Whether the declared requirement is actually resolvable is answered by Gate 4 (§8.2), not by this op.

**`deep_loop`:** when `agent.deep_loop: true`, sandbox/lineage gates run inside the LLM conversation so the model sees rejection feedback and retries in-context before `apply_callback` runs. Default false preserves the current post-hoc gate behavior.

### Metadata field tolerance

PatchSpec is **strict on operations, lenient on metadata.** Operation-level fields (`op`, `module_id`, `key`, `value`, `config`, …) mutate the Blueprint, so each Op model enforces `extra="forbid"`, a typo there bounces the patch. Top-level metadata fields (`rationale`, `root_cause`, `confidence`, `category`, `patch_id`) are descriptive only; the parser tolerates casing variants and synonym aliases so cheap models don't burn reprompt budget on cosmetics:

| Common LLM variant | Normalised to |
|---|---|
| `rootCause`, `rootcause`, `cause`, `rootCauseAnalysis` | `root_cause` |
| `reasoning`, `reason`, `description`, `summary`, `explanation` | `rationale` |
| `patchId`, `patchID` | `patch_id` |
| `runId`, `runID` | `run_id` |
| `Confidence`, `score` | `confidence` |
| `Category`, `failure_category`, `failureCategory` | `category` |

Anything else that doesn't fit a known top-level field is moved into `misc: dict[str, Any]` rather than rejected, the LLM's stray `"examples"`, `"notes"`, or `"verified_by"` field is preserved for post-mortem visibility but does not participate in mutation. The `misc` field is persisted alongside the patch in `patches/applied/*.json`.

## **8.6 FailureContext structure**

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

A generated patch clears four numbered gates plus a compile-check, in order, before it is ever written into the Blueprint, first failure wins and the patch is discarded or escalated to human review:

```
✓ guardrails  →  ✓ compile-check  →  ✓ lineage  →  ✓ sandbox  →  ✓ resolvability  →  patch applied
```

Gate 1 (guardrails) is deterministic policy: `agent.guardrails.forbidden_ops`, `allowed_paths`, `deny_patterns` (evaluated after `allowed_paths`, over the same resolved value; subtract-only, so it applies even when `allowed_paths` is empty), minimum confidence, enforced by `patch/apply.py::_check_guardrails`. **Under `agent.approval: auto` (2.2.0), an empty `allowed_paths` is deny-by-default rather than allow-all.** `auto` is the only mode where a patch applies with zero human review, so a file-touching op (`set_module_config_key`/`replace_module_config`/`insert_module`/`add_probe`/`add_arcade_ref` writing a `path` or `output_path`) is refused outright when no allowlist is configured, naming the offending value and pointing the operator at `agent.guardrails.allowed_paths` or a switch to `human`. `human` keeps the historical empty-means-unrestricted behavior, since a human reviews the patch before it applies. For `set_engine_config` specifically, Gate 1 also enforces the target engine's core `engine_config_allowlist.yml`: deny entries first (a key/value match raises naming the deny entry's `reason`), then allow-list membership (fail closed: a key on no allow entry is refused), then type, then `enum`/`range` when the matched entry declares one; see the permission-model paragraph above and `aqueduct/executor/engine_config_allowlist.py`. The compile-check (`patch/apply.py::apply_patch_file`, re-parses the patched Blueprint) rejects any PatchSpec whose operations produce a Blueprint that no longer passes the Parser; it runs immediately after Gate 1 but is not itself numbered (AGENTS.md and the module docstrings in `preview.py`/`resolvability_gate.py` number only the four gates below). Gate 2 (lineage, `patch/preview.py::run_lineage_gate`) checks whether the patch breaks a downstream column consumer via live `sqlglot` analysis; a patch whose operations touch zero modules (e.g. `set_engine_config`, which carries only `engine`/`key`/`value`) has no lineage surface to check at all, so the gate reports `not_applicable` with a reason rather than the misleading `pass` a patch that WAS checked and found clean also reports: informational only, it never blocks the patch. Gate 3 (sandbox, `patch/preview.py::run_sandbox_gate`) replays the patched Blueprint against representative data (a per-Ingress row sample by default, no live writes), building its owned session's engine config through the SAME resolver (`aqueduct.executor.session_config.resolve_session_engine_config`) `aqueduct run` uses; so a replay against a non-Spark engine sees that engine's real `engine.<name>.*` config (DuckDB's `memory_limit`/`threads`/`database_path`/`extension_repository`/`s3_*` and any httpfs/secrets wiring) instead of a Spark-only default. For the same zero-module patches Gate 2 reports `not_applicable` for, Gate 3 still runs and still reports `pass` on a clean replay, but words the `detail` honestly rather than letting it read as a validated fix: the session built and the sample replayed successfully under the PATCHED engine config, but a small local sample cannot reproduce the cluster-scale resource failure (OOM, shuffle spill) the patch is usually trying to fix; only the full re-run proves that. `gates_passed` is unaffected; only the `detail` string (also persisted to `patch_simulation`, so any downstream reader inherits the same honest wording) changes. Gate 4 (resolvability, `patch/resolvability_gate.py::run_resolvability_gate`, 2.66) is the odd one out in the pyramid: it never touches the Blueprint or the engine, it only asks PyPI whether every `declare_dependency` op's requirement is resolvable. `not_applicable` when the patch declares nothing; otherwise `pass`/`warn`/`fail`/`unavailable` per requirement, worst-wins across multiple requirements. Its `warn` is unlike every other gate's: it is not advisory, it is a hard defer; a `warn` here always routes the patch to pending/human review, never auto-apply, because the requirement genuinely is not yet satisfied in this environment and Aqueduct will not install it. `aqueduct patch preview --sandbox` runs the same pyramid on demand, before an operator decides whether to apply.

- **No silent mutations.** Every patch is a structured diff with a rationale and a confidence score. Low confidence escalates to human review.
- **No production data corruption.** The sandbox validates patches against representative data before they reach live writes.
- **No runaway loops.** Budgets bound wall-clock, tokens, and stuck-signature counts. A rolling rate-limit caps healing attempts per hour per blueprint.
- **No black-box decisions.** Every LLM turn persists with the gate that rejected it, a stable error signature, and the prompt version.

## **8.8 Drift detection (`aqueduct drift`)**

`aqueduct drift` is an **early warning** you can schedule ahead of the batch
(e.g. cron, 30 min before the nightly job) — it detects and reports upstream
schema drift; it does not heal anything. Healing stays entirely with `run`'s
self-heal, which fixes a pipeline *after* it actually fails; `run` itself is
untouched by `drift`.

Per Ingress, `drift`:

1. Reads the **live source schema metadata-only** (`df.schema`, zero Spark
   actions; parquet/delta from the footer/`_delta_log`, JDBC via a `LIMIT 0`
   probe).
2. Diffs against a **self-owned baseline**: the last-seen schema in
   `drift_checks`. No baseline yet ⇒ it stores the current schema and exits
   cleanly (no Probe dependency).
3. **Classifies** each change: a *dropped* or *type-changed* column is
   **breaking** (a downstream Channel that names it will fail); an *added*
   column is **benign** (a `SELECT named_cols` pipeline tolerates a superset).
   Both are recorded in `drift_checks` and printed in the report; neither
   triggers a heal — a breaking change is left for the next real `run` to
   catch and self-heal reactively.

Scope is **schema drift only**: value-distribution / data-quality drift is out
of scope (a noisier, separate concern). Exit codes: `0` (no drift / baseline
set / only benign drift), `DATA_OR_RUNTIME` (a breaking change was found, or a
source could not be read/diffed).

## **8.11 Remediation domains**

Aqueduct's self-healing operates in explicit **remediation domains**, a
domain is the boundary of what a patch is allowed to touch. Two are built:

| Domain | The patch edits | Its permission model |
| :- | :- | :- |
| `pipeline` | The Blueprint's modules, their config, and the edges between them, via a `PatchSpec` op (§8.5) | The Blueprint's own `agent.guardrails` (`forbidden_ops`, `allowed_paths`) |
| `engine_config` | An engine's session config, via `set_engine_config` writing the Blueprint's `engine.<name>` block | The target engine's core allowlist plus the effective-config delta check (§8, "Efficacy check") |

Every domain, present or future, follows the same principle:

- **Declarative, typed operations**: a fixed, closed grammar of ops (never
  freeform code generation; §1.4's "patch grammar over codegen" principle).
- **Per-domain validation gates**: guardrails, lineage-impact, sandbox
  replay, and resolvability gates (§8.5) sit between "the LLM proposed a
  patch" and "the patch is applied," scoped to what that domain can break.
- **Never freeform code execution**: a domain's operations are data, not
  code; the agent cannot ship an arbitrary script to remediate a failure.

Framing self-healing this way keeps each domain's contract explicit and
gives any domain added later the same shape to slot into, rather than a
one-off extension of the patch grammar.

**A domain is a property of the FIX, not of the failure.** The same failure
is often reachable from more than one domain: an executor OOM on a large
shuffle is fixed either by raising the shuffle-partition count
(`engine_config`) or by inserting a repartition step (`pipeline`). Anything
that classifies work by domain therefore has to allow more than one, and
must not group failures by domain. A benchmark scenario
(`.aqscenario.yml`) declares the domains its expected fix may touch in a
`domains:` list, and `aqueduct benchmark --domain <name>` selects on it; a
scenario declaring both is the normal case for a failure with two valid
fixes, not an ambiguity to be resolved. See
[`gallery/aqscenarios/README.md`](../gallery/aqscenarios/README.md) for the
scenario file format.

## **8.14 Heal-patch cross-engine provenance**

The healing system prompt is engine-flavored by design (§8's composed-prompt
rule: each engine registers its own `PromptRules` pack). A patch generated
while healing a DuckDB run can therefore carry DuckDB-dialect SQL, cast
syntax, or format options; if the same Blueprint is later compiled for
Spark, that content may be wrong there. Without provenance, healing could
silently manufacture a production defect on a different engine than the one
it was validated against.

**The `healed_by:` block.** `aqueduct patch apply` (and the `agent.
approval: auto` direct-write path) appends one record per applied patch to a
top-level `healed_by:` list on the Blueprint YAML, machine-written via the
same ruamel round-trip machinery as every other patch operation: Blueprint
authors never hand-write it:

```yaml
healed_by:
  - patch_id: fix-yellow-taxi-path
    engine: duckdb
    classification: engine_shaped   # dialect_neutral | engine_shaped
    applied_at: "2026-07-18T00:00:00Z"
    validated_on: []          # engines a GREEN run has validated this patch on since
    reverted_at: null         # set by `aqueduct patch revert`; absent on a live record
```

The record is bounded to these six fields: only what the compile-time
cross-engine gate and `aqueduct patch revert` need to read out of a
travelling Blueprint. `engine_config_delta`, `engine_version`,
`perf_baseline`, `perf_observations` and `run_id` moved to the `patch_index`
table in the observability store, keyed by the same `patch_id` (see
"Where the delta is recorded" above and "Perf attribution" below): those
fields grew with every green run, one before/after config dict per engine
and one perf note per engine per patch, and turned a Blueprint artifact into
a changelog. There is no migration and no back-compat read: a `healed_by`
record still carrying one of the moved fields fails schema validation,
naming the field and stating that its data now lives in the patch index.

The block is compiler-consumed metadata only: no engine executes it, and it
is excluded from `Manifest` assembly entirely, so it never perturbs a
compiled Manifest's content or checkpoint hash.

**Classification.** Every PatchSpec op (§8.5) is classified once, in
`aqueduct/patch/provenance.py`, as `dialect_neutral` (retry/timeout,
structural rewiring, resource/config numerics, schema hints: safe to carry
across engines unmodified) or `engine_shaped` (can introduce SQL text, cast
syntax, or format/session config). `set_module_config_key` is
field-sensitive: classified by whether the config key it touches is
dialect-bearing (`query`, `sql`, `format`, `mode`, `options.*`, …) or not
(`path`, retry counts, …). A patch's overall classification is the max over
its operations: one `engine_shaped` op makes the whole patch
`engine_shaped`.

**Compile-time gate.** For every `healed_by` record whose `classification`
is `engine_shaped`, whose `engine` differs from the compile's target engine,
and whose `validated_on` does not yet include the target engine, `compile()`
emits a suppressible warning (rule_id `cross_engine_heal`) naming the patch,
its origin engine, and the target engine. `dialect_neutral`-only records
never warn: they carry no dialect content to be wrong about. This is a
warning, not an unconditional error: healing's value is a shippable
Blueprint, and a human/CI reviewer decides whether to ship anyway.

**Strict escalation.** `warnings.strict` (`aqueduct.yml`, a list of
`rule_id`s, default empty) promotes listed rules from warning to a hard
`CompileError`: the same rule_id vocabulary as `warnings.suppress`, in the
opposite direction. Setting `warnings.strict: [cross_engine_heal]` makes an
unvalidated cross-engine heal fail the build.

**Self-clearing.** A GREEN run on engine X (the CLI `run` command's success
path) appends X to every `healed_by` record's `validated_on` list, when the
block exists and X is not already present: a Blueprint with no `healed_by:`
block is never touched. The stamp is best-effort: a write failure is logged
and never fails an otherwise-successful run (`aqueduct/patch/apply.py::
stamp_validated_engine`). A green run rewrites the Blueprint only when
`validated_on` actually changes: a run that adds no new engine to any
record leaves the YAML untouched.

**Perf attribution (warn-only).** `validated_on` is binary: the run after
the patch either succeeded or it did not. Config-op success is not binary.
The usual outcome of naive shuffle or partition tuning is a run that
completes and is much slower, which `validated_on` records as an
unqualified success while the patch persists into the Blueprint and every
later run inherits it. Two fields carry the non-binary half.

`perf_baseline` and `perf_observations` are recorded in the `patch_index`
table, not in the Blueprint (see "Where the delta is recorded" above):
they are apply-time and green-run facts, and a Blueprint artifact is not
the place for a history that grows with every run.

`perf_baseline` is snapshotted at apply time: the last green run of this
blueprint that finished before the patch was applied (wall-clock duration
from `run_records`, plus a volume proxy summed from `module_metrics`). It
is snapshotted rather than looked up later because the Blueprint travels
and the observability store does not.

`perf_observations` is written by the same green-run stamp that appends to
`validated_on`, once per engine, so the list is bounded by the engine count
rather than the run count. Each note is `observed` (the ratio, both
durations, and its own caveats) or `not_applicable` (which fact was
missing). There is no `pass` member, because nothing is judged, and no
`fail`, because nothing can fail: **Aqueduct sets no regression threshold.**
There is no measurement behind a number like "3x is a regression", so the
observed ratio is reported and a human decides. The note never blocks a
run, never changes acceptance, and never affects an exit code.

Two runs of one Blueprint are not automatically comparable, and the note
says so rather than implying a causation it cannot support. A baseline
whose engine set differs from the observing run's is refused outright
(`not_applicable`) rather than compared; `run_records` carries no `engine`
column, so the engine set is derived from the per-module `engine` its
`module_results` JSON already records. The input-volume proxy comes from
`module_metrics`, which the Spark executor writes per module and the DuckDB
executor writes only for Handoff modules, so on DuckDB it is reported as
unavailable with a stated reason, never as a zero. Every remaining
limitation (co-applied patches, changed input volume, the standing fact
that wall-clock time has many causes) is written into the note's `caveats`,
which travel with it into the patch index record. See
`aqueduct/patch/perf_attribution.py`.

**Undoing a heal (`aqueduct patch revert`).** A healed patch persists into
the Blueprint and every later run inherits it, including runs long past the
failure it was written for. `aqueduct patch revert <patch_id> --blueprint
<file>` undoes one applied patch's engine-config writes in place: each
`engine.<name>` key the patch wrote goes back to the value the
`engine_config_delta` recorded in the patch index captured, and the
`healed_by` record is stamped `reverted_at:` rather than deleted. Because
the prior value now lives in the patch index rather than in the record
itself, an unreachable observability store is a loud refusal, never an
empty mapping: a missing or unreadable index is indistinguishable from
"nothing was recorded" unless the command insists on telling the two
apart. Keeping the record is the point: deleting
it would erase the fact that a heal ever happened, and leaving it unmarked
would make it describe a Blueprint that no longer carries its change. Every
consumer reads the stamp, so a reverted record stops raising the cross-engine
warning above and stops accruing `validated_on` entries.

Only engine-config writes are revertible, because they are the only change
for which a prior value is recorded anywhere: `set_module_config_key` and its
siblings store the new value alone, so a module patch has no inverse to
compute. A revert is therefore not itself a patch, and the PatchSpec grammar
gains no op for it (the inverse of a write whose prior state was "absent" is
a key deletion, which no op expresses).

The command refuses, naming the reason and writing nothing, when: the patch
also carries a non-config operation (undoing half of it would leave a
Blueprint matching no state that ever ran); a later, not-itself-reverted
patch wrote one of the same keys (revert in reverse order, or use `patch
rollback`); the value has been edited since the patch was applied; the patch
id matches zero or more than one record; or the computed restore cannot be
shown to reproduce the recorded pre-patch effective config exactly. That last
check is mechanical rather than argued: the plan is applied to a copy and
re-resolved through the same function Gate 1 measured the delta with, and any
key that lands anywhere other than its recorded prior value: or any key the
patch never wrote that moves at all: aborts the revert.

`aqueduct patch rollback <blueprint> --to <patch_id>` remains the whole-file
counterpart: it restores the Blueprint from git history, undoing everything
in that commit, and is the documented fallback for every case `revert`
refuses.

**Surfacing healed config keys (`aqueduct doctor`).** For a Blueprint target,
doctor reads the engine-config delta and the perf notes back from the patch
index, keyed by `patch_id`, and emits one `healed-config:<patch_id>` row per
`healed_by` record whose patch index entry carries an `engine_config_delta`:
what was changed and when, whether a green run has validated it, the perf
notes' observed ratios verbatim, and the `patch revert` command that undoes
it. An unreadable patch index is a single `warn` row naming the reason,
never silence. It states **no staleness threshold**:
"healed more than N days ago" is a number nothing supports, since an
hour-old heal on a monthly pipeline is older in every sense that matters than
a year-old one on an hourly pipeline. The one condition that escalates to a
warning is an equality, not a threshold: the value the record says the patch
wrote is no longer what the effective config resolves to, so the record's
perf attribution no longer describes the live configuration and `patch
revert` will refuse it.

**Sandbox requirement.** Chained multi-patch healing (`agent.max_patches >
1`) refuses to run with `agent.sandbox_mode: off`: a `ConfigError` at
run-start with an actionable message. Each attempt's advancement test
depends on validating a candidate before it is folded into the chain;
without sandbox validation there is no safe way to tell "advanced" from
"about to compound a wrong fix." `sample` (default) or `preflight` are
required. A single-attempt heal (`max_patches: 1`, the default) is
unaffected — `sandbox_mode: off` stays legal there.

**Observability.** `heal_attempts` (see `docs/observability_guide.md`)
carries a `chain_link` column: the 1-based index of which attempt within
the heal an LLM-diagnosis row belongs to. `attempt_num` still carries the
reprompt sequence *within* one attempt; `chain_link` is an orthogonal
axis. Token totals aggregate across all attempts into the single
`healing_outcomes` row the combined patch produces.

**Scope note.** Chaining is wired into the single-model (`agent.approval:
auto`, non-cascade) heal path only. Multi-model cascade (§8's cascade
model) never chains: a cascade tier's escalation semantics and a chain's
advancement semantics are two independent axes that have not been
reconciled — each cascade tier still produces at most one patch per
attempt.

---

# **9. Type system**

## **9.1 Aqueduct invents no types: it adopts Arrow's**

Wherever a Blueprint names a column type (Ingress `schema_hint`, Channel `op: cast`, UDF `return_type`), it writes a spelling from Aqueduct's own type vocabulary (`aqueduct/typehub.py`, "the hub"), not a raw engine-native string. The hub is not invented: it borrows Apache Arrow's type semantics for the constructors it defines, because Arrow already solved the one distinction that matters most here (an instant vs. a naive wall-clock value, below). It is deliberately a **subset** of Arrow's full taxonomy, not a mirror (no unions, no dictionary or run-end encoding, no fixed-size lists), just the constructors both a distributed engine (Spark) and a single-node columnar engine (DuckDB) can implement. There is no `pyarrow` dependency anywhere in this: the hub borrows Arrow's semantics, not Arrow's code.

Every hub type is a value type describing comparison and storage semantics, never an engine spelling:

| Constructor | Canonical spelling | Semantics (Arrow-borrowed) |
| :- | :- | :- |
| Boolean | `boolean` | True/false. Arrow `bool`. |
| Tiny int | `tinyint` | 8-bit signed, -128..127. Arrow `int8`. |
| Small int | `smallint` | 16-bit signed. Arrow `int16`. |
| Int | `int` | 32-bit signed. Arrow `int32`. |
| Big int | `bigint` | 64-bit signed. Arrow `int64`. |
| Float | `float` | 32-bit IEEE-754. Arrow `float32`. |
| Double | `double` | 64-bit IEEE-754. Arrow `float64`. |
| String | `string` | Variable-length UTF-8, unbounded. Arrow `string`/utf8. |
| Binary | `binary` | Variable-length raw bytes, no encoding implied. Arrow `binary`. |
| Date | `date` | Calendar date, no time-of-day, no zone. Arrow `date32`. |
| Decimal | `decimal(p,s)` | Fixed-point, `p` total digits, `s` after the point: exact arithmetic, no binary-float rounding. Arrow `decimal128`. |
| **Timestamp (tz)** | `timestamp_tz` | An **INSTANT**: a UTC point independent of any wall clock. Arrow `timestamp[us, tz=UTC]`. |
| **Timestamp (ntz)** | `timestamp_ntz` | A **NAIVE** wall-clock value with no zone attached. Arrow `timestamp[us]` (no tz). |
| Duration | `duration(unit)` | A span of time, stored as a plain signed 64-bit integer count of `unit` ticks (`s`/`ms`/`us`/`ns`: Arrow's own `TimeUnit` granularities). Modeled on Arrow `duration[unit]` VALUE semantics, deliberately rendered as a plain integer on every engine rather than either engine's native `INTERVAL` type: see "Why `duration` is integer-backed" below. |
| Array | `array<T>` | Ordered, variable-length list, one element type. Arrow `list<T>`. |
| Map | `map<K,V>` | Unordered association, one key/value type pair. Arrow `map<K,V>`. |
| Struct | `struct<name:type,...>` | Ordered, fixed set of named, independently-typed fields. Arrow `struct<...>`. |

`timestamp_tz` / `timestamp_ntz` is the load-bearing pair the whole hub exists to make explicit. Two `timestamp_tz` values from different source zones compare and sort correctly against each other, because both are already normalized to the same instant line. Two `timestamp_ntz` values compare as plain numbers: there is no instant they correspond to without an externally supplied zone. This is exactly the distinction Arrow's own type system already draws, which is why the hub borrows it rather than inventing a third scheme.

A small set of familiar unambiguous aliases canonicalize silently at parse time (`long` → `bigint`, `integer` → `int`, `varchar`/`char` → `string`, `short` → `smallint`, `byte` → `tinyint`, `bool` → `boolean`). `decimal` with no precision/scale defaults to `decimal(10,0)`, matching Spark's own DDL default: a well-defined default, not an ambiguity.

**Why `duration` is integer-backed.** Every other composite/parametrized constructor above (`decimal(p,s)`, `array<T>`, ...) renders to a real native type on both engines. `duration(unit)` deliberately does not follow that pattern: Spark's day-time `INTERVAL` and DuckDB's `INTERVAL` do not share a Parquet representation either engine's writer/reader can round-trip against the other, so a hub constructor built on top of either engine's native interval type would inherit exactly the cross-engine fragility the hub exists to prevent. `render_type` renders `duration(unit)` as a plain `BIGINT`/`bigint` on both engines instead: a signed 64-bit integer count of `unit` ticks, with no logical-type ambiguity a Parquet reader/writer could disagree about. `unit` is metadata the hub carries (which tick size the integer counts); neither engine's cast machinery ever consults it. An author who wants one engine's own native interval semantics (calendar arithmetic, month/day/microsecond components) uses the `<engine>:` native namespace directly instead of `duration(unit)`.

Four surfaces carry a type spelling: Ingress `schema_hint`, Channel `op: cast`'s type map, UDF `return_type`, and Flow Report / lineage's reported column types (rendered in hub spellings). `parser/schema.py` still types these fields as plain strings (the grammar itself is unchanged) but `aqueduct/compiler/compiler.py` now parses every one of them through `typehub.parse_type()` at compile time, so an unrecognized spelling is a compile-time `TypeSpellingError` naming the nearest valid spellings, not a runtime parser crash three layers down.

## **9.2 Two kinds of ambiguity, two different rules**

The hub draws a hard line between two kinds of "this spelling could mean more than one thing", because the correct response is opposite for each:

- **Semantic ambiguity → reject at parse time, naming the alternatives.** A spelling is semantically ambiguous when the *value it describes* differs across engines: bare `timestamp` is the only one the hub currently defines this way. Spark's `timestamp` is an instant; DuckDB's `TIMESTAMP` is naive. The same Blueprint, unmodified, would silently mean a different value depending on which engine ran it. That is not a spelling problem the hub can quietly resolve: it is a genuine ambiguity in what the author meant, so the hub refuses to guess in silence and asks the author to say `timestamp_tz` or `timestamp_ntz` (or a native spelling naming one engine on purpose).
- **Representational ambiguity → canonicalize silently.** A spelling is representational when several familiar strings name the *same* value type: `long` and `bigint` are both a 64-bit signed integer everywhere; there is no engine on which they diverge. These fold to one canonical spelling with no warning, because there was never a real choice being hidden.

Misapplying this rule in either direction breaks the hub's contract: silently resolving `timestamp` (a semantic ambiguity) reintroduces exactly the bug the hub exists to prevent, and refusing `decimal` with no precision/scale as if it were semantically ambiguous (a representational default, not a real one) would make ordinary Spark-style DDL fail for no reason.

**Bare `timestamp` is REJECTED at compile time.** There is no deprecation window: bare `timestamp` never parses. It raises a `TypeSpellingError` (surfaced as a compile-time `CompileError` at whichever surface used it: `schema_hint`, `cast`, or `return_type`) naming both explicit spellings and the single-engine native escape hatch. Write `timestamp_tz` or `timestamp_ntz` explicitly, or a native spelling (`spark:timestamp`) if the Blueprint intentionally targets one engine only. There is no suppress mechanism for this: it is not a warning.

**The native namespace: an explicit, capability-gated escape hatch.** `<engine>:<spelling>` (e.g. `duckdb:HUGEINT`, `spark:interval day to second`) names a type in one engine's own vocabulary directly, bypassing the hub entirely. It is not validated for meaning, only for shape (non-empty engine token, non-empty spelling): whatever that engine's own runtime parser accepts, it accepts. This is governed by the capability framework (`type.native.<engine>`, §10.9), not exempt from it: writing `duckdb:HUGEINT` into a Blueprint compiled for `spark` is a compile-time `CompileError` naming the spelling, because `type.native.duckdb` is `unsupported` on the Spark engine. Docs and templates **recommend the portable hub spellings** for anything that has one; the native hatch exists for spellings the hub genuinely has no equivalent for (DuckDB's `HUGEINT`, Spark's `interval`/`variant`), and using it is an explicit, honest statement that this Blueprint is written for one engine.

## **9.3 The hub is a superset: it surfaces divergence, it does not hide it**

Every constructor in the table above is now a capability leaf (`type.<constructor>`, one per registered engine, plus `type.native.<engine>` for the escape hatch: see §10.9), checked recursively against every inventoried type surface at compile time. `channel.op.cast` being `supported` says an engine implements a cast operation; `type.array` being `supported` on that same engine says it can additionally cast to a composite spelling. Both engines shipped today declare all seventeen constructors `supported`, each backed by a real runtime mapping (`ExecutorProtocol.render_type`, §10.9) from the hub's canonical spelling to that engine's own native DDL: `array<int>` renders to DuckDB's `INTEGER[]`, `timestamp_tz` renders to Spark's plain `timestamp`, `duration(unit)` renders to a plain `BIGINT`/`bigint` on both. A hub spelling used against an engine with no verdict for that constructor, or no `render_type` mapper at all, is refused at the seam it would otherwise reach a parser through: never silently forwarded to a parser that was never going to understand it.

Read this plainly rather than as a portability guarantee: the hub does not make two engines equivalent, and it is not trying to. What it changes is the FAILURE MODE. Before the hub, an engine mismatch on a type: Spark's `timestamp` vs. DuckDB's `TIMESTAMP`, a composite spelling DuckDB's alias table didn't know: reached that engine's own parser raw and failed there, if it failed at all, as an engine stack trace with no Aqueduct context, or (worse, `timestamp`) didn't fail and just silently meant something different. The hub is a **superset** of what any one engine natively spells, chosen so that divergence between engines becomes a **visible refusal** (a `CompileError` or a named compiler warning, at the surface where the Blueprint is read) rather than a value that quietly means something else three layers downstream. That is a strictly honest trade, not a simplification: a Blueprint that only uses portable hub spellings on constructors every target engine declares `supported` runs the same way everywhere; one that reaches for a native escape hatch or a still-ambiguous bare spelling is now told so at compile time instead of finding out at 2am from a wrong row count.

---

# **10. Deployment & engine integration**

## **10.1 Engine configuration file**

Aqueduct reads a project-level `aqueduct.yml` configuration file from the working directory (or path specified by `--config` flag). This file sets deployment target, store backends, agent config, and engine defaults.

The canonical field reference with descriptions and defaults lives in the `aqueduct.yml.template` file shipped with the engine. The config blocks that `aqueduct.yml` can contain:

| Block | Owns |
| :- | :- |
| `deployment` | Engine selection (`engine: spark` or `engine: duckdb`), cluster target |
| `engine` | Per-engine settings, namespaced by engine name (2.0: see below): `engine.spark.master_url`, `engine.spark.conf`, `engine.duckdb` |
| `stores` | Backend selection for observability, depot, blob, and benchmark (DuckDB / Postgres / Redis / local / s3 / gcs / adls) |
| `probes` | Default probe signal limits |
| `danger` | Safety-gate overrides |
| `secrets` | Secrets provider (env / aws / gcp / azure / custom) |
| `webhooks` | Outbound webhook endpoints for run lifecycle events |
| `agent` | LLM connection defaults (provider, base_url, model, api_key, cascade, timeout, budget), CI webhook URL |
| `warnings` | Compiler/executor warning suppression rules |
| `checkpoint_root` | Local filesystem path overriding the derived `<store_dir>/checkpoints/` location for module checkpoint/resume state (2.8) |
| `handoff` | Cross-engine handoff spill location + failure-retention policy (`root`, `keep_on_failure`) for the compiler-synthesized Handoff module (2.35): see §10.9 |
| `timezone` | Universal session time zone applied to every registered engine's session at creation (2.38): see §10.3.1 |

### The `engine:` block (2.0)

Per-engine configuration is namespaced by engine name, mirrored between `aqueduct.yml` (engine-level defaults: the full per-engine field set below) and the Blueprint (`engine:` block, per-Blueprint overrides; see §4.2). The two levels are NOT always field-identical: each engine's Blueprint-level block carries only the fields where a per-pipeline override is meaningful, deliberately excluding deployment/connection concerns that describe how THIS installation runs rather than what this pipeline needs (Spark's Blueprint block has always excluded `master_url` for exactly this reason; DuckDB's Blueprint block (2.54) similarly excludes `database_path`, `extension_repository`, and the `s3_*` credential/endpoint fields; see the `engine.duckdb:` block below). A key under `engine.<name>.` belongs to that engine; every other engine accepts and ignores it (a suppressible `engine_key_ignored` warning, never an error; see §10.9 "Config-leaf governance"). Adding a new engine's settings is a new sub-block here, never a new top-level `<engine>_config` dict.

```yaml
engine:
  spark:
    master_url: "local[*]"           # SparkSession.builder.master() — validated against deployment.target
    conf:                            # per-run Spark session configuration
      spark.sql.shuffle.partitions: 200
  duckdb:
    memory_limit: "4GB"              # SET memory_limit — unset keeps DuckDB's own default
    threads: 4                       # SET threads — unset keeps DuckDB's own default
    database_path: "/data/run.duckdb"  # persistent file, replacing the default :memory: connection
    extension_repository: null       # SET custom_extension_repository — airgapped-mirror escape hatch
    s3_key_id_secret: null           # secret KEY NAME (resolved via secrets:), fed into CREATE SECRET
    s3_secret_access_key_secret: null  # secret KEY NAME — must be set together with s3_key_id_secret
    s3_region: null                  # not sensitive — given literally
```

This is the full `aqueduct.yml`-level field set. A Blueprint's own `engine.duckdb:` block (§4.2) accepts only `memory_limit`/`threads`; see the `engine.duckdb:` block section below for why the rest stay `aqueduct.yml`-only.

**2.0 BREAKING: moved off two pre-2.0 locations.** `deployment.master_url` (it is Spark's own cluster-connection string, not a cross-engine deployment concern; `deployment.target` stays where it is) and the top-level `spark_config` dict (named after one engine, with nowhere for a second engine's knobs to live) both move under `engine.spark:`. `aqueduct_config` bumps `"1.0"` → `"2.0"`. Both are now hard-rejected (`extra="forbid"`) at their old location: a pre-2.0 file fails at config-load naming the rejected key directly (`ConfigError`, exit code `CONFIG_ERROR`), never silently accepted or auto-migrated:

```text
# Before (1.0)                          # After (2.0)
aqueduct_config: "1.0"                  aqueduct_config: "2.0"
deployment:                             deployment:
  engine: spark                           engine: spark
  target: local                           target: local
  master_url: "local[*]"                engine:
spark_config:                             spark:
  spark.sql.shuffle.partitions: 200         master_url: "local[*]"
                                             conf:
                                               spark.sql.shuffle.partitions: 200
```

The Blueprint-level `spark_config:` block moves the same way, into a Blueprint-level `engine:` block with the identical inner shape (§4.2):

```text
# Before (1.0)                     # After
spark_config:                      engine:
  spark.sql.shuffle.partitions: 200  spark:
                                        conf:
                                          spark.sql.shuffle.partitions: 200
```

`engine.spark.conf` at both levels merge the same way `spark_config` always did (Blueprint wins on conflict). The `set_spark_config` PatchSpec op (§8) initially carried over unchanged by name, with only its write target moved to `engine.spark.conf.<key>`: it was later replaced outright by the engine-agnostic `set_engine_config` (§8), which addresses any registered engine's block, not just Spark's.

**This merge is engine-generic, not a Spark special case (2.53), and has THREE layers (2.64).** `aqueduct.executor.session_config.resolve_session_engine_config` layers a Blueprint's `engine.<name>:` block over that engine's `aqueduct.yml`-level `engine.<name>:` config, and this invocation's `-s/--set` overrides over both; lowest to highest: `aqueduct.yml` < Blueprint < `--set`. That holds for EVERY registered engine: the same rule Spark has always documented above, implemented once and shared. Through 2.52 the internal Manifest carrier for this was still named `spark_config` and read only on Spark's session-build path, so a Blueprint-level `engine.duckdb:` override had nowhere to go: DuckDB always got its `aqueduct.yml` config only, with no way for a Blueprint to override it. The internal carrier (never a YAML-facing name: this Blueprint/Manifest field is plumbing, not part of the grammar documented here) is now `engine_config: dict[str, dict]`, keyed by engine name, populated for every engine named in the `engine:` block, empty for one with nothing set. DuckDB's Blueprint-level block (2.54) carries `memory_limit`/`threads`; a future field added there participates in the same Blueprint-wins merge automatically.

**Why `--set` is the top layer (2.64).** `-s/--set` is documented as the highest-precedence source (`--set > blueprint > aqueduct.yml > defaults`, see the CLI reference), but engine config is not resolved by that plain overlay: it has its own merge, and `--set` used to be applied only to the `aqueduct.yml` layer of it. A value a self-heal had written into the Blueprint's `engine.<name>:` block therefore beat the flag a user typed at the prompt, inverting "explicit beats default" for the one source that is the most explicit statement a user can make about a run. `--set` is now a genuine third layer above the Blueprint rather than a mutation of the layer beneath it, stated once in `resolve_session_engine_config` and never re-implemented per call site. It is safe for a CLI flag to outrank a heal precisely because it is per-invocation and never written back to any file: it overrides a healed value for one run, it cannot undo one. Two visible consequences. First, `session_config_fingerprint` separates a session built with `--set` from one built without it, for free: the flag is inside the function whose output the fingerprint hashes, so nothing had to be added there; within a run, a heal writing a Blueprint value the flag shadows produces the SAME fingerprint and correctly triggers no session rebuild. Second, Gate 1's inert-write refusal (§8) becomes reachable from a source the Blueprint cannot outrank: a `set_engine_config` patch writing a key the invocation pins is refused with a message naming the exact `--set` path and its pinned value, rather than the ordinary "write a different value" advice, which would be false there.

### The `engine.duckdb:` block: session config + remote storage (2.41, Blueprint-level fields 2.54)

Every field is read by `_make_session`: none is a silent no-op (see AGENTS.md's "no silent no-ops" rule).

- **`memory_limit`/`threads`**: `SET memory_limit=...`/`SET threads=...`, applied right after connecting. Unset keeps DuckDB's own defaults. **Available at BOTH levels** (2.54): `aqueduct.yml`'s `engine.duckdb.*` sets the deployment default, a Blueprint's own `engine.duckdb:` block (§4.2) may override it per-pipeline (Blueprint wins); a pipeline plausibly needs more memory or more threads than the machine default, and that is a property of the pipeline, not the deployment.
- **`database_path`**: a persistent file, replacing the default `:memory:` connection. LOCAL PATHS ONLY (a remote URI scheme is rejected at config-load, mirroring `checkpoint_root`): DuckDB's own database file is always local even when the tables it reads/writes point at remote storage. Two independent reasons to set it: it raises a receiving cross-engine handoff island's RAM ceiling (a bare `:memory:` connection hard-caps it at available RAM), and it lets large intermediates spill to disk instead of aborting. `aqueduct.yml`-only: not on the Blueprint's `engine.duckdb:` block (a Blueprint doesn't pick which local file this installation's DuckDB process writes to, any more than it picks a Spark `master_url`).
- **`extension_repository`**: `SET custom_extension_repository=...`, applied before any extension install. The airgapped/hermetic-CI escape hatch: `httpfs` (below) autoinstalls over the network on first use by default, which fails on a cluster with no route to DuckDB's public extension repository. The other escape hatch, a pre-populated `~/.duckdb/extensions` directory, needs no config at all: DuckDB checks its local cache first. `aqueduct.yml`-only, same reasoning as `database_path`.
- **`s3_key_id_secret`/`s3_secret_access_key_secret`/`s3_region`**: S3/GCS credentials for remote ingress/egress/handoff paths. The first two are secret KEY NAMES (never a literal credential), resolved through the EXISTING `secrets:` block resolver (`aqueduct.secrets.resolve_secret`; the same function `@aq.secret()` calls) at session creation, and fed into DuckDB's own `CREATE SECRET (TYPE S3, KEY_ID ?, SECRET ?, REGION ?)` via parameter binding; never string-interpolated into SQL, so a credential value can never end up in a logged or rendered statement. `s3_region` is not sensitive and is given literally. The two secret-name fields must be set together (config-load validation error otherwise). `aqueduct.yml`-only: a Blueprint overriding a credential secret name or connection endpoint is a footgun, not a feature; same reasoning `master_url` has always had on the Spark side.

**Why no new capability leaf.** The Blueprint-level `memory_limit`/`threads` override reaches the exact same `_make_session` code path, through the exact same `engine_config` dict key, as the `aqueduct.yml`-level field: `aqueduct.executor.session_config.resolve_session_engine_config` merges them before either one is read. The capability question ("can this engine's session accept a memory_limit override at all") is already asked and answered once, by the existing `config.engine.duckdb.memory_limit`/`config.engine.duckdb.threads` `aqueduct.yml`-level leaves (§10.9); the Blueprint-level field is a second value SOURCE for the identical capability, not a second capability. This mirrors the existing precedent of Spark's Blueprint-level `engine.spark.conf` block, which has likewise never had its own grammar leaf distinct from `config.engine.spark.conf`.

**`httpfs` is a DuckDB EXTENSION, not a Python package**: nothing enters `pyproject.toml`, no new dependency or extra. On duckdb>=1.0, `autoinstall_known_extensions`/`autoload_known_extensions` both default to `True`, so any module touching an `s3://`/`gs://` path already makes DuckDB install and load `httpfs` on its own with zero Aqueduct code. `_make_session` proactively `INSTALL`s/`LOAD`s `httpfs` only when S3 credentials or `extension_repository` are configured (a deliberate signal of remote-storage intent) so an airgapped-install failure surfaces LOUDLY at session creation as `aqueduct.executor.duckdb_.extensions.DuckDBExtensionError` (an `AqueductError` naming both escape hatches), not as a bare `duckdb.IOException`/HTTP error buried inside a later query. When neither is configured, a DuckDB session-startup warning (`duckdb_httpfs_availability`, mirroring Spark's `jar_availability` rule: same diagnostic shape, not the same mechanism: a jar ships to Spark's executor fleet at session creation, a DuckDB extension installs per-connection in-process) fires if the compiled Manifest reads/writes a remote path and `httpfs` is not yet loaded, naming the network requirement and both escapes.

`aqueduct doctor`'s `handoff-access:duckdb` check (§10.4.3) attempts a real round trip against a remote `handoff.root` the same way it always has for a local one; it no longer unconditionally reports `skip` for a remote root.

## **10.2 Environment variables & .env**

- Aqueduct automatically loads `.env` from the directory of the config or blueprint file.
- Override with `-e KEY=VAL` (highest precedence) or `--env-file <path>`.
- Disable entirely with `AQ_NO_ENV_FILE=1`.

**Config overrides (`-s/--set`, 1.2).** `aqueduct run -s agent.approval=human -s stores.observability.backend=postgres …` sets dotted-path keys in the loaded `aqueduct.yml` config in memory for that invocation, repeatable, applied after the file is read and before validation. Distinct from `--ctx` (which sets Blueprint Context Registry values, not engine config). Values are parsed as YAML scalars (`true`/`123`/strings).

## **10.3 SparkSession lifecycle**

- The Executor creates one SparkSession per pipeline run.
- Session configuration from the Blueprint `engine.spark.conf` block is merged with `aqueduct.yml`'s `engine.spark.conf` (Blueprint takes precedence).
- **`spark.sql.parquet.outputTimestampType` defaults to `TIMESTAMP_MICROS` (2.36+),** set by the session factory at creation time, in place of Spark's own default (`INT96`, a legacy Hive-interop encoding whose Parquet files carry no logical-type annotation distinguishing an instant-aware timestamp from a naive one). This changes the on-disk encoding of any `timestamp` column an Egress module writes with Spark: values are unchanged, `INT96` is deprecated in the Parquet spec, and `TIMESTAMP_MICROS` is annotated correctly regardless of which engine reads the file back. An explicit `engine.spark.conf.spark.sql.parquet.outputTimestampType` value always overrides this default.
- On self-healing patch and resume: the SparkSession is preserved if the failure was application-level; recycled if JVM/network-level.
- On run completion or abort, the one-shot CLI relies on process-exit teardown to release the JVM, it deliberately does **not** call `session.stop()`, because `getOrCreate()` may have returned a shared/long-lived cluster (or test) session that other code still depends on. Short-lived helper commands that create a throwaway session (`doctor`, scaffolding) do stop theirs.

### **10.3.1 Universal session timezone (`timezone:`, 2.38)**

A top-level `aqueduct.yml` key, resolved through the engine registry (never a hardcoded engine list) and applied to EVERY registered engine's session at creation: Spark's `spark.sql.session.timeZone`, DuckDB's `SET TimeZone`, and whatever the equivalent is for any engine registered later.

```yaml
timezone: "UTC"   # IANA/Olson name, e.g. "UTC", "America/New_York"
```

Engine-native session-timezone settings already work standalone (`engine.spark.conf: {spark.sql.session.timeZone: UTC}`); a shared key only earns its keep once a Blueprint spans more than one engine (§10.9's cross-engine handoff). There, a divergent per-engine session time zone is a WRONG-ANSWER bug, not a config annoyance: `to_timestamp` on a naive string resolves to a different instant per engine, and a `timestamp_tz → date` cast lands differently. Two engines reading one key makes that divergence unrepresentable; two independent engine-native keys make it silent.

**Precedence.** An explicit engine-native override always wins for that engine: the same "explicit beats default" rule applied everywhere else in this project (`engine.spark.conf.spark.sql.parquet.outputTimestampType` over the session factory's own default, `engine.spark.conf` over `aqueduct.yml`'s copy of it, ...). `timezone:` is applied only when the target engine's own resolved config doesn't already set its native equivalent; when it does AND the two values disagree, a suppressible warning fires (rule id `engine_timezone_conflict`) naming the divergence; the whole point of the universal key is making cross-engine timezone divergence VISIBLE, so silently letting one engine drift defeats it. DuckDB has no `engine.duckdb.*` conf knob yet (§10.1), so this precedence fork is exercised on Spark today; a future DuckDB session-timezone knob would participate in the same rule.

## **10.4 Path resolution (1.1.0+)**

Every relative path inside a YAML file resolves to **that YAML file's parent directory**, never the CWD of the `aqueduct` command. See [CLI Reference](cli_reference.md) for details.

### **10.4.1 Observability store routing (DuckDB)**

`stores.observability.path` (DuckDB backend) is always a **routing base
directory** (2.0: the earlier single-shared-file layout was removed; a
`.db`-suffixed path is now a config-load error):

| `path` value | Layout | Parallelism |
| :- | :- | :- |
| *(unset: default)* | **Per-blueprint routing**: each blueprint writes its own file at `.aqueduct/observability/<blueprint_id>/observability.db` | ✅ Safe to run different blueprints in parallel, separate files |
| A directory, e.g. `/mnt/aqueduct/obs` | **Location-only routing**: same per-blueprint split, but under your directory: `<dir>/<blueprint_id>/observability.db` | ✅ Safe: separate files, custom location |
| ~~A file, e.g. `/mnt/aqueduct/obs.db`~~ | **Removed in 2.0**: DuckDB is single-writer, so one shared file was never parallel-safe, and a custom basename split reads from writes. Config load fails with a pointer here. | Use **Postgres** for one shared concurrent store |

**Caveats:**
- DuckDB takes an **exclusive lock** per file. Launching the *same* blueprint twice concurrently (one routed file) will block/fail, rare, but real.
- Want **one merged store for every blueprint** (shared file semantics)? Use the **Postgres** backend (MVCC, concurrent writers). Cross-blueprint *reads* over routed DuckDB files already work, the fleet commands (`report`, `runs`) aggregate across `<base>/*/observability.db`.
- **Reading while running:** `aqueduct report`/`runs` open short-lived read-only connections, so they don't block writers; a file mid-write is momentarily skipped by the fleet view. You do **not** need to stop pipelines to inspect, but for conflict-free continuous monitoring, use Postgres.
- *Planned:* dynamic templating in the path (e.g. `.aqueduct/obs-@aq.date.month().db` for time-partitioned stores).

### **10.4.2 Checkpoint root override (2.8)**

`checkpoint: true` (module- or manifest-level, see §4) writes module output to
Parquet for `--resume` support. By default this lands under the derived
`<store_dir>/checkpoints/<run_id>/` directory: the same routing base used by
the observability store (§10.4.1).

`checkpoint_root` (top-level `aqueduct.yml` key) overrides that derived
location entirely: when set, checkpoints for **both** a fresh run and a
`--resume` reload live directly under `<checkpoint_root>/<run_id>/`, bypassing
`store_dir` for this purpose only (observability signals still use
`store_dir`). Use it to point checkpoints at faster local disk, or a directory
explicitly shared between driver and workers on a Docker-based Spark
Standalone cluster.

**LOCAL FILESYSTEM PATHS ONLY.** A `checkpoint_root` value containing a remote
URI scheme (`s3://`, `s3a://`, `gs://`, `hdfs://`, `abfss://`, ...) is rejected
at config-load with an actionable error: remote checkpoint roots require
Hadoop-FS-API bookkeeping that Aqueduct does not yet implement. A relative path is
resolved against the project root (the `aqueduct.yml` directory).

**`--resume` fails closed at the CLI, then stays permissive at the engine
(2.68).** Every checkpointed run writes a `_manifest_hash` file alongside its
checkpoints. Before `aqueduct run --resume <run_id>` builds any engine
session, the CLI itself reads that stored hash back and compares it against
this run's freshly-compiled Manifest hash; on a mismatch it refuses
outright (`CONFIG_ERROR`), naming both hashes and pointing at `--force`.
Pass `--force` to reuse the checkpoints anyway — with `--force` (or on a
matching hash), execution proceeds exactly as before 2.68: both engines'
`execute()` independently re-compare that same stored hash against the
current Manifest's hash and, on a mismatch, emit a suppressible
`runtime_resume_hash_changed` warning through `aqueduct.warnings.emit()`
(suppressible via the engine-level `aqueduct.yml` `warnings.suppress` /
`--suppress-warning`, §4.2, the same mechanism session-startup warnings
use) and then PROCEED anyway, reusing whatever checkpoints exist. That
engine-level comparison is unchanged and, on its own (i.e. calling
`execute()` directly rather than through `aqueduct run`), is still purely
permissive — it is the CLI's fail-closed check, not the engine, that makes
`aqueduct run --resume` refuse by default. This is the direct counterpart
to the handoff spill's fail-closed detection below: see §10.4.3's callout.

### **10.4.3 Cross-engine handoff spill (2.35)**

`handoff:` (top-level `aqueduct.yml` block) configures WHERE the compiler-synthesized Handoff module's storage-spill parquet lands, and whether it survives a failed run. See §10.9 for what a Handoff module is and when the compiler inserts one; this section is the config surface only.

```yaml
handoff:
  root: ".aqueduct/handoff"   # default; any URI both engines can read+write (s3://…)
  keep_on_failure: true       # default — the resume story
  prune_eagerly: true         # default, see the same-run pruning paragraph below
```

Unlike `checkpoint_root`, `root` is **not** local-filesystem-only: a handoff spill must be reachable by BOTH engines on either side of a boundary, so a remote URI scheme (`s3://`, `gs://`, `abfss://`, ...) is accepted with no rejection. `handoff:` borrows `checkpoint`'s LIFECYCLE semantics (kept on failure, cleaned up on success); not its location or its local-only constraint, and not its config key (`handoff:` is its own top-level block, never nested under `checkpoint_root`). The two diverge past that shared lifecycle shape, though: a module checkpoint resumes across a CHANGED Manifest too, and (as of 2.68) `aqueduct run` itself refuses that by default before proceeding permissively under `--force` (§10.4.2, above). A handoff spill has no engine-level hash to compare in the first place: the Manifest hash is part of the spill's own directory (see layout below), so at the orchestrator layer a changed Manifest just resolves to a different directory and the prior spill is never looked at — no comparison, no warning, nothing to suppress. `aqueduct run --resume <run_id>` (2.68) closes that gap one layer up, the same CLI check described in §10.4.2: before building any engine session it scans `handoff.root` for *run_id* under every OTHER manifest-hash directory. Finding it there means the run_id exists but under a stale hash — refused (`CONFIG_ERROR`, both hashes named), unless `--force`. Finding nothing anywhere is not a mismatch (a run_id nobody has used, or one whose only checkpoints are the module kind above) and is left exactly as permissive as before: that island simply executes fresh. On the DuckDB side, a remote root is reached the same way any other remote path is (§10.9's `engine.duckdb:` subsection); `httpfs`, autoloaded on first touch, plus `engine.duckdb.s3_*` credentials (including the `s3_endpoint`/`s3_url_style`/`s3_use_ssl` non-AWS escape hatch) if the target requires authentication or is not AWS S3 itself. For an S3-flavored root touched by BOTH engines, use `s3a://`; Spark's bundled Hadoop FS registers `s3a://` (via the `hadoop-aws` package, resolved through `engine.spark.conf.spark.jars.packages`), not the legacy `s3://` scheme; DuckDB's `httpfs` accepts either scheme identically. See `docs/production_guide.md`'s "Object storage: MinIO / other non-AWS S3-compatible stores" for verified settings.

Directory layout: `<root>/<manifest_hash>/<run_id>/<edge_id>/`, one subdirectory per boundary per run. Deleted when the run succeeds; kept when it fails and `keep_on_failure` is true (the default), so a manual `aqueduct run --resume <run_id>` after a plain failure (with no Blueprint edit in between) can read the upstream island's already-materialized spill instead of recomputing it. A heal-triggered rerun does NOT get this: a heal patches the Manifest, which changes the whole-Manifest hash (and therefore `<manifest_hash>` in the path above) even when the patch touched only a downstream island, and the CLI's heal-retry path passes no `resume_run_id` at all once a patch has been applied; the two mechanisms never line up. Parquet is a fixed internal transport detail: there is no format knob, on either the Blueprint or the config side.

**Same-run eager pruning (`prune_eagerly`, default true).** Within one polyglot run, a boundary's spill does not have to wait for the whole run to end: it is deleted as soon as every island that reads it has finished successfully, since a handoff edge has exactly one reader island and nothing later in the same run will ever touch that directory again. This bounds peak spill storage on a long same-run chain instead of holding every boundary's output until the final island finishes. It only ever removes a spill whose reader already succeeded in THIS run. A spill feeding an island that has not run yet, or one this run resumed from a PRIOR run via `--resume`, is left alone, so a run that later fails at island N still has every spill feeding island N and everything after it intact on disk, and `--resume` behaves exactly as it did before this existed. `keep_on_failure` and the end-of-run deletion described above are unaffected either way; an eagerly pruned boundary is simply already gone by the time the end-of-run cleanup runs over it. Set `prune_eagerly` to false to defer every deletion to the run's own end instead.

**Keeping a spill is bounded by a release event, not by a clock.** `keep_on_failure: true` acquires disk; two deterministic actions give it back. First, a successful `--resume` deletes the spill it consumed: that spill was kept for exactly the rerun that has now read it, so its purpose is served (a FAILED resume keeps it, since it is still resumable). Second, the orphan sweep reclaims any kept-failure spill once a LATER run of the same blueprint has succeeded, which means the failure is resolved and nothing will ever resume from it again. "Succeeded" includes the `patched` run status, not only `success`: a heal that fixes the pipeline records `patched`, and that is the most common way a failure gets resolved. `finished_at` is used only to order two `run_records` rows against each other; there is no retention window, no age threshold, and no configurable number of days anywhere in this. Two consequences are stated rather than hidden. The first is closed: a blueprint that fails and is never run again used to keep its spill indefinitely with no way to reclaim it; `aqueduct handoff sweep --older-than <duration>` (e.g. `--older-than 7d`) is that explicit operator/watchdog action, additionally reclaiming a kept-failure spill whose run finished longer ago than the given age even though no later success has superseded it yet, never automatically and never without the flag. The second is decided and accepted: a failure under active investigation loses its spill if an unrelated scheduled run of the same blueprint succeeds in the meantime. Aqueduct builds no protection against that, neither an exemption for the most recent failure nor an opt-out setting. The hazard is unmeasured, and everything an operator actually debugs from survives the sweep: the `run_records` row, the `failure_contexts` row, and the stack trace are store records, not spill directories. A handoff spill is an intermediate parquet materialisation of one island's output, so losing it costs a rerun rather than a diagnosis, and guarding it would mean carrying a second retention rule here or a config key on every engine to protect a cost nobody has measured.

### **10.4.4 Depot mount routing (DuckDB)**

A depot mount under `stores.depots` (DuckDB backend) resolves in one of two
ways, decided by whether `path` is set.

| `path` value | Layout | Key isolation |
| :- | :- | :- |
| *(unset: default)* | **Per-blueprint routing**: the mount gets its own file at `.aqueduct/observability/<blueprint_id>/depot.db`, next to that blueprint's `observability.db` and never inside it | None needed: keys are raw, because the FILE is already per blueprint |
| A file, e.g. `/mnt/aqueduct/depot.db` | One shared file for every blueprint that names it | Keys are prefixed with `<blueprint_id>:`, unless `shared: true` asks for raw keys |

`--store-dir` replaces the routing base for a per-blueprint mount, the same
way it does for the observability store (§10.4.1).

`shared: true` requires an explicit `path`. A mount with no `path` lives in a
file no other blueprint reads, so asking to share it is a contradiction:
config load fails naming the mount. The `postgres` and `redis` backends also
require an explicit `path`, because there the value is a DSN or URL, not a
file this routing can derive.

## **10.5 Deployment targets**

The `deployment.target` field selects the Spark cluster type. Aqueduct validates
that `engine.spark.master_url` matches the declared `target` at config-load
(for `engine: spark` only), and `aqueduct doctor` provides target-specific
reachability and configuration guidance.

| Target | Status | Required `engine.spark.master_url` shape | Doctor checks |
| :- | :- | :- | :- |
| **local** | Supported (in-cluster) | Starts with `"local"` (e.g. `local[*]`) | In-process session: always ok |
| **standalone** | Supported (in-cluster) | Starts with `"spark://"` (e.g. `spark://host:7077`) | TCP probe to master host:port |
| **yarn** | Supported (in-cluster) | Exactly `"yarn"` | Warns if `HADOOP_CONF_DIR` / `YARN_CONF_DIR` env var is unset |
| **kubernetes** | Supported (in-cluster) | Starts with `"k8s://"` (e.g. `k8s://https://apiserver:443`) | TCP probe to API server host:port; warns if no `spark.kubernetes.*` keys in `engine.spark.conf` |
| **emr** | Deferred | n/a | Rejected at config-load with a "not yet supported" error |
| **dataproc** | Deferred | n/a | Rejected at config-load with a "not yet supported" error |

`emr` / `dataproc` are **remote-submit** targets planned for a future
release; in the current release they are rejected with a "not yet
supported" error at config-load. There is no built-in remote-submit target
today. To run on Databricks, wrap `aqueduct run` in a Databricks Workflows
`spark_python_task` (see the Production Guide).

See the **[Production Guide](production_guide.md)** for per-target cluster setup,
required env vars, `engine.spark.conf` keys, and the production readiness checklist.

## **10.6 `aqueduct test`: Isolated module testing**

`aqueduct test <test_file.yml>` runs Channel, Junction, Funnel, and Assert modules against inline data with no external I/O. Ingress and Egress are never executed. The session always runs on `local[*]`, `engine.spark.master_url` is deliberately ignored for cluster-pointed configs.

## **10.7 Orchestrator integration contract**

Aqueduct stays orchestrator-agnostic. Schedulers (Airflow, Dagster, Prefect) wrap `aqueduct run` and consume two stable surfaces: the **exit-code contract** and the **patch CLI JSON**. Both are part of the v1.0 stability guarantee.

| Exit code | Name | Meaning |
| :- | :- | :- |
| 0 | SUCCESS | Command completed successfully |
| 1 | CONFIG_ERROR | Configuration or schema error |
| 2 | DATA_OR_RUNTIME | Runtime / Spark / data error (includes remote job failure) |
| 3 | HEAL_PENDING | Patch staged for human review |
| 4 | VALIDATION_GATE | Patch rejected by validation |
| 64 | USAGE_ERROR | Invalid command usage |

Note: `USAGE_ERROR` is 64 (sysexits `EX_USAGE`), covering both an Aqueduct-detected usage mistake raised explicitly by a command (e.g. an unsupported `--store` value) and Click's own `UsageError` (unknown command, unknown flag, missing required argument, a bad `click.Choice` value) — `aqueduct/cli/__init__.py` repoints `click.exceptions.UsageError.exit_code` to this constant at import time, so both sources exit the same code. `5` was `USAGE_ERROR`'s value before this unification; it is retired and never reused.

## **10.8 Remote-submit targets**

`emr` and `dataproc` are **rejected at config‑load** in the current release.
Setting `deployment.target` to either of these values raises a `ConfigError`.
There is no built-in remote-submit target today; see §10.5 for the
Databricks migration path.

## **10.9 Engines and the capability framework**

> **A new engine is not required to support cross-engine handoff.** The handoff/island
> machinery (§4.3, §11.4) is experimental. What an engine must implement to be complete is the
> `ExecutorProtocol` plus its own capability declaration; taking part in a polyglot Blueprint is
> optional and unsupported territory.

The Blueprint grammar (module types, Channel ops, Egress write modes, feature flags) is engine-agnostic by design. `deployment.engine` selects which engine runs a compiled Manifest. No engine is required to implement the whole grammar, and a leaf an engine does implement may still need a minimum dependency version. The capability framework makes both facts explicit and enforced, so a Blueprint that asks an engine for something it cannot do fails at compile time with a specific message instead of at runtime with an engine stack trace.

### Engines that ship today

| Engine | What it is | Install | Entry point |
| :- | :- | :- | :- |
| `spark` | The reference engine. Distributed, cluster or local. Implements the full grammar. | `aqueduct-core[spark]` | `aqueduct.executor.spark.engine` |
| `duckdb` | Single-node, in-process. Implements a declared subset. | `aqueduct-core[duckdb]` | `aqueduct.executor.duckdb_.engine` |

The two are not interchangeable, and Aqueduct does not present them as such. A Blueprint that compiles for both engines is one whose leaves both engines have declared `supported`. That is a property the compiler checks per Blueprint, not a property of the product.

The DuckDB engine currently reads `parquet`, `csv`, and `json`; runs Channel `sql`, `join`, `filter`, `select`, `deduplicate`, `cast`, `rename`, `sort`, and `union`; runs every Junction mode and every Funnel mode; runs every Assert rule type (including `null_rate`, `custom`, and quarantine via the spillway port) and every `on_fail` action; runs Probe (8 of Spark's 9 built-in signals plus `custom`; see §4.4 for the two places its behavior genuinely diverges from Spark's); runs Python UDFs (`conn.create_function`); and writes `parquet` and `csv`, including the `on_new_columns` schema-drift write contract. Channel `sql`/`join`, Assert `sql`/`sql_row`, and Probe `threshold`/`custom` SQL are authored in Spark SQL and transpiled to DuckDB SQL with `sqlglot`. `execution_partitions` and Java UDFs are declared `unsupported` rather than silently accepted. Any of those paths may point at remote storage (`s3://`, `gs://`, ...); DuckDB's `httpfs` extension autoloads on first touch, and `engine.duckdb.*` config (below) wires memory/thread limits, a persistent database file, and S3 credentials reconciled with the `secrets:` resolver. The per-leaf verdicts are published as a generated matrix (see below) rather than restated here.

### How an engine registers

An engine registers itself through the `aqueduct.engines` setuptools entry-point group (`pyproject.toml`'s `[project.entry-points."aqueduct.engines"]` table maps an engine name to a module, e.g. `spark = "aqueduct.executor.spark.engine"`). Importing that module registers the engine's capability declaration as a side effect. `aqueduct/executor/capabilities.py::load_engines()` resolves and imports every entry point in the group exactly once per process, and `get_capabilities()` calls it before looking an engine up. Core never imports an engine's package by name: a new engine (e.g. DuckDB) ships its own entry point and needs no edit to `aqueduct/compiler/`, `aqueduct/config.py`, or any other core module to become a valid `deployment.engine` value. `deployment.engine` is validated against the set of registered engines at config-load time, not a fixed list of literals.

### Failing closed

`get_capabilities()` raises `UnknownEngineError` (an `AqueductError`, subclassing `CompileError`) for an engine with no registered capability declaration: an unknown name, a typo, or an engine whose package/extra is not installed. The message names the engine and lists what is registered. The compile-time gate and the doctor capability check both let this propagate rather than degrading to an empty result: a misconfigured or unregistered engine is a loud, actionable failure, never a silently-skipped gate. Callers that must tell an unregistered engine apart from an ordinary compile failure do it by exception type, not by matching on the message.

Two adjacent failure modes get their own diagnosis:

- **No engines registered at all.** An empty registry means aqueduct's own entry points are invisible to `importlib.metadata`, in practice a stale install whose metadata predates the entry-point declaration. Since engine validation is fail-closed, that state would otherwise hard-fail every `aqueduct.yml` load with a misleading "Registered engines: []". The error says what it actually is: reinstall the package.
- **A broken engine plugin.** An `aqueduct.engines` entry point that fails to import raises `EnginePluginError` (an `AqueductError`) naming the entry point, its target, and the underlying cause. A half-installed third-party engine surfaces as a clean Aqueduct error, not as a raw `ImportError` out of config loading. The plugin is broken or half-present, so the message ends in reinstall advice.
- **An incomplete or invalid declaration.** A `capabilities.yml` with a leaf that has no row, a row still on `undeclared`, a row naming a leaf that does not exist, an illegal verdict, or a malformed version specifier raises `CapabilityDeclarationError` (an `AqueductError`). This is a dev-time build failure, typically a developer who has just added a schema key that every engine now owes a verdict for, so reinstalling the package fixes nothing. The message names the offending leaves (also carried on the exception as `.leaves`) and gives the fix that works: run `aqueduct dev capabilities sync`, then declare a verdict per engine.
- **An undecided config-leaf scope.** A `config.*` field living under an `engine.<name>.*` block with no `engine_scoped: True` tag raises `CapabilityScopeError` (an `AqueductError`, a SIBLING of `CapabilityDeclarationError`; deliberately not a subclass, so a shared `except CapabilityDeclarationError:` cannot swallow it). See "Config-leaf scoping" below. Raised by the walker at every engine's registration time, never CI-only.

These three states are distinguished by exception type, never by matching message text.

### Verdicts

A verdict answers one question: if a Blueprint uses this leaf on this engine, what happens? Every engine declares one of four for every leaf.

| Verdict | Meaning | Effect |
| :- | :- | :- |
| `supported` | The engine runs this leaf. May carry a `requires` version constraint, for example `format: custom` needs `pyspark>=4.0`. | Compiles. |
| `unsupported` | The engine cannot run this leaf. | Blueprint leaf: `CompileError`. Config leaf: warning. |
| `ignored_with_warning` | The engine accepts the leaf and it has no effect. | Suppressible warning under `engine_key_ignored`. |
| `undeclared` | Nobody has decided yet. | Build failure at engine registration. |

`undeclared` is a sentinel rather than a verdict, and it is deliberately distinct from `unsupported`. "We have not decided" and "we decided the engine cannot do it" are different states, and a framework that conflates them cannot tell an honest refusal from an oversight. It is what the sync tool writes for a newly discovered leaf, and the build stays red until a human replaces it.

An engine earns `supported` rather than assuming it. For a leaf the engine executes (a Channel op, a format, a write mode, a module type, a feature flag) that means a real handler plus a test exercising it on that engine. For a leaf the engine never touches, where core orchestration behaves identically whichever engine is selected (the `agent:` block, webhooks, hooks, retry policy), it means an end-to-end test proving that on this engine, rather than an assumption that it must be so.

### Declarations are data, one explicit row per leaf

Each engine ships a YAML capability declaration alongside its package (`aqueduct/executor/spark/capabilities.yml`, `aqueduct/executor/duckdb_/capabilities.yml`) carrying one row for every capability leaf: a verdict, plus the optional `requires` constraint and `hint` text. Spark's file holds 208 rows (191 Blueprint-grammar leaves + 17 engine-scoped config leaves); DuckDB's holds 213 (191 + 22; 15 shared/DuckDB-only engine-scoped leaves plus the 7 `engine.duckdb.*` leaves from the 2.41 config-surface work, minus the 2 `engine.spark.*` leaves that are positionally Spark's to declare, not DuckDB's; see "Config-leaf scoping" below). Every engine's table also has 88 config leaves it is never asked about at all: see that section for why the two counts differ from a single flat total.

There is no default-verdict sweep. An engine states which leaves it supports, one row at a time, and never "everything, by assumption". A third-party engine author ships reviewable data rather than Python.

`load_declaration()` hard-validates the file at registration and raises `CapabilityDeclarationError` on: a row for a leaf that does not exist (a typo or a stale rename), an illegal verdict string, a malformed version specifier, a leaf with **no row at all**, or a row still parked on `undeclared`. An engine cannot register half-declared.

### Config-leaf scoping

Blueprint-grammar leaves (`module.type.*`, `channel.op.*`, formats, modes, `feature.*`) are engine-invariant by construction: every registered engine declares every one, because whether an engine can run a module type or a Channel op is genuinely a question every engine has to answer. `config.*` leaves (the `aqueduct.yml` surface) are not all that shape. Of the 105 `config.*` leaves `AqueductConfig` derives, ~88 run entirely in core code paths (`webhooks.*`, `secrets.*`, `stores.*`, most of `agent.*`, most of `danger.*`, …) that never dispatch through an engine at all: asking DuckDB whether it "supports" webhook retry backoff is a category error, not a governance win, and the framework used to force an answer anyway because the closure test needed something to compare against.

This is a **scoping** change, not a fourth verdict: `Support` stays `supported` / `unsupported` / `ignored_with_warning` / `undeclared`, and `verdict()` callers are unchanged in meaning. What changes is the **checklist**: which leaves an engine is asked about at all.

**The tag is mandatory and explicit: there is no "untagged means core" default.** Every `config.*` field carries `json_schema_extra={"engine_scoped": True}` or `{"engine_scoped": False}` in `aqueduct/config.py`:

```python
max_sample_rows: int = Field(..., json_schema_extra={"engine_scoped": True})
api_key: str | None = Field(..., json_schema_extra={"engine_scoped": False})
```

A field carrying **neither** key raises `CapabilityScopeError` naming the field and both legal resolutions, the moment the walker runs. An earlier design let an absent tag fall back to "core" implicitly; that was rejected; it let a brand-new field (or a genuinely engine-scoped one someone forgot to mark) disappear into the core bucket with nobody deciding, silently deleting the `engine_key_ignored` warning path it should have had. Requiring the `False` half explicitly is what makes "core" a decision instead of an omission. `aqueduct/executor/config_leaves.py::all_config_leaves()` yields the `True`-tagged fields (the checklist every engine must have a verdict for); `core_config_leaves()` yields the `False`-tagged complement (leaves that never appear in any engine's table at all; not a question that gets asked). Both are derived from the SAME per-field tag, so they cannot drift apart, and no committed snapshot file exists to go stale either: see "Why no snapshot file" below. Reclassifying a key is now a one-word `True`↔`False` diff at the field itself, reviewable in a pull request: the property the dropped snapshot file existed to provide.

**`engine.<name>.*` is positionally owned.** A leaf under a per-engine namespaced block (`engine.spark.master_url`, `engine.spark.conf`, `engine.duckdb.*`) can only ever mean something to that ONE engine, so it appears ONLY in that engine's own checklist: `all_config_leaves(engine="spark")` excludes every other engine's `engine.<name>.*` leaves, and `capability_tooling.governed_leaves(engine=...)` threads the same filter through `check`/`sync`/`scaffold`/`docs`. Spark's table therefore has 208 rows (191 grammar + 17 engine-scoped config, including its own two `engine.spark.*` leaves) and DuckDB's has 213 (191 + 22: 15 shared engine-scoped leaves, minus Spark's two, plus its own seven `engine.duckdb.*` leaves: `memory_limit`, `threads`, `database_path`, `extension_repository`, `s3_key_id_secret`, `s3_secret_access_key_secret`, `s3_region`). Because a field namespaced to one engine has no coherent "core" reading, a field discovered there tagged `False` (or untagged) is ALSO a contradiction and raises `CapabilityScopeError` at the walker.

**Why no snapshot file.** An earlier design considered committing a generated `core_config_leaves.yml` and diffing it in CI, the same pattern `docs/compatibility.md` uses. It was dropped: the per-field tag already IS the single source of truth, and it sits AT the field it describes rather than in a generated copy. What replaces the snapshot is one build-enforced invariant plus a check that already existed:

| someone does this | caught by | how loud |
| :- | :- | :- |
| untags a key some engine declared `unsupported`/`ignored_with_warning` | the invariant test (`tests/test_capabilities/test_config_scope_invariant.py`) | red build, names the leaf |
| untags a key but leaves the rows behind | the existing orphaned-row check (`dev capabilities check`) | red build, names the leaf |
| untags a key every engine declared `supported` | nothing, and nothing needs to: a `supported` verdict emits no warning, so there was no warning path to delete | inert by construction |
| adds a config field anywhere, forgets the tag | the walker raises `CapabilityScopeError` naming the field | every command fails locally |
| tags a field under `engine.<name>.*` as `False` (or leaves it untagged) | the walker raises `CapabilityScopeError`: a contradiction, not a valid state | every command fails locally |
| tags a new field `True` | new `undeclared` rows | red build |

The load-bearing row is the first: a leaf some engine declares non-`supported` has a live user-visible warning path (`_warn_ignored_config_keys` emits `engine_key_ignored` for any explicitly-set leaf whose verdict isn't `SUPPORTED`). Reclassifying such a leaf to core would silently delete that warning path with nothing else noticing: the only keys whose reclassification can destroy user-visible behavior are exactly the keys the invariant test forbids reclassifying.

**`explicitly_set_config_leaves()` is narrowed too.** `aqueduct/config.py::load_config()` calls it to find which leaves the user actually wrote, then calls `caps.verdict(leaf_id)` for each; once a core leaf leaves the checklist there is no row for it in any engine's table, so this walker narrows to the same tag; otherwise it would ask `verdict()` about an id no engine declares.

### Where the per-engine differences are published

There is one place to look up what an engine does with a given leaf: the engine matrix in `docs/compatibility.md`. It is generated from the same YAML declarations the compiler enforces, by `aqueduct dev capabilities docs`. Reading a verdict there and reading the gate's behaviour are the same act, so the published matrix cannot drift from the enforced one.

This document therefore describes the grammar once, engine-neutrally, and does not annotate each feature with per-engine footnotes. When you need to know whether your target engine runs a feature, the matrix answers it, including the `hint` explaining why an `unsupported` leaf is unsupported and whether that is permanent or not yet built.

### Type leaves

The hub type vocabulary (§9.1's `aqueduct.typehub`) is itself governed by the capability framework. One `type.<constructor>` leaf exists per hub type constructor (`type.boolean`, `type.array`, `type.decimal`, `type.timestamp_tz`, and so on, derived from the hub's own constructor enumeration rather than hand-listed) plus one `type.native.<engine>` leaf per registered engine for that engine's `<engine>:<spelling>` escape hatch. The native namespace is governed, not exempt: `type.native.spark` is `supported` on Spark and `unsupported` on DuckDB, and `type.native.duckdb` the reverse, so writing a DuckDB-only spelling into a Blueprint compiled for Spark is a compile-time error naming the offending spelling, not a runtime parser crash on the wrong engine. The gate walks every inventoried type surface (Channel `cast` columns, Ingress `schema_hint` fields, UDF `return_type`) recursively, so a composite spelling like `array<map<string,int>>` checks `type.array`, `type.map`, `type.string`, and `type.int` individually: the leaf-verdict question ("does the engine implement this constructor at all").

`ExecutorProtocol.render_type` (below) and `aqueduct.executor.protocol.render_native_type()` close the runtime half: they map a compiled spelling to each engine's own native type-system spelling at cast/schema_hint/UDF-return_type execution time, so a `supported` `type.*` leaf is backed by a real, working runtime path on both shipped engines; a composite spelling like `array<int>` renders to DuckDB's own `INTEGER[]` before the cast reaches DuckDB's parser. See §9.3 for the vocabulary's honest framing: a superset that surfaces engine divergence as a compile-time refusal rather than hiding it.

### Engine notes: differences a verdict cannot express

A verdict answers "does this engine run this leaf". Some differences are not of that shape: the engine runs the leaf, and the result differs in a way a reader needs to know. Those are listed here because there is nowhere in the data model to put them.

- **DuckDB `mode: append` is not atomic.** It reads the existing file, appends with `UNION ALL BY NAME`, and rewrites the target. A failure part-way through can leave the target damaged. Spark's `append` adds files to a directory and does not rewrite what is there.
- **DuckDB's Probe `row_count_estimate` is EXACT, not an estimate**: see §4.4. `execution_partitions` has no DuckDB equivalent at all (single-process, no partition concept) and is skipped with its own runtime warning rather than a value.
- **DuckDB materialises some Channel ops eagerly.** `sql`, `join`, `deduplicate` with a key, and every Funnel mode write into a uniquely named temp table at once instead of staying lazy. DuckDB's `register()` binds a name in a mutable catalog rather than capturing a value, and module ids are reused as registration aliases across a run, so a relation left unevaluated could resolve against the wrong binding later.
- **Bare `timestamp` is a hard compile-time rejection, not a warning** (§9.2): there is no deprecation window and no suppress mechanism; an author must write `timestamp_tz` or `timestamp_ntz` explicitly, so a Blueprint's zone semantics can never differ across engines by silent accident.

### The engine and the healing loop

Self-healing is engine-aware in two respects and engine-neutral in the rest. The engine supplies the healing prompt's persona and rules through `ExecutorProtocol.prompt_rules`, so an LLM diagnosing a DuckDB failure is told about DuckDB rather than about Spark. The engine also declares how its own exceptions map to `FailureContext` fields, through `ExecutorProtocol.extract_error`. Everything downstream of the prompt (the PatchSpec grammar, the apply gates, the budget, the patch lifecycle) is shared, because a patch is a Blueprint edit rather than engine-specific code.

Two consequences are worth stating plainly. A Blueprint patched after a failure on one engine carries no record of which engine produced the patch: if you heal on one engine and deploy on another, the patch travels with the Blueprint and gets no check beyond the ordinary compile gate. And the error signature that backs budget accounting (§8.5) is computed from the error class, location, and message, so it does not distinguish two engines that fail the same way.

### The compile gate

`aqueduct/compiler/capability_check.py` runs as the last step of `compile()` (see §3, the compiler pipeline). A module using an `unsupported` leaf fails compilation with a `CompileError` naming the module, the leaf, the engine, and the capability's hint. A module using an `ignored_with_warning` leaf gets a suppressible warning under rule_id `engine_key_ignored`, following the same `warnings.suppress` mechanism as every other compiler warning (see §4.2). A `requires` version constraint does not fail compilation: compile time has no way to know which dependency versions are installed in the environment that will run the job.

The gate checks three kinds of leaf:

1. `module.type.<Type>`, the module kind, emitted for every module. An engine that does not run a whole module type fails compilation cleanly instead of crashing mid-run.
2. The per-module config-dispatch leaves: Channel op, Egress mode, format and on-new-columns policy, Ingress format, Junction and Funnel fan mode.
3. The `feature.*` leaves the compiled Manifest actually exercises, derived from real Manifest fields rather than a hardcoded list. `feature.python_udf` and `feature.java_udf` come from each `udf_registry` entry's `lang`, so a Blueprint that declares no UDF exercises no UDF feature.

On an engine that declares every leaf `supported`, all three kinds are a no-op and the gate stays silent.

**Per island (2.34).** A Blueprint compiled with one or more modules pinning `engine:` (§4.3's "Cross-engine handoff" subsection) is partitioned into engine islands before this gate runs, and each island's modules are checked against its OWN engine; never against a different island's engine. This is what makes "an island whose engine is not registered" a `CompileError` (the same `UnknownEngineError` above) rather than only ever checking the single `deployment.engine` default. For a single-engine Blueprint (no module pins `engine:`) there is exactly one island, and this degenerates to the pre-2.34 single-gate call exactly.

The manifest-scoped `feature.*`/`type.*` leaves a `udf_registry` entry drives (UDF language, UDF return type) are NOT owned by one module (a UDF is registered once and referenced from SQL text by name) so `aqueduct/compiler/udf_attribution.py::attribute_udfs_to_islands` attributes each UDF to the island(s) whose SQL can actually reference it before the gate runs, reusing the same sqlglot parse `aqueduct/compiler/lineage.py`'s column lineage already does (never a second SQL parser). This is what makes the phase's flagship shape work: a Java UDF used only inside a Spark island's Channel SQL compiles cleanly even with an unrelated DuckDB island present elsewhere in the same Blueprint, because DuckDB's `feature.java_udf: unsupported` verdict (DuckDB is not on the JVM; a permanent gap, unlike `feature.python_udf`, which both engines support) is only checked against islands that actually reference the UDF. Attribution is fail-closed: a SQL-bearing construct sqlglot cannot parse keeps its island in that UDF's checked set rather than dropping it, and a UDF with no positively-attributed island AND no unparseable construct anywhere falls back to every island; the same conservative behavior as before per-island UDF attribution existed. The scanned surfaces are Channel `op: sql`'s `query`; `op: join`/`op: filter`'s `condition`/`expr`; `op: deduplicate`'s `order_by`; `op: sort`'s `order_by`/`columns` (either spelling, a string or a list; a UDF call there is legal on both engines even though Spark's own sort implementation only honors a trailing `ASC`/`DESC` token and would fail such a call at runtime, since the same field genuinely invokes the UDF on a DuckDB-resolved island); any Channel's `spillway_condition`; a Junction `conditional` branch's `condition`; and an Assert `sql`/`sql_row` rule's `expr`. Channel `op: select`'s column list is deliberately excluded (it names columns, not expressions, so a UDF call written there fails at runtime on either engine regardless of the gate) as are Probe/Assert `type: custom`'s Python callables, which carry no SQL text to scan.

### The synthetic Handoff module (2.35)

A boundary edge is where a compiled Blueprint actually crosses engines. The compiler splices in a synthetic **Handoff** module at each one, immediately after island derivation and before the per-island capability gate above: `A -> B` becomes `A -> handoff -> B`, with the original edge's port preserved on both new edges; `main` for an ordinary edge, or a Junction branch id when a Junction's own branch edge crosses the boundary directly (a cross-island spillway edge is already a `CompileError` in v1, §4.3, so `spillway` never reaches this point). Disjoint components pinned to different engines have no edge between them at all, so they get zero handoff modules: the same free lunch as the disjoint-component case in §4.3.

`Handoff` is a real `ModuleType` value, but it is **not authorable**: `parser.schema.ModuleSchema.validate_type` rejects `type: Handoff` in Blueprint YAML by name, with a dedicated message rather than the generic "unknown module type" one. Every handoff `Module` the compiler builds carries `synthetic=True` (mirroring `Edge.injected` one level up) and `engine=None`: it bridges two engines rather than resolving to one, so its config carries `from_engine`/`to_engine` instead. Its id is generated (`<from_id>__handoff__<to_id>`, collision-proof because `__` is reserved and rejected in authored module ids) and it gets its own rows in the observability store like any other module, and a passthrough row in column lineage (`output_column`/`source_column` both `"*"`, `source_table` the upstream module) rather than a SQL-parsed one.

**Transport contract (v1).** An engine-native parquet write to a URI: the upstream island materializes its output (`df.write.parquet` on Spark, `COPY ... TO ... (FORMAT PARQUET)` on DuckDB), the downstream island reads it back. Parquet is fixed, not a config key. A handoff module's `config` carries everything the executor needs to perform that write and read:

```json
{
  "edge_id": "extract__handoff__agg",
  "from_module": "extract",
  "to_module": "agg",
  "from_engine": "spark",
  "to_engine": "duckdb",
  "port": "main"
}
```

`edge_id` (equal to the handoff module's own id) is the one piece of the `<root>/<manifest_hash>/<run_id>/<edge_id>/` directory template (§10.4.3) only the compiler can supply: `root` comes from `aqueduct.yml`'s `handoff:` block, `manifest_hash`/`run_id` are resolved by the executor at run time, the same way `checkpoint_root` is threaded to `execute()` rather than baked into the Manifest.

**Type fidelity across the boundary.** The write/read is a raw DataFrame/relation passthrough: no hub type resolution or `render_type` mapping runs on the boundary, so type fidelity across it depends on each engine's own Parquet reader/writer agreeing on file-level logical-type annotations. Every hub constructor round-trips faithfully over a real Spark↔DuckDB Parquet handoff, including `timestamp_tz`: Aqueduct's Spark session factory (`aqueduct/executor/spark/session.py::make_spark_session`) sets `spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS` at session creation, as Aqueduct's own default, in place of Spark's own default (`INT96`, a legacy Hive-interop encoding with no Parquet logical-type annotation distinguishing an instant-aware timestamp from a naive one). This is set once at session creation, never toggled around an individual write, so it can never depend on thread timing under `--parallel` (independent components share one SparkSession). A user's own `engine.spark.conf` value for `spark.sql.parquet.outputTimestampType` always wins: the factory only applies its default when the key is absent from the resolved config. `timestamp_ntz` was never affected by this (Spark always writes it with a modern, correctly-annotated logical type). The reverse direction is unaffected in both variants: DuckDB's own Parquet writer always annotates `TIMESTAMPTZ`/`TIMESTAMP` correctly, and Spark reads both back as the matching hub type. `duration(unit)` (2.38) round-trips faithfully by construction rather than by a session-factory fix: it renders as a plain `BIGINT`/`bigint` on both engines (§9.1's "Why `duration` is integer-backed"), and a signed 64-bit integer carries no logical-type ambiguity a Parquet reader/writer could disagree about; verified both directions over a real Spark↔DuckDB handoff.

**Compile-time visibility.** Every insertion emits a suppressible warning, rule id `cross_engine_handoff_io`, naming the boundary (`Cross-engine handoff 'extract__handoff__agg': 'extract' (spark) -> 'agg' (duckdb)...`) through the same `aqueduct.warnings.emit` machinery as every other compiler warning; the extra I/O a split introduces is a real cost, visible before the run rather than discovered mid-run.

**Capability-gate interaction.** `module.type.Handoff` is a governed capability leaf like any other `ModuleType` member, declared `supported` on both shipped engines as of 2.36 (real engine-native transport exists and is tested: see below). The verdict is never actually consulted at the compile gate: a handoff module's id is never a member of any island's `module_ids` (islands are derived from the pre-insertion graph) so the per-island gate's per-island `manifest.modules` filter excludes it from every island's check by construction, the same way a disabled module is already excluded. That invariant is deliberately preserved rather than folding a handoff module into island membership to make it "execute": doing so would route it back through the per-island gate for no reason, since real execution goes through the orchestrator below instead (`tests/test_compiler/test_islands.py::test_handoff_modules_never_reach_the_capability_gate` enforces this).

### Runtime execution of a Handoff module (2.36)

Compile-time synthesis (2.35, above) only builds the graph shape; a polyglot Manifest is actually run by `aqueduct.executor.orchestrator.run_polyglot()`, a coordinator layered ABOVE the single-engine `ExecutorProtocol.execute()` calls every engine already implements. A single-engine Manifest (including one compiled for a single-engine Blueprint, which always has exactly one island) runs through this same function unchanged: it is a strict superset of the pre-2.36 single-engine path, not a special case of it.

**Per-island session lifecycle.** Sessions open LAZILY, one per island, in the topological order of the island dependency graph (an island depends on every island whose Handoff output it reads): a boundary edge is a dependency; disjoint different-engine components have none, so both still run, in `manifest.islands` order. A session closes immediately after its island's last module, via `ExecutorProtocol.close_session`. This is a deliberate v1 choice, not yet optimized: an engine's session closes even if that SAME engine recurs later in the run (`spark -> duckdb -> spark` opens two separate Spark sessions), rather than being kept alive across the gap. One `run_id` covers the whole `aqueduct run` invocation regardless of how many islands/sessions it opens: `run_id` was never an engine's own session/application id, and this does not change that.

**Transport, realized.** Each boundary's Handoff module dispatches on which side of the boundary the current island sits: the WRITE side (this island produced `from_module`) materializes the upstream DataFrame/relation to the resolved spill URI (`df.write.parquet` on Spark, `COPY ... TO ... (FORMAT PARQUET)` on DuckDB); the READ side reads it back (`spark.read.parquet`, DuckDB `read_parquet` over the directory's `*.parquet` files). A single sub-Manifest given to one island's `execute()` call never contains both halves of the same boundary's edges (only the one relevant to that island) so a Handoff module dispatches unambiguously by which edge is present.

**Spill lifecycle.** Directory layout is exactly `<root>/<manifest_hash>/<run_id>/<edge_id>/` (§10.4.3). Deleted when the whole run succeeds; kept when it fails and `handoff.keep_on_failure` is true (the default); the resume story: passing `resume_run_id` to `run_polyglot()` makes an island whose OUTGOING handoff spill already exists under that prior run skip re-execution entirely (its modules report `status="skipped"`) and downstream islands read the prior run's spill instead of a fresh one. This is a MANUAL `aqueduct run --resume <run_id>` after a plain failure, with no Blueprint edit in between: the Manifest hash, and therefore the spill's directory, is unchanged from the failed run. A heal-triggered retry never reaches this path at all: `aqueduct/cli/run.py` passes `resume_run_id` only `if patch_count == 0`, so once a patch has been applied the retry carries no resume id, and even if it did, the heal already changed `manifest_hash` (see below) so the prior spill directory would not be the one this run resolves to. A run's own cleanup targets its OWN `run_id` directory AND, when this run actually resumed from a prior one and then SUCCEEDED, the resumed-FROM `run_id` directory as well: that spill was kept for exactly the rerun that has now consumed it. A FAILED resume keeps it (still resumable). An orphan sweep runs at the START of `run_polyglot()`, before the current run's own spill exists on disk. Because a heal changes the compiled Manifest and therefore `manifest_hash`, consecutive runs of the same Blueprint across a patch write under DIFFERENT hash directories: the sweep scans the ENTIRE `handoff.root`, across every hash directory, not only the current run's, or a prior hash directory's kept-failure spill would never be revisited by anything and would accumulate forever, one heal at a time. It reclaims any `run_id` directory (under any hash directory) whose `run_records` status is terminal and not a still-protected kept failure (a successful run whose own cleanup never ran, a failed run when `keep_on_failure` is false, a failed run that a LATER succeeded run of the same blueprint has already resolved (see §10.4.3) or a `run_id` with no `run_records` row at all), and never touches a non-terminal (still-running or crashed-without-a-terminal-status) run's spill: the decision is keyed on `run_records` alone, so it is correct even when several Blueprints share one `handoff.root`. A hash directory left empty once every run underneath it has been swept is reclaimed too.

**The two IO stacks, and the loud-not-silent rule.** The ENGINES write/read spill natively (no `fsspec`: Spark and DuckDB both already speak local and remote URIs on their own, which is what keeps this cluster-ready with no new backend abstraction). Aqueduct's OWN cleanup (delete-on-success, keep-on-failure, orphan sweep) uses `fsspec` for a remote `handoff.root`, because unlike an engine's writer, cleanup has no engine-native way to list/delete an arbitrary URI scheme. On a remote root with `fsspec` NOT installed, engine writes keep succeeding while cleanup silently could not act: `run_polyglot()` makes this loud instead of silent: it emits a suppressible warning (rule id `handoff_cleanup_unavailable`) at the start of every run whenever the root is remote and `fsspec` is absent, naming the exact condition, rather than letting spill accumulate behind a debug log line.

**Observability.** A Handoff module gets a `module_metrics` row like any other module (`bytes_written` on the write side, `bytes_read` on the read side, plus `duration_ms`) measured from the spill directory's on-disk size, engine-agnostically. `Surveyor.record()` accepts an explicit `engine` override (falling back to its own construction-time engine when omitted) so a polyglot run's `FailureContext.engine` and structured error extraction (`ExecutorProtocol.extract_error`) reflect the ISLAND that actually failed, not the run's nominal deployment engine. The cross-engine heal-patch provenance gate (§10.9, `cross_engine_heal`) is checked once per DISTINCT island engine present in the compile, rather than only against the single `deployment.engine` default, for the same reason.

**Wired into `aqueduct run` (2.37).** `aqueduct/cli/run.py`'s healing loop routes a Manifest with more than one island through `run_polyglot(..., record_result=False)` in place of the single-engine `execute()` call; a single-engine Manifest (`len(manifest.islands) <= 1`) takes the exact same path it always has, unchanged. `record_result=False` lets the CLI call `surveyor.record(result, exc=..., engine=result.failed_engine)` itself, so a failed run is attributed to the ISLAND that actually failed rather than `deployment.engine`: the healing prompt (`generate_agent_patch`/`generate_cascade_patch`), and the `patch_index`/`healed_by` provenance record both key off that same failing-island engine. Lifecycle hooks pass `session=None` for a polyglot run (no single live session survives to hook time: every island's is already closed), which falls through to hooks' existing subprocess path; a `blueprint:` hook entry with `in_process: true` gets a `[hook_in_process_unavailable]` warning naming why (§4.2). Module-range selection (`--from`/`--to`) refuses a polyglot Manifest outright (`CONFIG_ERROR`) rather than silently running the whole graph: which island(s) a range spans is real cross-island work not attempted here. Rendering: the run header names every engine involved (not the single nominal default), each module's transcript line carries its own resolved engine, and a Handoff module's result renders as a first-class step (`⇄ from → to (engineA→engineB)`, bytes transferred, duration) rather than an anonymous module id. `report --format json` gains a top-level `engines` list and a per-module `engine` field (both persisted by `Surveyor.record()`, present for single-engine runs too). One known v1 cost, not yet optimized: each heal iteration calls `run_polyglot()` fresh, so every island's session is rebuilt on a retry rather than reused across iterations the way the single-engine path reuses its one session; the same "not yet optimized" framing as same-engine session reuse WITHIN a run, above.

### The version check

`aqueduct doctor` validates the constraint the compile gate cannot. `aqueduct/doctor/checks_io.py::check_capabilities` walks a compiled Blueprint's used capabilities and, for each one carrying a `requires` constraint, compares the installed dependency version (via `importlib.metadata`) against the declared specifier, reporting `ok`, `fail`, or `skip` (dependency not installed) per capability. `docs/compatibility.md` lists the version constraints each engine currently declares.

### How the leaf set stays honest

The canonical leaf set is derived rather than hand-maintained. Module types and pydantic schema fields come from `aqueduct/parser/schema.py` introspection; Channel ops, Probe built-in signal types, Egress modes, and Junction and Funnel fan modes come from named constants sitting next to their dispatch code (`channel_ops.py`, `probe_plugins.py::BUILTIN_SIGNAL_TYPES`, `spark/egress.py`, `spark/junction.py`, `spark/funnel.py`); a small hand-curated set covers cross-cutting feature flags and the few formats with a dedicated code path (`aqueduct/executor/capability_leaves.py`). Every `aqueduct.yml` key comes from `AqueductConfig` introspection (`aqueduct/executor/config_leaves.py`).

The closure test (`tests/test_capabilities/test_closure.py`) compares that derived set against each engine's YAML read straight from disk, and fails the build if a derived leaf has no row, if a row is still `undeclared`, or if a row names a leaf that no longer exists.

Reading the YAML from disk rather than from the loaded registry is what makes the test meaningful. The walker is code and the declaration is data, and the test is only worth running if the two are independent sources that can actually disagree. A test that compares the registry against the walker its own table was generated from cannot fail, whatever the table says.

### Verdict-to-test linking

"`supported` requires a test" (see Verdicts, above) was policy rather than mechanism until every `supported` **EXECUTION** row: the leaves `aqueduct/executor/capability_leaves.py::execution_leaves()` derives (`module.type.*`, `channel.op.*`, `probe.signal.*`, `ingress.format.*`, `egress.format.*`/`.mode.*`/`.on_new_columns.*`, `junction.mode.*`, `funnel.mode.*`, `feature.*`); gained an optional `tests:` key: a list of pytest node ids (`tests/test_executor_duckdb/test_executor.py::test_channel_filter`) or bare file paths where a whole file exercises the leaf. `config.*` leaves and the schema-authoring leaves (`module.field.*`, every `<type_lower>.field.*` and `<block>.field.*`: 2.42's per-module-type split, §4.3) are out of scope; they are warn-only or engine-invariant, with no per-engine runtime dispatch to exercise, so requiring a test id there would be busywork; `execution_leaves()` derives the in-scope set from the same per-category walkers `all_leaves()` unions, so the boundary is code, not a hand-maintained list.

`tests/test_capabilities/test_verdict_test_links.py` enforces two things per engine, reading each declaration from disk the same independent-sources way `test_closure.py` does: every `supported` EXECUTION row names at least one test id, and every declared id resolves against the real test tree (the file exists; a `::name`/`::Class::method` node id names something pytest would actually collect). A row failing either check is a genuine gap, not a formatting error: the fix is to link a real test or leave the leaf unbacked and let the build say so loudly, never to invent an id or quietly downgrade the verdict. `aqueduct dev capabilities check` also reports missing/dangling test links (informational; it does not gate this command's exit code, which stays keyed to leaf completeness) so the same signal is visible outside pytest. `sync`/`scaffold` never touch an existing row's bytes, so a `tests:` block survives a sync unchanged; a freshly scaffolded leaf gets a bare `undeclared` string with no `tests:` key at all.

### Adding a leaf: the workflow

1. Add a field to `parser/schema.py` or `config.py`, or a Channel op, write mode, fan mode, or feature flag.
2. The build breaks. Engine registration raises `CapabilityDeclarationError`, and the closure test names the offending leaf.
3. Run `aqueduct dev capabilities sync`, which appends the new leaf to every engine's YAML as `undeclared`. The build stays red, because `undeclared` is not a verdict.
4. A human replaces each `undeclared` with a real verdict for that engine.
5. The build passes.

`aqueduct dev capabilities check` reports drift without writing, which is what CI runs. `aqueduct dev capabilities docs` regenerates the engine matrix in `docs/compatibility.md` from the declarations.

The four commands live in the installed package (`aqueduct/executor/capability_tooling.py`, exposed through `aqueduct/cli/dev.py`), not in the repository's `scripts/` directory, which is not in the wheel. An engine registers only once every leaf on ITS OWN checklist (grammar leaves plus its own engine-scoped config leaves: see "Config-leaf scoping" below) carries a verdict, so an author who cannot generate the table cannot ship an engine: hand-writing ~200 rows does not scale. `scripts/capabilities.py` is a thin wrapper that forwards to the same code, so there is exactly one implementation.

### Starting a new engine

An engine author needs nothing but `pip install aqueduct-core`. `aqueduct dev capabilities scaffold --engine <name>` writes a complete `capabilities.yml` with every leaf present and every verdict set to `undeclared`, so the author is walked through the entire grammar and config surface one leaf at a time and the engine will not register until each row is a real decision. The scaffold is generated from the walkers, so it cannot go stale the way a checked-in template would.

Do not copy an existing engine's declaration. Cloning Spark's table hands a new engine 206 `supported` rows, which is a silent claim to implement the whole grammar and precisely the blindness the framework exists to prevent. Read it as a reference.

### `ExecutorProtocol`: the execution contract

A capability declaration says what an engine supports. `ExecutorProtocol` (`aqueduct/executor/protocol.py`) says how core talks to it. Every engine registers exactly one `ExecutorProtocol` instance, alongside its `EngineCapabilities`, as an import side effect of its `aqueduct.engines` entry-point module. The contract has three required members and an optional session-lifecycle pair.

**`execute`**: `(manifest, session, ...) -> ExecutionResult`. A compiled `Manifest` and an engine session handle in, a frozen `ExecutionResult` out. The common run options (`run_id`, `store_dir`, `checkpoint_root`, `surveyor`, `depot`, `resume_run_id`, `from_module`, `to_module`, `block_full_actions`, `warnings_*`) are the uniform part every engine accepts. A second group (`OPTIONAL_EXECUTE_KWARGS` in `aqueduct/executor/protocol.py`: `parallel`, `use_observe`, `sampling`, `observability_store`) is optional: the shared run path passes them to every engine, and `ExecutorProtocol.execute_kwargs` (a `frozenset[str] | None`, `None` meaning "consumes everything") names which of them the engine's real `execute()` accepts. Every caller that might pass one of these (`aqueduct/cli/run.py`, the patch sandbox gate (`aqueduct/patch/preview.py::run_sandbox_gate`)) routes through `call_execute()`/`filter_execute_kwargs()` (same module), which drops anything outside the target engine's allowlist and emits one suppressible `engine_kwarg_ignored` warning per dropped kwarg (see "Config-leaf governance", below) instead of forwarding an option the real `execute()` would raise on, or dropping it with no signal at all. An engine's real `execute()` therefore never receives an option it cannot honour, and the caller is told when that happened.

**`extract_error`**: an engine exception (or `None`) mapped to a `FailureContext` field dict (`error_class`, `root_exception`, `sql_state`, `suggested_columns`, `object_name`). Required, so an engine cannot register without a way to turn its own failures into the structured root-cause block the healing LLM reads.

**`prompt_rules`**: a `PromptRules` pack, the engine-specific half of the healing system prompt. It carries `persona` (the prompt's opening line), `root_cause_note` (what the engine's structured root-cause block contains, the prose counterpart of `extract_error`'s output), `rules` (the engine's error idioms, its advice, its API and config references), and `defer` (a `DeferRules`: the engine's slice of the defer-to-human section, naming the infrastructure it can actually fail on and the languages its UDFs are written in). All are required except `DeferRules.extra_bullets`, where "this engine has no extra defer category" is a complete answer.

**`make_session` / `close_session`**: `(SessionSpec) -> session` and `(session) -> None`, how an engine builds the handle `execute` runs against and tears it down. `SessionSpec` is the engine-agnostic construction request (`blueprint_id`, `engine_config`, `master_url`, `quiet`, `quiet_startup`, `engine_options`), the union of what registered engines need; an engine reads the fields it understands, so a single-node engine ignores `master_url`. `engine_options` is an opaque per-engine bag reserved for session needs the named fields don't cover: unpopulated by core, read only by an engine that understands its keys. Both members are optional at registration, because a compile-only engine or a test double has no session. The run path resolves them through `session_factory()` and `session_closer()`, which raise `EnginePluginError` naming the engine if a runnable engine reached the CLI without a factory.

**`render_type`**: `(HubType | NativeType) -> str`, one parsed hub type (§9.2's `aqueduct.typehub`) rendered to the engine's own native type-system spelling. Optional at registration, the same optionality class as `make_session`/the diagnostic readers below, not the required class: a third-party engine can register without a complete type mapper. The degrade contract is narrow and explicit rather than a silent fallback: an engine with no `render_type` still runs a Blueprint using only its own native escape-hatch spellings (`"<engine>:<spelling>"`, rendered as `.spelling` verbatim; no mapping needed) and any spelling its own runtime parser accepts raw, but a hub spelling (`bigint`, `array<int>`, …) against it is refused, not silently forwarded to a parser that cannot read it. `aqueduct.executor.protocol.render_native_type(engine, spelling)` is the one seam every engine's cast / schema_hint / UDF-return-type runtime consumption routes through: it parses `spelling` via `typehub.parse_type`, unwraps a same-engine `NativeType` verbatim, raises `EnginePluginError` for a foreign-engine `NativeType` (defensive; the compile-time `type.native.*` gate should already have refused it on any gated path) or for a hub type with no `render_type` registered, and otherwise calls the engine's `render_type`. Both shipped engines register a mapper: Spark's (`aqueduct/executor/spark/type_render.py`) is mostly identity, since the hub's canonical spellings already match Spark DDL character-for-character, except the timestamp pair (`timestamp_tz` renders to Spark's plain `timestamp`, `timestamp_ntz` to Spark's own `timestamp_ntz`); DuckDB's (`aqueduct/executor/duckdb_/type_render.py`) renders every constructor to DuckDB's own SQL spelling, including the composite constructors (`array<T>` → `T[]`, `map<K,V>` → `MAP(K, V)`, `struct<name:type,...>` → `STRUCT(name TYPE, ...)`, recursively). Both mappers are pure string logic (no `pyspark`/`duckdb` import) so neither needs the lazy-import discipline `execute` does.

### The healing prompt is composed

The healing system prompt is composed, not monolithic. The engine-independent scaffold (the PatchSpec schema, the op-selection table, the provenance rules, the output contract, the generic defer categories, the coaching and history sections) lives in the agent layer and holds no engine-specific text. At prompt-build time the agent pulls the target engine's `PromptRules` pack through this registry and renders it into the scaffold's engine slots. The agent layer imports no engine specifics, and the engine layer imports nothing from the agent layer. A new engine therefore ships its own healing persona and rules with its executor: it cannot inherit another engine's advice by accident, and it cannot register with none at all.

The scaffold is not one string. Parts of the prompt, including the defer-to-human section, are assembled at request time and only appear under certain settings. The guard against engine text leaking across therefore composes the whole prompt for a non-Spark engine, across every combination of those settings, and checks that no Spark vocabulary survives. Scanning the template constants alone would miss every fragment built around them.

Both requirements are enforced structurally rather than by convention. `ExecutorProtocol.__post_init__` raises `EnginePluginError` at construction time if `execute`, `extract_error`, or the `PromptRules` pack is missing or incomplete. `get_executor(engine)` (`aqueduct/executor/__init__.py`) and `get_protocol(engine)` (`aqueduct/executor/protocol.py`) resolve through the same `load_engines()`-backed registry that `get_capabilities()` uses, and raise the same `UnknownEngineError` for an unregistered engine. Every failure on this seam is an `AqueductError`, never a bare builtin.

Constructing an engine's protocol object never imports the engine's own heavy dependencies. For Spark, only calling `execute(...)` imports `pyspark`.

### Config-leaf governance

Everything above governs the Blueprint grammar. `aqueduct.yml`, the engine config, has its own leaf set: every `AqueductConfig` pydantic field, derived the same way (`aqueduct/executor/config_leaves.py::all_config_leaves()` walks the real `aqueduct/config.py` models rather than a hand-written list) and namespaced `config.*`, giving leaf ids like `config.engine.spark.master_url`, `config.stores.observability.backend`, and `config.agent.sandbox_master_url`. A field typed as a list or dict of sub-models (`stores.depots`, `agent.cascade`) is one atomic leaf rather than one per dynamic key. These leaves fold into the same closure test as the grammar leaves, so a registered engine must carry an explicit verdict for the union of both sets.

The gate exists because a key that means nothing on the selected engine, `engine.spark.master_url` on a single-node engine for instance, was otherwise a silent no-op: accepted, validated, then ignored with no signal. At config-resolution time the gate checks every leaf the user explicitly set (pydantic's `model_fields_set`, at each nesting level, so untouched defaults never warn) against the target engine's verdict.

Config-leaf verdicts always warn and never error. That asymmetry with the Blueprint gate is deliberate. A Blueprint is written for one pipeline on one engine, so an `unsupported` leaf there is a `CompileError`. An `aqueduct.yml` is not: the same file is expected to stay valid across engines, deployment profiles, and test overrides, and hard-failing config load over one inert key would make that impossible. So a config leaf resolving to anything other than `supported` emits one suppressible `engine_key_ignored` warning, using the same rule id and suppression machinery as the compile-time gate, and loading proceeds. `aqueduct/config.py::load_config()` runs the check once `deployment.engine` is validated, immediately before returning the resolved `AqueductConfig`.

One limit is worth knowing: the rule covers keys that are inert on an engine, not keys that mean something different on one. A key that changes results rather than being ignored is not something a warning can describe.

### `engine_kwarg_ignored`: the sibling rule for `execute()` kwargs

`engine_key_ignored` covers Blueprint/`aqueduct.yml` **keys**. `execute()` **kwargs** (the optional run-option arguments a caller passes to `ExecutorProtocol.execute`: `parallel`, `use_observe`, `sampling`, `observability_store`, listed as `OPTIONAL_EXECUTE_KWARGS` in `aqueduct/executor/protocol.py`) are a different surface with the same failure mode: a kwarg meaningful to one engine and meaningless to another. `engine_kwarg_ignored` is its own rule id, using the identical `aqueduct.warnings.emit` suppression machinery (`warnings.suppress` in `aqueduct.yml`, or `--suppress-warning` on the CLI), so it composes with every other warning rule the same way.

`call_execute(engine, ...)` / `filter_execute_kwargs(engine, kwargs, ...)` (`aqueduct/executor/protocol.py`) are the single mechanism: they look up the target engine's `ExecutorProtocol.execute_kwargs` allowlist, drop any `OPTIONAL_EXECUTE_KWARGS` name outside it, and emit one `engine_kwarg_ignored` warning per dropped kwarg naming both the kwarg and the engine. An engine that declares `execute_kwargs=None` (Spark today: its real `execute()` has a parameter for every name in `OPTIONAL_EXECUTE_KWARGS`) gets no filtering at all. Every caller that might pass one of these kwargs routes through this seam: `aqueduct/cli/run.py`'s run loop and the patch sandbox gate (`aqueduct/patch/preview.py::run_sandbox_gate`, which also resolves its target engine's own `ExecutorProtocol` for its session and `execute()` rather than hardcoding Spark); so a kwarg the target engine cannot honour is never silently dropped and never raises `TypeError` out of a mismatched signature.

---

# **11. Engine scope & boundaries**

## **11.1 What Aqueduct is**

- A **batch processing control plane**. Every pipeline run is finite.
- A **declarative layer over an execution engine**. Engineers describe *what* the pipeline does, not *how* the engine executes it.
- An **LLM-integrated operations tool**. Self-healing, patch lifecycle, and FailureContext are core.

## **11.2 What Aqueduct is not**

| Out of scope | Recommended alternative |
| :- | :- |
| **Streaming (Structured Streaming, Kafka)** | Deferred. Requires continuous process lifecycle management. |
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

## **11.4 When to split engines across a Blueprint**

> **EXPERIMENTAL.** Splitting one Blueprint across engines is experimental and receives no
> further investment. A new engine need not support handoff to be a first-class Aqueduct
> engine; running whole single-engine Blueprints is the bar.

A module's `engine:` field (§4.3) and the compiler-inserted Handoff module (§10.9) let one Blueprint span both engines. That capability has a cost, and the question of when to pay it deserves its own answer.

**The cost.** A boundary edge is a full materialise to parquet on one side and a full re-read on the other, an order of cost close to a shuffle, and it is paid on every run, unconditionally, whether or not the split earns its keep. If a stage can run in the engine already in use, it usually should. Do not pin an engine to make a stage faster: for pure performance, staying in one engine normally wins, because a handoff adds I/O a single-engine plan never pays.

**Three reasons that do earn it, none of them speed.**

1. Capability. The other engine has a format, an extension, or a function this one lacks, and there is no equivalent way to get the same result without it.
2. Scale mismatch. A large reduce runs on Spark, and the small result it produces is finished on an engine whose per-task overhead no longer dominates at that size.
3. Incremental migration. A pipeline moves to a new engine one stage at a time instead of being rewritten in one pass.

**The warning is the mechanism, and it points both ways.** Every boundary edge the compiler inserts emits a suppressible `cross_engine_handoff_io` warning (§10.9) naming the two modules and the two engines involved, before the run starts. If a `cross_engine_handoff_io` warning appears and the split was not deliberate, this section is the checklist for whether it should be. If it was deliberate, expect the warning: it confirms the boundary was found, not a defect to silence without reading it first.

**Portability.** A Blueprint with no `engine:` field anywhere is fully portable: it compiles and runs on whichever engine `deployment.engine` names, and it can move between engines without editing the graph. Adding one `engine:` pin declares a dependency on that engine for that module and everything the compiler resolves as its island (§4.3). Pin deliberately.

**A DuckDB-specific ceiling.** DuckDB opens a bare `:memory:` connection for every session, with no persistent-file option (§10.9). A boundary edge that hands data to a DuckDB island is bounded by that island's RAM, not by disk, regardless of how much disk the upstream Spark island has to work with.