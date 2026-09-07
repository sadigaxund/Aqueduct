# 1-3. Overview

# **1. Introduction**

## **1.1 Purpose of this document**

This document is the complete system design and implementation reference for Aqueduct, a declarative data-pipeline engine with integrated LLM-driven self-healing. It covers every component, design decision, data contract, configuration schema, and runtime behaviour.

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
| **Observability Store** | Append-only log of all runtime signals: Probe readings, stage metrics, errors. Per-pipeline routing (1.1.0+): `.aqueduct/<blueprint_id>/observability.db`. Grows unbounded; pruning it is the operator's responsibility (no built-in retention feature). |
| **Column Lineage** | Column lineage graphs and Flow Reports live in the `column_lineage` table **inside the observability store** (no separate store). The `stores.lineage` config option is **removed**; a legacy block in `aqueduct.yml` raises a `ConfigError`. |
| **Depot (KV Store)** | Persistent key-value store for pipeline state across runs: watermarks, last-run metadata. Configured under `stores.depots` (a name-keyed map of mounts; a `default` mount always exists). Every mount is **per-blueprint isolated** by default, by one of two mechanisms (§10.4.4): a mount with no `path` gets its own file at `.aqueduct/<blueprint_id>/depot.db`, and a mount with an explicit `path` shares that file with transparent `<blueprint_id>:` key prefixing. Opt a mount into cross-blueprint sharing with `shared: true`, which requires an explicit `path` (read via `@aq.depot.<name>.get`). Incremental Channels persist their watermark to the Depot (if configured); without a Depot the watermark is lost between runs and every run re-scans all source data. The compiler emits `perf_incremental_watermark_scan` when an incremental Channel has no upstream cache/checkpoint, because computing `MAX(watermark_column)` on the output requires a second full scan. |
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

**`base_dir`**: the top-level Blueprint file's own directory (empty string when compiled from an in-memory dict with no file). Every executor-side user-code import site, Assert `type: custom`'s `fn:`, Probe `type: custom`'s `module:`/`entry:`, `udf_registry`'s Python `module:`, and Egress/Ingress `format: custom`'s `class:`, resolves its dotted path against `base_dir` first (a sibling `.py` file next to the Blueprint), falling back to a normal `import` for installed packages. This exists because the `aqueduct` console-script entry point never puts the Blueprint's directory on `sys.path` (unlike `python -m`/`python -c`), so a bare `import` of a sibling file fails unless the user manually mutates `sys.path`. For an Arcade sub-Blueprint, callable refs still resolve against the **top-level** Blueprint's `base_dir`, not the arcade's own directory (one Manifest per compilation unit).

---

