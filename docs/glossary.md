# Glossary

Aqueduct uses a consistent theme (aqueducts, water flow) for its own names.
This page maps each themed name to the industry-standard term an experienced
data engineer already knows, so a new reader can map the vocabulary quickly
instead of re-learning concepts under new names. `docs/specs/01-overview.md`
§2 (Naming glossary) is the canonical, terse definition list; this page adds
the industry-term mapping and links each row to the spec section that owns
the full definition. Where a term has no honest industry equivalent, the
"Industry term" cell says so and the third column describes it in plain
language instead.

| Aqueduct term | Industry term | What it is | Where it is specified |
|---|---|---|---|
| Blueprint | Pipeline definition (declarative DAG file) | It is the YAML file that declares a pipeline's modules, edges, context, retry policy, and agent config. | [Blueprint format](specs/02-blueprint.md#42-top-level-structure) |
| Ingress | Source connector / reader | It is a module type that reads data from an external source into the pipeline. | [Ingress](specs/02-blueprint.md#ingress) |
| Channel | Transform stage | It is a module type that applies a transformation to one or more upstream DataFrames without triggering an engine action. | [Channel](specs/02-blueprint.md#channel) |
| Junction | Fan-out / conditional router | It is a module type that splits one incoming DataFrame into multiple downstream branches, by condition, broadcast, or partition key. | [Junction (Fan-out)](specs/02-blueprint.md#junction-fan-out) |
| Funnel | Fan-in / merge | It is a module type that merges multiple upstream DataFrames into one, by union, distinct union, coalesce, or positional zip. | [Funnel (Fan-in)](specs/02-blueprint.md#funnel-fan-in) |
| Egress | Sink / writer | It is a module type that writes data to an external target, and it is the only module type that triggers an engine action. | [Egress](specs/02-blueprint.md#egress) |
| Spillway | Dead-letter port | It is the error output port present on every module, and it routes row-level errors to a designated downstream module. | [Typed spillway routing](specs/02-blueprint.md#typed-spillway-routing-error_types) |
| Depot | Key-value state store | It is Aqueduct's persistent key-value store, and pipelines read and write named keys across runs, such as incremental watermarks. | [Depot mount routing](specs/07-stores-and-ops.md#1044-depot-mount-routing-duckdb) |
| Probe | Observability tap | It is a module type that attaches a non-blocking metric or sample check to another module's output edge, with zero engine actions by default. | [Probe](specs/02-blueprint.md#probe) |
| Arcade | Reusable sub-pipeline (inlined at compile time) | It is a module type that embeds a whole sub-Blueprint as one module in a parent Blueprint, and it is expanded into the parent's flat module list at compile time. | [Arcade (Sub-pipeline)](specs/02-blueprint.md#arcade-sub-pipeline) |
| Regulator | Conditional gate | It is a module type that reads one wired signal (from a Probe or Assert) and, when that signal is not passing, skips, aborts, or triggers the healing agent on everything downstream; it compiles away entirely when nothing is wired to it. | [Regulator](specs/02-blueprint.md#regulator) |
| Assert | Data-quality validation step | It is a module type that runs one or more rules (schema match, null rate, freshness, row counts, custom checks) against a DataFrame and can abort, warn, quarantine, or defer to the healing agent on failure. | [Assert](specs/02-blueprint.md#assert) |
| Handoff | Cross-engine transfer (materialize-and-reread boundary) | It is a compiler-inserted module that carries data across an engine boundary in one Blueprint, by writing parquet on the source engine and reading it back on the target engine. | [The synthetic Handoff module](specs/08-polyglot.md#the-synthetic-handoff-module-235) |
| Manifest | Compiled execution plan | It is the compiled, fully-resolved JSON form of a Blueprint after all context substitution and Arcade expansion, and it is what the executor actually runs. | [Why a Manifest?](specs/01-overview.md#why-a-manifest-why-not-run-the-yaml-directly) |
| Surveyor | Runtime supervisor (health monitor and retry manager) | It is the process that watches a pipeline run, evaluates health signals, applies retry policy, and triggers the LLM healing loop on failure. | [System architecture](specs/01-overview.md#31-high-level-overview) |
| Gate | Validation checkpoint (patch review pipeline) | It is one of the numbered checks (guardrails, lineage, sandbox, resolvability) that a proposed patch must pass before it is written to the Blueprint. | [Gate](specs/06-healing.md#6-gate) |
| PatchSpec | No direct industry equivalent (closest: a structured diff / change-request document) | It is the JSON document that lists the typed operations a patch applies to a Blueprint, produced by the LLM agent or written by hand. | [Patch grammar](specs/06-healing.md#85-patch-grammar) |
| heal loop | Auto-remediation loop | It is the sequence a pipeline failure goes through: capture, pending check, prune, generate, reprompt, gate, then confirm and write, ending either in an applied patch or a deferral to a human. | [The healing flow](specs/06-healing.md#82-the-healing-flow) |
| cascade | Model fallback chain | It is a configured list of LLM tiers tried in order, escalating to the next tier when one gets stuck, runs out of attempts, or defers. | [Design philosophy](specs/06-healing.md#81-design-philosophy) |

## Corrections to common assumptions

A few terms invite an industry analogue that does not quite hold once you
read the spec closely:

- **Funnel is not a join.** It merges DataFrames by union (`union_all`,
  `union`), aligned coalesce, or positional zip, never by a join key. A
  join is a `Channel` operation (`op: join`), a separate module type.
- **Regulator is not a circuit breaker.** A circuit breaker trips after a
  run of failures and later half-opens to test recovery. A Regulator reads
  one wired `passed`/not-passed signal and acts once; it has no failure
  count, no cooldown, and no half-open retry state.
- **Surveyor is more than an observability writer.** It does write runtime
  signals, but its defining role is process supervision: it evaluates
  health, drives the retry policy, and triggers the self-healing loop.
- **Arcade sits closer to an inlined subroutine than a task group.** An
  orchestrator "task group" is usually a UI-level grouping with no identity
  change to its members. An Arcade is expanded at compile time into the
  parent's own module list, with every child module id namespaced
  (`{arcade_id}__{child_id}`), which is closer to macro expansion than to a
  visual grouping.
- **PatchSpec has no clean off-the-shelf equivalent.** It is closest in
  spirit to a structured diff format like JSON Patch, but it is a
  domain-specific, schema-checked operation list scoped to Blueprint edits,
  not a generic document-patch format.
