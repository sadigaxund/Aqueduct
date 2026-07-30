# Aqueduct Roadmap

**Deferred features and future plans.** These are intentionally not part of the current specification, they are staged for future revision to avoid premature design decisions.

---

## Streaming (Spark Structured Streaming)

Architecturally compatible with Aqueduct's Module model: a streaming Ingress and streaming Egress bookend the same Channel chain. The Probe model requires adaptation since SparkListener signals differ for continuous streams (microbatch vs. continuous processing). Regulator gates require a re-evaluation model for streaming contexts.

**Status:** Deferred. No active work.

---

## Resume-from semantics (partial pipeline resume)

When a patch is applied, Aqueduct currently re-runs the entire pipeline from the beginning. This is less efficient than partial resume but is 100% reliable and trivial to implement.

**Why deferred:** Partial resume requires the Executor to cache intermediate DataFrames across a JVM session boundary, maintain a mapping of which Modules completed, and handle invalidation when a patch modifies an upstream Module. The correctness surface area is large.

**Middle-ground path:** If the Blueprint explicitly uses `df.checkpoint()` at a Module boundary, the engine may restart from that checkpoint location after a patch. This is opt-in and driven by the Blueprint author, Aqueduct does not insert checkpoints automatically.

**Future full implementation:**
- Any Module whose output was consumed by a completed Egress is not re-executed. Its output is considered finalised.
- Modules whose output was consumed only by other Modules (not yet written) are re-executed if upstream of the failing Module.
- The Planner recomputes the partial-DAG topological order.
- Invalidation: if a patch modifies a Module's config, all downstream Modules are invalidated and re-executed, even if they had completed successfully.

**Status:** Deferred. Current behaviour (full re-run) is correct and sufficient for most use cases.

---

## Remote-filesystem checkpoint root (s3a / hdfs)

Checkpoints (`checkpoint: true` on a module or manifest) are written to a local-filesystem path, by default derived from the observability store directory (`.aqueduct/observability/<blueprint_id>/checkpoints/<run_id>/`), or overridden with the `checkpoint_root:` engine-config key (2.8, LOCAL PATHS ONLY, see specs §10.4.2). On a distributed cluster where workers don't share the driver's filesystem (Docker-based Spark Standalone, k8s), the write fails per-module and degrades to a `runtime_checkpoint_write_failed` warning, the run succeeds but the recompute-avoidance benefit is lost.

**What's still deferred:** `checkpoint_root` accepting a *remote* URI (`s3a://`, `hdfs://`, ...) instead of a local path, it is currently rejected at config-load. The Parquet read/write already accepts `s3a://` URIs natively; the remaining work is the surrounding bookkeeping. Six pathlib-only call sites in `executor/spark/executor.py` (`mkdir`, the `_aq_done` done-marker `write_text`, the `_manifest_hash` write/read, and three `exists()` checks in the `--resume` reload loop) need Hadoop-FS-API equivalents (py4j, same pattern as `metrics.py`), best wrapped in a small local/remote checkpoint-IO abstraction.

**Why deferred:** the `_aq_done` marker semantics on object stores (no atomic rename, eventual consistency) is a real resume-correctness design question, and the degraded mode is safe, only an optimisation is lost, with a volume-mount workaround for Docker setups (or the new local `checkpoint_root` override for shared-mount deployments).

**Status:** Deferred.

---

## MCP: write-capable tools

**Shipped (no longer roadmap):** the read-only diagnostics ToolRegistry
(`aqueduct/tools/`, specs.md §8.10) and its stdio MCP transport,
`aqueduct mcp serve` (optional `[mcp]` extra) exposes `list_runs`,
`run_detail`, `lineage`, `patch_list`, `patch_show`, `probe_signals`,
`doctor`, and `blueprint_history` to any MCP client, redacted and
structurally read-only.

What remains deferred is the **write-capable** tool set, a materially
different trust level (an MCP client that can mutate pipelines, not just
inspect them), needing its own approval-gate design before any code:

| Tool name | Description |
|---|---|
| `patch_blueprint` | Accepts a run_id and optional module scope. Assembles the FailureContext, invokes the LLM loop, and returns the applied PatchSpec. |
| `run_pipeline` | Submits a Blueprint for execution and streams RunRecord status events. |

When write tools ship, `approval` in the agent config applies to the tool caller, `auto` approves patches immediately, `human` holds them for the user to confirm in the MCP client UI. Non-stdio transports (network/SSE) are similarly deferred, the current server is deliberately local-only.

**Status:** Read-only registry + stdio server shipped. Write tools + network transport: architectural design only, no code.

---

## Additional remediation domains

specs.md §8.11 frames self-healing as operating in explicit "remediation
domains," with the pipeline-definition domain (PatchSpec ops on the
Blueprint) as the only one that exists today. Candidate future domains, each
requiring its own typed operation grammar and validation gates before any
code is written:

- **Engine-config domain**: proposing changes to `aqueduct.yml` (retry
  policy, resource sizing) rather than the Blueprint itself.
- **Infra domain**: cluster/deployment-level remediation (e.g. bumping
  executor memory on repeated OOM), well outside the current patch grammar's
  scope and reach.

**Status:** Conceptual. No grammar, no gates, no code, the domain framing
exists so a future domain slots into the same shape instead of a one-off
extension of PatchSpec.

---

## ML inference as a built-in Channel op

A Channel module wrapping a model inference call (MLflow, SageMaker, Vertex AI endpoint) is architecturally straightforward. Feature store reads as Ingress modules are natural.

**Recommendation:** ML inference as a built-in Channel op type (`op: infer`) for v1.1 or later. Training orchestration is out of scope, use MLflow Pipelines / Vertex AI Pipelines / Kubeflow for that.

**Status:** Deferred.

---

## Type hub: long-tail constructors

The hub type vocabulary (`aqueduct/typehub.py`, see `docs/specs.md` §9) deliberately ships a subset of Arrow's full taxonomy. `duration(unit)` — measured against real engines here as an "interval" candidate — shipped in 2.38 as the simpler of two designs once sketched in this section: integer-backed, always rendering as a plain `bigint`/`BIGINT` on both engines (in a Channel cast, an Ingress `schema_hint`, everywhere), never a native `INTERVAL` type — matching every other hub constructor's one-spelling/one-`render_type`-mapping shape (`aqueduct/typehub.py`, `docs/specs.md` §9.1). The measurement that motivated it: Spark writes `interval day to second` to Parquet as an INT64 microsecond count (lossless); DuckDB writes the 12-byte interval physical type (millisecond, truncating); a cross-engine read of Spark's file yields a bare INT64; an explicit integer round-trips exactly on both — so the only portable, lossless representation is integer-backed. The remaining constructors below are not in the hub yet:

- **Unsigned integers** (`uint8`/`uint16`/`uint32`/`uint64`). Neither shipped engine's native type system distinguishes signed from unsigned integers the way Arrow does; adding these constructors without a real semantic difference on either engine would be vocabulary for its own sake.
- **Unions.** No authoring surface in the current Blueprint grammar needs a tagged/dense union column; deferred until one does.
- **Dictionary / run-end encoding.** These are storage encodings, not value types — orthogonal to what the hub vocabulary states about a column's meaning. Revisit if a Parquet/Arrow-native encoding hint becomes a real authoring need.
- **`string_view`.** Arrow's newer variable-length string representation; the hub's plain `string` already covers the value semantics both engines need, and neither engine's own DDL distinguishes the two.
- **Calendar month/year intervals (`interval_ym`).** Genuinely different value semantics from `duration(unit)` (calendar arithmetic vs. a fixed tick count); not needed by any current authoring surface.
- **In-engine native interval for `duration` arithmetic.** `duration(unit)` always renders as a plain integer — a Blueprint author who needs real calendar/timestamp arithmetic on a duration value (not just storage/comparison) still has to reach for the `<engine>:` native namespace (`spark:interval day to second`, `duckdb:INTERVAL`), which is honest but not portable. The design sketched and NOT built for `duration(unit)` above — a context-aware `render_type` giving a native interval in-engine for compute and an int64 only across a boundary — would close this gap, but only on concrete demand: it is a real seam change, since the cross-engine handoff boundary is a raw, type-oblivious Parquet passthrough by design (§10.9), and this would be the first constructor to need a wrap/unwrap step there.

**Status:** Deferred. Native namespace escape hatches (`<engine>:<spelling>`) cover every constructor above today.

---

## Iceberg / Hudi table formats

Ingress and Egress currently support Parquet, Delta Lake, CSV, JSON, and JDBC. Apache Iceberg and Apache Hudi are planned as additional table formats, both fit the existing `format:` config surface without schema changes.

**Status:** Planned, not started. Tracked in the compatibility matrix as "planned".

---

## Persist `model_cascade_position` to `heal_attempts`

The multi-model cascade tags every in-memory `AttemptRecord` with its 0-based tier index, but the `heal_attempts` table does not yet have a column for it, per-tier heal analytics (e.g. "which tier actually solves things") currently require correlating `healing_outcomes.model` instead. Adding the column needs a DDL migration for existing observability stores.

**Status:** Deferred. Small, self-contained.

---

## Flink execution engine

The engine portfolio is Spark and DuckDB. `deployment.engine` is validated against the engines actually registered through the `aqueduct.engines` entry-point group (`aqueduct/executor/capabilities.py`), so `engine: flink` fails at config-load with a `ConfigError` listing the registered engines rather than a special-cased stub.

**Status:** Out of scope. A Flink engine would be a separate project taken up on demand, not a planned addition.

---

## Multi-pipeline orchestration (native)

Aqueduct currently runs one pipeline per invocation. Cross-pipeline dependencies (pipeline A must complete before pipeline B starts) are handled externally via Depot watermarks and standard orchestrators (Airflow, Prefect) triggering `aqueduct run` commands.

A native Aqueduct workflow layer (a Blueprint of Blueprints) is a potential future feature.

**Status:** Deferred. The external-orchestrator pattern works well for current use cases.

---

## `aqueduct run` CLI integration for polyglot Blueprints

**Status: done (2.37).** `aqueduct/cli/run.py`'s healing loop now routes a >1-island Manifest through `run_polyglot()` (see specs.md §10.9's "Wired into `aqueduct run`"), including the run header naming every engine involved, per-module engine tags in the transcript and `report --format json`, and first-class Handoff-step rendering (bytes transferred + duration). `--from`/`--to` refuse a polyglot Manifest outright rather than attempting a partial cross-island selection — that remains future work if a real need for it shows up. Session reuse across heal iterations (today each retry rebuilds every island's session fresh) is a recorded, not-yet-built optimization, same framing as `run_polyglot()`'s existing same-engine-recurrence choice.

Still deferred, tracked separately (not part of the CLI-wiring pass above): a fan-shape conformance matrix for constructs (Junction/Funnel modes) whose behavior is validated per-engine but not yet cross-checked when a fan spans a boundary, a `timezone:` config key for a cross-engine `timestamp_ntz` boundary, and a doctor free-space check at `handoff.root`.