# Aqueduct Roadmap

**Deferred features and future plans.** These are intentionally not part of the current specification — they are staged for future revision to avoid premature design decisions.

---

## Streaming (Spark Structured Streaming)

Architecturally compatible with Aqueduct's Module model — a streaming Ingress and streaming Egress bookend the same Channel chain. The Probe model requires adaptation since SparkListener signals differ for continuous streams (microbatch vs. continuous processing). Regulator gates require a re-evaluation model for streaming contexts.

**Status:** Deferred. No active work.

---

## Resume-From Semantics (Partial Pipeline Resume)

When a patch is applied, Aqueduct currently re-runs the entire pipeline from the beginning. This is less efficient than partial resume but is 100% reliable and trivial to implement.

**Why deferred:** Partial resume requires the Executor to cache intermediate DataFrames across a JVM session boundary, maintain a mapping of which Modules completed, and handle invalidation when a patch modifies an upstream Module. The correctness surface area is large.

**Middle-ground path:** If the Blueprint explicitly uses `df.checkpoint()` at a Module boundary, the engine may restart from that checkpoint location after a patch. This is opt-in and driven by the Blueprint author — Aqueduct does not insert checkpoints automatically.

**Future full implementation:**
- Any Module whose output was consumed by a completed Egress is not re-executed. Its output is considered finalised.
- Modules whose output was consumed only by other Modules (not yet written) are re-executed if upstream of the failing Module.
- The Planner recomputes the partial-DAG topological order.
- Invalidation: if a patch modifies a Module's config, all downstream Modules are invalidated and re-executed, even if they had completed successfully.

**Status:** Deferred. Current behaviour (full re-run) is correct and sufficient for most use cases.

---

## Remote-Filesystem Checkpoint Root (s3a / hdfs)

Checkpoints (`checkpoint: true` on a module or manifest) are written to a local-filesystem path — by default derived from the observability store directory (`.aqueduct/observability/<blueprint_id>/checkpoints/<run_id>/`), or overridden with the `checkpoint_root:` engine-config key (2.8, LOCAL PATHS ONLY — see specs §10.4.2). On a distributed cluster where workers don't share the driver's filesystem (Docker-based Spark Standalone, k8s), the write fails per-module and degrades to a `runtime_checkpoint_write_failed` warning — the run succeeds but the recompute-avoidance benefit is lost.

**What's still deferred:** `checkpoint_root` accepting a *remote* URI (`s3a://`, `hdfs://`, ...) instead of a local path — it is currently rejected at config-load. The Parquet read/write already accepts `s3a://` URIs natively; the remaining work is the surrounding bookkeeping. Six pathlib-only call sites in `executor/spark/executor.py` (`mkdir`, the `_aq_done` done-marker `write_text`, the `_manifest_hash` write/read, and three `exists()` checks in the `--resume` reload loop) need Hadoop-FS-API equivalents (py4j, same pattern as `metrics.py`), best wrapped in a small local/remote checkpoint-IO abstraction.

**Why deferred:** the `_aq_done` marker semantics on object stores (no atomic rename, eventual consistency) is a real resume-correctness design question, and the degraded mode is safe — only an optimisation is lost, with a volume-mount workaround for Docker setups (or the new local `checkpoint_root` override for shared-mount deployments).

**Status:** Deferred.

---

## MCP (Model Context Protocol) Readiness

Aqueduct's LLM loop is architected to be exposed as an **MCP Server**. This will allow any MCP-compatible agent (Claude Desktop, Cursor, etc.) to discover and invoke Aqueduct capabilities directly as tools.

Candidate MCP tools:

| Tool name | Description |
|---|---|
| `patch_blueprint` | Accepts a run_id and optional module scope. Assembles the FailureContext, invokes the LLM loop, and returns the applied PatchSpec. |
| `get_lineage` | Accepts a pipeline_id, module_id, and column name. Returns the upstream and downstream ColumnLineageGraph for that column. |
| `get_flow_report` | Returns the Flow Report for a given run_id in structured JSON. |
| `run_pipeline` | Submits a Blueprint for execution and streams RunRecord status events. |

When Aqueduct operates as an MCP server, `approval` in the agent config applies to the tool caller — `auto` approves patches immediately, `human` holds them for the user to confirm in the MCP client UI.

**Status:** Architectural design only. No code.

---

## ML Inference as a Built-in Channel Op

A Channel module wrapping a model inference call (MLflow, SageMaker, Vertex AI endpoint) is architecturally straightforward. Feature store reads as Ingress modules are natural.

**Recommendation:** ML inference as a built-in Channel op type (`op: infer`) for v1.1 or later. Training orchestration is out of scope — use MLflow Pipelines / Vertex AI Pipelines / Kubeflow for that.

**Status:** Deferred.

---

## Iceberg / Hudi Table Formats

Ingress and Egress currently support Parquet, Delta Lake, CSV, JSON, and JDBC. Apache Iceberg and Apache Hudi are planned as additional table formats — both fit the existing `format:` config surface without schema changes.

**Status:** Planned, not started. Tracked in the compatibility matrix as "planned".

---

## Persist `model_cascade_position` to `heal_attempts`

The multi-model cascade tags every in-memory `AttemptRecord` with its 0-based tier index, but the `heal_attempts` table does not yet have a column for it — per-tier heal analytics (e.g. "which tier actually solves things") currently require correlating `healing_outcomes.model` instead. Adding the column needs a DDL migration for existing observability stores.

**Status:** Deferred. Small, self-contained.

---

## Flink Execution Engine

The `executor/__init__.py` factory has a `flink` stub that raises `NotImplementedError`. Flink support is not actively planned — the engine is designed for Spark batch pipelines.

**Status:** Deferred indefinitely.

---

## Multi-pipeline Orchestration (Native)

Aqueduct currently runs one pipeline per invocation. Cross-pipeline dependencies (pipeline A must complete before pipeline B starts) are handled externally via Depot watermarks and standard orchestrators (Airflow, Prefect) triggering `aqueduct run` commands.

A native Aqueduct workflow layer (a Blueprint of Blueprints) is a potential future feature.

**Status:** Deferred. The external-orchestrator pattern works well for current use cases.