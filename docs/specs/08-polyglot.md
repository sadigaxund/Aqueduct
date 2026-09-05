# 11. Engine Scope and Boundaries

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

### The synthetic Handoff module

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

**Type fidelity across the boundary.** The write/read is a raw DataFrame/relation passthrough: no hub type resolution or `render_type` mapping runs on the boundary, so type fidelity across it depends on each engine's own Parquet reader/writer agreeing on file-level logical-type annotations. Every hub constructor round-trips faithfully over a real Spark↔DuckDB Parquet handoff, including `timestamp_tz`: Aqueduct's Spark session factory (`aqueduct/executor/spark/session.py::make_spark_session`) sets `spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS` at session creation, as Aqueduct's own default, in place of Spark's own default (`INT96`, a legacy Hive-interop encoding with no Parquet logical-type annotation distinguishing an instant-aware timestamp from a naive one). This is set once at session creation, never toggled around an individual write, so it can never depend on thread timing under `--parallel` (independent components share one SparkSession). A user's own `engine.spark.conf` value for `spark.sql.parquet.outputTimestampType` always wins: the factory only applies its default when the key is absent from the resolved config. `timestamp_ntz` was never affected by this (Spark always writes it with a modern, correctly-annotated logical type). The reverse direction is unaffected in both variants: DuckDB's own Parquet writer always annotates `TIMESTAMPTZ`/`TIMESTAMP` correctly, and Spark reads both back as the matching hub type. `duration(unit)` round-trips faithfully by construction rather than by a session-factory fix: it renders as a plain `BIGINT`/`bigint` on both engines (§9.1's "Why `duration` is integer-backed"), and a signed 64-bit integer carries no logical-type ambiguity a Parquet reader/writer could disagree about; verified both directions over a real Spark↔DuckDB handoff.

**Compile-time visibility.** Every insertion emits a suppressible warning, rule id `cross_engine_handoff_io`, naming the boundary (`Cross-engine handoff 'extract__handoff__agg': 'extract' (spark) -> 'agg' (duckdb)...`) through the same `aqueduct.warnings.emit` machinery as every other compiler warning; the extra I/O a split introduces is a real cost, visible before the run rather than discovered mid-run.

**Capability-gate interaction.** `module.type.Handoff` is a governed capability leaf like any other `ModuleType` member, declared `supported` on both shipped engines (real engine-native transport exists and is tested: see below). The verdict is never actually consulted at the compile gate: a handoff module's id is never a member of any island's `module_ids` (islands are derived from the pre-insertion graph) so the per-island gate's per-island `manifest.modules` filter excludes it from every island's check by construction, the same way a disabled module is already excluded. That invariant is deliberately preserved rather than folding a handoff module into island membership to make it "execute": doing so would route it back through the per-island gate for no reason, since real execution goes through the orchestrator below instead (`tests/test_compiler/test_islands.py::test_handoff_modules_never_reach_the_capability_gate` enforces this).

### Runtime execution of a Handoff module

Compile-time synthesis (above) only builds the graph shape; a polyglot Manifest is actually run by `aqueduct.executor.orchestrator.run_polyglot()`, a coordinator layered ABOVE the single-engine `ExecutorProtocol.execute()` calls every engine already implements. A single-engine Manifest (including one compiled for a single-engine Blueprint, which always has exactly one island) runs through this same function unchanged: it is a strict superset of the single-engine path, not a special case of it.

**Per-island session lifecycle.** Sessions open LAZILY, one per island, in the topological order of the island dependency graph (an island depends on every island whose Handoff output it reads): a boundary edge is a dependency; disjoint different-engine components have none, so both still run, in `manifest.islands` order. A session closes immediately after its island's last module, via `ExecutorProtocol.close_session`. This is a deliberate v1 choice, not yet optimized: an engine's session closes even if that SAME engine recurs later in the run (`spark -> duckdb -> spark` opens two separate Spark sessions), rather than being kept alive across the gap. One `run_id` covers the whole `aqueduct run` invocation regardless of how many islands/sessions it opens: `run_id` was never an engine's own session/application id, and this does not change that.

**Transport, realized.** Each boundary's Handoff module dispatches on which side of the boundary the current island sits: the WRITE side (this island produced `from_module`) materializes the upstream DataFrame/relation to the resolved spill URI (`df.write.parquet` on Spark, `COPY ... TO ... (FORMAT PARQUET)` on DuckDB); the READ side reads it back (`spark.read.parquet`, DuckDB `read_parquet` over the directory's `*.parquet` files). A single sub-Manifest given to one island's `execute()` call never contains both halves of the same boundary's edges (only the one relevant to that island) so a Handoff module dispatches unambiguously by which edge is present.

**Spill lifecycle.** Directory layout is exactly `<root>/<manifest_hash>/<run_id>/<edge_id>/` (§10.4.3). Deleted when the whole run succeeds; kept when it fails and `handoff.keep_on_failure` is true (the default); the resume story: passing `resume_run_id` to `run_polyglot()` makes an island whose OUTGOING handoff spill already exists under that prior run skip re-execution entirely (its modules report `status="skipped"`) and downstream islands read the prior run's spill instead of a fresh one. This is a MANUAL `aqueduct run --resume <run_id>` after a plain failure, with no Blueprint edit in between: the Manifest hash, and therefore the spill's directory, is unchanged from the failed run. A heal-triggered retry never reaches this path at all: `aqueduct/cli/run.py` passes `resume_run_id` only `if patch_count == 0`, so once a patch has been applied the retry carries no resume id, and even if it did, the heal already changed `manifest_hash` (see below) so the prior spill directory would not be the one this run resolves to. A run's own cleanup targets its OWN `run_id` directory AND, when this run actually resumed from a prior one and then SUCCEEDED, the resumed-FROM `run_id` directory as well: that spill was kept for exactly the rerun that has now consumed it. A FAILED resume keeps it (still resumable). An orphan sweep runs at the START of `run_polyglot()`, before the current run's own spill exists on disk. Because a heal changes the compiled Manifest and therefore `manifest_hash`, consecutive runs of the same Blueprint across a patch write under DIFFERENT hash directories: the sweep scans the ENTIRE `handoff.root`, across every hash directory, not only the current run's, or a prior hash directory's kept-failure spill would never be revisited by anything and would accumulate forever, one heal at a time. It reclaims any `run_id` directory (under any hash directory) whose `run_records` status is terminal and not a still-protected kept failure (a successful run whose own cleanup never ran, a failed run when `keep_on_failure` is false, a failed run that a LATER succeeded run of the same blueprint has already resolved (see §10.4.3) or a `run_id` with no `run_records` row at all), and never touches a non-terminal (still-running or crashed-without-a-terminal-status) run's spill: the decision is keyed on `run_records` alone, so it is correct even when several Blueprints share one `handoff.root`. A hash directory left empty once every run underneath it has been swept is reclaimed too.

**The two IO stacks, and the loud-not-silent rule.** The ENGINES write/read spill natively (no `fsspec`: Spark and DuckDB both already speak local and remote URIs on their own, which is what keeps this cluster-ready with no new backend abstraction). Aqueduct's OWN cleanup (delete-on-success, keep-on-failure, orphan sweep) uses `fsspec` for a remote `handoff.root`, because unlike an engine's writer, cleanup has no engine-native way to list/delete an arbitrary URI scheme. On a remote root with `fsspec` NOT installed, engine writes keep succeeding while cleanup silently could not act: `run_polyglot()` makes this loud instead of silent: it emits a suppressible warning (rule id `handoff_cleanup_unavailable`) at the start of every run whenever the root is remote and `fsspec` is absent, naming the exact condition, rather than letting spill accumulate behind a debug log line.

**Observability.** A Handoff module gets a `module_metrics` row like any other module (`bytes_written` on the write side, `bytes_read` on the read side, plus `duration_ms`) measured from the spill directory's on-disk size, engine-agnostically. `Surveyor.record()` accepts an explicit `engine` override (falling back to its own construction-time engine when omitted) so a polyglot run's `FailureContext.engine` and structured error extraction (`ExecutorProtocol.extract_error`) reflect the ISLAND that actually failed, not the run's nominal deployment engine. The cross-engine heal-patch provenance gate (§10.9, `cross_engine_heal`) is checked once per DISTINCT island engine present in the compile, rather than only against the single `deployment.engine` default, for the same reason.

**Wired into `aqueduct run`.** `aqueduct/cli/run.py`'s healing loop routes a Manifest with more than one island through `run_polyglot(..., record_result=False)` in place of the single-engine `execute()` call; a single-engine Manifest (`len(manifest.islands) <= 1`) takes the exact same path it always has, unchanged. `record_result=False` lets the CLI call `surveyor.record(result, exc=..., engine=result.failed_engine)` itself, so a failed run is attributed to the ISLAND that actually failed rather than `deployment.engine`: the healing prompt (`generate_agent_patch`/`generate_cascade_patch`), and the `patch_index`/`healed_by` provenance record both key off that same failing-island engine. Lifecycle hooks pass `session=None` for a polyglot run (no single live session survives to hook time: every island's is already closed), which falls through to hooks' existing subprocess path; a `blueprint:` hook entry with `in_process: true` gets a `[hook_in_process_unavailable]` warning naming why (§4.2). Module-range selection (`--from`/`--to`) refuses a polyglot Manifest outright (`CONFIG_ERROR`) rather than silently running the whole graph: which island(s) a range spans is real cross-island work not attempted here. Rendering: the run header names every engine involved (not the single nominal default), each module's transcript line carries its own resolved engine, and a Handoff module's result renders as a first-class step (`⇄ from → to (engineA→engineB)`, bytes transferred, duration) rather than an anonymous module id. `report --format json` gains a top-level `engines` list and a per-module `engine` field (both persisted by `Surveyor.record()`, present for single-engine runs too). One known v1 cost, not yet optimized: each heal iteration calls `run_polyglot()` fresh, so every island's session is rebuilt on a retry rather than reused across iterations the way the single-engine path reuses its one session; the same "not yet optimized" framing as same-engine session reuse WITHIN a run, above.

