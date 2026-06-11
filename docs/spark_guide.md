# Aqueduct Spark Guide

Single reference for Spark behavior in Aqueduct — covers both blueprint authoring
(compiler warnings, cost model) and internal implementation rules (contributor rules,
pitfalls, transformation reference).

---

## For Blueprint Authors

### Compiler Warnings

Every warning links to an anchor here. Warnings are informational unless marked **[ERROR]**.

Each warning prints as `AQ-WARN [<rule_id>] …`; silence one with
`aqueduct --suppress-warning <rule_id> <command>` (repeatable, `'*'` = all)
or `warnings.suppress:` in `aqueduct.yml`. Current rule ids:

| Rule id | Flags | Detail |
|---|---|---|
| `perf_probe_sample_full_scan` | Probe sample-based signal — `df.sample()` is a full dataset scan | [probe-sample-cost](#probe-sample-cost) |
| `perf_incremental_watermark_scan` | `materialize: incremental` re-scans output for `MAX(watermark_column)` | [incremental-watermark-scan](#incremental-watermark-scan) |
| `perf_python_udf_row_at_a_time` | Python UDF bypasses Arrow/vectorized execution | [python-udf-performance](#python-udf-performance) |
| `perf_delta_append_no_partition` | `mode: append` without `partition_by`/repartition accumulates small files | [delivery-semantics-append-retry](#delivery-semantics-append-retry) |
| `perf_multi_consumer_no_cache` | Multi-consumer Channel without a Checkpoint re-evaluates the DAG per branch | [caching-strategy](#caching-strategy) |
| `perf_hadoop_fs_in_options` | Hadoop FS credentials in `options:` don't reach HadoopConfiguration — use `spark_config:` | — |
| `count_col_likely_count_star` | `COUNT(col)` silently skips NULLs — likely meant `COUNT(*)` | — |
| `file_format_no_repartition` | parquet/json/csv Egress without repartition/`partition_by` → one file per task | — |
| `jdbc_missing_partition` | JDBC Ingress without `partitionColumn`/bounds reads through one connection | [jdbc-ingress-parallelism](#jdbc-ingress-parallelism) |
| `kafka_checkpoint_stale` | Checkpointing a Kafka-fed Channel freezes a stale snapshot for other consumers | — |
| `nondeterministic_fanout` | `rand()`/`uuid()`/`current_timestamp()` diverges across consumer branches | — |
| `jar_availability` | Declared format (jdbc/kafka/delta/…) has no matching JAR on the session classpath (session-startup check) | — |

#### `probe-sample-cost`

**Triggered when:** A Probe has a signal of type `null_rates`, `row_count_estimate`
(sample method), `value_distribution`, or `distinct_count`.

`df.sample(fraction)` is a **row-level random filter**, not a partition prune.
Every row in every partition is read before the random filter is applied — Spark
cannot skip partitions because it doesn't know which rows pass until it reads them.

A `null_rates` probe with `fraction: 0.01` on a 10 TB dataset reads all 10 TB.
Only the output is 1% of rows; the I/O cost is 100%.

**Zero-cost alternatives:**

| Signal | Zero-cost method |
|---|---|
| `row_count_estimate` | `method: spark_listener` — reads from `observability.db`, no Spark action |
| `schema_snapshot` | Always zero-action |
| `partition_stats` | `df.rdd.getNumPartitions()` — zero action |

**To silence:** If you accept the cost, add `block_full_actions: false` explicitly in
the Probe config to document intent.

**Future:** File-level sampling (random subset of Parquet/Delta files via file
statistics) would make these signals genuinely cheap. Not yet implemented.

---

#### `incremental-watermark-scan`

**Triggered when:** A Channel has `materialize: incremental` and no `Checkpoint`
upstream.

After each incremental run Aqueduct advances the watermark via:

```python
df.agg(F.max(watermark_column)).collect()
```

This is an extra Spark action. If the DataFrame is not cached, Spark re-executes
the full DAG — a second scan of the output data. On large outputs (hundreds of GB)
this doubles the incremental step's reading cost.

**Mitigations:**
1. Add a `Checkpoint` upstream — materializes the DataFrame so subsequent actions
   read from checkpoint, not recompute the DAG.
2. Cache the Channel output upstream of the egress.
3. Accept the cost for small/medium datasets.

**Future:** Track watermark max as a side-effect of the write phase to eliminate
the extra action entirely.

---

#### `python-udf-performance`

**Triggered when:** A UDF in `udf_registry` has `lang: python` (the default).

Python UDFs execute **row-at-a-time** in the Python interpreter and bypass
Arrow-optimized (vectorized) execution entirely:

- Each row crosses the JVM↔Python serialization boundary
- No Arrow batch transfer, no SIMD/columnar processing
- For billions of rows: **10–100× slower** than a native Spark SQL expression

**The spillway is unaffected.** Spillway routing uses SQL `filter()` — fully
vectorized. The concern is the UDF body, not the error-routing mechanism.

**Alternatives:**

| Option | When to use |
|---|---|
| Native Spark SQL (`try_cast`, `try_divide`, `coalesce`, etc.) | Preferred — vectorized, zero overhead |
| `pandas_udf` with Arrow | Complex Python logic; batched via Arrow, much faster |
| `lang: java` | Maximum performance required |

---

#### `delivery-semantics-append-retry`

**Triggered when:** `retry_policy.max_attempts > 1` and any Egress uses `mode: append`.

Retries on an `append`-mode Egress produce duplicate rows — the failed run already
wrote some data before the failure. `mode: overwrite` is idempotent; `mode: append`
is not.

**Fix:** Use `mode: overwrite` for retried pipelines, or `max_attempts: 1` with
orchestrator-level retry handling.

---

### Probe Signal Cost Model

| Signal / Operation | I/O Cost | Spark Actions | Blocked by `block_full_actions` |
|---|---|---|---|
| `schema_snapshot` | Zero | 0 | No |
| `partition_stats` | Zero | 0 | No |
| `row_count_estimate` (spark_listener) | Zero | 0 | No |
| `sample_rows` | First partition(s) only | 1 (`limit.collect`) | No |
| `row_count_estimate` (sample) | **Full scan** | 1 | Yes |
| `null_rates` | **Full scan** | 2 | Yes |
| `value_distribution` | **Full scan** | 1 | Yes |
| `distinct_count` | **Full scan** | 1 | Yes |
| `data_freshness` (fraction=0 default) | **Full scan** | 1 | Yes |
| Incremental watermark `MAX()` | Full scan of output (if uncached) | 1 | No |
| Assert aggregate rules | Full scan | 1 (batched `agg`) | — |
| Assert `spillway_rate` | Full scan × 2 | 2 extra | — |

---

### SparkListener Row Estimates — Stage Fusion Caveat

The `row_count_estimate` signal with `method: spark_listener` queries `module_metrics`
in `observability.db` using the module's ID as the lookup key. This works correctly when each
logical module maps to its own Spark stage.

**Edge case — stage fusion:** Spark's Catalyst optimizer can fuse multiple logical
modules (Channel → Egress, or multiple narrow transforms) into a single physical stage.
When this happens, `recordsWritten` from the fused stage is attributed to one stage ID
and the per-module breakdown may not be available or may reflect the combined output
of multiple logical modules.

**Consequence:** The row estimate for a Channel that was fused with its downstream
Egress may show the Egress's written count, not the Channel's intermediate row count.
The estimate is still useful for capacity planning but should not be treated as exact
for per-module cost attribution.

**No fix required** — this is a Spark optimizer behavior. The caveat is noted here
so users understand why estimates occasionally differ from expected counts.

---

### Read and Write Mode Defaults

**Read mode** (controls behavior on malformed records):

| Mode | Behavior | Default? |
|---|---|---|
| `permissive` | Sets all fields to `null`; places corrupt raw string in `_corrupt_record` | **Yes** |
| `dropMalformed` | Silently drops the row | No |
| `failFast` | Raises immediately on first corrupt record | No |

**The permissive default is silent.** A bad CSV row becomes a null row in your DataFrame with no warning. Always use `mode: FAILFAST` in production Ingress configs or add an Assert rule checking for unexpected nulls after Ingress.

**Write mode** (controls behavior when destination already has data):

| Mode | Behavior | Default? |
|---|---|---|
| `errorIfExists` | Raises immediately if data exists at path | **Yes** |
| `overwrite` | Replaces all existing data | No |
| `append` | Adds to existing data | No |
| `ignore` | Skips write silently if data exists | No |

The `errorIfExists` default means re-running a pipeline without changing the output path will fail immediately. Aqueduct Egress config should always set `mode` explicitly — never rely on defaults.

---

### Parquet and JSON Output: One File Per Partition

When writing Parquet, JSON, CSV, or ORC, Spark writes **one file per partition** into a folder at the output path — not a single file. A pipeline writing 200 partitions produces 200 part files in a directory.

**Implications:**
- `coalesce(1)` before Egress produces a single file but forces all data through one task — avoid on large datasets
- Too many partitions = too many small files = slow downstream reads (S3 list overhead, Parquet metadata overhead)
- Default shuffle partitions (`spark.sql.shuffle.partitions: 200`) after a wide transform often produces 200 tiny files

**Rule of thumb:** target 128–512 MB per output file. Use `coalesce` (no shuffle) when reducing from many balanced partitions; use `repartition` when redistribution is needed.

**Parquet version incompatibility:** Parquet files written by older Spark versions (< 2.x) may be unreadable by newer Spark or vice versa. If reading files written by an older cluster, set `spark.sql.parquet.enableVectorizedReader: "false"` as a fallback.

---

### JDBC Ingress: Parallelism Requires Explicit Partition Config {#jdbc-ingress-parallelism}

`numPartitions` alone on a JDBC Ingress does **not** create multiple parallel read tasks. Spark opens a single connection and reads the full table unless you also provide:

```yaml
config:
  format: jdbc
  url: "jdbc:postgresql://host/db"
  dbtable: "orders"
  numPartitions: 10
  partitionColumn: "order_id"   # must be numeric
  lowerBound: 1
  upperBound: 10000000
```

Without `partitionColumn` + `lowerBound` + `upperBound`, the result is 1 partition regardless of `numPartitions`. With them, Spark opens `numPartitions` concurrent connections, each fetching a range — **all rows are returned** (lowerBound/upperBound determine stride, not a filter).

**Predicate pushdown via raw SQL:** When Spark cannot translate a DataFrame operation into pushdown SQL (complex UDFs, unsupported functions), pass a full SQL query as `dbtable` to force the database to do the work:

```yaml
config:
  format: jdbc
  dbtable: "(SELECT id, amount FROM orders WHERE status = 'completed' AND date > '2024-01-01') AS q"
```

The database runs the query; Spark reads only the result set. Use this when:
- The filter expression uses database-specific functions
- The table is huge and only a small slice is needed
- Spark's auto-pushdown is not confirmed in `df.explain()`

**Warning:** Setting `numPartitions` too high overwhelms the database with concurrent connections — each partition opens a separate JDBC connection. Match `numPartitions` to the database's max connection pool size, not the Spark cluster's core count.

**Non-disjoint predicates produce duplicate rows.** If using the `predicates` list API, ensure predicates are mutually exclusive; overlapping predicates cause the same row to appear in multiple partitions.

---

### Planned Future Checks

Not yet implemented — planned for future compiler passes:

- **Partition count advisory:** Warn when an Ingress path has many small files;
  suggest `coalesce` or `repartition`.
- **Skew detection:** Compare partition sizes from SparkListener metrics after a run;
  warn when max/median ratio exceeds threshold.
- **Egress format advisory:** Warn when writing to CSV without explicit schema —
  downstream readers trigger schema inference (full scan).
- **High-fanout Junction:** Warn when a Junction has many inputs without a broadcast
  hint, suggesting potential shuffle overhead.
- **Ingress read mode advisory:** Warn when `format: csv` or `format: json` Ingress omits `mode` — the `permissive` default silently converts corrupt records to null rows with no error or log entry. Suggest `mode: FAILFAST` for production.
- **Watermark column type:** Warn when `watermark_column` is a string type —
  `MAX()` on strings is lexicographic, not temporal.
- **Partition suggestion:** Based on source file count and size statistics, recommend
  `repartition(n)` value at compile time.
- **Delta small-file advisory:** Warn when an Egress uses `format: delta` with
  `mode: append` or `mode: merge` and no external OPTIMIZE job is referenced —
  incremental Delta writes accumulate small files without OPTIMIZE.

---

## For Contributors

Read this section before modifying any Executor module or implementing new Channel
operations. Rules here are enforced by code review.

### Core Spark Principles

- **Lazy evaluation:** DataFrames are immutable execution plans. No computation
  until an action (`.write`, `.count`, `.collect`).
- **Narrow vs wide transformations:** Narrow (`filter`, `map`) keep data in
  partitions. Wide (`groupBy`, `join`, `distinct`) cause shuffles — expensive on
  large datasets.
- **Predicate pushdown:** Spark pushes filters to the source when using SQL or
  DataFrame expressions. Use `spark.sql()` or `expr()` to leverage this; avoid
  Python-side filtering that breaks the pushdown chain.

### Implementation Rules (DO NOT VIOLATE)

**1. Never add Spark actions in the critical execution path.**

Probes: use `SparkListener` (zero-cost) or explicit `.sample()` guarded by
`block_full_actions`. Never call `.count()`, `.collect()`, or `.show()` on the
main DataFrame inside the execution loop.

**2. Performance-degrading actions require explicit user opt-in.**

Any `df.count()`, `df.collect()`, or extra scan added for observability MUST be:
- Gated behind a named flag in `aqueduct.yml` under the `metrics:` section
- Default `false` (zero-cost production mode)
- Accompanied by a visible `click.echo()` warning at run start when enabled —
  naming the flag, the cost, and instructing the user to disable in production

No silent performance degradation. Ever.

**3. Use SQL expressions over Python UDFs.**

Python UDFs force row-by-row JVM↔Python serialization. LLM patches must favor
SQL `CASE`, `RLIKE`, and built-in functions. When a UDF is unavoidable, use
`pandas_udf` (Arrow-batched).

**4. Spillway routing is SQL-native — keep it that way.**

Spillway uses `df.filter(spillway_condition)` + `withColumn(F.lit(...))` for
`_aq_error_*` columns. Both are pure Spark SQL — vectorized, no Python boundary.
Do not introduce Python-side row iteration into the spillway path.

**Spillway cannot intercept UDF runtime exceptions.** The `spillway_condition` is
evaluated *after* a row passes through the transformation. A UDF that raises an
exception causes the Spark task (and therefore the pipeline) to abort — the row is
never delivered to the spillway. Spark does not provide per-row exception routing at
the task level.

The correct pattern is to write **defensive UDFs** that never raise: catch all
exceptions inside the UDF and return a sentinel value (e.g. a struct with a nullable
`result` + `error` string field). The `spillway_condition` then routes rows where
`error IS NOT NULL`. See the Blueprint Author note in `docs/specs.md` for a code
example.

**5. Validate `schema_hint` explicitly.**

Spark's `nullable=false` is advisory only — not enforced at runtime. Ingress must
compare `df.schema` against the hint and raise `IngressError` on mismatch.

**6. Union by name, not position.**

Funnel `union_all` uses `unionByName(allowMissingColumns=True)` in permissive mode.
Never use positional `union()` — column order is not guaranteed across sources.

**7. Avoid unintentional shuffles.**

- Junction uses `filter` (narrow — no shuffle)
- Funnel `union_all` is narrow; `union` (distinct) is wide
- Check `df.explain()` in tests when touching join or aggregation paths

### Common Pitfalls

| Pitfall | Mitigation |
|---|---|
| `sample()` looks cheap but reads all data | Document as full scan; gate behind `block_full_actions` |
| `coalesce` can create skewed partitions | Prefer `repartition` for balanced output; document tradeoff when using `coalesce` |
| `coalesce` when reducing >10× from skewed data | Full shuffle via `repartition` is better — coalesce just merges adjacent partitions, preserving the skew |
| Date parsing returns `null` on failure silently | `schema_hint` catches type mismatches; validate formats in SQL with `try_cast` |
| `to_date` / `to_timestamp` returns `null` on parse failure, not an exception | Add an Assert `null_rate` rule on date columns after Ingress |
| `TimestampType` only has second-level precision | Milliseconds/microseconds are silently truncated. Store sub-second values as `long` (epoch ms) and cast in SQL |
| Python UDFs cause memory contention under GC pressure | Discourage; use SQL built-ins or `pandas_udf` |
| UDF return type mismatch → `null`, not exception | If the UDF return value doesn't match the declared return type, Spark silently returns `null`. Always declare and test return types. |
| UDF throws exception → whole task fails, not just that row | Write defensive UDFs: catch internally, return sentinel struct; route via `spillway_condition: "struct.error IS NOT NULL"` |
| `broadcast_side` hint overridden silently by AQE | Disable AQE for that join or rely on `autoBroadcastJoinThreshold` instead of hints |
| Broadcast table too large → driver OOM | `broadcast()` hint triggers a `collect()` on the driver. If the "small" table exceeds driver memory, the driver crashes. Check table size before broadcasting; rely on `autoBroadcastJoinThreshold` for automatic safe broadcast. |
| Catalyst chooses suboptimal join strategy on small tables | Expose `spark_config` overrides (`spark.sql.autoBroadcastJoinThreshold`) — do not force hints in Aqueduct code |
| `COUNT(*)` counts null rows; `COUNT(col)` does not | `SELECT COUNT(*) FROM t` includes rows where all columns are null. `SELECT COUNT(col) FROM t` skips rows where `col` is null. Assert `min_rows` rules use `COUNT(*)` — be aware if the DataFrame has all-null rows. |
| Grouping sets / cube / rollup with nulls produce wrong results | Null values in grouping columns are used as aggregation-level markers. Pre-filter nulls from grouping columns before any `GROUPING SETS`, `ROLLUP`, or `CUBE` operation. |
| `nullable=false` in schema is not enforced by Spark | Spark treats `nullable=false` as a Catalyst optimizer hint, not a constraint. Nulls can still appear. Aqueduct's `schema_hint` validation is the enforcement layer — do not rely on schema alone. |
| `watermark_column` is a string type | `MAX()` is lexicographic on strings — warn at compile time (planned) |
| Incremental `MAX()` rescans output | Instruct users to add Checkpoint or cache; tracked in `incremental-watermark-scan` warning |
| Join produces duplicate column names | When both DataFrames have a column with the same name (beyond the join key), the result has two columns with the same name — ambiguous to reference. Fix by: (1) using string join key (auto-deduplicates), (2) dropping the duplicate after join, or (3) renaming before join. |
| Natural join uses implicit column matching | `NATURAL JOIN` matches on all columns with the same name — silently produces wrong results if shared column names are coincidental. Never use natural joins in Channel SQL. |

### Transformation Reference

| Operation | Narrow / Wide | Shuffle? |
|---|---|---|
| `filter`, `select`, `withColumn` | Narrow | No |
| `unionByName` | Narrow | No |
| `coalesce` | Narrow | No (but may cause skew) |
| `distinct` | Wide | Yes |
| `groupBy`, `agg` | Wide | Yes |
| `join` | Wide | Yes (unless broadcast) |
| `repartition` | Wide | Yes |
| `sample` | Narrow | No (but reads all rows) |
| `limit` + `collect` | Narrow | No (stops after first N rows from first partition(s)) |
| `sortWithinPartitions` | Narrow | No — sorts each partition locally without a full sort exchange |

**`sortWithinPartitions` optimization:** When a wide transform (join, groupBy) follows a sort, sorting within each partition first can reduce the shuffle exchange cost significantly — the merge phase is cheaper when input partitions are already locally sorted. Prefer over a global `orderBy` before wide transforms:

```python
# Instead of:
df.orderBy("event_ts").groupBy("user_id").agg(...)

# Better:
df.sortWithinPartitions("event_ts").groupBy("user_id").agg(...)
```

---

### Resource Tuning for Production

Aqueduct exposes the full `spark_config` block — any Spark property can be set there.
Key settings for production and backfill jobs:

```yaml
spark_config:
  # Dynamic allocation — scale executors with workload
  spark.dynamicAllocation.enabled: "true"
  spark.dynamicAllocation.minExecutors: "2"
  spark.dynamicAllocation.maxExecutors: "20"
  spark.dynamicAllocation.executorIdleTimeout: "60s"

  # Task failure tolerance — default is 4; increase for flaky sources
  spark.task.maxFailures: "8"

  # Adaptive Query Execution — on by default in Spark 3.2+
  # Handles skew joins, coalesces shuffle partitions automatically
  spark.sql.adaptive.enabled: "true"
  spark.sql.adaptive.skewJoin.enabled: "true"

  # Executor decommissioning — graceful shutdown on spot/preemptible nodes
  spark.decommission.enabled: "true"
  spark.storage.decommission.enabled: "true"
```

For backfill jobs (large date range, many partitions): increase
`spark.sql.shuffle.partitions` (default 200 is often too low) and
`spark.dynamicAllocation.maxExecutors` to match the data volume.

#### Input partition sizing

Two properties control how Spark splits input files into tasks:

```yaml
spark_config:
  spark.sql.files.maxPartitionBytes: "134217728"   # 128 MB (default) — max bytes per task
  spark.sql.files.openCostInBytes: "4194304"        # 4 MB (default) — file open overhead estimate
```

**Small files:** thousands of tiny Parquet files create thousands of tiny tasks, overwhelming the scheduler.
Lower `openCostInBytes` or use `coalesce` / `repartition` after the Ingress read.

**Large files:** a single 10 GB file may exceed executor memory per partition.
Lower `maxPartitionBytes` (e.g. `"67108864"` for 64 MB) so Spark splits it into more tasks.

`aqueduct doctor` warns when an Ingress path has many small files (planned — see Planned Future Checks).

#### Python UDF memory overhead

Python UDFs allocate memory outside the JVM heap (Py4J buffers, Arrow batch buffers).
If this off-heap allocation exceeds `spark.executor.memoryOverhead`, YARN/K8s kills the container
with exit code 137 (OOM killer) — the error looks like an executor loss, not an OOM.

```yaml
spark_config:
  spark.executor.memoryOverhead: "1g"   # default is max(384MB, 10% of executor.memory)
                                         # increase to 1–2 GB when using Python UDFs
  spark.executor.extraJavaOptions: "-XX:+UseG1GC -XX:MaxGCPauseMillis=200"
  spark.memory.offHeap.enabled: "true"
  spark.memory.offHeap.size: "2g"        # for large shuffles or Arrow-backed operations
```

#### External Shuffle Service (required for dynamic allocation)

Dynamic allocation can only release executors that hold no shuffle data. Without the External
Shuffle Service, executors that wrote shuffle files cannot be released — defeating dynamic allocation.

On YARN: `spark.shuffle.service.enabled: "true"` (YARN NodeManager must have the shuffle service JAR).
On Kubernetes: deploy the Spark Shuffle Service as a DaemonSet; set `spark.shuffle.service.enabled: "true"`.

Without the shuffle service, `dynamicAllocation.enabled: true` still works but executor release is
severely limited. Do not run dynamic allocation in production without it.

#### AQE sub-configurations

Adaptive Query Execution (AQE) is on by default in Spark 3.2+. Key sub-configs to tune:

```yaml
spark_config:
  spark.sql.adaptive.enabled: "true"
  spark.sql.adaptive.coalescePartitions.enabled: "true"
  spark.sql.adaptive.advisoryPartitionSizeInBytes: "134217728"       # 128 MB target partition size
  spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes: "268435456"  # 256 MB skew threshold
  spark.sql.adaptive.localShuffleReader.enabled: "true"              # reduces network for broadcast joins
```

**AQE and `broadcast_side` hints:** Blueprint `op: join` supports a `broadcast_side` hint.
This is advisory — AQE may override it at runtime based on actual partition statistics.
If the physical plan must use a specific join strategy (e.g. for correctness in skewed data),
disable AQE for that join by setting `spark.sql.adaptive.enabled: "false"` in the Blueprint's
`spark_config`. Document the reason — disabling AQE is a tradeoff (you lose partition coalescing
and skew handling).

#### S3A committers (cloud deployments)

When writing to S3 via `s3a://`, Spark defaults to rename-based commit semantics.
On S3, rename = copy + delete: **slow for large outputs and a data integrity risk on failure**.

Use S3A committers instead:

```yaml
spark_config:
  # Directory committer — safe, widely supported
  spark.hadoop.fs.s3a.committer.name: "directory"
  spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version: "2"

  # Magic committer — fastest (no rename phase), requires S3 object consistency
  # spark.hadoop.fs.s3a.committer.name: "magic"
  # spark.hadoop.fs.s3a.committer.magic.enabled: "true"
```

The magic committer writes directly to the final S3 path during the task write phase — no
rename step. Requires S3 (not S3-compatible) with strong consistency (all AWS regions since 2020).
The directory committer is safer for non-AWS S3-compatible stores.

#### Delta Lake MERGE optimization

Delta MERGE operations can produce severe write skew when the merge key distribution is uneven.
Enable pre-write repartitioning:

```yaml
spark_config:
  spark.databricks.delta.merge.repartitionBeforeWrite.enabled: "true"
```

This repartitions the target DataFrame by the merge key before writing, distributing the output
evenly. Adds one shuffle but eliminates small-file skew in the target table.

### `DataFrame.observe()` and Whole-Stage Codegen

Aqueduct uses `DataFrame.observe()` to collect per-stage metrics (row counts, byte sizes).
This is generally safe, but `observe()` inserts a metric-collection node into the physical plan
that can break whole-stage codegen boundaries.

Whole-stage codegen fuses multiple operators into a single JVM function for maximum throughput.
An observer node forces a stage boundary, potentially:

- Disabling codegen for the stage containing the observed DataFrame
- Adding serialization/deserialization overhead at the boundary

For most workloads this overhead is negligible. For high-throughput pipelines (billions of rows,
tight latency requirements), it can be a 5–15% throughput regression.

**Mitigation:** Disable `observe()`-based metrics and rely solely on SparkListener:

```yaml
metrics:
  use_observe: false   # fall back to SparkListener-only (zero observe() overhead)
```

When `use_observe: false`, per-module row counts come from SparkListener stage metrics only
(subject to stage fusion caveats — see SparkListener Row Estimates section above).

### `aqueduct test` and Spark Master

`aqueduct test` creates a real SparkSession using the `spark_master` from `aqueduct.yml`.
If `aqueduct.yml` configures a remote master (`spark://...`, `yarn`, `k8s://...`), the test
session connects to that cluster — test runs are **not isolated** to the local machine.

**Best practice:** Set `AQ_SPARK_MASTER=local[*]` in your test environment:

```bash
export AQ_SPARK_MASTER=local[*]
aqueduct test
```

Or set `spark_master: local[*]` in a separate `aqueduct.test.yml` and pass it with `--config`.
The `AQ_SPARK_MASTER` env var overrides `aqueduct.yml` without modifying the file.

### Caching Strategy — Multi-Consumer Channels {#caching-strategy}

When a Channel has two or more downstream consumers (edges leaving it), Spark re-evaluates its full DAG independently for each consumer branch. Adding `checkpoint: true` on the Channel materialises the DataFrame once and serves all consumers from disk.

**When to add `checkpoint: true`:**
- Channel is deterministic (no `rand()`, `uuid()`, `current_timestamp()` in query)
- Computation is expensive (join, window, UDF) and repeating it is wasteful
- Upstream source is a stable file (Parquet, Delta, CSV)

**When NOT to cache or checkpoint:**

| Situation | Reason |
|---|---|
| Source is Kafka / streaming | Each read is a new micro-batch; caching freezes stale data for second consumer |
| Query uses `rand()`, `uuid()`, or `current_timestamp()` | Cache fixes one value; second consumer gets same frozen value — usually wrong |
| DataFrame is too large for cluster memory | Cache spills to disk under memory pressure, may evict other cached DFs and cause cascade re-computation — net cost higher than re-running |
| Second "consumer" is a Probe (tap mode) | Probe attaches as a side-tap and does not force a second full DAG re-execution; warning fires but cost is lower than implied |

**`checkpoint: true` vs `cache()`:**
Aqueduct's `checkpoint: true` writes Parquet to `checkpoint_dir` and breaks DAG lineage. This is stronger than Spark's in-memory `cache()` — it survives driver restarts and works for DataFrames too large to fit in memory. Use it when computation is expensive and the pipeline may be resumed. For lightweight DataFrames that fit in memory, in-memory caching (not currently exposed as a Blueprint flag) is faster but non-durable.

### Aqueduct Checkpoint vs. Spark `df.checkpoint()`

These are different mechanisms — do not confuse them:

| | **Aqueduct `checkpoint: true`** | **Spark `df.checkpoint()`** |
|---|---|---|
| **What it writes** | Parquet files + `_aq_done` marker to a local/NFS path | Shuffle files to HDFS/S3 set via `SparkContext.setCheckpointDir()` |
| **Purpose** | Module-level resume: skip already-completed modules on `aqueduct run --resume` | Truncate Spark DAG lineage to avoid stack overflow on deep iterative jobs |
| **Reliability** | Durable if `checkpoint_dir` points to durable storage (NFS, S3) | Durable on HDFS/S3; `localCheckpoint()` variant is NOT durable (lost on executor failure) |
| **Aqueduct uses** | Yes — via `checkpoint_dir` in `aqueduct.yml` | Never — Aqueduct does not call `df.checkpoint()` |

**K8s note:** If `checkpoint_dir` points to a path inside the driver pod (e.g. `/tmp/aq_checkpoints`),
checkpoints are lost when the pod restarts. Use a PersistentVolume or an S3/GCS path for durable resume.

### SparkListener Queue Overhead

Aqueduct's SparkListener collects stage metrics (recordsWritten, shuffleBytes,
duration) after each stage completes. In normal workloads this overhead is negligible.

In very high-throughput jobs with thousands of short-lived tasks per second, the
listener event queue can become a bottleneck — Spark's internal event bus is
synchronous and a slow listener blocks dispatch.

**If you observe unexpected driver pauses:** check `spark.scheduler.listenerbus.eventqueue.capacity`
(default 10000) and whether the listener queue is dropping events
(`spark.scheduler.listenerbus.eventqueue.executorManagement.capacity`). Aqueduct
does not register a heavy listener — all processing is deferred to after the run —
so this is unlikely to be the bottleneck unless many other listeners are registered.
