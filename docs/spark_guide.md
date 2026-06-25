# Aqueduct Spark Guide

Single reference for Spark behavior in Aqueduct — covers both blueprint authoring
(compiler warnings, cost model) and internal implementation rules (contributor rules,
pitfalls, transformation reference).

---

## For Blueprint Authors

### Compiler Warnings

Every warning links to an anchor here. Warnings are informational unless marked **[ERROR]**.

Each warning prints as `AQ-WARN [<rule_id>] …`. Only **registered** rules
(listed under `compiler/warnings/__init__.py`'s `RULES` list) are suppressible
via `aqueduct --suppress-warning <rule_id> <command>` (repeatable, `'*'` = all)
or `warnings.suppress:` in `aqueduct.yml`. Inline, runtime, and session-startup
warnings are **not suppressible**.

#### Registered (suppressible via `warnings.suppress`)

| Rule id | Flags | Detail |
|---|---|---|
| `count_col_likely_count_star` | `COUNT(col)` silently skips NULLs — likely meant `COUNT(*)` | — |
| `custom_probe_driver_code` | Custom probe signal backed by driver-side code — engine cannot enforce zero-cost | [custom-probe-driver-code](#custom-probe-driver-code) |
| `file_format_no_repartition` | parquet/json/csv Egress without repartition/`partition_by` → one file per task | [one-file-per-partition](#parquet-and-json-output-one-file-per-partition) |
| `jdbc_missing_partition` | JDBC Ingress without `partitionColumn`/bounds reads through one connection | [jdbc-ingress-parallelism](#jdbc-ingress-parallelism) |
| `kafka_checkpoint_stale` | Checkpointing a Kafka-fed Channel freezes a stale snapshot for other consumers | — |
| `nondeterministic_fanout` | `rand()`/`uuid()`/`current_timestamp()` diverges across consumer branches | [caching-strategy](#caching-strategy) |

#### Inline compiler (not suppressible)

| Rule id | Flags | Detail |
|---|---|---|
| `delivery_append_retry_dupes` | `mode: append` with `max_attempts > 1` — retries may produce duplicate rows | [delivery-append-retry-dupes](#delivery-append-retry-dupes) |
| `maintenance_optimize_non_delta` | `maintenance.optimize` on non-Delta format — OPTIMIZE is Delta-only | [maintenance-optimize-non-delta](#maintenance-optimize-non-delta) |
| `perf_delta_append_no_partition` | `mode: append` without `partition_by`/repartition accumulates small files | [append-no-partition](#append-no-partition) |
| `perf_hadoop_fs_in_options` | Hadoop FS credentials in `options:` don't reach HadoopConfiguration — use `spark_config:` | [hadoop-fs-in-options](#hadoop-fs-in-options) |
| `perf_incremental_watermark_scan` | `materialize: incremental` re-scans output for `MAX(watermark_column)` | [incremental-watermark-scan](#incremental-watermark-scan) |
| `perf_multi_consumer_no_cache` | Multi-consumer Channel without a Checkpoint re-evaluates the DAG per branch | [caching-strategy](#caching-strategy) |
| `perf_probe_sample_full_scan` | Probe sample-based signal — `df.sample()` is a full dataset scan | [probe-sample-cost](#probe-sample-cost) |
| `perf_python_udf_row_at_a_time` | Python UDF bypasses Arrow/vectorized execution | [python-udf-performance](#python-udf-performance) |

#### Runtime CLI (not suppressible)

| Rule id | Flags | Detail |
|---|---|---|
| `cluster_store_path_relative` | Relative observability store path on cluster target — lost on driver restart | [cluster-store-path-relative](#cluster-store-path-relative) |

#### Session-startup (not suppressible)

| Rule id | Flags | Detail |
|---|---|---|
| `jar_availability` | Declared format (jdbc/kafka/delta/…) has no matching JAR on the session classpath | — |

#### Doctor-only (not suppressible)

| Rule id | Flags | Detail |
|---|---|---|
| `iceberg_catalog` | `format: iceberg` module has no catalog key in blueprint `spark_config` | [iceberg-hudi](#iceberg--hudi-jars-catalog-and-maintenance) |

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

#### `append-no-partition`

**Triggered when:** An Egress uses `mode: append` with `format: parquet` or `format: delta`
and has no `partition_by`, `repartition`, or `coalesce`.

Each append run writes new files into the output directory. Without partitioning,
every run produces at least as many files as the number of Spark partitions at
the output stage (default 200 after a shuffle). Over time the directory fills with
thousands of tiny files, degrading read performance (S3 ListObjects latency, Parquet
metadata overhead, Hive metastore thrash).

**Mitigations:**
1. `partition_by: [high_cardinality_col, ...]` — organises files into a directory tree;
   each partition gets its own subdirectory so per-partition file counts stay bounded.
2. `repartition: N` or `coalesce: N` — reduces output file count at the cost of a shuffle
   (`repartition`) or potential skew (`coalesce`). Target 128–512 MB per file.
3. External `OPTIMIZE` job (Delta) — runs periodically to compact small files outside
   the pipeline. See the [post-write maintenance](#iceberg-hudi) table.

**Note:** The related `file_format_no_repartition` warning fires for ALL write modes
(append, overwrite, etc.) and all problematic formats (parquet, json, csv). This warning
is narrower — only `append` mode — because the severity is higher (compounding over time
vs. a one-time small-file problem).

---

#### `delivery-append-retry-dupes`

**Triggered when:** `retry_policy.max_attempts > 1` and any Egress uses `mode: append`.

Retries on an `append`-mode Egress produce duplicate rows — the failed run already
wrote some data before the failure. `mode: overwrite` is idempotent; `mode: append`
is not.

**Fix:** Use `mode: overwrite` for retried pipelines, or `max_attempts: 1` with
orchestrator-level retry handling.

---

#### `hadoop-fs-in-options`

**Triggered when:** An Ingress has Hadoop filesystem keys (`fs.s3a.*`, `fs.gs.*`,
`fs.azure.*`, `fs.hdfs.*`, `fs.abfs.*`) inside its `options:` block.

`DataFrameReader.option()` passes values to the connector (Parquet, CSV, JDBC, etc.)
but **does not** propagate them to Spark's `HadoopConfiguration`. The S3A, GCS,
Azure, or HDFS filesystem delegate reads Hadoop configuration to resolve credentials,
endpoints, and timeouts — so these keys are silently ignored.

**Fix:** Move them to `spark_config:` with the `spark.hadoop.` prefix:

```yaml
# Wrong — silently ignored:
config:
  format: parquet
  path: s3a://bucket/table/
  options:
    fs.s3a.access.key: "${AWS_ACCESS_KEY_ID}"
    fs.s3a.secret.key: "${AWS_SECRET_ACCESS_KEY}"

# Correct:
spark_config:
  spark.hadoop.fs.s3a.access.key: "${AWS_ACCESS_KEY_ID}"
  spark.hadoop.fs.s3a.secret.key: "${AWS_SECRET_ACCESS_KEY}"
```

See the [S3A committer](#s3a-committer) section for committer-specific options
(e.g. `spark.hadoop.fs.s3a.committer.name`) which follow the same pattern.

---

#### `maintenance-optimize-non-delta`

**Triggered when:** An Egress has `maintenance.optimize: true` but uses a format
other than `delta` (e.g. `parquet`, `csv`, `json`).

`OPTIMIZE` is a Delta Lake-only SQL command. Iceberg and Hudi have their own
compaction operations (`rewrite_data_files`, `run_compaction`) configured through
different `maintenance:` keys. On non-delta, non-iceberg, non-hudi formats the
maintenance block is silently skipped at runtime — the pipeline continues, but
the `optimize:` setting has no effect.

**Fix:** Set `format: delta` if you need OPTIMIZE, or use the correct key for your format:

| Format | Compaction key |
|--------|---------------|
| `delta` | `optimize: true` |
| `iceberg` | `rewrite_data_files: true` |
| `hudi` | `compaction: true` |

See the [post-write maintenance](#iceberg-hudi) table for the full reference.

---

#### `custom-probe-driver-code`

**Triggered when:** A Probe signal has `type: custom` and uses a `module:` + `entry:`
pointer (or a `plugin:` entry-point reference).

Custom probes run arbitrary Python on the Spark driver. Unlike built-in signals
(`null_rates`, `schema_snapshot`, `partition_stats`) which execute as lazy Spark
expressions, a driver-code callable can call `.collect()`, `.count()`, or other
Spark actions — the engine cannot enforce the zero-cost-observability contract.

**Suggested alternatives:**
1. **Inline SQL** (`sql:` or `passed_when:`) — executes as a native Spark expression,
   fully vectorized, zero driver-side Python overhead.
2. **Built-in signal types** — use `schema_snapshot`, `partition_stats`, or
   `row_count_estimate` with `method: spark_listener` for zero-cost observability.

**To silence:** If the callable is known to be cheap (e.g. it only inspects the
DataFrame schema without triggering an action), explicitly `--suppress-warning
custom_probe_driver_code` to document the conscious choice.

---

#### `cluster-store-path-relative`

**Triggered when:** `stores.observability.path` is a relative path and the deployment
target is a remote cluster (`spark://`, `yarn`, `k8s://`). This is a **runtime**
warning emitted by `aqueduct run`, not a compile-time compiler warning.

On cluster targets, the Spark driver may start in an ephemeral working directory
(YARN container, Kubernetes pod, remote Spark worker). A relative path like
`.aqueduct/observability` resolves against the current working directory — which
may differ across runs or disappear after a restart.

**Fix:** Set an absolute path (or use `stores.observability.backend: postgres` for
a database-backed store). If using a shared filesystem (NFS, EFS), mount it at a
fixed path:

```yaml
stores:
  observability:
    path: /mnt/shared/aqueduct/observability
```

For cloud deployments, prefer `backend: postgres` with a connection string over
a filesystem path — no CWD dependency.

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

**Assert `not_null` vs `null_rate` performance.** `not_null` with `on_fail: quarantine` runs a row-wise
`df.filter(col IS NULL)` — a single lazy transformation, zero extra Spark actions beyond the original
pipeline. `null_rate` uses `df.sample(fraction).agg()` to avoid a full scan on large data; if it were made
quarantine-able, the engine would need a full scan + filter + split (2 extra actions), defeating its
performance-through-sampling design. Choose `not_null` when you want to drop null rows; choose `null_rate`
when you want a sampled population alarm that does not pay the cost of a full scan.

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

**`mode: overwrite_partitions`** (Delta and partition-aware formats). Two strategies:

| Strategy | Config | Behaviour |
|---|---|---|
| `replaceWhere` | `replace_where: "<predicate>"` (e.g. `"event_date = '2025-01-01'"`) | Delta `replaceWhere` — atomically replaces rows matching the predicate. No dynamic partition overwrite. |
| Dynamic partition overwrite | `partition_by: ["col"]` + `mode: overwrite_partitions` | Spark's `spark.sql.sources.partitionOverwriteMode=dynamic` — replaces only the partitions present in the output DataFrame. Requires `partition_by` to be set. |

When neither `replace_where` nor `partition_by` is set, `mode: overwrite_partitions` falls back to a plain `overwrite` (full table replacement).

**`on_new_columns`** — schema-drift contract (Ingress and Egress). Compares the live DataFrame schema against `known_columns` (or `schema_hint` names as fallback); policies:

| Policy | Behaviour |
|---|---|
| `allow` | Default. Absorbs new columns silently. |
| `fail` | Raises if the source/DataFrame has columns outside the declared baseline. |
| `alert` | Warns, then proceeds with the new columns. |

No-op when no baseline is declared, or on the first write (`mode: merge`).

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

### Table addressing — wiring an external catalog {#catalog-wiring}

When a Blueprint uses `table:` (instead of `path:`) on an Ingress or Egress,
Spark resolves `catalog.schema.table` through whichever catalog the session is
configured to talk to. Aqueduct defines nothing about the table's internals ---
the catalog (Unity Catalog, AWS Glue, Hive metastore, Iceberg REST, Polaris)
owns the location, schema, format, and permissions.

The catalog connection is configured through standard `spark.sql.catalog.*`
properties in `spark_config:` (blueprint-level or `aqueduct.yml` engine-level).
There is no Aqueduct-specific catalog config:

```yaml
spark_config:
  # Spark session catalog (default in-memory — tables are session-scoped)
  # No special config needed; `table: my_table` works out of the box.

  # Unity Catalog (Databricks) — Spark automatically discovers UC tables
  # through the Databricks Runtime; no extra spark_config needed when
  # running on a Databricks cluster.

  # Hive metastore
  spark.sql.catalogImplementation: "hive"
  spark.sql.warehouse.dir: "s3a://my-bucket/warehouse"
  # spark.hadoop.hive.metastore.uris: "thrift://metastore:9083"

  # Iceberg REST catalog
  spark.sql.catalog.iceberg: "org.apache.iceberg.spark.SparkCatalog"
  spark.sql.catalog.iceberg.type: "rest"
  spark.sql.catalog.iceberg.uri: "https://iceberg-rest.example.com/"

  # Polaris (Iceberg catalog)
  spark.sql.catalog.polaris: "org.apache.iceberg.spark.SparkCatalog"
  spark.sql.catalog.polaris.type: "rest"
  spark.sql.catalog.polaris.credential: "${POLARIS_CREDENTIAL}"
  spark.sql.catalog.polaris.rest-catalog.uri: "https://polaris.example.com/api/catalog"
```

Once configured, use `table: catalog.db.table` in your Blueprint modules --- the
identifier is resolved through the active catalog.

**Limitations.** `time_travel` (version/timestamp pin) is not supported on
`table:`-addressed Ingress reads (the `spark.read.table()` API does not accept
DataFrameReader options). Use a Channel with `TIMESTAMP AS OF` / `VERSION AS OF`
SQL syntax instead. `register_as_table` is meaningless when `table:` is set on
an Egress --- the catalog table is already the direct write target.

---

### Iceberg & Hudi: Jars, Catalog, and Maintenance {#iceberg-hudi}

`format: iceberg` and `format: hudi` are **mechanical** — Spark performs all I/O;
Aqueduct only wires the config. Read/write need no special blueprint syntax
beyond the `format:` value, but two prerequisites apply.

**1. Jars (must match your Spark version exactly).** Add the runtime bundle via
`spark_config` (blueprint or `aqueduct.yml`). The Hudi bundle in particular is
Spark-version-pinned — a mismatch fails at session start.

```yaml
spark_config:
  # Iceberg (Spark 3.5 line shown — bump the 3.5 to match your Spark)
  spark.jars.packages: "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
  spark.sql.extensions: "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  spark.sql.catalog.local: "org.apache.iceberg.spark.SparkCatalog"
  spark.sql.catalog.local.type: "hadoop"
  spark.sql.catalog.local.warehouse: "/data/warehouse"
  # Hudi (match the 3.5 to your Spark)
  # spark.jars.packages: "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0"
  # spark.serializer: "org.apache.spark.serializer.KryoSerializer"
```

**2. Iceberg needs a catalog, not just a path.** `format: iceberg` with no
`spark.sql.catalog.*` fails at runtime; `aqueduct doctor` emits an
`iceberg_catalog` **warning** when a `format: iceberg` module has no catalog key
in the blueprint `spark_config`. Address Iceberg tables by their catalog
identifier (`table: local.db.orders`).

**Post-write maintenance** is format-aware (under the Egress `maintenance:` key):

| Format | `maintenance:` keys | SQL run | Addressed by |
|--------|---------------------|---------|--------------|
| `delta` | `optimize`, `zorder_by`, `vacuum: <hours>` | `OPTIMIZE` / `VACUUM` | `path` |
| `iceberg` | `rewrite_data_files: true`, `expire_snapshots: true` | `CALL <catalog>.system.rewrite_data_files` / `expire_snapshots` | `table` (`catalog.db.tbl`) |
| `hudi` | `compaction: true`, `clean: true` | `CALL run_compaction` / `run_clean` | `path` |

All maintenance ops are **non-fatal** (logged as warnings, pipeline continues).
Timing lands in `maintenance_metrics`: `optimize_ms` is the compaction-class op
(OPTIMIZE / rewrite_data_files / run_compaction) and `vacuum_ms` the cleanup-class
op (VACUUM / expire_snapshots / run_clean), across all three engines.

### Custom Python DataSources (`format: custom`) {#format-custom}

Spark 4.0+ supports user-defined Python DataSources via `format: custom` +
`class:` — an importable `DataSource` subclass that implements the
`DataSourceRegister` contract. Aqueduct supports this on both Ingress and
Egress:

```yaml
ingress:
  format: custom
  class: my_package.MyDataSource
egress:
  format: custom
  class: my_package.MySink
```

The class must be importable on the driver (i.e. in `PYTHONPATH` or an installed
package). `aqueduct doctor` verifies importability; a missing class raises an
error at module dispatch. As with UDFs, Aqueduct never inspects or modifies the
class body — the code is opaque to the engine.

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
| Window-spec columns appear in lineage | `PARTITION BY` / `ORDER BY` columns inside an `OVER()` clause are excluded from data-source lineage — `row_number() OVER (ORDER BY id) AS rn` reports `rn ← *` (not `rn ← id`). The ordering column is not a data dependency of the result row. |

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
