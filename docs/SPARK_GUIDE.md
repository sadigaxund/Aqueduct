# Aqueduct Spark Guide

Single reference for Spark behavior in Aqueduct — covers both blueprint authoring
(compiler warnings, cost model) and internal implementation rules (contributor rules,
pitfalls, transformation reference).

---

## For Blueprint Authors

### Compiler Warnings

Every warning links to an anchor here. Warnings are informational unless marked **[ERROR]**.

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
| `row_count_estimate` | `method: spark_listener` — reads from `obs.db`, no Spark action |
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
- **Watermark column type:** Warn when `watermark_column` is a string type —
  `MAX()` on strings is lexicographic, not temporal.
- **Partition suggestion:** Based on source file count and size statistics, recommend
  `repartition(n)` value at compile time.

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
| Date parsing returns `null` on failure silently | `schema_hint` catches type mismatches; validate formats in SQL with `try_cast` |
| Python UDFs cause memory contention under GC pressure | Discourage; use SQL built-ins or `pandas_udf` |
| Catalyst chooses suboptimal join strategy on small tables | Expose `spark_config` overrides (`spark.sql.autoBroadcastJoinThreshold`) — do not force hints in Aqueduct code |
| `watermark_column` is a string type | `MAX()` is lexicographic on strings — warn at compile time (planned) |
| Incremental `MAX()` rescans output | Instruct users to add Checkpoint or cache; tracked in `incremental-watermark-scan` warning |

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
