# Aqueduct Diagnostics & Performance Guide

Reference for all compiler warnings, runtime cost models, and best practices.
Warnings emitted by `aqueduct validate` and `aqueduct compile` link here.

---

## Compiler Warnings

### `probe-sample-cost`

**Triggered when:** A Probe module has a signal of type `null_rates`, `row_count_estimate` (sample method), `value_distribution`, or `distinct_count`.

**What it means:**

`df.sample(fraction)` in Spark is a **row-level random filter**, not a partition prune. Every row in every partition must be read before sampling occurs — the engine cannot skip partitions because it doesn't know which rows will pass the random filter until it evaluates each one.

A `null_rates` signal with `fraction: 0.01` on a 10 TB dataset still reads all 10 TB of data. Only the output is 1% of rows; the I/O cost is 100%.

**True cost:** Full dataset I/O + CPU for the RNG per row.

**Zero-cost alternatives:**

| Signal | Zero-cost method |
|---|---|
| `row_count_estimate` | `method: spark_listener` — reads from `obs.db`, no Spark action |
| `schema_snapshot` | Always zero-action |
| `partition_stats` | `df.rdd.getNumPartitions()` — zero action |

**If you need the signal and accept the cost:** The warning is informational. Add `block_full_actions: false` explicitly to document the intent.

**Future:** File-level sampling (picking a random subset of Parquet/Delta files using file statistics) would make these signals genuinely cheap. Not yet implemented.

---

### `incremental-watermark-scan`

**Triggered when:** A Channel has `materialize: incremental` and no `Checkpoint` module upstream.

**What it means:**

After each incremental run, Aqueduct updates the watermark by computing:

```python
df.agg(F.max(watermark_column)).collect()
```

This is an extra Spark action on the Channel output DataFrame. If the DataFrame's lineage has not been materialized (e.g. no cache, no prior write action), Spark re-executes the full DAG to compute the max — a second full scan of the output data.

On large outputs (hundreds of GB), this doubles the reading cost of the incremental step.

**Mitigations:**

1. **Cache the Channel output** upstream of the egress. The `MAX()` action then reads from memory/disk cache, not recomputing the DAG.
2. **Use a Checkpoint module** upstream — Checkpoint materializes the DataFrame to storage, so subsequent actions read from the checkpoint rather than recomputing.
3. **Accept the cost** for small/medium datasets where the extra scan is negligible.

**Future:** Track watermark max as a side-effect during the write phase to eliminate the extra action entirely.

---

### `python-udf-performance`

**Triggered when:** A UDF in `udf_registry` has `lang: python` (the default).

**What it means:**

Python UDFs registered via `spark.udf.register()` execute **row-at-a-time** in the Python interpreter. They bypass Spark's Arrow-optimized (vectorized) execution path entirely:

- Each row crosses the JVM↔Python serialization boundary
- No Arrow batch transfer
- No SIMD/columnar processing

For high-volume channels (billions of rows), a Python UDF can be **10–100× slower** than an equivalent native Spark SQL expression.

**The spillway is unaffected:** Spillway routing uses SQL `filter()` expressions — fully vectorized. The performance concern is the UDF body itself, not the error-routing mechanism.

**Better alternatives:**

| Option | When to use |
|---|---|
| Native Spark SQL (`try_cast`, `try_divide`, `coalesce`, etc.) | Preferred — vectorized, no Python overhead |
| `pandas_udf` with Arrow | When complex Python logic is unavoidable; batched via Arrow, much faster than row UDFs |
| Java/Scala UDF (`lang: java`) | When maximum performance is required |

**If Python UDF is unavoidable:** The warning is informational. Document the performance tradeoff in the blueprint's `description` field.

---

### `delivery-semantics-append-retry`

**Triggered when:** `retry_policy.max_attempts > 1` and any Egress uses `mode: append`.

**What it means:**

If a run fails after an Egress has already written some rows, a retry will append the same rows again — producing duplicates. `mode: overwrite` is idempotent; `mode: append` is not.

**Fix:** Use `mode: overwrite` for retried pipelines, or set `max_attempts: 1` and handle retries at the orchestrator level.

---

## Cost Model Reference

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
| `data_freshness` (fraction=0) | **Full scan** | 1 | Yes |
| Incremental watermark `MAX()` | Full scan of output (if uncached) | 1 | No |
| Assert aggregate rules | Full scan | 1 (batched `agg`) | — |
| Assert `spillway_rate` | Full scan × 2 | 2 extra | — |

---

## Planned Checks (Future)

These checks are not yet implemented but are planned for future compiler passes:

- **Partition count advisory:** Warn when an Ingress path has many small files (detected via file count stats) and suggest `coalesce` or `repartition`.
- **Skew detection:** After a run, compare partition sizes from SparkListener metrics; warn if max/median ratio exceeds threshold.
- **Egress format advisory:** Warn when writing to CSV without explicit schema — downstream readers will trigger schema inference (full scan).
- **High-fanout Junction:** Warn when a Junction has many inputs without a broadcast hint, suggesting potential shuffle overhead.
- **Watermark column type:** Warn when `watermark_column` is a string type — max() on strings is lexicographic, not temporal.
