# Checkpoint + resume after failure

Demonstrates `checkpoint: true` on a module. When a downstream module
fails and the pipeline retries, the checkpointed module is **skipped**
during re-execution — its prior output is reused.

## What it shows

- `checkpoint: true` — marks a module whose output is persisted across
  attempts (and across `aqueduct run --resume` sessions)
- `retry_policy` at the blueprint level — retries the whole pipeline
  on failure

The transform module is cheap (UPPER + select), but in a real pipeline
it might be an expensive JOIN or a JDBC pull. Checkpointing it means
the retry doesn't redo the expensive work.

> **`spark_config` on a Blueprint:** Add a `spark_config:` block at the
> Blueprint root to set Spark conf values scoped to that pipeline:
> ```yaml
> spark_config:
>   spark.sql.shuffle.partitions: 8
>   spark.sql.adaptive.enabled: "true"
> ```
>
> **`tags` / `description` on modules:** All modules accept optional
> `tags: [key: value]` and `description: "..."` fields for documentation
> and filtering. These appear in the observability store and the dashboard.

> **`checkpoint_root:` override (aqueduct.yml):** By default checkpoints
> land under `<store_dir>/checkpoints/<run_id>/`. Set the top-level
> `checkpoint_root:` key in `aqueduct.yml` to point them at a different
> directory instead — e.g. faster local disk, or a volume explicitly
> mounted into every worker container. **Local filesystem paths only** —
> `s3://`/`s3a://`/`gs://`/`hdfs://`/`abfss://` values are rejected at
> config-load (remote checkpoint roots are a roadmap item). Example:
> ```yaml
> checkpoint_root: "/mnt/fast-local/aqueduct-checkpoints"
> ```

## Run

```bash
aqueduct run blueprint.yml

# To test resume, add a deliberate failure downstream,
# then fix and re-run with --resume:
#   1. Temporarily rename category → categorie in the SQL
#   2. aqueduct run blueprint.yml (fails)
#   3. Fix the SQL
#   4. aqueduct run blueprint.yml --resume
```
