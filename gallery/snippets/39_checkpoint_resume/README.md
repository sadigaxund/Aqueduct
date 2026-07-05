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
