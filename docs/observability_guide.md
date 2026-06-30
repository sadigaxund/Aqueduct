# Aqueduct Observability Guide

**Everything you need to monitor, debug, and analyze your pipelines.**

This guide combines schema reference and practical diagnostic queries for
Aqueduct's observability, lineage, depot, and benchmark stores.

## Filesystem Layout (1.1.0+ — per-pipeline)

Aqueduct routes observability artefacts per blueprint so multiple pipelines
sharing a project directory cannot stomp on each other's `run_id` namespace
or DuckDB file locks:

```
.aqueduct/
  observability/
    <blueprint_id>/
      observability.db     ← run_records, heal_attempts, healing_outcomes,
                             failure_contexts, probe_signals, module_metrics,
                             maintenance_metrics, patch_simulation,
                             signal_overrides, explain_snapshot,
                             column_lineage, patch_index
      blobs/               ← Zstandard-compressed manifest_json, provenance_json,
                             stack_trace payloads (<run_id>/{manifest,prov,stack}.json.zst)
      checkpoints/         ← Parquet checkpoints written by --resume
  depot.db                 ← project-wide cross-run KV state (@aq.depot.*),
                             incremental-Channel watermarks
  benchmark.duckdb         ← appears next to the scenarios dir, not here:
                             written to <scenarios_dir>/.aqueduct/benchmark.duckdb
```

**The artefact map (1.2):** The incremental-Channel watermark
sidecar (`watermarks/`) and the `schema_snapshot` sidecar (`snapshots/`) were
removed: watermarks are persisted to the Depot only, and `schema_snapshot`
payloads live solely in `probe_signals`. The `blobs/` directory and the patch
lifecycle (`patches/`) are now written through a pluggable **object store**
(`stores.blob`) — `local` (default, the layout above) or `s3` / `gcs` / `adls`
so a cluster pod leaves no local-FS artefacts. The `patch_index` table is the
relational truth for the object-store patch lifecycle.

Per-pipeline routing is the new default. Pre-1.1.0 stores at
`.aqueduct/observability.db` still load (the CLI's `_resolve_obs_db` helper
falls back to the legacy shared path when no per-pipeline DB carries the
requested `run_id`). Override paths in `aqueduct.yml`'s `stores:` block; the
read-side commands (`report`, `lineage`, `heal`) use the canonical
`open_obs_read` resolver. `aqueduct runs` uses its own inline store discovery
(DuckDB: walks per‑pipeline directories; Postgres: queries the observability
schema directly) rather than `open_obs_read`.

## Backends

Each store is independently pluggable in `aqueduct.yml`:

| Store           | Backends                       | Notes |
|-----------------|--------------------------------|-------|
| `observability` | `duckdb` (default) \| `postgres` | Relational; needs joins/aggregates. `redis` is rejected at config-load. `column_lineage` lives in this store. |
| `depots`        | name-keyed map; each mount `duckdb` (default) \| `postgres` \| `redis` | Cross-run KV (`@aq.depot.*`). `redis` allowed here only. The `default` mount always exists; keys are **per-blueprint isolated** (prefixed by blueprint_id) unless a mount sets `shared: true`. Incremental-Channel watermarks persist here (no depot ⇒ no incremental state). |
| `blob`          | `local` (default) \| `s3` \| `gcs` \| `adls` | Object store for observability blobs + the patch lifecycle. `s3`/`gcs`/`adls` need the `[object-store]` extra (fsspec); missing SDK raises a hard `ConfigError` at config-load, not a warning. `local` keeps the on-disk layout above. |

> **Blob integrity warning:** When `stores.observability.backend` is remote
> (Postgres) but `stores.blob.backend` is left at its default (`local`, unset),
> Aqueduct emits a non‑suppressible `AqueductWarning` — externalised blobs
> (manifests, stack traces, provenance) are written to the driver's local disk
> instead of the remote backend. Set `stores.blob.backend` explicitly to silence
> it (to `local` to acknowledge, or to a cloud backend).

With `postgres`, tables live in named schemas (`observability`, `depot`).
With `redis`, depot keys live directly in the configured Redis DB.
Column lineage tables live inside the `observability` schema.
DuckDB files are stable and safe to query with any DuckDB CLI / library.

---

## Schema Reference

Columns marked were added in 1.1.0 via idempotent additive
`ALTER` migrations — pre-existing stores upgrade in place; rows written
before the migration have `NULL` in those columns.

### `observability.db`

#### `run_records`

| Column           | Type                | Notes |
|------------------|---------------------|-------|
| `run_id`         | VARCHAR PRIMARY KEY | UUID; in multi-patch heal (auto + `max_patches > 1`) this is the per-iteration id from iteration 1+ |
| `blueprint_id`   | VARCHAR NOT NULL    | Blueprint identifier |
| `status`         | VARCHAR NOT NULL    | `running`, `success`, `error`, `patched`. (`skipped` exists only as a per-module status inside `module_results`, never at run level.) |
| `started_at`     | TIMESTAMPTZ NOT NULL | Iteration start |
| `finished_at`    | TIMESTAMPTZ          | NULL while running |
| `module_results` | JSON                | Per-module status/error blobs |
| `parent_run_id`  | VARCHAR             | User-visible outer `run_id` for multi-patch iterations. NULL on iteration 0 and on single-patch runs. Join all iterations of one heal call with `WHERE COALESCE(parent_run_id, run_id) = '<outer>'`. |

`Surveyor.record()` writes via `INSERT … ON CONFLICT DO UPDATE`, so each
multi-patch iteration owns its own row (the pre-1.1.0 code issued a
plain `UPDATE` and silently dropped iterations 1..N).

#### `failure_contexts`

| Column              | Type                | Notes |
|---------------------|---------------------|-------|
| `run_id`            | VARCHAR PRIMARY KEY | FK to `run_records` |
| `blueprint_id`      | VARCHAR NOT NULL    | |
| `failed_module`     | VARCHAR NOT NULL    | Module where the failure surfaced |
| `error_message`     | VARCHAR NOT NULL    | Full error string |
| `stack_trace`       | VARCHAR             | Used as the prompt fallback when structured extraction fails |
| `manifest_json`     | VARCHAR             | Blob path or inline JSON — compiled Manifest at failure |
| `provenance_json`   | VARCHAR             | Blob path or inline JSON — ProvenanceMap slice for the failed module |
| `started_at`        | TIMESTAMPTZ NOT NULL | |
| `finished_at`       | TIMESTAMPTZ NOT NULL | |
| `error_class`       | VARCHAR             | Spark 4.0 error condition (e.g. `UNRESOLVED_COLUMN.WITH_SUGGESTION`) or JVM throwable class name |
| `root_exception`    | JSON                | `{type, message}` from the innermost JVM throwable or Python cause |
| `sql_state`         | VARCHAR             | ANSI SQLSTATE from `PySparkException.getSqlState()` |
| `suggested_columns` | JSON                | Parsed list of backtick-quoted suggestions from Spark's "Did you mean …?" segment |
| `object_name`       | VARCHAR             | Offending column / table / object |

The structured fields populate from `_extract_structured_error()` —
best-effort, lazy-imported. When extraction returned None the row carries
NULL on these columns and the LLM prompt falls back to the raw stack trace.

#### `heal_attempts` (1.1.0+)

One row per LLM turn inside the unified reprompt loop — finer-grained than
`healing_outcomes` (which collapses an entire healing session to one row).

| Column              | Type                | Notes |
|---------------------|---------------------|-------|
| `id`                | VARCHAR PRIMARY KEY | UUID per attempt |
| `run_id`            | VARCHAR NOT NULL    | Per-iteration run id (multi-patch) or outer run id (single-patch) |
| `attempt_num`       | INTEGER NOT NULL    | 1-based |
| `error_class`       | VARCHAR             | Mirrors `failure_contexts.error_class` when available |
| `where_field`       | VARCHAR             | Pydantic location string for validation errors |
| `normalized_message`| VARCHAR             | Normalised error text used to compute `signature_hash` |
| `signature_hash`    | VARCHAR             | Stable 16-char sha1 over `(error_class, where, normalized_message)` |
| `tokens_in`         | INTEGER NOT NULL    | Prompt tokens; 0 when provider does not report usage |
| `tokens_out`        | INTEGER NOT NULL    | Completion tokens |
| `latency_ms`        | INTEGER NOT NULL    | Per-attempt wall clock |
| `gate_that_rejected`| VARCHAR             | `schema` \| `apply` \| `validate` (deep-loop gates) \| `provider` \| `budget` \| `defer_rejected` \| NULL on success |
| `escalated`         | BOOLEAN NOT NULL    | TRUE when the attempt ran with bumped temperature + skeleton template after `same_error_consecutive` tripped |
| `stop_reason`       | VARCHAR             | Filled only on the loop's terminal row (UPDATE post-loop); NULL on intermediate rows |
| `prompt_version`    | VARCHAR             | `aqueduct.agent.PROMPT_VERSION` at attempt time |
| `recorded_at`       | VARCHAR NOT NULL    | ISO-8601 |

`stop_reason` vocabulary: `solved`, `exhausted_attempts`,
`budget_seconds_exceeded`, `budget_tokens_exceeded`, `stuck_signature`,
`progress_stalled`, `api_error`, `deferred`. `solved` describes LLM loop termination
only (a parseable PatchSpec returned) — it does NOT mean the heal fixed
the pipeline. Join `healing_outcomes.run_success_after_patch` for that.

Two values are added outside the loop vocabulary: `cached` and
`replayed` mark synthetic zero-token rows (`attempt_num=0`,
`tokens_in=tokens_out=0`) written when the heal cache resolved the failure
without calling the LLM at all.

#### `healing_outcomes`

| Column                    | Type    | Notes |
|---------------------------|---------|-------|
| `id`                      | VARCHAR PRIMARY KEY | UUID per healing session |
| `run_id`                  | VARCHAR NOT NULL | Per-iteration run id |
| `parent_run_id`           | VARCHAR | User-visible outer `run_id`. Use `WHERE parent_run_id = '<outer>'` to gather all iterations from one multi-patch heal. NULL on single-patch runs. |
| `failed_module`           | VARCHAR | |
| `failure_category`        | VARCHAR | LLM-assigned: `schema_drift`, `bad_path`, `format_mismatch`, etc. |
| `model`                   | VARCHAR | LLM model id |
| `patch_id`                | VARCHAR | NULL when every attempt was rejected (synthesised row) |
| `confidence`              | DOUBLE  | LLM self-rated 0.0–1.0 |
| `patch_applied`           | BOOLEAN | |
| `run_success_after_patch` | BOOLEAN | The authoritative "did this heal actually work" flag |
| `applied_at`              | VARCHAR | ISO-8601 |
| `prompt_version`          | VARCHAR | From `aqueduct.agent.PROMPT_VERSION` |
| `failure_signature`       | VARCHAR | exact signature hash of the pipeline failure this heal addressed (16-char sha1 of error class + module + normalized message) |
| `failure_signature_coarse`| VARCHAR | coarse signature hash (error class + module, no message) — enables per-signature-family analytics (which families are solved by which cascade tier) without joining `patch_index` |
| `resolution`              | VARCHAR | `llm` (fresh agent patch), `cached` (pending-patch reuse, zero tokens), `replayed` (archived patch re-validated through gates, zero tokens). NULL on legacy rows — treat as `llm` (`COALESCE(resolution,'llm')`) |
| `model_cascade_position`  | INTEGER | 0-based cascade tier index of the producing model. NULL outside cascade or when no LLM ran. `model` records the producing tier's model (previously the top-level `agent.model` even under cascade) |

Zero-token heal coverage: `aqueduct runs --heal-coverage` aggregates
`resolution` counts across discovered observability DBs.

When the unified loop exits with `patch=None` (every attempt rejected, or a
budget axis tripped before a valid patch landed), the CLI synthesises one
`healing_outcomes` row per `attempt_records` entry with
`patch_applied=false`, `run_success_after_patch=false`, and
`failure_category` derived from the attempt's signature.

#### `patch_simulation`

One row per gate the patch went through. `gate` vocabulary: `lineage`,
`sandbox`, `explain` (guardrail rejections are recorded in `heal_attempts`,
not here). `status` is `pass` | `fail` | `warn` | `skip` (`skip` when
`sandbox_mode: off` synthesises a pass-through row).

#### `patch_index` (1.2.x+)

The relational truth for the object-store patch lifecycle. One row
per `patch_id`; `status` moves `pending` → `applied` | `rejected`. The patch
*body* lives in the object store at `object_key`; this row carries enough
metadata (`signature`, `signature_coarse`, `error_class`, `where_field`,
`normalized_message`, `rationale`, `ops`) for the heal cache to resolve
pending-reuse, coaching retrieval, and prompt history **without reading a body**
— only zero-token replay fetches the body. Backend-blind: the same SQL serves
local-disk, s3, gcs, and adls patch stores, replacing the former `os.scandir`
over the `patches/` directory.

#### `signal_overrides`

User overrides for Probe signals via `aqueduct signal <signal_id> --value`.

#### `explain_snapshot`

Rolling per-module Spark physical-plan summary (`Exchange` / Python UDF /
broadcast counts). Compared to the previous snapshot by the explain gate.

#### `module_metrics`

Per-module I/O metrics (`records_read`, `bytes_read`, `records_written`,
`bytes_written`, `duration_ms`) from SparkListener and `DataFrame.observe()`.
`NULL` means "not collected", never "zero records".

**Resource profiling.** `aqueduct report <run_id> --profile` ranks a run's
modules by duration (heaviest first) with each module's share of total time and
bytes; `aqueduct report --profile --blueprint <id> --last N` trends per-module
duration across the last N runs and flags a module whose latest run is >1.5× its
window average (a slowdown). Pure read-side over this table — no extra Spark
action, no `$` conversion (raw resource units, map to cost yourself).

#### `maintenance_metrics`

Post-write maintenance timings per module. The two columns are **engine-generic
slots**: `optimize_ms` is the compaction-class op, `vacuum_ms` the cleanup-class
op — Delta `OPTIMIZE`/`VACUUM`, Iceberg `rewrite_data_files`/`expire_snapshots`,
or Hudi `run_compaction`/`run_clean`, depending on the Egress `format`.

#### `probe_signals`

| Column        | Type | Notes |
|---------------|------|-------|
| `run_id`      | VARCHAR | |
| `probe_id`    | VARCHAR | |
| `signal_type` | VARCHAR | `schema_snapshot`, `null_rates`, `row_count_estimate`, etc. |
| `payload`     | JSON | Signal-type-specific data |
| `captured_at` | TIMESTAMPTZ | |

### Blob externalisation (1.1.2+)

Large payloads (`manifest_json`, `provenance_json`, `stack_trace`) are stored as
Zstandard-compressed `.json.zst` files under `.aqueduct/observability/<bp>/blobs/<run_id>/`
instead of inline in the DuckDB row. The DB column stores only the relative blob
path. `blob_store.materialize()` transparently resolves blob paths to content on read.

### `column_lineage`

| Column          | Type    | Notes |
|-----------------|---------|-------|
| `blueprint_id`  | VARCHAR | |
| `run_id`        | VARCHAR | |
| `channel_id`    | VARCHAR | Source Channel module |
| `output_column` | VARCHAR | |
| `source_table`  | VARCHAR | |
| `source_column` | VARCHAR | |
| `captured_at`   | TIMESTAMPTZ | |

### `channel_fingerprints`

SQL-AST normalised fingerprint per `op: sql` Channel (Lineage v2). A
**changelog, not a run-log**: one row per *distinct* fingerprint per
`(blueprint_id, channel_id)`. A run whose Channel SQL is unchanged only bumps
`last_seen`/`last_run_id` (via `ON CONFLICT`), so the table grows with the
number of times the SQL semantically changed — not with the number of runs.
The fingerprint is formatting/comment/keyword-case insensitive (sqlglot
canonicalisation), so a pure reformat does **not** create a new row while a
real predicate/column change does.

| Column          | Type    | Notes |
|-----------------|---------|-------|
| `blueprint_id`  | VARCHAR | |
| `channel_id`    | VARCHAR | |
| `fingerprint`   | VARCHAR | SHA-256 of the canonical SQL |
| `canonical_sql` | VARCHAR | Normalised SQL (for diffing two fingerprints) |
| `first_seen`    | TIMESTAMPTZ | First run that produced this fingerprint |
| `last_seen`     | TIMESTAMPTZ | Most recent run still on this fingerprint |
| `first_run_id`  | VARCHAR | |
| `last_run_id`   | VARCHAR | |

PK `(blueprint_id, channel_id, fingerprint)`.

**Diagnostic — did a Channel's SQL change, and when?**
```sql
SELECT channel_id, fingerprint, first_seen, last_seen
FROM channel_fingerprints
WHERE blueprint_id = 'my.pipeline'
ORDER BY channel_id, first_seen;
```
More than one row for a `channel_id` = the SQL was edited; `first_seen` of the
newest row is when the new version first ran.

> **`report --trend <column>` adds no table.** The cross-run column-quality
> trend is a **read-side aggregate** over `probe_signals` (`null_rates` +
> `schema_snapshot` payloads unrolled at query time) — deliberately *not*
> persisted, to avoid duplicating data the probes already store.

### `drift_checks`

Audit log for `aqueduct drift` — one row per Ingress per drift run. Created
lazily by the `drift` command (not at every `run`). The **baseline is
self-owned**: the most recent row's `live_schema` for an `(blueprint_id,
module_id)` is the baseline the next check diffs against, so drift needs **no
`schema_snapshot` Probe** to function.

| Column             | Type    | Notes |
|--------------------|---------|-------|
| `id`               | VARCHAR | Row UUID |
| `blueprint_id`     | VARCHAR | |
| `module_id`        | VARCHAR | Ingress module checked |
| `checked_at`       | TIMESTAMPTZ | |
| `baseline_schema`  | JSON    | `{column: type}` diffed against (NULL on the first, baseline-setting check) |
| `live_schema`      | JSON    | `{column: type}` read live; becomes the next baseline |
| `status`           | VARCHAR | `baseline_set` \| `no_drift` \| `drift_benign` \| `drift_breaking` |
| `breaking_changes` | JSON    | List of `{column, kind, baseline_type, live_type}` for dropped/type-changed |
| `benign_changes`   | JSON    | List of added columns |
| `patch_id`         | VARCHAR | Staged patch id when a breaking drift was healed |

**Diagnostic — which sources drifted and got a patch?**
```sql
SELECT module_id, checked_at, status, patch_id
FROM drift_checks
WHERE blueprint_id = 'my.pipeline' AND status = 'drift_breaking'
ORDER BY checked_at DESC;
```

> Predicted-drift FailureContexts are driven through the agent **in memory** and
> are **not** written to `failure_contexts` — that table stays a record of real
> run failures, so failure analytics are never skewed by predictions.

### `<scenarios_dir>/.aqueduct/benchmark.duckdb`

#### `benchmark_results`

One row per `(scenario_id, model, prompt_version)` benchmark execution. Lives in its own store (DuckDB file or, with `stores.benchmark.backend: postgres`, the `benchmark` Postgres schema) — disjoint from observability rows, no `run_id` foreign key. `aqueduct benchmark-stats` aggregates it into a model leaderboard, hardest-scenario ranking, and a by-day pass-rate trend (all computed from the latest row per `(scenario, model)`); `aqueduct benchmark-diff` compares the two most recent runs per pair.

| Column                | Type                | Notes |
|-----------------------|---------------------|-------|
| `id`                  | VARCHAR PRIMARY KEY | |
| `recorded_at`         | VARCHAR NOT NULL    | ISO-8601 |
| `scenario_id`         | VARCHAR NOT NULL    | |
| `model`               | VARCHAR NOT NULL    | |
| `prompt_version`      | VARCHAR             | |
| `provider`            | VARCHAR             | |
| `base_url`            | VARCHAR             | |
| `passed`              | BOOLEAN NOT NULL    | |
| `patch_valid`         | BOOLEAN NOT NULL    | |
| `patch_applies`       | BOOLEAN NOT NULL    | |
| `confidence`          | DOUBLE              | |
| `duration_seconds`    | DOUBLE              | |
| `attempts_to_parse`   | INTEGER             | |
| `diag_score`          | DOUBLE              | |
| `root_cause_match`    | BOOLEAN             | |
| `category_match`      | BOOLEAN             | |
| `failures`            | JSON                | Hard assertion failures |
| `soft_failures`       | JSON                | |
| `violated_guardrails` | JSON                | NULL when scenario declares no guardrails; `[]` when defined-and-clean |
| `stop_reason`         | VARCHAR             | Same vocabulary as `heal_attempts.stop_reason` |
| `escalated`           | BOOLEAN             | |
| `tokens_in_total`     | INTEGER             | |
| `tokens_out_total`    | INTEGER             | |

### `depot.db`

#### `depot_kv`

Cross-run KV state (`@aq.depot.*`). Keys are **per-blueprint isolated** by
default: the engine transparently prefixes every key with `<blueprint_id>:`, so
two blueprints sharing a physical depot never collide (you'll see rows like
`sales:watermark`, `orders:watermark`). Configure mounts under `stores.depots`
(a name-keyed map); set `shared: true` on a mount for deliberate cross-blueprint
sharing (raw, unprefixed keys) — read those via `@aq.depot.<name>.get(...)`. For
parallel writers on a shared mount, use postgres/redis (concurrent), not a single
DuckDB file.

---

## Cookbook

Every recipe uses the **When → What you learn → What to do next** format.

### Run post-mortem

**When** a run failed and you want the headline.
**What you learn** Module, structured error fields, and the first-line error.
**What to do next** Pull the structured `error_class` / `object_name`
straight into your Spark UI search or grep against the blueprint.

```sql
SELECT r.run_id,
       r.status,
       r.parent_run_id,
       f.failed_module,
       f.error_class,
       f.object_name,
       f.suggested_columns,
       f.sql_state,
       substr(f.error_message, 1, 200) AS error
FROM run_records r
LEFT JOIN failure_contexts f USING (run_id)
WHERE r.run_id = '<run_id>';
```

**When** the structured-error block is unexpectedly NULL on a Spark failure.
**What you learn** Whether the executor actually handed the live exception
to the Surveyor (1.1.0 wired this — pre-1.1.0 rows are NULL by design).
**What to do next** If `module_results` shows `error` but all five
structured fields are NULL on a post-1.1.0 run, check that the executor
populated `ModuleResult.exception` for that module type.

### Heal-loop forensics

**When** you want to see what each LLM turn produced (1.1.0+).
**What you learn** Per-attempt signature, token spend, latency, which gate
rejected the attempt, and whether escalation kicked in.
**What to do next** Repeated `signature_hash` rows mean the model is stuck;
a row with `gate_that_rejected='apply'` means the patch parsed but failed
guardrails — fix the guardrail policy or add prompt context.

```sql
SELECT attempt_num,
       gate_that_rejected,
       escalated,
       error_class,
       substr(signature_hash, 1, 8) AS sig,
       tokens_in + tokens_out AS tokens,
       latency_ms,
       stop_reason
FROM heal_attempts
WHERE run_id = '<run_id>'
ORDER BY attempt_num;
```

**When** a multi-patch heal (auto + `max_patches > 1`) ran multiple iterations and you want the full
picture from the outer (user-visible) `run_id` (1.1.0+).
**What you learn** Every iteration row plus every attempt across all of them.
**What to do next** Cross-iteration patterns: which iteration finally
solved it, and which gate was the bottleneck.

```sql
WITH outer_runs AS (
    SELECT run_id
    FROM run_records
    WHERE COALESCE(parent_run_id, run_id) = '<outer>'
)
SELECT h.run_id,
       h.attempt_num,
       h.gate_that_rejected,
       h.stop_reason,
       h.tokens_in + h.tokens_out AS tokens
FROM heal_attempts h
JOIN outer_runs USING (run_id)
ORDER BY h.recorded_at;
```

**When** `heal_attempts` shows rows but `healing_outcomes` is empty
(symptom from the 1.1.0 synthesis fix).
**What you learn** Whether every attempt was rejected at apply time. The
1.1.0 CLI synthesises one `healing_outcomes` row per rejected attempt;
older rows really were lost.
**What to do next** If the synthesis is still absent on a 1.1.0 run, that
indicates the `apply_callback` path is bypassed — verify `_check_guardrails`
fired by inspecting `gate_that_rejected`.

```sql
SELECT ha.run_id,
       COUNT(ha.id) AS attempts,
       SUM(CASE WHEN ha.gate_that_rejected IS NOT NULL THEN 1 ELSE 0 END) AS rejections,
       (SELECT COUNT(*) FROM healing_outcomes ho WHERE ho.run_id = ha.run_id) AS outcome_rows
FROM heal_attempts ha
WHERE ha.run_id = '<run_id>'
GROUP BY ha.run_id;
```

**When** correlating LLM loop termination with whether the heal actually
fixed the pipeline.
**What you learn** `stop_reason='solved'` means a parseable PatchSpec was
returned, **not** that the patched pipeline succeeded.
**What to do next** Cross-check `run_success_after_patch` for the truth.

```sql
SELECT ha.run_id,
       ha.stop_reason,
       ho.patch_applied,
       ho.run_success_after_patch,
       ho.confidence
FROM heal_attempts ha
JOIN healing_outcomes ho ON ho.run_id = ha.run_id
WHERE ha.stop_reason IS NOT NULL
ORDER BY ha.recorded_at DESC
LIMIT 20;
```

### Cost & performance

**When** you want the LLM bill for one heal session.
**What you learn** Total tokens, LLM wall time, attempt count.
**What to do next** Pair with `BudgetConfig.max_tokens_total` —
consistently bumping into the cap is a signal to tighten prompts.

```sql
SELECT SUM(tokens_in + tokens_out) AS total_tokens,
       SUM(latency_ms) / 1000.0    AS llm_seconds,
       COUNT(*)                    AS attempts
FROM heal_attempts
WHERE run_id = '<run_id>';
```

**When** comparing models on the benchmark store.
**What you learn** Pass rate, guardrail-clean rate, average token cost,
stop-reason distribution.
**What to do next** Models with high `stuck_signature` rates need either
prompt engineering or a model swap; high `exhausted_attempts` rates argue
for a higher `max_reprompts` cap.

```sql
SELECT model,
       COUNT(*) AS runs,
       AVG(CASE WHEN passed THEN 1.0 ELSE 0.0 END) AS pass_rate,
       SUM(tokens_in_total + tokens_out_total)     AS tokens,
       SUM(CASE WHEN stop_reason = 'stuck_signature'    THEN 1 ELSE 0 END) AS stuck,
       SUM(CASE WHEN stop_reason = 'exhausted_attempts' THEN 1 ELSE 0 END) AS exhausted,
       SUM(CASE WHEN stop_reason = 'solved'             THEN 1 ELSE 0 END) AS solved
FROM benchmark_results
GROUP BY model
ORDER BY pass_rate DESC;
```

### Sandbox replay diagnostics (1.1.0+)

**When** a patch passed `agent.sandbox_mode: sample` but failed once
applied to production.
**What you learn** Whether the sample skipped the offending row shape.
Re-run with `sandbox_mode: preflight` (requires `danger.allow_full_preflight`)
to replay the full dataset.
**What to do next** Inspect `patch_simulation` to see which gate the patch
passed under sample mode, then re-stage the patch and replay under
preflight; a divergent result confirms sample miss.

```sql
SELECT patch_id, gate, status, sample_rows, detail, recorded_at
FROM patch_simulation
WHERE run_id = '<run_id>' AND gate = 'sandbox'
ORDER BY recorded_at;
```

### Recent failures across all blueprints

```sql
SELECT r.run_id,
       r.blueprint_id,
       r.started_at,
       f.failed_module,
       f.error_class
FROM run_records r
JOIN failure_contexts f USING (run_id)
WHERE r.status = 'error'
ORDER BY r.started_at DESC
LIMIT 10;
```

### Most common failure signatures

```sql
SELECT substr(signature_hash, 1, 8) AS sig,
       error_class,
       COUNT(*) AS times_hit
FROM heal_attempts
WHERE signature_hash IS NOT NULL
GROUP BY signature_hash, error_class
ORDER BY times_hit DESC
LIMIT 10;
```

### Column lineage

```sql
-- What feeds output column 'my_column'?
SELECT source_table, source_column, channel_id, blueprint_id
FROM column_lineage
WHERE output_column = 'my_column';
```

### Fleet query layer (`stores/queries.py`)

Computed at read‑time from the observability store — no extra write‑side
columns or aggregation tables:

| Function | Returns | Description |
|---|---|---|
| `fleet_summary` | `list[BlueprintSummary]` | Per‑blueprint roll‑up (last run status, success rate, heal count) across all discovered blueprints |
| `runs_over_time` | `list[DayCount]` | Daily run counts over a configurable window (`days` default 30) |
| `failure_categories` | `dict[str, int]` | Count of failures grouped by `error_class` |
| `heal_coverage` | `dict[str, int]` | Heals resolved by the signature memory cache (`memory`) vs the LLM (`agent`), per blueprint |

DuckDB: the functions iterate discovered per‑pipeline files. Postgres: a single
schema‑scoped query. Both backends return the same shape.

### Read‑only viewers

Two local, on‑demand observability viewers — neither runs in the data path:

| Viewer | Command | Extra | Description |
|---|---|---|---|
| Dashboard | `aqueduct dashboard` | `dashboard` (Streamlit + Plotly) | Fleet overview: cross‑blueprint runs, success/heal rates, per‑run module metrics, column‑lineage Sankey, doctor, config. Manual refresh; no background polling. |
| Studio | `aqueduct studio` | `tui` (Textual) | Interactive TUI: run list, ad‑hoc SQL over the observability store, doctor, config, lineage. |

Both are read‑only — they issue `SET read_only = true` on DuckDB and use a
read‑only connection on Postgres.

---

## Quick CLI reference

| Goal                       | Command                                   |
|----------------------------|-------------------------------------------|
| List recent runs           | `aqueduct runs --last 20`                 |
| Failed runs                | `aqueduct runs --failed`                  |
| Detailed report            | `aqueduct report <run_id>`                |
| Column lineage             | `aqueduct lineage <blueprint.yml>`        |
| Override a Probe signal    | `aqueduct signal <signal_id> --value false` |
| Heal a failed run          | `aqueduct heal <run_id>`                  |

**Tip:** DuckDB files are stable; point any DuckDB client at them for
custom dashboards. The `_resolve_obs_db()` helper inside the CLI walks the
per-pipeline directories to find which DB carries a given `run_id`, so the
read-side commands work without specifying a path.
