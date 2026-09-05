# Aqueduct Observability Guide

**Everything you need to monitor, debug, and analyze your pipelines.**

This guide combines schema reference and practical diagnostic queries for
Aqueduct's observability, lineage, depot, and benchmark stores.

## Filesystem layout (1.1.0+: per-pipeline)

Aqueduct routes observability artefacts per blueprint so multiple pipelines
sharing a project directory cannot stomp on each other's `run_id` namespace
or DuckDB file locks:

```
.aqueduct/
  <blueprint_id>/
    observability.db     ← run_records, heal_attempts, healing_outcomes,
                           failure_contexts, probe_signals, module_metrics,
                           maintenance_metrics, patch_simulation,
                           signal_overrides,
                           column_lineage, patch_index
    blobs/               ← Zstandard-compressed manifest_json, provenance_json,
                           stack_trace payloads (<run_id>/{manifest,prov,stack}.json.zst)
    checkpoints/         ← Parquet checkpoints written by --resume
    depot.db             ← this blueprint's cross-run KV state (@aq.depot.*),
                           incremental-Channel watermarks. Its own file,
                           never inside observability.db. A depot mount with
                           an explicit `path` lives wherever you point it
                           instead.
  benchmark.duckdb        ← appears next to the scenarios dir, not here:
                           written to <scenarios_dir>/.aqueduct/benchmark.duckdb
```

**The artefact map (1.2):** The incremental-Channel watermark
sidecar (`watermarks/`) and the `schema_snapshot` sidecar (`snapshots/`) were
removed: watermarks are persisted to the Depot only, and `schema_snapshot`
payloads live solely in `probe_signals`. The `blobs/` directory and the patch
lifecycle (`patches/`) are now written through a pluggable **object store**
(`stores.blob`): `local` (default, the layout above) or `s3` / `gcs` / `adls`
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
> Aqueduct emits a non‑suppressible `AqueductWarning`: externalised blobs
> (manifests, stack traces, provenance) are written to the driver's local disk
> instead of the remote backend. Set `stores.blob.backend` explicitly to silence
> it (to `local` to acknowledge, or to a cloud backend).

With `postgres`, tables live in named schemas (`observability`, `depot`).
With `redis`, depot keys live directly in the configured Redis DB.
Column lineage tables live inside the `observability` schema.
DuckDB files are stable and safe to query with any DuckDB CLI / library.

---

## Schema reference

Columns marked were added in 1.1.0 via idempotent additive
`ALTER` migrations: pre-existing stores upgrade in place; rows written
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
| `module_results` | JSON                | Per-module status/error blobs. Since 2.37 each entry also carries `engine`: the module's fully-resolved execution engine (`Manifest.modules[i].engine`), populated for every run, single-engine or polyglot alike (a single-engine run's every module simply names the same one engine). `aqueduct report --format json` surfaces this per module plus a run-level `engines` list (the distinct set actually present). Since 2.65 each entry also carries `warnings` (list of `[rule_id, message]` pairs) and `notes` (list of strings); `ModuleResult.warnings`/`.notes` were previously computed and displayed but never persisted. |
| `parent_run_id`  | VARCHAR             | User-visible outer `run_id` for multi-patch iterations. NULL on iteration 0 and on single-patch runs. Join all iterations of one heal call with `WHERE COALESCE(parent_run_id, run_id) = '<outer>'`. |
| `engine`         | VARCHAR             | Since 2.65: the run's execution engine (`spark` \| `duckdb`), stamped from `Surveyor`'s own `engine` constructor arg (or `record(engine=...)`'s override for a polyglot run's failing island). Previously engine was only available per-module inside the `module_results` JSON blob; a **successful** run's engine comparison required parsing that blob row-by-row. This column makes `WHERE engine = ?` work directly. Migrated in place on existing stores (`_RUN_RECORDS_MIGRATIONS` in `aqueduct/surveyor/ddl.py`, mirroring the Phase-84 `benchmark_results` migration pattern); NULL on rows written before the upgrade. No query in the codebase currently filters on this column, so the DDL no longer creates `idx_run_records_engine` for it; a store created before this change keeps the orphaned index harmlessly. |

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
| `manifest_json`     | VARCHAR             | Blob path or inline JSON: compiled Manifest at failure |
| `provenance_json`   | VARCHAR             | Blob path or inline JSON: ProvenanceMap slice for the failed module |
| `started_at`        | TIMESTAMPTZ NOT NULL | |
| `finished_at`       | TIMESTAMPTZ NOT NULL | |
| `error_class`       | VARCHAR             | Spark 4.0 error condition (e.g. `UNRESOLVED_COLUMN.WITH_SUGGESTION`) or JVM throwable class name |
| `root_exception`    | JSON                | `{type, message}` from the innermost JVM throwable or Python cause |
| `sql_state`         | VARCHAR             | ANSI SQLSTATE from `PySparkException.getSqlState()` |
| `suggested_columns` | JSON                | Parsed list of backtick-quoted suggestions from Spark's "Did you mean …?" segment |
| `object_name`       | VARCHAR             | Offending column / table / object |
| `engine`            | VARCHAR             | Execution engine the failure occurred on (`spark` \| `duckdb`). Stamped by `Surveyor.record()` from its required `engine` constructor arg by default; a polyglot run (§10.9's Handoff/islands) passes `Surveyor.record(result, engine=<failing island's engine>)` explicitly, so this column (and the `ExecutorProtocol.extract_error` used to populate the structured fields below it) reflects the island that actually failed, not the run's nominal `deployment.engine` default |

The structured fields populate from `_extract_structured_error()`:
best-effort, lazy-imported. When extraction returned None the row carries
NULL on these columns and the LLM prompt falls back to the raw stack trace.

#### `heal_attempts` (1.1.0+)

One row per LLM turn inside the unified reprompt loop, finer-grained than
`healing_outcomes` (which collapses an entire healing session to one row).

| Column              | Type                | Notes |
|---------------------|---------------------|-------|
| `id`                | VARCHAR PRIMARY KEY | UUID per attempt |
| `run_id`            | VARCHAR NOT NULL    | Per-iteration run id (multi-patch) or outer run id (single-patch) |
| `attempt_num`       | INTEGER NOT NULL    | 1-based |
| `error_class`       | VARCHAR             | Mirrors `failure_contexts.error_class` when available |
| `where_field`       | VARCHAR             | Pydantic location string for validation errors |
| `normalized_message`| VARCHAR             | Normalised error text: used to compute a signature at match time (`error_class`/`where`/`normalized_message` together identify a repeat failure); digits, quoted (`'…'`/`"…"`) values, backtick-quoted identifiers (`` `col` ``, Spark 4 `UNRESOLVED_COLUMN` style), and filesystem paths are collapsed to placeholders so failures differing only in specifics match identically |
| `signature_hash`    | VARCHAR             | **No longer populated (2.85+).** Column stays for schema compatibility (no migration) but every write leaves it NULL: an observability-store audit found it write-only (never selected by any reader; use `error_class`/`where_field`/`normalized_message` directly instead) |
| `tokens_in`         | INTEGER NOT NULL    | Prompt tokens; 0 when provider does not report usage |
| `tokens_out`        | INTEGER NOT NULL    | Completion tokens |
| `latency_ms`        | INTEGER NOT NULL    | Per-attempt wall clock |
| `gate_that_rejected`| VARCHAR             | `schema` \| `apply` \| `validate` (deep-loop gates) \| `provider` \| `budget` \| `defer_rejected` \| NULL on success |
| `escalated`         | BOOLEAN NOT NULL DEFAULT FALSE | **No longer populated (2.85+, C1).** Column stays (defaults FALSE) but every write leaves it at the default; also found write-only, never selected by any reader |
| `stop_reason`       | VARCHAR             | Filled only on the loop's terminal row (UPDATE post-loop); NULL on intermediate rows |
| `prompt_version`    | VARCHAR             | `aqueduct.agent.PROMPT_VERSION` at attempt time |
| `recorded_at`       | VARCHAR NOT NULL    | ISO-8601 |
| `chain_link`        | INTEGER             | 1-based index of which attempt within the chain this heal-attempt row belongs to. Orthogonal to `attempt_num`, which still counts reprompts *within* one attempt |
| `engine`            | VARCHAR             | Execution engine this attempt targeted (`spark` \| `duckdb`) |
| `defer_reason`      | VARCHAR             | **(2.66+)** Queryable bucket from `DeferToHumanOp.defer_reason` (`infrastructure` \| `upstream_schema_change` \| `data_shape_change` \| `insufficient_context` \| `other`) when this attempt's patch deferred to a human; NULL for every non-deferring attempt. Filled on the loop's terminal row alongside `stop_reason` (via `update_heal_attempt_stop_reason`), same timing as `stop_reason` itself: the value isn't known at the per-turn `record_heal_attempt` INSERT |

Columns added to `heal_attempts` after a release are migrated in place. Surveyor init runs idempotent `ALTER TABLE … ADD COLUMN IF NOT EXISTS` statements (see `_HEAL_ATTEMPTS_MIGRATIONS` in `aqueduct/surveyor/ddl.py`, which also carries `chain_link`, `engine`, and `defer_reason`) right after the `CREATE TABLE IF NOT EXISTS`. A pre-upgrade observability database therefore gains new columns on the next run, with no manual migration needed. `failure_contexts` and `healing_outcomes` gained `engine` the same way, via `_FAILURE_CONTEXTS_MIGRATIONS` and `_HEALING_OUTCOMES_MIGRATIONS` in the same file. `patch_index` gained it via `PATCH_INDEX_MIGRATIONS` in `aqueduct/patch/index.py`; `run_records` gained `engine` (+ its index) the same way via `_RUN_RECORDS_MIGRATIONS` (2.65).

`stop_reason` vocabulary: `solved`, `exhausted_attempts`,
`budget_seconds_exceeded`, `budget_tokens_exceeded`, `stuck_signature`,
`progress_stalled`, `api_error`, `deferred`. `solved` describes LLM loop termination
only (a parseable PatchSpec returned): it does NOT mean the heal fixed
the pipeline. Join `healing_outcomes.run_success_after_patch` for that.

**Removed.** `cached` and `replayed` used to mark synthetic
zero-token rows (`attempt_num=0`, `tokens_in=tokens_out=0`) written when the
signature-keyed heal cache resolved a failure without calling the LLM.
That cache is gone — `aqueduct run` short-circuits on an existing pending
patch before this table is ever written to for that iteration, so no
`heal_attempts` row is recorded at all for a short-circuited run. A
pre-2.3.0 database can still carry historical `cached`/`replayed` rows;
treat them as archival.

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
| `confidence`              | DOUBLE  | LLM self-rated 0.0-1.0 |
| `patch_applied`           | BOOLEAN | |
| `run_success_after_patch` | BOOLEAN | The authoritative "did this heal actually work" flag |
| `applied_at`              | VARCHAR | ISO-8601 |
| `prompt_version`          | VARCHAR | From `aqueduct.agent.PROMPT_VERSION` |
| `failure_signature`       | VARCHAR | exact signature hash of the pipeline failure this heal addressed (16-char sha1 of error class + module + normalized message) |
| `failure_signature_coarse`| VARCHAR | coarse signature hash (error class + module, no message), enables per-signature-family analytics (which families are solved by which cascade tier) without joining `patch_index` |
| `resolution`              | VARCHAR | `llm` (fresh agent patch) — the only value written since the signature-keyed heal cache was removed. A pre-2.3.0 database may still carry historical `cached` (pending-patch reuse) / `replayed` (archived patch re-validated through gates) rows; NULL on legacy rows, treat as `llm` (`COALESCE(resolution,'llm')`) |
| `model_cascade_position`  | INTEGER | 0-based cascade tier index of the producing model. NULL outside cascade or when no LLM ran. `model` records the producing tier's model (previously the top-level `agent.model` even under cascade) |
| `engine`                  | VARCHAR | Execution engine this heal targeted (`spark` \| `duckdb`) |

Zero-token heal coverage: `aqueduct runs --heal-coverage` aggregates
`resolution` counts across discovered observability DBs.

Cascade-tier vs outcome: `aqueduct runs --cascade` (2.85+, C1). Before this,
`model_cascade_position` was written by every cascade step but never
selected anywhere:

```console
$ aqueduct runs --cascade --store-dir .aqueduct
  tier  outcome  count  resolution
  ----  -------  -----  ----------
     0  success      12  llm
     1  success       4  llm
     1  failed        2  llm
```

When the unified loop exits with `patch=None` (every attempt rejected, or a
budget axis tripped before a valid patch landed), the CLI synthesises one
`healing_outcomes` row per `attempt_records` entry with
`patch_applied=false`, `run_success_after_patch=false`, and
`failure_category` derived from the attempt's signature.

#### `patch_simulation`

One row per gate the patch went through. `gate` vocabulary: `engine_config`,
`lineage`, `sandbox`, `resolvability` (Gate 4, 2.66: one row per
patch reporting the worst verdict across every `declare_dependency` op it
carries; guardrail rejections, meaning `forbidden_ops`, `allowed_paths`, and the
`set_engine_config` allowlist, are recorded in `heal_attempts`, not here).
`status` is `pass` | `fail` | `warn` | `not_applicable` | `unavailable`.

The three verdicts (`pass`, `warn`, `fail`) mean the gate ran. The other two
mean it did not, and they are **opposite facts**: the question is whether a
check was *owed*:

- **`not_applicable`**: no check was owed. Either the patch has no surface
  this gate looks at (the `lineage` gate against a `set_engine_config` op,
  which carries no module reference for a column-impact diff; the
  `engine_config` gate against a pipeline-only patch), or the operator
  declared none is owed here (`agent.sandbox_mode: off`, itself gated on
  `danger.allow_skip_sandbox`). Informational, never blocking. Distinct from
  `pass`: `pass` means the gate looked and found nothing wrong,
  `not_applicable` means there was nothing to look at.
- **`unavailable`**: a check *was* owed and the environment prevented it.
  The target engine's dependencies are not installed, its session would not
  start, or the Blueprint is polyglot and the sandbox replays only one
  engine. **Nothing about the patch was verified.** For the `sandbox` gate
  this **blocks auto-apply** and requires a human.

`detail` carries the reason in both cases, e.g. "no module-lineage surface
for this patch's ops", or "sandbox replay did not run: engine 'spark' would
not start (…); this patch was NOT replayed".

> **Changed in 2.1.0 (BREAKING).** The `sandbox` gate
> previously wrote `skip` for *both* of the above, so a patch that was never
> verified was indistinguishable from one that needed no verification, and
> auto-approval accepted both. `skip` is no longer written. Rows recorded
> before the split keep the old value and are **not** migrated: a `skip` row
> is genuinely ambiguous after the fact, and rewriting it would invent a
> distinction the data never carried. Treat pre-2.1.0 `skip` rows as
> "unknown which", and filter on `finished_at` if a query needs the new
> precision.

The `engine_config` gate is the mirror image of that pair: it reports
`not_applicable` for the patches `lineage` reports `pass` on (a
pipeline-only patch writes no engine config, so there is nothing for it to
compare) and `pass` when the patch's write really does change the effective
session config the target engine will run with (`aqueduct.yml`'s
`engine.<name>` block merged under the Blueprint's own; see
`docs/specs.md` §8.5). Its `fail` is a `set_engine_config` write whose
effective before/after are identical: a clean apply that changes nothing an
engine can see. That row is written for the record only: the refusal
itself is enforced at apply time, so a `fail` here is always accompanied by
a patch that never reached the Blueprint.

The `resolvability` gate reports `not_applicable` for a patch carrying no
`declare_dependency` op (most rows). `warn` means the declared requirement
resolves on PyPI but is not installed in this environment: unlike every
other gate's `warn`, this one is never advisory. It is a hard defer to a
human (install it, then `aqueduct patch apply <id>`), and the patch is
never auto-applied. `fail` means no such package (or no version satisfying
the specifier) exists on PyPI at all. `unavailable` means the PyPI check
itself could not run (network/timeout): nothing about the requirement was
verified, fail-closed like the `sandbox` gate's `unavailable`.

For the SAME zero-module patches, the `sandbox` gate still runs and can
still report `pass` on a clean replay, but its `detail` says so honestly
rather than reading as a validated fix: the session built and the sample
replayed successfully under the patched engine config, but a small local
sample cannot reproduce the cluster-scale resource failure (OOM, shuffle
spill) the patch usually targets: only the full re-run proves efficacy.
`status` is unaffected; only the wording changes.

#### `patch_index` (1.2.x+)

The relational truth for the object-store patch lifecycle. One row
per `patch_id`; `status` moves `pending` → `applied` | `rejected`. The patch
*body* lives in the object store at `object_key`; this row carries enough
metadata (`signature`, `signature_coarse`, `error_class`, `where_field`,
`normalized_message`, `rationale`, `ops`) for `aqueduct patch list`/`pull`,
the `aqueduct run` pending-patch guard (a `pending` row on the current
`blueprint_id` short-circuits before any LLM call), and prompt history
**without reading a body**. Backend-blind: the same SQL serves local-disk,
s3, gcs, and adls patch stores, replacing the former `os.scandir` over the
`patches/` directory. `engine` (VARCHAR) records the execution engine this
patch was healed against (`spark` \| `duckdb`) — auditability only, not used
by any lookup filter.

**Heal provenance columns.** Four columns carry apply-time heal
detail that used to live in the Blueprint's own `healed_by:` record and was
moved here because it grows with every green run: `engine_version`
(VARCHAR, the installed engine package version at heal time, nullable
best-effort), `engine_config_delta` (JSON, `{engine: {key: {before,
after}}}`, present only when the patch changed effective engine config),
`perf_baseline` (JSON, the pre-patch green run's wall-clock duration and
volume proxy, present only when a green run preceded the patch), and
`perf_observations` (JSON, one warn-only perf note per engine, written by
the same green-run stamp that updates the Blueprint's `validated_on`). The
Blueprint's `healed_by` record still names the `patch_id`; these four
columns are read back by `aqueduct doctor`'s `healed-config:<patch_id>`
rows and by `aqueduct patch revert`. See `aqueduct/patch/index.py` and
`docs/specs.md` §8.14.

#### `signal_overrides`

User overrides for Probe signals via `aqueduct signal <signal_id> --value`.

#### `module_metrics`

Per-module I/O metrics (`records_read`, `bytes_read`, `records_written`,
`bytes_written`, `duration_ms`). `NULL` means "not collected", never "zero
records" — a genuinely empty read or write is stored as a real `0`.

**Which columns each engine can fill.** The table and its writer are shared
(`MODULE_METRICS_DDL`/`write_module_metrics`, `aqueduct/executor/models.py`),
but the two engines derive the numbers by different means, so the NULLs differ
and that difference is a property of the engine, not a bug in the run:

| Column | Spark | DuckDB |
| :- | :- | :- |
| `duration_ms` | Always measured. | Always measured. |
| `records_read` | From `DataFrame.observe()`, collected off a downstream action that already scans the data (Ingress rows are attributed after the Egress writes). | A real `COUNT(*)` over the module's input, on Ingress, Channel, Junction, Funnel and Probe. |
| `records_written` | From SparkListener stage metrics. | From the `COPY` statement's own returned count on Egress; no extra scan. |
| `bytes_read` | From SparkListener stage metrics. | **NULL except where a path can be stat-ed**: a real local-filesystem size for an Ingress reading a local file or directory, and for a Handoff read (the spill directory). A mid-pipeline relation (Channel, Junction, Funnel, Probe) has no cheap byte source in DuckDB at all, and a remote path (`s3://`, `gs://`, …) has no cheap stat, so both stay NULL rather than reporting a fabricated `0`. |
| `bytes_written` | From SparkListener stage metrics. | Local-filesystem size of the Egress path; NULL for `table:`/`depot:` writes and remote paths. |

DuckDB's `records_read` costs one extra aggregate per module: unlike Spark's
`Observation`, which rides along on a scan that was happening anyway, a DuckDB
relation is a lazy plan re-executed at each consumption point, so there is
nothing to piggyback on. A `COUNT(*)` is the cheapest scan available and the
column is far more useful populated than permanently `NULL`.

**Cross-engine handoff (2.36).** A synthetic Handoff module (`aqueduct.compiler.handoff`, §10.9) gets a row here like any other module: `bytes_written`/`duration_ms` on the upstream (write) side, `bytes_read`/`duration_ms` on the downstream (read) side, measured from the spill directory's on-disk size. The DDL and writer (`MODULE_METRICS_DDL`/`write_module_metrics`, `aqueduct/executor/models.py`) are engine-agnostic and shared; the Handoff row was DuckDB's first `module_metrics` write, and that engine now writes a row for every module type it runs (see the per-engine table above). `records_read`/`records_written` stay NULL for a Handoff row on both engines: the transport is a byte-level parquet copy, not a row-counted operation.

**Indexes (2.65).** `idx_module_metrics_module (module_id)` serves the
cross-run per-module trend query (`report --profile --blueprint <id> --last
N`); `idx_module_metrics_run (run_id)` serves the actual per-run profile
lookup (`report <run_id> --profile`, `queries.py:270,280`), previously
unindexed, a full table scan on every profile call as the table grew.

**Resource profiling.** `aqueduct report <run_id> --profile` ranks a run's
modules by duration (heaviest first) with each module's share of total time and
bytes; `aqueduct report --profile --blueprint <id> --last N` trends per-module
duration across the last N runs and flags a module whose latest run is >1.5× its
window average (a slowdown). Pure read-side over this table, no extra Spark
action, no `$` conversion (raw resource units, map to cost yourself).

#### `maintenance_metrics`

Post-write maintenance timings per module. The two columns are **engine-generic
slots**: `optimize_ms` is the compaction-class op, `vacuum_ms` the cleanup-class
op: Delta `OPTIMIZE`/`VACUUM`, Iceberg `rewrite_data_files`/`expire_snapshots`,
or Hudi `run_compaction`/`run_clean`, depending on the Egress `format`.

#### `probe_signals`

| Column        | Type | Notes |
|---------------|------|-------|
| `run_id`      | VARCHAR | |
| `probe_id`    | VARCHAR | |
| `signal_type` | VARCHAR | `schema_snapshot`, `null_rates`, `row_count_estimate`, etc. |
| `payload`     | JSON | Signal-type-specific data |
| `captured_at` | TIMESTAMPTZ | |

**`sample_rows` redaction + write-time cap.** `sample_rows` is the only
built-in signal type that persists real sampled **data row content**
(`df.limit(n).collect()`): every other signal here is aggregate/statistical
(counts, rates, min/max/percentiles) and carries no comparable sensitivity or
size risk. Its `payload` is routed through the same `redact()`
(`aqueduct/redaction.py`) the `failure_contexts` failure path already uses,
so a registered `@aq.secret()` value inside a sampled row is scrubbed to
`[REDACTED]` before the INSERT, not stored raw. It also gets a dedicated,
count-based cap, enforced at write time: only the most recent 20 rows are
kept **per `probe_id`**. This cap is fixed — not configurable via
`aqueduct.yml`.

Every other observability table grows append-only forever; pruning it is the
operator's responsibility (there is no built-in retention/pruning feature).

### Blob externalisation (1.1.2+)

Large payloads (`manifest_json`, `provenance_json`, `stack_trace`) are stored as
Zstandard-compressed `.json.zst` files under `.aqueduct/<bp>/blobs/<run_id>/`
instead of inline in the DuckDB row. The DB column stores only the relative blob
path. `BlobStore.materialize()` (`aqueduct.stores.object_store`) transparently
resolves blob paths to content on read.

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

**Dedup against `channel_fingerprints` (2.65).** A Channel's lineage rows are
written only when its `channel_fingerprints` SQL fingerprint actually
*changed* since the last recorded run: a repeat run of unchanged SQL writes
nothing for that Channel (`aqueduct.compiler.lineage._unchanged_channel_ids`),
mirroring `channel_fingerprints`'s own changelog model instead of duplicating
every row on every compile. Handoff passthrough rows (no SQL, not
fingerprint-tracked) are always written. `lineage()` (`stores/queries.py`)
also changed: with no explicit `run_id`, the read now scopes to the **latest**
run in scope (optionally within `blueprint_id`) with `DISTINCT`/`ORDER BY`,
instead of an unscoped `LIMIT 500` that could mix rows from many historical
runs.

**Per-hop transform trace, not just the stored graph.** `aqueduct lineage <blueprint.yml> --chain <column> --types` gives a *deeper* view than a `column_lineage` query, a vertical, per-hop trace showing the sqlglot-inferred SQL type at every Channel the column passes through, with a `⚠ type change` marker on any hop where the inferred type shifts. It is computed on demand from the compiled manifest (no store read, no Spark action) rather than read from this table, so it works even before a run has ever persisted a `column_lineage` row. See [CLI Reference](cli_reference.md) for a worked example.

### `channel_fingerprints`

SQL-AST normalised fingerprint per `op: sql` Channel (Lineage v2). A
**changelog, not a run-log**: one row per *distinct* fingerprint per
`(blueprint_id, channel_id)`. A run whose Channel SQL is unchanged only bumps
`last_seen`/`last_run_id` (via `ON CONFLICT`), so the table grows with the
number of times the SQL semantically changed, not with the number of runs.
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

**Diagnostic: did a Channel's SQL change, and when?**
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
> `schema_snapshot` payloads unrolled at query time), deliberately *not*
> persisted, to avoid duplicating data the probes already store.

### `drift_checks`

Audit log for `aqueduct drift`: one row per Ingress per drift run. Created
lazily by the `drift` command (not at every `run`). The **baseline is
self-owned**: the most recent row's `live_schema` for an `(blueprint_id,
module_id)` is the baseline the next check diffs against, so drift needs **no
`schema_snapshot` Probe** to function. `drift` is **report-only**: it detects
and records drift but never heals it — a breaking change is left for the next
real `run` to hit and self-heal reactively.

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
| `patch_id`         | VARCHAR | Unused; always NULL. Column kept for schema stability, no longer written |

**Diagnostic: which sources have breaking drift?**
```sql
SELECT module_id, checked_at, status
FROM drift_checks
WHERE blueprint_id = 'my.pipeline' AND status = 'drift_breaking'
ORDER BY checked_at DESC;
```

### `<scenarios_dir>/.aqueduct/benchmark.duckdb`

#### `benchmark_results`

One row per `(scenario_id, model, prompt_version)` benchmark execution. Lives in its own store (DuckDB file or, with `stores.benchmark.backend: postgres`, the `benchmark` Postgres schema), disjoint from observability rows, no `run_id` foreign key. `aqueduct benchmark-stats` aggregates it into a model leaderboard, hardest-scenario ranking, and a by-day pass-rate trend (all computed from the latest row per `(scenario, model)`); `aqueduct benchmark-diff` compares the two most recent runs per pair.

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
| `refusal`             | VARCHAR             | Why the scenario declined to produce a patch: `policy` \| `inert` \| `guardrail` \| `invalid`; NULL when the run didn't refuse |
| `engine_config_gate`  | VARCHAR             | Engine-config gate outcome for this run: `pass` \| `fail` \| `not_applicable` |

### `depot.db`

#### `depot_kv`

Cross-run KV state (`@aq.depot.*`). Every mount is **per-blueprint isolated**
by default, and how depends on the mount's `path`.

A mount with no `path` (the default `default` mount) is routed to its own file
at `.aqueduct/<blueprint_id>/depot.db`. Keys inside it are raw,
because the file already belongs to one blueprint.

A mount with an explicit `path` is one file shared by every blueprint that
names it, so the engine transparently prefixes each key with `<blueprint_id>:`
and two blueprints never collide (you will see rows like `sales:watermark`,
`orders:watermark`).

Configure mounts under `stores.depots` (a name-keyed map); set `shared: true`
on a mount for deliberate cross-blueprint sharing (raw, unprefixed keys), which
requires an explicit `path`: read those via `@aq.depot.<name>.get(...)`. For
parallel writers on a shared mount, use postgres/redis (concurrent), not a
single DuckDB file.

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
to the Surveyor (1.1.0 wired this: pre-1.1.0 rows are NULL by design).
**What to do next** If `module_results` shows `error` but all five
structured fields are NULL on a post-1.1.0 run, check that the executor
populated `ModuleResult.exception` for that module type.

### Heal-loop forensics

**When** you want to see what each LLM turn produced (1.1.0+).
**What you learn** Per-attempt error signature (`error_class`/`where_field`),
token spend, latency, and which gate rejected the attempt.
**What to do next** Repeated identical `(error_class, where_field)` rows mean
the model is stuck; a row with `gate_that_rejected='apply'` means the patch
parsed but failed guardrails: fix the guardrail policy or add prompt context.

```sql
SELECT attempt_num,
       gate_that_rejected,
       error_class,
       where_field,
       tokens_in + tokens_out AS tokens,
       latency_ms,
       stop_reason
FROM heal_attempts
WHERE run_id = '<run_id>'
ORDER BY attempt_num;
```

> `signature_hash` is no longer populated (see the schema reference above);
> `escalated` is the same story (2.85+, C1: write-only, never read by any
> query), the column still exists in the DDL but every write leaves it
> FALSE. Group by `(error_class, where_field, normalized_message)` directly
> instead of a precomputed hash.

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
indicates the `apply_callback` path is bypassed: verify `_check_guardrails`
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

**When** comparing heal outcomes across execution engines, for example after
adding DuckDB support alongside Spark.
**What you learn** Whether one engine is producing more failures, more
rejected attempts, or a lower success rate than the other.
**What to do next** If `duckdb` shows a materially lower
`run_success_after_patch` rate, check `failure_category` and
`gate_that_rejected` for that engine specifically before assuming the model
itself regressed.

```sql
SELECT engine,
       COUNT(*) AS heals,
       SUM(CASE WHEN run_success_after_patch THEN 1 ELSE 0 END) AS succeeded
FROM healing_outcomes
GROUP BY engine;
```

**When** a heal ran with `agent.max_patches > 1` and you want to see how
many attempts the chain walked and which modules it diagnosed, in order.
**What you learn** The chain's per-attempt trail, module diagnosed, attempts
spent on that link, whether it advanced. A chain that never advances past
link 1 despite `max_patches > 1` usually means the model isn't producing a
patch that changes which module fails next (check `gate_that_rejected` on
those rows too).
**What to do next** If the chain repeatedly hits `chain_link` gaps (e.g.
link 1 then link 3 with no link 2), the missing link's own reprompt loop
never persisted a row: check for an exception in the `on_attempt` hook.

```sql
SELECT chain_link, MIN(attempt_num) AS first_attempt, MAX(attempt_num) AS last_attempt,
       COUNT(*) AS attempts_in_link, where_field AS module
FROM heal_attempts
WHERE run_id = '<run_id>' AND chain_link IS NOT NULL
GROUP BY chain_link, where_field
ORDER BY chain_link;
```

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

### Heal engine-config provenance

**When** a Blueprint's `healed_by:` record names a patch and you want the
engine-config diff and perf notes it wrote (the Blueprint record itself no
longer carries them, see the schema reference above).
**What you learn** What each engine key changed to and from, and the
warn-only wall-clock ratio a later green run observed.
**What to do next** Compare against the live `engine.<name>` block, or
run `aqueduct doctor` for the same information rendered as
`healed-config:<patch_id>` rows.

```sql
SELECT patch_id,
       engine,
       engine_version,
       engine_config_delta,
       perf_baseline,
       perf_observations
FROM patch_index
WHERE patch_id = '<patch_id>';
```

### Cost & performance

**When** you want the LLM bill for one heal session.
**What you learn** Total tokens, LLM wall time, attempt count.
**What to do next** Pair with `BudgetConfig.max_tokens_total`,
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

`signature_hash` is no longer populated (see the schema reference above);
group by the fields it used to hash instead:

```sql
SELECT error_class,
       where_field,
       COUNT(*) AS times_hit
FROM heal_attempts
GROUP BY error_class, where_field
ORDER BY times_hit DESC
LIMIT 10;
```

### LLM token cost per blueprint per month

`aqueduct report-costs` (2.85+, D7) aggregates `heal_attempts.tokens_in`/
`tokens_out`, previously stored but unqueryable except as a flat, un-grouped
100-row detail list (`heal_attempt_details()`):

```console
$ aqueduct report-costs --store-dir .aqueduct
  blueprint      month    tokens_in  tokens_out  tokens_total  attempts
  -------------  -------  ---------  ----------  ------------  --------
  my.pipeline    2026-08       4200        1850          6050        18

$ aqueduct report-costs --blueprint my.pipeline --format json
```

### Column lineage

```sql
-- What feeds output column 'my_column'?
SELECT source_table, source_column, channel_id, blueprint_id
FROM column_lineage
WHERE output_column = 'my_column';
```

### Fleet query layer (`stores/queries.py`)

Computed at read‑time from the observability store, no extra write‑side
columns or aggregation tables:

| Function | Returns | Description |
|---|---|---|
| `fleet_summary` | `list[BlueprintSummary]` | Per‑blueprint roll‑up (last run status, success rate, heal count) across all discovered blueprints |
| `runs_over_time` | `list[DayCount]` | Daily run counts over a configurable window (`days` default 30) |
| `failure_categories` | `dict[str, int]` | Count of failures grouped by `error_class` |
| `heal_coverage` | `dict[str, int]` | Heal counts grouped by `healing_outcomes.resolution`. `llm` is the only value written now; older stores may still carry historical `cached`/`replayed` rows |
| `gate_rejection_rates` | `dict[str, int]` | Count of `patch_simulation` rows with `status = 'fail'`, per `gate` (`engine_config`/`lineage`/`sandbox`/`resolvability`). `warn`, `not_applicable` and `unavailable` are not rejections: see the function's docstring for why. Note `unavailable` is not a rejection but *is* blocking for the `sandbox` gate, so a rising `unavailable` count means heals are stalling for humans without any patch being judged wrong; count it separately rather than reading it as health. A rising `resolvability` `warn` count (not counted here, see above) similarly means heals are stalling on missing packages rather than bad patches. Falls back to `heal_attempts.gate_that_rejected` counts when `patch_simulation` is unavailable |

DuckDB: the functions iterate discovered per‑pipeline files. Postgres: a single
schema‑scoped query. Both backends return the same shape.

### Read‑only access

`aqueduct report` and `aqueduct runs` consume these functions to answer
fleet questions on demand; neither runs in the data path. Every read is read‑only: DuckDB
connections issue `SET read_only = true`, and Postgres connections use a
read‑only role.

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
| Cascade tier vs outcome    | `aqueduct runs --cascade`                 |
| LLM cost per blueprint per month | `aqueduct report-costs`             |

**Tip:** DuckDB files are stable; point any DuckDB client at them for
custom dashboards. The `_resolve_obs_db()` helper inside the CLI walks the
per-pipeline directories to find which DB carries a given `run_id`, so the
read-side commands work without specifying a path.
