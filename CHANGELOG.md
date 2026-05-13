# CHANGELOG

---

## v1.0.0a2 — 2026-05-12

### Phase 26b — Secrets Provider Backends
_2026-05-13_

- New `aqueduct/secrets.py` module: `resolve_secret(key, provider, region, resolver) -> str` — unified secrets resolution with `os.environ` fast-path before any remote call
- **`provider: aws`** — fetches from AWS Secrets Manager via `boto3`; JSON blobs auto-unwrapped (key treated as JSON object key inside secret value); result cached into `os.environ`
- **`provider: gcp`** — fetches from GCP Secret Manager via `google-cloud-secret-manager`; short secret names expanded using `GCP_PROJECT` env var; result cached into `os.environ`
- **`provider: azure`** — fetches from Azure Key Vault via `azure-keyvault-secrets` + `DefaultAzureCredential`; vault URL from `AZURE_KEYVAULT_URL` env var; result cached into `os.environ`
- **`provider: custom`** — importlib-loaded callable `(key: str) -> str | None`; no shell/subprocess; path set via `secrets.resolver` in `aqueduct.yml`
- `SecretsConfig`: new fields `region`, `resolver` wired to `AqFunctions.secret()` and `compile()` signature
- `provider_options` rename: `ollama_options` renamed to `provider_options` across `config.py`, `parser/`, `surveyor/llm.py`, `cli.py`, `scenario.py`; `ollama_*` prefixed keys in `provider_options` route to `payload["options"]`; unprefixed keys merge to payload top-level
- `aqueduct doctor`: `check_secrets()` validates provider-specific SDK is importable; clear install hint per provider; custom resolver validated via importlib
- Optional extras in `pyproject.toml`: `aws = ["boto3"]`, `gcp = ["google-cloud-secret-manager"]`, `azure = ["azure-keyvault-secrets", "azure-identity"]`

### Phase 26a — Cluster/Cloud Hardening
_2026-05-13_

- `dir_bytes()` now queries Hadoop FileSystem API via py4j for cloud/HDFS paths (s3a://, gs://, hdfs://, etc.) — restores `bytes_written`/`bytes_read` metrics on any cluster; local paths unchanged
- `aqueduct doctor`: `deployment.env: cluster|cloud` + relative store paths → error (not warn); absolute paths → ok
- `aqueduct run`: warns on stderr when `env: cluster|cloud` but `store_dir` is not an absolute path — flags ephemeral CWD risk before Spark session starts
- `aqueduct.yml.template`: added `deployment.env` field documentation + full cluster deployment example (Standalone + NFS stores + S3A)



### Phase 25b — `partition_filters` on Ingress
_2026-05-13_

- New optional `partition_filters` config key on Ingress modules
- Value is a SQL predicate string; injected as `.where()` immediately after `reader.load()`
- Enables manual partition pruning when Spark's automatic pushdown doesn't trigger (e.g. runtime-resolved paths via Context variables)
- Invalid expressions raise `IngressError` at startup before any data flows
- Applied before `schema_hint` check; does not affect schema metadata

### Phase 25a — Post-Egress Maintenance Hooks
_2026-05-13_

- Optional `maintenance:` block on any Egress module runs Delta Lake OPTIMIZE and/or VACUUM synchronously after the write action
- `maintenance.optimize: true` → `OPTIMIZE delta.\`path\`` (with optional `ZORDER BY` via `zorder_by`)
- `maintenance.vacuum: N` → `VACUUM delta.\`path\` RETAIN N HOURS`
- Both ops are non-fatal: failures log as `WARNING`, pipeline continues
- Timing (`optimize_ms`, `vacuum_ms`) written to `maintenance_metrics` table in `obs.db`
- Compiler warning 8g: `maintenance.optimize: true` on non-delta Egress → emits warning at compile time

### Phase 24a — `error_type` Guardrail System
_2026-05-13_

- Assert rules now accept optional `error_type: <label>` — user-defined typed category for the failure (e.g. `DataQualityViolation`, `SLABreach`)
- `error_type` propagates: `AssertError.error_type` → `ModuleResult.error_type` → `FailureContext.error_type`
- Two new pre-trigger LLM guardrails in `agent.guardrails`:
  - `heal_on_errors: [Label]` — LLM only fires when failure `error_type` matches; empty = no restriction
  - `never_heal_errors: [Label]` — LLM never fires on matching `error_type`; takes priority over `heal_on_errors`
- Matching uses `error_type` label (Assert) OR exception class name from stack trace last line (infrastructure errors)
- `aqueduct doctor` warns when a guardrail entry doesn't match any `error_type` in the blueprint's Assert rules (typo guard)
- `never_heal_errors` takes priority over `heal_on_errors` when both match

### Phase 24b — `metrics_boundary` on Channel
_2026-05-13_

- New opt-in config key `metrics_boundary: true` on any Channel module
- When set, appends `df.repartition(n)` (where `n = df.rdd.getNumPartitions()`, driver-side, no Spark action) to force a Spark stage cut before the Channel's output
- Gives accurate per-module `recordsWritten` metrics from SparkListener — without this, stage fusion groups logical modules into one physical stage, making attribution inaccurate
- LLM self-healing uses `recordsWritten` to detect empty-output failures

### Phase 24c — Sidecar Watermark File
_2026-05-13_

- Eliminates double-scan of upstream DAG for incremental Channels
- **Before**: `MAX(watermark_col).collect()` on the lazy Channel DataFrame → full DAG re-executed before Egress write, then again during Egress write
- **After**: watermark computed from already-written Egress output (`spark.read.<fmt>(path).agg(MAX(col))`) — reads materialized files, not the upstream DAG. For `format: delta`, uses `SELECT MAX(col) FROM delta.\`path\`` which Spark can satisfy via Delta transaction log statistics (metadata-only in many cases)
- Watermark stored in `{store_dir}/watermarks/{blueprint_id}__{channel_id}.json` (atomic rename), Depot also updated for backwards compatibility
- Next run reads sidecar first, falls back to Depot, falls back to `'1900-01-01 00:00:00'` sentinel
- Watermark not advanced if Channel or Egress fails

### Phase 23B — Input Fingerprinting
_2026-05-11_

- `Manifest.inputs_fingerprint` — compile-time snapshot of each local Ingress module's file metadata (`path`, `size_bytes`, `last_modified` ISO-8601 UTC)
- Built during compilation step 6.5 via `os.stat()` — zero runtime cost, no Spark action
- Remote paths (`s3://`, `s3a://`, `gs://`, `hdfs://`, `abfs://`, `wasbs://`) and non-file formats (`jdbc`, `kafka`, `depot`, `dataframe`) store `null` for stat fields (key always present)
- Included in `Manifest.to_dict()` and embedded in `FailureContext` sent to LLM — enables distinguishing data-drift bugs from code bugs during post-mortems

---

### Phase 23C — Incremental Channel (`materialize: incremental`)
_2026-05-11_

- New Channel config keys: `materialize: incremental` and `watermark_column: <col>`
- On each run: last `MAX(watermark_column)` loaded from Depot under key `"{blueprint_id}:{module_id}:_watermark"`, substituted into query as `${ctx._watermark}` at runtime before Spark execution
- First run sentinel: `'1900-01-01 00:00:00'` → full scan; subsequent runs process only new rows
- After successful execution: new watermark written back to Depot; watermark not advanced on failure (safe re-processing)
- Warns at startup if downstream Egress uses `mode: overwrite` (data loss risk)
- `${ctx._watermark}` is a runtime substitution (not a Tier 0 context ref — not listed in `context:` block)

---

## v1.0.0a1 — 2026-05-12



---

### Phase 20 — Audit Cleanup + Production Readiness
_2026-05-07_

**Part A — Audit fixes**
- `surveyor.record(patched=True)` writes `"patched"` status to `run_records` when a self-healing patch succeeds; CLI passes it after successful re-run in `auto` and `aggressive` modes
- Parser `validate_id()` rejects module IDs containing `__` (reserved for Arcade expansion namespace) with a clear `ParseError`

**Part B — Config cleanup**
- `danger:` block in `aqueduct.yml`: `allow_aggressive_patching` (gates `approval_mode: aggressive`) and `allow_full_probe_actions` (gates expensive Probe Spark actions); both default `false`
- Startup warning printed to stderr when any `danger.*` is `true`
- `approval_mode: aggressive` now requires `danger.allow_aggressive_patching: true` or `--allow-aggressive` CLI flag
- `deployment.env: local | cluster | cloud` added to `DeploymentConfig`; Doctor warns on local paths in cluster/cloud mode
- `probes.block_full_actions_in_prod` superseded by `danger.allow_full_probe_actions` (inverted polarity — same effect)

**Part C — New production features**
- Per-pipeline store paths: default `.aqueduct/obs/{blueprint_id}/` instead of shared `.aqueduct/obs.db`; eliminates DuckDB write contention on multi-pipeline setups
- `approval_mode: ci` — fires `POST` to `agent.ci_webhook_url` with full PatchSpec JSON; external CI creates branch + PR; Aqueduct does not couple to any git provider
- `webhooks.on_patch_pending` — fires when a patch is staged to `patches/pending/`; makes `approval_mode: human` viable in team settings
- `webhooks.on_ci_patch` — dedicated webhook for `approval_mode: ci` path
- `schema_hint` additive/subset modes: `{mode: strict|additive|subset, columns: [...]}` dict form; `additive` allows extra upstream columns; `subset` allows missing optional columns; `strict` (default) unchanged
- `patches/pending/` and `patches/rejected/` added to `.gitignore` generated by `aqueduct init`
- `PatchSpec` gains optional `confidence: float`, `category: str`, `root_cause: str` fields — LLM prompted to fill them; `confidence < 0.7` auto-escalates to human review regardless of `approval_mode`
- `healing_outcomes` table in `obs.db` — persists every healing attempt with model, patch_id, confidence, patch_applied, run_success_after_patch
- `record_healing_outcome()` called at every patch decision point (guardrail block, human stage, auto apply, aggressive apply)

**UDFs**
- Java/Scala UDF support: `lang: java|scala`, `jar: path/to.jar`, `entry: com.example.MyClass` — uses `sparkContext.addJar` + `registerJavaFunction`; no cloudpickle dependency
- Python UDF `sys.path` fix: CWD inserted into `sys.path` before UDF import so `module: udfs.taxi_math` resolves correctly when running as installed CLI
- Python 3.13+ cloudpickle patch: if system `cloudpickle >= 3.0` installed, Aqueduct patches PySpark's bundled 2.2.1 at UDF registration time

---

### Phase 19 — Provenance Layer
_2026-05-04_

- `Manifest` + `ModuleManifest` — `provenance: dict[str, ValueProvenance]` records the origin of every resolved config value (context ref, arcade expansion, literal)
- Compiler + expander annotate provenance during resolution; sub-blueprint sources attributed to parent arcade module
- `FailureContext` replaces raw Blueprint YAML with provenance-annotated skeleton for the failed module — LLM prompt is 60-90% smaller and cleaner
- LLM guardrail checks use resolved values from provenance instead of raw patch op fields
- Patch applier translates provenance-aware intent ops into Blueprint YAML mutations
- Doctor command recurses into Arcades: `check_blueprint_sources` now resolves and checks sub-blueprint paths with full context injection

---

### Phase 18 — Git-Integrated Patch Lifecycle
_2026-05-03_

- `aqueduct patch commit --blueprint <path>` — finds applied patches newer than last git commit, builds `---aqueduct---` structured commit message, runs `git add && git commit`
- `aqueduct patch discard --blueprint <path>` — `git checkout HEAD -- <blueprint>`; uncommitted applied patches moved back to `patches/pending/`
- `aqueduct log <blueprint>` — parses git log for `---aqueduct---` blocks; table output with `--format json`; non-aqueduct commits shown as `(manual change)`
- `aqueduct rollback <blueprint> --to <patch_id>` — `git revert --no-edit <hash>`; `--hard` destructive mode requires `"yes"` confirmation
- `aqueduct patch list` — tabular view of pending / applied / rejected patches; `--status` filter
- All patch commands resolve `patches/` from project root via `aqueduct.yml` walk-up
- Run-start warning when uncommitted applied patches exist before `aqueduct run`
- Patch naming scheme: `{seq:05d}_{YYYYMMDDTHHmmss}_{slug}.json`
- New `set_module_config_key` op: surgical dot-notation update inside a module's config; LLM prompt updated to prefer it over `replace_module_config` for single-field fixes
- `FailureContext.doctor_hints` field: populated with `check_blueprint_sources()` warn/fail results; rendered in LLM prompt as "Blueprint issues detected before run"

---

### Phase 17 — Project Scaffold
_2026-05-02_

- `aqueduct init` creates full project scaffold: `blueprints/example.yml`, `aqueduct.yml`, `.gitignore`, `arcades/`, `tests/`, `patches/pending/`, `patches/rejected/`
- Runs `git init` + initial commit when not already in a git repo
- Existing files skipped; missing dirs always created

---

### Phase 16 — Store Layout Cleanup + `aqueduct runs`
_2026-05-02_

- `obs.db` merge: `runs.db` + `signals.db` → single `obs.db`; five tables under one file
- `aqueduct runs [--blueprint] [--failed] [--last N]` CLI command reads `obs.db`
- Store directory defaults unified: `observability.path=".aqueduct"`, `lineage.path=".aqueduct"`, `depot.path=".aqueduct/depot.db"`
- `blueprint_source_yaml` added to `FailureContext`; surveyor reads Blueprint file and populates it
- LLM system prompt: CRITICAL rule — use template expressions from Blueprint source, not resolved literal paths
- `llm_timeout`, `llm_max_reprompts`, `prompt_context` added to `AgentConnectionConfig`
- ruamel deepcopy corruption fix: `_ruamel_copy()` round-trip instead of `copy.deepcopy`
- `obs.db` cells no longer filled with zeros; uncollected metrics store `NULL`

---

### Phase 15 — Probe Signal Types Expansion
_2026-05-02_

- `value_distribution` — min / max / mean / stddev / configurable percentiles per column on sample
- `distinct_count` — approximate distinct-value count via `approx_count_distinct`; all columns in one `agg()` pass
- `data_freshness` — `max(column)` on full scan by default; `allow_sample: true` for speed/accuracy tradeoff
- `partition_stats` — `df.rdd.getNumPartitions()` — pure driver call, zero Spark action, never blocked
- All four types respect `block_full_actions` flag (`partition_stats` always runs); exceptions caught per-signal

---

### Phase 14 — Patch Dry-Run
_2026-05-01_ | **Superseded by aggressive mode redesign**

- ~~`validate_patch` field~~ — **removed**. `aggressive` mode now always validates in-memory (compile + full re-run) before writing. Behavior that required `validate_patch: true` is now the default and non-configurable. Blueprints with `validate_patch` in their `agent:` block will fail with "Extra inputs are not permitted" — remove the field.

---

### Phase 13 — `aqueduct test` Command
_2026-05-01_

- New `aqueduct/executor/spark/test_runner.py` — `run_test_file()`, `TestSuiteResult`, `TestCaseResult`, `AssertionResult`, `TestError`
- Test YAML format: `aqueduct_test: "1.0"`, `blueprint:`, `tests[].{id, module, inputs, assertions}`
- Input schema: `{col: type}` dict → PySpark StructType (supports `long → bigint`, `bool → boolean` etc.)
- Assertion types: `row_count` (exact), `contains` (rows must appear), `sql` (expr over `__output__` view)
- Testable modules: Channel, Junction, Funnel, Assert — Ingress/Egress raise `TestError`
- `aqueduct test <file>` CLI command with `--blueprint`, `--config`, `--quiet`; exit 1 on failure

---

### Phase 12 — Assert Module
_2026-05-01_

- Docs, CLI, and example pass: `docs/specs.md` Assert section, `aqueduct.template.yml` Assert block, `examples/comprehensive_demo/blueprint.yml` updated
- Signal overrides table added to `obs.db`; `evaluate_regulator()` checks it before probe signals

---

### Phase 11 — SQL Macros
_2026-05-01_

- Compile-time SQL macros: `{% macro name(params) %}...{% endmacro %}` defined in Blueprint; expanded before Spark sees SQL
- Parameterized macros: `{{ name(arg) }}` call syntax resolved in Channel `query` fields
- New `aqueduct/compiler/macros.py` — `expand_macros(sql, macros_dict)` pure function

---

### Phase 10 — CLI Commands + `op:join`
_2026-05-01_

- `aqueduct report` — tabular summary of last N runs from `obs.db`
- `aqueduct lineage <blueprint>` — renders column-level lineage from `lineage.db`; accepts blueprint file path; deduplicates output
- `aqueduct signal <module_id> <signal>` — persistent gate overrides via `signal_overrides` table; `--clear` to remove
- `aqueduct heal <blueprint>` — manually trigger LLM patch generation for last failed run
- Channel `op: join` — SQL-level join between two upstream modules with `broadcast` hint support

---

## v1.0.0a0 — 2026-04-27

Alpha release for PyPI (`aqueduct-core`). Phases 1–9.

- Project Journal consolidated into root CHANGELOG.md
- Build configuration optimized for PyPI distribution (tests and examples excluded)
- Package metadata updated with author and contact information

### Phase 9 — Sub-DAG Execution, LLM Guardrails & Patch Rollback
_2026-04-28 – 2026-05-01_

- `aqueduct run --from <module_id>` / `--to <module_id>` — partial DAG execution via BFS reachability; excluded modules recorded as `status: skipped`
- `aqueduct run --execution-date YYYY-MM-DD` — logical execution date replaces `date.today()` in `@aq.date.*` functions; enables backfill runs
- LLM guardrails: `AgentConfig.allowed_paths` (fnmatch patterns) + `AgentConfig.forbidden_ops` — violations stage patch to `patches/pending/`, halt self-healing loop
- `aqueduct patch rollback <patch_id>` — restores Blueprint from backup atomically; moves applied record to `patches/rolled_back/` with `rolled_back_at` timestamp
- Approval mode refactored: `disabled | human | auto | aggressive` with full run→patch→re-run loop in `cli.py`; `max_patches_per_run` wired for aggressive mode
- Agent connection config (`provider`, `base_url`, `model`) now in `aqueduct.yml`; policy (`approval_mode`, `on_pending_patches`) stays in Blueprint

---

### Phase 8B — Doctor, Assert, SparkListener, Executor Isolation
_2026-04-20 – 2026-04-24_

- `aqueduct doctor` — 7 independent checks (config, depot, observability, lineage, LLM reachability, Spark version, blueprint sources); `--blueprint` flag adds Ingress/Egress path checks and format/extension mismatch detection
- Assert module — `aqueduct/executor/spark/assert_.py`: 7 rule types (`min_rows`, `max_rows`, `null_rate`, `freshness`, `sql`, `schema_match`, `distinct_count`); three-phase eval (schema → batch aggregate → row-level); `on_fail`: abort / warn / webhook / quarantine / trigger_agent
- `check-config` CLI command — validate `aqueduct.yml` schema without running
- SparkListener metrics wired: `AqueductMetricsListener.onStageCompleted` captures `records_read/written`, `bytes_read/written`, `duration_ms` per module; persisted to `module_metrics` table
- Egress `register_as_table` — write to Spark/Hive metastore as external table
- LLM inference refactored to pure HTTP (`httpx`); `anthropic` SDK removed as dependency
- All Spark code isolated into `aqueduct/executor/spark/` subpackage; `pyspark` never imported outside it
- Per-module `on_failure` — each module can override the blueprint-level `retry_policy`
- Opt-in checkpoint/resume — `checkpoint: true` on module or blueprint; `--resume <run_id>` re-reads materialized Parquet; manifest hash guard on mismatch

---

### Phase 8A — Resilience, Lineage & LLM Self-Healing
_2026-04-19_

- `RetryPolicy` execution: `_with_retry()` wraps all 5 dispatch sites (Ingress, Channel, Junction, Funnel, Egress); exponential / linear / fixed backoff + jitter + `deadline_seconds`
- `max_attempts` default changed from 3 → 1; Egress `mode: append` + `max_attempts > 1` emits compile warning
- Column-level lineage writer: `aqueduct/compiler/lineage.py` uses sqlglot to extract source-table mappings; written to `lineage.db` (DuckDB) after successful run
- LLM self-healing: `aqueduct/surveyor/llm.py` — `trigger_llm_patch()` full loop: `FailureContext` → LLM API → `PatchSpec` validation (up to 3 re-prompts) → auto-apply or stage for human review
- Supports Anthropic, Ollama (`/api/chat` streaming), and OpenAI-compatible providers
- `approval_mode` default: `auto` → `disabled`; LLM only fires when explicitly configured
- `on_pending_patches: ignore | warn | block` — suppresses LLM when unreviewed patches exist
- Arcade `required_context` validation — missing context keys at expand time raise `ExpandError`
- Patches dir now relative to blueprint file (not CWD)
- `ruamel.yaml` round-trip preserves comments and key order after patch apply

---

### Phase 7 — Engine Hardening
_2026-04-18 – 2026-04-19_

- Open format passthrough — Ingress/Egress `SUPPORTED_FORMATS` whitelists removed; all formats passed verbatim to Spark DataFrameReader/DataFrameWriter
- Spillway (error row routing) — post-Channel split: bad rows get `_aq_error_module`, `_aq_error_msg`, `_aq_error_ts` columns; stored in `frame_store` as separate key
- Depot KV store — `DepotStore` (DuckDB-backed); `get/put/close`; `@aq.depot.get()` de-stubbed; `_last_run_id` written after each run
- Egress `format: depot` — writes scalar value to Depot instead of Spark path
- UDF registration — `aqueduct/executor/udf.py`; `register_udfs(udf_registry, spark)` called before module loop; Python UDFs via `importlib`

---

### Phase 6E — Comprehensive End-to-End Example
_2026-04-18_

- `examples/comprehensive_demo/` — 9-module blueprint exercising all module types, S3A/MinIO config, date-partitioned output; `generate_data.py` script with intentional data quality issues

---

### Phase 6D — Probe Executor
_2026-04-17_

- `aqueduct/executor/probe.py` — `execute_probe()`; signal types: `schema_snapshot`, `row_count_estimate` (sample + spark_listener stub), `null_rates`, `sample_rows`
- Probe modules excluded from topo-sort and appended at end; per-signal exception isolation; writes to `signals.db`

---

### Phase 6C — Junction + Funnel Executor
_2026-04-17_

- Junction — `conditional` (filter + `_else_` NOT), `broadcast` (same plan to all branches), `partition` (partition_key = value filter)
- Funnel — `union_all`, `union` (+ distinct), `coalesce` (row-aligned via `monotonically_increasing_id`), `zip` (unique column names required)
- Branch keys stored as `f"{junction_id}.{branch_id}"` in `frame_store`

---

### Phase 6B — Configuration Loading + Remote Spark
_2026-04-17_

- `aqueduct/config.py` — Pydantic v2 `AqueductConfig`; implicit lookup (no error on missing), explicit path (error on missing)
- `make_spark_session()` accepts `master_url`; supports `local[*]`, `spark://host:port`, `yarn`, `k8s://...`
- Blueprint `spark_config` wins over engine-level `spark_config` (per spec §10.3)

---

### Phase 6A — Patch Grammar (Manual)
_2026-04-16_

- `PatchSpec` — Pydantic v2 discriminated union on `op`; 10 operation types; `extra="forbid"`; `model_json_schema()` ready for LLM context
- `apply_patch_to_dict()` — operates on raw YAML dict; atomic write via `tmp + os.replace`; post-patch re-parse validates result
- `aqueduct patch apply <patch_file> --blueprint <blueprint.yml>` and `aqueduct patch reject <patch_id>` CLI commands

---

### Phase 5 — Mock Surveyor
_2026-04-16_

- `aqueduct/surveyor/` — `Surveyor(manifest, store_dir, webhook_url)` class
- `start()` opens `runs.db` (DuckDB), creates `run_records` + `failure_contexts` tables, inserts `status='running'`
- `record()` updates row to final status; on failure inserts `FailureContext`, fires webhook in daemon thread
- `fire_webhook()` — `urllib.request` POST in daemon thread; errors logged, never raised

---

### Phase 4 — SQL Channel
_2026-04-16_

- `aqueduct/executor/channel.py` — `execute_sql_channel()`: registers each upstream as Spark temp view by module ID; single-input also registered as `__input__` alias; temp views dropped in `finally` block
- Channel dispatch added to executor between Ingress and Egress

---

### Phase 3 — Ingress / Egress Executor
_2026-04-16_

- `aqueduct/executor/` — `ModuleResult`, `ExecutionResult` frozen dataclasses; `make_spark_session()` factory
- `read_ingress()` — lazy DataFrame; formats: parquet / csv / json; `schema_hint` validated against schema
- `write_egress()` — `.save()`; formats: parquet / csv / delta; modes: overwrite / append / error / ignore
- `execute()` — Kahn's topo-sort → Ingress reads → Egress writes; fail-fast on error
- `aqueduct run <blueprint.yml>` CLI command added

---

### Phase 2 — Compiler + CLI Wiring
_2026-04-16_

- `aqueduct/compiler/` — `Manifest` frozen dataclass; `resolve_tier1()` for `@aq.date.*`, `@aq.run.*`, `@aq.depot.*`, `@aq.secret()`
- Arcade expansion: loads sub-Blueprints, namespaces IDs, rewires parent edges
- Probe validation, Spillway edge validation, passive Regulator compile-away
- `aqueduct compile <blueprint.yml>` outputs resolved Manifest JSON

---

### Phase 1 — Parser
_2026-04-16_

- `aqueduct/parser/` — `Blueprint`, `Module`, `Edge`, `ContextRegistry`, `RetryPolicy`, `AgentConfig` frozen dataclasses
- Pydantic v2 schema with `extra="forbid"` at every level; Tier 1 (`@aq.*`) tokens pass through untouched
- Tier 0 resolution: `${ENV:-default}`, `${ctx.key}` cross-refs, `AQUEDUCT_CTX_*` env overrides, profile overrides, CLI overrides
- Kahn's cycle detection + topological order + spillway edge validation
- `aqueduct validate <blueprint.yml>` CLI command
