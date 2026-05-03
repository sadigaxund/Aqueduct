# Changelog

## [Unreleased] — 2026-05-03

### Phase 18 — Git-Integrated Patch Lifecycle

**Patch lifecycle commands**
- `aqueduct patch commit --blueprint <path>` — finds applied patches newer than last git commit, builds structured `---aqueduct---` commit message, runs `git add && git commit`
- `aqueduct patch discard --blueprint <path>` — `git checkout HEAD -- <blueprint>`; moves uncommitted applied patches back to `patches/pending/`
- `aqueduct log <blueprint>` — parses git log with `---aqueduct---` blocks; table + `--format json` output; shows `(manual change)` for non-aqueduct commits
- `aqueduct rollback <blueprint> --to <patch_id>` — `git revert --no-edit <hash>`; `--hard` destructive mode with "yes" confirmation
- `aqueduct patch reject` now accepts a file path (`patches/pending/00001_*.json`) in addition to bare patch_id slug; derives `patches_dir` from file path grandparent
- `aqueduct patch list` — new command; tabular view of pending/applied/rejected patches; `--status` filter; `--blueprint` or walk-up to `aqueduct.yml` for patches root
- All patch commands (`apply`, `commit`, `discard`, `list`, `reject`) now consistently resolve `patches/` from project root (walk-up to `aqueduct.yml`) instead of blueprint parent dir
- `_patches_root_from_blueprint()` shared helper centralises walk-up logic
- Run-start warning when uncommitted applied patches exist before `aqueduct run`
- Patch naming: `{seq:05d}_{YYYYMMDDTHHmmss}_{slug}.json` structured scheme

**`aqueduct init` — project scaffold (Phase 17)**
- Creates: `blueprints/example.yml`, `aqueduct.yml`, `.gitignore`, `arcades/`, `tests/`, `patches/pending/`, `patches/rejected/`
- `git init` + initial commit when not already in a git repo
- Existing files skipped; missing dirs always created

**Project root detection**
- `aqueduct run` and `doctor --blueprint` walk up from blueprint file to find `aqueduct.yml` (up to 8 levels); `os.chdir(project_root)` before Spark execution so relative YAML paths resolve correctly

**Patch grammar — `set_module_config_key`**
- New `set_module_config_key` operation: surgical dot-notation key update inside a module's config, leaves all other keys intact
- LLM system prompt updated: prefer `set_module_config_key` for single-field fixes; `replace_module_config` only for full restructures and must re-emit all keys

**LLM self-healing improvements**
- System prompt: SQL Channel queries reference upstream module IDs as Spark temp views — `${ctx.*}` is never valid inside SQL strings
- System prompt: garbled column names in a Channel failure → check upstream Ingress `format` for mismatches; Spark silently reads Parquet as CSV without erroring
- Doctor blueprint checks injected as hints into LLM prompt at failure time: `check_blueprint_sources()` runs before patch generation, warn/fail results included as "Blueprint issues detected before run" section

**`aqueduct doctor --blueprint`**
- Checks every Ingress/Egress path in the blueprint (local existence, glob matching, JDBC TCP reachability)
- **Format/extension mismatch detection**: warns when declared `format` doesn't match file extension (e.g. `format=csv` + `*.parquet`) — catches the silent Spark misread scenario
- Relative paths resolved from project root (same walk-up logic as `run`)
- JDBC TCP socket probe (3s timeout)

**`FailureContext` — `doctor_hints` field**
- `doctor_hints: tuple[str, ...]` added; populated at LLM call time with warn/fail blueprint doctor results
- Rendered in LLM user prompt as "Blueprint issues detected before run" section
- Stored in `to_dict()` output (DuckDB observability record)

### Phase 16 — Store Layout Cleanup + `aqueduct runs` + LLM Patch Reliability
- `llm_timeout`, `llm_max_reprompts`, `prompt_context` added to `AgentConnectionConfig` in `config.py`
- `blueprint_source_yaml` added to `FailureContext`; surveyor reads Blueprint file and populates it
- LLM system prompt: CRITICAL rule — use template expressions from Blueprint source, not resolved literal paths
- LLM user prompt: "Original Blueprint YAML" section injected when `blueprint_source_yaml` present
- ruamel deepcopy corruption fix: `_ruamel_copy()` round-trip instead of `copy.deepcopy`
- `aqueduct runs` CLI command: `[--blueprint] [--failed] [--last N]`; reads `obs.db`
- `obs.db` merge: `runs.db` + `signals.db` → single `obs.db`; all store paths now full file paths
- Store directory defaults unified: `observability.path=".aqueduct"`, `lineage.path=".aqueduct"`, `depot.path=".aqueduct/depot.db"`

### Phase 15 — Probe Signal Types Expansion
- `value_distribution`: min/max/mean/stddev + percentiles on sample
- `distinct_count`: approx_count_distinct per column on sample
- `data_freshness`: max(column); full scan by default; `allow_sample: true` for prod safety
- `partition_stats`: df.rdd.getNumPartitions() — zero Spark action; never blocked
- All 4 respect `block_full_actions` flag (partition_stats always runs)


---

## 1.0.0a0 — 2026-04-27

Alpha release preparation for PyPI.

- Project Journal consolidated into root CHANGELOG.md
- Build configuration optimized for PyPI (excluded tests and examples from distribution)
- Metadata updated with author and contact information

# Project Journal

---

## 2026-05-01 — Change 9: Phase 15 — Probe Signal Types Expansion

### Accomplished

**4 new Probe signal types** added to `aqueduct/executor/spark/probe.py`:

**`value_distribution`**
- Computes min / max / mean / stddev / configurable percentiles per column on a random sample
- Defaults to all numeric columns when `columns` is omitted (uses `isinstance(f.dataType, NumericType)`)
- 1 `df.agg()` action for all stats + 1 `approxQuantile` driver call per column (no additional Spark action)
- Blocked when `block_full_actions=True`
- Config: `columns` (optional), `fraction` (default 0.1), `percentiles` (default [0.25, 0.5, 0.75])

**`distinct_count`**
- Approximate distinct-value count per column via `approx_count_distinct` — error < 5%
- Batched into 1 `agg()` on a sample; all columns in one pass
- Blocked when `block_full_actions=True`
- Config: `columns` (optional, defaults to all), `fraction` (default 0.1)

**`data_freshness`**
- Captures `max(column)` — latest timestamp / date seen in the DataFrame
- Full scan by default (sample max is inaccurate); set `allow_sample: true` + `fraction` to trade accuracy for speed
- Blocked when `block_full_actions=True` unless `allow_sample: true`
- Config: `column` (required), `allow_sample` (default false), `fraction` (default 0.1)

**`partition_stats`**
- `df.rdd.getNumPartitions()` — purely a driver call, zero Spark action, never blocked
- Returns `{"num_partitions": N}`
- Config: none

**Also:**
- Fixed `--cov-fail-under=80` removed from default `addopts` in `pyproject.toml`; 80% threshold now enforced explicitly in CI only (running isolated test files no longer fails with exit code 2)
- `docs/specs.md` Probe section rewritten with full signal-type table + production guard note
- `README.md` Probe description updated
- `examples/comprehensive_demo/blueprint.yml` Probe block updated with all 8 signal types
- `.dev/TESTING.md` ⏳ items added for all 4 new signal types (12 test cases)

### Design Notes
- `value_distribution` and `distinct_count` share the same sampling pattern as `null_rates` — consistent UX.
- `data_freshness` intentionally defaults to full scan because max(ts) from a 10% sample can miss the actual maximum by hours in skewed data. `allow_sample: true` is an explicit opt-in for users who accept the tradeoff.
- `partition_stats` has zero Spark cost — useful for diagnosing data skew without triggering any shuffle.
- All 4 new types follow the same failure-isolation pattern: exceptions are caught per-signal, other signals continue.

---

## 2026-05-01 — Change 8: Phases 12-14 + Active Stubs 1-4

### Accomplished

**Phase 12 — Assert module** (already in executor from prior session; this session: docs pass only)
- `docs/specs.md` Assert section verified; `README.md` Assert usage example added; `TESTING.md` checklist complete; `aqueduct.template.yml` Assert block added; `examples/comprehensive_demo/blueprint.yml` updated.

**Phase 13 — `aqueduct test` command**
- New file: `aqueduct/executor/spark/test_runner.py` — `run_test_file()`, `TestSuiteResult`, `TestCaseResult`, `AssertionResult`, `TestError`
- Test YAML format: `aqueduct_test: "1.0"`, `blueprint:`, `tests[].{id, module, inputs, assertions}`
- Input schema: `{col: type}` dict → PySpark StructType via `_parse_datatype_string` (supports `long → bigint`, `bool → boolean` etc.)
- Assertion types: `row_count` (exact match), `contains` (rows must appear), `sql` (expr over `__output__` view)
- Testable: Channel, Junction, Funnel, Assert — Ingress/Egress raise `TestError`
- `aqueduct test <file>` CLI command: `--blueprint`, `--config`, `--quiet`; exit 1 on failure
- Example: `examples/comprehensive_demo/tests.aqtest.yml`

**Phase 14 — Patch dry-run (`validate_patch`)**
- `validate_patch: bool = False` on `AgentSchema`, `AgentConfig`, parser, `Manifest.to_dict()`
- In `aggressive` mode with `validate_patch=true`: patch compiled in memory first; invalid patches staged to `patches/pending/`, loop stops; on-disk Blueprint never touched

**Stubs 1-4 — Wiring previously inert schema fields**

*Stub 1 — `on_exhaustion: alert_only | trigger_agent`*
- `_on_retry_exhausted(exc, policy, module, blueprint_id, run_id, module_results)` helper added to `executor.py`
- All 5 dispatch sites (Ingress, Channel, Junction, Funnel, Egress) replaced bare `return _fail(...)` with `_on_retry_exhausted()` call
- `alert_only`: logs warning, sets `_GATE_CLOSED` in frame_store (or `continue` for Egress), blueprint does not stop
- `trigger_agent`: returns `_fail(..., trigger_agent=True)` — LLM loop fires even if `approval_mode=disabled`

*Stub 2 — `AssertError.trigger_agent` propagation*
- `executor.py` Assert dispatch `except AssertError` now passes `trigger_agent=exc.trigger_agent` to `_fail()`

*Stub 3 — `Regulator on_block=trigger_agent` propagation*
- `executor.py` Regulator dispatch `on_block=trigger_agent` branch now calls `_fail(..., trigger_agent=True)`

*Stub 4 — `probes.block_full_actions_in_prod`*
- `execute_probe()`, `_row_count_estimate()`, `_null_rates()` in `probe.py` all accept `block_full_actions: bool = False`
- `_row_count_estimate` sample path: skips `.count()`, returns `{"blocked": True, "estimate": None}` + warning
- `_null_rates`: skips `.count()` + `.collect()`, returns `{"blocked": True, "null_rates": {col: None}}` + warning
- `execute()` in `executor.py` gains `block_full_actions: bool = False` param
- CLI `run` command threads `cfg.probes.block_full_actions_in_prod` into `execute()`

*`ExecutionResult.trigger_agent` + CLI override*
- `ExecutionResult` gains `trigger_agent: bool = False` field (and `to_dict()` key)
- `_fail()` accepts `trigger_agent=` kwarg
- CLI run loop: computes `effective_mode = approval_mode`; if `result.trigger_agent` and `effective_mode == "disabled"`, escalates to `"human"` — LLM generates + stages patch even when healing is normally off

### Design Notes
- `alert_only` on data-producing modules propagates the `_GATE_CLOSED` sentinel so downstream modules skip cleanly. On Egress (terminal), it just `continue`s — no sentinel needed.
- `trigger_agent` bypasses `approval_mode=disabled` intentionally: an explicit rule asking for healing should always fire at minimum human-review staging.
- `block_full_actions_in_prod` only suppresses the sample-based Spark actions in Probe (`row_count_estimate method=sample`, `null_rates`). The zero-action `schema_snapshot` and `sample_rows` (`df.take()`) paths are unaffected — `take()` is not a full scan and is left to caller discretion.
- Stub 5 (non-env secrets providers) permanently deferred — env injection is the correct pattern; adding boto3/vault SDK dependencies for marginal benefit violates zero-extra-dependency philosophy.

---

## 2026-04-23 — Change 6: SparkListener Wiring + Assert Module + Docs

### Accomplished

**SparkListener wiring** — `AqueductMetricsListener.onStageCompleted` now captures `records_read`, `records_written`, `bytes_read`, `bytes_written`, `duration_ms` per module. `_write_stage_metrics()` helper persists to `module_metrics` table in `signals.db`. `set_active_module()` / `collect_metrics()` wired at all 5 dispatch sites (Ingress, Channel, Junction, Funnel, Egress). `probe.py` `method: spark_listener` now queries `module_metrics` table — no longer a stub.

**Assert module** — new `aqueduct/executor/spark/assert_.py`:
- `execute_assert(module, df, spark, run_id, blueprint_id) -> (passing_df, quarantine_df | None)`
- `AssertError(message, rule_id=None, trigger_agent=False)`
- Three-phase eval: schema_match (zero action) → batch aggregate rules (≤2 Spark actions) → row-level rules (lazy)
- All `min_rows` / `freshness` / `sql` rules batched into one `df.agg()`; all `null_rate` rules share one `df.sample().agg()`
- `on_fail`: abort, warn, webhook, quarantine, trigger_agent
- `executor.py`: Assert dispatch block after Funnel, before Regulator; `"Assert"` in `_SUPPORTED_TYPES`
- `parser/schema.py`: `"Assert"` added to `VALID_MODULE_TYPES`
- `executor/spark/__init__.py`: `AssertError` exported

**Documentation**
- `examples/comprehensive_demo/blueprint.yml`: full rewrite as copy-paste reference covering all module types, all config keys, all edge variants, context/profiles, UDF registry, retry policy, agent config, spark config
- `examples/nyc_taxi_demo/README.md`: implementation tracking guide with architecture diagram, data download commands, schema inspection, 17-step checklist, Assert rule table, verification queries
- `docs/specs.md`: Assert module section added (rule types, performance model, on_fail table); Section 13 added (Engine Scope & Boundaries — batch-only, no ML/streaming, Flink stub, scheduling strategy)

### Design Notes
- Assert is inline (main-port edges), not tap (attach_to) — row-level quarantine requires intercepting data flow.
- Spillway wiring on Assert is optional; missing spillway with quarantine rows logs warning and discards.
- `trigger_agent` on_fail raises `AssertError(trigger_agent=True)` — Surveyor's LLM loop integration is unchanged.

---

## 2026-04-20 — Change 4: Executor Refactor + Docs + Test Robustness

### Accomplished

**Executor spark/ refactor** — Isolated all Spark code into `aqueduct/executor/spark/`. Used `git mv` (preserves history). `executor/__init__.py` re-exports `execute`/`ExecuteError` for backwards compat. `models.py` stays at top level (engine-agnostic). Updated all test imports and monkeypatch paths (`aqueduct.executor.spark.executor`). Updated `cli.py` to use new paths. Verified no stale `from aqueduct.executor.<moved_module>` imports remain anywhere.

**Test robustness** — `AQ_SPARK_MASTER` env var wired into `conftest.py` (default `local[1]`). Both `_spark_is_healthy()` and `spark` fixture now use it. Derby log redirected to `/tmp/`. LLM tests (Ollama) already had `AQ_OLLAMA_URL` skip logic — no changes needed there.

**Docs update** — README installation section updated to show `aqueduct-core[spark]` extras pattern. Base vs spark vs llm extras explained. CLAUDE.md key deps updated (`pyspark`/`anthropic` marked as optional extras). TESTING.md env var table already present. Global memory updated with PyPI distribution and executor architecture notes.

### Design Notes
- `get_executor()` factory deferred — re-export pattern is simpler and keeps backwards compat.
- `AQ_LLM_URL` not introduced — Anthropic unit tests mock `_call_llm`; only Ollama integration tests need a real endpoint (covered by existing `AQ_OLLAMA_URL`).

---

## 2026-04-20 — Change 3: Per-Module on_failure + Checkpoint/Resume

### Accomplished

**Per-module `on_failure`** — `on_failure: dict` was parsed on every `Module` but executor ignored it; all modules used same blueprint-level `retry_policy`. Added `_module_retry_policy(module, manifest_policy) -> RetryPolicy`: returns `RetryPolicy(**module.on_failure)` when set, else falls back to manifest policy. Applied at all 5 dispatch sites (Ingress, Channel, Junction, Funnel, Egress).

**Opt-in checkpoint/resume** — New `checkpoint: bool = False` on `Blueprint`, `Module`, and `Manifest` (schema + models + parser + compiler).
- `_write_checkpoint()` helper: writes DataFrame as Parquet + `_aq_done` marker. Called after each successful Ingress/Channel/Funnel/Junction (with data). Egress: done-sentinel only (data already written to target).
- On resume (`--resume <run_id>`): for each module, if `_aq_done` exists in checkpoint dir, skip execution and reload Parquet into frame_store.
- Manifest hash guard: writes `_manifest_hash` file on first run; warns (non-fatal) on mismatch when resuming.
- CLI: `aqueduct run blueprint.yml --resume <run_id>`.
- Zero overhead when `checkpoint=false` (default).

### Design Notes
- Source staleness ignored — designed for immutable batch sources (Parquet/JSON files).
- Delta rollback deferred — would need storing target table path per-run + `RESTORE TABLE` SQL.
- `on_failure` YAML keys map directly to `RetryPolicy` field names — no renaming layer needed.
- Checkpoint write failure → logged warning, blueprint continues (non-fatal).

---

## 2026-04-19 — Change 2: Patch Lifecycle Fixes + LLM Demo

### Accomplished

**Patch apply `_aq_meta` bug** — `load_patch_spec` in `aqueduct/patch/apply.py` was passing the full staged JSON (including `_aq_meta` metadata) to `PatchSpec.model_validate_json`. PatchSpec has `extra="forbid"` → validation error. Fixed: parse to dict first, `pop("_aq_meta", None)`, then validate.

**ruamel.yaml round-trip** — `apply.py` and `surveyor/llm.py` `_auto_apply` both used `yaml.dump()` which sorts keys alphabetically and strips comments. Replaced with `ruamel.yaml` (added to `pyproject.toml` dependencies). Blueprint comments and key order now preserved after patch apply. Only the patched config block loses inline comments (replaced by plain dict from LLM).

**`approval_mode` default** — was `"auto"` (LLM fires on every failure by default). Changed to `"disabled"`. Blueprints without explicit `agent:` block no longer call LLM. Schema and models updated. `tests/test_parser.py` assertion updated.

**`on_pending_patches`** — new field on `AgentConfig`. Values: `"ignore"` | `"warn"` (default) | `"block"`. `cli.py run` checks `patches/pending/` before execution; `surveyor.py` suppresses LLM when pending patches exist and policy != `"ignore"`.

**`patches_dir` relative to blueprint** — was hardcoded `Path("patches")` (CWD-relative). Changed to `Path(blueprint).parent / "patches"` so patches always land next to the blueprint regardless of CWD.

**Executor exhaustion log** — `_with_retry()` now logs `attempt N/N failed; giving up` on the last attempt (previously silent).

**Ollama native provider** — `aqueduct/surveyor/llm.py` added `_call_ollama_native()` using `/api/chat` endpoint with streaming (`stream: True`). Prints `[aqueduct llm] model thinking....` dots to stderr as tokens arrive. Fixes `ReadTimeout` from non-streaming 120s wait. Model default in integration test changed from `gemma3` → `gemma3:12b`.

**`conftest.py` Ollama default** — `AQ_OLLAMA_URL` now defaults to `http://localhost:11434` when unset; tests skip if that host unreachable. No longer requires env var to be set explicitly to skip gracefully.

**Schema drift demo** — `examples/llm_healing_demo/` rewritten: JSON source with `ReviewDate` field, blueprint SQL references old `review_date`. Spark error suggests correct name. Blueprint cleaned of all hint comments. Channel config corrected to use `op: sql` + `query:` (channel.py expects those keys, not `sql:`).

### Design Decisions
- `on_pending_patches: block` is the recommended production setting; `warn` for dev
- Patches dir co-located with blueprint avoids confusion when running from project root
- LLM only fires when `approval_mode in ("auto", "human")` AND no pending patches (unless `ignore`)

### Open
- Retry default `max_attempts: 3` → `1` (planned, not yet done)
- Compile warning: Egress `mode: append` + `max_attempts > 1` → potential duplicates
- `aqueduct patch apply` `--patches-dir` must be specified manually (no auto-derive from blueprint path)

---

## 2026-04-19 — Phase 8: Resilience, Lineage, LLM Self-Healing

### Accomplished

**Phase A — RetryPolicy + deadline_seconds**
- `aqueduct/parser/models.py`: Added `deadline_seconds: int | None = None` to `RetryPolicy`
- `aqueduct/parser/schema.py`: Added `deadline_seconds: int | None = None` to `RetryPolicySchema`
- `aqueduct/parser/parser.py`: Threads `deadline_seconds` through RetryPolicy construction
- `aqueduct/compiler/models.py`: `to_dict()` now includes all RetryPolicy fields (`backoff_max_seconds`, `jitter`, `transient_errors`, `non_transient_errors`, `deadline_seconds`)
- `aqueduct/executor/executor.py`: Added `_is_retriable()`, `_backoff_seconds()`, `_with_retry()` helpers. All module dispatches (Ingress, Channel, Junction, Funnel, Egress) now wrapped in `_with_retry()`. Backoff strategies: exponential/linear/fixed with optional jitter and deadline check.

**Phase B — Lineage Writer**
- NEW `aqueduct/compiler/lineage.py`: `_extract_sql_lineage()` uses sqlglot to parse SparkSQL SELECT statements and extract per-output-column source table+column mappings. `write_lineage()` writes to `store_dir/lineage.db` (DuckDB, table `column_lineage`). Called automatically after successful execution when `store_dir` is set. Non-fatal — swallows all exceptions.

**Phase C — LLM Self-Healing**
- NEW `aqueduct/surveyor/llm.py`: `trigger_llm_patch()` — full loop: FailureContext → Anthropic SDK → PatchSpec validation (up to MAX_REPROMPTS=3 re-prompts on schema failure) → `_auto_apply()` (write Blueprint atomically + archive) or `_stage_for_human()` (write to patches/pending/). Strips markdown fences from LLM response. Returns PatchSpec or None.
- `aqueduct/surveyor/surveyor.py`: `Surveyor.__init__` now accepts `blueprint_path` and `patches_dir`. `record()` calls `trigger_llm_patch()` on failure when `agent.approval_mode in ("auto", "human")`. LLM errors are non-fatal (logged, not re-raised).
- `aqueduct/executor/executor.py`: Regulator `on_block=trigger_agent` now returns `_fail()` (status=error) instead of silent skip, so Surveyor sees a failure and fires the LLM loop.
- `aqueduct/cli.py`: `Surveyor()` construction now passes `blueprint_path=Path(blueprint)` and `patches_dir=Path("patches")`.

**Phase D — Arcade required_context validation**
- `aqueduct/parser/models.py`: Added `required_context: tuple[str, ...] = ()` to `Blueprint` dataclass
- `aqueduct/parser/parser.py`: Threads `required_context=tuple(validated.required_context)` through AST construction
- `aqueduct/compiler/expander.py`: After loading sub-Blueprint, checks that all `sub_bp.required_context` keys exist in `arcade_module.context_override`. Missing keys → `ExpandError` with clear message listing missing keys.

### Testing
Added ~40 new ⏳ items to `.dev/TESTING.md` across 4 sections (RetryPolicy, Lineage, LLM, Arcade validation).
Updated Regulator trigger_agent test item to reflect new behavior (fail → LLM loop).

### Design Notes
- `_with_retry` uses closure capture of `module` and `spark` — lambdas in executor capture correct loop variables because they execute immediately (not deferred)
- Lineage written after successful execution (not on failure) — so lineage only exists for clean runs
- LLM loop is non-blocking and non-fatal: if Anthropic API is down or key is missing, blueprint still records its failure and webhook fires normally
- Regulator `trigger_agent` → `_fail()` means the blueprint reports error, which triggers the Surveyor's LLM loop on return; cleaner than trying to invoke LLM mid-execution

### Remaining Stubs
1. Secrets providers (aws/gcp/azure/vault) — only `env` works
2. `SparkListener` row count estimate — returns `estimate: null`
3. Absolute deadline (`valid_until: "09:00"`) — needs scheduler integration

---

## 2026-04-19 — Design Discussion: Resilience Layer + Pending Stubs

### Pending Stubs (implement in order)

1. **LLM self-healing loop** (highest priority — the point of the project)
   - `on_block=trigger_agent` in Regulator: currently falls through to skip
   - Surveyor builds perfect `FailureContext` but never calls Anthropic SDK
   - `patch/grammar.py` ready; `patch apply` CLI works; nothing calls it automatically
   - Wire: `FailureContext` → `surveyor/llm.py` → PatchSpec → `apply_patch_to_dict` → re-run

2. **UDF registration** (unblocks SQL with custom functions)
   - `udf_registry` parsed into Manifest but `channel.py` ignores it
   - `executor/udf.py` already written with `register_udfs()`
   - Wire: call `register_udfs(manifest.udf_registry, spark)` before module loop in executor

3. **Retry policy execution**
   - `RetryPolicy` model fully defined in `parser/models.py` + `parser/schema.py`
   - Parsed and stored in Blueprint; executor never reads it — modules just fail immediately
   - Config: `max_attempts`, `backoff_strategy` (exponential/linear/fixed), `backoff_base_seconds`, `backoff_max_seconds`, `jitter`, `on_exhaustion` (trigger_agent/abort/alert_only), `transient_errors` list

4. **Lineage writer**
   - `stores.lineage` path in config; sqlglot installed; nothing writes to `lineage.db`
   - Wire: Compiler already parses SQL via sqlglot — extract column-level lineage, write to DuckDB at compile time

5. **SparkListener row count**
   - `row_count_estimate[method=spark_listener]` in probe.py returns `{"estimate": None}`
   - Real wiring needs JVM bridge (`SparkListener`); complex, low ROI vs. sample method

6. **Secrets providers**
   - Only `provider: env` works; aws/gcp/azure/vault accepted in config but ignored

7. **Arcade `required_context` validation**
   - `compiler/expander.py:168` comment — Arcade `required_context` field parsed but never validated against caller's context at expand time

### Design Discussion: Blueprint-Level Resilience (Checkpointing + Retry)

#### What Spark does natively (for context)
Spark already handles executor/task-level fault tolerance:
- **Stage checkpointing**: Spark breaks a job into stages; stage output written to shuffle files on disk (not HDFS by default). If an executor crashes, Spark re-runs only the lost tasks in that stage, not the full job.
- **`df.checkpoint()`**: Forces materialization to HDFS/S3 and cuts lineage. Used to break very long lineage chains that would OOM the driver if recomputed. NOT a "save progress" mechanism — it's a lineage truncation tool.
- **Task retries**: `spark.task.maxFailures` (default 4) — Spark retries individual tasks on different executors on transient failures (OOM, network).

#### What Spark does NOT handle
- Module-level (Blueprint-level) retries — if `read_ingress()` fails because S3 is down for 30s, Spark just fails the job
- Cross-run state — Spark has no concept of "resume from module N on re-run"
- Data quality gates blocking downstream — that's Aqueduct's Regulator
- Adaptive retry based on failure type (transient vs. schema error vs. logic error)

#### Blueprint-Level Resilience Options

**1. Static module retry (implement RetryPolicy)**
Each module can have a `retry_policy` in its config. Executor catches module-level errors, checks if retriable, sleeps with backoff, re-invokes the module function.
```yaml
- id: raw_orders
  type: Ingress
  config: { format: parquet, path: "s3a://..." }
  retry_policy:
    max_attempts: 3
    backoff_strategy: exponential   # or: linear, fixed
    backoff_base_seconds: 30
    backoff_max_seconds: 600
    jitter: true
    transient_errors: ["AnalysisException: Path does not exist", "java.io.IOException"]
    on_exhaustion: abort            # or: trigger_agent, alert_only
```
Implementation: wrap each module dispatch in executor with a retry loop. Backoff computed in pure Python (no Spark). Module function re-called from scratch each attempt. `transient_errors` matched as substring of exception string.

**2. Blueprint-level checkpointing (cross-run resume)**
After each module succeeds, write its `frame_store` key (the lazy plan) to a materialized Parquet file in `.aqueduct/checkpoints/<run_id>/<module_id>/`. On re-run with `--resume <run_id>`, executor reads checkpoint for already-completed modules instead of re-executing them.

Design questions to resolve:
- Checkpoint = materialized Parquet, so schema is preserved. But it's a Spark action (`.write.parquet()`), which adds latency to every module. Opt-in via `checkpoint: true` on module or blueprint level?
- Resume must use same run_id or a new one? New run_id means depot watermarks don't conflict.
- Checkpoint invalidation: if Blueprint changes between runs, checkpoint is stale. Hash Manifest JSON as checkpoint key?

**3. Dynamic/reactive retry via LLM (trigger_agent)**
Current design: `on_exhaustion: trigger_agent` → send `FailureContext` to LLM → LLM proposes PatchSpec → patch applied → blueprint re-runs from scratch. This is already the Phase 8 plan.

Enhancement: make `trigger_agent` available as `on_block` in Regulator too (data quality failure → LLM proposes SQL fix in the Channel feeding that Probe).

**4. Deadline / validity window**
Not a runtime timeout — user-defined constraint: "if still failing after N seconds from first attempt, give up." Primary use case: daily/hourly batch blueprints where a stale retry hours later produces wrong output (daily report that must land before 09:00 is useless if it lands at 22:00).

Add `deadline_seconds` to `RetryPolicy`:
```yaml
retry_policy:
  max_attempts: 5
  backoff_strategy: exponential
  backoff_base_seconds: 60
  deadline_seconds: 3600    # give up after 1h from first failure regardless of attempts remaining
  on_exhaustion: abort      # or trigger_agent
```
Implementation: executor records `first_attempt_at = time.time()` on first failure. Before each retry, check `time.time() - first_attempt_at > deadline_seconds` → if exceeded, treat as exhausted (same path as max_attempts reached). Pure Python, no JVM complexity.

Two deadline semantics — start with relative, defer absolute:
- **Relative** (implement now): `deadline_seconds` from first failure. Self-contained in executor.
- **Absolute** (future): `valid_until: "09:00"` — wall-clock batch window. Requires scheduler to pass the window in; out of scope until scheduling layer exists.

#### Recommended implementation order
1. **Static retry + deadline** (pure Python; `RetryPolicy` model already exists — just wire executor; add `deadline_seconds` field)
2. **LLM trigger_agent on exhaustion** (Phase 8 natural extension)
3. **Opt-in checkpointing** (design cost/resume trade-off first)
4. **Absolute deadline / valid_until** (needs scheduler integration)

---

## 2026-04-18 — Phase 7: Engine Hardening

### Accomplished

Four sub-phases implemented in one session.

**Phase A — Open Format Passthrough**
- `aqueduct/executor/ingress.py`: Removed `SUPPORTED_FORMATS` whitelist; `format` must be non-empty string (or raises `IngressError`); all other formats passed verbatim to Spark's DataFrameReader. CSV-specific defaults preserved.
- `aqueduct/executor/egress.py`: Removed `SUPPORTED_FORMATS` whitelist; added `depot` parameter; added `format: depot` pseudo-format (writes to DepotStore instead of Spark path, supports `value` or `value_expr`); Spark write exceptions now wrapped in `EgressError`.

**Phase B — Spillway (Error Row Routing)**
- `aqueduct/executor/executor.py`: Changed `_SIGNAL_PORTS = frozenset({"signal"})` — spillway is now a data edge, not a control-only signal port. Post-Channel split: if `spillway_condition` + spillway edge, splits df into good/bad; error_df gets `_aq_error_module`, `_aq_error_msg`, `_aq_error_ts` columns via lazy Spark transforms; stores both keys in frame_store. Warnings logged for mismatched (condition but no edge, or edge but no condition).

**Phase C — Depot KV Store**
- NEW `aqueduct/depot/__init__.py` + `aqueduct/depot/depot.py`: `DepotStore` class with `get(key, default)`, `put(key, value)`, `close()`. Per-call DuckDB connections; graceful on missing file in `get`; creates file+schema on first `put`; UPSERT with `updated_at` timestamp.
- `aqueduct/compiler/runtime.py`: `runtime_prev_run_id()` de-stubbed → delegates to `depot_get("_last_run_id", "")`.
- `aqueduct/cli.py`: Creates `DepotStore` before compile; passes `depot` to `compiler_compile()` and `execute()`; writes `_last_run_id` after each run; calls `depot.close()`.
- `aqueduct/executor/executor.py`: `execute(..., depot=None)` parameter; passes `depot` to `write_egress`.

**Phase D — UDF Registration**
- `aqueduct/parser/models.py`: `udf_registry: tuple[dict[str, Any], ...] = ()` added to `Blueprint`.
- `aqueduct/parser/parser.py`: `udf_registry=tuple(validated.udf_registry)` threaded through.
- `aqueduct/compiler/models.py`: `udf_registry: tuple[dict[str, Any], ...] = ()` added to `Manifest`; included in `to_dict()`.
- `aqueduct/compiler/compiler.py`: `udf_registry=blueprint.udf_registry` in Manifest construction.
- NEW `aqueduct/executor/udf.py`: `register_udfs(udf_registry, spark)` — Python UDFs only (via `importlib`); raises `UDFError` on import failure, missing entry, or unsupported language.
- `aqueduct/executor/executor.py`: Calls `register_udfs(manifest.udf_registry, spark)` before module loop; wraps `UDFError` as `ExecuteError`.

### Testing
Added full Phase 7 test checklist to `.dev/TESTING.md` (passthrough, spillway, depot, UDFs — ~40 new ⏳ items).

### Next Phases (in order)
1. **LLM Integration** — FailureContext → Anthropic SDK → PatchSpec → auto-apply
2. **Cloud Storage & Production Hardening** — S3/GCS/ADLS backends for observability store; secrets provider; retry policy execution

---

## 2026-04-18 — Phase 6.9: Comprehensive End-to-End Example

### Accomplished

**Files created (all under `examples/comprehensive_test/`):**
- `generate_data.py` — boto3 + pandas + pyarrow script; uploads ~1000 orders + ~200 customers (parquet) to MinIO `raw-data` bucket; intentional data quality issues (~5% null amounts, ~3% future dates, ~10% malformed emails)
- `blueprint.yml` — 9-module blueprint exercising all Phase 6 module types: Ingress × 2, Channel × 2, Probe, Regulator, Junction, Funnel, Egress; date-partitioned output path via `@aq.date.today()`
- `aqueduct.yml` — remote Spark master (`spark://<IP ADDRESS>:7077`), full S3A/MinIO config (`hadoop-aws:3.3.4`), resource sizing
- `README.md` — prerequisites, step-by-step run instructions, DuckDB verification queries, known-limitations table

**Key topology decision:** Junction → Channel (via branch port) is NOT supported — Channel dispatch uses `_incoming_main` (port == "main" only, line 218 executor.py). Design changed to Junction → Funnel → Channel, which correctly uses `_incoming_data` for Funnel and `_incoming_main` for the post-merge Channel.

### Phase 6 Limitations Documented

| Feature | Why skipped |
|---|---|
| JDBC ingress/egress (PostgreSQL) | `SUPPORTED_FORMATS` = {parquet, csv, json} / {parquet, csv, delta} |
| Spillway | Signal-port concept, not a module type |
| Depot KV (`format: depot`, `@aq.depot.get()`) | Runtime wiring absent |
| Python UDF via YAML | No `udf_registry` wiring in Channel dispatch |
| Regulator gate with `passed` signal | No signal type emits `passed` → gate always open |

### Observations
- S3A path style access (`path.style.access=true`) required for MinIO; virtual-hosted style is S3-only
- `hadoop-aws:3.3.4` + `aws-java-sdk-bundle:1.12.367` is the correct pairing for Spark 3.5.x
- `@aq.date.today(format='%Y-%m-%d')` inside a double-quoted YAML string with single-quoted format arg should parse correctly (outer `"` allows inner `'`)
- Probe `row_count_estimate` with `method: spark_listener` will write `estimate: null` (stub); documented in README

### What's Next (Phase 7)

LLM Integration — `aqueduct/surveyor/llm.py`:
1. Package `FailureContext` → call Anthropic SDK with `PatchSpec.model_json_schema()` in system prompt
2. Validate response as PatchSpec (up to 3 re-prompts on schema failure)
3. `approval_mode: auto` → apply immediately; `human` → write to patches/pending/
4. On success: apply patch, persist to patches/applied/, re-run blueprint
5. Wire `evaluate_regulator` to a new `quality_check` signal type that emits `passed: bool`

---

---

## 2026-04-17 — Probe Executor

### Accomplished

**Files created:**
- `aqueduct/executor/probe.py` — `execute_probe(module, df, spark, run_id, store_dir)`; signal types: `schema_snapshot` (zero Spark action, writes JSON file + DuckDB), `row_count_estimate` (sample method: `df.sample(f).count()`; spark_listener: stub), `null_rates` (`df.sample(f)` → per-column null counts), `sample_rows` (`df.take(n)`); all exceptions caught per-signal; probe-level exceptions also caught; writes to `store_dir/signals.db`

**Files updated:**
- `aqueduct/executor/executor.py` — `Probe` added to `_SUPPORTED_TYPES`; Probe modules excluded from `_topo_sort` and appended at end (no edges → would land first otherwise); Probe dispatch: uses `attach_to` for frame_store lookup, calls `execute_probe`, swallows all exceptions; `store_dir: Path | None = None` added to `execute()` signature
- `aqueduct/surveyor/surveyor.py` — `get_probe_signal(probe_id, signal_type=None)` added; opens read-only connection to `signals.db`; returns deserialized payload dicts
- `aqueduct/cli.py` — `store_dir=resolved_store_dir` passed to `execute()`

### Decisions Made

| Decision | Rationale |
|---|---|
| Probe signals in separate `signals.db` | Avoid DuckDB single-write-connection conflict with Surveyor's open `runs.db` connection |
| Probe modules excluded from topo-sort, appended at end | Probes have no edges (use `attach_to`); topo-sort would place them first with in_degree=0 |
| Per-signal exception catch | One bad signal type should not block schema_snapshot which has no Spark action |
| `store_dir=None` → probe skips I/O but still records success | Tests that don't care about probes don't need tmp_path setup |

---

## 2026-04-17 — Junction + Funnel Executor Modules

### Accomplished

**Files created:**
- `aqueduct/executor/junction.py` — `execute_junction(module, df) -> dict[str, DataFrame]`; modes: `conditional` (filter + `_else_` NOT logic), `broadcast` (same lazy plan to all branches), `partition` (`partition_key = value` filter, fallback to branch id); `JunctionError`
- `aqueduct/executor/funnel.py` — `execute_funnel(module, upstream_dfs) -> DataFrame`; modes: `union_all` (`unionByName`, strict/permissive schema_check), `union` (union_all + `.distinct()`), `coalesce` (row-aligned join via `monotonically_increasing_id` + `F.coalesce` for overlapping cols), `zip` (row-aligned join, must have unique col names); `FunnelError`

**Files updated:**
- `aqueduct/executor/executor.py` — `_SUPPORTED_TYPES` extended with `"Junction"` and `"Funnel"`; Junction dispatch stores branches as `frame_store["junction_id.branch_id"]`; Funnel dispatch collects all main-port upstreams (handles branch keys like `"jn.branch_eu"`)
- `.dev/TESTING.md` — added `## Junction` and `## Funnel` sections (~40 new ⏳ items)

### Decisions Made

| Decision | Rationale |
|---|---|
| Junction branch keys as `f"{module.id}.{branch_id}"` | Allows downstream edges to reference specific branches; Egress looks up by `edge.from_id` directly |
| Funnel collects upstream_dfs from main-port edges (not from `inputs` config) | Consistency with Channel pattern; `inputs` config list used for ordering inside `execute_funnel` |
| `_else_` with no explicit conditions → unfiltered df | Graceful fallback; avoids SQL `NOT ()` syntax error |
| `monotonically_increasing_id` for coalesce/zip | Lazy, no Spark action; standard Spark idiom for positional join |
| zip mode validates unique column names upfront | Prevents silent column shadowing after join |

### What's Next (Phase 7)

LLM Integration — `aqueduct/surveyor/llm.py`:
1. Package `FailureContext` → call Anthropic SDK with `PatchSpec.model_json_schema()` in system prompt
2. Validate response as PatchSpec (up to 3 re-prompts on schema failure)
3. `approval_mode: auto` → apply immediately; `human` → write to patches/pending/
4. On success: apply patch, persist to patches/applied/, re-run blueprint

---

## 2026-04-17 — Config + Remote Spark

### Accomplished

**Files created:**
- `aqueduct/config.py` — Pydantic v2 `AqueductConfig` with sub-models: `DeploymentConfig`, `StoresConfig`, `AgentEngineConfig`, `ProbesConfig`, `SecretsConfig`, `WebhooksConfig`; all `extra="forbid"`, all frozen; `load_config(path=None)` — implicit lookup (no-error on missing), explicit path (error on missing), empty-file safe

**Files updated:**
- `aqueduct/executor/session.py` — `make_spark_session()` now accepts `master_url: str = "local[*]"`; passes it to `builder.master()`; supports `local[*]`, `spark://host:port`, `yarn`, `k8s://...`
- `aqueduct/cli.py` `run` command:
  - New `--config` option (default: `aqueduct.yml` in CWD)
  - `--store-dir` and `--webhook` now default to `None`; fall back to config then hard defaults
  - Engine-level `spark_config` merged with Blueprint `spark_config` (Blueprint wins)
  - `master_url` sourced from `cfg.deployment.master_url`
  - Log line now shows `master=<url>`

### Decisions Made

| Decision | Rationale |
|---|---|
| Missing implicit config = no error, use defaults | Local dev should work with zero config files; errors only on explicit bad path |
| Blueprint spark_config wins over engine config | Per spec §10.3: "Blueprint config takes precedence" |
| `--store-dir` default changed from `.aqueduct` to `None` | Allows config file to control default without CLI flag interference |

### What's Next (Phase 7)

LLM Integration — `aqueduct/surveyor/llm.py`:
1. Package `FailureContext` → call Anthropic SDK with `PatchSpec.model_json_schema()` in system prompt
2. Validate response as PatchSpec (up to 3 re-prompts on schema failure)
3. `approval_mode: auto` → apply immediately; `human` → write to patches/pending/
4. On success: apply patch, persist to patches/applied/, re-run blueprint

---

## 2026-04-16 — Phase 6: Patch Grammar (Manual)

### Accomplished

Built the complete patch system. Humans (and in Phase 7, the LLM) can now author a PatchSpec JSON and apply it to a Blueprint via CLI.

**Files created:**
- `aqueduct/patch/__init__.py`
- `aqueduct/patch/grammar.py` — Pydantic v2 PatchSpec schema with discriminated union on `op` field; 10 operation types, all with `extra="forbid"`; `PatchSpec.model_json_schema()` ready for LLM context in Phase 7
- `aqueduct/patch/operations.py` — `apply_operation()` dispatch + 10 implementation functions operating on raw YAML dicts; `PatchOperationError` for failed preconditions; `_set_nested()` for dot-notation context keys
- `aqueduct/patch/apply.py` — `load_patch_spec()`, `apply_patch_to_dict()`, `apply_patch_file()`, `reject_patch()`; backup → atomic write (tmp + os.replace) → archive lifecycle

**Files updated:**
- `aqueduct/cli.py` — `patch` group with `apply` and `reject` subcommands:
  - `aqueduct patch apply <patch_file> --blueprint <blueprint.yml> [--patches-dir patches]`
  - `aqueduct patch reject <patch_id> --reason "..." [--patches-dir patches]`

### Decisions Made

| Decision | Rationale |
|---|---|
| Operations work on raw YAML dict, not AST | Need round-trip YAML write; AST has no serializer back to YAML |
| Post-patch re-parse with Parser | Guarantees patched Blueprint is always valid; catches op logic bugs |
| Atomic write via tmp + `os.replace` | Single syscall, no partial-write window on POSIX |
| Archive failure non-fatal | Patch already applied to Blueprint; losing the archive is recoverable |
| Dot-notation for `replace_context_value` key | Consistent with `${ctx.paths.input}` syntax users already know |

### What's Next (Phase 7)

LLM Integration:
1. `aqueduct/surveyor/llm.py` — package `FailureContext` → call Anthropic SDK → validate PatchSpec response → `apply_patch_to_dict`
2. Wire into `Surveyor.record()`: on failure with `approval_mode: auto`, call LLM loop
3. Max 3 re-prompt attempts on schema validation failure
4. On success: write PatchSpec to `patches/applied/`, re-run blueprint

---

## 2026-04-16 — Phase 5: Mock Surveyor

### Accomplished

Built the observability layer. Blueprint failures are now persisted and alertable.

**Files created:**
- `aqueduct/surveyor/__init__.py`
- `aqueduct/surveyor/models.py` — frozen `RunRecord` + `FailureContext` (with `to_dict()`/`to_json()`)
- `aqueduct/surveyor/webhook.py` — `fire_webhook(url, payload, timeout)` — daemon thread POST via `urllib.request`; errors logged to stderr, never raised
- `aqueduct/surveyor/surveyor.py` — `Surveyor(manifest, store_dir, webhook_url)` class:
  - `start(run_id)` — opens `<store_dir>/runs.db` (DuckDB), creates `run_records` + `failure_contexts` tables, inserts `status='running'` row
  - `record(result, exc=None)` — updates row to final status; on failure: inserts `failure_contexts`, fires webhook in daemon thread; returns `FailureContext | None`
  - `stop()` — closes DB connection

**Files updated:**
- `aqueduct/cli.py` — `run` command:
  - `--store-dir` (default `.aqueduct`) and `--webhook` options added
  - `uuid.uuid4()` now generated in CLI (not executor); passed to both `execute()` and `surveyor.start()`
  - `ExecuteError` from executor wrapped into synthetic `ExecutionResult` so surveyor always gets a result
  - `surveyor.record()` called before `spark.stop()`; failure_ctx shown in exit message

### Decisions Made

| Decision | Rationale |
|---|---|
| DuckDB file at `<store_dir>/runs.db` | Satisfies hard rule; single file; MVCC handles concurrent reads |
| `fire_webhook` returns the Thread | Tests can `.join()` to verify delivery without sleep |
| `ExecuteError` → synthetic `ExecutionResult` in CLI | Surveyor always sees a complete result; avoids special-casing in record() |
| `failed_module="_executor"` sentinel | Distinguishes bare executor errors (cycle, unsupported type) from module-level failures |

### What's Next (Phase 6)

Patch Grammar — `aqueduct patch apply <patch.yml>`:
1. `aqueduct/patcher/models.py` — frozen `Patch` dataclass (module_id, field, old_value, new_value)
2. `aqueduct/patcher/patcher.py` — `apply_patch(manifest, patch)` → new Manifest
3. `aqueduct patch apply` CLI command: load patch YAML, apply to Manifest, re-execute

---

## 2026-04-16 — Phase 4: SQL Channel

### Accomplished

Added `op: sql` Channel support. `aqueduct run tests/fixtures/valid_minimal.yml` now works end-to-end.

**Files created:**
- `aqueduct/executor/channel.py` — `execute_sql_channel(module, upstream_dfs, spark, udf_registry=None)`
  - Validates `op: sql` and non-empty `query`
  - Registers each upstream as a temp view by module ID
  - Single-input Channel also registered as `__input__` alias
  - Temp views dropped in `finally` block after SQL executes
  - `udf_registry` param accepted as stub (Phase 5)

**Files updated:**
- `aqueduct/executor/executor.py` — Channel dispatch added between Ingress and Egress; collects `upstream_dfs` dict from `frame_store` via incoming main-port edges; catches `ChannelError` into fail-fast result

### Decisions Made

| Decision | Rationale |
|---|---|
| Drop temp views in `finally` | Clean catalog prevents name collisions across Channel executions in the same blueprint |
| `__input__` alias for single-input only | Multi-input Channels must reference by module ID to avoid ambiguity |
| `udf_registry` stub now, not later | Defines the call signature so tests can verify the param exists; avoids breaking change when UDF support lands |

### What's Next (Phase 5)

Mock Surveyor — on Executor failure, log the FailureContext and optionally call a webhook; no LLM yet:
1. `aqueduct/surveyor/models.py` — frozen `FailureContext` dataclass
2. `aqueduct/surveyor/surveyor.py` — `survey(result, manifest, exc)` logs to DuckDB or file, optional webhook POST
3. Wire into `aqueduct run` CLI: on `result.status == "error"`, call surveyor before exit

---

## 2026-04-16 — Phase 3: Ingress/Egress Executor

### Accomplished

Built the minimal Executor layer (Ingress + Egress only). No Channels, no LLM.

**Files created:**
- `aqueduct/executor/__init__.py`
- `aqueduct/executor/models.py` — frozen `ModuleResult`, `ExecutionResult` with `to_dict()`
- `aqueduct/executor/session.py` — `make_spark_session(blueprint_id, spark_config)` factory; calls `builder.getOrCreate()`
- `aqueduct/executor/ingress.py` — `read_ingress(module, spark)` → lazy DataFrame; formats: parquet/csv/json; `schema_hint` validated via `df.schema` (no Spark actions)
- `aqueduct/executor/egress.py` — `write_egress(df, module)` → `.save()`; formats: parquet/csv/delta; modes: overwrite/append/error/ignore
- `aqueduct/executor/executor.py` — `execute(manifest, spark, run_id)`: Kahn's topo-sort → Ingress reads → Egress writes; fail-fast on error; raises `ExecuteError` for unsupported module types

**Files updated:**
- `aqueduct/cli.py` — added `run` command: parse → compile → `make_spark_session` → `execute` → report per-module status → `spark.stop()`

### Decisions Made

| Decision | Rationale |
|---|---|
| `ExecuteError` raised (not caught) for unsupported module types | Silent skip risks data loss; fast failure forces fix before next phase |
| `frame_store[module.id] = df` keyed by module ID | Enables multi-source DAGs later; Egress looks up upstream by edge `from_id` |
| Signal-port edges excluded from topo-sort | They don't carry DataFrames; mixing them would break in-degree counts |
| `spark.stop()` called in CLI, not in executor | Executor accepts a session it does not own; avoids double-stop on shared cluster sessions |

### What's Next (Phase 4)

Single `op: sql` Channel — register upstream as temp view, run `spark.sql(query)`, pass to downstream:
1. `aqueduct/executor/channel.py` — `execute_channel(module, frame_store, spark)`
2. Extend `executor.py` to dispatch Channel modules
3. Fixture: Ingress → SQL Channel → Egress
4. `aqueduct run tests/fixtures/valid_minimal.yml` should complete end-to-end

---

## 2026-04-16 — Phase 2: Compiler + CLI wiring

### Accomplished

Completed Phase 2 (Compiler) and wired `aqueduct/cli.py` to the full Compiler.

**Files created:**
- `aqueduct/compiler/models.py` — frozen `Manifest` dataclass with `to_dict()`
- `aqueduct/compiler/runtime.py` — `AqFunctions` + `resolve_tier1()` dispatcher; `@aq.date.*`, `@aq.run.*`, `@aq.depot.*`, `@aq.secret()`
- `aqueduct/compiler/expander.py` — Arcade expansion: loads sub-Blueprints, namespaces IDs, rewires parent edges
- `aqueduct/compiler/wirer.py` — Probe validation, Spillway edge validation, passive Regulator compile-away
- `aqueduct/compiler/compiler.py` — orchestrator: Tier 1 resolve → Arcade expand → validate → compile-away → Manifest
- `tests/test_compiler.py` — full compiler test suite

**Files updated:**
- `aqueduct/cli.py` — `compile` command now calls `compiler.compile()` and outputs `manifest.to_dict()` JSON; uses `blueprint_id` key
- `aqueduct/parser/models.py` — added `attach_to`, `ref`, `context_override` fields to `Module`
- `aqueduct/parser/schema.py` — added matching fields to `ModuleSchema`

**Test results:** 82/82 passed, ~91% coverage.

### Decisions Made

| Decision | Rationale |
|---|---|
| Nested `@aq.*` calls resolved innermost-first | Allows `@aq.date.offset(base=@aq.date.today(), days=1)` with quoted inner result |
| Passive Regulators (no signal-port edge) compiled away entirely | Zero runtime overhead for opt-in quality gates that have no wired signal |
| Arcade entry/exit detection via internal-edge analysis | Entry = no incoming internal edge; Exit = no outgoing internal edge |

### What's Next (Phase 3)

Single `op: sql` Channel — build order from CLAUDE.md:
1. `aqueduct/executor/` — `SparkSession` factory, Ingress (read → temp view), Egress (write)
2. SQL Channel: register upstream as temp view, run `spark.sql(query)`, pass to downstream
3. Wire through `aqueduct run <blueprint.yml>` CLI command
4. Test with local SparkSession: Ingress CSV → SQL transform → Egress Parquet

---

## 2026-04-16 — Phase 1: Parser

### Accomplished

Built the complete Parser layer. No Spark, no LLM — pure Python.

**Files created:**
- `pyproject.toml` — project manifest, deps, pytest/ruff/black config
- `aqueduct/__init__.py`
- `aqueduct/cli.py` — `validate` and `compile` commands (Parser only for now)
- `aqueduct/parser/models.py` — frozen dataclasses: `Blueprint`, `Module`, `Edge`, `ContextRegistry`, `RetryPolicy`, `AgentConfig`
- `aqueduct/parser/schema.py` — Pydantic v2 schema; `extra="forbid"` at every level
- `aqueduct/parser/resolver.py` — Tier 0 resolution: `${ENV:-default}` + `${ctx.key}` cross-refs + `AQUEDUCT_CTX_*` env overrides + profile overrides + CLI overrides
- `aqueduct/parser/graph.py` — Kahn's algorithm cycle detection + topological order + spillway validation
- `aqueduct/parser/parser.py` — orchestrator returning immutable Blueprint AST
- `tests/test_parser.py` — 35 tests across 4 classes
- `tests/fixtures/` — valid_minimal, valid_with_profile, invalid_cycle, invalid_schema

**Test results:** 35/35 passed, 80.68% coverage (threshold: 80%).

### Decisions Made

| Decision | Rationale |
|---|---|
| `extra="forbid"` on all Pydantic schemas | Spec requirement: unknown fields are hard errors for LLM safety |
| Tier 1 (`@aq.*`) tokens pass through resolver untouched | Resolver is Parser-only; Tier 1 belongs to the Compiler |
| `detect_cycles` + `validate_spillway_targets` both run before AST is returned | Catch all graph errors in one pass at parse time |
| Context cross-refs resolved iteratively (up to 20 passes) | Handles transitive refs (`A → B → C`) without requiring topo-sort of context keys |
| `EdgeSchema` uses `Field(alias="from")` | `from` is a Python keyword; Pydantic alias handles YAML ↔ Python name mapping |

### What's Next (Phase 2)

Build Ingress/Egress Wrapper — prove Spark I/O works via YAML config:

1. Add `aqueduct/executor/` with a minimal `SparkSession` factory.
2. Implement `Ingress` execution: `spark.read.format(...).load(path)` → named temp view.
3. Implement `Egress` execution: `df.write.format(...).mode(...).save(path)`.
4. Wire through `aqueduct run <blueprint.yml>` CLI command.
5. Test: a Blueprint with one Ingress + one Egress reads a CSV and writes Parquet on local Spark.

No Channels, no Surveyor, no LLM in Phase 2.

---

## 0.1.0 — 2026-04-20

Initial release.

- Declarative YAML Blueprint blueprints for Apache Spark
- Modules: Ingress, Egress, Channel, Junction, Funnel, Probe, Regulator, Arcade
- LLM self-healing loop (Anthropic, Ollama, OpenAI-compatible)
- PatchSpec grammar: structured, auditable LLM patches
- Depot KV store for cross-run state
- Spillway error-row routing
- RetryPolicy with exponential/linear/fixed backoff + deadline
- Column-level lineage writer (sqlglot + DuckDB)
- CLI: validate / compile / run / patch apply / patch reject
