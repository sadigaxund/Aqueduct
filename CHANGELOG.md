# CHANGELOG

---

### feat(doctor): collapse noise + reword agent line; fix pyspark sql_ctx warning
_2026-05-17_

- **Spark check split into reachability (default) + `--preflight` (full).**
  Default is now a fast, bounded TCP probe of the master host:port (+ S3A
  endpoint) — no SparkSession, ~3s, no slow-vs-broken ambiguity, no false
  "master unreachable?" timeout. `--preflight` builds a real session with
  the actual `spark_config`, runs a task, checks version + storage, and is
  **unbounded** (waits out cold-start / jar shipping; Ctrl-C to abort).
  Removed the misleading 45s `SPARK_PROBE_TIMEOUT` / `ThreadPoolExecutor`
  path. `--skip-spark` unchanged.

- **`doctor` default view shows only actionable rows** (ok / warn / fail).
  `skip` rows (not-applicable like local-mode `cluster-stores`, or
  not-configured like `webhook` / `storage`) collapse into one
  `· skipped: a, b, c` line. `--verbose` restores the full per-row list.
- **`agent` warn line reworded** — `configured provider=anthropic;
  ANTHROPIC_API_KEY not set — set it, or switch agent.provider to
  openai_compat (Ollama/vLLM/LM Studio). Self-healing only; pipeline runs
  fine without it.` Old text implied anthropic was the only option and
  read like a hard failure.
- **`explain_gate._formatted_plan`**: prefer `df.sparkSession` over
  `df.sql_ctx` — accessing `.sql_ctx` (even via `hasattr`) fired pyspark's
  `DataFrame.sql_ctx is an internal property … use DataFrame.sparkSession`
  UserWarning and would break on pyspark removal.

---

### feat(cli): unified, transparent `.env` loading across every command
_2026-05-17_

`.env` auto-load (`_resolve_and_load_env`) was wired into only `run` /
`doctor` / `validate`. `stores info`, `stores migrate`, `patch preview`,
`test`, `report`, `runs`, `lineage`, `signal`, `heal`, `benchmark` skipped it
→ those required a manual `source .env` while the other three did not. Cause:
the loader was per-command copy-paste, not centralized — easy to forget on
new subcommands. Reported via `stores info --config=aqueduct.yml` failing
with `Missing environment variables` on the cluster showcase.

---

### fix(parser): resolve Tier-0 tokens in `spark_config` + `macros` (ISSUE-027)
_2026-05-16_

`parser.parse` wrapped module `config` / `context_override` in
`resolve_value()` but passed top-level `spark_config` and `macros` verbatim.
`${ENV:-default}` / `${ctx.*}` in those blocks were never substituted — Spark
and the macro engine do no var expansion, so e.g.
`spark.jars.packages: "${MY_PKG:-org.example:pkg:1.0}"` reached Spark literally
→ `IllegalArgumentException` on Maven coordinates.

- `spark_config` / `macros` now resolved via `resolve_value(dict(...),
  ctx_map)` in `parser/parser.py` — parity with module config. `{{ param }}`
  macro placeholders untouched (not `${...}`); `@aq.*` / reserved
  `${ctx._watermark}` still deferred.
- Resolution wrapped in `try/except ValueError → ParseError` (was outside any
  guard — a bad `${ctx.*}` escaped as raw `ValueError`, not `ParseError`).

---

### fix(parser): preserve `${ctx._watermark}` through parse/compile
_2026-05-16_

`materialize: incremental` Channels reference `${ctx._watermark}`, which the
Executor substitutes per-run (Depot value, or sentinel `1900-01-01 00:00:00`
on first run). The Tier-0 resolver resolved all `${ctx.*}` at parse time and
raised `Undefined context reference: ${ctx._watermark}` — so any incremental
Blueprint authored the documented way failed `aqueduct validate` **and**
`aqueduct run` (compile). It only worked in executor unit tests that
hand-build a Manifest and bypass the Parser.

- Added `_RESERVED_DEFERRED = {"_watermark"}` in `parser/resolver.py`.
  `_sub_ctx` now preserves a reserved token verbatim (like Tier-1 `@aq.*`)
  instead of raising, so it survives into the Manifest query for the
  Executor's runtime substitution.
- Surfaced by new gallery snippet `19_depot_incremental`.

---

## v1.0.0a2 — 2026-05-12

### CLI cleanup — unified env resolution + validate/doctor by header
_2026-05-15_

Pre-release consistency pass. No new behaviour — same checks, coherent
surface.

- **`.env` resolution unified.** One helper drives `run` / `doctor` /
  `validate`: first existing of `--env-file` → `<input-file dir>/.env`
  → `<cwd>/.env`, first file wins (no stacking), existing env vars never
  overwritten. The "loaded N variable(s)" line is now `aqueduct -v`
  (DEBUG) only — was default-verbosity noise, and `validate` /
  `check-config` previously did not load `.env` at all (false-negative
  on configs `run` accepted).
- **`check-config` removed; folded into `validate`.** `aqueduct
  validate <file>...` detects each file by its version header
  (`aqueduct:` → Blueprint, `aqueduct_config:` → engine config),
  accepts multiple files, and validates each accordingly. No argument →
  validates `./aqueduct.yml`.
- **`doctor` takes a positional file, not flags.** `--config` /
  `--blueprint` dropped; `aqueduct doctor <file>` sniffs the header
  (config / blueprint / aqtest / aqscenario). No argument → checks
  `./aqueduct.yml`, announced explicitly (no silent magic).
- Two-tier model documented in CLI_REFERENCE: `validate` = static /
  offline / CI gate; `doctor` = live / online / pre-deploy.
- **Agent prompt:** soft macro-preservation hint. When the failing
  Blueprint defines `macros:`, the prompt now lists them and nudges the
  model to keep `{{ macros.NAME }}` references instead of inlining
  expanded SQL (macros are one-way compile-time substitution — no
  provenance links expanded SQL back to source). Best-effort, not
  enforced.

BREAKING: `aqueduct check-config` removed (use `aqueduct validate`);
`aqueduct doctor --config` / `--blueprint` removed (pass the file
positionally).

---

### Rename — `llm` → `agent` across user-facing + internal surface
_2026-05-15_

Pre-v1.0 clean break (no deprecation aliases — stability policy applies
from v1.0.0). The self-healing subsystem is consistently "the agent"
everywhere now.

- **Config**: `agent.llm_timeout` → `agent.timeout`,
  `agent.llm_max_reprompts` → `agent.max_reprompts` (engine +
  per-blueprint). Old keys → `ConfigError` (`extra="forbid"`).
- **Module**: `aqueduct/agent/__init__.py` → `aqueduct/agent/__init__.py`.
  `generate_llm_patch` → `generate_agent_patch`, `_call_llm` →
  `_call_agent`, `LLMPatchResult` → `AgentPatchResult`,
  `_llm_usable` → `_agent_usable`.
- **Doctor**: check id `llm` → `agent`; `check_llm` → `check_agent`.
- **Pytest marker**: `@pytest.mark.llm` → `@pytest.mark.agent`;
  `pyproject.toml` marker + CI `pytest -m "not ... and not agent"`.
  Test files `test_llm.py` → `test_agent.py`,
  `test_llm_integration.py` → `test_agent_integration.py`.
- Templates, spec §8.7 + examples, TEST_MANIFEST updated.

`provider`, `base_url`, `model`, `provider_options` already lived under
`agent:` and were not LLM-prefixed — unchanged. PyPI keyword "llm"
retained for search discoverability.

---

### Phase 30b — Stability Contract for v1.0
_2026-05-15_

Freeze the consumer-facing surface so v1.0 means "downstream tooling can
build against this without fear."

- **`aqueduct/exit_codes.py`** new module — six stable integer constants
  (`SUCCESS=0`, `CONFIG_ERROR=1`, `DATA_OR_RUNTIME=2`, `HEAL_PENDING=3`,
  `VALIDATION_GATE=4`, `USAGE_ERROR=5`). Documented in
  `docs/STABILITY.md`; CI wrappers and Airflow operators can `case $?` /
  `result.returncode == aqueduct.exit_codes.X`.
- **`aqueduct schema --target {blueprint,config,patch}`** new CLI
  command emits the Pydantic-derived JSON Schema. Unlocks IDE
  autocomplete + CI gate without exposing internal model classes.
  `-o <file>` to write, `-` for stdout.
- **`--format json`** added to `aqueduct runs` and `aqueduct patch list`
  (`aqueduct patch preview` already had it from Phase 29a). Output is
  the v1.0 contract for machine consumers.
- **Public API freeze** — `aqueduct/__init__.py` documents the contract
  surface and the (intentionally small) `__all__`: `parse`, `ParseError`,
  `AqueductWarning`, `__version__`. All other names live under
  subpackages and are not part of v1.0.
- **README "Versioning & Stability" section** — exit-code table,
  stable surface list, deprecation policy (one-minor-release warning
  then removal). Placed between Development and License so contributors
  see the contract on landing.

Pre-v1.0 reminder: the policy doc applies starting at `v1.0.0`. During
alpha / RC, breaking changes can land in any patch.

---

### Phase 30a — Extended Spark Warnings + Suppression Registry
_2026-05-15_

Two-tier modular warning system on top of the pre-existing 8a–8g
performance diagnostics, plus standardized `AQ-WARN [rule_id]` output so
users can copy IDs straight into a suppression list.

**Tier 1 — compile-time** (pure static analysis on the compiled Manifest,
zero runtime cost):
- `kafka_checkpoint_stale` — Channel with `checkpoint: true` fed by a
  Kafka Ingress freezes the second consumer's stream view to a single
  micro-batch.
- `nondeterministic_fanout` — multi-consumer Channel whose SQL uses
  `rand()` / `uuid()` / `current_timestamp()` without a Checkpoint
  upstream → each branch re-evaluates the function independently.
- `count_col_likely_count_star` — `COUNT(col)` silently skips NULLs;
  flag for review unless the user actually meant null-aware counting.
- `file_format_no_repartition` — Egress `parquet` / `json` / `csv`
  without `repartition` / `coalesce` / `partition_by` produces one tiny
  file per task partition.
- `jdbc_missing_partition` — JDBC Ingress without `partitionColumn` +
  `lowerBound` + `upperBound` (or `predicates`) reads through a single
  executor connection.

**Tier 2 — session-startup** (one-time, runs once per `SparkSession`,
~ms cost):
- `jar_availability` — scans the Blueprint for `format: kafka` /
  `delta` / `iceberg` / `hudi` and JDBC driver classes, reads
  `spark.sparkContext._jsc.sc().listJars()` once, warns when no JAR
  whose name contains the expected fragment is loaded.

**Modular registry:** rules live in `aqueduct/compiler/warnings/`
(tier 1) and `aqueduct/executor/spark/warnings/` (tier 2). Each rule
file exports `RULE_ID: str` and `def check(manifest, [spark]) ->
list[str]`. Add a rule by dropping a file in the package and appending
to the registry's `RULES` list.

**Standardized output format:**
    AQ-WARN [rule_id] human-readable message
Distinct from Spark's `WARNING:` and Python's
`path:line: Category: msg`. Easy to `grep AQ-WARN` and easy to copy the
bracketed ID into the suppress list.

**Suppression:**
- `aqueduct.yml`:
  ```yaml
  warnings:
    suppress: [count_col_likely_count_star, jdbc_missing_partition]
    silence_all: false
  ```
- CLI: `--suppress-warning <rule_id>` (repeatable) and `--no-warnings`
  (silence everything). CLI flags merge with config.
- `WarningsConfig` Pydantic model + `compile(warnings_suppress=...,
  warnings_silence_all=...)` and `execute(warnings_suppress=...,
  warnings_silence_all=...)` keyword args for library users.
- Process-global default installed via
  `aqueduct.warnings.set_default_suppress()`; falls back when no
  explicit suppress is passed.

**Legacy 8a–8g retrofit:** the pre-existing compiler warnings keep their
behaviour but now use stable rule IDs (`perf_probe_sample_full_scan`,
`perf_incremental_watermark_scan`, `perf_python_udf_row_at_a_time`,
`perf_delta_append_no_partition`, `perf_multi_consumer_no_cache`,
`perf_hadoop_fs_in_options`, `maintenance_optimize_non_delta`,
`delivery_append_retry_dupes`) and emit through the same
`aqueduct.warnings.emit()` channel — all suppressible via the same
mechanism.

**Other:**
- Fixed stray escape-sequence `SyntaxWarning` in
  `executor/spark/executor.py:599` docstring (regex flagged by
  Python 3.12+).
- Existing CLI helper `_compile_with_warnings` now recognises the
  `AqueductWarning` category and renders `AQ-WARN [id] msg`; other
  `UserWarning`s keep the legacy `WARNING:` prefix.

---

### Phase 29b — the explain gate: Post-Patch `explain()` Regression Check
_2026-05-15_

Sits on top of Phase 29a — same surfaces (`aqueduct patch preview`,
auto/aggressive in-flight gates). Compares per-module physical-plan node
counts in the patched Manifest against the most recent baseline, warns on
regression. Warn-only by default; aggressive mode can opt in to blocking.

- **`aqueduct/patch/explain_gate.py`** new module. `capture_plan_snapshot(df)`
  pulls `df._jdf.queryExecution().explainString(ExplainMode.FormattedMode())`
  via py4j (same pattern as `metrics.py:_hadoop_fs_bytes`), counts
  `Exchange` / `BatchEvalPython` / `BroadcastExchange` nodes.
  `run_explain_gate(baseline, after, touched_modules=...)` diffs per
  module — any increase in shuffles / Python UDF nodes or decrease in
  broadcast hints becomes an `ExplainRegression` warning.

- **`observability.explain_snapshot`** new table — rolling per-module baseline. Captured
  by the executor after every successful module run (non-Egress). Surveyor
  prunes to the most recent `keep_last_n` (default 5) rows per
  `(blueprint_id, module_id)` so storage is bounded. Cleanly survives
  brand-new pipelines: the explain gate returns `status="skip"` when no baseline rows
  exist yet.

- **`agent.block_on_explain_regression: bool = false`** new config knob on
  `AgentConnectionConfig` (engine default) and `AgentSchema` (blueprint
  override). Default `false` preserves current behaviour — the explain gate prints
  warnings, never blocks. `true` in aggressive mode rejects any patch with
  a regression and lets the agent retry with a new patch.

- **Executor wiring** — `execute(..., explain_capture: dict | None = None)`
  fills the dict during sandbox runs without persisting (no `observability.db` write);
  real runs persist via `Surveyor.record_explain_snapshot()`. Same capture
  loop drives both sinks. Egress modules are skipped (no DataFrame in the
  frame_store).

- **`_run_patch_gates_inline` returns the explain gate alongside the lineage gate/3** —
  `observability.patch_simulation` audit table now records `gate="explain"` rows. Auto
  mode prints regressions; aggressive mode prints + (optionally) blocks
  per `block_on_explain_regression`.

- **Parser fix** — `patch_validation`, `max_heal_attempts_per_hour`, and the
  new `block_on_explain_regression` were not being wired from the Pydantic
  schema into the `AgentConfig` dataclass. Phase 29a's `patch_validation`
  fell back to engine default in all blueprint overrides; now respected.

- **Spec §8.7 agent matrix, ALL_TABLES `explain_snapshot` + `patch_simulation`
  gate enum**, both templates, CHANGELOG, TEST_MANIFEST updated.

Phase 29b unblocked by Phase 29a (same surfaces, same gate registry).
Catalyst tree walk for plan extraction stays Deferred — formatted-string
parsing is plenty for v1.0.

---

### Phase 29a — Patch Validation Pyramid (lineage + sandbox gates)
_2026-05-14_

Catch broken patches before they touch real data. Builds two new gates on top
of the existing patch lifecycle (the guardrails gate schema + post-apply Parser re-check,
plus the deterministic guardrails from Audit Batch 1).

- **`aqueduct patch preview <file> --blueprint <bp>`:** unified diff of the
  Blueprint YAML + the guardrails gate (guardrails re-check) + the lineage gate (live sqlglot
  lineage impact). With `--sandbox`, also runs the sandbox gate (compile patched
  manifest, drop all Egress modules, inject per-Ingress `LIMIT N`, replay
  through the real executor). `--sample N` controls the limit; `--sample 0`
  = unbounded. `--format json` for machine-readable output.

- **the lineage gate — live lineage impact** (`aqueduct/patch/preview.py`): re-runs
  `sqlglot._extract_sql_lineage` over the patched Blueprint, compares output
  columns of touched modules against the pre-patch downstream consumers, and
  surfaces missing-column warnings. No dependency on a prior successful run
  — works on brand-new pipelines.

- **the sandbox gate — sandbox replay**: compiles the patched Blueprint, captures and
  drops every Egress, optionally rewrites Ingress configs with a new
  `sandbox_limit` marker (consumed by `aqueduct/executor/spark/ingress.py`).
  Real SparkSession; real lazy plan; no sinks written. Egress targets are
  surfaced in the report so reviewers see exactly which writes were skipped.

- **`agent.patch_validation` config knob** (engine + per-blueprint):
  `full_run` (default) keeps the existing behaviour — the lineage gate + the sandbox gate +
  full Spark run, Blueprint written only on full-run success. `sandbox`
  short-circuits the full run when the sandbox gate passes; Blueprint is written
  immediately. Lets `aggressive` mode close patch loops in seconds rather
  than minutes when the operator accepts sample-based confidence.

- **Auto/aggressive integration**: both modes run the lineage gate + the sandbox gate as a
  pre-filter before the in-memory full re-run. A sandbox failure stages the
  patch for human review (auto) or kicks the aggressive loop into a new
  iteration. `patch_validation: sandbox` makes the sandbox gate the only validation
  step.

- **`observability.patch_simulation` table**: every gate evaluation is appended with
  patch_id, gate, status, detail, sample_rows, duration_ms — audit trail
  for "why we accepted/rejected this patch without running the full
  pipeline." Phase 28 store backends carry it automatically.

- **`ingress.py` `sandbox_limit` hook**: when the executor's Ingress sees
  this key in the module config, it wraps `reader.load()` in `.limit(N)`.
  Never set by user-authored Blueprints — only by `run_sandbox_gate`'s
  in-memory manifest rewrite.

- **Templates, spec §8.7, CLI_REFERENCE, ALL_TABLES.md** updated with the
  new `patch_validation` knob and the `patch_simulation` table schema.

Phase 29b (post-patch `explain()` regression check) is **deferred** — see
`TODOs.md` for scope.

---

### Phase 28 — Pluggable Store Backends
_2026-05-14_

Replace DuckDB's single-writer file constraint with a backend abstraction so multiple `aqueduct run` invocations can share observability / lineage / depot state. Default behaviour is unchanged (DuckDB everywhere).

- **`aqueduct/stores/` package** — `ObsStore`, `LineageStore`, `DepotStore` ABCs plus three concrete adapters (`duckdb_`, `postgres`, `redis_`). All adapters share a `RelationalCursor` wrapper that rewrites DuckDB-style `?` placeholders to driver-native `%s` for Postgres on the way in, so call-site SQL stays portable.
- **Config layer split:** `RelationalBackend = Literal["duckdb", "postgres"]` for `obs` / `lineage`; `KVBackend = Literal["duckdb", "postgres", "redis"]` for `depot`. Redis-as-obs / redis-as-lineage is rejected by Pydantic at config-load. SDK-availability check at `load_config()` errors with install hints when `psycopg2` / `redis-py` are missing.
- **Postgres backend:** single database, three schemas (`obs`, `lineage`, `depot`), auto-created idempotently on first connect. Shared DSNs are pool-deduplicated — three matching DSNs in `aqueduct.yml` open one pool.
- **Redis backend (depot-only):** in-memory KV for high-QPS watermark reads. `BackendUnsupportedError` on attempts to use `connect()` for relational queries (defense-in-depth; the Literal split already prevents the misconfiguration upstream).
- **Wired call sites:** `Surveyor.start/record/record_healing_outcome/count_recent_heal_attempts/evaluate_regulator/get_probe_signal/stop`, `aqueduct signal`, `DepotStore`, `compiler/lineage.write_lineage`, `executor/spark/probe.execute_probe`, `executor/spark/executor` `_write_stage_metrics` / `_write_maintenance_metrics` / `_update_metric` all dispatch through `aqueduct.stores.get_stores(cfg)`. Each function retains a default-DuckDB fallback so existing tests and direct programmatic callers keep working.
- **`aqueduct stores` CLI group:** `stores info` prints each store's resolved backend + location label (DSN passwords redacted). `stores migrate --from-duckdb <old.db>` copies depot KV rows into the configured target backend; obs/lineage row migration is deferred to a follow-up (manual route documented in CLI help).
- **`aqueduct doctor`:** new per-store reachability probes via `check_store_backend()` — DuckDB connect + `SELECT 1`, Postgres connect + `SELECT 1`, Redis `PING`. Replaces the legacy `check_depot` / `check_observability` calls.
- **`pyproject.toml`:** new `[postgres]` and `[redis]` extras (`psycopg2-binary>=2.9`, `redis>=5.0`); `[all]` now pulls both.
- **Templates and docs:** `aqueduct.yml.template` + spec §10.1 + `docs/ALL_TABLES.md` updated with the backend matrix and Postgres schema layout.

---

### Phase 27 — Audit Cleanup
_2026-05-14_

Six sequential audit batches reconciling spec ↔ code drift, tightening config validation, hardening security guardrails, and surfacing previously silent failure modes. Each batch shipped independently same day.

#### Audit Batch 6 — Spec sync (final), default model bump, cloudpickle hardening, JSON logs

- **Spec §10.1 — `stores:` block reconciled with code.** Previously listed `stores.observability.path: ".aqueduct"` and `stores.lineage.path: ".aqueduct"` (root-dir style); reality is per-store full file paths under `stores.obs.path` / `stores.lineage.path` / `stores.depot.path`. Spec block rewritten to match `AqueductConfig.stores` (`StoresConfig.obs|lineage|depot`). Closes the final `obs` vs `observability` drift item from audit §2.1. Footnote added flagging `s3 | gcs | adls` backends as deferred per `TODOs.md` Phase 28.
- **Default `agent.model` bumped to `claude-sonnet-4-6`** in `AgentConnectionConfig` (was `claude-3-5-sonnet-latest`). Matches README, spec, and shipped template. Closes audit §2.1 row 9.
- **Cloudpickle patch hardened (`aqueduct/executor/spark/udf.py:_patch_pyspark_cloudpickle`)** — defensive rewrite. Tries every known PySpark cloudpickle import path (`pyspark.cloudpickle`, `pyspark.cloudpickle_fast`, `pyspark._cloudpickle`); verifies the bundled module actually exposes `dumps` / `loads` / `CloudPickler` before patching; emits an explicit, actionable WARNING for each failure mode (system cloudpickle missing / no importable bundled module / module shape changed / version-parse failure). Previous behaviour: silently no-op on any failure → users hit cryptic infinite-recursion UDF crashes at runtime. Now every silent failure mode is surfaced. Closes audit §3.10.
- **Global `--log-format {text,json}` flag** on the CLI root group. `text` (default) preserves the current human-readable output. `json` swaps in a minimal one-record-per-line JSON formatter (`{ts, level, logger, msg}` + `exc` when an exception was logged). Targeted at ops teams shipping logs to Loki / Splunk / Datadog. Implementation: new `_AqueductJsonLogFormatter` duck-type in `cli.py`, applied via `logging.getLogger().handlers.clear() + addHandler(...)` so it overrides any `basicConfig` defaults idempotently. Closes the structured-log item from audit §7.3.

---

#### Audit Batch 5 — README scope, LLM rate-limit, ALL_TABLES reference, robustness cleanup

- **README LLM-autonomy callout (top of file):** new ⚠ block surfacing that the LLM only edits Blueprints when `agent.approval_mode` is `auto` or `aggressive`, that the default is `disabled`, that production should prefer `human` / `ci`, and that `aggressive` additionally requires `danger.allow_aggressive_patching: true`. Restates that guardrails are deterministic at patch-apply time, not prompt-time. Adopted reviewer scans the autonomy guarantees in the first paragraphs instead of having to find §8 of the spec.
- **README "Scope — What Aqueduct Is and Is Not" section:** pulls the boundary statement from spec §13 into a two-column table on the README. Makes it clear up-front that Aqueduct is batch (not streaming), data prep (not ML training), CLI (not scheduler), and LLM-scoped-to-PatchSpec (not autonomous infra agent). Also fixes a stale README mention of the removed `block_full_actions_in_prod` flag.
- **LLM rate-limit / spend-cap (`agent.max_heal_attempts_per_hour`):** new optional integer field on both `AgentSchema` (per-blueprint) and `AgentConnectionConfig` (engine default), with blueprint override winning. When set, the self-healing loop counts rows in `healing_outcomes` within the last 60 minutes and skips further LLM HTTP calls once the cap is reached — emitting a clear `⊘ LLM rate-limit reached` line. Default `None` = unlimited; no behaviour change for existing configs. Implementation reuses the existing `observability.db` (no schema change) via the new `Surveyor.count_recent_heal_attempts()` helper.
- **New `docs/ALL_TABLES.md` reference:** single document inventorying every DuckDB table Aqueduct writes — `run_records`, `failure_contexts`, `healing_outcomes`, `signal_overrides`, `probe_signals`, `module_metrics`, `maintenance_metrics`, `column_lineage`, `depot_kv` — with column types, the Python file that owns each DDL, and example SQL queries. Linked from the README documentation table. Closes audit §2.4.
- **Robustness cleanup:**
  - `aqueduct/compiler/compiler.py` `inputs_fingerprint`: widen the `os.stat()` exception handler from `OSError` to `(OSError, ValueError)` so malformed paths (embedded null bytes, oversized components, URI-shaped strings the remote-scheme list does not catch) record `size_bytes=None` like other unfingerprintable paths instead of bubbling up as a hard compile error.
  - `aqueduct/config.py` `WebhookEndpointConfig` docstring: clarify that `${attempt}` is the *number of failed modules* in the current run (`on_failure` scope), not a 1-based retry counter. The implementation has always populated it that way; the docstring now matches.

---

#### Audit Batch 4 — Doctor per-file flags, compile --show selector, Manifest rationale, cleanup

- **`aqueduct doctor --aqtest <file>` and `aqueduct doctor --aqscenario <file>`:** schema pre-flight on `.aqtest.yml` and `.aqscenario.yml` files. Validates the file's own schema, resolves the `blueprint:` reference, and cross-checks every `tests[].module` / `inject_failure.module` against the referenced blueprint's module IDs. No Spark or LLM call. Reuses `aqueduct.surveyor.scenario.load_scenario` for the scenario path and a lightweight inline parser for aqtest. Flags are additive: any combination of `--blueprint`, `--aqtest`, `--aqscenario` runs in one doctor pass.
- **`aqueduct compile --show {manifest,provenance,inputs,all}`:** selectable output for the compile command. `manifest` (default) preserves the original full-JSON behaviour. `provenance` renders the ProvenanceMap as a per-module table (`key | source_type | original_expression | resolved_value`) and a `# Context` section. `inputs` renders the `inputs_fingerprint` as a per-Ingress table (`module_id | path | size | last_modified`). `all` emits the full JSON followed by both tables. Helpful for debugging "where did this resolved path come from?" without grepping a 2 k-line Manifest JSON.
- **Specs §4.1.1 — "Why a Manifest? Why not run the YAML directly?":** new spec subsection answering a common reviewer question. Tabulates, for each Blueprint construct that needs compile-time resolution (`${ctx.*}`, `@aq.date.*`, `@aq.secret`, `@aq.depot.get`, Arcade refs, macros, passive Regulators, `--execution-date`), why deferred resolution would either break determinism or create a runtime dependency surface that does not belong inside Spark. Also documents that ProvenanceMap and `inputs_fingerprint` exist primarily for the LLM, not the Executor.
- **CLI_REFERENCE.md** clarified — `aqueduct lineage <blueprint>` argument is the Blueprint **file path**, not a bare ID; added rows for the new `doctor --aqtest`, `doctor --aqscenario`, and `compile --show` flags; extended the doctor check table to cover the two new schema pre-flight checks.
- **`surveyor/llm.py` silent `except: pass` sites replaced with `logger.debug(..., exc_info=True)`** — patch-history file load and `patches/rules.md` read. Default runs see nothing; `aqueduct -v` users can now diagnose why a prompt section silently dropped a file.

---

#### Audit Batch 3 — Layer-boundary docs + missing test inventory

- **Layer-boundary policy clarified for `aqueduct/doctor.py`:** all three `pyspark` imports in `doctor.py` (`check_spark`, `check_storage`, `check_cloudpickle`) were already inside function bodies — `import aqueduct.doctor` from a pyspark-less interpreter does not raise. The module is now documented as the single intentional exception to the "spark imports stay inside `executor/spark/`" rule, both in `doctor.py`'s module docstring and in `CLAUDE.md`. No runtime behaviour change.
- **Confirmed `aqueduct doctor` and `aqueduct run` share the same config foundation:** both paths call `aqueduct.config.load_config()`. The early secrets-SDK check added in Audit Batch 2 fires in both, so a misconfigured `secrets.provider` surfaces identically (doctor wraps it in `CheckResult("config", "fail")`, run raises). The existing `doctor:check_secrets` SDK branch is now technically unreachable from `run_doctor` (load_config already raised) but is retained as a defensive check for direct callers.
- **Test manifest backlog:** `tests/TEST_MANIFEST.md` gained a new "Audit Cleanup — 2026-05-14" section plus `[ ] **NEW**` checkboxes inline next to each pre-existing topic that the audit identified as missing coverage. Covers: guardrail-bypass regressions on `replace_module_config` / `insert_module` / `add_probe` / `add_arcade_ref` (Batch 1 §3.1), arcade-expanded `inputs_fingerprint` (§3.7), `MetricsConfig.use_observe` end-to-end (Batch 1), `aqueduct --version` (Batch 2), `DeploymentConfig` Literal validation (Batch 2), `[secrets]` extra + load-time SDK check (Batch 2), template hygiene, and cloudpickle patch fragility (§3.10). Tests still to be written by the test-generation pass.

---

#### Audit Batch 2 — Config tightening, version flag, secrets packaging, spec sync

- **`aqueduct --version` flag:** wires `click.version_option` to `aqueduct.__version__`, which is now sourced from installed package metadata (`importlib.metadata.version("aqueduct-core")`). No more hardcoded literal drifting from `pyproject.toml`. Falls back to `0.0.0+unknown` when run from a source checkout without an editable install.
- **`DeploymentConfig.engine` and `target` typed as `Literal[...]`:** typos in `aqueduct.yml` (e.g. `target: kubernetss`) now fail at config validation instead of at session-creation time. Accepted `engine` values: `spark`, `flink`. Accepted `target` values: `local | standalone | yarn | kubernetes | databricks | emr | dataproc`.
- **Spec §14.1 / §14.2 / §14.4 / §14.10 reconciled with code:** spec previously documented a top-level `deployment_env:` key, a nested `spark.master/config:` block, and a non-existent `store_dir:` field. Real schema is `deployment.env`, `deployment.master_url`, top-level `spark_config:`, and per-store `stores.{obs,lineage,depot}.path`. Spec now matches what the Pydantic model actually accepts. Invented `AQ_SPARK_EXECUTOR_CORES` / `_MEMORY` / `_DRIVER_MEMORY` env vars (never read by code) removed from §14.2.
- **New `[secrets]` extra (`pip install aqueduct-core[secrets]`):** aggregates the existing `[aws]`, `[gcp]`, `[azure]` extras so users who haven't decided on a backend can install all three at once. `[all]` now also includes `[secrets]`.
- **Early secrets-backend SDK check at `load_config()`:** if `secrets.provider` is `aws | gcp | azure` but the matching SDK package isn't importable, `ConfigError` is raised immediately with both granular and aggregate install hints. Previously this surfaced as a generic `ImportError` mid-compile when the first `@aq.secret()` resolved. `aqueduct doctor:check_secrets` retains the same check for offline diagnostics.

---

#### Audit Batch 1 — Security + Config Hygiene

- **Patch guardrail fix (security):** `agent.guardrails.allowed_paths` now enforces against every patch op that can write a `path`/`output_path` config value — `set_module_config_key`, `replace_module_config`, `insert_module`, `add_probe`, and `add_arcade_ref`. Previously only `set_module_config_key` was checked, so an LLM-generated `replace_module_config` could carry a path outside the fnmatch whitelist and bypass the guard. Provenance-based `${ctx.*}` resolution is preserved for all ops. New regression tests in `tests/test_patch/test_guardrails_rollback.py`.
- **Removed dead `_check_guardrails` shadow in `aqueduct/cli.py`** — the real enforcement is in `aqueduct/patch/apply.py`. The CLI-side copy was never reachable from production code paths (the `run` command imports the apply.py version). Existing tests that referenced the dead function were rewritten against the authoritative implementation.
- **Removed dead `probes.block_full_actions_in_prod` flag** — superseded by `danger.allow_full_probe_actions` (inverted polarity) in Phase 20 but the field remained in `ProbesConfig` and the shipped `aqueduct.yml.template`. Users following the template configured a no-op. Field removed; template comment block rewritten to point at the active gate. Existing `aqueduct.yml` files that still set the flag will fail with `extra inputs are not permitted` — delete the key.
- **New `metrics.use_observe` toggle (default `true`):** wraps each Ingress read / Egress write with `DataFrame.observe()` for accurate per-module `records_read` / `records_written`. Set `false` in high-throughput production blueprints to skip the observe() node and avoid the ~5–15% throughput hit from broken whole-stage codegen — SparkListener stage metrics still collected (stage-fusion caveat applies). Wired through `aqueduct.executor.spark.executor.execute(use_observe=...)` and `aqueduct.executor.spark.metrics.observe_df(enabled=...)`.

---

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
- Timing (`optimize_ms`, `vacuum_ms`) written to `maintenance_metrics` table in `observability.db`
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
- `healing_outcomes` table in `observability.db` — persists every healing attempt with model, patch_id, confidence, patch_applied, run_success_after_patch
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
- `aqueduct patch log <blueprint>` — parses git log for `---aqueduct---` blocks; table output with `--format json`; non-aqueduct commits shown as `(manual change)`
- `aqueduct patch rollback <blueprint> --to <patch_id>` — `git revert --no-edit <hash>`; `--hard` destructive mode requires `"yes"` confirmation
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

- `observability.db` merge: `runs.db` + `signals.db` → single `observability.db`; five tables under one file
- `aqueduct runs [--blueprint] [--failed] [--last N]` CLI command reads `observability.db`
- Store directory defaults unified: `observability.path=".aqueduct"`, `lineage.path=".aqueduct"`, `depot.path=".aqueduct/depot.db"`
- `blueprint_source_yaml` added to `FailureContext`; surveyor reads Blueprint file and populates it
- LLM system prompt: CRITICAL rule — use template expressions from Blueprint source, not resolved literal paths
- `llm_timeout`, `llm_max_reprompts`, `prompt_context` added to `AgentConnectionConfig`
- ruamel deepcopy corruption fix: `_ruamel_copy()` round-trip instead of `copy.deepcopy`
- `observability.db` cells no longer filled with zeros; uncollected metrics store `NULL`

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
- Signal overrides table added to `observability.db`; `evaluate_regulator()` checks it before probe signals

---

### Phase 11 — SQL Macros
_2026-05-01_

- Compile-time SQL macros: `{% macro name(params) %}...{% endmacro %}` defined in Blueprint; expanded before Spark sees SQL
- Parameterized macros: `{{ name(arg) }}` call syntax resolved in Channel `query` fields
- New `aqueduct/compiler/macros.py` — `expand_macros(sql, macros_dict)` pure function

---

### Phase 10 — CLI Commands + `op:join`
_2026-05-01_

- `aqueduct report` — tabular summary of last N runs from `observability.db`
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
- LLM self-healing: `aqueduct/agent/__init__.py` — `trigger_llm_patch()` full loop: `FailureContext` → LLM API → `PatchSpec` validation (up to 3 re-prompts) → auto-apply or stage for human review
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
