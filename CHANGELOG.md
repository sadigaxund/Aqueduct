# Changelog

All notable, consumer-facing changes to Aqueduct (`aqueduct-core`) are
recorded here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [SemVer](https://semver.org/). The stability contract
applies from v1.0.0 — during alpha/RC, breaking changes may land in any
release and are marked **BREAKING**.

## [1.0.1] — 2026-05-23

### Fixed
- **`aqueduct run` now exits `3` (HEAL_PENDING) when `approval_mode: human`
  or `ci` stages a patch under `patches/pending/`** — previously the
  command exited `1` regardless of why the run failed, so downstream
  tooling (Airflow operator, CI gates) had no way to distinguish
  "needs human approval" from a hard runtime fault. The exit-code
  contract documented in `aqueduct/exit_codes.py` and
  `docs/specs.md §10.7` was not actually emitted by the CLI; this lands
  the missing wiring. Hard runtime fault now correctly exits `2`
  (DATA_OR_RUNTIME). Exit `1` is reserved for config / parse errors.
- **Tier-0 resolution now applied to the Blueprint `agent:` block**
  (`base_url`, `model`, `provider_options`, `prompt_context`). Previously
  these fields were passed through the parser un-resolved, so
  `${ENV_VAR}` and `${ctx.*}` references stayed literal — for example,
  `base_url: "${AQ_OLLAMA_URL}/v1"` reached httpx as the literal string
  `${AQ_OLLAMA_URL}/v1` (no scheme), surfacing as a confusing URL-protocol
  error. Resolution is wrapped in `try/except ValueError → ParseError`
  for parity with `spark_config` / `macros` — a missing env var now
  raises a clean `parse error: agent config resolution failed: …`
  instead of a raw `ValueError` traceback.

### Added
- **Apache Airflow integration** (`aqueduct.integrations.airflow`, Phase 31):
  drop-in `AqueductOperator` plus deferrable `AqueductPatchSensor` /
  `AqueductPatchTrigger`. The operator subprocesses `aqueduct run` and maps
  the engine's stable exit codes onto Airflow outcomes; `HEAL_PENDING`
  (exit 3) triggers an async patch-approval wait that releases the worker
  slot via Airflow 2.7+ deferrable triggers. Install with
  `pip install aqueduct-core[airflow]`. See
  `aqueduct/integrations/airflow/README.md` for the full DAG example.
- New extras: `[airflow]` (slim, Airflow only) and `[schedulers]`
  (aggregate of scheduler integrations). `[all]` now pulls in
  `[schedulers]`.
- `docs/specs.md §10.7 — Orchestrator Integration Contract`: documents
  the engine-agnostic exit-code + patch-CLI JSON surface that any
  scheduler integration consumes.

## [1.0.0] — 2026-05-18

First stable release. The stability contract (`docs/STABILITY.md`, exit
codes, frozen public API) is now in force; subsequent breaking changes
follow SemVer. Consolidates everything previously staged as Unreleased
and the `1.0.0a2` pre-release (which never shipped separately).

### Added
- `aqueduct benchmark` accepts a single `.aqscenario.yml` (positional or
  `--scenarios`), not only a directory.
- `aqueduct benchmark --provider` / `--base-url` / `--timeout` to override
  the agent connection per run (precedence: flag > `aqueduct.yml` agent >
  default). `--timeout 0` = unbounded read (connect still fails fast).
- `aqueduct init` now writes a `.gitignore` (Spark/Aqueduct runtime
  artifacts, `.env`, ephemeral `patches/{pending,rejected}/`); existing
  files, including a user `.gitignore`, are never overwritten.
- `aqueduct doctor` `--aqtest` / `--aqscenario` pre-flight, combinable
  with a config/blueprint probe in one pass.
- `aqueduct benchmark --format json` now includes the generated
  `patch` (PatchSpec) per result, so a failure can be diagnosed without
  re-running. Table mode prints a `(N failed — rerun with --format
  json …)` hint when any scenario fails.
- Stable exit codes (`aqueduct/exit_codes.py`) and `docs/STABILITY.md`;
  downstream tooling can branch on `$?`.
- `aqueduct schema --target {blueprint,config,patch}` emits the JSON
  Schema for IDE autocomplete / CI gating.
- `--format json` on `aqueduct runs` and `aqueduct patch list`.
- Two-tier suppressible warning system with stable `AQ-WARN [rule_id]`
  IDs and `warnings.suppress` / `--suppress-warning`. New rules:
  `kafka_checkpoint_stale`, `nondeterministic_fanout`,
  `count_col_likely_count_star`, `file_format_no_repartition`,
  `jdbc_missing_partition`, `jar_availability`.
- Post-patch `explain()` regression check and
  `agent.block_on_explain_regression` (warn-only by default).
- `aqueduct patch preview` with lineage + sandbox validation gates;
  `agent.patch_validation: full_run | sandbox`.
- Pluggable observability/lineage/depot store backends (Postgres;
  Redis for depot) with `aqueduct stores info|migrate` and
  `[postgres]` / `[redis]` extras.
- Secrets providers `aws` / `gcp` / `azure` / `custom` with
  `[aws]` / `[gcp]` / `[azure]` extras.
- Global `--log-format {text,json}` for structured log shipping.
- `agent.max_heal_attempts_per_hour` spend-cap on self-healing.
- `aqueduct compile --show {manifest,provenance,inputs,all}`.
- Ingress `partition_filters`; Egress `maintenance:` (Delta
  OPTIMIZE/VACUUM); Channel `metrics_boundary`; Channel
  `materialize: incremental` + `watermark_column`; `Manifest`
  input fingerprinting.
- Assert `error_type` plus `agent.guardrails.heal_on_errors` /
  `never_heal_errors` pre-trigger guards.

### Changed
- `aqueduct benchmark` scoring is now two-tier: **correctness gates**
  PASS/FAIL (`patch_is_valid`, `patch_applies`, `expected_patch`), while
  **diagnosis quality** (`root_cause_contains`, `expected_category`,
  `max_attempts`, `min_confidence`) is recorded as a diagnosis score and
  reported (`diag_score`/`soft_failures` in JSON, `d%` + `Diag score` in
  the table) but **never fails an otherwise-correct fix**.
- Gallery `aqscenarios` benchmark suite reworked for fidelity: each
  scenario now has its own blueprint carrying exactly one real defect,
  `inject_failure.error_message` mirrors authentic Spark output (error
  class, `SQLSTATE`, suggestion list), and scoring is op-agnostic
  (outcome + diagnosis, not a hard-coded patch op). Previously 4/5
  scenarios injected an error unrelated to the shared clean blueprint
  (unsolvable/ungradable).
- `.env` is now auto-loaded by **every** command from the directory of
  the config/blueprint passed (previously only `run`/`doctor`/`validate`).
  A one-line stderr notice reports what was loaded; `-e KEY=VAL`
  (docker-style, highest precedence) and `AQ_NO_ENV_FILE=1` added.
- `aqueduct benchmark` default `--workers` is now `1` (serial); set `>1`
  to parallelize scenario×model pairs.
- `aqueduct test` always runs on `local[*]` and ignores
  `deployment.master_url` (isolated unit tests); `--master` overrides for
  cluster-runtime-dependent modules.
- `aqueduct doctor` default view shows only actionable rows; skip/green
  low-signal rows collapse into one line, `--verbose` expands. The Spark
  check is a fast bounded reachability probe by default; `--preflight`
  runs a full unbounded session check. `agent` no longer warns when
  self-healing is simply unconfigured (opt-in). Convention: ✗ = "will
  break", ⚠ = "runs but fragile". doctor stays advisory.
- Init scaffold directories renamed for consistency: `tests/` → `aqtests/`,
  `benchmarks/` → `aqscenarios/`.
- Default `agent.model` is now `claude-sonnet-4-6`.
- Incremental-Channel watermark is computed from materialized Egress
  output instead of re-scanning the upstream DAG twice.
- Cluster/cloud deployments with relative store paths now error in
  `doctor` and warn at run start.
- Public API frozen to a small `__all__` (`parse`, `ParseError`,
  `AqueductWarning`, `__version__`); everything else is internal.
- **BREAKING:** `aqueduct benchmark --output` renamed to `--format`
  (data-shape selector; consistent with `runs`/`report`/`patch list`).
  `-o`/`--output` is now reserved exclusively for file destinations
  (`schema`); `compile --show` keeps its artefact-slice meaning.

### Deprecated
- _None._

### Removed
- **BREAKING:** `heal --scenario` removed. `heal` is production-only
  (heal a real failed `run_id`); scenario evaluation is
  `aqueduct benchmark <file-or-dir>`.
- **BREAKING:** `heal --print-prompt-format` removed — folded into
  `--print-prompt [text|json]` (bare = text, `--print-prompt json` for
  JSON).
- **BREAKING:** `warnings.silence_all` config field and `--no-warnings`
  flag removed. Use `warnings.suppress: ["*"]` or
  `--suppress-warning '*'`.
- **BREAKING:** `aqueduct check-config` removed — use `aqueduct validate`
  (auto-detects blueprint vs engine config by header, accepts multiple
  files).
- **BREAKING:** `aqueduct doctor --config` / `--blueprint` removed — pass
  the file positionally (header-sniffed).
- **BREAKING:** `agent.llm_timeout` → `agent.timeout`,
  `agent.llm_max_reprompts` → `agent.max_reprompts` (the self-healing
  subsystem is uniformly "the agent"; no aliases).
- **BREAKING:** `probes.block_full_actions_in_prod` →
  `danger.allow_full_probe_actions` (inverted polarity).
- `ollama_options` renamed to `provider_options`.

### Fixed
- Postgres store backend (`stores.*.backend: postgres`) no longer crashes
  on DuckDB-only DDL/upsert SQL; non-DuckDB store `path` (a DSN) is no
  longer mis-created as a local directory. (Full multi-backend
  certification still pending.)
- `--parallel` Probe race that silently dropped Probe signals
  (ISSUE-042).
- Tier-0 tokens (`${VAR:-default}`, `${ctx.*}`) are now resolved in
  top-level `spark_config` and `macros` (ISSUE-027).
- `materialize: incremental` blueprints referencing `${ctx._watermark}`
  no longer fail `validate`/`run`.
- Duplicate missing-env-var error lines collapsed to one.
- pyspark `DataFrame.sql_ctx` deprecation warning from the explain gate.
- Misleading agent "failed after N attempts" log now reports the actual
  attempt count.
- Bundled `aqtest.yml.template` rewritten to match the real
  `aqueduct test` runner schema (it previously documented a
  non-functional format).

## [1.0.0a1] — 2026-05-12

### Added
- `danger:` block (`allow_aggressive_patching`,
  `allow_full_probe_actions`), defaulting `false`, with a startup
  warning when any is enabled.
- `approval_mode: ci` with `agent.ci_webhook_url`; webhooks
  `on_patch_pending` / `on_ci_patch` make `human`/`ci` review viable
  for teams.
- `PatchSpec.confidence|category|root_cause`; `confidence < 0.7`
  auto-escalates to human review regardless of `approval_mode`.
- `healing_outcomes` table persisting every healing attempt.
- Per-pipeline store paths (`.aqueduct/obs/{blueprint_id}/`),
  removing DuckDB write contention across pipelines.
- Java/Scala UDFs (`lang: java|scala`, `jar:`, `entry:`).
- `schema_hint` `strict | additive | subset` modes.
- Git-integrated patch lifecycle: `aqueduct patch
  commit|discard|log|rollback|list`; surgical `set_module_config_key`
  op.
- Provenance layer — smaller/cleaner LLM prompts; `doctor` recurses
  into Arcades.
- `aqueduct init` project scaffold (writes `.gitignore` ignoring the
  ephemeral patch dirs); `aqueduct runs`; `aqueduct report`;
  `aqueduct lineage`; `aqueduct signal`; `aqueduct heal`;
  `aqueduct test`.
- Probe signal types: `value_distribution`, `distinct_count`,
  `data_freshness`, `partition_stats`.
- Compile-time SQL macros; Channel `op: join` with broadcast hint.

### Changed
- `runs.db` + `signals.db` consolidated into a single
  `observability.db`.

### Removed
- **BREAKING:** `agent.validate_patch` field removed — `aggressive`
  mode now always validates in-memory before writing.

## [1.0.0a0] — 2026-04-27

First alpha on PyPI (`aqueduct-core`). Phases 1–9: the core engine.

### Added
- Declarative blueprint pipeline: Parser → Compiler → Executor with
  `aqueduct validate | compile | run`.
- Modules: Ingress, Egress, Channel (SQL), Junction, Funnel, Probe;
  Spillway error-row routing; Assert data-quality module
  (`abort|warn|webhook|quarantine|trigger_agent`).
- Resolution: Tier-0 (`${ENV:-default}`, `${ctx.*}`, env/profile/CLI
  overrides) and Tier-1 (`@aq.date.*`, `@aq.run.*`, `@aq.depot.*`,
  `@aq.secret()`); Arcades (reusable sub-blueprints).
- Depot KV store; Python/Java/Scala UDFs; `RetryPolicy` (exponential/
  linear/fixed + jitter + deadline); per-module `on_failure`;
  opt-in checkpoint/resume.
- LLM self-healing: `approval_mode: disabled|human|auto|aggressive`,
  deterministic guardrails (`allowed_paths`, `forbidden_ops`), patch
  staging/rollback; Anthropic + Ollama + OpenAI-compatible providers
  over plain HTTP (no `anthropic` SDK dependency).
- Observability: Surveyor (DuckDB) run records / failure contexts;
  SparkListener per-module metrics; column-level lineage via sqlglot;
  `aqueduct doctor`.
- Remote Spark (`local[*]`, `spark://`, `yarn`, `k8s://`); partial DAG
  (`--from`/`--to`); `--execution-date` for backfills.
