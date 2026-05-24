# Changelog

All notable, consumer-facing changes to Aqueduct (`aqueduct-core`) are
recorded here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [SemVer](https://semver.org/). The stability contract
applies from v1.0.0 — during alpha/RC, breaking changes may land in any
release and are marked **BREAKING**.

## [Unreleased]

### Added
- **Guardrail compliance chain** (Phase 33 Part B Scope C). Three coupled
  changes close the gap where `agent.guardrails` was defined per-blueprint
  but never enforced uniformly:
  1. **Step 1 — prompt injection.** A terse imperative `## Guardrails`
     section is appended to the user prompt whenever
     `manifest.agent.guardrails` has any non-empty field
     (`forbidden_ops`, `allowed_paths`, `heal_on_errors`,
     `never_heal_errors`). The model now sees the constraints upfront
     instead of producing a patch production silently rejects. Threaded
     through `generate_agent_patch(..., guardrails=...)` and `build_prompt`.
  2. **Step 2 — scenario enforcement.** `_try_apply_patch` (benchmark
     code path) now invokes `patch.apply._check_guardrails` before the
     dict apply, matching production. Previously benchmark used
     `apply_patch_to_dict` directly and skipped guardrail checks, which
     made the leaderboard over-report PASS vs production reality.
  3. **Step 3 — guardrail-clean rate metric.** `ScenarioResult` and the
     `benchmark_results` table gain a `violated_guardrails` field
     (`None` when scenario blueprint declares no guardrails — excluded
     from the rate; `[]` when defined-and-clean; non-empty when violated).
     A new leaderboard row "Guardrail-clean" reports the rate. Surfaced
     in `benchmark --format json` and persisted with idempotent ALTER
     migration for pre-existing benchmark stores.
- **Effect-based grader** (Phase 33 Part B Scope C). Replaces the old
  `expected_patch.ops` op-name-equality grader (deleted) with a
  post-patch effect check: assert the patched blueprint's target module
  config matches a `config_contains` map. SQL-typed fields (`query`,
  `sql`) are compared via sqlglot AST normalization so whitespace,
  quoting, and alias-case differences no longer trip false fails. Old
  scenarios that still use the deleted `ops:`/`forbidden_ops:` syntax
  produce a single hard failure pointing at the migration path.
- **Sample guardrail scenario** at
  `gallery/aqscenarios/06_guardrail_forbidden_op.aqscenario.yml` — same
  column-rename failure as scenario 01, but the blueprint declares
  `forbidden_ops: [replace_module_config]` so the model must produce a
  surgical patch. Exercises all three guardrail-chain steps end to end.

### Changed
- **Migrated all gallery scenarios** to the new
  `expected_patch.effect:` syntax. Each scenario's `config_contains`
  specifies the post-patch value that the fix must land — accepts any
  valid op (set_module_config_key, replace_module_config, etc.) that
  reaches the same end state. Scenario 05 (type_string_vs_numeric)
  has multiple valid fixes; its `expected_patch` is intentionally empty
  and gates on `patch_applies` + `root_cause_contains` only.
- **`ScenarioResult` carries `violated_guardrails`**. Backward-compatible
  field addition (default `None`). The leaderboard renderer falls back
  to `getattr` so older `ScenarioResult` instances from external callers
  don't break.

### Added
- **Benchmark persistence + regression detection** (Phase 33 Part A).
  Each `(scenario, model)` `ScenarioResult` from `aqueduct benchmark` is
  now persisted to `<scenarios_dir>/.aqueduct/benchmark.duckdb` so prior
  runs are queryable and CI can gate on regressions. Schema includes
  `prompt_version`, `provider`, `base_url`, `failures`, `soft_failures`,
  and the existing pass/quality metrics.
  - `aqueduct benchmark` gains `--no-persist`, `--store-path PATH`, and
    `--gate-on-regression` flags. `--gate-on-regression` runs the diff
    after persistence and exits non-zero if any `(scenario, model)` pair
    shows a regression — drop-in CI hook for "did a prompt edit silently
    break a scenario?"
  - New `aqueduct benchmark-diff` command: reads the store, compares the
    two most recent runs per pair, prints a status table
    (`NEW` / `= same` / `↑ IMPROVE` / `✗ REGRESS`), and exits non-zero
    on any regression. Supports `--scenario`, `--model`, `--store-path`,
    `--format` filters.
  - Regression metrics: `passed`, `patch_valid`, `patch_applies`
    boolean flips, plus `diag_score` drop > 5pp. LLM-self-reported
    `confidence` is deliberately NOT part of the gate — confidence is
    persisted on every row but excluded from the diff (overconfidence
    bias + cross-model incomparability would produce noise, not signal).
  - Baseline selection prefers exact
    `(scenario, model, prompt_version)` triple and falls back to most
    recent regardless of `prompt_version` with a
    `baseline_prompt_mismatch=True` flag — a `PROMPT_VERSION` bump no
    longer masquerades as a regression.
  - Persistence is best-effort: a locked store or missing `duckdb`
    dependency logs a warning and returns 0, never fails the benchmark
    command.
- **`healing_outcomes.prompt_version` column** (Phase 33 Part A).
  Additive in-place ALTER, idempotent on existing DBs (mirrors the
  `id INTEGER → VARCHAR` migration pattern already in
  `surveyor.Surveyor.start()`). `record_healing_outcome()` populates
  the column from `aqueduct.agent.PROMPT_VERSION` by default; explicit
  override honored. Makes the "version ↔ heal outcome" correlation the
  docs claim finally answerable in SQL.
- **`ScenarioResult` carries `prompt_version`, `provider`, `base_url`**.
  Backward-compatible field additions (all default `None`); populated
  in both the early-exit FailureContext-build-failure branch and the
  normal path of `run_scenario`.

### Changed
- **`agent.timeout` default 120 → 300 seconds**. The previous default
  was hostile to local-model cold-start (model load into VRAM can take
  30-90s before any inference, eating most of the old 120s window).
  300s tolerates cold-start + inference on small/medium local models;
  hosted APIs (Anthropic) typically respond in <30s so the larger
  ceiling does not affect them. Set explicitly to `120` to restore
  pre-1.0.3 behaviour.
- **LLM API failure log now includes an actionable hint** for the two
  most common transient modes:
  - Timeout → suggests `--timeout 600` and shows a concrete pre-warm
    `curl` against the configured `base_url`
  - Connection failure → suggests connectivity checks against the
    configured endpoint

## [1.0.2] — 2026-05-23

### Added
- **External secrets provider dispatch for `@aq.secret('KEY')`** (Phase 32).
  Previously `@aq.secret()` was a silent alias of `${KEY}` — read from
  `os.environ` only — and `secrets.provider: aws|gcp|azure` was validated
  at config load but never dispatched to. `aqueduct.yml` now loads in two
  passes: pass 1 expands `${VAR}` and validates the config (including
  `secrets.provider`); pass 2 calls the configured provider for every
  `@aq.secret('KEY')` token and re-validates. Backends: `env` (default,
  reads `os.environ`), `aws` (Secrets Manager via `boto3`), `gcp`
  (Secret Manager via `google-cloud-secret-manager`), `azure` (Key Vault
  via `azure-keyvault-secrets` + `azure-identity`), `custom` (dotted-path
  callable). Each provider uses its cloud's ambient credential chain
  (boto3 default chain / GCP ADC / Azure `DefaultAzureCredential`) — no
  cloud credentials live in `aqueduct.yml`. `resolve_secret()` no longer
  caches into `os.environ`, so provider-side rotation takes effect on the
  next call and the per-call audit trail is preserved.
- **Secret redaction registry (`aqueduct/redaction.py`)** scrubs every
  resolved `@aq.secret()` value (replaced with `[REDACTED]`) from outputs
  that cross a trust boundary or hit persistent storage. Wired at five
  sinks: console / log output (`click.echo` + root-logger filter),
  observability.db rows, patch sidecar files
  (`patches/{pending,applied,rejected}/*.json`), outbound webhook
  payloads (body only — headers and URL are intentionally untouched
  because user-authored credentials in headers are the intended
  transmission path), and LLM agent request payloads (`system_prompt` +
  `messages` sent to Anthropic / OpenAI-compat endpoints). Defense-in-
  depth, not the primary defense — values shorter than 8 chars or below
  2.5 bits/char of Shannon entropy are NOT registered and emit
  `AQ-WARN [secret-weak-redact]` instead, because substring removal of
  short common identifiers produces too many false positives.
  Token-boundary matching (`(?<!\w)X(?!\w)`) prevents a secret value
  equal to `hunter2` from redacting occurrences inside `hunter2_module`.

## [1.0.1] — 2026-05-23

### Fixed
- **`aqueduct run` no longer crashes when the `git` binary is missing**
  from the runtime environment. The uncommitted-applied-patches check
  shells out to `git log`; previously a missing or non-executable
  binary raised `PermissionError` / `FileNotFoundError` before the
  return-code branch ran, killing the run. The exception is now caught
  and the check falls back to "treat all applied patches as
  uncommitted" — same behavior as a `git`-less directory.
- **`aqueduct patch list --format json` now emits `run_id`,
  `blueprint_id`, `failed_module`** (sourced from the patch file's
  `_aq_meta` block). Without these fields downstream integrations
  (Airflow trigger, CI gates) had no reliable way to match a patch to
  the run that produced it. Also updates the Airflow
  `AqueductPatchTrigger` to match by exact `run_id` first, with the old
  substring heuristic kept as a fallback for pre-1.0.1 patches.
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
