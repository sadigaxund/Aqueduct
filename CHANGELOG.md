# Changelog

All notable, consumer-facing changes to Aqueduct (`aqueduct-core`) are
recorded here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [SemVer](https://semver.org/). The stability contract
applies from v1.0.0 — during alpha/RC, breaking changes may land in any
release and are marked **BREAKING**.

## [Unreleased]

### Added
- **Unified reprompt loop with multi-axis budget** (Phase 34). The agent's
  two disjoint loops (inner `max_reprompts` and outer aggressive retry) are
  collapsed into a single `generate_agent_patch` loop driven by a stateful
  `BudgetTracker`. The same loop now catches schema/JSON parse failures AND
  apply-time gate rejections (guardrail violation, parse/compile failure on
  the patched blueprint) when callers pass an `apply_callback`. Apply
  rejections feed back into the next reprompt instead of being silently
  dropped in `human` / `auto` / `ci` modes — previously a one-shot wasted
  call.
- **`BudgetConfig` (six axes) + `BudgetTracker`** in `aqueduct.agent.budget`:
  `max_reprompts` (default 5), `max_seconds` (120s), `max_tokens_total`
  (50k), `same_error_consecutive` (2), `same_signature_overall` (3),
  `progress_stalled_window` (3). First axis to trip terminates the loop and
  records the `stop_reason` (`solved`, `exhausted_attempts`,
  `budget_seconds_exceeded`, `budget_tokens_exceeded`, `stuck_signature`,
  `progress_stalled`, `api_error`). Exposed in `aqueduct.yml` as
  `agent.budget:` (frozen pydantic model; same instance shared between
  production heal and benchmark — divergence would silently invalidate the
  leaderboard).
- **Error-signature engine** (`aqueduct.agent.signature`). Stable
  16-char sha1 over `(error_class, where, normalized_message)`. Normalizer
  collapses whitespace, lowercases, strips ANSI, replaces digits with `N`,
  quoted strings with `"X"`, and `/path/like/things` with `/PATH` — two
  failures that differ only in volatile bits hash identically. Six factory
  helpers cover Pydantic `ValidationError`, JSON parse failures, generic
  exceptions, apply-gate rejections, and plain reprompt text. The same
  hashing layer is reused by Phase 33 Part C (coaching loop, post-Phase-34).
- **Stuck-detection escalation BEFORE abort** (Phase 34 #6). When
  `same_error_consecutive` trips (2 identical signatures in a row), the
  loop does NOT immediately abort. Instead it bumps sampling temperature to
  0.8 AND swaps the reprompt template to the skeleton-anchored variant for
  one more attempt. If that attempt also produces the same signature the
  loop terminates with `stuck_signature`. Costs one extra attempt over
  naive abort and recovers most cases where the model would have escaped
  with a different seed.
- **Skeleton-anchored reprompt template** (Phase 34 #5). When the validator
  flags a structural error (top-level `operations` missing AND op-level
  fields at root) OR when escalation triggers, the reprompt shows a minimal
  valid PatchSpec skeleton + a one-line "you forgot the `operations[]`
  wrapper" hint and stops echoing the bad output — which biases small
  models toward small edits of their mistake. Cleaner correction signal
  measurably improves recovery on small-model stuck-signature cases.
- **`heal_attempts` table** in `observability.db` (Phase 34 #3). One row
  per LLM turn (success or failure) — `attempt_num`, `signature_hash`,
  `error_class`, `where_field`, `normalized_message`, `tokens_in/out`,
  `latency_ms`, `gate_that_rejected` (`schema` / `apply` / `provider` /
  None), `escalated`, `stop_reason`, `prompt_version`. Wired through the
  unified loop's `on_attempt` hook so `aqueduct run` populates it
  automatically. Post-mortem can now answer "what did attempt 2 say" —
  which `healing_outcomes` alone could not.
- **`benchmark_results` gains `stop_reason`, `escalated`,
  `tokens_in_total`, `tokens_out_total`** (Phase 34 #7 — benchmark =
  production parity). Idempotent additive ALTER for pre-existing stores so
  pre-1.0.4 benchmark histories survive intact. The leaderboard can now
  distinguish "model gave up" (`stuck_signature`) from "ran out of attempts"
  (`exhausted_attempts`).
- **`AgentPatchResult`** gains `stop_reason`, `tokens_in_total`,
  `tokens_out_total`, `attempt_records`, `escalated` fields. All default
  to safe values so older callers and test fixtures keep working
  unchanged.

- **Structured Spark error extraction** (Phase 35). Surveyor now calls
  `_extract_structured_error(exc)` before stringifying the failure: pulls
  Spark 4.0 `PySparkException.getCondition()` / `getErrorClass()` +
  `getMessageParameters()` + `getSqlState()`, walks `Py4JJavaError.java_exception.getCause()`
  for the innermost JVM throwable (capped at 10 hops), and falls back to the
  Python cause chain. Extraction is lazy-imported and best-effort — any
  failure returns None so a buggy extractor cannot block self-heal.
- **`FailureContext` gains optional fields** `error_class`, `root_exception`
  (`{type, message}`), `sql_state`, `suggested_columns` (parsed from
  `UNRESOLVED_COLUMN.WITH_SUGGESTION` proposal lists), and `object_name`.
  All default to None / empty tuple; legacy callers see no behaviour change.
  Idempotent ALTER on `observability.db.failure_contexts` upgrades existing
  stores in place.
- **Conditional prompt rendering**: structured fields replace the raw stack
  trace block when extraction succeeded, producing a focused "Root cause
  (structured)" section with the offending column name + actual columns
  available. The full trace is shown only when extraction returned None.
  Deleted `_truncate_stack` + `_STACK_TRACE_MAX_LINES` — Phase 35 makes the
  arbitrary line-count cutoff unnecessary.
- **`inject_failure.structured:` block** in `.aqscenario.yml` lets
  benchmark scenarios carry the same structured fields production extracts
  from live exceptions, so the leaderboard exercises the identical prompt
  branch. `gallery/aqscenarios/01_schema_drift_column_rename.aqscenario.yml`
  migrated as worked example. Legacy scenarios with no block fall through
  to the stack-trace path.

### Changed
- **Provider calls return token usage.** `_call_anthropic` and
  `_call_openai_compat` now return `(text, tokens_in, tokens_out)` tuples;
  budget axes can therefore enforce real cost ceilings instead of just
  attempt counts. Anthropic uses `usage.input_tokens`/`output_tokens`;
  OpenAI-compatible endpoints use `usage.prompt_tokens`/`completion_tokens`.
  Zero when the provider does not report usage.
- **`aqueduct benchmark` shares the same `BudgetConfig` as production
  heal** — read from `agent.budget:` in `aqueduct.yml`. Apply-gate
  rejections (guardrail violation, compile failure on the patched
  blueprint) now feed back into the same reprompt loop benchmark and
  production use, closing the "leaderboard cheats" path where the
  benchmark would silently pass on a patch production would reject.

### Documentation
- New `docs/QUERIES.md` — diagnostic SQL cookbook against the observability,
  lineage, depot, and benchmark stores. Recipes organised by use case
  (run post-mortem, token spend, stuck-signature detection, leaderboard,
  regression diff, cross-pipeline aggregates) with reading guides for each
  output. Cross-linked from `ALL_TABLES.md`.
- `ALL_TABLES.md` filesystem-layout block updated for per-pipeline routing
  (`.aqueduct/observability/<blueprint_id>/{observability,lineage}.db`);
  added the aggressive `run_id` vs `parent_run_id` semantics note that
  every multi-table join needs.

### Changed
- **`approval_mode: aggressive` collapsed into `auto` + `max_patches`.** The
  two near-duplicate self-heal modes shared a 90+ LOC code path differing only
  in a danger gate, an explain-gate strictness flag, and the default patch
  count. `approval_mode: auto` is now the single auto-apply mode with a new
  `max_patches: int = 1` knob (default 1 = single-shot, the historical `auto`
  behaviour). Setting `max_patches > 1` opts into the multi-patch reprompt
  loop and requires `danger.allow_multi_patch: true` (alias:
  `allow_aggressive_patching`). The explain gate's hard-block on regression
  now fires whenever `max_patches > 1`, not based on the mode name.

  Backwards compatibility: `approval_mode: aggressive` keeps parsing but
  emits a `[deprecated]` warning at parse time and is normalised to `auto`
  internally. `aggressive_max_patches` keeps parsing as a `validation_alias`
  on `max_patches` and emits the same warning. `danger.allow_aggressive_patching`
  is a `validation_alias` on `danger.allow_multi_patch`. CLI flag
  `--allow-aggressive` is kept as a secondary form of `--allow-multi-patch`.
  All four deprecated names are slated for removal in `aqueduct: "2.0"` schema.
  Default `max_patches` change (5 → 1) flips behaviour for blueprints that
  relied on the previous default; explicit values are unaffected.

### Added
- Regulator modules now accept a `config.poll_seconds: float = 30.0` knob
  controlling the cadence of the gate-poll loop while `timeout_seconds > 0`.
  Replaces a hardcoded 2-second poll interval that wasted driver CPU and
  observability-DB reads on batch-tier signals. Clamped to a minimum of 0.5s.
  Tune lower for interactive use, higher for hour-scale external triggers.
- `agent.sandbox_mode: sample | preflight | off` (blueprint + engine config).
  Controls patch sandbox replay fidelity. `sample` (default) keeps existing
  1000-row replay; `preflight` runs full dataset (no Egress) and requires
  `danger.allow_full_preflight: true`; `off` skips the gate entirely and
  requires `danger.allow_skip_sandbox: true`. Engine prints a startup warning
  for `preflight` / `off` and a `⚠ DANGER COMBO` line when
  `sandbox_mode=off` + `approval_mode=aggressive` are both set.
- `danger.allow_full_preflight` and `danger.allow_skip_sandbox` config flags.

### Fixed
- **Path resolution.** Every relative path inside a YAML file now resolves to
  that YAML's parent directory, not the CWD of `aqueduct run`. Affects
  `module.config.path`, `module.config.data_dir`, `input_dir`, `output_dir`,
  `jar`, and `stores.*.path`. URI-style values (`s3://`, `postgresql://`,
  etc.) and absolute paths pass through unchanged. The on-disk YAML is never
  rewritten — only the in-memory compiled `Manifest`/config carry absolute
  paths, so LLM context (the raw blueprint dict) is unaffected. Fixes
  sandbox replay's `"events_raw produced no DataFrame"` when the blueprint
  is invoked from a sub-dir. Matches Compose / k8s / Terraform conventions.
- `run_records` now writes one row per aggressive-mode iteration, with
  `parent_run_id` linking back to the user-visible outer `run_id`. Previously
  `Surveyor.record()` issued a plain `UPDATE WHERE run_id = ?` against a row
  that only existed for iteration 0 — iterations 1..N silently no-op'd, so an
  aggressive heal with 3 attempts persisted exactly **one** `run_records`
  row (iteration 0's). Two changes: (1) `run_records` schema grows a
  `parent_run_id VARCHAR` column with idempotent ALTER for pre-1.1.0 DBs;
  (2) `Surveyor.record()` switched from `UPDATE` to `INSERT … ON CONFLICT
  DO UPDATE` so each iteration owns its row; (3) new
  `Surveyor.register_iteration(run_id, parent_run_id)` called by the CLI
  before each non-first `execute()` so the iteration row carries its parent.
  Join all iterations of one heal call:
  `WHERE COALESCE(parent_run_id, run_id) = '<outer>'`.
- `healing_outcomes` no longer silently empty when the unified reprompt loop
  exits with `patch=None` (every attempt rejected by `apply_callback`, or
  budget axis tripped before a valid patch landed). The aggressive-mode loop
  in `aqueduct/cli.py` now synthesises one `healing_outcomes` row per
  `agent_result.attempt_records` entry, with `patch_applied=false`,
  `run_success_after_patch=false`, and `failure_category` derived from the
  attempt's signature. Previously `heal_attempts` had the per-attempt log but
  `healing_outcomes` was blank, so `WHERE parent_run_id=<outer>` returned
  zero rows even after 3+ in-loop rejections. Confirmed against
  `02_guardrail_apply_reject.yml --allow-aggressive` (3 heal_attempts rows,
  0 healing_outcomes rows pre-fix).
- `heal_attempts` no longer double-writes per attempt. The unified loop's
  `on_attempt` hook INSERTs one row per attempt with `stop_reason=NULL`;
  the post-loop terminal update now calls a new
  `Surveyor.update_heal_attempt_stop_reason()` that UPDATEs the same row's
  `stop_reason` instead of INSERTing a duplicate. Affects both `aqueduct run`
  self-heal and `aqueduct heal <run_id>` heal-from-store.
- `healing_outcomes` gained a `parent_run_id` column so cross-iteration
  aggressive runs can be queried by the user-visible outer `run_id`.
  In aggressive mode the existing `run_id` column held a per-iteration uuid
  starting at iteration 2 — joins against `run_records.run_id` returned
  partial results. New column NULL on non-aggressive paths; idempotent
  ALTER added in `Surveyor.start()` so pre-1.1.0 stores upgrade in place.
- Phase 34 apply-gate guardrail check now wired into the production heal
  loops (`aqueduct run` self-heal at `cli.py:1625`, `aqueduct heal <run_id>`
  heal-from-store at `cli.py:3565`). Previously only the benchmark path
  (`scenario.py`) passed `apply_callback=` — production heal exited
  `solved` even when the patch then failed `_check_guardrails`, and the
  outer code silently staged the blocked patch. Rejections now feed back
  as reprompts inside the unified loop with `gate_that_rejected='apply'`,
  matching Phase 34's "unified loop" promise on both paths.
- Shared `_resolve_obs_db(cfg, store_dir, run_id)` helper in `cli.py` —
  every read-side command (`runs`, `report`, `lineage`, `heal`) used to
  reinvent observability path resolution with a naive
  `Path(cfg.stores.observability.path).parent`, which never worked when
  the user kept the default and per-pipeline routing put the file at
  `.aqueduct/observability/<blueprint_id>/observability.db`. New helper
  honours `--store-dir`, then explicit `aqueduct.yml` path, then walks the
  per-pipeline dirs to find which DB carries the requested `run_id`,
  falling back to the legacy shared path. Fixes `aqueduct heal <run_id>`
  failing with `observability.db not found at .aqueduct/observability.db`
  on a perfectly valid run_id, plus `aqueduct runs` list mode now unions
  across per-pipeline DBs instead of silently showing zero.
- Phase 35 structured-error extractor now fires on the common failure path.
  The Spark executor catches `IngressError` / `ChannelError` / etc. inside
  its module loop and reports via `ModuleResult`, so the live exception
  never escaped to `Surveyor.record(exc=...)` — the extractor silently
  no-op'd and `failure_contexts.error_class / object_name /
  suggested_columns / sql_state / root_exception` stayed NULL on the
  overwhelming majority of real failures. Fix: `ModuleResult` gained an
  optional `exception: BaseException | None` field; the executor's
  centralised `_on_retry_exhausted` helper + Assert error site now populate
  it; `Surveyor.record()` falls back to the first failed module's
  `exception` when its `exc=` kwarg is None. Chain preservation already
  worked end-to-end (`raise IngressError(...) from exc`) — only the
  hand-off was missing.
- `aqueduct run` final status line + `_last_run_id` Depot key + `on_success`
  webhook payload now report the outer (user-visible) `run_id` instead of
  the LAST aggressive iteration's per-iteration uuid. The printed run_id
  now matches `heal_attempts.run_id` and `healing_outcomes.parent_run_id`,
  so the value users copy from stderr can actually retrieve the heal
  history. Also fixed `_run_patch_gates_inline` to accept the renamed
  `iteration_run_id` kwarg (kwarg-name mismatch crashed aggressive mode
  on the first patch).
- Documented that `stop_reason='solved'` describes LLM loop termination
  only (parseable PatchSpec returned), NOT whether the heal actually fixed
  the pipeline. To check the latter, join against
  `healing_outcomes.run_success_after_patch`. Updates to `aqueduct/agent/budget.py`
  docstring, `docs/ALL_TABLES.md`, and `docs/CLI_REFERENCE.md`.

## [1.0.3] — 2026-05-24

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
  `expected_patch.ops` op-name-equality grader (deleted, see Removed)
  with a post-patch effect check: assert the patched blueprint's target
  module config matches a `config_contains` map. SQL-typed fields
  (`query`, `sql`) are compared via sqlglot AST normalization so
  whitespace, quoting, and alias-case differences no longer trip false
  fails.
- **Sample guardrail scenario** at
  `gallery/aqscenarios/06_guardrail_forbidden_op.aqscenario.yml` — same
  column-rename failure as scenario 01, but the blueprint declares
  `forbidden_ops: [replace_module_config]` so the model must produce a
  surgical patch. Exercises all three guardrail-chain steps end to end.

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
- **Migrated all gallery scenarios** to the new `expected_patch.effect:`
  syntax. Each scenario's `config_contains` specifies the post-patch
  value the fix must land — accepts any valid op (set_module_config_key,
  replace_module_config, etc.) that reaches the same end state. Scenario
  05 (type_string_vs_numeric) has multiple valid fixes; its
  `expected_patch` is intentionally empty and gates on `patch_applies` +
  `root_cause_contains` only.
- **`ScenarioResult` carries `violated_guardrails: list[str] | None`**.
  Backward-compatible field addition (default `None`).

### Removed
- **`expected_patch.ops:` / `expected_patch.forbidden_ops:` scenario
  syntax**. Op-name equality grading produced false negatives on capable
  models that chose a valid alternative op (e.g. `replace_module_config`
  where the scenario pinned `set_module_config_key`). Replaced by
  effect-based grading (see Added). Old-syntax scenarios now fail with
  a single hard error pointing at the migration path; gallery scenarios
  have all been migrated.

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
