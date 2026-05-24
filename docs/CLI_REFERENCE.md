# Aqueduct CLI Reference

Commands grouped by workflow. All commands accept `--config <path>` to point at a
non-default `aqueduct.yml`; Aqueduct also walks up from the CWD automatically.

**Global flags** (placed before the subcommand):

| Flag | Description |
|---|---|
| `--version` | Print the installed `aqueduct-core` version (sourced from `importlib.metadata`) and exit. |
| `-v`, `--verbose` | DEBUG logging — LLM responses, SQL plans, resolver steps. |
| `--log-format text\|json` | `text` (default) is the human-readable formatter. `json` emits one JSON object per record (`ts`, `level`, `logger`, `msg`, plus `exc` when an exception was logged) for shipping to Loki / Splunk / Datadog. |

**Output-flag conventions** (three distinct, non-overlapping concerns):

| Flag | Meaning | Where |
|---|---|---|
| `--format {table\|json\|…}` | Data **shape** of the result | `runs`, `report`, `benchmark`, `patch list`, `patch log` |
| `-o`, `--output <path>` | File **destination** (`-` = stdout) | `schema`, `compile` |
| `--show {manifest\|provenance\|inputs\|all}` | Which compiled-**artefact slice** to emit | `compile` |

---

## Validate vs Doctor — two-tier model

- **`aqueduct validate`** — static. Parse + schema only, no side effects, fast.
  Use in CI / pre-commit. "Is it well-formed?"
- **`aqueduct doctor`** — live. Actually connects: Spark, stores, agent
  endpoint, secrets SDK, blueprint sources. Use before first deploy.
  "Will it actually run here?"

Both detect file kind by the version header — `aqueduct: "1.0"` is a
Blueprint, `aqueduct_config: "1.0"` is engine config — so no `--config` /
`--blueprint` flag is needed; just pass the file (or nothing, to use
`./aqueduct.yml`).

**`.env` resolution** (every command that takes a config/blueprint):
auto-loads `<dir-of-the-config-or-blueprint-you-passed>/.env` — the
**cwd is not searched** (removed footgun). Override with `--env-file
<path>` (fallback only, if no anchored `.env`). Real environment
variables are never overwritten. A one-line stderr notice always reports
what loaded, e.g. `(env: loaded 3 var(s) from /path/.env)`. Per-var
override: `-e KEY=VAL` (repeatable, docker-style, highest precedence).
Disable discovery entirely with `AQ_NO_ENV_FILE=1` (CI). Precedence:
`-e` > real env > anchored `.env` > `--env-file` > `${VAR:-default}`.

---

## 1. Project Setup

| Command | Description |
|---|---|
| `aqueduct init` | Scaffold project in CWD: `aqueduct.yml.template`, `blueprints/`, `arcades/`, `aqtests/`, `aqscenarios/`, `patches/{pending,rejected}/`, plus a `.gitignore` (Spark/Aqueduct runtime junk + `.env`); runs `git init` + first commit. Existing files (incl. a user `.gitignore`) are never overwritten |
| `aqueduct doctor` | Live connectivity probe: config, stores, agent reachability, Spark version, blueprint sources. No argument → checks `aqueduct.yml` in CWD |
| `aqueduct doctor <file>` | File type detected by version header (`aqueduct_config:` → config probe; `aqueduct:` → also probes Ingress/Egress paths + JDBC) |
| `aqueduct doctor --skip-spark` | Skip JVM startup — fast CI health check |
| `aqueduct doctor --aqtest <path>` | Schema pre-flight on a `.aqtest.yml` file (blueprint ref + module IDs) |
| `aqueduct doctor --aqscenario <path>` | Schema pre-flight on a `.aqscenario.yml` file (blueprint ref + `inject_failure.module`) |
| `aqueduct doctor --preflight` | Build a real (unbounded) Spark session + storage check instead of the default fast bounded reachability probe |
| `aqueduct doctor --verbose` | Expand skipped / low-signal rows (default view is actionable rows only) |
| `aqueduct doctor --env-file <path>` / `-e KEY=VAL` | Fallback `.env` / per-var override. Disable discovery with `AQ_NO_ENV_FILE=1` |

---

## 2. Development Loop

| Command | Description |
|---|---|
| `aqueduct validate <file>...` | Static parse + schema check, no side effects. File type auto-detected by version header (`aqueduct:` → Blueprint, `aqueduct_config:` → engine config). Accepts multiple files. No argument → validates `aqueduct.yml` in CWD. Subsumes the old `check-config`. |
| `aqueduct validate --env-file <path>` / `-e KEY=VAL` | Fallback `.env` / per-var override so `${VAR}` placeholders resolve. `AQ_NO_ENV_FILE=1` disables discovery |
| `aqueduct compile <blueprint>` | Output fully-resolved Manifest JSON to stdout |
| `aqueduct compile <blueprint> --show {manifest\|provenance\|inputs\|all}` | Select which slice of the compiled artefact to emit. Default `manifest` = current JSON output. `provenance` and `inputs` render readable tables for debugging. |
| `aqueduct run <blueprint>` | Compile and execute the full pipeline |
| `aqueduct test <file.aqtest.yml>` | Run isolated module tests against inline DataFrames — no sources/sinks. Always runs on `local[*]` (ignores `deployment.master_url`); `--master <url>` overrides for cluster-runtime-dependent modules |

**`aqueduct run` flags**

| Flag | Description |
|---|---|
| `--from <module_id>` | Start execution from this module (inclusive); skip upstream |
| `--to <module_id>` | Stop at this module (inclusive); skip downstream |
| `--execution-date YYYY-MM-DD` | Pin `@aq.date.*` / `@aq.runtime.timestamp()` for backfill idempotency |
| `--resume <run_id>` | Skip modules with checkpoints from a prior run (`checkpoint: true`) |
| `--parallel` | Execute independent DAG branches concurrently |
| `--ctx key=value` | Override a Blueprint `context:` variable (repeatable) |
| `--profile <name>` | Activate a `context_profiles:` entry |
| `--run-id <uuid>` | Override auto-generated run UUID |
| `--env-file <path>` | Fallback `.env` if no `.env` beside the blueprint/config. `-e KEY=VAL` per-var override (repeatable); `AQ_NO_ENV_FILE=1` disables discovery |
| `--store-dir <path>` | Override observability store directory |
| `--allow-aggressive` | Allow `approval_mode: aggressive` without setting `danger.allow_aggressive_patching: true` |

```bash
# Examples
aqueduct run pipeline.yml
aqueduct run pipeline.yml --from clean_orders --to write_output
aqueduct run pipeline.yml --execution-date 2026-01-15          # backfill
aqueduct run pipeline.yml --ctx env=prod --profile prod
aqueduct -v run pipeline.yml                                    # verbose
```

---

## 3. Observability

| Command | Description |
|---|---|
| `aqueduct runs` | List recent runs across all blueprints |
| `aqueduct runs --blueprint <path>` | Filter by blueprint |
| `aqueduct runs --failed` | Show only failed runs |
| `aqueduct runs --last <n>` | Limit to last N runs |
| `aqueduct report <run_id>` | Flow Report for a completed run |
| `aqueduct report <run_id> --format json` | Machine-readable output (`table` \| `json` \| `csv`) |
| `aqueduct lineage <blueprint>` | Column-level lineage graph. `<blueprint>` is the Blueprint **file path** (not a bare ID) — `aqueduct lineage` looks up the blueprint_id from the parsed file. |
| `aqueduct lineage <blueprint> --from <table>` | Filter lineage to a source table |
| `aqueduct lineage <blueprint> --column <col>` | Trace a single column |
| `aqueduct signal <signal_id>` | Show current gate override status |
| `aqueduct signal <signal_id> --value false` | Close gate (block downstream) |
| `aqueduct signal <signal_id> --error "reason"` | Close gate with reason string |
| `aqueduct signal <signal_id> --value true` | Clear override (resume normal evaluation) |

Gate overrides are persistent across runs until explicitly cleared.

---

## 4. LLM Self-Healing

| Command | Description |
|---|---|
**`aqueduct heal` is production-only** — it heals a real failed run. Scenario evaluation is a separate concern: `aqueduct benchmark`.

| Command | Description |
|---|---|
| `aqueduct heal <run_id>` | Trigger LLM patch generation for a failed run (reads its `FailureContext` from the observability store, stages a real patch) |
| `aqueduct heal <run_id> --module <id>` | Scope healing to one module |
| `aqueduct heal <run_id> --print-prompt` | Print the system + user prompt that would be sent, then exit (no LLM call) |
| `aqueduct heal <run_id> --print-prompt json` | Same, as `{"system": "...", "user": "..."}` JSON (bare `--print-prompt` = text) |
| `aqueduct benchmark <file-or-dir>` | Evaluate one `.aqscenario.yml` **or** a directory of them (recursive) against one or more models; prints a comparison table. Path is positional or `--scenarios`. No Spark |
| `aqueduct benchmark <path> --model <id>` | Repeatable: `--model A --model B` for multi-model comparison. Defaults to `agent.model` |
| `aqueduct benchmark <path> --provider <p> --base-url <url> --timeout <s>` | Per-run agent-connection override (precedence: flag > `aqueduct.yml` agent > default). `--timeout 0` = unbounded read (connect still fails fast) |
| `aqueduct benchmark <path> --workers <n>` | Concurrency. Default `1` (serial); `>1` parallelizes scenario×model pairs |
| `aqueduct benchmark <path> --format json` | Per-result JSON incl. the generated `patch`, `diag_score`, `soft_failures`. Table mode prints a `(N failed — rerun with --format json …)` hint |
| `aqueduct benchmark <path> --no-persist` | Skip writing the per-result row to `<scenarios_dir>/.aqueduct/benchmark.duckdb`. Default = persist for later regression diffs |
| `aqueduct benchmark <path> --store-path <file>` | Override the benchmark-store DuckDB path |
| `aqueduct benchmark <path> --gate-on-regression` | After persist, diff each (scenario, model) vs prior row; exit non-zero on any regression (drop in `passed`, `patch_applies`, > 5pp `diag_score` drop) |
| `aqueduct benchmark-diff` | Read the benchmark store, compare the two most-recent rows per (scenario, model), print a `NEW / = same / ↑ IMPROVE / ✗ REGRESS` table; exit non-zero on regression |
| `aqueduct benchmark-diff --scenario <id>` / `--model <id>` / `--store-path <file>` / `--format text\|json` | Filters + output shape for the diff command |

Scoring is two-tier: **correctness** (`patch_is_valid`, `patch_applies`, `expected_patch`) gates PASS/FAIL; **diagnosis quality** (`root_cause_contains`, `expected_category`, `max_attempts`, `min_confidence`) is recorded as `diag_score` but never fails an otherwise-correct fix. Use `aqueduct benchmark` to compare models or catch regression after prompt changes.

### Multi-axis budget (1.1.0)

`aqueduct run` self-heal AND `aqueduct benchmark` share the same `agent.budget:` block in `aqueduct.yml`. Six axes, first to trip terminates the loop and the reason is recorded as `stop_reason`:

| Axis | Default | `stop_reason` when tripped |
|---|---|---|
| `max_reprompts` | 5 | `exhausted_attempts` |
| `max_seconds` | 120 | `budget_seconds_exceeded` |
| `max_tokens_total` | 50000 | `budget_tokens_exceeded` |
| `same_error_consecutive` | 2 | escalation (temp=0.8 + skeleton template) for ONE attempt, then `stuck_signature` |
| `same_signature_overall` | 3 | `stuck_signature` |
| `progress_stalled_window` | 3 | `progress_stalled` |

`stop_reason` also `solved` (LLM returned a parseable PatchSpec) or `api_error` (provider raised). Every value is in `aqueduct.agent.budget.STOP_REASONS`.

> **`solved` ≠ "heal worked".** `stop_reason='solved'` only means the LLM loop terminated cleanly with a parseable patch. The patch may still fail the apply / lineage / sandbox / explain gates downstream. To check whether the heal actually fixed the pipeline, query `healing_outcomes.run_success_after_patch` for the same `run_id` (or `parent_run_id` in aggressive mode).

### `heal_attempts` table (1.1.0)

`observability.db.heal_attempts` records one row per LLM turn (success or failure). Columns: `attempt_num`, `error_class`, `where_field`, `normalized_message`, `signature_hash` (stable 16-char sha1), `tokens_in`, `tokens_out`, `latency_ms`, `gate_that_rejected` (`schema` / `apply` / `provider` / NULL on success), `escalated`, `stop_reason`, `prompt_version`. Populated by `aqueduct run` self-heal. Query directly via `duckdb` to post-mortem any heal.

### Structured Spark errors (1.1.0)

When the failure exception is a `PySparkException` or `Py4JJavaError`, the Surveyor extracts `error_class`, `object_name`, `suggested_columns` (from `UNRESOLVED_COLUMN.WITH_SUGGESTION` proposals), `sql_state`, and `root_exception` into `failure_contexts`. The agent prompt renders a `## Root cause (structured)` block instead of the raw stack trace whenever any structured field is present. Behaviour is automatic — no CLI flag. Inspect the rendered prompt via `aqueduct heal <run_id> --print-prompt`.

---

## 5. Patch Lifecycle

Patches live in `patches/pending/`, `patches/applied/`, and `patches/rejected/`.
All `patch` commands derive the `patches/` root by walking up from the blueprint to `aqueduct.yml`.

| Command | Description |
|---|---|
| `aqueduct patch list` | Show pending patches (walk-up from CWD) |
| `aqueduct patch list --status all` | Show pending + applied + rejected |
| `aqueduct patch preview <file> --blueprint <path>` | Phase 29a — render unified diff + lineage-gate impact. Add `--sandbox` to also run sandbox-gate replay (`--sample N`, default 1000; `--sample 0` = unbounded). `--format json` for machine-readable report. |
| `aqueduct patch apply <file> --blueprint <path>` | Validate and apply patch to Blueprint; archive to `applied/` |
| `aqueduct patch reject <file\|slug> --reason <text>` | Move patch to `rejected/`; record reason |
| `aqueduct patch commit --blueprint <path>` | `git add` + `git commit` for all applied patches since last commit |
| `aqueduct patch discard --blueprint <path>` | Restore Blueprint to HEAD; move applied patches back to `pending/` |

**Shared flags**

| Flag | Description |
|---|---|
| `--blueprint <path>` | Anchor for `patches/` root derivation |
| `--patches-dir <path>` | Explicit patch root (overrides walk-up) |
| `--reason <text>` | Required by `patch reject` |
| `--status pending\|applied\|rejected\|all` | Filter for `patch list` |

---

## 6. Git History & Rollback

| Command | Description |
|---|---|
| `aqueduct patch log <blueprint>` | Parse git log for `---aqueduct---` patch commits; show table |
| `aqueduct patch log <blueprint> --format json` | Machine-readable output |
| `aqueduct patch rollback <blueprint> --to <patch_id>` | Restore blueprint file(s) to pre-patch state; creates a new forward commit (non-destructive) |

`aqueduct patch rollback` is file-scoped — it touches only the files changed by the target patch commit, not the entire repo.

---

## `aqueduct doctor` Checks

| Check | What it verifies |
|---|---|
| Config | `aqueduct.yml` loads and parses without error |
| Depot | DuckDB depot file readable/writable |
| Observability | `observability.db` accessible |
| Lineage | `lineage.db` accessible |
| Agent | Self-healing config sanity; warns only when intent is explicit (opt-in — silent when simply unconfigured) |
| Spark | Default: fast bounded TCP reachability probe (master + S3 endpoint, no session). `--preflight`: real unbounded session + version + storage. `--skip-spark`: skipped |
| Blueprint sources (`--blueprint`) | Ingress/Egress paths exist; format/extension mismatches flagged |
| Arcade sub-blueprints (`--blueprint`) | Recursive source check with full context injection |
| `.aqtest.yml` schema (`--aqtest`) | aqueduct_test version, blueprint reference resolves, every test case's `module` exists in the referenced blueprint, assertions declared |
| `.aqscenario.yml` schema (`--aqscenario`) | aqueduct_scenario version, blueprint reference resolves, `inject_failure.module` names a real module |

---

## Exit Codes

Stable contract (`aqueduct/exit_codes.py`, `docs/STABILITY.md`) — SemVer applies; downstream tooling may key off these.

| Code | Name | Meaning |
|---|---|---|
| `0` | SUCCESS | Command completed without error |
| `1` | CONFIG_ERROR | Malformed `aqueduct.yml` or Blueprint schema error |
| `2` | DATA_OR_RUNTIME | Runtime failure — Spark error, Assert failure, missing file, network |
| `3` | HEAL_PENDING | Healing staged a patch for human review (`human` / `ci` mode) |
| `4` | VALIDATION_GATE | A validation-pyramid gate (guardrails / lineage / sandbox / explain) rejected a patch non-interactively |
| `5` | USAGE_ERROR | Invalid flag / missing arg (Click) |
