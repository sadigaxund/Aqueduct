# Aqueduct CLI Reference

Commands grouped by workflow. All commands accept `--config <path>` to point at a
non-default `aqueduct.yml`; Aqueduct also walks up from the CWD automatically.

**Global flags** (placed before the subcommand):

| Flag | Description |
|---|---|
| `--version` | Print the installed `aqueduct-core` version (sourced from `importlib.metadata`) and exit. |
| `-v`, `--verbose` | DEBUG logging — LLM responses, SQL plans, resolver steps. |
| `--log-format text\|json` | `text` (default) is the human-readable formatter. `json` emits one JSON object per record (`ts`, `level`, `logger`, `msg`, plus `exc` when an exception was logged) for shipping to Loki / Splunk / Datadog. |

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

**`.env` resolution** (shared by `run` / `doctor` / `validate`): first
existing of `--env-file` → `<input-file dir>/.env` → `<cwd>/.env`. Existing
environment variables are never overwritten. The "loaded N variable(s)"
line is `aqueduct -v` (DEBUG) only — silent at default verbosity.

---

## 1. Project Setup

| Command | Description |
|---|---|
| `aqueduct init` | Scaffold project in CWD: `aqueduct.yml.template`, `blueprints/`, `arcades/`, `tests/`, `benchmarks/`, `patches/{pending,rejected}/`; runs `git init` + first commit |
| `aqueduct doctor` | Live connectivity probe: config, stores, agent reachability, Spark version, blueprint sources. No argument → checks `aqueduct.yml` in CWD |
| `aqueduct doctor <file>` | File type detected by version header (`aqueduct_config:` → config probe; `aqueduct:` → also probes Ingress/Egress paths + JDBC) |
| `aqueduct doctor --skip-spark` | Skip JVM startup — fast CI health check |
| `aqueduct doctor --aqtest <path>` | Schema pre-flight on a `.aqtest.yml` file (blueprint ref + module IDs) |
| `aqueduct doctor --aqscenario <path>` | Schema pre-flight on a `.aqscenario.yml` file (blueprint ref + `inject_failure.module`) |
| `aqueduct doctor --env-file <path>` / `--no-env-file` | Override / disable `.env` discovery |

---

## 2. Development Loop

| Command | Description |
|---|---|
| `aqueduct validate <file>...` | Static parse + schema check, no side effects. File type auto-detected by version header (`aqueduct:` → Blueprint, `aqueduct_config:` → engine config). Accepts multiple files. No argument → validates `aqueduct.yml` in CWD. Subsumes the old `check-config`. |
| `aqueduct validate --env-file <path>` / `--no-env-file` | Override / disable `.env` discovery before validation (so `${VAR}` placeholders resolve) |
| `aqueduct compile <blueprint>` | Output fully-resolved Manifest JSON to stdout |
| `aqueduct compile <blueprint> --show {manifest\|provenance\|inputs\|all}` | Select which slice of the compiled artefact to emit. Default `manifest` = current JSON output. `provenance` and `inputs` render readable tables for debugging. |
| `aqueduct run <blueprint>` | Compile and execute the full pipeline |
| `aqueduct test <file.aqtest.yml>` | Run isolated module tests against inline DataFrames — no Spark I/O |

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
| `--env-file <path>` | Load `.env` file; omit to auto-discover `.env` at project root |
| `--no-env-file` | Disable `.env` auto-discovery |
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
| `aqueduct heal <run_id>` | Trigger LLM patch generation for a failed run |
| `aqueduct heal <run_id> --module <id>` | Scope healing to one module |
| `aqueduct heal --scenario <file.aqscenario.yml>` | Simulated failure — no Spark required; tests LLM diagnosis against expected assertions |
| `aqueduct heal <run_id> --print-prompt` | Print the system + user prompt that would be sent to the LLM, then exit (no LLM call) |
| `aqueduct heal <run_id> --print-prompt --print-prompt-format json` | Same, but output as `{"system": "...", "user": "..."}` JSON |
| `aqueduct benchmark --scenarios <dir>` | Run all `.aqscenario.yml` files and print accuracy table |
| `aqueduct benchmark --scenarios <dir> --model <id>` | Benchmark a specific model (repeatable for multi-model comparison) |
| `aqueduct benchmark --scenarios <dir> --workers <n>` | Parallel execution |

`.aqscenario.yml` files define a synthetic `FailureContext` + expected patch assertions.
Use `aqueduct benchmark` to compare models or measure regression after prompt changes.

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
| LLM reachability | HTTP GET to configured `base_url` succeeds |
| Spark version | JVM starts; PySpark version reported |
| Blueprint sources (`--blueprint`) | Ingress/Egress paths exist; format/extension mismatches flagged |
| Arcade sub-blueprints (`--blueprint`) | Recursive source check with full context injection |
| `.aqtest.yml` schema (`--aqtest`) | aqueduct_test version, blueprint reference resolves, every test case's `module` exists in the referenced blueprint, assertions declared |
| `.aqscenario.yml` schema (`--aqscenario`) | aqueduct_scenario version, blueprint reference resolves, `inject_failure.module` names a real module |

---

## Exit Codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Pipeline failure, validation error, or CLI error |
| `2` | Patch apply failed |
| `3` | Assert rule violation (`on_fail: abort`) |
