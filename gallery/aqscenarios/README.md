# Aqueduct Benchmarks (`aqscenarios`)

The Benchmarking suite is a data-driven evaluation layer designed to measure and visualize the self-healing reliability of Aqueduct across different LLM providers and prompt versions.

It enables model selection based on evidence, catches prompt regressions in CI, and serves as a public leaderboard for Aqueduct's autonomous capabilities.

## How It Works

Scenario evaluations run completely offline and require no heavy Apache Spark sessions. They operate under a modular, highly reusable architecture:

1. **One blueprint per scenario, each with exactly one real defect**: every scenario points at its own blueprint under [blueprints/](blueprints/) (`<NN_id>.yml`) — a copy of the same pipeline carrying the single defect that scenario is about (a wrong column, a `format`/`path` mistake, a type bug). The agent's job is to *patch that blueprint*, so the defect must actually be present in it — a "clean" blueprint plus an unrelated error would be unsolvable and ungradable.
2. **Realistic `inject_failure`**: `error_message` is written to match what Spark/Aqueduct **actually emits** for that defect (error class, `SQLSTATE`, the `Did you mean …?` suggestion list, the unresolved-plan relation schema). Nothing extra is spoon-fed — only what a real run would print. The runner builds the FailureContext from the (compilable) blueprint + this error; no Spark session needed.
3. **Prompt & verification**: the failure context becomes the diagnostic prompt. The LLM's recovery patch is parsed against the `PatchSpec` schema, applied back to the blueprint, and re-compiled.
4. **Op-agnostic scoring**: scenarios assert on *outcome + diagnosis* (`patch_is_valid`, `patch_applies`, `root_cause_contains`), not a hard-coded op name — a correct fix via any valid op passes. (True runtime correctness — "does the patched query actually run" — needs the future `--execute` mode; see the project TODOs Phase 33.)

The defect lives in the blueprint and the error mirrors a real Spark failure, so each scenario is a faithful, gradable recovery task — not a narrative bolted onto a healthy pipeline.

## The `benchmark` Command

Aqueduct includes a native benchmarking CLI to run the scenario suite against one or more models:

```bash
# Run the full suite against the default model
aqueduct benchmark gallery/aqscenarios/

# A single scenario
aqueduct benchmark gallery/aqscenarios/format_csv_read_as_parquet.aqscenario.yml

# Compare multiple models side-by-side (--model is repeatable)
aqueduct benchmark gallery/aqscenarios/ --model claude-opus-4-7 --model llama3
```

### Testing with Local Models (Ollama / Custom)

**Ad-hoc (no config file)** — override the connection on the command line.
`benchmark` takes the connection triad as flags (precedence: flag >
`aqueduct.yml` `agent` > built-in default):

```bash
aqueduct benchmark gallery/aqscenarios/ \
  --provider openai_compat \
  --model smallthinker:3b \
  --base-url http://<OLLAMA_ADDRESS>:11434/v1 \
  --timeout 600
```

`openai_compat` needs no API key. `--model` is repeatable for a
multi-model comparison. Raise `--timeout` (default 120s) for large or
cold local models — a 7B+ model loading into VRAM on its first call
routinely exceeds 120s; pre-warming it (`ollama run <model>` once) also
avoids the cold-start hit. `--timeout 0` = no limit (unbounded read; the
connect phase still fails fast if the host is unreachable). Unbounded is
never the default — one stuck model would hang the whole suite.

**Durable / CI** — put the connection in `aqueduct.yml` and run with
`--config` (or from its directory):

```yaml
agent:
  provider: openai_compat
  model: "smallthinker:3b"
  base_url: "http://localhost:11434/v1"  # or remote, e.g. http://<OLLAMA_ADDRESS>:11434/v1
```

Provider tuning (`provider_options` — temperature, ollama opts, …) and
`guardrails` are **config-only**; there are no flags for them. `-e
KEY=VAL` is a generic env primitive — it only affects agent config if
`aqueduct.yml` explicitly references `${KEY}`. It is **not** a shortcut
for setting the provider/model/base-url; use the flags above.

### Example Comparison Output

| Scenario                      | claude-3.5-sonnet | llama-3-70b | gpt-4o |
| :---------------------------- | :---------------- | :---------- | :----- |
| `01_schema_drift_column_rename`  | **PASS** 0.94     | **PASS** 0.81 | FAIL   |
| `04_bad_path_typo`               | **PASS** 0.99     | **PASS** 0.88 | **PASS** 0.72 |
| `oom_config_fix`              | FAIL              | FAIL        | FAIL   |
| ...                           |                   |             |        |
| **Parse rate**                | 100%              | 92%         | 87%    |
| **Apply rate**                | 91%               | 85%         | 79%    |

## Canonical Scenarios

The goal is to maintain 20–30 canonical scenarios covering the most frequent data engineering failure classes:
- **Schema Drift**: Column renames, type changes, missing fields.
- **Pathing Errors**: Typos, incorrect S3/DBFS prefixes, missing partitions.
- **Format Mismatches**: CSV vs. Parquet vs. Delta confusion.
- **Resource/OOM**: Memory config fixes, executor tuning.
- **SQL Errors**: Column not found, invalid window functions, syntax errors.

### Implemented Example Scenarios

This directory contains 5 canonical benchmark scenarios covering the most prominent failure modes:

| Scenario | Category | Injected Failure | Ground Truth Recovery Action |
|---|---|---|---|
| [`01_schema_drift_column_rename`](01_schema_drift_column_rename.aqscenario.yml) | `schema_drift` | Upstream renamed `event_ts` -> `event_time`, breaking downstream SQL selection. | Re-map `event_ts` to the new `event_time` column in `clean_events`. |
| [`02_sql_bad_column_ref`](02_sql_bad_column_ref.aqscenario.yml) | `sql_column_not_found` | SQL query references non-existent `signup_date` instead of `signup_ts`. | Correct `signup_date` column reference inside the `clean_users` query. |
| [`03_format_csv_read_as_parquet`](03_format_csv_read_as_parquet.aqscenario.yml) | `format_mismatch` | Ingress reads CSV source file declaring `format: parquet`. | Switch format config key on `users_raw` from `parquet` to `csv`. |
| [`04_bad_path_typo`](04_bad_path_typo.aqscenario.yml) | `bad_path` | Ingress file path has a typo (`events_raw.csv` instead of `events.csv`). | Correct `path` config on Ingress module `events_raw`. |
| [`05_type_string_vs_numeric`](05_type_string_vs_numeric.aqscenario.yml) | `type_mismatch` | Upstream events `event_id` is parsed as a string, downstream sum aggregate fails. | Apply type casting (`CAST`) inside the query in `clean_events` to numeric. |


## Scoring & Metrics

- **Parse Rate**: Percentage of LLM responses that correctly follow the `PatchSpec` JSON schema.
- **Apply Rate**: Percentage of patches that successfully pass internal validation and can be applied to the Blueprint.
- **Success Rate (PASS)**: Percentage of patches that result in a successful Spark run with correct data output.
- **Accuracy Score**: A decimal value (0.0 - 1.0) comparing the generated patch against the **Ground Truth** (`expected.patch`) using AST-based comparison.

## Prompt Versioning

Goal: correlate score improvements/regressions to specific system-prompt changes.

**Status — partially implemented:**
- `PROMPT_VERSION` constant (`aqueduct/agent/__init__.py`), manually bumped on significant prompt changes. ✅
- Stamped into applied-patch metadata (`_aq_meta.prompt_version`). ✅
- **Not yet:** `prompt_version` is *not* recorded in `healing_outcomes`, and benchmark results carry no `prompt_version` (benchmark has no persistence at all). So cross-version correlation / regression tracking is **not possible yet** — tracked as Phase 33 (benchmark persistence + `healing_outcomes.prompt_version`).

## Future: Integrity & Signing (Phase 25)

To support **Aggressive Mode** (autonomous patching without human review), we are implementing a signing layer:
- **Patch Signatures**: SHA-256 hashes of patches + blueprint state.
- **Verification**: `aqueduct run` verifies signatures before applying autonomous patches to detect tampering.
- **Audit Log**: Verified patches are surfaced in `aqueduct patch log` with a `✓` status.
