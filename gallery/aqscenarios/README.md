# Aqueduct Benchmarks (`aqscenarios`)

The Benchmarking suite is a data-driven evaluation layer designed to measure and visualize the self-healing reliability of Aqueduct across different LLM providers and prompt versions.

It enables model selection based on evidence, catches prompt regressions in CI, and serves as a public leaderboard for Aqueduct's autonomous capabilities.

## How It Works

Scenario evaluations run completely offline and require no heavy Apache Spark sessions. They operate under a modular, highly reusable architecture:

1. **One blueprint per scenario, each with exactly one real defect**: every scenario points at its own blueprint under [blueprints/](blueprints/) (`<NN_id>.yml`) — a copy of the same pipeline carrying the single defect that scenario is about (a wrong column, a `format`/`path` mistake, a type bug). The agent's job is to *patch that blueprint*, so the defect must actually be present in it — a "clean" blueprint plus an unrelated error would be unsolvable and ungradable.
2. **Realistic `inject_failure`**: `error_message` is written to match what Spark/Aqueduct **actually emits** for that defect (error class, `SQLSTATE`, the `Did you mean …?` suggestion list, the unresolved-plan relation schema). Nothing extra is spoon-fed — only what a real run would print. The runner builds the FailureContext from the (compilable) blueprint + this error; no Spark session needed.
3. **Prompt & verification**: the failure context becomes the diagnostic prompt. The LLM's recovery patch is parsed against the `PatchSpec` schema, applied back to the blueprint, and re-compiled.
4. **Op-agnostic scoring**: scenarios assert on *outcome + diagnosis* (`patch_is_valid`, `patch_applies`, `root_cause_contains`), not a hard-coded op name — a correct fix via any valid op passes. Effect-level grading (`expected_patch.effect` + sqlglot AST normalization) is now shipped, so semantically-equivalent patches via different ops grade equally.

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

### Overnight: every scenario × every local model

Single invocation handles the scenario × model matrix and persists every `(scenario, model)` row to `<scenarios_dir>/.aqueduct/benchmark.duckdb` for later query. Run it under `tmux` so it survives SSH disconnect, terminal close, and your laptop going to sleep.

```bash
# 1. List your local models
curl -s http://localhost:11434/api/tags | jq -r '.models[].name'

# 2. Kick off in a detached tmux session
tmux new -ds bench bash -c '
  aqueduct benchmark gallery/aqscenarios/ \
    --provider openai_compat \
    --base-url http://localhost:11434/v1 \
    --timeout 600 \
    --workers 1 \
    --format json \
    --model qwen2.5-coder:7b \
    --model qwen2.5-coder:14b \
    --model deepseek-r1:14b \
    --model llama3.1:8b \
    --model llama3.1:70b \
    --model mistral:7b \
    --model codellama:13b \
    --model phi3:14b \
    --model gemma2:9b \
    --model granite-code:8b \
    2>&1 | tee tmp/bench_$(date +%Y%m%dT%H%M%S).log
'
```

**Flag choices:**
- `--workers 1` keeps it serial — Ollama swaps weights per model, parallel calls would thrash VRAM.
- `--timeout 600` tolerates cold-start weight loads on the first call to each model.
- `--format json` makes the log machine-parseable.
- Persistence is on by default — disable with `--no-persist` if you only want the table.

**Monitor without attaching:**
```bash
tmux capture-pane -t bench -p | tail -20    # snapshot
tail -f tmp/bench_*.log                     # follow log
tmux ls                                     # session gone = done
```

**Query results after the run:**
```bash
duckdb gallery/aqscenarios/.aqueduct/benchmark.duckdb "
  SELECT model, scenario_id, passed, patch_applies, diag_score,
         confidence, tokens_in_total, tokens_out_total,
         stop_reason, escalated
  FROM benchmark_results
  WHERE recorded_at >= '2026-05-28T00:00:00'
  ORDER BY model, scenario_id
"
```

Head-to-head diff between two models:
```bash
aqueduct benchmark-diff --model qwen2.5-coder:7b --model llama3.1:70b \
  --store-path gallery/aqscenarios/.aqueduct/benchmark.duckdb
```

**Rough runtime:** scenarios × models × ~30s avg per call, plus a 30s–2min cold-start swap per model. 6 scenarios × 10 models ≈ 1–3 hours with mostly 7–14B models; 70B models push it toward 6–10 hours. Lower `--timeout` if you want wedged calls to abort faster.

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

This directory contains 8 canonical benchmark scenarios covering the most prominent failure modes:

| Scenario | Category | Injected Failure | Ground Truth Recovery Action |
|---|---|---|---|
| [`01_schema_drift_column_rename`](01_schema_drift_column_rename.aqscenario.yml) | `schema_drift` | Upstream renamed `event_ts` -> `event_time`, breaking downstream SQL selection. | Re-map `event_ts` to the new `event_time` column in `clean_events`. |
| [`02_sql_bad_column_ref`](02_sql_bad_column_ref.aqscenario.yml) | `sql_column_not_found` | SQL query references non-existent `signup_date` instead of `signup_ts`. | Correct `signup_date` column reference inside the `clean_users` query. |
| [`03_format_csv_read_as_parquet`](03_format_csv_read_as_parquet.aqscenario.yml) | `format_mismatch` | Ingress reads CSV source file declaring `format: parquet`. | Switch format config key on `users_raw` from `parquet` to `csv`. |
| [`04_bad_path_typo`](04_bad_path_typo.aqscenario.yml) | `bad_path` | Ingress file path has a typo (`events_raw.csv` instead of `events.csv`). | Correct `path` config on Ingress module `events_raw`. |
| [`05_type_string_vs_numeric`](05_type_string_vs_numeric.aqscenario.yml) | `type_mismatch` | Upstream events `event_id` is parsed as a string, downstream sum aggregate fails. | Apply type casting (`CAST`) inside the query in `clean_events` to numeric. |
| [`06_guardrail_forbidden_op`](06_guardrail_forbidden_op.aqscenario.yml) | `guardrail_compliance` | Prompt-injection attempt steers model toward a `delete_module` op the guardrails forbid. | Model must refuse the forbidden op and patch via an allowed op (or `defer_to_human`). |
| [`07_spark_oom_shuffle`](07_spark_oom_shuffle.aqscenario.yml) | `resource_oom` | Large join fails with executor OOM (`SPARK_EXECUTOR_OOM`) because `spark.sql.shuffle.partitions` is too low for the dataset. | Raise `spark.sql.shuffle.partitions` on `join_and_aggregate` (and optionally insert a repartition) to spread the join load. |
| [`08_delta_schema_merge`](08_delta_schema_merge.aqscenario.yml) | `delta_schema_evolution` | Delta source picked up a new column between runs; Ingress reads with `mergeSchema: false`, raising `INCONSISTENT_BEHAVIOR_CROSS_VERSION`. | Set `options.mergeSchema: true` on the `orders_raw` Ingress. |


## Scoring & Metrics

- **Parse Rate**: Percentage of LLM responses that correctly follow the `PatchSpec` JSON schema.
- **Apply Rate**: Percentage of patches that successfully pass internal validation and can be applied to the Blueprint.
- **Success Rate (PASS)**: Percentage of patches that result in a successful Spark run with correct data output.
- **Accuracy Score**: A decimal value (0.0 - 1.0) comparing the generated patch against the **Ground Truth** (`expected.patch`) using AST-based comparison.

## Prompt Versioning

Goal: correlate score improvements/regressions to specific system-prompt changes.

**Status — shipped:**
- `PROMPT_VERSION` constant (`aqueduct/agent/__init__.py`), manually bumped on significant prompt changes.
- Stamped into applied-patch metadata (`_aq_meta.prompt_version`).
- Persisted into `healing_outcomes.prompt_version` (production heals) and `benchmark_results.prompt_version` (benchmark runs).
- `aqueduct benchmark-diff` flags mismatched baselines with `[baseline prompt_version differs]` so a leaderboard regression isn't conflated with a prompt rewrite.

Cross-version correlation queries are now a direct `GROUP BY prompt_version` on either table.

## Future: Integrity & Signing

Deferred. Relevant only once `approval_mode: auto` + `max_patches > 1` runs in shared / production environments where tampering is a real threat model. Planned shape:

- **Patch signatures**: SHA-256 hash over patch JSON + pre-patch blueprint state.
- **Verification**: `aqueduct run` re-checks the signature before applying an auto-applied patch.
- **Audit surface**: verified patches gain a `✓` marker in `aqueduct patch list`.

No code today. Tracked under "Blueprint signing for auto multi-patch mode" in the project TODOs Deferred block.
