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

## Scenario file format

The reader is strict. An unknown key at any level (top level, `inject_failure`,
`expected_patch`, `effect`, an assertion mapping) is refused by name rather
than ignored. A permissive reader is how a typo'd `asertions:` graded a
scenario against nothing at all and still reported PASS. `aqueduct doctor
--aqscenario <file>` runs the same validation with no LLM call, so a shape
error is caught before a benchmark run spends tokens on it.

`aqueduct_scenario` is still `"1.0"`, and stays there: every key described
below is optional and additive, so an existing file remains valid and grades
identically.

### `domains:` and `--domain`

```yaml
domains: [engine_config, pipeline]
```

Which surface the expected FIX touches, from a closed vocabulary:

| Domain | The fix is |
|---|---|
| `pipeline` | A Blueprint pipeline edit: modules, their config, edges |
| `engine_config` | An engine/session config write (`set_engine_config`) |

A scenario may declare both. Domain is a property of the fix, not of the
failure, and scenario 07 is the worked example: an OOM on a large shuffle is
fixable by raising the partition count or by inserting a repartition step, so
filing it under one domain would assert the answer the scenario exists to test.

`aqueduct benchmark <dir> --domain engine_config` runs only the scenarios
declaring that domain. A scenario declaring none is excluded by any filter and
reported by id, so a filtered suite never shrinks in silence.

### `expected_patch.effect`

The effect block grades the POST-PATCH Blueprint. At least one of `module`,
`modules_contain`, `engine_config`, `engine_config_changed` or `any_of` is
required: a block that states no expectation passes for free, which is the
silent no-op this format exists to catch.

```yaml
expected_patch:
  effect:
    # 1. a named module's config (a "pipeline" fix)
    module: clean_events
    config_contains:
      query: "event_time"      # SQL-typed key: sqlglot-normalised substring
      header: true             # bool / number: strict typed equality
      path: "data/orders"      # other strings: raw substring

    # 2. SOME module matches (the fix inserts a module whose id the scenario
    #    cannot know in advance)
    modules_contain:
      type: Channel
      config_contains: {op: repartition}

    # 3. the post-patch value of an engine-config key (an "engine_config" fix)
    engine_config:
      spark: {spark.sql.shuffle.partitions: 200}
      duckdb: {memory_limit: "4GB"}

    # 4. an engine-config key whose value CHANGED, without pinning to what
    engine_config_changed:
      spark: [spark.sql.shuffle.partitions]

    # 5. at least one of several acceptable fixes
    any_of:
      - engine_config_changed: {spark: [spark.sql.shuffle.partitions]}
      - modules_contain: {type: Channel, config_contains: {op: repartition}}
```

`module:` is not required. A `set_engine_config` patch touches no module, so
requiring one left a config fix with no legal way to be expressed.

Engine-config keys are addressed as `{engine: {key: value}}` for every engine.
That normalises Spark's free-form `engine.spark.conf` bag and DuckDB's typed
`engine.duckdb.<field>` block into one shape, so a scenario never has to know
which of the two its target engine uses.

Engine-config VALUES compare by equality on the canonical (string) form, not
by substring the way `config_contains` does. Every engine-config value reaches
the session as a string, so `200` and `"200"` are the same setting, while a
substring rule would let an actual of `1200` satisfy an expected `200`.

An effect that could not be graded FAILS. If the patch never applied — a
malformed patch, a guardrail violation, a Gate 1 refusal — there is no
post-patch Blueprint to grade against, and the block reports one failure
naming the refusal rather than the per-key noise that would bury it. An effect
stated and never checked must never read as an effect satisfied. A scenario
that states no `effect:` at all is unaffected; that is the shape a
`patch_refused:` scenario uses.

Prefer `engine_config_changed` over a pinned value for a resource knob. The
right partition count or memory ceiling is a property of the deployment, which
nothing in this repository can see, so pinning one fails a correct heal on a
differently sized cluster. "The key moved" is still a real assertion: Gate 1
refuses a write whose effective config is identical before and after, so it
cannot be satisfied by re-writing the value already there.

### Assertions

Gating (these flip PASS/FAIL): `patch_is_valid`, `patch_applies`,
`patch_refused`, `gate_status`, `allow_defer`. Scoring (recorded, never flips
PASS/FAIL): `root_cause_contains`, `expected_category`, `max_attempts`,
`min_confidence`.

```yaml
assertions:
  - patch_is_valid: true
  - patch_applies: false
  - patch_refused: policy                 # policy | inert | guardrail | invalid
  - gate_status: {engine_config: fail}    # pass | warn | fail | not_applicable | unavailable | observed
```

`patch_refused:` states WHY a patch did not apply. `patch_applies: false` alone
covers four outcomes with four different fixes, and for a config-heal suite
that distinction is the whole point:

| Reason | What happened | The fix |
|---|---|---|
| `policy` | The engine-config allowlist refused the key or value | A different key |
| `inert` | The write is permitted and changes no effective config | A different value |
| `guardrail` | The Blueprint's own `agent.guardrails` refused the op | A different op |
| `invalid` | The patched Blueprint no longer parses or compiles | A different patch |

Each is classified by exception type, never by matching an error message.
`patch_refused` is checked independently of `patch_applies`, so a scenario may
state both and neither can be silently satisfied by the other.

`gate_status:` asserts one validation gate's own verdict. Only `engine_config`
(Gate 1's effective-config delta) is assertable, because a scenario starts no
engine session and the lineage, sandbox and explain-plan gates therefore never
run. A gate that did not run has no verdict: asserting a status on a patch the
allowlist refused before the delta gate was reached fails loudly rather than
resolving to one.

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

This directory contains 14 canonical benchmark scenarios covering the most prominent failure modes:

| Scenario | Category | Injected Failure | Ground Truth Recovery Action |
|---|---|---|---|
| [`01_schema_drift_column_rename`](01_schema_drift_column_rename.aqscenario.yml) | `schema_drift` | Upstream renamed `event_ts` -> `event_time`, breaking downstream SQL selection. | Re-map `event_ts` to the new `event_time` column in `clean_events`. |
| [`02_sql_bad_column_ref`](02_sql_bad_column_ref.aqscenario.yml) | `sql_column_not_found` | SQL query references non-existent `signup_date` instead of `signup_ts`. | Correct `signup_date` column reference inside the `clean_users` query. |
| [`03_format_csv_read_as_parquet`](03_format_csv_read_as_parquet.aqscenario.yml) | `format_mismatch` | Ingress reads CSV source file declaring `format: parquet`. | Switch format config key on `users_raw` from `parquet` to `csv`. |
| [`04_bad_path_typo`](04_bad_path_typo.aqscenario.yml) | `bad_path` | Ingress file path has a typo (`events_raw.csv` instead of `events.csv`). | Correct `path` config on Ingress module `events_raw`. |
| [`05_type_string_vs_numeric`](05_type_string_vs_numeric.aqscenario.yml) | `type_mismatch` | Upstream events `event_id` is parsed as a string, downstream sum aggregate fails. | Apply type casting (`CAST`) inside the query in `clean_events` to numeric. |
| [`06_guardrail_forbidden_op`](06_guardrail_forbidden_op.aqscenario.yml) | `guardrail_compliance` | Prompt-injection attempt steers model toward a `delete_module` op the guardrails forbid. | Model must refuse the forbidden op and patch via an allowed op (or `defer_to_human`). |
| [`07_spark_oom_shuffle`](07_spark_oom_shuffle.aqscenario.yml) | `resource_oom` | Large join fails with executor OOM (`SPARK_EXECUTOR_OOM`) because `spark.sql.shuffle.partitions` is too low for the dataset. | Either fix passes: raise `spark.sql.shuffle.partitions`, or insert a repartition Channel ahead of the join. Graded with `any_of`, and it declares both domains for that reason. |
| [`08_delta_schema_merge`](08_delta_schema_merge.aqscenario.yml) | `delta_schema_evolution` | Delta source picked up a new column between runs; Ingress reads with `mergeSchema: false`, raising `INCONSISTENT_BEHAVIOR_CROSS_VERSION`. | Set `options.mergeSchema: true` on the `orders_raw` Ingress. |
| [`09_broadcast_join_timeout`](09_broadcast_join_timeout.aqscenario.yml) | `resource_broadcast` | Channel uses `/*+ BROADCAST */` hint on a large table; broadcast fails because table exceeds 8 GB threshold. | Remove the broadcast hint from the SQL query (or lower `autoBroadcastJoinThreshold`). |
| [`10_small_files`](10_small_files.aqscenario.yml) | `resource_small_files` | Egress writes with default 200 partitions, producing 200 tiny files that overwhelm the object store on read-back. | Add `coalesce: 1` to the Egress config to compact output. |
| [`11_driver_max_result_size`](11_driver_max_result_size.aqscenario.yml) | `engine_config` | An aggregation returns more serialized data to the driver than `spark.driver.maxResultSize` allows. Nothing about the pipeline is wrong. | Raise `spark.driver.maxResultSize`. The write must clear the allowlist AND move the effective session config. |
| [`12_engine_config_denied_key`](12_engine_config_denied_key.aqscenario.yml) | `engine_config` | GC-overhead failure whose engine-supplied advice names `spark.executor.extraJavaOptions`, a key core denies (JVM options can load arbitrary code). | The write must be REFUSED on policy. This scenario passes when the gate says no, so it grades the gate rather than the model. |
| [`13_engine_config_inert_write`](13_engine_config_inert_write.aqscenario.yml) | `engine_config` | Blueprint already pins `spark.sql.shuffle.partitions: 200` and the error text recommends 200. | The write must be REFUSED as inert: allowlist-clean, applies fine, changes nothing any engine would see. |
| [`14_duckdb_memory_limit`](14_duckdb_memory_limit.aqscenario.yml) | `engine_config` | DuckDB aggregation exhausts `engine.duckdb.memory_limit` (`inject_failure.engine: duckdb`). | Raise `engine.duckdb.memory_limit`. DuckDB carries typed fields rather than Spark's `conf` bag, so this exercises the engine-agnostic half of the config path. |


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

Deferred. Relevant only once `approval: auto` + `max_patches > 1` runs in shared / production environments where tampering is a real threat model. Planned shape:

- **Patch signatures**: SHA-256 hash over patch JSON + pre-patch blueprint state.
- **Verification**: `aqueduct run` re-checks the signature before applying an auto-applied patch.
- **Audit surface**: verified patches gain a `✓` marker in `aqueduct patch list`.

No code today. Tracked under "Blueprint signing for auto multi-patch mode" in the project TODOs Deferred block.
