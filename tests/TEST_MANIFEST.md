# Aqueduct Test Manifest

## How to use this file
- ✅ = test implemented and passing
- ⏳ = test needed but not yet written
- ❌ = test failing (see failure report)

When adding a new feature, add a task under the relevant module with the exact function/class to test and expected behavior.

## Environment variables (tests/conftest.py)

| Variable | Default | Purpose |
|---|---|---|
| `AQ_SPARK_MASTER` | `local[1]` | Spark master URL. Set to `spark://host:7077` or `yarn` for remote clusters. |
| `AQ_PG_DSN` | _(unset)_ | Postgres DSN for integration tests. Example: `postgresql://user:pass@localhost:5432/db`. |
| `AQ_REDIS_URL` | `redis://localhost:6379/15` | Redis URL for depot store integration tests. |
| `ANTHROPIC_API_KEY` | _(unset)_ | Set in agent tests only to exercise the env-key branch; no live call is made. |

> No Ollama / live-LLM env vars. The unit suite never contacts a model — see
> the LLM testing policy below.

Spark artifacts are isolated to `/tmp/`:
- warehouse → `/tmp/aqueduct_test_spark_warehouse`
- metastore → in-memory Derby (`jdbc:derby:memory:aqueduct_test_metastore`)
- Derby log → `/tmp/aqueduct_test_derby.log`

---

## Test markers & execution modes

| Marker | Default | Needs |
|---|---|---|
| _(none)_ | runs | nothing — pure unit tests, deterministic |
| `integration` | **skipped** | live Postgres + Redis (see env vars) |
| `scenario` | **skipped** | scenario runner (`aqueduct scenario run`) — non-deterministic eval, not regression |

Commands:
```bash
pytest                                   # default: unit only — fully deterministic, no model
pytest -m integration                    # store integration (PG + Redis)
aqueduct scenario run                    # non-deterministic LLM eval (separate runner)
```

### LLM testing policy

**The unit suite NEVER contacts a model. There are no live-LLM fixtures or
markers.** A live model (e.g. `gemma3:12b` ≈ 8-12 GB) is slow, RAM-heavy and
flaky, and what it returns measures *model precision*, not Aqueduct code.

Agent-loop coverage is deterministic: `tests/test_surveyor/test_agent.py`
mocks `aqueduct.agent._call_agent` (reprompt loop, parse, dispatch, auto-apply)
or patches `httpx.post` with canned JSON (`_call_openai_compat` /
`_call_anthropic` provider routing). Assertions target structural properties
(patch JSON shape, retry count, prompt-section presence, guardrail rejection
reason, payload key routing) — never exact LLM output strings.

Non-deterministic model evaluation belongs to **scenarios** (`.aqscenario.yml`)
run via `aqueduct scenario run` — benchmark/eval, not pass/fail unit gates.

Any test that contacts a real LLM endpoint in `pytest` is a bug — rewrite with
a mock or move it into a `.aqscenario.yml`.

---

## Engine Feature Sanity 
This section tracks high-level functional verification of core features against the Technical Specifications.

### Phase A: Core Engine (Structure & Data Flow)
- ✅ **Cycle Detection:** Parser identifies and rejects circular dependencies.
- ✅ **Ingress Versatility:**
  - ✅ Formats: Parquet, Delta, CSV, JSON, JDBC, Kafka (via generic Spark pass-through).
  - ✅ `schema_hint` enforcement: Supports both flat-dict `{col: type}` and nested `{mode, columns}` formats. Normalizes type aliases.
- ✅ **Channel Operations:**
  - ✅ SQL temp view registration: Modules available by ID in SQL.
  - ✅ Macro Expansion: Parameterised `{{ macros.fn(args) }}` expand at compile time.
  - ✅ `__input__` alias: Auto-registration for single-input channels.
  - ✅ Native Ops: `deduplicate`, `filter`, `select`, `rename`, `cast`, `repartition`, `coalesce`, `cache`, `union` verified.
- ✅ **Sort Direction:** `sort` handles `DESC`/`ASC` via manual direction parsing (verified Phase 21C).
- ✅ **Junction (Fan-out):** `conditional` (filter-based), `broadcast` (zero-shuffle), and `partition` (key-based) modes.
- ✅ **Funnel (Fan-in):** `union_all` (zero-shuffle), `union` (distinct), `coalesce` (aligned), and `zip` (monotonically increasing ID join).
- ✅ **Egress Performance:**
  - ✅ Standard modes: `overwrite`, `append`, `error`, `ignore`.
  - ✅ `mode: merge`: Delta Lake `MERGE INTO` support with key-based upserts.
  - ✅ `partition_by`: Columns correctly passed to Spark writer.

### Phase B: Observability & Quality Gates
- ✅ **Assert Module (Inline):**
  - ✅ Aggregate Rules: `min_rows`, `null_rate`, `freshness`, `sql` batched into 1-2 Spark actions.
  - ✅ Row-level Rules: `sql_row` correctly routes failing rows to spillway port.
  - ✅ `spillway_rate`: Evaluated post-row-level; aborts if quarantine fraction exceeds threshold.
- ✅ **Probe Signals (Tap):**
  - ✅ Signal Battery: Schema, null_rates, distribution, distinct, freshness, partition_stats.
  - ✅ `row_count_estimate`: `method: sample` and `method: spark_listener` (with documented lazy limitation).
  - ✅ Cost Controls: `block_full_actions` suppresses costly signals in production mode.
  - ✅ Persistence: DuckDB `observability.db` stores signals with run_id/captured_at metadata.
- ✅ **Regulator (Gate):**
  - ✅ Passive Compile-away: Zero runtime overhead for unwired regulators.
  - ✅ Active Evaluation: Gate closes on `False` or Surveyor evaluation error.
  - ✅ `on_block` behaviors: `skip` (downstream propagation) vs `abort` vs `trigger_agent`.
- ✅ **Error Handling:**
  - ✅ Retry Logic: 3x retry on transient IO for Ingress/Egress.
  - ✅ Fail-fast: Blueprint `status="error"` recorded on unrecoverable failure.
  - ✅ Agent Signaling: `trigger_agent: true` set on result for self-healing.

### Phase C: Persistence & Advanced Logic
- ✅ **Checkpoint & Resume:**
  - ✅ `checkpoint: true` writes intermediate DataFrames to Parquet in `store_dir`.
  - ✅ `resume_run_id`: Reloads state and skips already-completed modules.
  - ✅ Manifest Hash: Validation warns if blueprint changed since checkpoint.
- ✅ **Arcade (Sub-pipelines):** Inlining, namespacing, and ID collision prevention.
- ✅ **Context Registry:**
  - ✅ Tier 1 Functions: `@aq.date.today`, `@aq.runtime.run_id`, `@aq.env`, `@aq.secret`.
  - ✅ Backfill: `--execution-date` flag correctly pins logical date functions.
  - ✅ Profile Priority: CLI flags > Env Vars > Profile Overrides > Static Defaults.
- ✅ **Depot KV Store:** State capture during Egress; Compile-time resolution for `@aq.depot.get`.
- ✅ **Job Planning:** Topo-sort execution; Parallel dispatch via `ThreadPoolExecutor` (Verified on Python 3.14).

### Phase D: Self-Healing & CLI Tooling
- ✅ **Patch Grammar (PatchSpec):** Normalization, atomicity (Atomic Revert), and guardrails.
- ✅ **Self-Healing Loop:**
  - ✅ Failure Capture: `Surveyor` records `FailureContext` to DuckDB.
  - ✅ Context Assembly: Evidence (logs, schema, provenance) passed to LLM.
  - ✅ Staging: `aqueduct heal` stages patches for human review.
  - ✅ Aggressive Mode: Autonomous fix-and-verify loop verified end-to-end.
  - ✅ Confidence Gate: Low-confidence patches escalate to human review.
- ✅ **CLI Tooling:**
  - ✅ `aqueduct init`: 1.0 template with model defaults.
  - ✅ `aqueduct doctor`: Engine, store, and resource connectivity probes.
  - ✅ `aqueduct report`: Flow visualization showing duration and status.

---

## Parser (`aqueduct/parser/`)

### `graph.py`
- ✅ `detect_cycles`: self‑loop raises ParseError
- ✅ `detect_cycles`: 3‑node cycle raises ParseError
- ✅ `detect_cycles`: disconnected graph (no cycles) passes
- ✅ `depends_on`: module with `depends_on: [other_module]` executes after `other_module` even with no edge between them
- ✅ `depends_on`: `depends_on` referencing non-existent module ID raises ParseError
- ✅ `depends_on`: `depends_on` + explicit edge to same module → no duplicate edge added

### `resolver.py`
- ✅ missing env var without default raises ParseError
- ✅ nested `${ctx.foo.bar}` resolved correctly
- ✅ `${ctx._watermark}` NOT in ctx_map → preserved verbatim (token unchanged), no `Undefined context reference` raised (reserved-deferred carve-out) — `tests/test_parser/test_resolver.py`
- ✅ non-reserved unknown `${ctx.foo}` still raises `Undefined context reference: ${ctx.foo}` (carve-out is exact-set, not blanket underscore) — `tests/test_parser/test_resolver.py`
- ✅ `_sub_ctx` preserves `${ctx._watermark}` while still resolving other real `${ctx.*}` keys in the same string — `tests/test_parser/test_resolver.py`
- ✅ end-to-end: a `materialize: incremental` Blueprint with `WHERE ts > ${ctx._watermark}` parses + compiles (`aqueduct validate` rc=0); Manifest Channel query still contains the literal `${ctx._watermark}` token — `tests/test_parser/test_resolver.py`
- ✅ ISSUE-027: `spark_config: {spark.jars.packages: "${MY_PKG:-org.example:pkg:1.0}"}` → parsed Blueprint `spark_config` value is `org.example:pkg:1.0` (env unset → default applied) — `tests/test_parser/test_resolver.py::test_spark_config_env_default_applied`
- ✅ ISSUE-027: `macros` value containing `${AQ_REGION:-US}` → resolved (`country = 'US'`); `{{ param }}` placeholders in the same macro left untouched for the compiler — `tests/test_parser/test_resolver.py::test_macros_env_resolved_jinja_placeholder_untouched`
- ✅ ISSUE-027: `${ctx.key}` in `spark_config` resolves from the context map (happy path) — `tests/test_parser/test_resolver.py::test_spark_config_ctx_resolves_from_context_map`
- ✅ ISSUE-027: undefined non-reserved `${ctx.x}` in `spark_config` raises `ParseError` — FIXED: `parser.py` hoists `spark_config`/`macros` `resolve_value()` into a guarded block (`ValueError→ParseError`, mirrors module-config pattern). `tests/test_parser/test_resolver.py::test_spark_config_undefined_ctx_raises_parseerror`

### `schema.py`
- ✅ unknown module type fails validation
- ✅ missing required `id` field fails

---

## Compiler (`aqueduct/compiler/`)

### `runtime.py`
- ✅ `@aq.date.today()` with custom format
- ✅ `@aq.depot.get()` missing key returns default
- ✅ `@aq.secret()` missing provider raises CompileError

### `expander.py`
- ✅ Arcade expansion namespaces IDs correctly
- ✅ Arcade with missing required_context fails

### Performance diagnostic warnings — `compiler.py`
- ✅ Probe with `null_rates` signal → `warnings.warn` contains "FULL DATASET SCAN" and "SPARK_GUIDE.md#probe-sample-cost"
- ✅ Probe with `row_count_estimate` (sample method) → warns; `row_count_estimate` with `method: spark_listener` → no warning
- ✅ Probe with `value_distribution` signal → warns
- ✅ Probe with `distinct_count` signal → warns
- ✅ Probe with `schema_snapshot` or `partition_stats` only → no warning emitted
- ✅ Channel with `materialize: incremental` and no Checkpoint upstream → warns containing "second scan" and "SPARK_GUIDE.md#incremental-watermark-scan"
- ✅ Channel with `materialize: incremental` + Checkpoint upstream → no warning
- ✅ UDF registry entry with `lang: python` → warns containing "row-at-a-time" and "SPARK_GUIDE.md#python-udf-performance"
- ✅ UDF registry entry with `lang: java` → no warning
- ✅ Egress with `format: delta` + `mode: append` + no `partition_by`/`repartition` → warns containing "small files"
- ✅ Egress with `format: parquet` + `mode: append` + no partition hint → warns
- ✅ Egress with `format: delta` + `mode: append` + `partition_by` present → no warning
- ✅ Egress with `format: delta` + `mode: overwrite` (no append) → no warning
- ✅ Channel with 2+ downstream consumers and no Checkpoint → warns containing "re-evaluate" and consumer count
- ✅ Channel with 2+ downstream consumers where a Checkpoint exists upstream → no warning
- ✅ Channel with single downstream consumer → no warning

---

### Phase 26b — Secrets Provider Backends

#### `secrets.py` — `resolve_secret()`
- ✅ `provider: env`: returns `os.environ[key]`; raises `SecretsError` when key missing
- ✅ `provider: env`: does NOT call boto3/google/azure SDK regardless of installed deps
- ✅ `provider: aws`: fetches from Secrets Manager; result injected into `os.environ`; second call returns `os.environ` fast-path (no SDK call)
- ✅ `provider: aws`: JSON blob value → unwraps inner key matching secret name suffix
- ✅ `provider: aws`: SDK not installed → `SecretsError` containing "boto3"
- ✅ `provider: gcp`: short name expanded using `GCP_PROJECT` env var; full resource path assembled
- ✅ `provider: gcp`: SDK not installed → `SecretsError` containing "google-cloud-secret-manager"
- ✅ `provider: azure`: `AZURE_KEYVAULT_URL` read from env; SDK not installed → `SecretsError` containing "azure-keyvault-secrets"
- ✅ `provider: custom`: `resolver` path loaded via importlib; callable signature `(key: str) -> str | None`
- ✅ `provider: custom`: callable returns `None` → `SecretsError` raised
- ✅ `provider: custom`: bad `resolver` path (module not found) → `SecretsError` with import path in message

#### `doctor.py` — `check_secrets()`
- ✅ `provider: env` → always passes (no dep check)
- ✅ `provider: aws`, boto3 missing → CheckResult status="error" containing "pip install aqueduct-core[aws]"
- ✅ `provider: aws`, boto3 present → CheckResult status="ok"
- ✅ `provider: gcp`, SDK missing → error with `[gcp]` install hint
- ✅ `provider: azure`, SDK missing → error with `[azure]` install hint
- ✅ `provider: custom`, `resolver=None` → error (resolver required for custom provider)
- ✅ `provider: custom`, valid resolver → importlib load attempted; ok if callable found

#### `agent/__init__.py` — `provider_options` dispatch
- ✅ `provider_options` with `ollama_num_thread: 8` → `payload["options"]["num_thread"] = 8` (prefix stripped)
- ✅ `provider_options` with generic key `temperature: 0.5` → `payload["temperature"] = 0.5`
- ✅ mixed `ollama_*` + generic keys → both dispatched correctly; no key collision
- ✅ `provider_options: null` → payload unchanged
- ✅ old `ollama_options` key rejected at parse time (schema validation error)

---

## Executor (`aqueduct/executor/`)

### `ingress.py` — `read_ingress()`
**Signature:** `read_ingress(module: Module, spark: SparkSession) -> DataFrame`
**Key config keys:** `format` (any Spark format string, required), `path` (required), `options` (dict), `schema_hint` (list of {name, type}), `header`/`infer_schema` (CSV only)
**Phase 7 change:** `SUPPORTED_FORMATS` whitelist removed. `format=None/""` → IngressError immediately. Any other format passed to Spark; Spark raises AnalysisException for unknown formats, wrapped in IngressError.

- ✅ `format=None` raises `IngressError` containing "'format' is required" ← **updated behavior (Phase 7)**
- ✅ `format="ghost"` (unknown): Spark rejects → `IngressError` containing "ghost" ← **was: Aqueduct rejected; now: Spark rejects, same user-visible result**
- ✅ missing `path` in config raises `IngressError` containing "'path' is required"
- ✅ `schema_hint` with missing column raises `IngressError` containing "not found"
- ✅ `schema_hint` with wrong type raises `IngressError` containing "type mismatch"
- ✅ valid parquet path returns lazy DataFrame (no Spark action)
- ✅ csv format applies `header` and `inferSchema` defaults
- ✅ `options` dict forwarded to reader

### `egress.py` — `write_egress()`
**Signature:** `write_egress(df: DataFrame, module: Module, depot: Any = None) -> None`
**Key config keys:** `format` (any Spark format string OR "depot"), `path` (required for non-depot), `mode` (overwrite/append/error/errorifexists/ignore), `partition_by` (list), `options` (dict)
**Depot-only keys:** `key` (required), `value` (static string) OR `value_expr` (SQL aggregate expression)
**Phase 7 change:** `SUPPORTED_FORMATS` whitelist removed; `format="depot"` routes to DepotStore write instead of Spark; Spark write errors now wrapped in EgressError.

- ✅ `format=None` raises `EgressError` containing "'format' is required" ← **updated behavior**
- ✅ unknown Spark format (e.g. `"avro"`) passes through to writer (Spark raises on bad path/JAR, not Aqueduct) ← **new behavior**
- ✅ missing `path` raises `EgressError` containing "'path' is required"
- ✅ unsupported `mode` raises `EgressError` containing mode name and "Supported:"
- ✅ `partition_by` forwarded to writer
- ✅ `options` dict forwarded to writer
- ✅ write with `mode: overwrite` on existing path succeeds
- ✅ `register_as_table` set → `CREATE EXTERNAL TABLE IF NOT EXISTS` called with correct name, format, location
- ✅ `register_as_table` DDL failure (no Hive metastore) → warning logged, blueprint continues (non-fatal)
- ✅ `register_as_table` absent → no DDL executed
- ✅ `format="depot"`, `depot=None` → `EgressError` containing "no DepotStore is wired"
- ✅ `format="depot"`, `key=None/""` → `EgressError` containing "requires 'key'"
- ✅ `format="depot"`, valid `key` + `value`: `depot.put(key, value)` called; no Spark write
- ✅ `format="depot"`, valid `key` + `value_expr`: single Spark agg action; `depot.put` called with aggregate result
- ✅ Spark write failure (bad path, wrong format) raises `EgressError` wrapping original exception

### `executor.py` — `execute()`
- ✅ linear Ingress → Egress blueprint returns `ExecutionResult(status="success")`
- ✅ `ExecutionResult.module_results` contains one entry per module, all `status="success"`
- ✅ unsupported module type (`Channel`, `Probe`, etc.) raises `ExecuteError`
- ✅ IngressError propagated → `ExecutionResult(status="error")` with module error recorded
- ✅ EgressError propagated → `ExecutionResult(status="error")` with module error recorded
- ✅ missing upstream DataFrame (no main-port edge) → `ExecutionResult(status="error")`
- ✅ `run_id` auto-generated when not supplied; format is valid UUID4
- ✅ `execute()` with a supplied `run_id` echoes that ID in the result
- ✅ cycle in Manifest edge graph raises `ExecuteError`

### `models.py`
- ✅ `ExecutionResult` is frozen; mutation raises `FrozenInstanceError`
- ✅ `ExecutionResult.to_dict()` serialises to JSON-compatible dict

### `session.py` — `make_spark_session()`
- ✅ returns an active `SparkSession`
- ✅ `spark_config` entries applied as Spark conf properties
- ✅ calling twice returns the same session (getOrCreate semantics)

---

## Channel (`aqueduct/executor/channel.py`)

### `execute_sql_channel()`
- ✅ unsupported op (not `'sql'`) raises `ChannelError`
- ✅ missing or empty `query` raises `ChannelError`
- ✅ empty `upstream_dfs` raises `ChannelError`
- ✅ upstream DataFrame registered as temp view named after its module ID
- ✅ single-input Channel: upstream also registered as `__input__` view
- ✅ multi-input Channel: all upstreams registered; `__input__` NOT registered
- ✅ temp views dropped after execution (catalog clean after return)
- ✅ SQL syntax error → `ChannelError` containing original exception message
- ✅ `SELECT * FROM read_input` resolves when upstream ID is `read_input`
- ✅ `SELECT * FROM __input__` resolves on single-input Channel
- ✅ result is a lazy DataFrame (no Spark action triggered inside channel)

### Executor integration (`executor.py`)
- ✅ Ingress → Channel → Egress blueprint returns `ExecutionResult(status="success")`
- ✅ Channel with no incoming edge recorded as error in `ExecutionResult`
- ✅ ChannelError recorded in `ExecutionResult(status="error")`
- ✅ multi-input Channel (two Ingress → one Channel) executes correctly
- ✅ Channel result DataFrame available to downstream Egress via `frame_store`

---

## Probe (`aqueduct/executor/probe.py`)

### `execute_probe()`
- ✅ no `signals` in config → returns immediately without writing anything
- ✅ unknown signal type → warning logged; other signals still captured
- ✅ `schema_snapshot`: JSON file written to `store_dir/snapshots/<run_id>/<probe_id>_schema.json`
- ✅ `schema_snapshot`: DuckDB row inserted into `probe_signals` with correct payload shape
- ✅ `schema_snapshot`: zero Spark actions triggered (no count/collect)
- ✅ `row_count_estimate` method=sample: DuckDB row inserted with `estimate` > 0
- ✅ `row_count_estimate` method=spark_listener: queries `module_metrics` table; returns `estimate` from `records_written` (or `records_read`) when row exists
- ✅ `row_count_estimate` method=spark_listener: returns `estimate=None` when no `module_metrics` row yet exists
- ✅ `null_rates`: payload contains `null_rates` dict keyed by requested columns
- ✅ `null_rates` with no `columns` key uses all DataFrame columns
- ✅ `sample_rows`: payload contains `rows` list of at most `n` dicts
- ✅ exception inside one signal does not prevent other signals from being captured
- ✅ exception inside `execute_probe` does not propagate to caller

#### New signal types (Phase 15)
- ✅ `value_distribution`: payload has `stats` dict; each column has `min`, `max`, `mean`, `stddev`, `count_non_null`, `percentiles` keys
- ✅ `value_distribution` with no `columns` → only numeric columns included automatically
- ✅ `value_distribution` `block_full_actions=True` → `{"blocked": True, "stats": {}}`; warning logged
- ✅ `distinct_count`: payload has `distinct_counts` dict keyed by columns with integer values
- ✅ `distinct_count` with no `columns` → all DataFrame columns
- ✅ `distinct_count` `block_full_actions=True` → `{"blocked": True, "distinct_counts": {col: None}}`
- ✅ `data_freshness`: payload has `column`, `max_value` keys
- ✅ `data_freshness` missing `column` → signal fails, other signals captured normally
- ✅ `data_freshness` `block_full_actions=True` + `allow_sample=false` (default) → `{"blocked": True, "column": ...}`
- ✅ `data_freshness` `block_full_actions=True` + `allow_sample=true` → executes on sample; `sampled=True` in payload
- ✅ `partition_stats`: payload has `num_partitions` key; integer ≥ 1; zero Spark action
- ✅ `partition_stats` `block_full_actions=True` → still executes (not a Spark action)
- ✅ `threshold` with `expr: "COUNT(*) > 0"` on non-empty DF → payload `{"passed": true, "value": ..., "expr": ...}`
- ✅ `threshold` with `expr: "COUNT(*) > 0"` on empty DF → payload `{"passed": false, ...}`
- ✅ `threshold` missing `expr` → signal fails with ValueError, other signals still captured
- ✅ `threshold` signal written to `probe_signals` → `evaluate_regulator()` returns `True` when passed
- ✅ `threshold` signal with `passed=false` → `evaluate_regulator()` returns `False`, Regulator closes gate
- ✅ Regulator `timeout_seconds=5`: gate closed initially, signal inserted mid-poll → gate opens, downstream runs
- ✅ Regulator `timeout_seconds=1`: gate stays closed past timeout → `on_block` applies (not forced-open)

### Executor integration (`executor.py`)
- ✅ Probe appended after non-Probe modules in execution order (runs last)
- ✅ Probe with `attach_to` pointing to completed Ingress: signals written to DB
- ✅ Probe with missing `attach_to` source (source failed): logged warning, result `status="success"`
- ✅ Probe failure does not change blueprint `ExecutionResult(status="success")`
- ✅ `execute()` with `store_dir=None`: Probe result is `status="success"` but no DB written
- ✅ Ingress → Probe (schema_snapshot) → Egress blueprint returns `ExecutionResult(status="success")`

### `module_metrics` / `df.observe()` collection
- ✅ `observe_df()` on Spark 3.3+: returns `(observed_df, Observation)` with correct alias
- ✅ `observe_df()` on Spark < 3.3 (or mock): returns `(original_df, None)` — no crash
- ✅ `get_observation(obs, alias)` returns correct count after action fired
- ✅ `get_observation(None, alias)` returns 0
- ✅ `dir_bytes()` on existing local file: returns non-zero size
- ✅ `dir_bytes()` on existing local directory: returns sum of file sizes
- ✅ `dir_bytes()` on cloud path (s3://...): returns 0
- ✅ `dir_bytes()` on nonexistent path: returns 0
- ✅ `_write_stage_metrics()` creates `module_metrics` table if absent and inserts one row
- ✅ `_write_stage_metrics()` with `store_dir=None` is a no-op
- ✅ Egress succeeds → `module_metrics` row has `records_written > 0` (Spark 3.3+, local write)
- ✅ Egress succeeds → `module_metrics` row has `bytes_written > 0` for local path
- ✅ Egress succeeds → `module_metrics` row has `duration_ms > 0`
- ✅ Ingress succeeds → `module_metrics` row has `bytes_read > 0` for local path, `records_read = 0`
- ✅ Channel/Junction/Funnel → `module_metrics` row has `duration_ms > 0`, other fields zero

### Assert module
- ✅ `schema_match` passes: zero Spark action triggered
- ✅ `schema_match` fails (missing column) with `on_fail=abort`: `AssertError` raised
- ✅ `schema_match` fails (wrong type) with `on_fail=abort`: `AssertError` raised
- ✅ `min_rows` passes: single batched `df.agg()` used (at most 1 Spark action for all aggregate rules)
- ✅ `min_rows` fails with `on_fail=abort`: `AssertError` raised
- ✅ `max_rows` fails with `on_fail=warn`: warning logged, blueprint continues
- ✅ `null_rate` passes: shared `df.sample().agg()` used
- ✅ `null_rate` fails with `on_fail=abort`: `AssertError` raised
- ✅ `freshness` passes: `max(col)` batched into shared `df.agg()`
- ✅ `freshness` fails with `on_fail=warn`: warning logged, blueprint continues
- ✅ `freshness` column has all nulls: fail message includes "no non-null values"
- ✅ `freshness` with `on_fail=quarantine`: stale rows in `quarantine_df`, fresh rows in `passing_df` (ISSUE-015, ISSUE-016 fixed)
- ✅ `freshness` with `on_fail=quarantine` + spillway edge: end-to-end rows routed correctly
- ✅ `freshness` with `on_fail=quarantine` + no spillway edge: treated as warn (not CompileError yet)
- ✅ `freshness` on_fail: quarantine with numeric column → cast via to_timestamp() (ISSUE-017 fixed)
- ✅ `freshness` on_fail: quarantine with nulls → NULLs route to quarantine (ISSUE-015 fixed)
- ✅ `freshness` on_fail: quarantine missing column → AssertError raised (ISSUE-018 fixed)
- ✅ `min_rows` with `on_fail=quarantine`: treated as warn per log
- ✅ `max_rows` with `on_fail=quarantine`: treated as warn per log
- ✅ `sql` (aggregate) with `on_fail=quarantine`: treated as warn per log
- ✅ `null_rate` with `on_fail=quarantine`: treated as warn per log
- ✅ `sql` rule passes: custom aggregate expr evaluated in batched `agg()`
- ✅ `sql` rule fails with `on_fail=webhook`: `fire_webhook` called; blueprint continues
- ✅ `sql_row` rule: passing rows on main port, failing rows in `quarantine_df`
- ✅ `sql_row` rule with `on_fail=abort` (non-quarantine): `AssertError` raised if any failing rows
- ✅ `custom` fn: callable loaded via `importlib`, result dict validated
- ✅ `custom` fn with `quarantine_df` returned: quarantine rows get `_aq_error_*` columns
- ✅ `custom` fn raises exception: warning logged, pass-through (non-fatal)
- ✅ `custom` fn with bad `fn` path: `AssertError` raised with clear message
- ✅ multiple aggregate rules → exactly 1 Spark action (min_rows + freshness + sql batched)
- ✅ mixed aggregate + null_rate → at most 2 Spark actions
- ✅ `on_fail=trigger_agent`: `AssertError.trigger_agent=True`
- ✅ gate closed upstream → Assert `status="skipped"`, sentinel propagated downstream
- ✅ `sql_row`/`custom`/`freshness` with `on_fail=quarantine` + no spillway edge → treated as warn/log (previously: silent runtime discard)
- ✅ Assert with no rules configured → pass-through, `status="success"`
- ✅ end-to-end: Ingress → Assert(`min_rows` abort rule fires) → `ExecutionResult(status="error")`
- ✅ end-to-end: Ingress → Assert(`sql_row` quarantine) → Egress(good) + Egress(quarantine), both written
- ✅ `error_type` on rule → `AssertError.error_type` set correctly
- ✅ `error_type` propagates: `AssertError` → `ModuleResult.error_type` → `FailureContext.error_type`
- ✅ rule without `error_type` → `AssertError.error_type` is `None`
- ✅ multiple rules with different `error_type` → only first-failing rule's label in `FailureContext`

### Surveyor `get_probe_signal()`
- ✅ returns empty list when `observability.db` does not exist
- ✅ returns rows matching `probe_id` after `execute_probe` writes them
- ✅ `signal_type` filter returns only rows of that type
- ✅ `payload` field is a deserialized dict (not a raw JSON string)
- ✅ rows ordered by `captured_at DESC`

---

## Junction (`aqueduct/executor/junction.py`)

### `execute_junction()`
- ✅ unsupported mode raises `JunctionError`
- ✅ missing `mode` (None) raises `JunctionError`
- ✅ empty `branches` raises `JunctionError`
- ✅ branch missing `id` raises `JunctionError`
- ✅ branch missing `condition` in conditional mode raises `JunctionError`
- ✅ missing `partition_key` in partition mode raises `JunctionError`

#### conditional mode
- ✅ branch with explicit condition returns `df.filter(condition)` (lazy, no Spark action)
- ✅ `_else_` branch returns rows not matched by any explicit condition
- ✅ `_else_` with no other explicit conditions returns unfiltered df
- ✅ multiple explicit conditions: `_else_` excludes all of them

#### broadcast mode
- ✅ all branches reference the same unmodified DataFrame object

#### partition mode
- ✅ branch without `value` falls back to branch `id` as partition value
- ✅ branch with explicit `value` uses that value in filter expression

### Executor integration (`executor.py`)
- ✅ Junction with no main-port incoming edge recorded as error in `ExecutionResult`
- ✅ JunctionError recorded in `ExecutionResult(status="error")`
- ✅ Junction branches stored as `frame_store["junction_id.branch_id"]`
- ✅ Ingress → Junction (broadcast) → two Egress modules executes successfully
- ✅ Ingress → Junction (conditional) → Egress receives filtered DataFrame

---

## Funnel (`aqueduct/executor/funnel.py`)

### `execute_funnel()`
- ✅ unsupported mode raises `FunnelError`
- ✅ missing `mode` (None) raises `FunnelError`
- ✅ missing `inputs` raises `FunnelError`
- ✅ fewer than 2 `inputs` raises `FunnelError`
- ✅ unknown input module ID in `inputs` raises `FunnelError`

#### union_all mode
- ✅ stacks two DataFrames with same schema (schema_check: strict, default)
- ✅ `schema_check: permissive` allows mismatched schemas (missing cols filled null)
- ✅ `schema_check: strict` with mismatched schemas raises `FunnelError`
- ✅ result is lazy (no Spark action triggered)

#### union mode
- ✅ result is union_all + deduplicated (`.distinct()`)
- ✅ result is lazy

#### coalesce mode
- ✅ two DataFrames with overlapping columns: first non-null value wins per row
- ✅ non-overlapping columns from all inputs present in result
- ✅ result is lazy (no Spark action triggered)

#### zip mode
- ✅ two DataFrames with distinct columns: all columns present in result
- ✅ duplicate column name across inputs raises `FunnelError`
- ✅ result is lazy

### Executor integration (`executor.py`)
- ✅ Funnel with no incoming data edges recorded as error in `ExecutionResult`
- ✅ FunnelError recorded in `ExecutionResult(status="error")`
- ✅ two Ingress → Funnel (union_all) → Egress executes successfully
- ✅ Junction (broadcast) → two paths → Funnel (union) round-trip executes successfully

---

## Surveyor (`aqueduct/surveyor/`)

### `models.py`
- ✅ `RunRecord` is frozen; mutation raises `FrozenInstanceError`
- ✅ `RunRecord.to_dict()` contains all required keys
- ✅ `FailureContext` is frozen; mutation raises `FrozenInstanceError`
- ✅ `FailureContext.to_dict()` contains `run_id`, `blueprint_id`, `failed_module`, `error_message`, `stack_trace`
- ✅ `FailureContext.to_json()` is valid JSON deserializable back to original fields

### `webhook.py` — `fire_webhook()`
- ✅ returns a `threading.Thread` that is already started
- ✅ returned thread is a daemon thread
- ✅ POST sends JSON body with `Content-Type: application/json`
- ✅ network error (unreachable host) does not raise — failure logged to stderr
- ✅ HTTP 4xx response does not raise — warning logged to stderr

### Webhook scopes
- ✅ `on_success` webhook fires after successful run (mock HTTP server)
- ✅ `on_success` webhook NOT fired when run fails
- ✅ `on_success: null` (default) — no webhook call made on success
- ✅ `on_success` simple string URL form accepted by `WebhooksConfig`
- ✅ `on_success` template vars: `${run_id}`, `${blueprint_id}`, `${blueprint_name}`, `${module_count}` resolved in payload
- ✅ `on_failure_webhook` on module fires when retry exhausts (mock HTTP server)
- ✅ `on_failure_webhook` fires even when `on_exhaustion=alert_only` (blueprint continues)
- ✅ `on_failure_webhook` fires even when `on_exhaustion=abort` (blueprint fails)
- ✅ `on_failure_webhook` simple string URL form accepted by schema
- ✅ `on_failure_webhook` full dict form (url, method, payload, headers) accepted by schema
- ✅ `on_failure_webhook` template vars: `${module_id}`, `${error_message}`, `${error_type}`, `${run_id}`, `${blueprint_id}` resolved
- ✅ `on_failure_webhook=None` (default) — no per-module webhook call made

### `surveyor.py` — `Surveyor`
- ✅ `start()` creates `.aqueduct/observability.db` and tables if not existing
- ✅ `start()` inserts a `run_records` row with `status='running'`
- ✅ `record()` raises `RuntimeError` if called before `start()`
- ✅ `record()` updates `run_records` row to `status='success'` on success
- ✅ `record()` updates `run_records` row to `status='error'` on failure
- ✅ `record()` inserts `failure_contexts` row on failure
- ✅ `record()` returns `None` on success
- ✅ `record()` returns `FailureContext` on failure
- ✅ `FailureContext.failed_module` is the first failing module_id from result
- ✅ `FailureContext.failed_module` is `_executor` when no module results (bare ExecuteError)
- ✅ `FailureContext.stack_trace` populated when `exc=` argument supplied
- ✅ `FailureContext.stack_trace` is `None` when `exc=None`
- ✅ `FailureContext.manifest_json` is valid JSON
- ✅ `stop()` closes DB connection; second `stop()` is a no-op
- ✅ two successive runs to same store: both rows persisted in `run_records`
- ✅ webhook NOT fired on success even if `webhook_url` configured
- ✅ webhook fired on failure when `webhook_url` configured (mock server)
- ✅ webhook NOT fired when `webhook_url=None`

---

## Patch Grammar (`aqueduct/patch/`)

### `grammar.py` — PatchSpec validation
- ✅ valid PatchSpec JSON parses without error
- ✅ `operations` list empty → `ValidationError`
- ✅ unknown top-level field → `ValidationError` (extra="forbid")
- ✅ unknown `op` value → `ValidationError` (discriminator mismatch)
- ✅ `replace_module_config` missing `config` → `ValidationError`
- ✅ `replace_edge` extra field → `ValidationError` (extra="forbid")
- ✅ `PatchSpec.model_json_schema()` returns valid JSON Schema dict

### `operations.py` — individual operations

#### `replace_module_config`
- ✅ existing module config replaced with new dict
- ✅ unknown module_id raises `PatchOperationError`

#### `replace_module_label`
- ✅ module label updated
- ✅ unknown module_id raises `PatchOperationError`

#### `insert_module`
- ✅ module appended to modules list
- ✅ specified edges_to_remove removed; edges_to_add added
- ✅ duplicate module_id raises `PatchOperationError`
- ✅ edges_to_remove referencing non-existent edge raises `PatchOperationError`
- ✅ module missing `id` raises `PatchOperationError`

#### `remove_module`
- ✅ module removed from modules list
- ✅ all edges referencing the module removed
- ✅ edges_to_add wired in after removal
- ✅ unknown module_id raises `PatchOperationError`

#### `replace_context_value`
- ✅ top-level context key replaced
- ✅ nested dot-notation key (`paths.input`) replaced
- ✅ Blueprint with no context block raises `PatchOperationError`
- ✅ invalid dot path (intermediate key not a dict) raises `PatchOperationError`

#### `add_probe`
- ✅ Probe module added to modules list
- ✅ edges_to_add appended
- ✅ missing `attach_to` raises `PatchOperationError`
- ✅ type != 'Probe' raises `PatchOperationError`
- ✅ attach_to targeting unknown module raises `PatchOperationError`

#### `replace_edge`
- ✅ edge endpoint updated (new_from_id)
- ✅ edge endpoint updated (new_to_id)
- ✅ edge port updated (new_port)
- ✅ non-existent edge raises `PatchOperationError`
- ✅ no new field provided raises `PatchOperationError`

#### `set_module_on_failure`
- ✅ on_failure block set on module
- ✅ unknown module_id raises `PatchOperationError`

#### `replace_retry_policy`
- ✅ top-level retry_policy replaced

#### `add_arcade_ref`
- ✅ Arcade module added to modules list
- ✅ edges_to_remove / edges_to_add applied
- ✅ type != 'Arcade' raises `PatchOperationError`
- ✅ missing `ref` raises `PatchOperationError`
- ✅ duplicate id raises `PatchOperationError`

### `apply.py`

#### `load_patch_spec()`
- ✅ valid JSON file → returns `PatchSpec`
- ✅ file not found → `PatchError`
- ✅ invalid JSON → `PatchError`
- ✅ schema violation → `PatchError` with Pydantic details

#### `apply_patch_to_dict()`
- ✅ returns modified dict; input bp unchanged (deep copy)
- ✅ first-operation failure raises `PatchError` with op index in message
- ✅ operations applied left-to-right (second op sees first op's changes)

#### `apply_patch_file()`
- ✅ patched Blueprint written to blueprint_path
- ✅ original backed up to patches/backups/<patch_id>_<ts>_<name>
- ✅ PatchSpec archived to patches/applied/ with `applied_at` field added
- ✅ `ApplyResult.operations_applied` matches len(operations)
- ✅ Blueprint not found → `PatchError`
- ✅ post-patch Blueprint that fails Parser → `PatchError`; original Blueprint unchanged
- ✅ atomic write: failure mid-write leaves original Blueprint intact
- ✅ re-parsing the patched Blueprint succeeds (integration test with valid_minimal.yml)

#### `reject_patch()`
- ✅ pending patch moved to patches/rejected/
- ✅ rejected file contains `rejected_at` and `rejection_reason` fields
- ✅ patch_id not in patches/pending/ → `PatchError`

---

## Configuration (`aqueduct/config.py`)

### `load_config()`
- ✅ no file present (implicit lookup) → returns `AqueductConfig` with all defaults
- ✅ explicit path that does not exist → `ConfigError`
- ✅ empty YAML file → returns `AqueductConfig` with all defaults
- ✅ valid aqueduct.yml → returns correctly populated `AqueductConfig`
- ✅ invalid YAML syntax → `ConfigError`
- ✅ unknown top-level key → `ConfigError` (extra="forbid")
- ✅ unknown nested key in deployment → `ConfigError`

### `AqueductConfig` defaults
- ✅ `deployment.target` defaults to `"local"`
- ✅ `deployment.master_url` defaults to `"local[*]"`
- ✅ `stores.observability.path` defaults to `".aqueduct/observability.db"` ← **renamed from `observability`; now full file path**
- ✅ `stores.lineage.path` defaults to `".aqueduct/lineage.db"` ← **now full file path**
- ✅ `stores.depot.path` defaults to `".aqueduct/depot.db"` ← **updated (was `.aqueduct/depot.duckdb`)**
- ✅ `agent.timeout` defaults to `120.0`
- ✅ `agent.max_reprompts` defaults to `3`
- ✅ `agent.prompt_context` defaults to `None`
- ✅ `agent.default_model` defaults to `"claude-sonnet-4-6"`
- ✅ `probes.max_sample_rows` defaults to `100`
- ✅ `secrets.provider` defaults to `"env"`
- ✅ `webhooks.on_failure` defaults to `None`
- ✅ `webhooks.on_success` defaults to `None`
- ✅ `webhooks.on_success` string URL coerced to `WebhookEndpointConfig`
- ✅ `AqueductConfig` is frozen; mutation raises `ValidationError`

### Config file overrides
- ✅ custom `master_url` in config read back correctly
- ✅ partial config (only `deployment` section) → other sections use defaults
- ✅ `spark_config` dict entries preserved in returned config

## Remote Spark (`aqueduct/executor/session.py`)

### `make_spark_session()` — master_url parameter
- ✅ default `master_url="local[*]"` used when arg omitted
- ✅ custom master_url passed to `builder.master()`
- ✅ `"yarn"` master_url does not raise at construction time
- ✅ `"spark://host:7077"` master_url does not raise at construction time
- ✅ Blueprint `spark_config` merged; Blueprint values take precedence over engine config

---

## Regulator (`aqueduct/executor/executor.py` + `aqueduct/surveyor/surveyor.py`)

### `Surveyor.evaluate_regulator()`
- ✅ returns `True` when `start()` not called (no run_id)
- ✅ returns `True` when no signal-port edge wired to regulator
- ✅ returns `True` when `observability.db` does not exist
- ✅ returns `True` when no rows found for probe_id / run_id
- ✅ returns `True` when latest signal payload has no `passed` key
- ✅ returns `True` when latest signal `passed=None`
- ✅ returns `False` when latest signal `passed=False`
- ✅ returns `True` when latest signal `passed=True`
- ✅ uses newest row (ORDER BY captured_at DESC); older `passed=False` ignored if newest `passed=True`
- ✅ returns `True` on any DuckDB exception (open-gate-on-error policy)

### Executor integration (`executor.py`)
- ✅ Regulator with open gate
- [✅] `test_surveyor_start_stop`
- [✅] `test_surveyor_record_success`
- [✅] `test_surveyor_record_failure_with_ctx`
- [✅] `test_surveyor_regulator_duckdb_exception` (fail-open verified)
- ✅ Regulator with closed gate + `on_block=skip`: `frame_store[regulator_id] = _GATE_CLOSED`, `status="skipped"`
- ✅ Regulator with closed gate + `on_block=abort`: blueprint returns `ExecutionResult(status="error")`
- ✅ Regulator with closed gate + `on_block=trigger_agent`: `ExecutionResult(status="error", trigger_agent=True)` — LLM loop fires even with `approval_mode=disabled`
- ✅ downstream of skipped Regulator also records `status="skipped"` (sentinel propagation)
- ✅ Regulator with no main-port incoming edge records `status="error"`

---

---

## Blueprint Execution Tests (`tests/test_blueprints.py`)

Full compile → execute cycle with real `local[*]` Spark. No mocks.
Blueprints live in `tests/fixtures/blueprints/`. All I/O paths injected via `cli_overrides`.
`sample_data` session fixture provides: `orders.parquet` (10 rows: 5 US region, 5 EU region, 1 null amount at row index 3 which is US), `customers.parquet` (5 rows).

- ✅ `test_linear_ingress_egress`: Ingress → Egress; 10 rows in output
- ✅ `test_channel_sql_filter`: Channel SQL filter removes null-amount row; 9 rows in output
- ✅ `test_junction_conditional_split`: Junction splits US/EU; each output has 5 correct-region rows
- ✅ `test_funnel_union_all`: two identical inputs stacked; output has 20 rows
- ✅ `test_spillway_error_routing`: null row → spillway (1 row + `_aq_error_*`); good rows → main (9 rows)
- ✅ `test_probe_does_not_halt_blueprint`: Probe runs; observability.db written; blueprint succeeds
- ✅ `test_regulator_open_gate_passthrough`: no surveyor → gate open → all 10 rows in output
- ✅ `test_regulator_closed_gate_skips_downstream`: mock surveyor returns False → gate + sink both "skipped"
- ✅ `test_junction_funnel_channel_pattern`: Junction → Funnel → Channel (regression); all 10 rows + `blueprint_tag` column in output
- ✅ `test_chained_channels`: Ingress → Channel (filter) → Channel (add tag) → Egress; 9 rows + `tag` column in output
- ✅ `test_lineage_written_after_channel_run`: Channel blueprint with store_dir set; `lineage.db` written with rows

---

## Phase 7 — Engine Hardening

### Open Format Passthrough (`ingress.py`, `egress.py`)

#### `read_ingress()` — passthrough
- ✅ unknown format (e.g. `"jdbc"`) no longer raises `IngressError` — passes directly to Spark
- ✅ missing `format` (None/empty) raises `IngressError`
- ✅ CSV format-specific defaults still applied for `fmt == "csv"`
- ✅ non-CSV unknown format: no format-specific defaults applied, options forwarded verbatim
- ✅ Spark `AnalysisException` on bad path wrapped in `IngressError`

#### `write_egress()` — passthrough
- ✅ unknown format (e.g. `"avro"`) no longer raises `EgressError` — passes to Spark
- ✅ missing `format` (None/empty) raises `EgressError`
- ✅ `format: depot` does NOT call `df.write` — calls `depot.put()` instead
- ✅ `format: depot` with `depot=None` raises `EgressError`
- ✅ `format: depot` missing `key` raises `EgressError`
- ✅ `format: depot` with `value`: depot.put called with resolved string value
- ✅ `format: depot` with `value_expr`: single Spark agg action executed; depot.put called with result
- ✅ SUPPORTED_MODES still enforced; unknown mode raises `EgressError`
- ✅ Spark write failure wrapped in `EgressError`

### Spillway (`executor.py` — Channel dispatch)

- ✅ `spillway_condition` set + spillway edge present: `frame_store[id]` = good rows, `frame_store["id.spillway"]` = error rows
- ✅ error rows have `_aq_error_module`, `_aq_error_msg`, `_aq_error_ts` columns
- ✅ good rows do NOT have `_aq_error_*` columns
- ✅ `spillway_condition` set + NO spillway edge: warning logged; all rows in main stream
- ✅ spillway edge + NO `spillway_condition`: warning logged; `frame_store["id.spillway"]` = empty DataFrame
- ✅ spillway Egress resolves `frame_store["channel_id.spillway"]` via `_frame_key`
- ✅ `_SIGNAL_PORTS` no longer contains `"spillway"` — spillway edge participates in topo-sort
- ✅ end-to-end: Channel with spillway_condition → two Egress (main + spillway) both succeed

### Depot KV Store (`aqueduct/depot/depot.py`)

#### `DepotStore`
- ✅ `get(key)` returns default when DB file does not exist
- ✅ `get(key)` returns default when key absent in existing DB
- ✅ `put(key, value)` creates DB file on first call
- ✅ `put(key, value)` twice: second value overwrites first (upsert)
- ✅ `put` sets `updated_at` to a recent UTC timestamp
- ✅ `get` with DB access error returns default (no exception raised)
- ✅ `close()` is a no-op (does not raise)

#### Runtime integration (`runtime.py`)
- ✅ `@aq.runtime.prev_run_id()` returns `""` when depot has no `_last_run_id`
- ✅ `@aq.runtime.prev_run_id()` returns last written run_id after CLI run

#### CLI integration (`cli.py`)
- ✅ `aqueduct run` writes `_last_run_id` to depot after blueprint completes
- ✅ second `aqueduct run` sees previous run_id via `@aq.runtime.prev_run_id()`

### UDF Registration (`aqueduct/executor/udf.py`)

#### `register_udfs()`
- ✅ empty registry is a no-op (no error)
- ✅ python UDF: imports module, finds function, calls `spark.udf.register`
- ✅ `entry` defaults to UDF `id` when not specified
- ✅ missing `module` raises `UDFError`
- ✅ non-existent `module` path raises `UDFError`
- ✅ function name not found in module raises `UDFError`
- ✅ unsupported `lang` (scala/java/sql) raises `UDFError`
- ✅ end-to-end: Channel SQL calls registered UDF by name; result correct

#### Manifest threading
- ✅ `blueprint.udf_registry` parsed from YAML and present in Blueprint AST
- ✅ `manifest.udf_registry` populated from blueprint after compile
- ✅ `manifest.to_dict()` includes `udf_registry` list

---

---

## Phase 8 — Resilience, Lineage, LLM Self-Healing

### RetryPolicy + deadline_seconds (`aqueduct/parser/models.py`, `aqueduct/executor/executor.py`)

**`RetryPolicy` fields:** `max_attempts` (int), `backoff_strategy` (exponential/linear/fixed), `backoff_base_seconds` (int), `backoff_max_seconds` (int), `jitter` (bool), `on_exhaustion` (trigger_agent/abort/alert_only), `transient_errors` (tuple[str]), `non_transient_errors` (tuple[str]), `deadline_seconds` (int|None)

**`_with_retry(fn, policy, module_id)`:** calls fn(), retries on retriable exceptions with backoff, checks deadline.
**`_is_retriable(exc, policy)`:** returns False if exc message matches any `non_transient_errors` pattern; if `transient_errors` non-empty, only those patterns are retriable; otherwise all errors are retriable.
**`_backoff_seconds(attempt, policy)`:** exponential = `base * 2^attempt`, linear = `base * (attempt+1)`, fixed = `base`; capped at `max_seconds`; jitter multiplies by random [0.5, 1.0].

- ✅ `RetryPolicy` with `deadline_seconds=3600` round-trips through schema validation (YAML → Schema → Model)
- ✅ `_is_retriable`: non_transient_errors pattern blocks retry even if transient match present
- ✅ `_is_retriable`: transient_errors list non-empty, error NOT matching → False
- ✅ `_is_retriable`: transient_errors list non-empty, error matching → True
- ✅ `_is_retriable`: both lists empty → True (all errors retriable by default)
- ✅ `_backoff_seconds` exponential: attempt 0=base, attempt 1=2×base, attempt 2=4×base
- ✅ `_backoff_seconds` linear: attempt 0=base, attempt 1=2×base, attempt 2=3×base
- ✅ `_backoff_seconds` fixed: all attempts return base
- ✅ `_backoff_seconds` cap: result never exceeds `backoff_max_seconds`
- ✅ `_backoff_seconds` jitter=False: result equals formula exactly; jitter=True: result in [0.5×formula, formula]
- ✅ `_with_retry`: fn succeeds first attempt → returns result, no sleep
- ✅ `_with_retry`: fn fails then succeeds → returns result after one retry
- ✅ `_with_retry`: fn always fails, max_attempts=3 → raises last exception after 3 attempts
- ✅ `_with_retry`: non-retriable exception → raises immediately without retry (max_attempts=3 but only 1 call)
- ✅ `_with_retry`: deadline_seconds elapsed after first failure → stops retrying, raises last exception
- ✅ executor Ingress wrapped in retry: Ingress that fails twice then succeeds → `ExecutionResult(status="success")`

### Lineage Writer (`aqueduct/compiler/lineage.py`)

**`_extract_sql_lineage(channel_id, sql, upstream_ids)`:** returns list of `{channel_id, output_column, source_table, source_column}` dicts. Uses sqlglot to parse SparkSQL.
**`write_lineage(blueprint_id, run_id, modules, edges, store_dir)`:** writes to `store_dir/lineage.db`, table `column_lineage`. Non-fatal — swallows all exceptions.

- ✅ `_extract_sql_lineage`: `SELECT a, b FROM tbl` → two rows with `source_column=a/b`, `source_table=tbl`
- ✅ `_extract_sql_lineage`: `SELECT a * 2 AS doubled FROM tbl` → output_column=`doubled`, source_column=`a`
- ✅ `_extract_sql_lineage`: `SELECT * FROM tbl` → row with `output_column="*"`, `source_column="*"`
- ✅ `_extract_sql_lineage`: invalid SQL → returns `[]` (no exception raised)
- ✅ `_extract_sql_lineage`: single upstream → source_table inferred when column has no table qualifier
- ✅ `write_lineage`: creates `lineage.db` and `column_lineage` table when not present
- ✅ `write_lineage`: inserts one row per output_column/source_column pair for each Channel
- ✅ `write_lineage`: non-Channel modules (Ingress, Egress) do not produce lineage rows
- ✅ `write_lineage`: sqlglot exception does not propagate (non-fatal)
- ✅ `write_lineage`: called after successful blueprint execution with `store_dir` set; `lineage.db` written

### Agent Self-Healing (`aqueduct/agent/__init__.py`)

All tests mock `aqueduct.agent._call_agent` (or patch `httpx.post`) — no live model.

**`generate_agent_patch(failure_ctx, model, patches_dir, ...)`:** runs the reprompt
loop, validates PatchSpec, returns `AgentPatchResult(patch, attempts, reprompt_errors)`.
Does NOT apply or stage — caller decides.
**`stage_patch_for_human(patch_spec, patches_dir, failure_ctx)`:** writes to
`patches/pending/<patch_id>.json` with `_aq_meta` annotation.
**`archive_patch(...)`:** archives an applied PatchSpec to `patches/applied/`.

- ✅ `generate_agent_patch`: `ANTHROPIC_API_KEY` not set → `result.patch is None`
- ✅ `generate_agent_patch`: valid JSON → `result.patch` is a PatchSpec with expected `patch_id`
- ✅ `generate_agent_patch`: always-invalid response → `result.patch is None`, `call_count == MAX_REPROMPTS`
- ✅ `generate_agent_patch`: custom `max_reprompts` honoured (call_count == N)
- ✅ `generate_agent_patch`: invalid then valid → reprompts, succeeds, exactly 2 calls
- ✅ `generate_agent_patch`: `timeout=` forwarded to `_call_agent`
- ✅ `_call_openai_compat`: `ollama_*` provider_options stripped into `payload["options"]`; generic keys merged top-level; mixed both dispatched; `None` leaves payload unchanged (httpx.post mocked)
- ✅ `stage_patch_for_human`: creates `patches/pending/<patch_id>.json` with `_aq_meta.run_id` / `_aq_meta.blueprint_id`
- ✅ Surveyor `record()`: on failure returns FailureContext; on success returns None (LLM loop NOT triggered from record())

### Arcade `required_context` validation (`aqueduct/compiler/expander.py`)

**Behavior:** After loading sub-Blueprint, checks that every key in `sub_bp.required_context` is present in `arcade_module.context_override`. Missing keys → `ExpandError`.

- ✅ Arcade with `required_context: [foo]` and `context_override: {foo: bar}` → expands successfully
- ✅ Arcade with `required_context: [foo]` and no `context_override` → `ExpandError` containing `foo`
- ✅ Arcade with `required_context: [foo, bar]`, `context_override: {foo: x}` (missing bar) → `ExpandError` containing `bar`
- ✅ Arcade with empty `required_context` → always expands regardless of `context_override`
- ✅ Blueprint with `required_context: [env]` correctly round-trips through Parser (field preserved in AST)

---

## Failure Report

- [✅] `tests/test_cli/test_cli_aggressive.py::test_aggressive_mode_invalid_patch_stops_loop`: RESOLVED. Autonomous fix-and-verify loop verified end-to-end.
- [✅] `tests/test_parser/test_resolver.py::test_spark_config_undefined_ctx_raises_parseerror`: RESOLVED (ISSUE-027). App hoisted `spark_config`/`macros` `resolve_value()` into a guarded block raising `ParseError` on `ValueError` (mirrors `parser.py:131` module-config pattern). `xfail` marker removed; 21/21 resolver tests pass. Issue moved to `.dev/RESOLVED/ISSUE-027.md`.

---

## Per-module `on_failure` (`aqueduct/executor/executor.py`)

**Behavior:** `_module_retry_policy()` returns `RetryPolicy(**module.on_failure)` when set, else manifest-level policy. Applied at all 5 dispatch sites.

- ✅ `_module_retry_policy`: `on_failure=None` → returns manifest policy unchanged
- ✅ `_module_retry_policy`: valid `on_failure` dict → returns RetryPolicy with those fields
- ✅ `_module_retry_policy`: `on_failure` with unknown key → raises `ExecuteError` with message containing "invalid keys"
- ✅ Ingress module with `on_failure.max_attempts=3` retries 3×; other modules use manifest `max_attempts=1`
- ✅ `on_failure.on_exhaustion=abort` → blueprint stops after exhaustion; `trigger_agent` still fires LLM

## Checkpoint / Resume (`aqueduct/executor/executor.py`)

**Behavior:** `checkpoint: true` (blueprint or module level) writes Parquet + `_aq_done` marker after each successful data-producing module. `--resume <run_id>` reloads checkpoints and skips completed modules.

- ✅ `checkpoint=false` (default) → no files written to `.aqueduct/checkpoints/`
- ✅ blueprint-level `checkpoint: true` → all modules checkpointed after success
- ✅ per-module `checkpoint: true` only → only that module checkpointed; others not
- ✅ Ingress checkpoint: `.aqueduct/checkpoints/<run_id>/<module_id>/data/` Parquet exists after success
- ✅ Channel checkpoint: same path + `_aq_done` marker
- ✅ Funnel checkpoint: same pattern
- ✅ Egress checkpoint: only `_aq_done` written (no DataFrame)
- ✅ Junction checkpoint: each branch saved as `<branch_id>/` subfolder
- ✅ `--resume <run_id>` → module with `_aq_done` skipped, ModuleResult status="success"
- ✅ `--resume <run_id>` → Parquet reloaded into frame_store; downstream can consume it
- ✅ `--resume` with non-existent run_id → `ExecuteError` with clear path message
- ✅ `--resume` with mismatched manifest hash → warning logged, execution continues
- ✅ Checkpoint write failure (disk full) → warning logged, blueprint continues (non-fatal)

## `checkpoint` field in Parser/Compiler

- ✅ Blueprint with `checkpoint: true` round-trips through Parser → `Blueprint.checkpoint == True`
- ✅ Module with `checkpoint: true` round-trips through Parser → `Module.checkpoint == True`
- ✅ `Manifest.checkpoint` populated from Blueprint; `to_dict()` includes it
- ✅ Omitting `checkpoint` → defaults to `False` at all levels

---

## Phase 9 — Sub-DAG Execution, Backfill, Guardrails, Patch Rollback

### Sub-DAG selectors (`--from` / `--to`) — `aqueduct/executor/spark/executor.py`

**`_reachable_forward(start_id, edges)`:** BFS on data edges from start_id.
**`_reachable_backward(start_id, edges)`:** BFS on reverse data edges to start_id.
**`_selector_included(modules, edges, from_module, to_module)`:** returns `None` (no filter) when both are None; otherwise intersects forward set and backward set.

- ✅ `_reachable_forward`: linear A→B→C, start=A → {A, B, C}
- ✅ `_reachable_forward`: start=B → {B, C} (A excluded)
- ✅ `_reachable_forward`: fan-out A→B, A→C → {A, B, C}
- ✅ `_reachable_backward`: linear A→B→C, target=C → {A, B, C}
- ✅ `_reachable_backward`: target=B → {A, B} (C excluded)
- ✅ `_selector_included`: both None → returns None (no selector active)
- ✅ `_selector_included`: from_module only → returns forward-reachable set from that module
- ✅ `_selector_included`: to_module only → returns backward-reachable set up to that module
- ✅ `_selector_included`: both set → returns intersection (from forward ∩ to backward)
- ✅ `_selector_included`: from_module not in manifest → raises `ExecuteError` with clear message
- ✅ `_selector_included`: to_module not in manifest → raises `ExecuteError` with clear message
- ✅ executor: module not in `included_ids` → `ModuleResult(status="skipped")`, frame_store not populated
- ✅ executor: skipped upstream + included downstream → frame_store miss produces natural `ExecutionResult(status="error")` with clear message
- ✅ end-to-end: `--from clean_orders` skips Ingress module; ExecutionResult includes skipped Ingress entry
- ✅ end-to-end: `--from A --to B` on 3-module chain A→B→C: C status="skipped", A+B execute

### Logical execution date (`--execution-date`) — `aqueduct/compiler/runtime.py`

**`AqFunctions._execution_date`:** `date | None`, set at construction. **`_base_date()`:** returns `_execution_date` when set, else `date.today()`.

- ✅ `AqFunctions(execution_date=date(2026,1,15))._base_date()` returns `date(2026,1,15)`
- ✅ `AqFunctions()._base_date()` returns today's date
- ✅ `date_today()` with execution_date set → returns `"2026-01-15"` (not today)
- ✅ `date_yesterday()` with execution_date=2026-01-15 → `"2026-01-14"`
- ✅ `date_month_start()` with execution_date=2026-01-15 → `"2026-01-01"`
- ✅ `runtime_timestamp()` with execution_date set → `"2026-01-15T00:00:00+00:00"` (midnight UTC)
- ✅ `runtime_timestamp()` without execution_date → current UTC timestamp (not midnight)
- ✅ `compile()` with `execution_date=date(2026,1,15)` passed through to `AqFunctions`; `@aq.date.today()` resolves to `"2026-01-15"` in Manifest context
- ✅ CLI `--execution-date 2026-01-15` parses to `date(2026,1,15)` and passed to compiler
- ✅ CLI `--execution-date` invalid format → click error with clear message

### LLM Guardrails — `aqueduct/patch/apply.py` + `aqueduct/parser/`

**`_check_guardrails(patch_spec, bp_raw)`:** deterministic enforcement — reads `agent.guardrails` from Blueprint YAML dict, raises `PatchError` on violation. Not LLM-dependent.
**`GuardrailsConfig.allowed_paths`:** fnmatch patterns for `path`/`output_path` config values; empty = unrestricted.
**`GuardrailsConfig.forbidden_ops`:** op names blocked from auto-apply; empty = all permitted.

- ✅ `allowed_paths=[]` → no path violations regardless of patch content
- ✅ `forbidden_ops=[]` → no op violations regardless of patch content
- ✅ patch op in `forbidden_ops` → `PatchError` raised containing op name (deterministic)
- ✅ patch `set_module_config_key` with `key=path`, value matching an `allowed_paths` pattern → no violation
- ✅ patch `set_module_config_key` with `key=path`, value NOT matching any `allowed_paths` → `PatchError` raised
- ✅ patch with non-path key (e.g. `key=format`) → no path violation even if `allowed_paths` set
- ✅ patch `replace_module_config` with `config.path` matching `allowed_paths` → no violation. closes the bypass where full-config replacements were unchecked.
- ✅ patch `replace_module_config` with `config.path` NOT matching `allowed_paths` → `PatchError` raised
- ✅ patch `replace_module_config` with `config.output_path` NOT matching → `PatchError` raised
- ✅ patch `insert_module` whose `module.config.path` does not match → `PatchError` raised (inserted module would write outside allowlist)
- ✅ patch `add_probe` whose `module.config.path` does not match → `PatchError` raised
- ✅ patch `add_arcade_ref` whose `module.config.output_path` does not match → `PatchError` raised
- ✅ patch `set_module_config_key` with `module_id` containing `__` (arcade-expanded) → guardrail skipped, apply step raises clearer "Module not found" error. Confirms the carve-out remains.
- ✅ no `agent.guardrails` in Blueprint → unrestricted (no error)
- ✅ guardrail violation during auto-apply loop → `PatchError` raised; blueprint run ends with status="error"
- ✅ `GuardrailsConfig` round-trips through schema → parser → model (empty defaults)
- [✅] `test_agent_config_schema_parses_allowed_paths`
- [✅] `test_patch_rollback_restores_blueprint` (updated to Git-based CLI)
- ✅ old flat `allowed_paths`/`forbidden_ops` directly under `agent:` → schema validation error (extra="forbid")
- ✅ `heal_on_errors` + `never_heal_errors` parse correctly from YAML → `GuardrailsConfig` fields populated
- ✅ `never_heal_errors` matches `error_type` from `FailureContext` → LLM blocked, message emitted
- ✅ `never_heal_errors` matches stack trace class name → LLM blocked
- ✅ `never_heal_errors` takes priority over `heal_on_errors` when both match
- ✅ `heal_on_errors` non-empty, `error_type` matches → LLM proceeds
- ✅ `heal_on_errors` non-empty, `error_type` does NOT match → LLM blocked, message emitted
- ✅ `heal_on_errors=[]` (default) → no restriction, LLM proceeds
- ✅ `never_heal_errors=[]` (default) → no restriction
- ✅ `_check_heal_guardrails()` with `failure_ctx.error_type=None` → falls back to stack trace class
- ✅ `_check_heal_guardrails()` with both `error_type` and stack class → either match is sufficient

### Doctor guardrail typo detection — `aqueduct/doctor.py`
- ✅ `heal_on_errors` entry matches known Assert `error_type` → no warning
- ✅ `heal_on_errors` entry does NOT match any Assert `error_type` → `CheckResult(status="warn")` with clear message
- ✅ `never_heal_errors` entry typo → warning emitted
- ✅ no `heal_on_errors`/`never_heal_errors` → no warning emitted
- ✅ blueprint has no Assert modules → any entry produces warning (none to match against)

### Patch Rollback — `aqueduct patch rollback` — `aqueduct/cli.py`

**Phase 18 redesign:** file backups eliminated; rollback uses git via `aqueduct patch rollback <blueprint> --to <patch_id>`.
Old `patch rollback` tests above are superseded by Phase 18 rollback tests.

### Phase 10 — Channel `op: join` + SQL Macros ✅

#### Channel `op: join` — `aqueduct/executor/spark/channel.py`

- ✅ `op: join` missing `left` → `ChannelError`
- ✅ `op: join` missing `right` → `ChannelError`
- ✅ `op: join` missing `condition` for non-cross join → `ChannelError`
- ✅ `op: join` `join_type: cross` without condition → valid, no ON clause
- ✅ `op: join` invalid `join_type` → `ChannelError`
- ✅ `op: join` `broadcast_side: right` → `/*+ BROADCAST(right) */` hint in SQL
- ✅ `op: join` `broadcast_side: left` → `/*+ BROADCAST(left) */` hint in SQL
- ✅ `op: join` generates correct `LEFT JOIN` / `INNER JOIN` SQL
- ✅ unsupported `op` value → `ChannelError`
- ✅ end-to-end: Ingress × 2 → Channel(op: join) → Egress — joined rows correct (Spark test)

#### SQL Macros — `aqueduct/compiler/macros.py`

- ✅ `{{ macros.name }}` simple substitution → resolved in query
- ✅ `{{ macros.name(key=val) }}` parameterized → `{{ key }}` placeholders substituted
- ✅ quoted param value (`period='day'`) → quotes stripped, value inserted
- ✅ unknown macro name → `MacroError`
- ✅ missing param in body → `MacroError`
- ✅ empty macros dict → text returned as-is
- ✅ no `{{` in text → early return
- ✅ `resolve_macros_in_config` recurses into dict values
- ✅ `resolve_macros_in_config` recurses into list items
- ✅ `resolve_macros_in_config` passes through non-string values unchanged
- ✅ full compile: macros in Blueprint → expanded in Manifest query string (no `{{` in Manifest)
- ✅ end-to-end: Ingress → Channel(macro in query) → Egress runs correctly

### Phase 11 — Missing CLI Commands

#### `aqueduct report` — `aqueduct/cli.py`

- ✅ valid run_id → table output with module rows and status icons
- ✅ valid run_id + `--format json` → JSON with run_id, blueprint_id, status, module_results
- ✅ valid run_id + `--format csv` → CSV with header row
- ✅ unknown run_id → exit code 1 with error message
- ✅ missing observability.db → exit code 1 with error message

#### `aqueduct lineage` — `aqueduct/cli.py`

- ✅ valid blueprint_id → table of channel_id, output_column, source_table, source_column
- ✅ `--from <table>` filters to only that source_table
- ✅ `--column <col>` filters to only that output_column
- ✅ `--format json` → JSON array
- ✅ no rows → "No lineage records found" message, exit 0
- ✅ missing lineage.db → exit code 1 with error message

#### `aqueduct signal` — `aqueduct/cli.py` + `surveyor.py`

- ✅ `--value false` → row inserted in `signal_overrides` with `passed=False`
- ✅ `--value true` → row deleted from `signal_overrides`
- ✅ `--error "msg"` alone → row inserted with `passed=False` and `error_message` set
- ✅ `--error "msg" --value true` → exit code 1 (conflicting flags)
- ✅ no flags → prints current override status
- ✅ no override set → "no persistent override" message
- ✅ `evaluate_regulator()` checks `signal_overrides` BEFORE `probe_signals`
- ✅ override with `passed=False` → `evaluate_regulator()` returns False even if probe_signals says True
- ✅ `--value true` clears override → `evaluate_regulator()` resumes reading probe_signals

#### `aqueduct heal` — `aqueduct/cli.py`

- ✅ run_id with failure_context → FailureContext reconstructed, generate_agent_patch called
- ✅ `--module` overrides `failed_module` field in FailureContext passed to LLM
- ✅ run_id with no failure_context → exit code 1 with clear message
- ✅ missing observability.db → exit code 1
- ✅ no agent model configured in aqueduct.yml → exit code 1 with clear message
- ✅ LLM returns valid patch → patch staged in patches/pending/

### Phase 13 — `aqueduct test` Command

#### Test runner core — `aqueduct/executor/spark/test_runner.py`

- ✅ inline rows + schema → `createDataFrame` succeeds for all supported types (long, string, double, boolean, timestamp)
- ✅ unknown schema type → passes through to Spark DDL (Spark raises if truly invalid)
- ✅ `row_count` assertion passes: exact count match
- ✅ `row_count` assertion fails: non-zero exit, message shows expected vs actual
- ✅ `contains` assertion passes: all expected rows found in output
- ✅ `contains` assertion fails: missing rows listed in message
- ✅ `sql` assertion passes: expr over `__output__` returns truthy
- ✅ `sql` assertion fails: expr returns falsy
- ✅ `sql` assertion error: bad SQL → `passed=False` with error message
- ✅ Channel module executed against inline inputs → correct output rows
- ✅ Assert module: passing rows returned, quarantine rows discarded (no spillway edge in test)
- ✅ Ingress/Egress module → `TestError` with clear message
- ✅ missing `module` field → `TestCaseResult` with error
- ✅ module not found in blueprint → `TestCaseResult` with error
- ✅ missing `inputs` → `TestCaseResult` with error
- ✅ missing blueprint → `TestError`
- ✅ Junction module: first branch used when no `branch:` specified
- ✅ Junction module: `branch: <name>` targets specific branch

#### `aqueduct test` CLI command — `aqueduct/cli.py`

- ✅ all tests pass → exit code 0, "N passed" in output
- ✅ any test fails → exit code 1, failure details and "N failed" in output
- ✅ test file error (bad blueprint path) → exit code 1 with error message
- ✅ invalid YAML test file → exit code 1 with parser error
- ✅ `--quiet` suppresses Spark progress (quiet=True passed to make_spark_session)
- ✅ `--blueprint` overrides blueprint path from test file
- ✅ ISSUE-026: `stop_spark_session(spark)` skips `spark.stop()` when `AQ_TESTING` is set (returns without touching the session) — `tests/test_executor/test_executor_session.py::TestStopSparkSessionGuard`
- ✅ ISSUE-026: `stop_spark_session(spark)` calls `spark.stop()` when `AQ_TESTING` is unset (monkeypatch.delenv + Mock spark, assert `.stop()` called once) — `tests/test_executor/test_executor_session.py::TestStopSparkSessionGuard`
- ✅ ISSUE-026: invoking `aqueduct test` then `aqueduct doctor` via CliRunner inside the suite does NOT tear down the shared session-scoped `spark` fixture (a subsequent `spark.range(1).count()` still works) — `tests/test_cli/test_cli_issue026.py`

### Phase 14 — Aggressive mode in-memory validation (validate_patch removed)

`validate_patch` field removed. `aggressive` mode now always validates patch in-memory (compile + re-run) before writing to Blueprint. Non-configurable. Tests that covered old `validate_patch` field removed from `test_coverage_gaps.py`.

#### CLI dispatch — `aqueduct/cli.py` (aggressive mode)
- ✅ `approval_mode: aggressive` + patch produces invalid Blueprint (compile fail) → Blueprint unchanged, loop stops
- ✅ `approval_mode: aggressive` + patch valid but re-run fails → `on_heal_failure` applied, loop continues
- ✅ `approval_mode: aggressive` + patch valid + re-run succeeds → Blueprint written to disk, loop stops

---

## Stubs 1-4 — on_exhaustion / trigger_agent / block_full_actions

### `ExecutionResult.trigger_agent` — `aqueduct/executor/models.py`

- ✅ `ExecutionResult` has `trigger_agent: bool = False` field
- ✅ `ExecutionResult.to_dict()` includes `trigger_agent` key
- ✅ `trigger_agent=True` frozen dataclass — mutation raises `FrozenInstanceError`

### `_on_retry_exhausted()` + `_fail()` — `aqueduct/executor/spark/executor.py`

**Behavior:** `_fail()` accepts `trigger_agent` kwarg; `_on_retry_exhausted()` maps `on_exhaustion` → (gate_closed, fail_result).

- ✅ `on_exhaustion: abort` → `_on_retry_exhausted` returns `(False, fail_result)` with `trigger_agent=False`
- ✅ `on_exhaustion: alert_only` → returns `(True, None)` — warning logged, gate_closed sentinel set
- ✅ `on_exhaustion: trigger_agent` → returns `(False, fail_result)` with `trigger_agent=True`
- ✅ Ingress `on_exhaustion: alert_only` exhausted → `frame_store[module.id] = _GATE_CLOSED`, downstream skipped, blueprint continues
- ✅ Channel `on_exhaustion: alert_only` exhausted → same sentinel behavior
- ✅ Egress `on_exhaustion: alert_only` exhausted → `continue` (no sentinel needed — Egress is terminal)
- ✅ Ingress `on_exhaustion: trigger_agent` exhausted → `ExecutionResult(trigger_agent=True)`
- ✅ Egress `on_exhaustion: trigger_agent` exhausted → `ExecutionResult(trigger_agent=True)`

### Assert `trigger_agent` propagation — `executor.py` Assert dispatch

- ✅ Assert rule with `on_fail: trigger_agent` → `AssertError.trigger_agent=True` → `ExecutionResult.trigger_agent=True`
- ✅ Assert rule with `on_fail: abort` → `ExecutionResult.trigger_agent=False`

### `danger.allow_full_probe_actions` — `executor/spark/probe.py`

The legacy `probes.block_full_actions_in_prod` flag was removed in v1.0.0a3. The
active gate is `danger.allow_full_probe_actions` (inverted polarity: when false,
costly Probe sample-scan signals are skipped). `cli.py` derives the
`block_full_actions` argument passed into `execute_probe()` from
`not cfg.danger.allow_full_probe_actions`.

**`execute_probe(…, block_full_actions=False)`**, **`_row_count_estimate(…, block_full_actions=False)`**, **`_null_rates(…, block_full_actions=False)`**.

- ✅ `block_full_actions=False` (default) → `row_count_estimate` sample `.count()` executes normally
- ✅ `block_full_actions=True` → `row_count_estimate` method=sample → skips `.count()`, returns `{"blocked": True, "estimate": None}` + warning logged
- ✅ `block_full_actions=True` → `row_count_estimate` method=spark_listener → DuckDB query still runs (no Spark action, not affected)
- ✅ `block_full_actions=True` → `null_rates` → skips `.count()` + `.collect()`, returns `{"blocked": True, "null_rates": {col: None, ...}}` + warning logged
- ✅ `block_full_actions=False` → `null_rates` executes normally
- ✅ `execute()` accepts `block_full_actions: bool = False`; threaded to `execute_probe()`

### CLI trigger_agent override — `aqueduct/cli.py`

- ✅ `result.trigger_agent=True` + `approval_mode=disabled` → `effective_mode` set to `"human"`, message printed to stderr
- ✅ `result.trigger_agent=False` + `approval_mode=disabled` → loop breaks immediately (no LLM)
- ✅ `result.trigger_agent=True` + `approval_mode=human` → `effective_mode` stays `"human"` (already correct; no override message printed)
- ✅ `not cfg.danger.allow_full_probe_actions` passed to `execute()` as `block_full_actions`

---

## Phase 16 — Store Layout + `aqueduct runs` + LLM Patch Reliability

### Store layout — `observability.db` merge (`aqueduct/config.py`, `surveyor/`, `executor/spark/`)

- ✅ `stores.observability.path` defaults to `".aqueduct/observability.db"` (full file path; field renamed from `observability`)
- ✅ `stores.lineage.path` defaults to `".aqueduct/lineage.db"` (full file path)
- ✅ `stores.depot.path` defaults to `".aqueduct/depot.db"`
- ✅ unknown key `stores.observability` in YAML → `ConfigError` (extra="forbid")
- ✅ `aqueduct run` with default `stores.observability.path` → DuckDB store under per-pipeline `.aqueduct/observability/<blueprint_id>/observability.db`
- ✅ `aqueduct run` with **custom** `stores.observability.path: /tmp/my_obs.db` (+ `stores.lineage.path: /tmp/my_lin.db`) → store files created at exactly those paths/filenames; CLI does NOT clobber to `observability.db`/`lineage.db`
- ✅ `aqueduct run --store-dir DIR` → obs/lineage under `DIR/` (get_stores `store_dir_override`), rebuild branch skipped
- ✅ `Surveyor.start()` creates `observability.db` (not `runs.db`)
- ✅ `Surveyor.evaluate_regulator()`: reads `signal_overrides` + `probe_signals` from `observability.db`
- ✅ `Surveyor.get_probe_signal()`: reads from `observability.db`; returns empty list if `observability.db` absent
- ✅ `execute_probe()`: writes `probe_signals` rows to `observability.db`
- ✅ `_write_stage_metrics()`: writes `module_metrics` rows to `observability.db`
- ✅ `records_read` updated via `_update_metric` after Egress completes (Phase 18 logic)
- ✅ `aqueduct signal`: reads/writes `signal_overrides` in `observability.db`
- ✅ `aqueduct doctor` obs check: opens `observability.db` file (not directory probe)

### `schema_snapshot` path (`aqueduct/executor/spark/probe.py`)

- ✅ `schema_snapshot`: JSON written to `store_dir/snapshots/<run_id>/<probe_id>_schema.json` (not `store_dir/signals/<run_id>/...`)

### `aqueduct runs` command (`aqueduct/cli.py`)

- ✅ `aqueduct runs` with no observability.db → prints "No runs found" without error
- ✅ `aqueduct runs` lists recent runs ordered by `started_at DESC`
- ✅ `aqueduct runs --failed` → shows only runs with `status="error"`
- ✅ `aqueduct runs --blueprint blueprint.yml` → filters by blueprint_id from file
- ✅ `aqueduct runs --last 5` → shows at most 5 rows
- ✅ default output has columns: `run_id`, `blueprint_id`, `status`, `started_at`, `finished_at`

### LLM `prompt_context` threading (`aqueduct/agent/__init__.py`, `aqueduct/parser/`, `aqueduct/compiler/`)

- ✅ `agent.prompt_context` in `aqueduct.yml` → appended to LLM system prompt
- ✅ `agent.prompt_context` in Blueprint `agent:` block → appended to LLM system prompt (after engine-level context)
- ✅ both engine and blueprint `prompt_context` set → both included; blueprint comes second
- ✅ `AgentConfig.prompt_context` round-trips through Parser → `Blueprint.agent.prompt_context`
- ✅ `Manifest.to_dict()["agent"]["prompt_context"]` present when set

### `blueprint_source_yaml` in LLM context (`aqueduct/surveyor/`)

- ✅ `FailureContext.blueprint_source_yaml` populated when blueprint file exists at `_blueprint_path`
- ✅ `FailureContext.blueprint_source_yaml` is `None` when blueprint file path not set
- ✅ `FailureContext.to_dict()` includes `"blueprint_source_yaml"` key
- ✅ LLM user prompt includes "Original Blueprint YAML" section when `blueprint_source_yaml` is non-None
- ✅ LLM system prompt includes CRITICAL rule about using template expressions (not resolved literal paths)

### ruamel YAML formatting preservation (`aqueduct/patch/apply.py`, `aqueduct/patch/operations.py`)

- ✅ `apply_patch_to_dict()` uses round-trip copy (not `copy.deepcopy`) — input Blueprint comment metadata preserved
- ✅ patched Blueprint YAML has list items at col+2 (`  - item`) not col 0 (`- item`)
- ✅ `insert_module` op: injected module dict preserves string quotes in output YAML
- ✅ patched Blueprint YAML has list items at col+2 (`  - item`) not col 0 (`- item`)
- ✅ `insert_module` op: injected module dict preserves string quotes in output YAML
- ✅ `replace_module_config` op: injected config dict strings are double-quoted in output YAML
- ✅ round-trip of patched Blueprint through Parser succeeds (no YAML parse error)

### `agent.timeout` / `agent.max_reprompts` (`aqueduct/config.py`, `aqueduct/agent/__init__.py`)

- ✅ `AgentConnectionConfig.timeout` default `120.0`; custom value in YAML respected
- ✅ `AgentConnectionConfig.max_reprompts` default `3`; custom value in YAML respected
- ✅ `generate_agent_patch()` uses `timeout` for HTTP socket timeout (not hardcoded 120)
- ✅ LLM returns invalid PatchSpec JSON → reprompts up to `max_reprompts` times; returns None after

---

## Phase 17 — `aqueduct init`

### `init` command (`aqueduct/cli.py`)

- ✅ `aqueduct init` in empty dir: creates `blueprints/`, `aqueduct.yml.template`, `arcades/`, `tests/`, `patches/pending/`, `patches/rejected/`, `benchmarks/`
- ✅ `aqueduct init`: project name derived from `cwd.name` (no `--name` flag; dropped 2026-05-15)
- ✅ `aqueduct init` when files already exist: existing files skipped (not overwritten), new dirs still created
- ✅ `git init` run when not already in a git repo; skipped when already in one
- ✅ `git commit` run after scaffold; output line printed
- ✅ `git commit` fails with "nothing to commit" → no error printed (silent)
- ✅ git not installed → scaffold succeeds; git steps skipped with warning


## Phase 18 — Git-Integrated Patch Lifecycle

### `_uncommitted_applied_patches()` — `aqueduct/cli.py`
- ✅ applied patch with `applied_at` > last git commit timestamp → returned
- ✅ applied patch with `applied_at` ≤ last git commit timestamp → not returned
- ✅ not in a git repo → all applied patches returned
- ✅ blueprint never committed → all applied patches returned (git log returns empty)
- ✅ no applied patches dir → returns empty list
- ✅ `_aq_meta.applied_at` field used when top-level `applied_at` absent

### Patch naming — `_patch_filename()` — `aqueduct/agent/__init__.py`
- ✅ `stage_patch_for_human` writes `{seq:05d}_{ts}_{slug}.json` format
- ✅ `archive_patch` writes same structured naming
- ✅ seq = count of all .json files across pending/ + applied/ + rejected/ + 1
- ✅ `reject_patch` resolves `*_{patch_id}.json` glob when exact name not found

### `aqueduct patch commit` — `aqueduct/cli.py`
- ✅ no uncommitted patches → prints "Nothing to commit" and exits 0
- ✅ 1 uncommitted patch → commit message subject = patch rationale
- ✅ N>1 uncommitted patches → commit message subject = "N patches applied"
- ✅ `---aqueduct---` block present in commit message with patch stems, run_id, ops
- ✅ `git add <blueprint> && git commit` run; short hash printed on success
- ✅ not in a git repo → error on `git add`; exits 1
- ✅ ops deduplicated (same op type multiple times → appears once in ops field)

### `aqueduct patch discard` — `aqueduct/cli.py`
- ✅ `git checkout HEAD -- blueprint` restores blueprint to last committed state
- ✅ uncommitted applied patches moved back to `patches/pending/`
- ✅ no uncommitted patches → git checkout still runs; no patches moved
- ✅ git checkout failure → exits 1 with error message
- ✅ patches moved count printed in output

### `aqueduct patch log <blueprint>` — `aqueduct/cli.py`
- ✅ no git history for blueprint → prints "No git history for this blueprint."
- ✅ commit with `---aqueduct---` block → patch_id + ops extracted and shown
- ✅ commit without `---aqueduct---` block → shows "(manual change)"
- ✅ `--format json` → array of objects with hash, date, patches, ops, run_id fields
- ✅ long patches column truncated to 40 chars with `..` suffix

### `aqueduct patch rollback <blueprint> --to <patch_id>` — `aqueduct/cli.py`
- ✅ patch_id found → checks out blueprint file(s) from parent commit; stages and commits; prints hash
- ✅ patch_id found in arcade commit (multiple files) → all touched files restored and committed together
- ✅ patch_id not found → error message with hint to run `aqueduct patch log`; exits 1
- ✅ parent commit resolution fails (first-ever commit) → exits 1 with error
- ✅ `git checkout <file>` failure → exits 1 with stderr; no commit created
- ✅ `git commit` failure → exits 1 with stderr
- ✅ `--hard` flag no longer accepted (removed; passing it produces click error)

### Run-start uncommitted patch warning — `aqueduct/cli.py`
- ✅ uncommitted applied patches exist → warning printed to stderr before run starts
- ✅ no uncommitted patches → no warning
- ✅ warning text includes "aqueduct patch commit --blueprint <path>"

### `aqueduct patch reject` — path-or-slug argument — `aqueduct/cli.py`
- ✅ full file path passed (e.g. `patches/pending/00001_*.json`) → patches_dir derived from grandparent; patch moved to rejected/
- ✅ bare patch_id slug passed (old behaviour) → `--patches-dir` or CWD/patches used
- ✅ file path with `parent.name == "pending"` but file does not exist → derivation still correct, not found error from reject_patch
- ✅ rejected file written with `rejected_at` and `rejection_reason` fields

### `aqueduct patch list` — `aqueduct/cli.py`
- ✅ pending patches present → tabular output with file, patch_id, rationale columns
- ✅ no pending patches → "No pending patches found" message
- ✅ `--status=applied` → lists applied/ dir
- ✅ `--status=all` → lists pending/, applied/, rejected/ sections
- ✅ `--blueprint <path>` → patches_dir derived via walk-up from blueprint
- ✅ no blueprint, no patches-dir → walk-up to aqueduct.yml to find project root
- ✅ rationale truncated to 60 chars in table output
- ✅ apply/reject hint lines printed after pending table

### `_patches_root_from_blueprint()` — `aqueduct/cli.py`
- ✅ blueprint in `blueprints/` subdir, `aqueduct.yml` at project root → returns `<root>/patches`
- ✅ no `aqueduct.yml` found after 8 levels → returns `<blueprint_parent>/patches`
- ✅ all patch commands (`apply`, `commit`, `discard`, `list`, `reject`) use same root when `--patches-dir` not set

### `aqueduct doctor <blueprint>` — format/extension mismatch — `aqueduct/doctor.py`
- ✅ `format=parquet` + path `*.parquet` → ok, no mismatch warning
- ✅ `format=csv` + path `*.parquet` → warn: "format='csv' but file extension suggests different format"
- ✅ `format=parquet` + path `*.csv` → warn
- ✅ `format=delta` → no mismatch check (delta dirs have no single extension)
- ✅ unknown format → no mismatch check
- ✅ glob with mixed extensions (some match, some don't) → warn on mismatch files
- ✅ non-glob path: single file checked for extension mismatch

### LLM doctor hints injection — `aqueduct/cli.py` + `aqueduct/agent/__init__.py`
- ✅ blueprint has warn doctor result → `failure_ctx.doctor_hints` non-empty before LLM call
- ✅ doctor check throws exception → exception swallowed; `doctor_hints` stays empty; self-healing continues
- ✅ `doctor_hints` non-empty → LLM prompt contains "Blueprint issues detected before run" section
- ✅ `doctor_hints` empty → section absent from LLM prompt
- ✅ `FailureContext.to_dict()` includes `doctor_hints` list

## Phase 19 — Provenance Layer

### `ValueProvenance` / `infer_value_provenance()` — `aqueduct/compiler/provenance.py`
- ✅ literal string → source_type="literal", original_expression=value
- ✅ non-string literal (int, bool) → source_type="literal"
- ✅ `${ctx.paths.foo}` → source_type="context_ref", context_key="paths.foo"
- ✅ `${ENV_VAR:-default}` → source_type="env_ref", env_var="ENV_VAR"
- ✅ `@aq.date.today()` → source_type="tier1"
- ✅ arcade_module_id set + ctx ref → source_type="arcade_inherited", context_key preserved
- ✅ arcade_module_id set + literal → source_type="arcade_inherited", context_key=None

### `build_config_provenance()` — `aqueduct/compiler/provenance.py`
- ✅ flat config dict → one key per scalar
- ✅ nested config dict → dot-notation keys (e.g. "options.mergeSchema")
- ✅ list value → tracked at list key level (not per-item)
- ✅ None raw_config → empty result

### `ProvenanceMap` — `aqueduct/compiler/provenance.py`
- ✅ `for_module()` returns correct `ModuleProvenance` or None
- ✅ `to_dict()` is JSON-serializable (no pyspark types, no dataclasses)

### Compiler builds ProvenanceMap — `aqueduct/compiler/compiler.py`
- ✅ top-level module with literal path → `source_type="literal"` in provenance
- ✅ top-level module with `${ctx.path}` → `source_type="context_ref"`, context_key correct
- ✅ context value tracked with correct source_type in `ProvenanceMap.context`
- ✅ `blueprint_path=None` → provenance_map still built (empty blueprint_path)
- ✅ `Manifest.provenance_map` is not None after `compile()` with blueprint_path

### Expander tags arcade modules — `aqueduct/compiler/expander.py`
- ✅ expanded module ID (`arcade__submod`) has `arcade_module_id` set
- ✅ expanded module has correct `sub_blueprint_path` and `original_module_id`
- ✅ arcade config value from context_override key → `source_type="arcade_inherited"`, `context_key` set
- ✅ arcade config literal value → `source_type="arcade_inherited"`, `context_key=None`
- ✅ `expand_arcades()` returns 3-tuple `(modules, edges, provenance_dict)`
- ✅ nested arcade (arcade inside arcade) → provenance tracked at both levels

### `FailureContext.provenance_json` — `aqueduct/surveyor/models.py`
- ✅ `provenance_json` field present; defaults to None
- ✅ `to_dict()` includes `provenance_json`
- ✅ `provenance_json=None` → `to_dict()["provenance_json"]` is None

### Surveyor builds provenance_json — `aqueduct/surveyor/surveyor.py`
- ✅ Manifest has provenance_map → `failure_ctx.provenance_json` is valid JSON
- ✅ provenance slice contains only failed module + full context block (not all modules)
- ✅ Manifest has no provenance_map → `provenance_json` is None


### LLM prompt provenance section — `aqueduct/agent/__init__.py`
- ✅ `_build_provenance_section(None)` → empty string
- ✅ arcade-expanded module → "Arcade-expanded" and "does NOT exist in the Blueprint YAML" in output
- ✅ context_ref value → "use replace_context_value(key=...)" hint shown
- ✅ literal value → "use set_module_config_key" hint shown
- ✅ env_ref value → env var name shown, no patch suggestion
- ✅ context block summary lists all context keys with resolved values
- ✅ `blueprint_source_section` placeholder gone from template; `provenance_section` present

### Guardrails resolve `${ctx.*}` — `aqueduct/patch/apply.py`
- ✅ `set_module_config_key` with `path="${ctx.paths.foo}"` + provenance_map with resolved value → matches `allowed_paths`
- ✅ `set_module_config_key` with literal path → matches normally without provenance_map
- ✅ `replace_context_value` op is never path-checked
- ✅ `apply_patch_file()` accepts optional `provenance_map` kwarg

### `check_blueprint_sources_from_manifest()` — `aqueduct/doctor.py`
- ✅ arcade-expanded Ingress modules included (no recursion needed)
- ✅ path values are fully resolved strings (no `${ctx.*}` refs)
- ✅ format mismatch detected on resolved path
- ✅ JDBC module checked by host:port
- ✅ cloud URI → skip result
- ✅ project root derived from `provenance_map.blueprint_path`

### Parallel branch execution — `aqueduct/executor/spark/executor.py`
- ✅ `_find_connected_components`: single module → one component
- ✅ `_find_connected_components`: two modules connected by edge → one component
- ✅ `_find_connected_components`: two disconnected Ingress→Egress chains → two components
- ✅ `_find_connected_components`: signal-only edge (port="signal") does not merge components
- ✅ `parallel=False` (default) → `_find_connected_components` never called; serial loop runs
- ✅ `parallel=True`, single component → correctly identified and executed serially
- ✅ `parallel=True`, two independent components → dispatched to `ThreadPoolExecutor` and executed concurrently
- ✅ `parallel=True`, one component fails → first failure sets `_cancel_event`; other component continues or skips
- ✅ `parallel=True`, trigger_agent failure → `ExecutionResult.trigger_agent=True` propagated correctly
- ✅ `parallel=True`, both components succeed → `ExecutionResult(status="success")` with all module results merged
- ✅ Verified on Python 3.14 (with `pyspark.cloudpickle` patch active in `session.py`)

- ✅ unexpected thread exception (not ChannelError etc) → cancel_event set, error logged, run returns error

### Channel op completion — `aqueduct/executor/spark/channel.py`

#### op=deduplicate
- ✅ no key, no order_by → `dropDuplicates()` on all columns
- ✅ key only → `dropDuplicates([key_cols])` — arbitrary row kept per key
- ✅ key + order_by → Window+row_number(); row with rank=1 kept; `_aq_rank` column dropped
- ✅ order_by without key → ChannelError raised

#### op=filter
- ✅ valid condition → rows matching condition returned
- ✅ missing condition → ChannelError
- ✅ invalid SQL expression → ChannelError wrapping Spark exception


#### op=select
- ✅ list of columns → only those columns in result
- ✅ single string column → works (auto-wrapped in list)
- ✅ missing columns field → ChannelError
- ✅ non-existent column name → ChannelError from Spark

#### op=rename
- ✅ dict form `{old: new}` → column renamed
- ✅ list form `[{from, to}]` → column renamed
- ✅ multiple renames applied in order
- ✅ missing columns → ChannelError

#### op=cast
- ✅ dict form `{col: type}` → column cast
- ✅ list form `[{column, type}]` → column cast
- ✅ invalid type string → ChannelError wrapping Spark exception
- ✅ missing columns → ChannelError

#### op=sort
- ✅ string order_by → single sort expr applied
- ✅ list order_by → multiple sort exprs applied in order
- ✅ missing order_by → ChannelError

#### op=union
- ✅ two upstreams → rows combined via unionByName
- ✅ allow_missing_columns=true (default) → missing cols filled with null
- ✅ allow_missing_columns=false → AnalysisException if schemas differ
- ✅ single upstream → ChannelError (requires ≥2)

#### op=repartition
- ✅ num_partitions only → df.repartition(n)
- ✅ num_partitions + column → df.repartition(n, col)
- ✅ missing num_partitions → ChannelError

#### op=coalesce
- ✅ num_partitions set → df.coalesce(n)
- ✅ missing num_partitions → ChannelError
- ✅ coalesce to 1 → single partition (verified via df.rdd.getNumPartitions())

#### op=cache
- ✅ no storage_level → defaults to MEMORY_AND_DISK
- ✅ storage_level: DISK_ONLY → df.persist(StorageLevel.DISK_ONLY)
- ✅ invalid storage_level → ChannelError with valid levels listed
- ✅ cached df is reused (same object reference in frame_store)

#### multi-input guard
- ✅ single-input op with 2 upstreams → ChannelError mentioning "use op=union first"

#### unknown op
- ✅ op: "banana" → ChannelError listing all valid ops

#### metrics_boundary
- ✅ `metrics_boundary: false` (default) → result df returned unchanged, no repartition applied
- ✅ `metrics_boundary: true` on `op: sql` → result df wrapped with `repartition(n)` where `n = df.rdd.getNumPartitions()`
- ✅ `metrics_boundary: true` on `op: filter` → boundary applied
- ✅ `metrics_boundary: true` on `op: union` → boundary applied
- ✅ `metrics_boundary: true` with 0-partition df → `repartition(1)` used (not `repartition(0)`)
- ✅ `metrics_boundary: true` on `op: repartition` → boundary applied after user's repartition (accepted; user opted in)
- ✅ `metrics_boundary` absent from config → no repartition (falsy default)

---

### Phase 21 Part C: Bug Fixes — `aqueduct/executor/spark/`

#### schema_hint flat dict bypass — `ingress.py`
- ✅ flat dict `{col_name: type}` → treated as strict schema check (previously silently skipped)
- ✅ nested dict `{mode: additive, columns: [...]}` → still works correctly
- ✅ list form `[{name, type}]` → still works correctly
- ✅ flat dict with wrong type → IngressError raised with column name and mismatch detail
- ✅ flat dict with missing column → IngressError raised
- ✅ type alias normalization: `LONG` accepted as `bigint`, `INTEGER` as `int`, `BOOL` as `boolean`, `SHORT` as `smallint`, `BYTE` as `tinyint`
- ✅ mixed case alias `Long`/`STRING` normalized correctly
- ✅ types not in alias map lowercased verbatim (`DOUBLE` → `double`)

#### spillway_rate rule — `assert_.py`
- ✅ no quarantine rules → spillway_rate gets count=0, passes when max>0
- ✅ 20% rows quarantined, max=0.3 → passes
- ✅ 20% rows quarantined, max=0.1 → fires on_fail
- ✅ on_fail=abort → AssertError raised; passing_df still returned before raise
- ✅ on_fail=warn → warning logged, pipeline continues, quarantine_df returned
- ✅ spillway_rate always evaluated after row-level rules (Phase 4 ordering)
- ✅ empty quarantine_df (no row rules match) → quarantine_count=0

#### mode: merge — `egress.py`
- ✅ mode=merge, format=delta, path, merge_key (str) → MERGE INTO executed via spark.sql
- ✅ mode=merge, merge_key=[list] → ON clause uses AND-joined conditions
- ✅ mode=merge, format=parquet → EgressError: only delta supported
- ✅ mode=merge, missing merge_key → EgressError
- ✅ mode=merge, table: catalog_name → uses catalog name (not delta.`path`)
- ✅ MERGE INTO: matched rows updated, unmatched rows inserted (end-to-end Delta)
- ✅ temp view `_aq_merge_src` dropped in finally block even on failure

---

### Phase 22 — Scenario Testing + LLM Benchmark

#### `aqueduct/surveyor/scenario.py` — scenario model + runner
- ✅ `load_scenario`: valid .aqscenario.yml → AqScenario dataclass
- ✅ `load_scenario`: missing aqueduct_scenario version → ValueError
- ✅ `load_scenario`: missing `id` → ValueError
- ✅ `load_scenario`: missing `inject_failure` → ValueError
- ✅ `_match_op_spec`: exact key match → True
- ✅ `_match_op_spec`: value_contains substring → True / False
- ✅ `_match_op_spec`: partial spec (only `op`) → matches any op of that type
- ✅ `_check_expected_patch`: all ops matched → no failures
- ✅ `_check_expected_patch`: unmatched expected op → failure message with generated ops listed
- ✅ `_check_expected_patch`: forbidden op present → failure message
- ✅ `_check_assertions`: patch_is_valid=true + patch=None → failure
- ✅ `_check_assertions`: patch_applies=true + apply succeeds → patch_applies=True
- ✅ `_check_assertions`: patch_applies=true + apply fails → failure with error detail
- ✅ `run_scenario`: bad blueprint path → ScenarioResult(passed=False, failures=[...])
- ✅ `run_scenario`: LLM returns None → ScenarioResult(passed=False, patch_valid=False)
- ✅ `format_benchmark_table`: single model single scenario → correct table shape
- ✅ `format_benchmark_table`: summary rows (parse rate, apply rate, pass rate, avg confidence)

#### Prompt versioning — `aqueduct/agent/__init__.py`
- ✅ `PROMPT_VERSION` constant present in module
- ✅ `stage_patch_for_human`: _aq_meta includes prompt_version
- ✅ `archive_patch`: _aq_meta includes prompt_version

#### CLI — `aqueduct/cli.py`
- ✅ `heal --scenario <path>`: loads scenario, runs against configured model, prints PASS/FAIL
- ✅ `heal --scenario <path>`: scenario fails → sys.exit(1)
- ✅ `heal <run_id>`: still works (existing flow unbroken)
- ✅ `heal` with no args: error message prompting for run_id or --scenario
- ✅ `heal <run_id> --print-prompt`: prints system block + user block to stdout, exits 0, no LLM called
- ✅ `heal <run_id> --print-prompt --print-prompt-format json`: output is valid JSON with "system" and "user" keys
- ✅ `heal --scenario <path> --print-prompt`: builds FailureContext from scenario, prints prompt, exits 0
- ✅ `heal --print-prompt` with no agent.model configured: succeeds (model guard skipped)
- ✅ `benchmark --scenarios <dir> --model A --model B`: runs all scenarios, prints table
- ✅ `benchmark --output json`: outputs JSON dict {scenario_id: {model: {passed, confidence, ...}}}
- ✅ `benchmark`: any FAIL → sys.exit(1); all PASS → sys.exit(0)

## Phase 23B — Input Fingerprinting

#### Compiler — `aqueduct/compiler/compiler.py`
- ✅ `compile()`: local Ingress path → `inputs_fingerprint[module_id]` has `size_bytes` int and ISO-8601 `last_modified`
- ✅ `compile()`: remote Ingress path (`s3a://...`) → `inputs_fingerprint[module_id]` has `size_bytes=None`, `last_modified=None`
- ✅ `compile()`: format=jdbc Ingress → fingerprint entry has `size_bytes=None` (skip stat)
- ✅ `compile()`: path does not exist (OSError) → fingerprint entry has `size_bytes=None`
- ✅ `compile()`: non-Ingress modules not in `inputs_fingerprint`
- ✅ `Manifest.to_dict()` includes `inputs_fingerprint` key
- ✅ `compile()`: Arcade-expanded Ingress (sub-blueprint Ingress namespaced as `{arcade_id}__{child_id}`) with a local path → fingerprint entry exists keyed by the expanded ID with stat fields populated. confirms `inputs_fingerprint` walks the post-expansion module list, not pre-expansion.
- ✅ `compile()`: Arcade-expanded Ingress with remote path → fingerprint entry exists keyed by expanded ID with `size_bytes=None`, `last_modified=None`. Mirrors the top-level remote-path case.

## Phase 23C — Incremental Channel

#### Executor — `aqueduct/executor/spark/executor.py`
- ✅ `execute()`: `materialize=incremental`, no prior watermark → query `${ctx._watermark}` replaced with sentinel `'1900-01-01 00:00:00'`
- ✅ `execute()`: `materialize=incremental`, prior watermark in Depot → query substituted with stored value
- ✅ `execute()`: `materialize=incremental`, success → new MAX(watermark_column) written to Depot
- ✅ `execute()`: `materialize=incremental`, Channel fails → watermark NOT updated in Depot
- ✅ `execute()`: `materialize=incremental`, downstream Egress has `mode=overwrite` → warning logged
- ✅ `execute()`: no `materialize` key → normal Channel execution, no watermark logic
- ✅ `execute()`: `materialize=incremental`, depot=None → query uses sentinel, no crash

#### Phase 24c — Sidecar Watermark
- ✅ `_read_watermark_sidecar()`: sidecar absent → returns None
- ✅ `_read_watermark_sidecar()`: valid sidecar → returns watermark string
- ✅ `_read_watermark_sidecar()`: corrupt JSON → returns None (non-fatal)
- ✅ `_write_watermark_sidecar()`: writes atomic rename (`*.json.tmp` → `*.json`)
- ✅ `_write_watermark_sidecar()`: `store_dir=None` → no-op, no crash
- ✅ `_compute_watermark_from_output()`: parquet format → `spark.read.parquet.agg(MAX).collect()`
- ✅ `_compute_watermark_from_output()`: delta format → `SELECT MAX() FROM delta.\`path\``
- ✅ `_compute_watermark_from_output()`: path doesn't exist → returns None (non-fatal)
- ✅ `execute()`: incremental Channel → sidecar read takes priority over Depot value
- ✅ `execute()`: incremental Channel + Egress succeeds → sidecar written at `store_dir/watermarks/`
- ✅ `execute()`: incremental Channel + Egress fails → sidecar NOT written, watermark NOT advanced
- ✅ `execute()`: incremental Channel, depot=None → sidecar still written after Egress write
- ✅ `execute()`: `_pending_watermarks` NOT populated for non-incremental Channel
- ✅ `execute()`: `agg(MAX).collect()` on lazy Channel df NO LONGER called before Egress write (no double-scan)

## Compiler Warning — Hadoop FS Keys in Ingress Options (ISSUE-001)

#### Compiler — `aqueduct/compiler/compiler.py`
- ✅ Ingress with `options: {fs.s3a.access.key: ...}` → `AQ-WARN [perf_hadoop_fs_in_options]` containing "spark_config"
- ✅ Ingress with `options: {fs.gs.project.id: ...}` → warning emitted
- ✅ Ingress with `options: {fs.azure.account.key: ...}` → warning emitted
- ✅ Ingress with `options: {header: true}` (non-Hadoop key) → no warning
- ✅ Non-Ingress module with `options` containing `fs.s3a.*` → no warning (only Ingress checked)

## JDBC Ingress Path Not Required (ISSUE-002)

#### Executor — `aqueduct/executor/spark/ingress.py`
- ✅ JDBC Ingress with no `path` field → no `IngressError` raised; `reader.load()` called without args
- ✅ JDBC Ingress with `path` present → `path` passed to `reader.load(path)` as before
- ✅ Kafka/depot/dataframe Ingress with no `path` → no `IngressError`
- ✅ Parquet Ingress with no `path` → `IngressError` with `format='parquet'` in message
- ✅ CSV Ingress with no `path` → `IngressError` raised (path required for file formats)

## Phase 25a — Post-Egress Maintenance Hooks

#### Egress — `aqueduct/executor/spark/egress.py`
- ✅ `run_maintenance`: `optimize: true` → `OPTIMIZE delta.\`path\`` SQL executed
- ✅ `run_maintenance`: `zorder_by: [col]` → `ZORDER BY (col)` appended to OPTIMIZE SQL
- ✅ `run_maintenance`: `zorder_by` omitted → no ZORDER clause
- ✅ `run_maintenance`: `vacuum: 168` → `VACUUM delta.\`path\` RETAIN 168 HOURS` executed
- ✅ `run_maintenance`: OPTIMIZE failure → warning logged, returns `optimize_ms=None`, pipeline continues
- ✅ `run_maintenance`: VACUUM failure → warning logged, returns `vacuum_ms=None`, pipeline continues
- ✅ `run_maintenance`: both `optimize` and `vacuum` absent → no SQL executed, returns `{optimize_ms: None, vacuum_ms: None}`
- ✅ `run_maintenance`: `zorder_by` as string (not list) → treated as single column

#### Executor — `aqueduct/executor/spark/executor.py`
- ✅ Egress with `maintenance:` block → `run_maintenance` called after successful write
- ✅ Egress with no `maintenance:` block → `run_maintenance` NOT called
- ✅ Maintenance timing written to `maintenance_metrics` table in `observability.db`
- ✅ Maintenance write failure → debug log only, pipeline continues

#### Compiler — `aqueduct/compiler/compiler.py`
- ✅ Egress with `maintenance.optimize: true` + `format: parquet` → warning 8g emitted
- ✅ Egress with `maintenance.optimize: true` + `format: delta` → no warning
- ✅ Egress with `maintenance.vacuum: 168` only (no optimize) + `format: parquet` → no warning

## Phase 25b — `partition_filters` on Ingress

#### Executor — `aqueduct/executor/spark/ingress.py`
- ✅ `partition_filters` set → `.where(expr)` applied to df after `reader.load()`; returned df is filtered
- ✅ `partition_filters` absent → no `.where()` call; df unchanged
- ✅ `partition_filters` with date literal/context value (e.g. `event_date >= '2024-01-01'`) → filter applied correctly
- ✅ `partition_filters` with invalid SQL expr → `IngressError` raised with filter expression in message
- ✅ `partition_filters` applied before `schema_hint` check (filter does not affect schema metadata)
- ✅ `partition_filters` on JDBC Ingress (no path) → `.where()` applied after `reader.load()` (no path arg)

---

### `MetricsConfig.use_observe` — `aqueduct/config.py` + `aqueduct/executor/spark/metrics.py`

- ✅ `MetricsConfig` parses `use_observe: true` and `use_observe: false` without error
- ✅ `MetricsConfig` rejects extra keys (e.g. `use_observe: true` plus `unknown: 1` → `extra="forbid"` raises `ValidationError`)
- ✅ `observe_df(df, name, alias, enabled=False)` returns `(df, None)` without inserting an `Observation` node — verifiable via `df.explain()` not containing `CollectMetrics`
- ✅ `observe_df(df, name, alias, enabled=True)` returns a wrapped df with a usable `Observation` (Spark 3.3+)
- ✅ `execute(use_observe=False)` path completes a full Ingress→Channel→Egress run; resulting `module_metrics.records_written` is `NULL` (not collected) but the pipeline succeeds
- ✅ `cli.py:run` reads `cfg.metrics.use_observe` and forwards it to `execute()`; default `true` reproduces pre-audit behaviour

### `aqueduct --version` — `aqueduct/cli.py` + `aqueduct/__init__.py`

- ✅ `aqueduct --version` exits 0 and prints `aqueduct <version>` with the version sourced from `importlib.metadata.version("aqueduct-core")`
- ✅ `aqueduct.__version__` falls back to `"0.0.0+unknown"` when `importlib.metadata.version("aqueduct-core")` raises `PackageNotFoundError` (simulate with monkeypatch)
- ✅ `aqueduct --help` lists `--version` in its options block

### `DeploymentConfig` Literal validation — `aqueduct/config.py`

- ✅ `DeploymentConfig(target="local")` accepts every documented value (`local`, `standalone`, `yarn`, `kubernetes`, `databricks`, `emr`, `dataproc`)
- ✅ `DeploymentConfig(target="bogus")` raises `pydantic.ValidationError` mentioning the full list of accepted literals
- ✅ `DeploymentConfig(engine="spark")` and `DeploymentConfig(engine="flink")` both validate; `engine="duckdb"` raises `ValidationError`
- ✅ `load_config(path)` propagates the Literal validation error wrapped in `ConfigError` with the user-friendly formatter

### `[secrets]` extra + early SDK check — `pyproject.toml` + `aqueduct/config.py`

- ✅ `pyproject.toml` exposes `secrets` extra that aggregates `aws`, `gcp`, `azure`
- ✅ `pyproject.toml`: `all` extra pulls in `spark` and `secrets`
- ✅ `load_config()` with `secrets.provider=env` does not import any cloud SDK
- ✅ `load_config()` with `secrets.provider=aws` and `boto3` importable → returns `AqueductConfig` without error
- ✅ `load_config()` with `secrets.provider=aws` and `boto3` NOT importable (monkeypatch `importlib.util.find_spec` to return None) → raises `ConfigError` containing both `pip install aqueduct-core[aws]` and `pip install aqueduct-core[secrets]`
- ✅ Same as above for `provider=gcp` and `provider=azure`
- ✅ `load_config()` with `secrets.provider=custom` does not run the SDK check (no `ConfigError` even when no SDK is present)
- ✅ `aqueduct doctor:check_secrets` still produces a structured `CheckResult` for the same misconfiguration (does not break when `load_config` already raised)

### `aqueduct/templates/default/aqueduct.yml.template` — fields documented

- ✅ Template no longer contains `block_full_actions_in_prod`
- ✅ Template contains a commented `metrics:` block describing `use_observe`
- ✅ Generating a project via `aqueduct init` produces an `aqueduct.yml` that parses without error against the current `AqueductConfig` schema

### cloudpickle patch fragility (`aqueduct/doctor.py`)

- ✅ `check_cloudpickle()` on a Python version + pyspark combination where `pyspark.cloudpickle` does not exist (simulate with monkeypatch) → returns `CheckResult(status="skip")` rather than raising
- ✅ `check_cloudpickle()` on Python 3.13+ with system `cloudpickle>=3.0` installed → reports `ok` with patched version
- ✅ `check_cloudpickle()` on Python 3.13+ with no system `cloudpickle` → reports `warn` with install hint

### doctor `pyspark` import discipline

- ✅ `import aqueduct.doctor` from a fresh interpreter (no pyspark installed) does NOT raise `ImportError`. Verifies the three pyspark imports remain inside function bodies, not at module top. Regression for the documented "doctor.py is the spark-isolation exception" rule in `CLAUDE.md`.

---

## Audit Cleanup (Batches 4–6) — 2026-05-14

### `doctor --aqtest` / `doctor --aqscenario` — `aqueduct/doctor.py:check_aqtest()` / `check_aqscenario()`

- ✅ `check_aqtest(path)`: missing file → single `CheckResult(status="fail", detail contains "file not found")`
- ✅ `check_aqtest(path)`: malformed YAML → `fail` with `invalid YAML` in detail
- ✅ `check_aqtest(path)`: top-level non-mapping → `fail`
- ✅ `check_aqtest(path)`: missing or wrong `aqueduct_test` version → `fail`
- ✅ `check_aqtest(path)`: missing `blueprint:` field → `fail`
- ✅ `check_aqtest(path)`: blueprint reference does not resolve → `fail` with resolved path in message
- ✅ `check_aqtest(path)`: empty `tests:` list → single `warn` result
- ✅ `check_aqtest(path)`: test case `module` does not exist in referenced blueprint → `fail` listing available module IDs
- ✅ `check_aqtest(path)`: test case missing `assertions` → reported under "test case issues"
- ✅ `check_aqtest(path)`: all module IDs resolve + assertions present → `ok`
- ✅ `check_aqscenario(path)`: reuses `aqueduct.surveyor.scenario.load_scenario` so the same version/key checks apply
- ✅ `check_aqscenario(path)`: `inject_failure.module` not in referenced blueprint → `fail`
- ✅ `check_aqscenario(path)`: blueprint reference points at non-existent file → `fail`
- ✅ `check_aqscenario(path)`: valid scenario → `ok` with `id` and `failed_module` echoed
- ✅ `aqueduct doctor --aqtest <path>` runs only the aqtest check + the standard config / store / secrets checks
- ✅ `aqueduct doctor --aqtest <path> --aqscenario <path2>` runs both file pre-flights in one pass
- ✅ `aqueduct doctor <blueprint> --aqtest <path>` runs all per-file checks (additive flags)
- ✅ Any failed `aqtest` / `aqscenario` check sets process exit code 1

### `compile --show {manifest|provenance|inputs|all}` — `aqueduct/cli.py:_render_compile_show()`

- ✅ `--show manifest` (default) → byte-identical JSON to pre-flag behaviour
- ✅ `--show provenance` → emits the `# Context` section first, then a `# Module: <id>` section per module, each with a `key | source_type | original_expression | resolved_value` table
- ✅ `--show provenance` on a blueprint with no `context:` block → still emits per-module tables; context section omitted
- ✅ `--show inputs` → emits `module_id | path | size | last_modified` table; remote paths render `—` for size + last_modified
- ✅ `--show inputs` on a blueprint with no Ingress modules → "(no Ingress modules; inputs_fingerprint is empty)"
- ✅ `--show all` → full manifest JSON + both rendered tables, separated by `── Provenance ──` and `── Inputs fingerprint ──` headers
- ✅ `--show provenance` rendered table uses `original_expression` (not `origin_expression`) for the column header — guards against the field-rename regression
- ✅ Invalid value (e.g. `--show foo`) → click reports allowed choices and exits non-zero

### LLM spend-cap — `agent.max_heal_attempts_per_hour`

- ✅ `AgentSchema` accepts integer values and `null` for `max_heal_attempts_per_hour` (frozen at `extra="forbid"`)
- ✅ `AgentConnectionConfig` accepts integer values and `null` for `max_heal_attempts_per_hour`
- ✅ Blueprint value of `max_heal_attempts_per_hour` wins over engine value when both are set
- ✅ `Surveyor.count_recent_heal_attempts(within_minutes=60)` returns 0 when `start()` has not been called (no connection)
- ✅ `Surveyor.count_recent_heal_attempts(within_minutes=60)` counts rows whose `applied_at >= now - 60min`; rows outside the window are excluded
- ✅ `Surveyor.count_recent_heal_attempts(...)` swallows DB errors and returns 0 (defensive)
- ✅ CLI loop: with `max_heal_attempts_per_hour=2` and 2 prior healing rows in `observability.db`, the next failure emits the `⊘ LLM rate-limit reached` line and breaks the loop without calling `generate_agent_patch`
- ✅ CLI loop: with `max_heal_attempts_per_hour=None` (default) the rate-limit check is skipped entirely

### Cloudpickle hardening — `aqueduct/executor/spark/udf.py:_patch_pyspark_cloudpickle()`

- ✅ Python ≤ 3.12 → function returns immediately, no warning logged
- ✅ Python 3.13+, system `cloudpickle` not installed → `logger.warning` with `pip install cloudpickle` hint
- ✅ Python 3.13+, `pyspark.cloudpickle` import succeeds → patch applied, `logger.info` confirmation
- ✅ Python 3.13+, `pyspark.cloudpickle` raises ImportError but `pyspark.cloudpickle_fast` succeeds → patch applied, log includes `cloudpickle_fast` as the path
- ✅ Python 3.13+, none of `pyspark.cloudpickle` / `cloudpickle_fast` / `_cloudpickle` importable → `logger.warning` ("not importable under any known path") + skip
- ✅ Python 3.13+, bundled module imported but missing `dumps` / `loads` / `CloudPickler` → `logger.warning` listing the missing attrs + skip (no AttributeError)
- ✅ Python 3.13+, version-parse failure on `__version__` strings → `logger.warning` mentioning parse failure + skip
- ✅ Python 3.13+, system cloudpickle version ≤ bundled version → no patch, no warning

### `--log-format json` — `aqueduct/cli.py:_AqueductJsonLogFormatter`

- ✅ `_AqueductJsonLogFormatter.format(record)` returns a valid JSON object string with `ts` / `level` / `logger` / `msg` keys
- ✅ `ts` is ISO-8601 UTC parsed from `record.created`
- ✅ Records with `exc_info` set get an additional `exc` field containing the formatted traceback string
- ✅ Records with non-serialisable arguments fall back to `str()` via `default=str` (no `TypeError`)
- ✅ `aqueduct -v --log-format json validate <file>` emits JSON lines for every log record (no `INFO foo:` formatted lines mixed in)
- ✅ `aqueduct --log-format text` (default) produces the same output as `aqueduct` without the flag — regression guard
- ✅ Invalid value (e.g. `--log-format xml`) → click reports allowed choices and exits non-zero

---

## Phase 28 — Pluggable Store Backends

> **Note:** Backend-agnostic store interface tests have been migrated to `tests/test_stores/`.

### `aqueduct/stores/base.py`

- ✅ `RelationalCursor.execute(sql, params)` with `paramstyle="qmark"` passes SQL through unchanged
- ✅ `RelationalCursor.execute(sql, params)` with `paramstyle="format"` rewrites `?` → `%s` before calling the underlying cursor
- ✅ `RelationalCursor.executemany(...)` performs the same rewrite once per call
- ✅ `BackendUnsupportedError` raised on `RedisDepotStore.connect()`
- ✅ `get_stores(cfg)` returns a `StoreBundle` with the expected concrete adapter classes for each backend combination (duckdb/duckdb/duckdb; duckdb/duckdb/redis; postgres×3) — `tests/test_stores/test_base.py`

### `aqueduct/stores/duckdb_.py`

- ✅ `DuckDBObservabilityStore.connect()` opens / closes a real DuckDB connection per call
- ✅ `DuckDBObservabilityStore.connect()` creates parent directories when the path's parent does not yet exist — `tests/test_stores/test_observability_store.py::test_duckdb_creates_parent_dirs`
- ✅ `DuckDBDepotStore.kv_get(missing_key, default="x")` returns `"x"` without raising
- ✅ `DuckDBDepotStore.kv_put / kv_get / kv_delete` round-trip
- ✅ Equivalent behavior verified for `DuckDBLineageStore`

### `aqueduct/stores/postgres.py`

- ✅ `_get_pool` caches the pool per DSN (two `connect()` calls against the same DSN do not create two pools) — `tests/test_stores/test_postgres.py`
- ✅ `_ensure_schema(dsn, "observability")` is idempotent — calling it twice does not raise
- ✅ `PostgresObservabilityStore.connect()` sets `search_path` to `"obs"` so unqualified `SELECT … FROM run_records` resolves correctly
- ✅ `PostgresObservabilityStore.location_label` redacts password from DSN (`postgresql://user:secret@host/db` → `postgresql://user@host/db`)
- ✅ `PostgresDepotStore.kv_put / kv_get / kv_delete` round-trip against a real PG instance (integration test, marker `integration`)
- ✅ `psycopg2` missing → `ImportError` with the documented install hint at first `_get_pool()` call — `tests/test_stores/test_postgres.py`

### `aqueduct/stores/redis_.py`

- ✅ `_get_client` caches the client per URL — `tests/test_stores/test_redis.py`
- ✅ `RedisDepotStore.kv_get(missing_key, default="x")` returns `"x"`
- ✅ `RedisDepotStore.kv_put / kv_get / kv_delete` round-trip (integration test with `redis` running on localhost)
- ✅ `RedisDepotStore.location_label` strips the password component from a URL
- ✅ `redis-py` missing → `ImportError` with the install hint — `tests/test_stores/test_redis.py`

### `aqueduct/config.py`

- ✅ `RelationalStoreConfig(backend="redis", path="x")` raises `pydantic.ValidationError`
- ✅ `KVStoreConfig(backend="redis", path="redis://h/0")` validates
- ✅ `RelationalStoreConfig(backend="duckdb"|"postgres", ...)` both validate
- ✅ `KVStoreConfig(backend="duckdb"|"postgres"|"redis", ...)` all three validate
- ✅ `load_config(...)` raises `ConfigError` when `stores.observability.backend == "postgres"` and `psycopg2` is not importable (monkeypatch `importlib.util.find_spec`) — `tests/test_parser/test_config.py::test_load_config_postgres_missing_driver`
- ✅ `load_config(...)` raises `ConfigError` when `stores.depot.backend == "redis"` and `redis` is not importable — `tests/test_parser/test_config.py::test_load_config_redis_missing_driver`
- ✅ `load_config(...)` with all backends `duckdb` does not import psycopg2 or redis — `tests/test_parser/test_config.py`

### Wired call sites

- ✅ `Surveyor(stores=bundle)` honours the supplied bundle — `record_healing_outcome()` writes against `bundle.obs` — `tests/test_surveyor/test_surveyor.py::test_surveyor_uses_injected_stores`
- ✅ `Surveyor()` without `stores=` falls back to a `DuckDBObservabilityStore` at `store_dir/observability.db` — `tests/test_surveyor/test_surveyor.py::test_surveyor_default_store`
- ✅ `DepotStore(backend=Redis...)` round-trips a watermark via `kv_put`/`kv_get` [DuckDB param always runs; Redis param needs `AQ_REDIS_URL`] — `tests/test_stores/test_wired_backends.py`
- ✅ `write_lineage(..., lineage_store=postgres_store)` writes rows into the `column_lineage` table [DuckDB param always runs; Postgres param needs `AQ_PG_DSN`] — `tests/test_stores/test_wired_backends.py`
- ✅ `execute(..., observability_store=postgres_store, lineage_store=postgres_store)` end-to-end run persists `run_records`, `module_metrics`, `column_lineage`, `probe_signals` rows into Postgres [integration — skips without `AQ_PG_DSN`] — `tests/test_stores/test_wired_backends.py`
- ✅ `aqueduct signal <id>` with the Postgres backend reads/writes the `observability.signal_overrides` schema-qualified table [integration — skips without `AQ_PG_DSN`] — `tests/test_stores/test_wired_backends.py`

### `aqueduct stores` CLI

- ✅ `aqueduct stores info` prints three rows (obs / lineage / depot) with the configured backend + location label — `tests/test_cli/test_cli_stores.py`
- ✅ `aqueduct stores migrate --from-duckdb <empty.db>` reports zero rows migrated without error — `tests/test_cli/test_cli_stores.py`
- ✅ `aqueduct stores migrate --from-duckdb <populated.db>` copies all rows into the target backend, idempotent on re-run — `tests/test_cli/test_cli_stores.py`
- ✅ `aqueduct stores migrate` refuses when source and target depot resolve to the same DuckDB file — `tests/test_cli/test_cli_stores.py`
- ✅ `aqueduct stores migrate --store obs` exits non-zero with a "not yet supported" error (v1 ships depot only) — `tests/test_cli/test_cli_stores.py`

### `aqueduct doctor`

- ✅ `check_store_backend("observability", cfg, is_kv_only=False)` returns `ok` for a reachable DuckDB — `tests/test_cli/test_cli_doctor.py::TestDoctorStoreBackends`
- ✅ `check_store_backend("observability", cfg, ...)` returns `fail` with `redis` because Literal split prevents the combination at config layer; if injected programmatically the function still rejects — `tests/test_cli/test_cli_doctor.py::TestDoctorStoreBackends`
- ✅ `check_store_backend(... backend=postgres, dsn=invalid)` returns `fail` with the connection error — `tests/test_cli/test_cli_doctor.py::TestDoctorStoreBackends`
- ✅ `aqueduct doctor` output replaces the old `depot` / `observability` rows with `observability` / `lineage` / `depot` backend-aware rows — `tests/test_cli/test_cli_doctor.py::TestDoctorStoreBackends`

---

## Phase 29a — Patch Validation Pyramid

### `aqueduct/patch/preview.py`

#### `touched_module_ids(spec)`

- ✅ patch with one `set_module_config_key` → returns `[module_id]`
- ✅ patch with `replace_module_config` + `insert_module` + `add_probe` → returns each module_id once, insertion order preserved
- ✅ patch with only `replace_context_value` → returns `[]` (no module touched)
- ✅ patch with `replace_edge` → returns `[to_id]` only (consumer side)

#### `_live_lineage_rows(bp_dict)`

- ✅ Blueprint with no SQL Channels → returns `[]`
- ✅ Blueprint with one `op: sql` Channel pulling 3 columns from 1 upstream → returns 3 rows with correct `source_table` / `source_column`
- ✅ Blueprint with `SELECT *` Channel → returns wildcard rows (`output_column='*'`) — the lineage gate must tolerate this without false positives
- ✅ Multi-input Channel resolves `source_table` per column when sqlglot can disambiguate; falls back to first upstream when ambiguous

#### `run_lineage_gate(before, after, spec)`

- ✅ patch that does NOT change any Channel SQL → returns `status="pass"`, empty warnings
- ✅ patch that renames a column in a Channel query and the renamed column is consumed downstream → returns one `LineageWarning` per missing column, `status="warn"`
- ✅ patch that drops a SELECT column consumed downstream → returns a warning identifying both the consumer module and the missing column name
- ✅ patch on a module with no downstream consumers → `status="pass"` even if columns disappear
- ✅ `SELECT *` patched module → no false-positive warnings (wildcard handled)

#### `run_sandbox_gate(...)`

- ✅ patched Blueprint that parses + compiles + executes without error → `status="pass"` and `egress_targets` lists every dropped Egress
- ✅ patched Blueprint that fails to compile → `status="fail"` with the CompileError text in `detail`
- ✅ patched Blueprint where a module fails at runtime → `status="fail"` mentions the failing module_id
- ✅ `sample_rows=1000` → Ingress modules receive `sandbox_limit=1000` in their config; ingress.py wraps `.limit(1000)` post-load
- ✅ `sample_rows=0` → no `sandbox_limit` marker injected; full data flows
- ✅ Egress modules: none are executed; all are listed in `egress_targets` with `id`, `format`, `path`, `mode`
- ✅ Spark unavailable (mock `make_spark_session` to raise) → `status="skip"`, not `fail`
- ✅ Temp Blueprint file is unlinked after the call (even on exception paths)

#### `render_unified_diff(before, after)`

- ✅ identical Blueprints → empty diff string
- ✅ single-field change → diff contains exactly one `-`/`+` pair

### `aqueduct/executor/spark/ingress.py` — `sandbox_limit`

- ✅ `read_ingress(module)` with `sandbox_limit=100` in config → returned DataFrame has plan node `LocalLimit 100` or equivalent
- ✅ `read_ingress(module)` without `sandbox_limit` → returned DataFrame plan has no LIMIT node
- ✅ `sandbox_limit` applied AFTER `partition_filters` (limit narrower than filter)
- ✅ `sandbox_limit` applied BEFORE `schema_hint` validation (limit does not change schema metadata)

### `agent.patch_validation` config

- ✅ `AgentConnectionConfig(patch_validation="full_run")` validates and is the default
- ✅ `AgentConnectionConfig(patch_validation="sandbox")` validates
- ✅ `AgentConnectionConfig(patch_validation="bogus")` raises `pydantic.ValidationError`
- ✅ Blueprint-level `agent.patch_validation` overrides engine default (`manifest.agent.patch_validation or cfg.agent.patch_validation`)
- ✅ `manifest.agent.patch_validation=None` → engine default wins

### Auto/aggressive integration — `cli.py:_run_patch_gates_inline`

- ✅ auto mode + the sandbox gate pass + `patch_validation=full_run` → full Spark run is still executed
- ✅ auto mode + the sandbox gate pass + `patch_validation=sandbox` → full Spark run is SKIPPED; Blueprint written directly
- ✅ auto mode + the sandbox gate fail → patch staged for human via `on_heal_failure`, `healing_outcomes.patch_applied=false`, loop breaks
- ✅ aggressive mode + the sandbox gate fail → `continue` to next iteration with `last_apply_error` populated; no Blueprint write
- ✅ aggressive mode + the sandbox gate pass + `patch_validation=sandbox` → Blueprint written, loop breaks
- ✅ Each gate evaluation writes one row to `observability.patch_simulation` (lineage + sandbox)

### `aqueduct patch preview` CLI

- ✅ `aqueduct patch preview <patch>.json --blueprint bp.yml` (text format) — exit 0 when the lineage gate passes
- ✅ `aqueduct patch preview ... --sandbox --sample 0` — the sandbox gate runs unbounded; Egress targets printed
- ✅ `aqueduct patch preview ... --format json` emits a top-level object with `patch_id`, `diff`, `lineage gate`, and (when `--sandbox`) `sandbox gate` keys
- ✅ Patch that fails the guardrails gate guardrails exits with code 2 before the lineage gate/3 run
- ✅ the lineage gate warnings do not cause non-zero exit (status `warn` is informational)
- ✅ the sandbox gate `fail` causes exit code 2

### `observability.patch_simulation` table — `Surveyor.record_patch_simulation()`

- ✅ Insert one row → `SELECT COUNT(*) FROM patch_simulation` returns 1
- ✅ Insert preserves all fields (patch_id, gate, status, detail, sample_rows, duration_ms, run_id, blueprint_id, recorded_at)
- ✅ Method is a no-op when `Surveyor.start()` has not been called (`self._obs is None`)
- ✅ Internal exceptions inside the insert never propagate to the healing loop (e.g. patched observability store raising on connect)

---

## Phase 29b — the explain gate: explain() regression check

### `aqueduct/patch/explain_gate.py`

#### `capture_plan_snapshot(df)`

- ✅ returns dict with `exchange_count`, `python_udf_count`, `broadcast_count`, `plan_text` keys
- ✅ counts `Exchange ` substring occurrences in formatted plan text
- ✅ counts `BatchEvalPython` substring occurrences
- ✅ counts `BroadcastExchange` substring occurrences
- ✅ falls back to `df._jdf.queryExecution().toString()` when `ExplainMode.fromString` is unavailable
- ✅ returns zero counts + empty plan text when both extraction paths fail (never raises)

#### `run_explain_gate(baseline, after, touched_modules=...)`

- ✅ empty `baseline` dict → `status="skip"`, detail mentions "baseline not yet established"
- ✅ baseline + matching after with identical counts → `status="pass"`, no regressions
- ✅ `after.exchange_count > baseline.exchange_count` → ExplainRegression with metric=`"exchange"`
- ✅ `after.python_udf_count > baseline.python_udf_count` → ExplainRegression with metric=`"python_udf"`
- ✅ `after.broadcast_count < baseline.broadcast_count` → ExplainRegression with metric=`"broadcast"`
- ✅ `touched_modules=["m1"]` → only `m1` compared even if other modules are in both maps
- ✅ `touched_modules=None` → intersection of `baseline.keys()` and `after.keys()` compared
- ✅ status `warn` when at least one regression; `pass` otherwise (never `fail`)
- ✅ `baseline_run_id` populated with one of the baseline `run_id`s on compare

### `aqueduct/surveyor/surveyor.py` — Phase 29b methods

- ✅ `Surveyor.record_explain_snapshot(...)` writes one row to `observability.explain_snapshot`
- ✅ Rolling prune: after N+1 inserts for the same `(blueprint_id, module_id)`, oldest row deleted; only `keep_last_n` rows remain
- ✅ `Surveyor.latest_explain_snapshots()` returns `{module_id: {exchange_count, python_udf_count, broadcast_count, plan_text, run_id, captured_at}}` with one row per module (most recent `captured_at`)
- ✅ Method is a no-op when `_obs` is None or `blueprint_id` is None
- ✅ Internal exceptions never propagate

### `agent.block_on_explain_regression` config

- ✅ `AgentConnectionConfig(block_on_explain_regression=False)` is the default
- ✅ `AgentConnectionConfig(block_on_explain_regression=True)` validates
- ✅ Blueprint-level `agent.block_on_explain_regression` overrides engine default
- ✅ Blueprint `block_on_explain_regression=None` → engine default wins
- ✅ Parser populates `AgentConfig.block_on_explain_regression` from Pydantic schema (regression test for the Phase 29a missed-field bug — verifies parser wires the field, not just defaults)

### Executor wiring — `aqueduct/executor/spark/executor.py`

- ✅ `execute(..., explain_capture=dict)` fills the dict with per-module snapshots during a successful run
- ✅ `explain_capture` is NOT written to `observability.explain_snapshot` even when `surveyor` is also passed; both sinks happen independently
- ✅ `execute(..., surveyor=X, explain_capture=None)` writes to `observability.explain_snapshot` per successful non-Egress module
- ✅ Egress modules are NEVER captured (no DataFrame in frame_store, never iterated)
- ✅ Failure during capture for one module does NOT abort the run or skip the next module

### `aqueduct/patch/preview.py` — the sandbox gate explain wiring

- ✅ `run_sandbox_gate(..., explain_capture=d)` forwards the dict to `execute()` and fills it during sandbox replay
- ✅ When `explain_capture` is omitted, behaviour is identical to Phase 29a (no per-module snapshot collection)

### Auto/aggressive integration — `cli.py:_run_patch_gates_inline`

- ✅ Returns 4-tuple `(lineage gate, sandbox gate, explain gate, gates_passed)`; explain gate is None only when the explain gate raised internally
- ✅ the explain gate row appended to `observability.patch_simulation` with `gate="explain"` (Note: manifest said "explain gate", code uses "explain")
- ✅ Auto mode + the explain gate warn → ⚠ regressions printed; loop continues to patch_validation logic
- ✅ Aggressive mode + `block_on_explain_regression=False` + the explain gate warn → continues to the sandbox gate decision (current behaviour)
- ✅ Aggressive mode + `block_on_explain_regression=True` + the explain gate warn → patch rejected, `last_apply_error` populated, `continue` to next iteration; `healing_outcomes.patch_applied=false`
- ✅ Aggressive mode + `block_on_explain_regression=True` + the explain gate pass → proceeds to the sandbox gate / `patch_validation` decision normally

### `aqueduct patch preview` CLI

- ✅ `aqueduct patch preview <patch>.json --blueprint bp.yml --sandbox` text output includes "Explain gate" section with status + duration
- ✅ `--format json --sandbox` emits top-level `explain` key (Note: manifest said "explain gate") with `status`, `detail`, `duration_ms`, `baseline_run_id`, `regressions`
- ✅ Empty `observability.explain_snapshot` → the explain gate reports `status="skip"`, exit code stays 0
- ✅ the explain gate warn (regression) does NOT raise exit code beyond the sandbox gate status (warn-only at CLI surface)

### `observability.explain_snapshot` table

- ✅ Table created on `Surveyor.start()` via `_EXPLAIN_SNAPSHOT_DDL`
- ✅ Primary key `(blueprint_id, run_id, module_id)` — re-inserting same triplet is idempotent (INSERT OR REPLACE)
- ✅ DDL `IF NOT EXISTS` — second `start()` does not raise
- ✅ DuckDB and Postgres backends both honour the DDL (paramstyle qmark→format rewrite)

---

## Phase 30a — Extended Spark Warnings + Suppression

### `aqueduct/warnings.py` core infra

- ✅ `AqueductWarning` subclasses `UserWarning` — `tests/test_warnings.py`
- ✅ `emit(rule_id, msg)` calls `warnings.warn` with category `AqueductWarning` and prefix `[aqueduct:rule_id] msg` — `tests/test_warnings.py`
- ✅ `emit(..., suppress={rid})` is a no-op when `rid` matches `rule_id` — `tests/test_warnings.py`
- ✅ `set_default_suppress([rid])` makes subsequent `emit(rid, ...)` no-ops without explicit suppress arg — `tests/test_warnings.py`
- ✅ `set_default_suppress([], silence_all=True)` silences every emit including non-listed IDs — `tests/test_warnings.py`
- ✅ Explicit `emit(..., suppress=...)` arg takes priority over default — `tests/test_warnings.py`
- ✅ `emit()` never raises — internal exceptions swallowed — `tests/test_warnings.py`
- ✅ `install_cli_formatter()` is idempotent; second call is a no-op — `tests/test_warnings.py`
- ✅ `install_cli_formatter()` renders `AqueductWarning` as `AQ-WARN [rule_id] msg\n`; non-Aqueduct warnings keep default formatting — `tests/test_warnings.py`

### `aqueduct/compiler/warnings/` tier 1 rules

#### `kafka_checkpoint_stale.py`
- ✅ `RULE_ID == "kafka_checkpoint_stale"`
- ✅ Channel with `checkpoint=True` + Kafka Ingress upstream → one warning
- ✅ Channel with `checkpoint=True` + Parquet Ingress upstream → no warning
- ✅ Channel with `checkpoint=False` + Kafka Ingress upstream → no warning

#### `nondeterministic_fanout.py`
- ✅ `RULE_ID == "nondeterministic_fanout"`
- ✅ 2-consumer Channel with `rand()` in SQL → one warning
- ✅ Single-consumer Channel with `rand()` → no warning
- ✅ Multi-consumer Channel without nondeterministic fn → no warning
- ✅ Multi-consumer Channel with `rand()` + `checkpoint=True` → no warning
- ✅ Detects `uuid()`, `current_timestamp()`, `now()`, `random()`, case-insensitive

#### `count_col_likely_count_star.py`
- ✅ `RULE_ID == "count_col_likely_count_star"`
- ✅ `COUNT(user_id)` → one warning
- ✅ `COUNT(*)` → no warning
- ✅ `COUNT(DISTINCT col)` → no warning
- ✅ Multiple `COUNT(col)` in one query → one warning per match

#### `file_format_no_repartition.py`
- ✅ `RULE_ID == "file_format_no_repartition"`
- ✅ Parquet Egress without partition hints → warning
- ✅ Parquet Egress with `partition_by` → no warning
- ✅ Parquet Egress with `repartition` → no warning
- ✅ Parquet Egress with `coalesce` → no warning
- ✅ Delta Egress without partition hints → no warning (transaction log handles)
- ✅ JSON, CSV trigger the rule same as parquet

#### `jdbc_missing_partition.py`
- ✅ `RULE_ID == "jdbc_missing_partition"`
- ✅ JDBC Ingress without the 4 options → warning
- ✅ JDBC Ingress with `partitionColumn` + `lowerBound` + `upperBound` → no warning
- ✅ JDBC Ingress with `predicates` → no warning
- ✅ JDBC Egress (write) is NOT flagged (rule is Ingress-only)

#### Registry — `aqueduct/compiler/warnings/__init__.py`
- ✅ `RULES` contains all five `(rule_id, check)` tuples
- ✅ `run_all(manifest)` returns `[(rule_id, msg), ...]` for every rule that fires
- ✅ `run_all(manifest, suppress={"kafka_checkpoint_stale"})` skips that rule entirely (never invokes the check fn)
- ✅ A check that raises is silently skipped (other rules still run)

### `aqueduct/executor/spark/warnings/` tier 2 rules

#### `jar_availability.py`
- ✅ `RULE_ID == "jar_availability"`
- ✅ Blueprint with `format: kafka` + no `spark-sql-kafka` JAR loaded → warning
- ✅ Blueprint with `format: kafka` + `spark-sql-kafka-0-10` JAR loaded → no warning (substring match)
- ✅ `format: delta` checks `delta-core` / `delta-spark` fragments
- ✅ `format: iceberg` checks `iceberg-spark`
- ✅ JDBC modules with `options.driver` set but no JDBC-ish JAR loaded → warning naming the driver classes
- ✅ Core `format: jdbc` without an explicit driver class → no JAR warning (core Spark handles)

### Compiler integration — `aqueduct/compiler/compiler.py`
- ✅ `compile(bp, warnings_suppress={"kafka_checkpoint_stale"})` does NOT emit that rule
- ✅ `compile(bp, warnings_silence_all=True)` emits zero Aqueduct warnings (tier 1 + legacy 8a–8g)
- ✅ Legacy rules 8a–8g now use stable rule IDs (`perf_probe_sample_full_scan`, `perf_incremental_watermark_scan`, `perf_python_udf_row_at_a_time`, `perf_delta_append_no_partition`, `perf_multi_consumer_no_cache`, `perf_hadoop_fs_in_options`, `maintenance_optimize_non_delta`, `delivery_append_retry_dupes`)
- ✅ Each legacy ID is suppressible via the same `warnings.suppress` list

### Executor integration — `aqueduct/executor/spark/executor.py`
- ✅ `execute(manifest, spark)` calls `run_all(manifest, spark)` after `getOrCreate()` and emits findings
- ✅ `execute(..., warnings_suppress={"jar_availability"})` skips the rule
- ✅ `execute(..., warnings_silence_all=True)` emits zero Aqueduct warnings

### CLI integration — `aqueduct/cli.py`
- ✅ `aqueduct --suppress-warning kafka_checkpoint_stale compile bp.yml` skips that rule
- ✅ `aqueduct --suppress-warning a --suppress-warning b ...` (repeatable) merges both into suppress set
- ✅ `aqueduct --no-warnings compile bp.yml` silences all Aqueduct warnings
- ✅ `warnings.suppress` in `aqueduct.yml` is merged with CLI flags on top
- ✅ `warnings.silence_all: true` in `aqueduct.yml` mirrors `--no-warnings`
- ✅ `_compile_with_warnings()` renders `AqueductWarning` as `AQ-WARN [id] msg`; falls back to `WARNING:` for other UserWarnings

### `WarningsConfig` Pydantic model — `aqueduct/config.py`
- ✅ `WarningsConfig().suppress == []` and `silence_all == False`
- ✅ `WarningsConfig(suppress=["foo"])` validates
- ✅ `WarningsConfig(silence_all=True)` validates
- ✅ Extra unknown keys → `pydantic.ValidationError` (`extra="forbid"`)
- ✅ `AqueductConfig().warnings` exists with default `WarningsConfig`

---

## Phase 30b — Stability Contract for v1.0

### `aqueduct/exit_codes.py`
- ✅ Constants `SUCCESS=0`, `CONFIG_ERROR=1`, `DATA_OR_RUNTIME=2`, `HEAL_PENDING=3`, `VALIDATION_GATE=4`, `USAGE_ERROR=5` exposed at module level — tests/test_cli/test_exit_codes.py::test_exit_codes_exposed
- ✅ `__all__` includes all six names — tests/test_cli/test_exit_codes.py::test_exit_codes_in_all
- ✅ Importable from top-level:  — tests/test_cli/test_exit_codes.py::test_exit_codes_exposed`from aqueduct import exit_codes`

### `aqueduct schema` command
- ✅ `aqueduct schema --target blueprint` emits JSON Schema with `$defs` and `properties`
- ✅ `aqueduct schema --target config` emits AqueductConfig schema
- ✅ `aqueduct schema --target patch` emits PatchSpec schema
- ✅ `aqueduct schema --target bogus` rejected by Click before execution
- ✅ `-o file.json` writes the schema to disk, prints confirmation to stderr; stdout silent
- ✅ Default `-o -` writes JSON to stdout
- ✅ Output parses as valid JSON (`json.loads` round-trip)
- ✅ Generation failure exits with code 2

### `aqueduct runs --format json`
- ✅ Returns list of `{run_id, blueprint_id, status, started_at, finished_at, first_failed_module}` objects
- ✅ Empty store → returns `[]` (not `"No runs found."`), exit 0
- ✅ `--failed --format json` filters to status=error rows only
- ✅ `--blueprint <id> --format json` filters to that blueprint
- ✅ Output parses as valid JSON

### `aqueduct patch list --format json`
- ✅ Returns list of `{status, file, patch_id, rationale, confidence, category}` objects
- ✅ Empty patches dir → returns `[]`, exit 0
- ✅ `--status all --format json` includes pending + applied + rejected with `status` field set
- ✅ Output parses as valid JSON
- ✅ Each entry includes `status` matching the lifecycle dir

### Public API surface — `aqueduct/__init__.py`
- ✅ `from aqueduct import parse, ParseError, AqueductWarning, __version__` succeeds
- ✅ `__all__` contains exactly: `__version__`, `parse`, `ParseError`, `AqueductWarning`
- ✅ `from aqueduct import exit_codes` succeeds
- ✅ Subpackage internals (`aqueduct.compiler`, `aqueduct.executor.spark`, etc.) still importable but not in `__all__`

### README — Versioning & Stability section
- ✅ Section "Versioning & Stability" present between Development and License
- ✅ Exit code table includes all six codes with constant names + meanings
- ✅ Deprecation policy paragraph present
- ✅ Stable surface list mentions `parse`, `ParseError`, `AqueductWarning`, `exit_codes`

---

## Pre-release CLI cleanup — env resolution + validate/doctor unification

### `_resolve_and_load_env(explicit, anchor, cli_env)` — Phase 30 (SUPERSEDES the pre-Phase-30 cwd/`disabled` design below)
- ✅ Precedence: `-e KEY=VAL` > real `os.environ` > `<anchor dir>/.env` > `--env-file` > unset
- ✅ `<anchor dir>/.env` discovered (config/blueprint directory); first existing of [anchor/.env, --env-file] wins, no stacking
- ✅ **cwd/.env is NEVER searched** (footgun removed) — a `.env` only in cwd, with anchor elsewhere, is ignored
- ✅ `anchor=None` + no `--env-file` → no file loaded (no cwd fallback)
- ✅ `AQ_NO_ENV_FILE=1` → `.env` discovery skipped; `-e` overrides STILL applied; emits `(env: .env discovery disabled — AQ_NO_ENV_FILE)`
- ✅ `-e KEY=VAL` written to `os.environ` (overwrites real env + later `.env`); `_apply_cli_env` returns count
- ✅ `-e` malformed (`no-equals`, empty key) → `click.BadParameter`
- ✅ Existing env vars never overwritten by the `.env` file (delegates to `_load_env_file`)
- ✅ Stderr notice ALWAYS emitted when something loaded: `(env: loaded N var(s) from <path>)`, `; N from -e` suffix when `-e` used, `(env: no .env file found; N from -e)` when only `-e`
- ✅ `@_env_options` decorator present on ALL 13 config commands (run/doctor/validate/test/report/runs/lineage/signal/heal/benchmark/patch preview/stores info/stores migrate); `--no-env-file` flag absent everywhere
- ✅ `stores info` / `stores migrate` resolve `${VAR}` from anchored `.env` (regression: previously required manual `source .env`)

### `_sniff_file_kind(path)`
- ✅ `aqueduct: "1.0"` header → `"blueprint"`
- ✅ `aqueduct_config: "1.0"` → `"config"`
- ✅ `aqueduct_test: "1.0"` → `"aqtest"`
- ✅ `aqueduct_scenario: "1.0"` → `"aqscenario"`
- ✅ no recognised header → `None`
- ✅ unreadable file → `None` (never raises)
- ✅ `aqueduct_config` matched before `aqueduct` (longest-prefix; no false blueprint match)

### `aqueduct validate <file>...`
- ✅ Blueprint file → `✓ [blueprint: id  N modules, M edges]`, exit 0
- ✅ Config file → full engine summary (engine/target/stores/secrets/webhooks), exit 0
- ✅ Invalid config (`stores.obs`) → `✗`, exit 1
- ✅ Multiple files → each validated independently; exit 1 if ANY invalid
- ✅ No argument + `aqueduct.yml` in CWD → validates it, prints `(no file given → …)`
- ✅ No argument + no `aqueduct.yml` → `✗ no file given`, exit 1
- ✅ Unknown header → falls back to blueprint parse (parser emits precise error)
- ✅ `.aqtest` / `.aqscenario` file → informational redirect to `doctor`, not a hard fail
- ✅ `check-config` command no longer registered (removed)

### `aqueduct doctor [TARGET]`
- ✅ No TARGET + `aqueduct.yml` in CWD → uses it, prints `(no file given → checking aqueduct.yml)`
- ✅ TARGET with `aqueduct_config:` header → config probe path
- ✅ TARGET with `aqueduct:` header → blueprint source probe path
- ✅ TARGET with `aqueduct_test:` → routed to aqtest pre-flight
- ✅ TARGET with `aqueduct_scenario:` → routed to aqscenario pre-flight
- ✅ TARGET with no recognised header → `✗ unrecognised Aqueduct file`, exit 1
- ✅ `--config` / `--blueprint` flags removed (positional only)
- ✅ `.env` anchored to resolved input file's directory
- ⏳ Default view omits `skip` rows; emits one `· skipped: <names>  (not applicable / not configured — --verbose for detail)` line
- ⏳ `--verbose` → all rows shown (incl. `-` skip rows), no collapse line
- ⏳ All rows `ok` + some `skip` → still `✓ all checks passed` (skip never fails)
- ⏳ `agent` warn detail mentions configured provider + openai_compat alternative + "pipeline runs fine without it"
- ⏳ No `skip` rows → no `· skipped:` line printed
- ⏳ `explain_gate._formatted_plan` uses `df.sparkSession` (no `sql_ctx` access) — no pyspark UserWarning emitted
- ⏳ Default (no `--preflight`): Spark check = TCP reachability, no SparkSession built; `local*` master → ok "local mode"; unreachable remote → fail in ~3s with honest msg ("Not a timeout"), NOT 45s
- ⏳ `_host_port` parses `spark://h:p`, `http://h:p`, `h:p`; bad → None. `_tcp_ok` False on refused/unroutable within timeout
- ⏳ Default: S3A endpoint TCP-probed when `spark.hadoop.fs.s3a.endpoint` set; else falls back to `check_storage(spark_ok=False)`
- ⏳ `--preflight`: builds real session w/ spark_config, runs task, version + storage; unbounded (no timeout); failure → `preflight session failed: …`
- ⏳ `SPARK_PROBE_TIMEOUT` / `ThreadPoolExecutor` removed from doctor (no import, no ref)
- ⏳ `--skip-spark` still short-circuits before any probe
- ⏳ `CheckResult` has `group` + `quiet_when_ok` fields (defaults `"general"`, `False`)
- ⏳ Default render hides `status==skip` AND (`status==ok` and `quiet_when_ok`); `--verbose` shows all
- ⏳ Hidden rows collapse to one `· more <names>  (ok / not applicable / not configured — --verbose)` line, left-aligned to same column as shown rows (name `ljust(col_w)`, col_w ≥ len("more"))
- ⏳ cloudpickle ok → `quiet_when_ok=True` (hidden default, shown on warn/fail or --verbose)
- ⏳ `check_agent`: provider=anthropic + no key + no base_url → `skip` "self-healing not configured (opt-in)"; + base_url set → `warn`; + key present → `ok`
- ⏳ `check_storage(spark_config, spark_ok=False, skipped=True)` → `skip` "configured (…); not probed (--skip-spark)" (NOT warn "Spark check failed")
- ⏳ `cluster-stores` with relative DuckDB paths in `env: cluster` → `warn` (not `fail`); suite still `✓ all checks passed`; message states runnable + shared-FS/postgres fix + safe-to-ignore caveat
- ⏳ Additive: `doctor aqueduct.yml --aqscenario X.aqscenario.yml` runs config probe AND scenario pre-flight in one invocation (positional + flag, different kinds)
