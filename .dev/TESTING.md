# Aqueduct Test Manifest

## How to use this file
- ✅ = test implemented and passing
- ⏳ = test needed but not yet written
- ❌ = test failing (see failure report)

When adding a new feature, add a task under the relevant module with the exact function/class to test and expected behavior.

## Environment variables (tests/conftest.py)

| Variable | Default | Purpose |
|---|---|---|
| `AQ_SPARK_MASTER` | `local[1]` | Spark master URL used by the `spark` session fixture. Set to `spark://host:7077` or `yarn` to run tests against a remote cluster. |
| `AQ_OLLAMA_URL` | `http://localhost:11434` | Ollama base URL. LLM integration tests (`test_llm_integration.py`) skip automatically when unreachable. |
| `AQ_OLLAMA_MODEL` | `gemma3:12b` | Model name sent to Ollama in integration tests. |

Spark artifacts are isolated to `/tmp/`:
- warehouse → `/tmp/aqueduct_test_spark_warehouse`
- metastore → in-memory Derby (`jdbc:derby:memory:aqueduct_test_metastore`)
- Derby log → `/tmp/aqueduct_test_derby.log`

---

## Parser (`aqueduct/parser/`)

### `graph.py`
- ✅ `detect_cycles`: self‑loop raises ParseError
- ✅ `detect_cycles`: 3‑node cycle raises ParseError
- ✅ `detect_cycles`: disconnected graph (no cycles) passes

### `resolver.py`
- ✅ missing env var without default raises ParseError
- ✅ nested `${ctx.foo.bar}` resolved correctly

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

---

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
- ✅ `schema_snapshot`: JSON file written to `store_dir/signals/<run_id>/<probe_id>_schema.json`
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
- ⏳ `value_distribution`: payload has `stats` dict; each column has `min`, `max`, `mean`, `stddev`, `count_non_null`, `percentiles` keys
- ⏳ `value_distribution` with no `columns` → only numeric columns included automatically
- ⏳ `value_distribution` `block_full_actions=True` → `{"blocked": True, "stats": {}}`; warning logged
- ⏳ `distinct_count`: payload has `distinct_counts` dict keyed by columns with integer values
- ⏳ `distinct_count` with no `columns` → all DataFrame columns
- ⏳ `distinct_count` `block_full_actions=True` → `{"blocked": True, "distinct_counts": {col: None}}`
- ⏳ `data_freshness`: payload has `column`, `max_value` keys
- ⏳ `data_freshness` missing `column` → signal fails, other signals captured normally
- ⏳ `data_freshness` `block_full_actions=True` + `allow_sample=false` (default) → `{"blocked": True, "column": ...}`
- ⏳ `data_freshness` `block_full_actions=True` + `allow_sample=true` → executes on sample; `sampled=True` in payload
- ⏳ `partition_stats`: payload has `num_partitions` key; integer ≥ 1; zero Spark action
- ⏳ `partition_stats` `block_full_actions=True` → still executes (not a Spark action)

### Executor integration (`executor.py`)
- ✅ Probe appended after non-Probe modules in execution order (runs last)
- ✅ Probe with `attach_to` pointing to completed Ingress: signals written to DB
- ✅ Probe with missing `attach_to` source (source failed): logged warning, result `status="success"`
- ✅ Probe failure does not change blueprint `ExecutionResult(status="success")`
- ✅ `execute()` with `store_dir=None`: Probe result is `status="success"` but no DB written
- ✅ Ingress → Probe (schema_snapshot) → Egress blueprint returns `ExecutionResult(status="success")`

### SparkListener / `module_metrics`
- ✅ `AqueductMetricsListener.set_active_module()` resets accumulated metrics
- ✅ `AqueductMetricsListener.collect_metrics()` returns accumulated dict and resets state
- ✅ `AqueductMetricsListener.collect_metrics()` with no active module returns all-zero dict
- ✅ `_write_stage_metrics()` creates `module_metrics` table if absent and inserts one row
- ✅ `_write_stage_metrics()` with `store_dir=None` is a no-op
- ✅ Egress succeeds → `module_metrics` row exists in `signals.db` with `module_id` matching Egress
- ✅ Egress failure → no `module_metrics` row written (listener reset on exception)
- ✅ `row_count_estimate` method=spark_listener: when `module_metrics` row exists, `estimate` equals `records_written` value

### Assert module
- ✅ `schema_match` passes: zero Spark action triggered
- ✅ `schema_match` fails (missing column) with `on_fail=abort`: `AssertError` raised
- ✅ `schema_match` fails (wrong type) with `on_fail=abort`: `AssertError` raised
- ✅ `min_rows` passes: single batched `df.agg()` used (at most 1 Spark action for all aggregate rules)
- ✅ `min_rows` fails with `on_fail=abort`: `AssertError` raised
- ✅ `max_rows` fails with `on_fail=warn`: warning logged, blueprint continues
- ✅ `null_rate` passes: shared `df.sample().agg()` used
- ✅ `null_rate` fails with `on_fail=abort`: `AssertError` raised
- ✅ `null_rate` on aggregate rule with `on_fail=quarantine`: treated as warn (quarantine is row-level only)
- ✅ `freshness` passes: `max(col)` batched into shared `df.agg()`
- ✅ `freshness` fails with `on_fail=warn`: warning logged, blueprint continues
- ✅ `freshness` column has all nulls: fail message includes "no non-null values"
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
- ✅ no spillway edge + quarantine rows produced → warning logged, rows discarded
- ✅ Assert with no rules configured → pass-through, `status="success"`
- ✅ end-to-end: Ingress → Assert(`min_rows` abort rule fires) → `ExecutionResult(status="error")`
- ✅ end-to-end: Ingress → Assert(`sql_row` quarantine) → Egress(good) + Egress(quarantine), both written

### Surveyor `get_probe_signal()`
- ✅ returns empty list when `signals.db` does not exist
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
- ⏳ `on_success` webhook fires after successful run (mock HTTP server)
- ⏳ `on_success` webhook NOT fired when run fails
- ⏳ `on_success: null` (default) — no webhook call made on success
- ⏳ `on_success` simple string URL form accepted by `WebhooksConfig`
- ⏳ `on_success` template vars: `${run_id}`, `${blueprint_id}`, `${blueprint_name}`, `${module_count}` resolved in payload
- ⏳ `on_failure_webhook` on module fires when retry exhausts (mock HTTP server)
- ⏳ `on_failure_webhook` fires even when `on_exhaustion=alert_only` (blueprint continues)
- ⏳ `on_failure_webhook` fires even when `on_exhaustion=abort` (blueprint fails)
- ⏳ `on_failure_webhook` simple string URL form accepted by schema
- ⏳ `on_failure_webhook` full dict form (url, method, payload, headers) accepted by schema
- ⏳ `on_failure_webhook` template vars: `${module_id}`, `${error_message}`, `${error_type}`, `${run_id}`, `${blueprint_id}` resolved
- ⏳ `on_failure_webhook=None` (default) — no per-module webhook call made

### `surveyor.py` — `Surveyor`
- ✅ `start()` creates `.aqueduct/runs.db` and tables if not existing
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
- ✅ unknown nested key in `deployment` → `ConfigError`

### `AqueductConfig` defaults
- ✅ `deployment.target` defaults to `"local"`
- ✅ `deployment.master_url` defaults to `"local[*]"`
- ✅ `stores.observability.path` defaults to `".aqueduct/signals"`
- ✅ `stores.lineage.path` defaults to `".aqueduct/lineage"`
- ✅ `stores.depot.path` defaults to `".aqueduct/depot.duckdb"`
- ✅ `agent.default_model` defaults to `"claude-sonnet-4-20250514"`
- ✅ `probes.max_sample_rows` defaults to `100`
- ✅ `secrets.provider` defaults to `"env"`
- ✅ `webhooks.on_failure` defaults to `None`
- ⏳ `webhooks.on_success` defaults to `None`
- ⏳ `webhooks.on_success` string URL coerced to `WebhookEndpointConfig`
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
- ✅ returns `True` when `signals.db` does not exist
- ✅ returns `True` when no rows found for probe_id / run_id
- ✅ returns `True` when latest signal payload has no `passed` key
- ✅ returns `True` when latest signal `passed=None`
- ✅ returns `False` when latest signal `passed=False`
- ✅ returns `True` when latest signal `passed=True`
- ✅ uses newest row (ORDER BY captured_at DESC); older `passed=False` ignored if newest `passed=True`
- ✅ returns `True` on any DuckDB exception (open-gate-on-error policy)

### Executor integration (`executor.py`)
- ✅ Regulator with open gate (no surveyor): transparent pass-through, `status="success"`
- ✅ Regulator with open gate (surveyor returns True): downstream receives DataFrame
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
- ✅ `test_probe_does_not_halt_blueprint`: Probe runs; signals.db written; blueprint succeeds
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

### LLM Self-Healing (`aqueduct/surveyor/llm.py`)

**`trigger_llm_patch(failure_ctx, model, api_endpoint, max_tokens, approval_mode, blueprint_path, patches_dir)`:** calls Anthropic API, validates PatchSpec, dispatches to `_auto_apply` or `_stage_for_human`.
**`_stage_for_human(patch_spec, patches_dir, failure_ctx)`:** writes to `patches/pending/<patch_id>.json` with `_aq_meta` annotation.
**`_auto_apply(patch_spec, blueprint_path, patches_dir, failure_ctx)`:** applies patch to Blueprint YAML on disk atomically; archives to `patches/applied/`; returns None on parse failure.

- ✅ `_stage_for_human`: creates `patches/pending/<patch_id>.json` with correct fields
- ✅ `_stage_for_human`: written JSON contains `_aq_meta.run_id` and `_aq_meta.blueprint_id`
- ✅ `_auto_apply`: applies valid patch → Blueprint file on disk is modified
- ✅ `_auto_apply`: patch produces invalid Blueprint → Blueprint unchanged, returns None
- ✅ `_auto_apply`: archives PatchSpec to `patches/applied/` with `applied_at` and `auto_applied=True`
- ✅ `trigger_llm_patch`: `ANTHROPIC_API_KEY` not set → returns None (RuntimeError caught internally)
- ✅ `trigger_llm_patch`: LLM returns markdown-fenced JSON → fences stripped, parsed correctly
- ✅ `trigger_llm_patch`: LLM returns invalid PatchSpec → reprompt up to MAX_REPROMPTS times; returns None after exhaustion
- ✅ Surveyor `record()`: on failure with `approval_mode=auto`, `trigger_llm_patch` is called (mock LLM)
- ✅ Surveyor `record()`: on success, LLM loop NOT triggered

### Arcade `required_context` validation (`aqueduct/compiler/expander.py`)

**Behavior:** After loading sub-Blueprint, checks that every key in `sub_bp.required_context` is present in `arcade_module.context_override`. Missing keys → `ExpandError`.

- ✅ Arcade with `required_context: [foo]` and `context_override: {foo: bar}` → expands successfully
- ✅ Arcade with `required_context: [foo]` and no `context_override` → `ExpandError` containing `foo`
- ✅ Arcade with `required_context: [foo, bar]`, `context_override: {foo: x}` (missing bar) → `ExpandError` containing `bar`
- ✅ Arcade with empty `required_context` → always expands regardless of `context_override`
- ✅ Blueprint with `required_context: [env]` correctly round-trips through Parser (field preserved in AST)

---

## Failure Report (last run)
<!-- Auto‑populated by the cheap model after test run -->
- **Status**: 391 passed, 4 skipped, 1 xfailed. Coverage: 86.63%.
Issues reported in:
- None
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
- ⏳ executor: skipped upstream + included downstream → frame_store miss produces natural `ExecutionResult(status="error")` with clear message
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
- ⏳ CLI `--execution-date 2026-01-15` parses to `date(2026,1,15)` and passed to compiler
- ⏳ CLI `--execution-date` invalid format → click error with clear message

### LLM Guardrails — `aqueduct/cli.py` + `aqueduct/parser/`

**`_check_guardrails(patch, agent)`:** returns error string on violation, else None.
**`AgentConfig.allowed_paths`:** tuple of fnmatch patterns; empty = unrestricted.
**`AgentConfig.forbidden_ops`:** tuple of op names; empty = all permitted.

- ✅ `allowed_paths=[]` → no path violations regardless of patch content
- ✅ `forbidden_ops=[]` → no op violations regardless of patch content
- ✅ patch op in `forbidden_ops` → returns error message containing op name
- ✅ patch config.path matching an `allowed_paths` pattern → no violation
- ✅ patch config.path NOT matching any `allowed_paths` pattern → returns error message containing path
- ✅ patch with no `config.path` (e.g. `replace_module_label`) → no path violation even if `allowed_paths` set
- ⏳ guardrail violation → patch staged in `patches/pending/`, not applied; blueprint ends with status="error"
- ✅ `AgentConfig.allowed_paths` round-trips through schema → parser → model (empty default)
- ✅ `AgentConfig.forbidden_ops` round-trips through schema → parser → model (empty default)
- ✅ `allowed_paths` + `forbidden_ops` in Blueprint YAML parsed correctly to `AgentConfig`

### Patch Rollback — `aqueduct patch rollback` — `aqueduct/cli.py`

**Behavior:** locates backup in `patches/backups/<patch_id>_*`; restores Blueprint atomically via tmp+rename; moves applied record to `patches/rolled_back/` with `rolled_back_at`.

- ✅ `patch rollback <id>`: backup exists → Blueprint restored to backup content
- ✅ `patch rollback <id>`: restore is atomic (tmp file + os.replace, not direct overwrite)
- ✅ `patch rollback <id>`: applied record moved from `patches/applied/` to `patches/rolled_back/`
- ✅ `patch rollback <id>`: rolled-back file contains `rolled_back_at` ISO timestamp
- ✅ `patch rollback <id>`: no backup found → error message; Blueprint unchanged
- ⏳ `patch rollback <id>`: multiple backups for same patch_id → most recent used (sort by filename ts)
- ✅ `patch rollback <id>`: no applied record (patch was staged-only) → rollback still restores Blueprint if backup exists

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
- ⏳ end-to-end: Ingress × 2 → Channel(op: join) → Egress — joined rows correct (Spark test)

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
- ⏳ full compile: macros in Blueprint → expanded in Manifest query string (no `{{` in Manifest)
- ⏳ end-to-end: Ingress → Channel(macro in query) → Egress runs correctly

### Phase 11 — Missing CLI Commands

#### `aqueduct report` — `aqueduct/cli.py`

- ⏳ valid run_id → table output with module rows and status icons
- ⏳ valid run_id + `--format json` → JSON with run_id, blueprint_id, status, module_results
- ⏳ valid run_id + `--format csv` → CSV with header row
- ⏳ unknown run_id → exit code 1 with error message
- ⏳ missing runs.db → exit code 1 with error message

#### `aqueduct lineage` — `aqueduct/cli.py`

- ⏳ valid blueprint_id → table of channel_id, output_column, source_table, source_column
- ⏳ `--from <table>` filters to only that source_table
- ⏳ `--column <col>` filters to only that output_column
- ⏳ `--format json` → JSON array
- ⏳ no rows → "No lineage records found" message, exit 0
- ⏳ missing lineage.db → exit code 1 with error message

#### `aqueduct signal` — `aqueduct/cli.py` + `surveyor.py`

- ⏳ `--value false` → row inserted in `signal_overrides` with `passed=False`
- ⏳ `--value true` → row deleted from `signal_overrides`
- ⏳ `--error "msg"` alone → row inserted with `passed=False` and `error_message` set
- ⏳ `--error "msg" --value true` → exit code 1 (conflicting flags)
- ⏳ no flags → prints current override status
- ⏳ no override set → "no persistent override" message
- ⏳ `evaluate_regulator()` checks `signal_overrides` BEFORE `probe_signals`
- ⏳ override with `passed=False` → `evaluate_regulator()` returns False even if probe_signals says True
- ⏳ `--value true` clears override → `evaluate_regulator()` resumes reading probe_signals

#### `aqueduct heal` — `aqueduct/cli.py`

- ⏳ run_id with failure_context → FailureContext reconstructed, generate_llm_patch called
- ⏳ `--module` overrides `failed_module` field in FailureContext passed to LLM
- ⏳ run_id with no failure_context → exit code 1 with clear message
- ⏳ missing runs.db → exit code 1
- ⏳ no agent model configured in aqueduct.yml → exit code 1 with clear message
- ⏳ LLM returns valid patch → patch staged in patches/pending/

### Phase 13 — `aqueduct test` Command

#### Test runner core — `aqueduct/executor/spark/test_runner.py`

- ⏳ inline rows + schema → `createDataFrame` succeeds for all supported types (long, string, double, boolean, timestamp)
- ⏳ unknown schema type → passes through to Spark DDL (Spark raises if truly invalid)
- ⏳ `row_count` assertion passes: exact count match
- ⏳ `row_count` assertion fails: non-zero exit, message shows expected vs actual
- ⏳ `contains` assertion passes: all expected rows found in output
- ⏳ `contains` assertion fails: missing rows listed in message
- ⏳ `sql` assertion passes: expr over `__output__` returns truthy
- ⏳ `sql` assertion fails: expr returns falsy
- ⏳ `sql` assertion error: bad SQL → `passed=False` with error message
- ⏳ Channel module executed against inline inputs → correct output rows
- ⏳ Assert module: passing rows returned, quarantine rows discarded (no spillway edge in test)
- ⏳ Ingress/Egress module → `TestError` with clear message
- ⏳ missing `module` field → `TestCaseResult` with error
- ⏳ module not found in blueprint → `TestCaseResult` with error
- ⏳ missing `inputs` → `TestCaseResult` with error
- ⏳ missing blueprint → `TestError`
- ⏳ Junction module: first branch used when no `branch:` specified
- ⏳ Junction module: `branch: <name>` targets specific branch

#### `aqueduct test` CLI command — `aqueduct/cli.py`

- ⏳ all tests pass → exit code 0, "all N test(s) passed"
- ⏳ any test fails → exit code 1, failure listed in output
- ⏳ test file error (bad blueprint path) → exit code 1
- ⏳ `--quiet` suppresses Spark progress (quiet=True passed to make_spark_session)
- ⏳ `--blueprint` overrides blueprint path from test file

### Phase 14 — Patch Dry-Run (`validate_patch`)

#### Schema + Model — `aqueduct/parser/schema.py`, `parser/models.py`, `parser/parser.py`, `compiler/models.py`

- ✅ `validate_patch` defaults to `False` in `AgentConfig`
- ✅ `validate_patch: true` in Blueprint YAML → `AgentConfig.validate_patch = True` after parse
- ✅ `manifest.to_dict()["agent"]["validate_patch"]` reflects the value

#### CLI dispatch — `aqueduct/cli.py` (aggressive mode)

- ⏳ `approval_mode: aggressive` + `validate_patch: true` + patch produces invalid Blueprint → patch staged in `patches/pending/`, Blueprint unchanged
- ⏳ `approval_mode: aggressive` + `validate_patch: true` + patch valid → patch written to disk, loop continues
- ⏳ `approval_mode: aggressive` + `validate_patch: false` (default) → patch written immediately (existing behavior unchanged)

---

## Stubs 1-4 — on_exhaustion / trigger_agent / block_full_actions

### `ExecutionResult.trigger_agent` — `aqueduct/executor/models.py`

- ⏳ `ExecutionResult` has `trigger_agent: bool = False` field
- ⏳ `ExecutionResult.to_dict()` includes `trigger_agent` key
- ⏳ `trigger_agent=True` frozen dataclass — mutation raises `FrozenInstanceError`

### `_on_retry_exhausted()` + `_fail()` — `aqueduct/executor/spark/executor.py`

**Behavior:** `_fail()` accepts `trigger_agent` kwarg; `_on_retry_exhausted()` maps `on_exhaustion` → (gate_closed, fail_result).

- ⏳ `on_exhaustion: abort` → `_on_retry_exhausted` returns `(False, fail_result)` with `trigger_agent=False`
- ⏳ `on_exhaustion: alert_only` → returns `(True, None)` — warning logged, gate_closed sentinel set
- ⏳ `on_exhaustion: trigger_agent` → returns `(False, fail_result)` with `trigger_agent=True`
- ⏳ Ingress `on_exhaustion: alert_only` exhausted → `frame_store[module.id] = _GATE_CLOSED`, downstream skipped, blueprint continues
- ⏳ Channel `on_exhaustion: alert_only` exhausted → same sentinel behavior
- ⏳ Egress `on_exhaustion: alert_only` exhausted → `continue` (no sentinel needed — Egress is terminal)
- ⏳ Ingress `on_exhaustion: trigger_agent` exhausted → `ExecutionResult(trigger_agent=True)`
- ⏳ Egress `on_exhaustion: trigger_agent` exhausted → `ExecutionResult(trigger_agent=True)`

### Assert `trigger_agent` propagation — `executor.py` Assert dispatch

- ⏳ Assert rule with `on_fail: trigger_agent` → `AssertError.trigger_agent=True` → `ExecutionResult.trigger_agent=True`
- ⏳ Assert rule with `on_fail: abort` → `ExecutionResult.trigger_agent=False`

### `probes.block_full_actions_in_prod` — `executor/spark/probe.py`

**`execute_probe(…, block_full_actions=False)`**, **`_row_count_estimate(…, block_full_actions=False)`**, **`_null_rates(…, block_full_actions=False)`**.

- ⏳ `block_full_actions=False` (default) → `row_count_estimate` sample `.count()` executes normally
- ⏳ `block_full_actions=True` → `row_count_estimate` method=sample → skips `.count()`, returns `{"blocked": True, "estimate": None}` + warning logged
- ⏳ `block_full_actions=True` → `row_count_estimate` method=spark_listener → DuckDB query still runs (no Spark action, not affected)
- ⏳ `block_full_actions=True` → `null_rates` → skips `.count()` + `.collect()`, returns `{"blocked": True, "null_rates": {col: None, ...}}` + warning logged
- ⏳ `block_full_actions=False` → `null_rates` executes normally
- ⏳ `execute()` accepts `block_full_actions: bool = False`; threaded to `execute_probe()`

### CLI trigger_agent override — `aqueduct/cli.py`

- ⏳ `result.trigger_agent=True` + `approval_mode=disabled` → `effective_mode` set to `"human"`, message printed to stderr
- ⏳ `result.trigger_agent=False` + `approval_mode=disabled` → loop breaks immediately (no LLM)
- ⏳ `result.trigger_agent=True` + `approval_mode=human` → `effective_mode` stays `"human"` (already correct; no override message printed)
- ⏳ `cfg.probes.block_full_actions_in_prod` passed to `execute()` as `block_full_actions`
