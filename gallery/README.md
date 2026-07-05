# Aqueduct Gallery

Runnable examples, smallest to largest. Every directory is
self-contained: `cd` into it, read its `README.md`, run it.

## Showcase

End-to-end deployments you can stand up and operate.

| Example | What it teaches |
|---|---|
| [`showcase/01-spark-cluster`](showcase/01-spark-cluster) | Deploying Aqueduct against a real distributed Spark Standalone cluster + MinIO (S3A) + Postgres-backed stores. Guided, interactive walkthrough: the driver/cluster networking contract, the S3A jar story, spillway + quality gates on a cluster, reading the run back from Postgres. |
| [`showcase/02-airflow`](showcase/02-airflow) | Aqueduct as an Apache Airflow operator. Compose-based stack (Airflow 2.10 + Postgres + triggerer). Two DAGs: happy-path wiring (no LLM) and a `HEAL_PENDING` round-trip where the task defers on `UNRESOLVED_COLUMN`, releases the worker slot, waits for human patch approval, and resumes green. |
| [`showcase/03-self-healing`](showcase/03-self-healing) | Self-healing tutorial — run a pipeline that fails, inspect the structured error in DuckDB, watch the LLM generate a patch, and observe guardrail rejection + budget exhaustion in action. Step-by-step with three interactive demos. |
| [`showcase/04-dashboard-showcase`](showcase/04-dashboard-showcase) | End-to-end dashboard tour: 4 comprehensive pipelines (orders_etl, user_analytics, inventory_healing, events_daily) running on Spark Standalone + MinIO + Postgres. Auto-healing, probes, asserts, spillway, repartition, UDFs, schema drift, and full lineage — all visible in the Streamlit dashboard. |
| [`showcase/05-socketnews-snowflake`](showcase/05-socketnews-snowflake) | **Scaffold** — Portfolio-grade stack: SocketNews API → Aqueduct/Spark → Snowflake → Grafana, scheduled by Airflow with micro-batch incremental processing. Architecture and docker-compose plan described; implementation in progress. |

## Snippets

One feature per directory, minimal Blueprint, runs locally with no
external services. The fastest way to see a single capability in
isolation.

**Ingress**
- `01_ingress_csv_options` — CSV read with reader options
- `03_ingress_delta_incremental` — Delta source, incremental read
- `04_ingress_jdbc_postgres` — JDBC source (Postgres/SQLite via env var)
- `30_ingress_schema_hints` — override Spark's inferred column types

**Pipeline identity & context**
- `02_blueprint_variables` — Context Registry `${ctx.*}` with profiles, CLI overrides, and env-var resolution

**Channel (transform)**
- `05_channel_sql_udf` — SQL op with a registered UDF
- `06_channel_deduplicate` — native `deduplicate` op
- `07_channel_join_variants` — join types (inner / left / semi / anti)
- `08_channel_union_fanout` — union plus fan-out to multiple consumers
- `09_channel_repartition_cache` — `repartition` / `coalesce` / `cache`
- `10_sql_macros` — reusable SQL via `{{ macros.* }}`
- `25_channel_native_ops` — native ops: `select`, `rename`, `cast`, `sort`

**Quality, gates, error routing**
- `11_spillway_channel` — Channel `spillway_condition` row-level routing
- `12_assert_null_sampling` — `Assert` null-rate with sampling
- `13_assert_freshness` — `Assert` freshness window
- `14_assert_row_quarantine` — failing rows quarantined, pipeline continues
- `15_regulator_timeout` — Regulator gate driven by a Probe signal
- `24_assert_types_full` — all assert types: `min_rows`, `sql`, `spillway_rate`, `schema_match`
- `40_error_types_quality` — typed spillway: `error_types` on edges + `description` and `tags` on modules

**Routing**
- `16_junction_conditional_else` — all 3 Junction modes: conditional (branching), broadcast (same-to-all), partition (round-robin)
- `17_funnel_coalesce_fallback` — `Funnel` coalesce as a fallback merge
- `36_funnel_modes` — all 3 Funnel merge modes: union_all, union (dedup), zip (interleave)

**Observability & state**
- `18_probe_value_dist` — `Probe` value-distribution signal
- `19_depot_incremental` — Depot watermark powering `materialize: incremental`
- `26_probe_signals_all` — all 8 probe signal types on one blueprint

**Agent & self-healing**
- `27_self_healing` — LLM-driven self-healing with `@aq.*` runtime functions and Probe diagnostics

**Reuse**
- `20_arcade_reusable` — an Arcade (reusable sub-Blueprint) used twice
- `23_table_first` — read/write by catalog identifier (catalog.schema.table)

**Advanced I/O**
- `21_probe_custom` — custom probe signal via inline SQL, module pointer, or entry-point plugin
- `22_custom_datasource` — Spark 4.0 Python DataSource (`format: custom`)
- `28_delta_merge_retry` — Delta `MERGE INTO` Egress with RetryPolicy (exponential backoff)
- `34_time_travel` — Delta/Parquet time-travel read at a specific version or timestamp
- `35_register_as_table` — Egress catalog registration (`register_as_table`) for cross-module table access
- `37_egress_modes` — 5 Egress write modes side-by-side: append, error, ignore, overwrite_partitions, replace_where
- `38_partition_io` — Ingress `partition_filters` + Egress `partition_by` for partitioned data lakes

**JVM & execution**
- `29_java_udf_parallel` — Java/Scala UDF reference contract + parallel execution mode

**Runtime & context**
- `33_runtime_functions` — `@aq.run.id()`, `@aq.blueprint.name()`, `@aq.version()`, and all built-in runtime functions

**Secrets & depot (cross-run state)**
- `31_depot_kv` — `@aq.depot.get('key')` cross-run KV store for watermarks and state
- `32_secret_injection` — `@aq.secret('KEY')` env-var secret injection without hardcoding

**Execution control**
- `39_checkpoint_resume` — `checkpoint: true` + retry policy for resume after failure

## Aqtests

[`aqtests/`](aqtests) holds isolated-module unit tests run with
`aqueduct test`: one Channel/Junction/Funnel/Assert module fed inline
rows, output asserted — no sources, no fixtures, Spark-backed, seconds.
See [`aqtests/README.md`](aqtests/README.md).

- [`01_channel_dedup`](aqtests/01_channel_dedup) — inline-row keep-latest dedup Channel; `row_count` / `contains` / `sql`
- [`02_assert_quarantine`](aqtests/02_assert_quarantine) — row-level quarantine quality gate isolated Assert testing
- [`03_junction_branch`](aqtests/03_junction_branch) — conditional multi-branch routing and branch targeting with `branch:`
- [`04_funnel_merge`](aqtests/04_funnel_merge) — multi-input union merge Funnel testing
- [`05_nulls_and_sql`](aqtests/05_nulls_and_sql) — YAML null rows, null checks in contains, aggregate SQL assertions

## Aqscenarios

[`aqscenarios/`](aqscenarios) holds self-healing scenario tests: a Blueprint plus
an injected failure, run via `aqueduct benchmark` to exercise the Agent loop
deterministically across models. No Spark needed. See
[`aqscenarios/README.md`](aqscenarios/README.md).

- [`01_schema_drift_column_rename`](aqscenarios/01_schema_drift_column_rename.aqscenario.yml) — upstream renamed `event_ts` -> `event_time`, breaking downstream SQL selection.
- [`02_sql_bad_column_ref`](aqscenarios/02_sql_bad_column_ref.aqscenario.yml) — SQL query references non-existent `signup_date` instead of `signup_ts`.
- [`03_format_csv_read_as_parquet`](aqscenarios/03_format_csv_read_as_parquet.aqscenario.yml) — Ingress reads CSV source file declaring `format: parquet`.
- [`04_bad_path_typo`](aqscenarios/04_bad_path_typo.aqscenario.yml) — Ingress file path has a typo (`events_raw.csv` instead of `events.csv`).
- [`05_type_string_vs_numeric`](aqscenarios/05_type_string_vs_numeric.aqscenario.yml) — Upstream events `event_id` is parsed as a string, downstream sum aggregate fails.
- [`06_guardrail_forbidden_op`](aqscenarios/06_guardrail_forbidden_op.aqscenario.yml) — LLM proposes a `remove_module` operation blocked by `forbidden_ops` guardrail.
- [`07_spark_oom_shuffle`](aqscenarios/07_spark_oom_shuffle.aqscenario.yml) — Large shuffle hits executor OOM; patch adjusts `shuffle.partitions` and adds a repartition.
- [`08_delta_schema_merge`](aqscenarios/08_delta_schema_merge.aqscenario.yml) — Delta read fails with schema mismatch; patch adds `mergeSchema: true`.

**Next:** Scenarios for JDBC connection errors, partition discovery failure, and UDF serialization are ideal candidates to add — see
[`aqscenarios/CONTRIBUTING.md`](aqscenarios/CONTRIBUTING.md).

> **OpenLineage integration:** Aqueduct emits lineage events to any
> OpenLineage-compatible backend (Marquez, Atlan, etc.) via
> `openlineage:` config in `aqueduct.yml`. This is configured per
> deployment, not per Blueprint — see `docs/specs.md` §7 and
> `docs/production_guide.md`.