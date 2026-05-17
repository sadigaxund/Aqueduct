# Aqueduct Gallery

Runnable examples, smallest to largest. Every directory is
self-contained: `cd` into it, read its `README.md`, run it.

## Showcase

End-to-end deployments you can stand up and operate.

| Example | What it teaches |
|---|---|
| [`showcase/01-spark-cluster`](showcase/01-spark-cluster) | Deploying Aqueduct against a real distributed Spark Standalone cluster + MinIO (S3A) + Postgres-backed stores. Guided, interactive walkthrough: the driver/cluster networking contract, the S3A jar story, spillway + quality gates on a cluster, reading the run back from Postgres. |

## Snippets

One feature per directory, minimal Blueprint, runs locally with no
external services. The fastest way to see a single capability in
isolation.

**Ingress**
- `01_ingress_csv_options` — CSV read with reader options
- `02_ingress_parquet_s3` — Parquet from S3-style object storage
- `03_ingress_delta_incremental` — Delta source, incremental read
- `04_ingress_jdbc_postgres` — JDBC source (Postgres)

**Channel (transform)**
- `05_channel_sql_udf` — SQL op with a registered UDF
- `06_channel_deduplicate` — native `deduplicate` op
- `07_channel_join_variants` — join types (inner / left / semi / anti)
- `08_channel_union_fanout` — union plus multi-consumer fan-out
- `09_channel_repartition_cache` — `repartition` / `coalesce` / `cache`
- `10_sql_macros` — reusable SQL via `{{ macros.* }}`

**Quality, gates, error routing**
- `11_passive_regulator_spillway` — passive Regulator + spillway routing
- `12_assert_null_sampling` — `Assert` null-rate with sampling
- `13_assert_freshness` — `Assert` freshness window
- `14_assert_row_quarantine` — failing rows quarantined, pipeline continues
- `15_regulator_timeout` — Regulator gate driven by a Probe signal

**Routing**
- `16_junction_conditional_else` — `Junction` conditional split + else branch
- `17_funnel_coalesce_fallback` — `Funnel` coalesce as a fallback merge

**Observability & state**
- `18_probe_value_dist` — `Probe` value-distribution signal
- `19_depot_incremental` — Depot watermark powering `materialize: incremental`

**Reuse**
- `20_arcade_reusable` — an Arcade (reusable sub-Blueprint) used twice

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

[`aqscenarios/`](aqscenarios) holds self-healing scenario tests: a
Blueprint plus an injected failure, run via `aqueduct benchmark` to
exercise the Agent loop deterministically across models.
See [`aqscenarios/README.md`](aqscenarios/README.md).
