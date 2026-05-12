# Aqueduct Gallery: Atomic Module Samples

Minimal, focused snippets designed for learning and copy-pasting into production blueprints.

## Ingress Variants
- [x] `01_ingress_csv_options`: Header, inferSchema, and custom delimiters.
- [x] `02_ingress_parquet_s3`: S3 connectivity and schema hints.
- [x] `03_ingress_delta_incremental`: Time-travel and versioning in Delta Lake.
- [x] `04_ingress_jdbc_postgres`: Relational source configuration.

## Channel Operations
- [x] `05_channel_sql_udf`: Registering and calling Python UDFs.
- [x] `06_channel_deduplicate`: "Keep last record per ID" patterns.
- [x] `07_channel_join_variants`: Left, Anti, and Cross joins with broadcast hints.
- [x] `08_channel_union_fanout`: Merging and splitting streams.
- [x] `09_channel_repartition_cache`: Performance tuning with repartitioning and caching.

## Data Quality & Assertions
- [x] `10_passive_regulator_spillway`: Manual routing of invalid data.
- [x] `11_assert_null_sampling`: Statistical null rate checks on large datasets.
- [x] `12_assert_freshness`: SLA-bound freshness validation.
- [x] `13_assert_row_quarantine`: Record-level diversion using `sql_row`.

## Advanced Routing & Merging
- [x] `14_junction_conditional_else`: Switch/Case style routing with catch-all.
- [x] `15_funnel_coalesce_fallback`: Multi-source gap filling (primary/secondary).

## Observability & Control
- [x] `16_probe_value_dist`: Statistical distributions and percentiles via DuckDB.
- [x] `17_regulator_timeout`: Time-bound polling gates for async approvals.
