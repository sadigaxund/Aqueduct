# Atomic Module Samples To Implement

These samples are minimal, focused YAML snippets designed for copy-pasting into real blueprints.

## Ingress Variants
- [x] `01_ingress_csv_options/blueprint.yml`: Showing header, inferSchema, and custom delimiters.
- [x] `02_ingress_parquet_s3/blueprint.yml`: Reading from S3 with explicit schema hints.
- [x] `03_ingress_delta_incremental/blueprint.yml`: Using versioning/time-travel in Delta Lake.
- [x] `04_ingress_jdbc_postgres/blueprint.yml`: Standard JDBC options for relational sources.

## Channel Operations
- [x] `05_channel_sql_udf/blueprint.yml`: Registering and calling a Python UDF in SQL.
- [ ] `06_channel_deduplicate/blueprint.yml`: Pattern for "keep last record per ID".
- [ ] `07_channel_join_variants/blueprint.yml`: Showcase Left, Anti, and Cross joins with broadcast hints.
- [ ] `08_channel_cache_strategy/blueprint.yml`: Using MEMORY_AND_DISK vs MEMORY_ONLY.

## Assertion Rules
- [ ] `09_assert_null_sampling/blueprint.yml`: Checking null rates on a 10% sample for performance.
- [ ] `10_assert_freshness/blueprint.yml`: Validating that the latest timestamp is within N hours.
- [ ] `11_assert_row_quarantine/blueprint.yml`: Using `sql_row` to route bad data to a spillway.

## Junction & Funnel
- [ ] `12_junction_conditional_else/blueprint.yml`: Routing with a "catch-all" branch.
- [ ] `13_funnel_coalesce_fallback/blueprint.yml`: Merging primary and secondary sources to fill gaps.

## Observability (Probes & Regulators)
- [ ] `14_probe_value_dist/blueprint.yml`: Capturing percentiles and distributions for numeric columns.
- [ ] `15_regulator_timeout/blueprint.yml`: A gate that proceeds automatically if a signal doesn't arrive.
