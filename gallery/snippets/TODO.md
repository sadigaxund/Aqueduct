# Atomic Module Samples To Implement

These samples are minimal, focused YAML snippets designed for copy-pasting into real blueprints.

## Ingress Variants
- [ ] `ingress_csv_options.yml`: Showing header, inferSchema, and custom delimiters.
- [ ] `ingress_parquet_s3.yml`: Reading from S3 with explicit schema hints.
- [ ] `ingress_delta_incremental.yml`: Using versioning/time-travel in Delta Lake.
- [ ] `ingress_jdbc_postgres.yml`: Standard JDBC options for relational sources.

## Channel Operations
- [ ] `channel_sql_udf.yml`: Registering and calling a Python UDF in SQL.
- [ ] `channel_deduplicate.yml`: Pattern for "keep last record per ID".
- [ ] `channel_join_variants.yml`: Showcasing Left, Anti, and Cross joins with broadcast hints.
- [ ] `channel_cache_strategy.yml`: Using MEMORY_AND_DISK vs MEMORY_ONLY.

## Assertion Rules
- [ ] `assert_null_sampling.yml`: Checking null rates on a 10% sample for performance.
- [ ] `assert_freshness.yml`: Validating that the latest timestamp is within N hours.
- [ ] `assert_row_quarantine.yml`: Using `sql_row` to route bad data to a spillway.

## Junction & Funnel
- [ ] `junction_conditional_else.yml`: Routing with a "catch-all" branch.
- [ ] `funnel_coalesce_fallback.yml`: Merging primary and secondary sources to fill gaps.

## Observability (Probes & Regulators)
- [ ] `probe_value_dist.yml`: Capturing percentiles and distributions for numeric columns.
- [ ] `regulator_timeout.yml`: A gate that proceeds automatically if a signal doesn't arrive.
