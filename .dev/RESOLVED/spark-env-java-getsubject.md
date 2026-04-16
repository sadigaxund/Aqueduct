# Issue: Spark I/O Failures (getSubject)

## Summary
Multiple Spark I/O tests are failing with an `UnsupportedOperationException`. This appears to be an environmental issue between Spark's security manager and the Java version (17+) in the current environment.

## Failing Tests
- `tests/test_executor_ingress.py`: `test_ingress_valid_parquet`, `test_ingress_csv_defaults`, `test_ingress_custom_options`, `test_ingress_schema_hint_missing_col`, `test_ingress_schema_hint_wrong_type`
- `tests/test_executor_egress.py`: `test_egress_partition_by`, `test_egress_custom_options`, `test_egress_mode_overwrite`
- `tests/test_executor_orchestration.py`: `test_execute_success`, `test_execute_egress_error`, `test_execute_run_id_auto_gen`, `test_execute_run_id_supplied`

## Error Details
```
pyspark.errors.exceptions.captured.UnsupportedOperationException: getSubject is not supported
```

## Context
This is a known issue when running Spark on Java 17+ without specific JVM flags to disable the security manager or when Hadoop libraries are missing/mismatched. Since the engine is designed for production Spark clusters where these are typically configured, this is considered a local environment configuration failure rather than a code bug.
