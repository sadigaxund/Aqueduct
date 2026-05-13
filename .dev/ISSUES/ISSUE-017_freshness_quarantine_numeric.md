# ISSUE-017: Freshness quarantine fails on numeric columns

## Description
When `freshness` rule is used with `on_fail: quarantine` on a numeric column (Double/Long) representing a Unix timestamp, Spark raises an `AnalysisException` due to type mismatch.

## Root Cause
The quarantine filter expression `F.col(col) >= (F.current_timestamp() - F.expr(interval))` compares a numeric column with a `TIMESTAMP` value. Spark does not automatically cast numeric columns to timestamps in this context.

## Reproduction
Run `tests/test_executor/test_executor_assert_quarantine.py::test_freshness_quarantine_numeric`.

## Expected Behavior
Numeric columns should be automatically treated as Unix timestamps (in seconds) or cast appropriately, matching the behavior of the aggregate `freshness` check.
