# ISSUE-016: Freshness quarantine missing observability columns

## Description
When `freshness` rule is used with `on_fail: quarantine`, the resulting quarantine DataFrame lacks the standard `_aq_error_module`, `_aq_error_rule`, `_aq_error_msg`, and `_aq_error_ts` columns that other row-level rules (like `sql_row`) provide.

## Root Cause
In `aqueduct/executor/spark/assert_.py`, the `freshness` branch (lines 119-133) applies `filter` but does not add the `withColumn` metadata calls present in `_apply_row_rule`.

## Reproduction
Run `tests/test_executor/test_executor_assert_quarantine.py::test_freshness_quarantine_success`.

## Expected Behavior
Quarantine DataFrames should consistently include `_aq_error_*` metadata columns.
