# ISSUE-015: Freshness quarantine drops NULL rows

## Description
When `freshness` rule is used with `on_fail: quarantine`, rows where the timestamp column is `NULL` are dropped from both the passing and quarantine DataFrames.

## Root Cause
In `aqueduct/executor/spark/assert_.py`:
```python
130:                     fresh_expr = F.col(col) >= (F.current_timestamp() - F.expr(interval))
131:                     q_df = passing_df.filter(~fresh_expr)
132:                     passing_df = passing_df.filter(fresh_expr)
```
In Spark, `filter(expr)` and `filter(~expr)` both exclude rows where `expr` evaluates to `NULL`. Since `NULL >= something` is `NULL`, rows with `NULL` timestamps are lost.

## Reproduction
Run `tests/test_executor/test_executor_assert_quarantine.py::test_freshness_quarantine_nulls`.

## Expected Behavior
NULL rows should be routed to the quarantine DataFrame as they fail the freshness check.
