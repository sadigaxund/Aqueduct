# ISSUE-018: Freshness rule missing 'column' validation

## Description
The `freshness` rule does not raise a `ValueError` when the required `column` configuration field is missing. It silently skips evaluation instead.

## Root Cause
In `aqueduct/executor/spark/assert_.py`, lines 123-125:
```python
123:                 col = rule.get("column")
124:                 max_age_hours = float(rule.get("max_age_hours", 24))
125:                 if col:
```
If `col` is `None`, the block is skipped without warning or error.

## Reproduction
Run `tests/test_executor/test_executor_assert_quarantine.py::test_freshness_quarantine_missing_column_key`.

## Expected Behavior
Missing `column` for a `freshness` rule should raise a `ValueError` during execution (or a validation error during parsing).
