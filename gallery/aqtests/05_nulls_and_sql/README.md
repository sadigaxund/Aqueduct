# 05 — Nulls and SQL (`aqueduct test`)

This example serves as a reference for advanced assertion mechanics under `aqueduct test`, specifically detailing:
1. **Handling SQL NULL values** in both input inline rows (by using YAML `null`) and in `contains` assertions.
2. **Writing aggregate SQL assertions** that run against the local `__output__` temp view of the module under test.

## Files

| File | Role |
|---|---|
| `blueprint.yml` | `raw_items` → `items_pass` (Channel) → `save_items` |
| `blueprint.aqtest.yml` | Sets up inline rows containing explicit NULLs, and asserts them using `contains` and complex SQL queries. |
| `data/input/items.csv` | Sample CSV data on disk for end-to-end `aqueduct run` support. |

## Run the Isolated Test

```bash
cd gallery/aqtests/05_nulls_and_sql
aqueduct test blueprint.aqtest.yml
```

Expected: `1 tests | 1 passed | 0 failed`, with each assertion `✓`.

## Run the Pipeline End-to-End

```bash
aqueduct run blueprint.yml
```

## What it teaches

- **Explicit NULLs**: Use YAML `null` (or `~`) to denote NULL in the input `rows`.
- **NULL Assertions**: In a `contains` assertion, setting a column to `null` verifies that the column in the output is indeed SQL `NULL`.
- **SQL Aggregates**: The `sql` assertion evaluates any valid Spark SQL expression against the `__output__` view. The first column of the first row returned by the query must evaluate to a truthy value (`True` or non-zero/non-empty) for the assertion to pass.
