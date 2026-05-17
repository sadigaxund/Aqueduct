# 04 — Funnel Merge (`aqueduct test`)

This example demonstrates isolated-module testing for a `Funnel` module, which takes multiple upstream inputs (fan-in) and merges them.

When testing a `Funnel` module in isolation using `aqueduct test`, you declare multiple named upstream keys under the `inputs:` section—one for each upstream module feeding the Funnel. The isolated test runner feeds all declared DataFrames into the Funnel and evaluates assertions on the combined output.

## Files

| File | Role |
|---|---|
| `blueprint.yml` | `source_a` & `source_b` → `union_sources` (Funnel) → `save_result` |
| `blueprint.aqtest.yml` | Declares two upstream inputs (`source_a` and `source_b`) with inline rows and verifies that the output contains all 4 rows combined. |
| `data/input/primary.csv` | Sample CSV data on disk for end-to-end `aqueduct run` support. |
| `data/input/secondary.csv` | Sample CSV data on disk for end-to-end `aqueduct run` support. |

## Run the Isolated Test

```bash
cd gallery/aqtests/04_funnel_merge
aqueduct test blueprint.aqtest.yml
```

Expected: `1 tests | 1 passed | 0 failed`, with each assertion `✓`.

## Run the Pipeline End-to-End

```bash
aqueduct run blueprint.yml
```

## What it teaches

- Isolated testing supports multiple distinct upstream inputs (fan-in).
- To test a `Funnel` module in isolation, declare all its parent dependencies inside the `inputs:` configuration block.
- Standard SQL assertions and row counts can verify complex merge logic (e.g. union, join, coalesce) without needing real databases or file streams.
