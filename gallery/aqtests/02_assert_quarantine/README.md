# 02 — Assert Quarantine (`aqueduct test`)

This example demonstrates isolated-module testing for an `Assert` quality gate module with row-level quarantine rules.

When testing an `Assert` module in isolation using `aqueduct test`, the isolated test runner executes the quality gate rules and returns the **passing** DataFrame (where rows that failed the quarantine checks have been discarded) to be asserted against. This lets you verify that quality gate conditions are routing data correctly in a isolated sandbox.

## Files

| File | Role |
|---|---|
| `blueprint.yml` | `raw_orders` → `filter_large_amounts` (Assert) → `passing_save` & `quarantined_save` |
| `blueprint.aqtest.yml` | Feeds the Assert module four inline rows and verifies that exactly two pass the `amount >= 10` gate. |
| `data/input/orders.csv` | Static CSV data on disk to allow end-to-end execution of the pipeline using `aqueduct run`. |

## Run the Isolated Test

```bash
cd gallery/aqtests/02_assert_quarantine
aqueduct test blueprint.aqtest.yml
```

Expected: `1 tests | 1 passed | 0 failed`, with each assertion `✓`.

## Run the Pipeline End-to-End

```bash
aqueduct run blueprint.yml
```

## What it teaches

- `aqueduct test` on an `Assert` module automatically resolves and returns the **passing port** (i.e. `passing_df`), filtering out any quarantined rows.
- Assert row-level quality gates are highly testable without needing a live pipeline or actual file system I/O.
