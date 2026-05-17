# 03 — Junction Branch (`aqueduct test`)

This example demonstrates isolated-module testing for a `Junction` fan-out module, showing how to assert against specific output branches.

A `Junction` module produces multiple named output streams (ports). When testing a `Junction` in isolation using `aqueduct test`:
- By default, assertions are run against the **first** branch defined in the module's `branches` configuration.
- To assert against a different, specific branch, you can add the `branch:` key to your test case.

## Files

| File | Role |
|---|---|
| `blueprint.yml` | `raw_tickets` → `route_tickets` (Junction) → `save_active` & `save_other` |
| `blueprint.aqtest.yml` | Defines two test cases: one checking the default (`active`) branch and another targeting the `other` branch via the `branch:` property. |
| `data/input/tickets.csv` | Companion CSV data on disk for end-to-end `aqueduct run` support. |

## Run the Isolated Test

```bash
cd gallery/aqtests/03_junction_branch
aqueduct test blueprint.aqtest.yml
```

Expected: `2 tests | 2 passed | 0 failed`, with each assertion `✓`.

## Run the Pipeline End-to-End

```bash
aqueduct run blueprint.yml
```

## What it teaches

- Isolated testing of multi-branch conditional routing (Junctions) is fully supported.
- By omitting `branch:`, `aqueduct test` routes assertions to the first branch in the module config list.
- Use the `branch: <branch_id>` configuration under the test definition to target and assert on any other output branch.
