# Aqtests (`aqueduct test`)

Fast, deterministic **unit tests for a single module**. An aqtest feeds one Channel / Junction / Funnel / Assert module inline rows and asserts its output — no Ingress, no Egress, no external sources, no fixture files. Needs a local Spark session; runs in seconds.

This is distinct from `aqscenarios/` (Spark-free self-heal evals run via `aqueduct benchmark`) and from `snippets/` (whole-Blueprint feature demos run via `aqueduct run`).

## Implemented Examples

| Example | Focus Module | Core Assertions & Mechanics | What it teaches |
|---|---|---|---|
| [`01_channel_dedup`](01_channel_dedup) | `Channel` | `row_count`, `contains`, `sql` | Inline-row testing of a keep-latest dedup Channel; mapping upstream ids to `inputs`; querying the `__output__` temp view. |
| [`02_assert_quarantine`](02_assert_quarantine) | `Assert` | `row_count`, `contains`, `sql` | Isolated testing of an Assert module with row-level quarantine rules. Shows that `aqueduct test` automatically returns the `passing_df` (filtering out failing rows) for Assert quality gates. |
| [`03_junction_branch`](03_junction_branch) | `Junction` | `row_count`, `contains` | Isolated testing of conditional multi-branch routing. Shows how to target a non-default branch using the `branch:` parameter, and how omitting it defaults to the first branch. |
| [`04_funnel_merge`](04_funnel_merge) | `Funnel` | `row_count`, `contains` | Isolated testing of fan-in (multi-input) modules. Demonstrates how to declare multiple upstream entries under the `inputs:` block to mock a union merge of separate inputs. |
| [`05_nulls_and_sql`](05_nulls_and_sql) | `Channel` | `contains` (with `null`), aggregate `sql` | Pure assertion-mechanics reference. Highlights YAML `null` in inputs, verifying null columns in `contains` rules, and utilizing multi-row SQL aggregate assertions over `__output__`. |

## Test file shape

```yaml
aqueduct_test: "1.0"
blueprint: blueprint.yml          # relative to the test file

tests:
  - id: <case_id>
    module: <module_id>           # the module under test
    # branch: <branch_id>         # optional: for Junction branch routing
    inputs:
      <upstream_module_id>:
        schema: { col: long, other: string }
        rows:
          - [1, "a"]
          - [2, null]             # explicit NULL row value
    assertions:
      - type: row_count
        expected: 2
      - type: contains
        rows:
          - { col: 1, other: "a" }
          - { col: 2, other: null }  # checks that 'other' is SQL NULL
      - type: sql
        expr: "SELECT count(*) = 2 FROM __output__"
```

Run any example with `aqueduct test <file>.aqtest.yml`.
