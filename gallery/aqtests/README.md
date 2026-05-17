# Aqtests (`aqueduct test`)

Fast, deterministic **unit tests for a single module**. An aqtest feeds
one Channel / Junction / Funnel / Assert module inline rows and asserts
its output — no Ingress, no Egress, no external sources, no fixture
files. Needs a local Spark session; runs in seconds.

This is distinct from `aqscenarios/` (Spark-free self-heal evals run via
`aqueduct benchmark`) and from `snippets/` (whole-Blueprint feature
demos run via `aqueduct run`).

| Example | What it teaches |
|---|---|
| [`01_channel_dedup`](01_channel_dedup) | Inline-row testing of a keep-latest dedup Channel; `row_count` / `contains` / `sql` assertions; `__output__` view. |

## Test file shape

```yaml
aqueduct_test: "1.0"
blueprint: blueprint.yml          # relative to the test file

tests:
  - id: <case_id>
    module: <module_id>           # the module under test
    inputs:
      <upstream_module_id>:
        schema: { col: long, other: string }
        rows:
          - [1, "a"]
    assertions:
      - type: row_count
        expected: 1
      - type: contains
        rows:
          - { col: 1 }
      - type: sql
        expr: "SELECT count(*) = 1 FROM __output__"
```

Run any example with `aqueduct test <file>.aqtest.yml`.
