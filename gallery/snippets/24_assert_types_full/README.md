# Assert Types — Full Demo

Demonstrates all Assert rule types in a single pipeline.

## Rules shown

| Rule | Behaviour |
|------|-----------|
| `min_rows` | Pipeline fails if fewer than 5 rows |
| `max_rows` | Pipeline fails if more than 100 rows |
| `sql` | Aggregate check: `MAX(amount)` must be ≤ 2000 |
| `spillway_rate` | Ensures zero rows route to the spillway port |
| `schema_match` | Validates column names and types match expectations |
| `not_null` | Checks `order_id` column has no null values |
| `custom` | Callable check: no completed order exceeds $1000 (see `rules.py`) |

All rules on this pipeline pass — the source data is clean. To test failure,
change a threshold (e.g. `min_count: 100`) or remove a column.

### `on_fail: warn` — non-fatal assertions

Any Assert rule supports `on_fail: warn` to log quality issues without
aborting the pipeline:

```yaml
rules:
  - id: soft_null_check
    type: null_rate
    column: amount
    max_rate: 0.1
    on_fail: warn    # warn instead of abort
```

Other `on_fail` values: `abort` (default), `quarantine`, `trigger_agent`,
`webhook` (POSTs to a URL with failure details).

## How to Run

```bash
aqueduct run blueprint.yml
```
