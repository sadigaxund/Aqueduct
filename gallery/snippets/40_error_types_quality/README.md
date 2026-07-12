# Typed Spillway — error_types + description + tags

Demonstrates three features in one snippet:

1. **`error_types` on spillway edges** — quarantined rows are labelled
   by Assert rule, and each spillway edge only receives rows whose
   `_aq_error_type` matches its `error_types` filter.
2. **`description` on modules** — free-text explanation visible in the
   dashboard UI and LLM context.
3. **`tags` on modules** — string list used for filtering and scoped
   search in the observability store.

## Setup

```bash
pip install -r requirements.txt
```

## Data quality rules

| Rule | SQL | error_type | Rows | Route |
|------|-----|------------|------|-------|
| `missing_amount` | `amount IS NULL` | `MissingValue` | Bob (id=2) | → `missing_values.csv` |
| `extreme_amount` | `amount < 0 OR amount > 10000` | `DataQualityViolation` | Charlie (id=3), Eve (id=5) | → `quality_violations.csv` |
| (no rule matches) | — | — | Alice, Diana, Frank (ids 1,4,6) | → `clean.parquet` |

## How to Run

```bash
python populate_data.py

aqueduct run blueprint.yml
python inspect_results.py
```

## How error_types work

1. Each Assert rule declares `error_type: <label>` — a custom string.
2. Rows matching that rule are quarantined with `_aq_error_type = <label>`.
3. A spillway edge with `error_types: [MissingValue]` only passes rows
   whose `_aq_error_type` is `MissingValue`.
4. Multiple edges from the same spillway port with different
   `error_types` act as a typed catch block — each routes to a
   different sink.

```yaml
edges:
  - from: quality_gate
    to: missing_values
    port: spillway
    error_types: [MissingValue]

  - from: quality_gate
    to: quality_violations
    port: spillway
    error_types: [DataQualityViolation]
```

An edge **without** `error_types` is a catch-all — it receives all
quarantined rows. This is independent of the Assert `action` setting;
quarantine rows always go to the spillway port, and the edge's
`error_types` filter is evaluated at the Spark DataFrame level (zero
extra actions).

## Where description and tags appear

- **Dashboard** (in the showcase): the Run detail tab shows per-module
  `description` and `tags` in the module metrics table.
- **LLM context**: when the agent generates a healing patch, module
  descriptions and tags are included in the prompt.
- **Observability store**: `module_metrics` table stores `tags` for
  filtering.
