# Ingress Schema Hints

Demonstrates `schema_hint` on Ingress — override Spark's inferred column types
when reading structured files.

## Setup

```bash
pip install -r requirements.txt
```

## Why schema hints?

Spark's `inferSchema` is convenient but sometimes gets it wrong:

| Problem | Example |
|---------|---------|
| Leading zeros lost | `"001"` read as integer `1` |
| Date ambiguity | `"01/02/2025"` could be Jan 2 or Feb 1 |
| Currency strings | `"$19.99"` parsed as null without hints |
| Mixed precision | `price` read as `double` when it should be `decimal` |

Schema hints let you pin types without writing a full DDL schema.

## Forms

```yaml
# Compact dict form (column_name → type_string)
schema_hint:
  id: string
  price: double

# Explicit columns list (with optional `mode: strict | lenient`)
schema_hint:
  columns:
    - name: id
      type: string
    - name: price
      type: double
  mode: strict   # fail if actual schema doesn't match (default: strict)
```

## What this snippet shows

The CSV has `id = 001, 002, ...` — without a `schema_hint`, Spark infers
`id` as integer (`1, 2, ...`), losing the leading zeros. With the hint
`id: string`, the values are preserved verbatim.

### `on_new_columns` — schema drift guard on Ingress

Add `on_new_columns` to catch unexpected column additions without
silently passing bad data downstream:

```yaml
config:
  format: csv
  path: data/input/products.csv
  schema_hint:
    id: string
    price: double
  on_new_columns: fail    # abort if columns appear that aren't in schema_hint
```

| Value | Behaviour |
|-------|-----------|
| `allow` | Pass new columns through (default) |
| `alert` | Log a warning, pass data through |
| `fail` | Raise an error and abort the pipeline |

Also available on Egress — guards against upstream schema changes
writing columns the target table doesn't expect.

### `known_columns` — production schema guard

When reading from a production table (especially JDBC), list the columns
you expect. Spark will fail-fast if the table's schema diverges:

```yaml
config:
  format: jdbc
  url: "${JDBC_URL}"
  table: "public.orders"
  known_columns: ["id", "customer_id", "amount", "order_date"]
```

Without this, an upstream `ALTER TABLE ... DROP COLUMN amount` would
silently produce `null` values. With `known_columns`, the pipeline
aborts with a clear schema-mismatch error.

## How to Run

```bash
python populate_data.py

aqueduct run blueprint.yml
```
