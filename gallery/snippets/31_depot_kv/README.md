# Depot KV — Cross-Run State

Demonstrates `@aq.depot.get()` — Aqueduct's cross-run KV store.

## Setup

```bash
pip install -r requirements.txt
```

## How it works

The depot is an implicit per-blueprint KV store (backed by DuckDB). After each
successful run, the executor automatically updates:
- `watermark` keys from `materialize: incremental` modules
- `_last_run_id` — the previous run UUID

You can read these with `@aq.depot.get()` in any Blueprint config position.

## Blueprint walkthrough

The Channel uses `materialize: incremental` with `watermark_column: created_at`.
The SQL filter reads the last watermark:

```sql
WHERE created_at > '{{ @aq.depot.get('watermark', '2025-06-01') }}'
```

- **First run**: depot empty → default `2025-06-01` → processes rows 1-6
- **Second run**: depot watermark = `2025-06-20` → WHERE clause filters everything → zero rows

Add new rows to `events.csv` between runs and only the new records will appear.

## Depot write side — `format: depot` Egress

The pipeline also writes a KV entry (`last_watermark → run_id`) to the depot
using an Egress with `format: depot`:

```yaml
- id: save_watermark
  type: Egress
  config:
    format: depot
    key: last_watermark
    value: "@aq.run.id()"
```

This is how you programmatically persist state between runs — later runs
read it with `@aq.depot.get('last_watermark')`.

## Additional depot functions

| Call | Purpose |
|------|---------|
| `@aq.depot.get('key')` | Read a KV entry (default: `""`) |
| `@aq.depot.get('key', 'default_val')` | Read with fallback |
| `@aq.depot.<name>.get('key')` | Read from a named mount (configured under `stores.depots.<name>` in `aqueduct.yml`) |
| `@aq.run.prev_id()` | Read `_last_run_id` from the default depot |

## How to Run

```bash
# First run — processes all 6 rows
python populate_data.py

aqueduct run blueprint.yml
python -c "import pandas; print(pandas.read_parquet('data/output/events.parquet'))"

# Second run — watermark filters everything (zero new rows)
aqueduct run blueprint.yml

# Add a new row to events.csv and run again — only the new row appears
echo "7,purchase,1005,200.00,2025-07-01" >> data/input/events.csv
aqueduct run blueprint.yml
```
