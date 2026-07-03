# Delta MERGE & Retry Policy

Demonstrates **Delta MERGE** Egress (upsert by `order_id`) with a **RetryPolicy**.

## Key Concepts

| Feature | What it does |
|---------|--------------|
| `mode: merge` | Delta `MERGE INTO` — inserts new rows, updates existing ones by `merge_keys` |
| `merge_keys: [order_id]` | Match condition: row exists with same order_id → update; else insert |
| `mergeSchema: true` | Auto-add new columns from source if missing in target |
| RetryPolicy | 4 attempts with exponential backoff (1s base) |

A `populate_delta.py` script creates the initial Delta table from `orders.csv`.
Then `aqueduct run` applies the MERGE with update/insert rows from `updates.csv`.
Subsequent runs with different `updates.csv` content will upsert again.

## How to Run

```bash
# Step 1: create the initial Delta table
python populate_delta.py

# Step 2: merge updates into it
aqueduct run blueprint.yml
python -c "import pandas; df=pandas.read_parquet('data/output/delta_orders'); print(df)"

# Modify updates.csv with new data, then run step 2 again:
aqueduct run blueprint.yml   # merges: updates existing, inserts new
```
