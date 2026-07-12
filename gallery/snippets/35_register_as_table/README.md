# Register as Table — Egress Catalog Registration

Demonstrates `register_as_table` on Egress — write your output AND publish
it to the Spark catalog so it's queryable by name in the same session.

## Setup

```bash
pip install -r requirements.txt
```

## How it works

```yaml
- id: output
  type: Egress
  config:
    format: parquet
    path: data/output/products.parquet
    mode: overwrite
    register_as_table: my_catalog.my_schema.my_table
```

After `save()` completes, the engine executes:
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS my_catalog.my_schema.my_table
USING parquet
LOCATION 'data/output/products.parquet'
```

**Cross-session visibility** requires a persistent metastore (Hive, Unity
Catalog, Iceberg REST). Without one, the table only lives for the Spark
session duration.

## How to Run

```bash
python populate_data.py

aqueduct run blueprint.yml
```

The blueprint:
1. Reads `products.csv`
2. Filters to in-stock items → writes parquet + registers as `in_stock_products` table
3. Reads back from the catalog table to confirm registration
