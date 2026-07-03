# Table-First Addressing

Read and write data by catalog identifier (`catalog.schema.table`) instead of a filesystem `path:`.

## How it works

Ingress modules with `table:` use `spark.read.table(table)` — the catalog resolves
the table's location, schema, and format. Egress modules use `df.write.saveAsTable(table)`.

The catalog is configured through standard `spark_config` properties
(`spark.sql.catalog.*`). No Aqueduct-specific catalog config is needed —
Spark's own catalog configuration (Unity Catalog, Hive, Iceberg REST, Polaris, Glue)
is used as-is.

```yaml
- id: src
  type: Ingress
  config:
    table: my_catalog.my_schema.my_table
    # no format, no path required
```

```yaml
- id: out
  type: Egress
  config:
    format: parquet
    table: my_catalog.my_schema.my_output
    mode: overwrite
```

`table:` and `path:` are mutually exclusive — setting both raises an error.

## Limitations

- `time_travel` (version/timestamp pin) is not supported on `table:`-addressed
  Ingress reads; use a Channel with `TIMESTAMP AS OF` syntax.
- `register_as_table` is meaningless when `table:` is set on an Egress — logged
  as a non-fatal warning.

## How to Run

```bash
python populate.py               # creates demo tables in local catalog
aqueduct doctor blueprint.yml     # doctor checks table existence
aqueduct run blueprint.yml
```

> The blueprint's `spark_config` enables **Hive catalog** (`spark.sql.catalogImplementation: hive`)
> so the managed table created by `populate.py` persists across Spark sessions via
> a Derby metastore (`metastore_db/`). Without this the catalog is in-memory only
> and `table: demo_table` would fail with `TABLE_OR_VIEW_NOT_FOUND`.
>
> To test against a real catalog (Unity, Iceberg REST, Glue), replace
> `spark.sql.catalogImplementation` with `spark.sql.catalog.*` properties and use
> the full three-level identifier (`catalog.schema.table`) in the `table:` value.
