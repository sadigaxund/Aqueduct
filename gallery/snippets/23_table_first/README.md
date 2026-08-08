# Table-First Addressing

Read and write data by catalog identifier (`catalog.schema.table`) instead of a filesystem `path:`.

## How it works

Ingress modules with `table:` resolve against the engine's own catalog: Spark
uses `spark.read.table(table)`; DuckDB uses `con.table(table)`. Egress modules
mirror that — Spark's `df.write.saveAsTable(table)`, DuckDB's own
`CREATE [OR REPLACE] TABLE ... AS`.

The catalog is configured through each engine's native config, not an
Aqueduct-specific one. On Spark that's `engine.spark.conf`
(`spark.sql.catalog.*`/`spark.sql.catalogImplementation`) — Spark's own
catalog configuration (Unity Catalog, Hive, Iceberg REST, Polaris, Glue) is
used as-is. On DuckDB it's `engine.duckdb.database_path` — a persistent
catalog file, since DuckDB's default `:memory:` connection can't survive
into a separate process the way this demo's populate/run/inspect steps need.

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
python populate_tables.py        # creates demo tables in both engines' local catalogs
aqueduct doctor blueprint.yml     # doctor checks table existence
aqueduct run blueprint.yml
aqueduct run blueprint.yml --config aqueduct.yml --set deployment.engine=duckdb  # or on DuckDB
```

> The blueprint's `engine.spark.conf` enables **Hive catalog** (`spark.sql.catalogImplementation: hive`)
> so the managed table created by `populate_tables.py` persists across Spark sessions via
> a Derby metastore (`metastore_db/`). Without this the catalog is in-memory only
> and `table: demo_table` would fail with `TABLE_OR_VIEW_NOT_FOUND`.
>
> `aqueduct.yml`'s `engine.duckdb.database_path` (`demo.duckdb`) is the DuckDB
> equivalent — a persistent catalog file `populate_tables.py` also seeds, so
> `table: demo_table` resolves the same way on that engine. Without it DuckDB's
> default `:memory:` connection would be empty and per-process, and `table:
> demo_table` would fail with `Catalog Error: Table with name demo_table does
> not exist!`.
>
> To test against a real catalog (Unity, Iceberg REST, Glue), replace
> `spark.sql.catalogImplementation` with `spark.sql.catalog.*` properties and use
> the full three-level identifier (`catalog.schema.table`) in the `table:` value.
