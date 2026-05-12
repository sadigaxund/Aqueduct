# ISSUE-002: JDBC Ingress Requires Redundant 'path' Field

## Status
- **Type**: Bug / Architectural Gap
- **Severity**: Medium (Workaround available)
- **Phase**: 19 (Gallery Population)
- **Found by**: User testing `04_ingress_jdbc_postgres` snippet

## Description
The `aqueduct.executor.spark.ingress.read_ingress` function enforces a non-empty `path` field for all Ingress modules:

```python
    path: str | None = cfg.get("path")
    if not path:
        raise IngressError(f"[{module.id}] 'path' is required in Ingress config")
```

However, for `format: jdbc`, Spark does not require a path. It uses `url` and `dbtable` (or `query`) instead. Forcing a `path` field for JDBC is counter-intuitive and results in a `parse error` for standard JDBC blueprints.

## Root Cause
Hardcoded validation in the Spark executor's ingress layer.

## Proposed Fix
Make `path` optional in the executor and only pass it to `reader.load()` if present, or if the format is file-based (parquet, csv, etc.).

## Workaround
Add a dummy `path` field to the JDBC module configuration:
```yaml
    config:
      format: jdbc
      path: "none"  # Satisfy Aqueduct validator; Spark JDBC ignores this
      options:
        url: "..."
        dbtable: "..."
```
