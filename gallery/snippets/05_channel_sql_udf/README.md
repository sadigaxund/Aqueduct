# SQL Channel & Python UDF Snippet

Demonstrates how to extend Spark SQL with custom Python logic using **User Defined Functions (UDFs)**.

## Key Features
- **SQL Channel**: Uses standard SQL syntax to transform data.
- **Python UDF**: Registers a native Python function (`logic.py`) that can be called directly within the SQL query.
- **Hybrid Execution**: Combines the performance of SQL with the flexibility of Python.


## How to Run

1. **Execute the Pipeline**:
   ```bash
   aqueduct run blueprint.yml
   ```

2. **Inspect Results**:
   ```bash
   python inspect_results.py
   ```

## Anatomy of a UDF

In `blueprint.yml`, the UDF is registered in the `udf_registry`:
```yaml
udf_registry:
  - id: mask_sensitive
    lang: python
    module: logic
    entry: mask_email
    return_type: string
```

It is then called like a built-in function in the SQL query:
```sql
SELECT 
  name,
  mask_sensitive(email) as email_masked
FROM raw_users
```

> [!NOTE]
> When using Python UDFs, Aqueduct automatically issues a performance warning if the UDF is not vectorized. For high-volume production pipelines, consider using `pandas_udf` (Arrow-optimized).

> **Parameterized UDF factories:** Set `params:` on a UDF registration to
> create reusable UDF instances (e.g. `mask_email(params.suffix)` with
> different suffixes per call site). See `docs/specs.md` §4.4.2.
