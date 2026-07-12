# SQL Channel & Python UDF Snippet

Demonstrates how to extend Spark SQL with custom Python logic using **User Defined Functions (UDFs)**.

## Setup

```bash
pip install -r requirements.txt
```

## Key Features
- **SQL Channel**: Uses standard SQL syntax to transform data.
- **Python UDF**: Registers a native Python function (`logic.py`) that can be called directly within the SQL query.
- **Parameterized UDF**: A **factory pattern** — `entry` points to a factory, `params:` provides keyword arguments resolved at compile time (including `${ctx.*}` and `@aq.*` tokens).

## How to Run

1. **Generate test data**:
   ```bash
   python populate_data.py
   ```

2. **Execute the Pipeline**:
   ```bash
   aqueduct run blueprint.yml
   ```

3. **Inspect Results**:
   ```bash
   python inspect_results.py
   ```

## Anatomy of a UDF

### Static UDF
The UDF is registered in the `udf_registry`:
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
SELECT name, mask_sensitive(email) as email_masked FROM raw_users
```

### Parameterized UDF (Factory Pattern)
```yaml
udf_registry:
  - id: mask_phone
    lang: python
    module: logic
    entry: make_phone_masker    # factory function
    return_type: string
    params:                     # resolved at compile time, passed to the factory
      visible_digits: 4
      prefix: "+1-***-***-"
```

The factory `make_phone_masker(visible_digits=4, prefix="+1-***-***-")` returns a closure that masks phone numbers to show only the last 4 digits.

### Benefits of Parameterized UDFs

- **Reuse**: One factory powers multiple blueprints with different settings.
- **Secrets support**: `params:` values support `@aq.secret()`, so API keys, salts, and other config values never appear in the Blueprint as plain text.
- **Zero runtime overhead**: The factory is called once at compile time; the returned closure is the registered UDF.

> [!NOTE]
> When using Python UDFs, Aqueduct automatically issues a performance warning if the UDF is not vectorized. For high-volume production pipelines, consider using `pandas_udf` (Arrow-optimized).
