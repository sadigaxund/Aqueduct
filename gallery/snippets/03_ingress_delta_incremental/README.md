# Delta Lake Ingress Snippet

Demonstrates how to use Delta Lake's **Time Travel** and **Versioning** features in an Aqueduct pipeline.

## Requirements

- **Delta Lake JAR** matching your PySpark version. The `aqueduct.yml` defaults to Spark 4.0.x.
  If you have a different version, set env vars:

  ```bash
  # Spark 4.1.x
  DELTA_SPARK_VER=4.1 aqueduct run blueprint.yml

  # Future Spark 4.2+
  DELTA_SPARK_VER=4.2 DELTA_VER=4.3.0 aqueduct run blueprint.yml
  ```

  `populate_delta.py` auto-detects your Spark version, so just run it directly.

## Setup

Delta Lake tables have a special directory structure (`_delta_log`). You can't just use a single file; you need a proper Delta table.

1. **Populate the Table**:
   This script creates a Delta table with 3 versions (0, 1, and 2):
   ```bash
   python populate_delta.py
   ```

## How to Run

1. **Execute the Pipeline**:
   The blueprint is configured to jump back to **Version 2** (`versionAsOf: "2"`):
   ```bash
   aqueduct run blueprint.yml
   ```

2. **Inspect Results**:
   ```bash
   python inspect_results.py
   ```

## Key Delta Options

- `versionAsOf`: Read a specific numerical version of the table.
- `timestampAsOf`: Read the state of the table at a specific date/time.
- `ignoreChanges`: Skip processing of data changes (useful for streaming sources).
- `ignoreDeletes`: Skip processing of deleted rows.
