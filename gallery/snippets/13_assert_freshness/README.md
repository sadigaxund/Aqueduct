# Native Freshness Quarantine

Demonstrates Aqueduct's ability to natively handle data freshness as both a global health metric and a row-level filter.

## Setup

```bash
pip install -r requirements.txt
```

## Key Concept: Native Quarantine
Previously, `freshness` was a global "all-or-nothing" check. With the latest engine updates, the `freshness` rule now supports **Native Quarantine**:

- **Global View**: The engine still calculates `MAX(timestamp)` and logs a warning if the source is stale.
- **Row-Level View**: If `on_fail: quarantine` is set, the engine automatically identifies individual rows older than the threshold and diverts them to the `spillway`.

This removes the need for manual SQL expressions to clean up "stale" records.

## How to Run

1. **Generate test data with relative timestamps**:
   ```bash
   python populate_events.py
   ```

2. **Execute the Pipeline**:
   ```bash
   aqueduct run blueprint.yml
   ```

3. **Inspect Results**:
   ```bash
   python inspect_results.py
   ```

The output will show fresh records in the `main` stream and older records diverted to the `spillway`, all handled by a single rule in the `Assert` module.
