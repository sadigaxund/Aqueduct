# Funnel Coalesce (Fallback Pattern)

Demonstrates how to merge multiple DataFrames and fill data gaps using the `coalesce` mode in a Funnel.

## Key Concept: Coalesce Merge
The `Funnel` module in `coalesce` mode performs a row-aligned merge across multiple inputs:
1. It assigns a synthetic row ID to each input DataFrame.
2. It joins them based on this row ID.
3. For columns that exist in multiple inputs, it uses Spark's `F.coalesce()` to pick the first non-NULL value found from left-to-right (in the order listed in `inputs`).

### Use Cases
- **Master Data Management**: Filling missing customer details from a secondary CRM.
- **Sensor Fusion**: Using a backup sensor reading when the primary sensor returns a NULL or error value.
- **Cache Fallback**: Preferring fresh API results but falling back to cached local data for missing records.

## How to Run

1. **Execute the Pipeline**:
   ```bash
   aqueduct run blueprint.yml
   ```

2. **Inspect Results**:
   ```bash
   python inspect_results.py
   ```

## Configuration
In `blueprint.yml`:
```yaml
- id: merge_sources
  type: Funnel
  config:
    mode: coalesce
    inputs:
      - ingress_primary   # Preferred source
      - ingress_fallback  # Secondary source (fills gaps)
```

**Note**: This mode requires the DataFrames to have the same row count and be logically aligned by index.
