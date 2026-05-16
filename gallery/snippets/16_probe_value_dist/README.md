# Value Distribution Probing

Demonstrates how to capture statistical distributions (percentiles, mean, stddev) for numeric data using the `Probe` module.

## Key Concept: Probes
A **Probe** is a non-invasive module that "taps" into a data stream.
- It does **not** modify the data.
- It runs alongside the main pipeline.
- It writes results to a separate observability store (DuckDB).

### Why use Value Distributions?
- **Detect Skew**: See if your prices or scores are drifting away from expected ranges.
- **Outlier Detection**: High StdDev or unexpected 95th percentiles can signal data quality issues.
- **Reporting**: Automatically capture aggregate stats for stakeholders without writing custom SQL aggregations in your Egress logic.

## Setup

1. **Generate Test Data**:
   ```bash
   python populate_data.py
   ```

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
- id: price_probe
  type: Probe
  attach_to: raw_products
  config:
    signals:
      - type: value_distribution
        columns: [price]
        fraction: 0.2  # 20% sample
        percentiles: [0.25, 0.5, 0.75, 0.95]
```

Results are stored in the default per-pipeline `.aqueduct/observability/<blueprint_id>/observability.db` (DuckDB format).
