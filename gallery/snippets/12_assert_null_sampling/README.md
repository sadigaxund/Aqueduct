# Null Rate Sampling Assertion

Demonstrates how to perform performance-optimized data quality checks using **Sampling-based Assertions**.

## Key Concept: Null Rate Check
The `null_rate` assertion calculates the fraction of NULL values in a column. 

### Why Sample?
On multi-terabyte datasets, running a full `COUNT(*)` to check for NULLs can be extremely expensive. 
Aqueduct allows you to specify a `fraction` (e.g., `0.1` for 10%):
1. Spark takes a random sample of the data.
2. The assertion is evaluated only on that sample.
3. This provides a high-confidence statistical "canary" for data quality at a fraction of the cost.

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
- id: email_null_check
  type: null_rate
  column: email
#   max: 0.05      # Max allowed null rate (6%) ~ Passes
  max: 0.01      # Max allowed null rate (1%) ~ Fails
  fraction: 0.1  # Only scan 10% of the data
```
