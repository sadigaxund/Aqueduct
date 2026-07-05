# CSV Ingress Snippet

Demonstrates how to read CSV files with explicit options (headers, schema inference).

## How to Run

1. **Execute the Pipeline**:
   ```bash
   aqueduct run blueprint.yml
   ```

2. **Inspect Results**:
   ```bash
   python inspect_results.py
   ```

> **Other formats:** JSON (`format: json`), ORC (`format: orc`), and Avro
> (`format: avro`) use the same `options:` pattern. Replace the format key
> and set spark.jars or spark.packages if the format's Spark connector isn't
> bundled.

### Linear-edge sugar

When every module is a single-input/single-output type (Ingress, Channel,
Egress, Assert), the `edges:` block can be **omitted entirely** — the
compiler chains modules in declaration order:

```yaml
modules:
  - id: yellow_taxi_csv
    type: Ingress
    config: {format: csv, path: data/input/sample_data.csv, options: {header: "true", inferSchema: "true"}}
  - id: output_save
    type: Egress
    config: {format: parquet, path: data/output/output.parquet, mode: overwrite}
# No edges: block needed — compiler injects edges marked injected: true
```

If the Blueprint uses Junctions, Funnels, Probes (signal port), or
Regulators, edges must be explicit — those ports are ambiguous in a
flat chain.
