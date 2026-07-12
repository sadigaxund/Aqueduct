# CSV Ingress Snippet

Demonstrates how to read CSV files with explicit options (headers, schema inference).

## Setup

```bash
pip install -r requirements.txt
```

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

> **Other formats:** JSON (`format: json`), ORC (`format: orc`), and Avro
> (`format: avro`) use the same `options:` pattern. Replace the format key
> and set spark.jars or spark.packages if the format's Spark connector isn't
> bundled.