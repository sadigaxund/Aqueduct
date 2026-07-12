# Java UDF Reference & Parallel Mode

Demonstrates two features:

1. **Java UDF reference** — wiring a JVM-language UDF in the Blueprint
2. **Parallel execution** — disconnected DAG components run concurrently

## Setup

```bash
pip install -r requirements.txt
```

## Java UDF Contract

```yaml
udfs:
  - name: category_label
    lang: java
    class_name: com.aqueduct.udfs.CategoryLabel
    jar_path: udf/jars/aqueduct-udfs.jar
```

The JAR is not shipped with this snippet — you must compile and provide it.
The UDF implements `org.apache.spark.sql.api.java.UDF1[String, String]`.

To enable the Java UDF, uncomment the `udfs:` block and change the query to use `category_label(category)`.

## Parallel Mode

With `--parallel`, the two disconnected components run simultaneously
(products + customers), reducing wall-clock time.

## How to Run

```bash
# Runs out of the box — uses a simple column alias instead of the JAR'd UDF:
python populate_data.py

aqueduct run blueprint.yml

# Inspect both outputs:
python3 inspect_results.py
```
