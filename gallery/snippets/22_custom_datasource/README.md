# Custom Python DataSource

Read from a user-defined `pyspark.sql.datasource.DataSource` via `format: custom`.

> **Requires Spark 4.0+** — the Python DataSource API (`spark.dataSource`
> registry) does not exist on earlier Spark. On older Spark the engine raises a
> clear error.

## How it works
`datasource.py` defines `MyDataSource` (a `DataSource` subclass). The blueprint
points at it by fully-qualified name:

```yaml
- id: src
  type: Ingress
  config:
    format: custom
    class: datasource.MyDataSource   # importable; never an inline code body
```

At run time Aqueduct imports the class, verifies it subclasses `DataSource`,
registers it with the session, and reads via its `name()`. `aqueduct doctor`
verifies importability up front. Custom DataSources also work on Egress
(`format: custom` + `class:`); path is optional (the sink may carry its target
in `options`).

## How to Run
```bash
aqueduct doctor blueprint.yml   # verifies the class imports
aqueduct run blueprint.yml
```
The module must be on Python's ``sys.path`` at import time. Since the
``aqueduct`` CLI entry point doesn't auto-add CWD (only ``python -c`` /
``python script.py`` do), the blueprint's ``spark_config`` declares
``spark.submit.pyFiles: datasource.py`` so Spark distributes it to the
driver's Python path. If you add more importable files, append them as a
comma-separated list.
