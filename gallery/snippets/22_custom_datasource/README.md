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
The module must be importable — running from this directory puts `datasource.py`
on the path.
