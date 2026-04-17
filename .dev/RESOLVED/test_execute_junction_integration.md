# test_execute_junction_integration

- **Module and function**: `executor.py` / `execute()`
- **Error message**: `aqueduct.executor.executor.ExecuteError: Module type 'Junction' (id='j_cond') is not supported.`
- **Context**: The `execute()` function lacks support for `Junction` modules, causing the orchestration to immediately raise an `ExecuteError`. The user specifically instructed not to fix `executor.py` and only write tests and `.dev/ISSUES`.

```text
tests/test_executor_orchestration.py:325: in test_execute_junction_integration
    result = execute(manifest, spark)
aqueduct/executor/executor.py:124: in execute
    raise ExecuteError(
aqueduct.executor.executor.ExecuteError: Module type 'Junction' (id='j_cond') is not supported. Supported: ['Channel', 'Egress', 'Ingress'].
```
