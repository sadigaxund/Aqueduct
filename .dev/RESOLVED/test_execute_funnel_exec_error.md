# test_execute_funnel_exec_error

- **Module and function**: `executor.py` / `execute()`
- **Error message**: `aqueduct.executor.executor.ExecuteError: Module type 'Funnel' (id='f1') is not supported.`
- **Context**: The `execute()` function lacks support for `Funnel` modules.

```text
tests/test_executor_orchestration.py:435: in test_execute_funnel_exec_error
    result = execute(manifest, spark)
aqueduct/executor/executor.py:124: in execute
    raise ExecuteError(
aqueduct.executor.executor.ExecuteError: Module type 'Funnel' (id='f1') is not supported. Supported: ['Channel', 'Egress', 'Ingress'].
```
