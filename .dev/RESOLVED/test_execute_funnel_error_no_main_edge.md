# test_execute_funnel_error_no_main_edge

- **Module and function**: `executor.py` / `execute()`
- **Error message**: `aqueduct.executor.executor.ExecuteError: Module type 'Funnel' (id='f1') is not supported.`
- **Context**: The `execute()` function lacks support for `Funnel` modules.

```text
tests/test_executor_orchestration.py:413: in test_execute_funnel_error_no_main_edge
    result = execute(manifest, spark)
aqueduct/executor/executor.py:124: in execute
    raise ExecuteError(
aqueduct.executor.executor.ExecuteError: Module type 'Funnel' (id='f1') is not supported. Supported: ['Channel', 'Egress', 'Ingress'].
```
