# test_execute_junction_error_no_main_edge

- **Module and function**: `executor.py` / `execute()`
- **Error message**: `aqueduct.executor.executor.ExecuteError: Module type 'Junction' (id='j1') is not supported.`
- **Context**: The `execute()` function lacks support for `Junction` modules, causing it to fail validation on `_SUPPORTED_TYPES`.

```text
tests/test_executor_orchestration.py:342: in test_execute_junction_error_no_main_edge
    result = execute(manifest, spark)
aqueduct/executor/executor.py:124: in execute
    raise ExecuteError(
aqueduct.executor.executor.ExecuteError: Module type 'Junction' (id='j1') is not supported. Supported: ['Channel', 'Egress', 'Ingress'].
```
