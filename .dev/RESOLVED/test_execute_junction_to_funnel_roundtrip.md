# test_execute_junction_to_funnel_roundtrip

- **Module and function**: `executor.py` / `execute()`
- **Error message**: `aqueduct.executor.executor.ExecuteError: Module type 'Junction' (id='j1') is not supported.`
- **Context**: The `execute()` function lacks support for `Junction` modules, so the round-trip junction->funnel integration test fails immediately.

```text
tests/test_executor_orchestration.py:469: in test_execute_junction_to_funnel_roundtrip
    result = execute(manifest, spark)
aqueduct/executor/executor.py:124: in execute
    raise ExecuteError(
aqueduct.executor.executor.ExecuteError: Module type 'Junction' (id='j1') is not supported. Supported: ['Channel', 'Egress', 'Ingress'].
```
