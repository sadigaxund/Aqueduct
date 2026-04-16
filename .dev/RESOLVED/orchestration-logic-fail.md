# Issue: Orchestrator Logic and Exceptions

## Summary
The Executor orchestrator is failing to raise `ExecuteError` for several negative test cases (cycles, unsupported types, missing edges). Additionally, a `TypeError` is occurring in Ingress error reporting.

## Failing Tests
- `tests/test_executor_orchestration.py`: 
    - `test_execute_unsupported_module_type` (Expected ExecuteError, none raised)
    - `test_execute_ingress_error` (TypeError in IngressError instantiation)
    - `test_execute_missing_upstream_edge` (Expected ExecuteError, none raised)
    - `test_execute_cycle_in_manifest` (Expected ExecuteError, none raised)

## Error Details
Example TypeError:
```
TypeError: IngressError.__init__() missing 1 required positional argument: 'message'
```

## Context
These failures indicate logic gaps in `aqueduct/executor/executor.py` where validation checks are being bypassed or exceptions are being swallowed/misconfigured.
