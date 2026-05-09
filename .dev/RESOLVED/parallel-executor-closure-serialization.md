# ISSUE: Parallel Executor Closure Serialization Failure in Pytest

## Description
When running `execute(parallel=True)`, the engine uses `concurrent.futures.ThreadPoolExecutor` to run independent DAG components. The worker function `_run_component` is a nested closure defined inside `execute()`.

In a `pytest` environment (especially with coverage or detailed failure reporting), this nested closure triggers an infinite recursion during serialization/pickling:
`E when serializing function reconstructor`
`E when serializing function object`
...

This occurs even if the test logic itself is correct, as long as `ThreadPoolExecutor` is invoked. It does not appear to happen when running the engine directly via CLI (e.g., `aqueduct doctor` which uses a similar pattern).

## Impact
- Parallel execution cannot be verified via standard `pytest` suites without disabling reporting/coverage tools that trigger this serialization.
- Potential instability in environments that use distributed tracing or debugging tools that rely on pickling task arguments.

## Rationale
Nested functions (closures) are not natively pickleable in Python. While `ThreadPoolExecutor` on Linux/macOS usually uses threads and avoids pickling, many observability tools (including `pytest` failure decorators and `coverage.py`) may attempt to inspect or serialize the task callable, leading to this crash.

## Recommended Fix
Refactor `_run_component` into a standalone top-level function in `aqueduct/executor/spark/executor.py` and pass necessary state (frame_store, manifest, etc.) explicitly as arguments.
