# test_execute_probe_global_exception

- **Module and function**: `probe.py` / `execute_probe()`
- **Error message**: `FileExistsError: [Errno 17] File exists: '/tmp/pytest-of.../store'`
- **Context**: The `execute_probe()` method states that "exception inside execute_probe does not propagate to caller." However, the exception wrapping does not include `store_dir.mkdir(parents=True, exist_ok=True)` when the directory might be created improperly and raise an OS exception natively. The `FileExistsError` propagates up and breaks the promise to the caller.

```text
tests/test_probe.py:171: in test_execute_probe_global_exception
    execute_probe(module, df, spark, "run-1", store_dir)
aqueduct/executor/probe.py:187: in execute_probe
    store_dir.mkdir(parents=True, exist_ok=True)
../../../.virtualenvs/aqueduct/lib/python3.14/pathlib.py:1189: in mkdir
    os.mkdir(self, mode)
E   FileExistsError: [Errno 17] File exists: '/tmp/pytest-of-sakhund/pytest-72/test_execute_probe_global_exce0/store'
```
