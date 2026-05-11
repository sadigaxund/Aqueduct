# Issue: `aqueduct test` command crashes with ImportError due to renamed exception class

**Module/Function**: `aqueduct.cli` (the `test` sub-command, around line 1460)

**Error**:
```
ImportError: cannot import name 'TestError' from 'aqueduct.executor.spark.test_runner'
  (/home/sakhund/Personal/Projects/Aqueduct/aqueduct/executor/spark/test_runner.py)
```

**Stack Trace**:
```
aqueduct/cli.py:1460: in <module scope of test command>
    from aqueduct.executor.spark.test_runner import TestError, run_test_file
ImportError: cannot import name 'TestError' from 'aqueduct.executor.spark.test_runner'
```

**Context**:
The exception class in `aqueduct/executor/spark/test_runner.py` was renamed from `TestError` to `TestSchemaError` (line 78). However, `aqueduct/cli.py` still imports the old name `TestError` (line 1460) and catches it (line 1484). This causes all invocations of `aqueduct test` to fail immediately at import time with an `ImportError`, making the command completely non-functional.

**Affected tests** (all fail with the same `ImportError`):
- `tests/test_cli/test_cli_test.py::test_cli_test_pass`
- `tests/test_cli/test_cli_test.py::test_cli_test_fail`
- `tests/test_cli/test_cli_test.py::test_cli_test_bad_blueprint`
- `tests/test_cli/test_cli_test.py::test_cli_test_quiet`
- `tests/test_cli/test_cli_test.py::test_cli_test_blueprint_override`

**Fix required in application code** (`aqueduct/cli.py` line 1460 and 1484):
Change `TestError` → `TestSchemaError` in the import and the except clause.
