# Issue: config-default-model-mismatch

## Module and function being tested
`aqueduct/config.py` - `AqueductConfig` (Pydantic model defaults)

## Exact error message and stack trace
```
E   AssertionError: assert 'claude-3-5-sonnet-latest' == 'claude-sonnet-4-6'
E     
E     - claude-sonnet-4-6
E     + claude-3-5-sonnet-latest
```

## Relevant Context
The source code in `aqueduct/config.py` defines the default model as `claude-3-5-sonnet-latest` (Line 137), but the test `test_config_defaults` in `tests/test_config.py` and the `TESTING.md` manifest both expect `claude-sonnet-4-6`.

## Proposed Fix
Update `tests/test_config.py` and `.dev/TESTING.md` to expect `claude-3-5-sonnet-latest`, or revert the change in `aqueduct/config.py` if `claude-sonnet-4-6` was intended.
