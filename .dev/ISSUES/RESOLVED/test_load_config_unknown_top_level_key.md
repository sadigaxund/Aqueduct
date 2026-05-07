# Issue: test_load_config_unknown_top_level_key fails

### Module and Function
`aqueduct.config.load_config`

### Exact Error Message
```
AssertionError: Regex pattern did not match.
Expected regex: 'Config validation failed'
Actual message: '1 validation error in /tmp/pytest-of-sakhund/pytest-0/test_load_config_unknown_top_l0/extra.yml:
  • unknown_field — Extra inputs are not permitted'
```

### Context
The `_format_config_error` function in `aqueduct/config.py` was updated to provide more detailed error messages, but the tests were not updated to match the new format. The tests currently expect the literal string "Config validation failed" which no longer appears in the exception message.

### Affected Tests
- `tests/test_config.py::test_load_config_unknown_top_level_key`
- `tests/test_config.py::test_load_config_unknown_nested_key`
