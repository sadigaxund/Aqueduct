# Issue: test_probe_does_not_halt_blueprint fails

## Module and function being tested
`tests/test_blueprints.py` -> `test_probe_does_not_halt_blueprint`

## Exact error message and stack trace
```
______________________ test_probe_does_not_halt_blueprint ______________________
tests/test_blueprints.py:158: in test_probe_does_not_halt_blueprint
    assert (store / "signals.db").exists()
E   AssertionError: assert False
E    +  where False = exists()
E    +    where exists = (PosixPath('/tmp/pytest-of-sakhund/pytest-0/test_probe_does_not_halt_bluep0/signals') / 'signals.db').exists
```

## Relevant context
The `Probe` module implementation was updated to use `obs.db` instead of `signals.db` as per the latest specs, but the test assertion was not updated to reflect this change.

## Proposed Fix
Change `assert (store / "signals.db").exists()` to `assert (store / "obs.db").exists()` in `tests/test_blueprints.py`.
