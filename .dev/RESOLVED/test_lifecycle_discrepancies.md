# Issue: Outdated Rollback Tests in test_phase9.py

## Description
The test suite in `tests/test_phase9.py` (Phase 13 tests) contains several failing tests for patch rollback.
These tests attempt to invoke `aqueduct patch rollback <patch_id>`, which does not exist in the current CLI.
The current implementation of rollback is at the top-level (`aqueduct rollback <blueprint> --to <patch_id>`) and is git-based, whereas the tests attempt to use a file-based `backups/` directory approach.

## Failing Tests
- `test_patch_rollback_restores_blueprint`
- `test_patch_rollback_moves_applied_to_rolled_back`
- `test_patch_rollback_record_contains_rolled_back_at`
- `test_patch_rollback_no_applied_record_still_restores_blueprint`

## Recommendation
The tests should be updated to use `aqueduct rollback` and verify git state, or a `patch rollback` alias should be added to the CLI to support the expected file-based workflow if that was intended as a fallback.

---

# Issue: Surveyor Exception Leak in evaluate_regulator

## Description
`test_surveyor_regulator_duckdb_exception` in `tests/test_surveyor_core.py` is failing.
The test purposefully corrupts `obs.db` and expects `surveyor.evaluate_regulator()` to catch the resulting `duckdb.IOException` and return `True` (Open gate) per spec.
However, the exception escapes the `try...except Exception:` block in `aqueduct/surveyor/surveyor.py`.

## Root Cause
Likely `duckdb.IOException` does not inherit from `Exception` in a way that is caught by the current block, or there is a nesting issue in the `try...finally` block involving the connection closure.

## Recommendation
Update `aqueduct/surveyor/surveyor.py` to catch `(Exception, duckdb.Error)` or specifically handle `duckdb.IOException` to ensure the "fail-open" regulator spec is honored.
