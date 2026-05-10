# Technical Report: CLI Global CWD Mutation Analysis

## Executive Summary
This report analyzes a critical side-effect in the `aqueduct run` command: the permanent mutation of the process-level Current Working Directory (CWD). While this mutation is intended to facilitate relative path resolution within Blueprints, the failure to restore the original CWD introduces instability into the test suite and violates best practices for CLI and library development.

## Problem Description
The `aqueduct run` command (defined in `aqueduct/cli.py`) identifies the "project root" by searching for `aqueduct.yml`. Once found, it executes:

```python
os.chdir(_project_root)
```

This change is **global** to the Python process. Because the Aqueduct test suite uses `click.testing.CliRunner`, which executes CLI commands in-process by default, any test that follows a `run` command will inherit the mutated CWD.

### Observed Impact
- **Test Flakiness**: Tests that rely on relative paths to fixtures (e.g., `tests/test_parser/test_config.py`) fail when executed after a CLI run because they are looking in the wrong directory.
- **Isolation Breach**: CLI commands should ideally be side-effect free regarding the environment that invokes them.

## Root Cause Analysis
The `run` command is designed to anchor execution to the project root so that Blueprint paths (e.g., `data/*.parquet`) are consistently resolved. This is a valid requirement. However, the implementation lacks a "cleanup" phase to revert the environment once the command finishes or fails.

## Recommended Solution
The recommended fix is to wrap the execution logic in a `try...finally` block to ensure restoration of the original state.

### Implementation Pattern
```python
def run(...):
    _old_cwd = os.getcwd()
    try:
        os.chdir(_project_root)
        # ... execution logic ...
    finally:
        os.chdir(_old_cwd)
```

### Benefits
1. **Test Reliability**: Ensures every test starts with a clean, predictable environment.
2. **Library Safety**: Protects potential library users from unexpected environment mutations.
3. **Robustness**: The `finally` block ensures restoration even if the command crashes or is interrupted.

## Current Status
The issue was identified during the stabilization of the aggressive healing test suite. While the immediate test failures have been bypassed by managing test execution order, the underlying architectural flaw remains in the source code. 

**Decision**: Per user instruction, no source code changes will be applied at this time. This report serves as the documentation of the defect and the verified path to resolution.
