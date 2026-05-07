# CLI: Global CWD mutation in `run` command

## Description
The `aqueduct run` command calls `os.chdir(_project_root)` to resolve relative paths in blueprints. However, it does not restore the original working directory upon completion. 

When running tests via `CliRunner` (which by default runs in the same process), this causes subsequent tests that rely on relative paths to fixtures to fail if they are executed after a `run` command.

## Impact
- Test suite instability depending on execution order.
- Unexpected behavior if `aqueduct` is used as a library (though it is primarily a CLI).

## Recommendation
Use a context manager to temporarily change the CWD, or avoid global `os.chdir` in favor of absolute path resolution within the engine.
