# Issue: test_patch_reject_no_patches_dir_uses_cwd_walk_up

## Module
aqueduct.cli.patch_reject

## Error Message
```
AssertionError: assert 1 == 0
```
Command output showed `✗ reject failed: Patch 'P123' not found`.

## Context
When `--patches-dir` is not set, the `patch reject` command defaults to `Path("patches")` instead of using the `_patches_root_from_blueprint` walk-up logic that other commands use. This means it fails to find the patch if the current working directory is not the project root. This violates the requirement that all patch commands use the same root derivation logic when `--patches-dir` is not set.
