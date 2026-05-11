# Patch Commit Fails to Find Uncommitted Patches

## Tested Module
`aqueduct.cli` (`aqueduct patch commit`)

## Issue
The command `aqueduct patch commit` fails to locate applied patches because it assumes `applied_at` is stored under `_aq_meta` inside the patch file. 

However, `aqueduct.patch.apply.apply_patch_file` writes `applied_at` at the root of the JSON file (`raw_spec["applied_at"] = applied_at`). 

Because of this mismatch, `data.get("_aq_meta", {}).get("applied_at")` evaluates to `None`, causing `_uncommitted_applied_patches` to discard all applied patches. 

This results in `aqueduct patch commit` always printing "Nothing to commit — no applied patches since last git commit." and exiting with 0, meaning the commit message generation and `git commit` execution are bypassed.

## Stack Trace / Error
```
AssertionError: assert 'Fixing the bug' in ''
```
The test asserts that the `captured_msg` passed to `git commit -m` contains the expected text, but because `git commit` is never invoked, `captured_msg` remains an empty string.

## Context
Found when running `tests/test_cli/test_cli_patch.py::test_patch_commit_one_patch`.
