# Compiler Checkpoint Identification Bug

## Failing Tests
- `test_incremental_channel_no_checkpoint_warns`
- `test_incremental_channel_with_checkpoint_no_warn`
- `test_channel_multi_consumer_with_checkpoint_no_warn`

## Module and Function
`aqueduct.compiler.compiler.py` (lines 220 and 287)
`aqueduct.compiler.compiler.py` (lines 240-243)

## Error Detail
The compiler is emitting warnings for multi-consumer channels and incremental channels regarding redundant dataset scans, but it fails to recognize when an upstream Checkpoint is present. 

The bug is caused by this code in `aqueduct/compiler/compiler.py`:

```python
# Line 220 & 287
checkpoint_ids = {m.id for m in modules if m.type == "Checkpoint"}

# Line 240
upstream_types = {um.type for um in modules if um.id in upstream_ids}
if "Checkpoint" not in upstream_types:
```

Aqueduct modules use a boolean property `checkpoint: true` rather than a dedicated module type `"Checkpoint"`. Because `"Checkpoint"` is not a valid module type according to the Pydantic `Blueprint` schema, no module will ever be identified as a checkpoint, and the warnings will incorrectly fire.

Additionally, `test_incremental_channel_no_checkpoint_warns` fails because the `pytest.warns` regex fails to match the `UserWarning` message due to newlines not being matched by the standard `.*` regex matcher.

## Context
These tests were introduced to track compiler warning performance gotchas as specified in `SPARK_GUIDE.md`. The user explicitly requested not to edit the application code at this phase to resolve the test failures.
