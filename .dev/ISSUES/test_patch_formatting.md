# Issue: Patch YAML Formatting and Round-trip

**Module/Function**: `aqueduct.patch.apply.apply_patch_file` / `apply_patch_to_dict`

**Error**:
```
AssertionError: assert '\n  - id: m2\n' in ...
AssertionError: assert 'value' in ...
AssertionError: assert "SELECT 'x' as col" in ...
```

**Context**:
The patch application layer does not perfectly preserve or construct correct `ruamel.yaml` ASTs. Specifically:
1. List indentation is rendered without offset (e.g., `- item` instead of `  - item`).
2. `insert_module` and `replace_module_config` do not wrap injected strings in proper quote preservation types (`ruamel.yaml.scalarstring.DoubleQuotedScalarString`).
3. These structural flaws cause tests to fail and could break downstream user workflows when patches are generated via LLM and applied to blueprints.

As per instructions, this is a missing feature in the application code, so it is being logged as an issue rather than fixed directly.
