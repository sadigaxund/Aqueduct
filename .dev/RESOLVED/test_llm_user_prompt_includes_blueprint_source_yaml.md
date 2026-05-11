# Issue: test_llm_user_prompt_includes_blueprint_source_yaml

**Module and function**: `aqueduct.surveyor.llm._build_user_prompt`

**Exact error message**:
```
TypeError: FailureContext.__init__() got an unexpected keyword argument 'blueprint_source_yaml'
```

**Context**:
The `FailureContext` dataclass in `aqueduct/surveyor/models.py` does not include a `blueprint_source_yaml` field. Consequently, `FailureContext` fails to initialize when passed this parameter, breaking `_build_user_prompt` which is expected to include the "Original Blueprint YAML" section when `blueprint_source_yaml` is present.
