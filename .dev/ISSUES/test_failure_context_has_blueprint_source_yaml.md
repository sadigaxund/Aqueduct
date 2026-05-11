# Issue: test_failure_context_has_blueprint_source_yaml

**Module/Function**: `aqueduct.surveyor.models.FailureContext`

**Error**:
```python
E       TypeError: FailureContext.__init__() got an unexpected keyword argument 'blueprint_source_yaml'
```

**Context**:
The `FailureContext` dataclass in `aqueduct/surveyor/models.py` lacks the `blueprint_source_yaml` attribute. This is required for threading the original uncompiled YAML text to the LLM for analysis as described in the prompt context features. Because this is a missing feature in application code, this test is failing and I am raising this issue per instructions rather than modifying the dataclass.
