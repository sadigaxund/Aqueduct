# Issue: FailureContext lacks blueprint_source_yaml field during surveyor.record() when blueprint_path is None

**Module/Function**: `aqueduct.surveyor.surveyor` (Surveyor.record)

**Error**:
```
TypeError: FailureContext.__init__() got an unexpected keyword argument 'blueprint_source_yaml'
```

**Context**:
This is related to the missing `blueprint_source_yaml` field in `FailureContext`. When `surveyor.record()` is called and `self._blueprint_path` is `None`, it should explicitly set `blueprint_source_yaml=None` when creating the `FailureContext`. The `FailureContext` dataclass currently rejects this kwarg.

**Affected tests**:
- `tests/test_surveyor/test_surveyor.py::TestSurveyorBlueprintSourceYaml::test_surveyor_sets_blueprint_source_yaml_none_when_file_missing`

**Fix required in application code**:
1. Add `blueprint_source_yaml: str | None = None` to `FailureContext` in `aqueduct/surveyor/models.py`.
2. Add it to `FailureContext.to_dict()`.
3. In `Surveyor.record()`, pass `blueprint_source_yaml=None` if `_blueprint_path` is missing.
