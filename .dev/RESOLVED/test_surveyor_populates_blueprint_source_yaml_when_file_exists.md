# Issue: FailureContext lacks blueprint_source_yaml field during surveyor.record()

**Module/Function**: `aqueduct.surveyor.surveyor` (Surveyor.record)

**Error**:
```
TypeError: FailureContext.__init__() got an unexpected keyword argument 'blueprint_source_yaml'
```

**Context**:
The LLM patching agent requires the original `blueprint_source_yaml` to be present in the `FailureContext` (populated by `Surveyor.record()`) so it can be injected into the user prompt when `provenance_json` is not available or as supplementary context. The `FailureContext` dataclass in `models.py` does not currently declare `blueprint_source_yaml`, and `Surveyor.record()` does not attempt to read the blueprint file and set this field. 

**Affected tests**:
- `tests/test_surveyor/test_surveyor.py::TestSurveyorBlueprintSourceYaml::test_surveyor_populates_blueprint_source_yaml_when_file_exists`

**Fix required in application code**:
1. Add `blueprint_source_yaml: str | None = None` to `FailureContext` in `aqueduct/surveyor/models.py`.
2. Add it to `FailureContext.to_dict()`.
3. In `Surveyor.record()`, read `self._blueprint_path` and pass its content to `FailureContext(..., blueprint_source_yaml=...)`.
