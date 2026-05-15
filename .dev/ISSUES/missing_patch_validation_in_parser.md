# ISSUE: Missing `patch_validation` field in `aqueduct/parser/parser.py`

## Description
The `patch_validation` field was added to the Pydantic `AgentSchema` in `aqueduct/parser/schema.py` (Phase 29a), but it was not added to the `AgentConfig` conversion logic in `aqueduct/parser/parser.py`. This causes the field to be ignored when parsing a Blueprint, even if it is explicitly set in the YAML.

## Impact
Blueprint-level overrides for `agent.patch_validation` are non-functional. The engine always falls back to the global default from `aqueduct.yml`.

## Evidence
`aqueduct/parser/parser.py`:
```python
170:     agent = AgentConfig(
171:         approval_mode=validated.agent.approval_mode,
172:         on_pending_patches=validated.agent.on_pending_patches,
173:         model=validated.agent.model,
174:         aggressive_max_patches=validated.agent.aggressive_max_patches,
175:         provider=validated.agent.provider,
176:         base_url=validated.agent.base_url,
177:         provider_options=validated.agent.provider_options,
178:         guardrails=GuardrailsConfig(
179:             forbidden_ops=tuple(validated.agent.guardrails.forbidden_ops),
180:             allowed_paths=tuple(validated.agent.guardrails.allowed_paths),
181:             heal_on_errors=tuple(validated.agent.guardrails.heal_on_errors),
182:             never_heal_errors=tuple(validated.agent.guardrails.never_heal_errors),
183:         ),
184:         prompt_context=validated.agent.prompt_context,
185:         llm_timeout=validated.agent.llm_timeout,
186:         llm_max_reprompts=validated.agent.llm_max_reprompts,
187:         confidence_threshold=validated.agent.confidence_threshold,
188:         on_heal_failure=validated.agent.on_heal_failure,
189:     )
```
Missing `patch_validation=validated.agent.patch_validation`.

## Proposed Fix
Update `aqueduct/parser/parser.py` to include the field:
```python
        on_heal_failure=validated.agent.on_heal_failure,
        patch_validation=validated.agent.patch_validation,
    )
```
