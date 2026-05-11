# Issue: test_llm_system_prompt_includes_template_expressions_rule

**Module and function**: `aqueduct.surveyor.llm._build_system_prompt`

**Exact error message**:
```
E       assert 'using template expressions' in 'You are an expert Apache Spark blueprint repair agent for the Aqueduct blueprint engine.\n\nA blueprint has failed. Y... or "action")\n\nRespond with ONLY valid JSON. No prose, no markdown fences, no explanation outside the JSON object.\n'
```

**Context**:
The `_SYSTEM_PROMPT_TEMPLATE` in `aqueduct/surveyor/llm.py` does not contain the required critical rule about using template expressions instead of resolved literal paths.
