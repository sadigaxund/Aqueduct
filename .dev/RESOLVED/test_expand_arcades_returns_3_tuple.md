# ISSUE: test_expand_arcades_returns_3_tuple

## Module and function being tested
`aqueduct.compiler.expander.expand_arcades` (via `_expand_recursive`)

## Exact error message and stack trace
```
TypeError: can only concatenate list (not "tuple") to list
```

Stack trace:
```
  File "/home/sakhund/Personal/Projects/Aqueduct/tests/test_compiler/test_expander.py", line 129, in test_expand_arcades_returns_3_tuple
    result = expand_arcades(bp.modules, bp.edges, path.parent)
  File "/home/sakhund/Personal/Projects/Aqueduct/aqueduct/compiler/expander.py", line 185, in expand_arcades
    return _expand_recursive(modules, edges, base_dir, depth=0)
  File "/home/sakhund/Personal/Projects/Aqueduct/aqueduct/compiler/expander.py", line 274, in _expand_recursive
    for other_m in (result_modules + modules):
                    ~~~~~~~~~~~~~~~^~~~~~~~~
TypeError: can only concatenate list (not "tuple") to list
```

## Relevant context
The `Blueprint` model (in `aqueduct/parser/models.py`) defines `modules` and `edges` as `tuple`.
However, `expand_arcades` (and `_expand_recursive`) expect `list` and attempt to concatenate a `list` (`result_modules`) with the input `modules` (which is a `tuple` when passed from a parsed Blueprint).

## Recommended Fix
In `aqueduct/compiler/expander.py`, `expand_arcades` should convert its inputs to lists:
```python
def expand_arcades(
    modules: list[Module],
    edges: list[Edge],
    base_dir: Path,
) -> tuple[list[Module], list[Edge], dict]:
    return _expand_recursive(list(modules), list(edges), base_dir, depth=0)
```
