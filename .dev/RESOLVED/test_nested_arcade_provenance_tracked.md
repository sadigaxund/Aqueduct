# Expander Recursive Tuple Concatenation Bug

## Failing Tests
- `test_expand_arcades_returns_3_tuple`
- `test_nested_arcade_provenance_tracked`

## Module and Function
`aqueduct.compiler.expander.py` (line 274, `_expand_recursive`)

## Error Detail
The tests for nested arcades and the 3-tuple return value of `expand_arcades` are failing due to a `TypeError` in `_expand_recursive`. 

```
aqueduct/compiler/expander.py:274: in _expand_recursive
    for other_m in (result_modules + modules):
                    ^^^^^^^^^^^^^^^^^^^^^^^^
E   TypeError: can only concatenate list (not "tuple") to list
```

The error indicates that `result_modules` is incorrectly being returned or initialized as a tuple rather than a list in recursive calls, causing list concatenation to crash when evaluating nested arcades.

## Context
These tests were written to verify that nested arcades correctly track provenance at both levels and that the expander returns a 3-tuple (modules, edges, provenance_dict). As requested, the application code was not modified to fix this test failure.
