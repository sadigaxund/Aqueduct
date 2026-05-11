# Issue: test_patch_filename_seq

**Module and function**: `aqueduct.surveyor.llm._patch_filename`

**Exact error message**:
```
E       AssertionError: assert False
E        +  where False = <built-in method startswith of str object at 0x...>()
E        +    where <built-in method startswith of str object at 0x...> = '20260511T005636_test.json'.startswith
```

**Context**:
The `_patch_filename` function currently generates filenames with the format `{ts}_{slug}.json`, but the specification requires `{seq:05d}_{ts}_{slug}.json`. The sequence number should be the count of all `.json` files across `pending/`, `applied/`, and `rejected/` plus one. This prevents sequential tracking and violates the Phase 18 patch lifecycle requirements.
