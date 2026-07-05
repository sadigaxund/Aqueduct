# Cycle Detection — DAG Edges & Hook Chains

Two kinds of cycle detection: **compile-time DAG edge cycles** and
**runtime hook chain cycles**.

```
44_dag_cycle_detection/
├── aqueduct.yml          # shared config (local[*])
├── data/nodes.csv         # input for all blueprints
├── dag/
│   ├── blueprint.yml      # DAG edge cycle → compile error
│   └── blueprint_fixed.yml# cycle removed → runs clean
├── hook/
│   ├── hook_cycle_a.yml   # chains to B on success
│   └── hook_cycle_b.yml   # chains back to A → cycle guard
└── README.md
```

## 1. DAG Edge Cycle (compile time)

### What to run

```bash
# See the cycle error (no Spark started):
aqueduct compile dag/blueprint.yml

# Run the fixed acyclic version:
aqueduct run dag/blueprint_fixed.yml
```

### What to expect

`aqueduct compile dag/blueprint.yml` exits immediately with:

```
✗ Cycle detected in module graph. Involved modules:
  ['source_a', 'transform_b', 'transform_c', 'output']
```

The cycle: `source_a → transform_b → transform_c → source_a` (C feeds
back to A). No Spark session starts — Kahn's algorithm catches it at
parse time.

`aqueduct run dag/blueprint_fixed.yml` runs all 4 modules clean:

```
✓ source_a    9.5 s
✓ transform_b 950 ms
✓ transform_c 123 ms
✓ output      3 rows · 3.4 s
✓ blueprint complete
```

---

## 2. Hook Chain Cycle (runtime)

### What to run

```bash
aqueduct run hook/hook_cycle_a.yml
```

### What to expect

1. **hook_cycle_a** runs its pipeline (2 modules, 3 rows).  
   `on_success` fires → chains to `hook_cycle_b.yml`.

2. **hook_cycle_b** runs its pipeline (2 modules, 3 rows).  
   `on_success` fires → tries to chain back to `hook_cycle_a.yml`.

3. **Cycle guard** checks `AQUEDUCT_HOOK_CHAIN` — finds `hook_cycle_a`
   already in the chain. **Skips the hook** with a warning. No infinite
   loop, exit code 0.

```
▶ hook_cycle_a  ·  2 modules  ·  run xxx
  ✓ source   7.6 s
  ✓ output   3 rows
· hooks · on_success (1)
  ▶ hook_cycle_b  ·  2 modules  ·  run yyy
    ✓ source   7.4 s
    ✓ output   3 rows
  · hooks · on_success (1)
  ⚠ [hook_cycle] blueprint hook skipped — hook_cycle_a.yml
    is already in the hook chain (would loop forever)
  ✓ aqueduct run hook_cycle_b.yml   27.2 s
✓ run complete
```

Key: the second `blueprint:` hook is never executed — the guard
(`_chain_paths()` + `_MAX_DEPTH=8`) prevents infinite recursion.
