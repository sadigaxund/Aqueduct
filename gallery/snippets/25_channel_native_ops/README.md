# Native Channel Ops

Demonstrates native Channel operations — no SQL, no UDFs.

## Operations shown

| Op | What it does |
|----|--------------|
| `select` | Keep only specified columns |
| `rename` | Rename `salary` → `annual_comp` |
| `cast` | Change `annual_comp` type to `double` |
| `sort` | Sort descending by `age` |

Native ops are declarative — Spark translates them to optimized physical
operations without SQL parsing overhead.

### `metrics_boundary: true`

Add `metrics_boundary: true` to any Channel config to force a Spark stage
boundary after the op. This gives you accurate per-stage metrics in the Spark
UI (at the cost of an extra shuffle):

```yaml
- id: sorted_output
  type: Channel
  config:
    op: sort
    by: [{column: age, order: desc}]
    metrics_boundary: true
```

Useful when you need to measure a single op's I/O cost in isolation.

> **`op: join`** — native join op (inner, left, semi, anti) is also available
> on Channel. Unlike the SQL approach, the join key columns are declared
> declaratively and Spark optimizes them the same way. See `docs/specs.md` §4.4.

## How to Run

```bash
aqueduct run blueprint.yml
```
