# 01 — Channel Dedup (`aqueduct test`)

Isolated-module testing: assert a single Channel's logic against inline
rows. No Ingress, no Egress, no external sources, no fixture files —
Spark builds the input DataFrame from the `rows` you list.

## Files

| File | Role |
|---|---|
| `blueprint.yml` | `raw_users` → `dedup_latest` (Channel) → `output_save`. |
| `blueprint.aqtest.yml` | Feeds `dedup_latest` three inline rows, asserts only the latest survives. |
| `data/input/users.csv` | Same data on disk so `aqueduct run` works too (optional). |

## Run

```bash
cd gallery/aqtests/01_channel_dedup
aqueduct test blueprint.aqtest.yml
```

Expected: `1 tests | 1 passed | 0 failed`, with each assertion `✓`.

Run the full pipeline instead:

```bash
aqueduct run blueprint.yml
```

## What it teaches

- `aqueduct test` exercises **one module** in isolation — `module:` picks
  it, `inputs:` keys are the **upstream module ids** it reads.
- `inputs.<id>.schema` uses simple type names (`long`, `string`,
  `double`, `timestamp`, …); `rows` are positional lists.
- Assertion types: `row_count` (`expected:`), `contains`
  (`rows:` — each listed row must be found), `sql` (`expr:` against the
  `__output__` view; truthy result passes).
- Testable module types: Channel, Junction, Funnel, Assert. Ingress and
  Egress are never executed by `aqueduct test`.
- Always runs on `local[*]` — `deployment.master_url` is ignored (unit
  test, not integration). `--master` overrides only if a module needs
  cluster runtime.
