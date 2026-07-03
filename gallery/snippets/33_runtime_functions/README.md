# Runtime Functions — @aq.run.* and @aq.blueprint.*

Demonstrates Aqueduct's built-in runtime functions — resolved at compile time.

## Available functions

| Call | Returns |
|------|---------|
| `@aq.run.id()` | The current run's UUID |
| `@aq.run.prev_id()` | Previous run's UUID (from depot key `_last_run_id`) |
| `@aq.run.timestamp()` | ISO-8601 UTC timestamp of the run |
| `@aq.blueprint.id()` | The blueprint's `id:` field |
| `@aq.blueprint.name()` | The blueprint's `name:` field |
| `@aq.blueprint.path()` | Filesystem path to the blueprint file |
| `@aq.blueprint.dir()` | Directory containing the blueprint file |
| `@aq.deployment.env()` | Deployment environment string |
| `@aq.deployment.target()` | Deployment target string |
| `@aq.version()` | Engine version string |
| `@aq.date.today()` | Today's date (`YYYY-MM-DD`) |
| `@aq.date.now()` | Current timestamp (`YYYY-MM-DD HH:MM:SS`) |

**Resolution scope:** These functions resolve **only during Blueprint
compilation** (not in `aqueduct.yml`). They work in any string config position
— SQL queries, paths, probe configs, etc.

**How to Run**

```bash
aqueduct run blueprint.yml
python -c "import pandas; print(pandas.read_parquet('data/output/items.parquet'))"
```

Each run produces a different `run_uuid` and `run_timestamp`.
