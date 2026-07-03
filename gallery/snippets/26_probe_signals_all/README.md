# All Probe Signals

Demonstrates all 8 built-in probe signal types.

## Signals

| Signal | What it captures |
|--------|------------------|
| `schema_snapshot` | Column names, types, nullable flags |
| `row_count_estimate` | Approximate count (metadata-based) |
| `null_rates` | NULL ratio per column |
| `sample_rows` | Row sample (configurable count) |
| `value_distribution` | Value frequency per column |
| `distinct_count` | Distinct value counts |
| `data_freshness` | MAX(timestamp) and age |
| `partition_stats` | Row counts per partition column value |

Results are stored in the `probe_signals` table in the observability store.

> **`probes:` global config:** Add a `probes:` block at the Blueprint root
> to set default sampling rate, seed, or signal overrides for all Probes
> at once:
> ```yaml
> probes:
>   sample_rows_count: 100
>   default_seed: 42
> ```
> Individual Probe modules can override these per signal.

## How to Run

```bash
aqueduct run blueprint.yml
```

Query results:
```bash
duckdb .aqueduct/observability.db \
  "SELECT signal_name, estimate FROM probe_signals"
```
