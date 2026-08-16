# All Probe Signals

Demonstrates all 9 built-in probe signal types.

## Setup

```bash
pip install -r requirements.txt
```

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
| `execution_partitions` | Spark physical partition count (`df.rdd.getNumPartitions()`) — takes no config |
| `threshold` | SQL aggregate boolean `expr:` — emits `passed` for Regulator gating |

Results are stored in the `probe_signals` table in the observability store.

> **`probes:` global config:** Add a `probes:` block to `aqueduct.yml` to
> set sampling governance for every Probe in the run:
> ```yaml
> probes:
>   max_sample_rows: 100
>   default_sample_fraction: 0.1
> ```
> A signal's own `n:`/`fraction:` still overrides these per signal.

## How to Run

```bash
python populate_data.py

aqueduct run blueprint.yml
```

Query results:
```bash
duckdb .aqueduct/observability.db \
  "SELECT signal_name, estimate FROM probe_signals"
```
