# Time Travel — Read at Version

Demonstrates `time_travel:` on Ingress — read a Delta (or Parquet) table
at a specific historical version or timestamp.

## How it works

```yaml
config:
  format: delta
  path: data/delta_events
  time_travel:
    version: 1          # Delta versionAsOf
    # OR
    timestamp: "2026-01-15 10:00:00"   # Delta/Iceberg timestampAsOf
```

`version` and `timestamp` are mutually exclusive. Time travel is supported
for `path:`-based reads only. For `table:`-addressed reads, use a Channel
with `TIMESTAMP AS OF` SQL syntax instead.

This snippet reads the same Delta table at two points: version 1 (2 rows)
and the latest version (4 rows).

## Setup

```bash
pip install -r requirements.txt
```

```bash
python populate_delta.py   # creates data/delta_events with 3 versions
```

## How to Run

```bash
aqueduct run blueprint.yml
python -c "import pandas as p; print('V1:', p.read_parquet('data/output/v1_snapshot.parquet')); print('CURRENT:', p.read_parquet('data/output/current_snapshot.parquet'))"
```

V1 has 2 rows; current has 4.
