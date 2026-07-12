# Partition IO — partition_filters + partition_by

Demonstrates writing partitioned data and reading back only the
partitions you need.

## Setup

```bash
pip install -r requirements.txt
```

## What it shows

- `partition_by: ["month"]` on Egress — writes data into
  `data/output/partitioned_txns/month=2024-01/`, `month=2024-02/`, etc.
- `partition_filters: {region: ["US"]}` on Ingress — reads only the US
  rows across all month partitions (Spark partition pruning skips
  irrelevant files)

## Run

```bash
# Write the partitioned dataset + read back US-only rows
python populate_data.py

aqueduct run blueprint.yml

# Inspect the partition layout
find data/output/partitioned_txns -type d
# data/output/partitioned_txns/month=2024-01/
# data/output/partitioned_txns/month=2024-02/
# data/output/partitioned_txns/month=2024-03/
# data/output/partitioned_txns/month=2024-04/
```
