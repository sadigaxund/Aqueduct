# Funnel — Union All, Union (Dedup), Zip

Compares all three Funnel merge modes from the same two inputs.

## Modes

| Mode | Behaviour | Row count (this data) |
|------|-----------|----------------------|
| **union_all** | Appends all rows — duplicates preserved | 7 (3 + 4) |
| **union** | Concatenates, dropping duplicate rows | 6 (order_id 1003 deduped) |
| **zip** | Interleaves rows by position, truncated to shortest input | 3 (min of 3 and 4) |

## What this snippet shows

- Two CSV Ingress sources (3 rows + 4 rows, overlapping `order_id 1003`)
- Three parallel Funnels, each fed from the same two inputs
- Each Funnel uses a different merge mode
- Downstream Channels aggregate results per product
- inspect_results.py shows the row count difference between modes

## How to Run

```bash
aqueduct run blueprint.yml
python inspect_results.py
```
