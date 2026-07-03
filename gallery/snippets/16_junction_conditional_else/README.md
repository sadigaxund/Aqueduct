# Junction Modes Comparison

Compares all three Junction modes — **conditional**, **broadcast**, and
**partition** — from the same input data.

| Mode | Behavior | Output |
|---|---|---|
| **conditional** | Each row goes to exactly one port based on a predicate — `active` (NEW/PENDING) or `other` (else). | `active_tickets.parquet` (4 rows), `other_tickets.parquet` (2 rows) |
| **broadcast** | Every row is copied to every port. | `broadcast_A.parquet`, `broadcast_B.parquet` (6 rows each, identical) |
| **partition** | Rows are routed by matching `partition_key` column value — `status=NEW` to port 0, `status=PENDING` to port 1. Unmatched rows are dropped. | `partition_A.parquet` (2 rows, NEW), `partition_B.parquet` (2 rows, PENDING) |

## How to Run

```bash
aqueduct run blueprint.yml
python inspect_results.py
```
