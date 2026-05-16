# Depot Incremental Snippet

Demonstrates **watermark-based incremental processing** backed by the **Depot**
KV store — process only new rows each run, no streaming engine required.

## Key Concept

The `new_events` Channel sets `materialize: incremental` with
`watermark_column: event_ts`. Each run:

1. The engine reads the last persisted `MAX(event_ts)` from the Depot.
2. It injects that value as `${ctx._watermark}` (first run uses the sentinel
   `1900-01-01 00:00:00`, so the first run is a full scan).
3. After the run succeeds, the new `MAX(event_ts)` is written back to the Depot.

Downstream Egress uses `mode: append` so each run adds only the new slice.

## How to Run

1. **First run** (full scan — all 5 rows):
   ```bash
   aqueduct run blueprint.yml
   python inspect_results.py
   ```

2. **Append new rows** to `data/input/events.csv` with a later `event_ts`, then
   **run again**. Only the rows past the stored watermark are processed and
   appended — the first 5 are not reprocessed.

The watermark lives in `.aqueduct/depot.db` (DuckDB, auto-created, git-ignored).
Delete `.aqueduct/` and `data/output/` to reset the demo.

## Related

`@aq.depot.get("key")` reads arbitrary Depot state at **compile time**, and an
`Egress` with `format: depot` writes KV state at **runtime** — the same store
that powers the watermark here.
