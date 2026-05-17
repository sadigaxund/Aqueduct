# Regulator with Polling Timeout

Demonstrates how to use a `Regulator` as a time-bound gate that waits for an external signal before automatically proceeding.

## Key Concept: Time-Bound Gates
A **Regulator** usually evaluates a signal (from a `Probe` or a manual override) and immediately decides whether to `open` or `close` the gate. 

By adding `timeout_seconds`, you transform the Regulator into a **polling gate**:
1. If the signal is `passed: True`, the gate opens immediately.
2. If the signal is `passed: False`, the engine **waits** and polls for changes.
3. If a "Go" signal (or manual override) arrives before the timeout, the pipeline proceeds immediately.
4. If no signal arrives within the timeout period, the gate **automatically opens** (timeout fallback).

### Use Cases
- **Manual Approval Fallback**: Wait 1 hour for a manager to approve a large transaction; if they don't respond, proceed with a "Warning" flag.
- **Asynchronous Data Arrival**: Wait for a slow external API to update its "ready" status in a cache.
- **Human-in-the-loop**: Pause the pipeline to allow a developer to inspect a `Probe` snapshot in the DuckDB store.

## How to Run

1. **Execute the Pipeline and Monitor Timing**:
   ```bash
   python inspect_results.py
   ```

## Configuration
In `blueprint.yml`:
```yaml
- id: gatekeeper
  type: Regulator
  config:
    timeout_seconds: 10
    on_block: skip
```

The `timeout_seconds` property triggers a polling loop in the Aqueduct executor.
