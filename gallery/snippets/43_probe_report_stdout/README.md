# Probe `report: stdout` — Signal Results in the Terminal

Demonstrates the Probe `report: stdout` config option. When set, each
signal's result prints as dim `↳` note lines under the Probe's row in the
post-run summary.

## How to Run

```bash
aqueduct run blueprint.yml
```

You'll see note lines like:

```
  ✓ probe_count
   ↳ row_count_estimate: method=exact · estimate=10
  ✓ probe_schema
   ↳ schema_snapshot:
     order_id: int
     customer: string
     amount: double
     region: string
     status: string
     score: int
  ✓ probe_nulls
   ↳ null_rates:
     amount: 0.0
     score: 0.0
```

## How it works

- Add `report: stdout` to any Probe's config — each signal result then
  prints as a `↳` note in the terminal.
- Single-value payloads collapse onto one line
  (`row_count_estimate: method=exact · estimate=10`).
- Dict/tabular payloads expand to one indented line per entry.
- Output is capped at 10 lines unless `--verbose` (`-v`).
- Signals are still persisted to the observability store as before —
  `report: stdout` is purely additive.
- Sampling governance and `danger.allow_full_probe_actions` apply
  unchanged.
- Notes are never counted in the runtime warning roll-up (they are not
  warnings — they are informational).

```yaml
  - id: probe_count
    type: Probe
    attach_to: raw_data
    config:
      report: stdout
      signals:
        - type: row_count_estimate
          method: exact
```

## Signal types

| Signal | Payload shape | Example stdout line |
|--------|--------------|-------------------|
| `row_count_estimate` | scalar + method | `row_count_estimate: method=exact · estimate=10` |
| `schema_snapshot` | dict of `col: type` | Multi-line, one per column |
| `null_rates` | dict of `col: rate` | `null_rates: amount=0.0` |
| `sample_rows` | list of dicts | Up to n rows shown |
| `value_distribution` | dict with stats + percentiles | `value_distribution: columns=["amount"]` |
| `distinct_count` | dict of `col: count` | `distinct_count: region=4` |
