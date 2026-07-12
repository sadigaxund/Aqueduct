# Custom Probe Signals

Demonstrates the `type: custom` probe signal — extend observability with your own
metrics without forking the engine.

## Three forms (pick exactly one per signal)

| Form | Config keys | Runs as | Use when |
|------|-------------|---------|----------|
| **Inline SQL** | `sql:` and/or `passed_when:` | a Spark expression | the metric is expressible in SQL (this snippet) |
| **Module pointer** | `module:` + `entry:` | trusted driver code | you have an importable package (mirrors UDFs) |
| **Entry-point plugin** | `plugin: <name>` | trusted driver code | you distribute the signal as an installable package |

### Inline SQL (used here)
```yaml
- type: custom
  sql: "percentile(price, 0.99)"   # arbitrary metric → "estimate"
  passed_when: "MAX(price) < 1000"  # optional boolean → "passed" (Regulator gate)
```
`passed_when` makes a custom signal usable as a **Regulator gate** exactly like
the built-in `threshold` signal — the Regulator reads the latest `passed` verdict.

### Callable forms
A pointer/plugin signal resolves to a callable with the contract:
```python
def p99_latency(df, sig_cfg) -> dict:
    return {"estimate": ..., "metadata": {...}, "passed": True}
```
Register it as an entry point in your package's `pyproject.toml`:
```toml
[project.entry-points."aqueduct.probe_signals"]
p99_latency = "myorg.aq_probes:p99_latency"
```

> **Security & cost:** callable forms run on the Spark **driver** as code — the
> same trust model as UDFs. The engine cannot enforce zero-cost observability for
> them, so a callable that does a full `.collect()`/`.count()` is your cost to
> own. The compiler emits a `custom_probe_driver_code` warning for pointer/plugin
> signals. Inline SQL does not trigger it.

## Setup

```bash
pip install -r requirements.txt
```

```bash
python populate_data.py
```

## How to Run
```bash
aqueduct run blueprint.yml
python inspect_results.py
```

Results land in `probe_signals` (signal_type = `custom`) in the default
per-pipeline `.aqueduct/observability/<blueprint_id>/observability.db`.
