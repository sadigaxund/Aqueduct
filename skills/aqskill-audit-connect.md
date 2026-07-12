---
name: aqskill-audit-connect
description: Audit Aqueduct's executor (and any other legitimate pyspark call site) for APIs that are incompatible or degraded under Spark Connect (`spark.remote(...)`). Use when evaluating Spark Connect adoption, before adding a Connect deployment mode, or after touching aqueduct/executor/spark/**, aqueduct/patch/explain_gate.py, or aqueduct/doctor/**. Documents current behavior only — does not propose runtime fixes.
---

# Aqueduct Spark Connect Compat Audit

Extends `aqskill-audit-health.md` with detection for Spark-Connect-incompatible
APIs. Spark Connect (`SparkSession.builder.remote("sc://...")`) replaces the
classic driver-embedded `SparkContext`/py4j-gateway session with a thin gRPC
client. Anything that reaches into the JVM driver process directly —
`spark._jvm`, `spark._jsc`, `spark.sparkContext`, `df._jdf`, `df.rdd`,
`SparkListener` registration — has no meaning on the client side of a Connect
session and either raises `AttributeError`/`NotImplementedError` or silently
returns nothing, depending on whether pyspark itself intercepts the attribute.

This skill is **audit only**. It documents today's behavior for the
support-matrix in `docs/compatibility.md`; it does not modify executor code
or propose remediation timelines (those belong in `docs/roadmap.md` if ever
scheduled).

## ⚠️ Verify before you report (precision gate — read first)

1. **Read the cited `file:line` + surrounding function** — is the call reachable in a normal run, or only in a `--preflight`/opt-in path?
2. **Trace the exception path** — is the call wrapped in a local `try/except` that degrades gracefully, wrapped in an *outer* per-signal/per-check try/except several frames up, or entirely unguarded (propagates as an unhandled exception that aborts the module/run)? This determines the degrades-vs-breaks column — do not guess, read the call stack.
3. **Confirm the caller path is reachable under Connect at all** — some sites (e.g. `doctor --preflight`) only run when pyspark is classic-session; if the CLI/doctor already gates on session type, note that.
4. **Confirm still true at HEAD** — re-read current content, not a remembered audit.

## Detection commands

```bash
# Direct JVM/py4j gateway access — no equivalent in Connect's gRPC client
rg -n '_jvm\b|_jsc\b|py4j' aqueduct/executor/ aqueduct/patch/ aqueduct/doctor/

# SparkListener registration/callback-based metrics (listener runs in the JVM,
# unreachable from a Connect client process)
rg -n 'SparkListener|addSparkListener|StatusTracker' aqueduct/executor/ aqueduct/

# RDD-level API — DataFrame.rdd is not implemented on Connect DataFrames
rg -n '\.rdd\b' aqueduct/executor/

# Driver-side SparkContext handle — not exposed by a Connect SparkSession
rg -n 'sparkContext\b' aqueduct/executor/ aqueduct/doctor/

# Driver-side Hadoop FS access via the JVM (byte counts, path existence checks)
rg -n 'hadoopConfiguration|FileSystem\.get\(|_jsc\.hadoopConfiguration' aqueduct/executor/ aqueduct/doctor/

# Config access via the driver SparkContext's private _conf (vs spark.conf.get, which IS Connect-safe)
rg -n 'sparkContext\._conf' aqueduct/

# Physical-plan / explain internals reaching into the JVM query execution object
rg -n '_jdf\b|queryExecution\(\)|explainString\(' aqueduct/patch/ aqueduct/executor/
```

## What each hit class means under Connect

| Pattern | Why it breaks/degrades under Connect |
|---|---|
| `spark._jvm` | The Connect `SparkSession` has no `_jvm` attribute at all — there is no local JVM gateway; the client talks gRPC to a remote Spark Connect server. Accessing it raises `AttributeError`. |
| `spark._jsc` / `spark.sparkContext` | Connect's `SparkSession` does not expose a `SparkContext` handle (no driver-local JVM to point at). Accessing `.sparkContext` raises `PySparkNotImplementedError` (pyspark ≥3.5) or `AttributeError`. |
| `df.rdd` | The Connect `DataFrame` has no RDD-level materialization path — `.rdd` raises `NotImplementedError` ("RDD is not supported in Spark Connect"). |
| `df._jdf` | Connect DataFrames are plan proxies (protobuf `Relation`), not py4j-wrapped `Dataset` objects — there is no `_jdf`. |
| `SparkListener` registration | Listeners run inside the JVM driver process. A Connect client process has no driver JVM to attach a listener to; listener-based metrics never fire. |
| `hadoopConfiguration()` / `FileSystem.get()` via `_jvm` | Depends on `spark._jvm`/`_jsc`, so it inherits that failure — no way to reach the driver's Hadoop `Configuration` object from a Connect client. |
| `sparkContext._conf` | Depends on `sparkContext`. Use `spark.conf.get(...)` instead — that IS Connect-safe (goes through the gRPC config-get RPC), so this specific pattern has a direct fix if ever prioritized. |

## Known-legitimate exceptions (do not flag)

- **`DataFrame.observe()` / `pyspark.sql.Observation`** (`aqueduct/executor/spark/metrics.py::observe_df`) — this is the *Observation* API, not the SparkListener API. It has a Connect-native implementation (`pyspark.sql.connect.observation.Observation`) and works under Connect. Comments in this codebase referring to "SparkListener stage metrics" describe the *mechanism this Observation API replaced*, not what `observe_df` itself calls — do not conflate the two when auditing.
- **`logging.getLogger("py4j")`** (`session.py:31`) — a Python logging-namespace string, not a py4j API call. No JVM access.
- **`aqueduct/surveyor/error_extraction.py`** — lazily imports `py4j.protocol.Py4JJavaError` inside a guarded `try/except` purely to `isinstance`-check an already-raised exception for structured error extraction. Under Connect, this import still succeeds (py4j ships with pyspark) but the `isinstance` check simply never matches (Connect raises `SparkConnectGrpcException`, not `Py4JJavaError`) — falls through to the generic path. Degrades gracefully by design already; not a new finding.
- **`aqueduct/executor/spark/warnings/jar_availability.py`** module docstring mentioning `_jsc` — documents the *mechanism*, verify the actual call site separately (it is a real hit — see the degradation table in `docs/compatibility.md`).

## Reporting format

Follow `aqskill-audit-code.md`'s table style. Every row needs `file:line`,
the feature it powers, and a verified degrades/breaks/works classification —
never guess from the pattern name alone, trace the try/except chain.

```markdown
### Spark Connect degradation candidates
| file:line | feature | behavior under Connect | fallback / current mitigation |
```

The verified table for the current tree lives in `docs/compatibility.md`
under "Spark Connect". Re-run this skill and diff before trusting an old
table — executor code moves between phases.
