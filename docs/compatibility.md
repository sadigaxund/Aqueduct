# Compatibility Matrix

The combinations below are what Aqueduct is tested against in CI. Anything outside this range is **not** supported — it may work, but will not get bug fixes, and version pin errors at install time are intentional.

<!-- COMPAT_RESULTS_START -->
## Latest run

| Combo | Python | Spark | Delta | Postgres | Java | Status |
|---|---|---|---|---|---|---|
| LTS | 3.11 | 4.1.2 | 4.1.0 | 17.10 | 17 | ✅ |
| Latest | 3.13 | 4.1.2 | 4.2.0 | 18.4 | 21 | ✅ |
| Legacy | 3.12 | 3.5.8 | 3.3.0 | 17.10 | 17 | ✅ |
<!-- COMPAT_RESULTS_END -->

## Python × Spark

| Feature | Python 3.11 | Python 3.12 | Python 3.13 |
|---|---|---|---|
| Core engine (Parser, Compiler, Executor, Surveyor) | ✅ | ✅ | ⚠ (cloudpickle monkeypatch required — see notes) |
| Spark 4.0 (`pyspark>=4.0,<5.0`) | ✅ | ✅ | ⚠ |
| Spark 3.x (`pyspark==3.5.8`) | ⚠ — tested via Legacy CI combo | ⚠ — tested via Legacy CI combo | ❌ |
| Delta Lake (`delta-spark>=4.0,<5.0` for Spark 4.x; `delta-spark==3.3.0` for Spark 3.5) | ✅ | ✅ | ⚠ |
| Custom Python DataSource (`format: custom`) | ✅ | ✅ | ⚠ |
| Iceberg / Hudi | planned | planned | planned |

## Notes

- **`pyspark>=4.0,<5.0`** is hard-pinned in `pyproject.toml`. The Legacy CI combo installs PySpark 3.5.8 explicitly by bypassing the pin — this is safe for testing but not recommended for production, where the pin is intentional. Widening the range would require runtime version gates throughout the executor — deliberately out of scope.
- **Custom Python DataSource (`format: custom`) needs Spark 4.0+** — it uses the `spark.dataSource` registry introduced in Spark 4.0. Since the supported floor is already 4.0+ this affects no supported install; on the unsupported Legacy 3.5 lane the engine raises a clear `RuntimeError` (a per-feature capability gate, not a hard requirement bump). All other features run across the full supported matrix.
- **Python 3.13 + PySpark 4.0** needs a system-installed `cloudpickle>=3.0`; the bundled cloudpickle 2.x in current PySpark recurses or segfaults during UDF serialization. Aqueduct monkeypatches at startup when both conditions hold. See `aqueduct/executor/spark/udf.py::_patch_pyspark_cloudpickle` — the patch self-deprecates once upstream PySpark ships cloudpickle ≥ 3.
- **LLM providers**: Anthropic (default) and any OpenAI-compatible endpoint (Ollama, vLLM, LM Studio, NVIDIA NIM, together.ai, Groq). Per-provider quirks documented in `gallery/aqscenarios/README.md`.
- **Stores**: DuckDB embedded (default), Postgres (`aqueduct-core[postgres]`), Redis (`aqueduct-core[redis]`). All tested against the matrix above.
- **Remote-submit targets**: Databricks Jobs API (`aqueduct-core[databricks]` — optional `databricks-sdk>=0.30`; the submitter works with raw `httpx` as well). EMR / Dataproc deferred. See the [Production Guide](production_guide.md) for per-target setup.
- **Airflow**: `aqueduct-core[airflow]` (requires `apache-airflow>=2.7`). Tested against Python 3.11 + 3.12 only (Airflow's own compatibility ceiling). Provides `AqueductOperator`, `AqueductPatchSensor`, and `AqueductPatchTrigger` — lazy-imported from `aqueduct.integrations.airflow`. DAG import does not pull pyspark.
- **Secrets providers**: `env` (built-in, no SDK needed), `aws` (`aqueduct-core[aws]`), `gcp` (`aqueduct-core[gcp]`), `azure` (`aqueduct-core[azure]`). All tested against the matrix above.

## Spark Connect

Aqueduct is **not tested or supported against `spark.remote(...)` (Spark
Connect) sessions.** The engine assumes a classic driver-embedded
`SparkSession` with a local JVM gateway (py4j). The table below documents
today's *actual* behavior if a Connect session were substituted — produced by
running `skills/aqskill-audit-connect.md`'s detection commands against the
tree and tracing each hit's exception path. This is a snapshot of current
incompatibilities, not a roadmap; no Connect support is scheduled (see
`docs/roadmap.md` if that changes).

| Call site | Feature it powers | Behavior under Connect | Fallback / current mitigation |
|---|---|---|---|
| `aqueduct/executor/spark/channel.py:330` (`_apply_metrics_boundary`) | `metrics_boundary: true` Channel config — forces a `repartition()` boundary so `SparkListener`-derived stage metrics attribute correctly per-Channel | **Breaks.** `df.rdd.getNumPartitions()` raises on a Connect `DataFrame` (`.rdd` is not implemented in Connect); the call is unguarded at this call site and propagates as an unhandled exception, failing the module | None today — opt-in config, only reachable when a Blueprint sets `metrics_boundary: true` |
| `aqueduct/executor/spark/probe.py:426` (`_partition_stats`) | `type: partition_stats` Probe signal | **Degrades.** Same `df.rdd.getNumPartitions()` call, but the dispatch loop in `execute_probe` wraps every signal in a per-signal `try/except` (probe.py:629) — the signal is skipped, a `runtime_probe_signal_error` warning fires, and the pipeline continues | Graceful — signal silently omitted, warning surfaced |
| `aqueduct/executor/spark/metrics.py:101-104` (`_hadoop_fs_bytes`) | Byte-count metrics (`bytes_read`/`bytes_written`) for cloud/HDFS paths (s3a://, gs://, hdfs://, etc.) | **Degrades.** `spark._jvm` / `spark._jsc.hadoopConfiguration()` raise `AttributeError` on a Connect session; the whole function body is wrapped in `try/except Exception: return None` | Graceful — byte-count metrics come back `None` (not collected) rather than 0; no crash |
| `aqueduct/patch/explain_gate.py:50-66` (`_formatted_plan`, Gate 4) | Post-patch physical-plan regression detection (`Exchange`/`BroadcastExchange`/`BatchEvalPython` node counts vs. baseline) | **Degrades to a false signal.** `df._jdf` does not exist on a Connect `DataFrame`; both the primary path and its own fallback (`df._jdf.queryExecution().toString()`) raise, caught by the outer `try/except`, returning `""`. `_count_markers("")` yields `(0, 0, 0)` for every module — Gate 4 would compare a real baseline against all-zero "after" counts, which reads as a large *regression* rather than "plan unavailable." Gate 4 is warn-only by default (`agent.block_on_explain_regression` gates whether this blocks) | None — this is the one site where "degrades" is worse than "no data": it produces spurious regression warnings, not silence |
| `aqueduct/executor/spark/warnings/jar_availability.py:34-41` (`_loaded_jar_names`) | `jar_availability` compiler/session-startup warning (missing JDBC/Kafka/Delta/Iceberg/Hudi driver JARs) | **Degrades silently (false negative).** `spark.sparkContext._jsc` raises on Connect (`sparkContext` itself is not exposed by a Connect `SparkSession`); wrapped in `except Exception: return []`, so the function reports "no JARs loaded" and the warning never fires — even when a required driver really is missing | None — the check becomes a permanent no-op under Connect, not a crash |
| `aqueduct/doctor/__init__.py:825-841` (cloud-URI `--preflight` check) | `aqueduct doctor --preflight` existence check for `s3a://`/`gs://`/`abfss://` Ingress/Egress paths, reusing Spark's own Hadoop `FileSystem` credentials | **Degrades gracefully.** `spark._jvm` / `_jsc.hadoopConfiguration()` raise; caught by the function's own `except Exception as exc: return CheckResult(name, "warn", ...)` — doctor reports a `warn`, not a crash | Graceful — surfaces as a doctor warning with the exception text |

**Works today, verified not a false positive:**

- `DataFrame.observe()` / `pyspark.sql.Observation` (`aqueduct/executor/spark/metrics.py::observe_df`, used for `records_written` row counts) — this is the Connect-native Observation API, not `SparkListener`. Comments elsewhere in the codebase describing "SparkListener stage metrics" refer to the mechanism this API replaced for zero-cost row counting, not to a live listener registration — there is no `addSparkListener` call anywhere in the tree (verified by grep).
- `aqueduct/surveyor/error_extraction.py`'s lazy `py4j.protocol.Py4JJavaError` import — the import succeeds under Connect (py4j still ships with pyspark) but the `isinstance` check simply never matches a `SparkConnectGrpcException`; already falls through to the generic error path by design.

**Summary:** 6 verified call sites reach into JVM/py4j-only internals; 4 degrade gracefully (existing try/except already absorbs the failure), 1 produces a misleading result (explain-plan Gate 4 reads as false regressions rather than "unavailable"), and 1 breaks outright (opt-in `metrics_boundary` Channel config, unguarded). None were found to silently corrupt data — the failure modes are either a clean skip, a `None`/`warn` result, or an unhandled exception that aborts the affected module/gate (surfaced through the normal failure path, not swallowed past detection).

## Production pinning

For deployments, pin transitively to lock the cloudpickle + jvm + Java + python combination:

```bash
pip install aqueduct-core[spark]==1.2.2
pip freeze > requirements.txt
```

Python ecosystem doesn't lock by default — that's a `pip` reality, not an Aqueduct constraint.

## Out of scope

- **Spark 3.x** — tested via the Legacy CI combo (PySpark 3.5.8 + Delta 3.3.0) but not officially supported. The `pyproject.toml` pin remains `>=4.0,<5.0`; the Legacy lane is a compatibility signal only.
- **Python 3.10 and earlier** — drops dataclass `kw_only`, several typing features we use.
- **Python 3.14** — not yet released; will be tested when it stabilises.
- **Flink executor** — aspirational, tracked in `TODOs.md` Deferred block.
