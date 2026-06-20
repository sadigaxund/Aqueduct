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
- **Airflow**: `aqueduct-core[airflow]` (requires `apache-airflow>=2.7`). Tested against Python 3.11 + 3.12 only (Airflow's own compatibility ceiling). Provides `AqueductOperator`, `AqueductPatchSensor`, and `AqueductPatchTrigger` — lazy-imported from `aqueduct.integrations.airflow`. DAG import does not pull pyspark.
- **Secrets providers**: `env` (built-in, no SDK needed), `aws` (`aqueduct-core[aws]`), `gcp` (`aqueduct-core[gcp]`), `azure` (`aqueduct-core[azure]`). All tested against the matrix above.

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
