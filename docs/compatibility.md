# Compatibility Matrix

The combinations below are what Aqueduct is tested against in CI. Anything outside this range is **not** supported — it may work, but will not get bug fixes, and version pin errors at install time are intentional.

## Python × Spark

| Feature | Python 3.11 | Python 3.12 | Python 3.13 |
|---|---|---|---|
| Core engine (Parser, Compiler, Executor, Surveyor) | ✅ | ✅ | ⚠ (cloudpickle monkeypatch required — see notes) |
| Spark 4.0 (`pyspark>=4.0,<5.0`) | ✅ | ✅ | ⚠ |
| Spark 3.x | ❌ — not supported, not tested | ❌ | ❌ |
| Delta Lake (`delta-spark>=4.0,<5.0`) | ✅ | ✅ | ⚠ |
| Iceberg / Hudi | planned (TODO Phase 45) | planned | planned |

## Notes

- **`pyspark>=4.0,<5.0`** is hard-pinned in `pyproject.toml`. Spark 3.x users get a clean `pip` resolution error instead of subtle runtime failures. Widening the range would require runtime version gates throughout the executor + a Spark 3.x CI lane — both deliberately out of scope.
- **Python 3.13 + PySpark 4.0** needs a system-installed `cloudpickle>=3.0`; the bundled cloudpickle 2.x in current PySpark recurses or segfaults during UDF serialization. Aqueduct monkeypatches at startup when both conditions hold. See `aqueduct/executor/spark/udf.py::_patch_pyspark_cloudpickle` — the patch self-deprecates once upstream PySpark ships cloudpickle ≥ 3.
- **LLM providers**: Anthropic (default) and any OpenAI-compatible endpoint (Ollama, vLLM, LM Studio, NVIDIA NIM, together.ai, Groq). Per-provider quirks documented in `gallery/aqscenarios/README.md`.
- **Stores**: DuckDB embedded (default), Postgres (`aqueduct-core[postgres]`), Redis (`aqueduct-core[redis]`). All tested against the matrix above.

## Production pinning

For deployments, pin transitively to lock the cloudpickle + jvm + Java + python combination:

```bash
pip install aqueduct-core[spark]==1.1.0
pip freeze > requirements.txt
```

Python ecosystem doesn't lock by default — that's a `pip` reality, not an Aqueduct constraint.

## Out of scope

- **Spark 3.x** — would require version gates + a CI lane; no concrete user demand.
- **Python 3.10 and earlier** — drops dataclass `kw_only`, several typing features we use.
- **Python 3.14** — not yet released; will be tested when it stabilises.
- **Flink executor** — aspirational, tracked in `TODOs.md` Deferred block.
