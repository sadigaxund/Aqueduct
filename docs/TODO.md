## Open Questions / Discussions

---

7. do nyc demo, test spark master connectivity, and maybe go fix ScrapeTL and train Gemma

8. When at Phase where we do LLM vector DB thing, also discuss possibility of editing custom prompts to the LLM. 

9. Update SPARK_GUIDE.md with notes from book.

10. regarding nyc example, was last trying to make llm work, couldnt because of above.

11. Prove that this claim stands true: "What context_override does: at expansion time, the compiler merges the parent's context with the arcade's context_override. So ${ctx.input_table} inside the arcade resolves to whatever the parent passed — not a global. Each use of the same Arcade in the same Blueprint gets its own context values.


---
### Might or Might nots

16. build a benchmark where there are bunch of faulty blueprints, and we try to fix them using LLM, and see how well it does. make it as realistic as possible. try to make blueprints with various different types of errors.

17. aqueduct docs

18. Flink Engine:
- Add `aqueduct/executor/flink/` subpackage from scratch (no code reuse from Spark executor — different execution model, different module semantics).
- Extend `pyproject.toml`: `flink = ["apache-flink>=1.18"]` extra.
- `get_executor("flink")` in `executor/__init__.py` already raises `NotImplementedError` as a placeholder.
- Module types that need re-evaluation for Flink: Junction (→ KeyedStream?), Funnel, Regulator (streaming watermarks), Egress mode (no overwrite concept for streaming sinks).
- Blueprint/Manifest schema is engine-agnostic; Flink-specific config goes in `deployment:` or per-module `flink_config:` block — do NOT add Spark-isms to the shared schema.
- Config: `deployment.engine: flink`, `deployment.master_url: "flink://<SPARK_MASTER>"` (or jobmanager address).

---

### Definite Future Work

18. think about interactive TUI style selection feature, where for commands like lineage, report or patch, if you don't specify something it would justlist possible options

19. Streaming, Machine Learning / MLOps
---

### Running Thoughts/Notes

17. okay so I have change a format from 'parquet' option to 'csv' to simulate a faulty scenario. If you look at the module you can deduct that within path the file extension is .parquet but then it must be format = parquet, since format = csv caused an error in processing. However, I believe the error message itself might have confused an LLM, because Spark was actually able to decode parquet files as csv. Therefore, an issue (which was guessed to be a column name mismatch) propogated into the next sql module, and finally the below patch was generated, however I couldn't apply it, see the output below:

----------- GENERATED PATCH ----------
{
"patch_id": "fix-green-taxi-path",
"run_id": null,
"rationale": "The SQL query in 'green_trips_prepared' has a unresolved column reference 'VendorID'. Correct it by setti>
"operations": [
{
"op": "set_module_config_key",
"module_id": "green_trips_prepared",
"key": "query",
"value": "SELECT \nVendorID,\nlpep_pickup_datetime AS pickup_datetime,\nlpep_dropoff_datetime AS dropoff_date>
}
],
"_aq_meta": {
"run_id": "586675f3-6989-457a-a147-4dbf2ae4e4a6",
"blueprint_id": "nyc_taxi_demo",
"failed_module": "green_trips_prepared",
"staged_at": "2026-05-02T23:45:37.219217+00:00"
}
}
----------- COMMAND OUTPUT ----------
aqueduct patch apply patches/pending/00002_20260502T234537_fix-green-taxi-path.json --blueprint blueprints/NYC_Taxi_Demo.yml 
✗ patch failed: Patched Blueprint is invalid (PatchSpec operations produced a Blueprint that does not pass the Parser):
Module config resolution failed: Undefined context reference: ${ctx.tables.green_taxi_trips}



12. Update CHANGELOG.md with latest changes, you move completed tasks from TODOs.md there.


