## Planned Future Work

### UI / Frontend (Planned — do not implement yet)
- Pipeline graph view: nodes = modules, edges = data flow; colour by status
- Edge labels: row count + bytes from SparkListener metrics in `signals.db`
- Node tooltip: duration, schema snapshot, null rates, sample rows
- Lineage panel: click column → trace source across modules (from `lineage.db`)
- Data source: poll `runs.db` + `signals.db` + `lineage.db` (all DuckDB); or extend webhook for per-module push events
- Backend needed: thin read-only API over the three DuckDB files (FastAPI or similar)
- Prerequisite: SparkListener implementation (exact counts) + per-module webhook events

### Flink Engine (Planned — do not implement yet)
- Add `aqueduct/executor/flink/` subpackage from scratch (no code reuse from Spark executor — different execution model, different module semantics).
- Extend `pyproject.toml`: `flink = ["apache-flink>=1.18"]` extra.
- `get_executor("flink")` in `executor/__init__.py` already raises `NotImplementedError` as a placeholder.
- Module types that need re-evaluation for Flink: Junction (→ KeyedStream?), Funnel, Regulator (streaming watermarks), Egress mode (no overwrite concept for streaming sinks).
- Blueprint/Manifest schema is engine-agnostic; Flink-specific config goes in `deployment:` or per-module `flink_config:` block — do NOT add Spark-isms to the shared schema.
- Config: `deployment.engine: flink`, `deployment.master_url: "flink://host:8081"` (or jobmanager address).

---

## Open Questions / Discussions

3. What would be the best way of handling the iterations, that is if an llm couldn't fix the issue on first go, what should the system do? Should it try again with the same blueprint? Or should it try to fix it again with the already fixed blueprint? What kind of automation strategy should we use? having like a max attempts after which it would notify the user about the failure?

    Moreover, should we really let LLM patches let loose on production environments like that, is there a way to introduce a safe mode where it can reliably run and verify the patch, and only if it is safe and correct, it should be applied to the production environment? Some kind of preview run


6. prefabs, like currently in order to make anything we have to write blueprint and the pypspark/sql code of every module from zero, what if we have a set of predefined modules that we separate from main functionality, and use them like blocks to build a pipeline? How would it affect the current system and LLM's ability to detect and fix issues? Moreover, do we segregate them like we did with

7. do nyc demo, test spark master connectivity, and maybe go fix ScrapeTL and train Gemma

8. does doctor command also test LLM connectivity?

9. JOURNAL.md is now moved into CHANGELOG.md