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

3. [FOR FRONTEND] What would be the best way of handling the iterations, that is if an llm couldn't fix the issue on first go, what should the system do? Should it try again with the same blueprint? Or should it try to fix it again with the already fixed blueprint? What kind of automation strategy should we use? having like a max attempts after which it would notify the user about the failure?

Moreover, should we really let LLM patches let loose on production environments like that, is there a way to introduce a safe mode where it can reliably run and verify the patch, and only if it is safe and correct, it should be applied to the production environment? Some kind of preview run


6. [FOR FRONTEND] prefabs, like currently in order to make anything we have to write blueprint and the pypspark/sql code of every module from zero, what if we have a set of predefined modules that we separate from main functionality, and use them like blocks to build a pipeline? How would it affect the current system and LLM's ability to detect and fix issues? Moreover, do we segregate them like we did with

7. do nyc demo, test spark master connectivity, and maybe go fix ScrapeTL and train Gemma

8. When at Phase where we do LLM vector DB thing, also discuss possibility of editing custom prompts to the LLM. 
9. Update SPARK_GUIDE.md with notes from book.
10. regarding nyc example, was last trying to make llm work, couldnt because of above.


11. Prove that this claim stands true: "What context_override does: at expansion time, the compiler merges the parent's context with the arcade's context_override. So ${ctx.input_table} inside the arcade resolves to whatever the parent passed — not a global. Each use of the same Arcade in the same Blueprint gets its own context values.

12. Update CHANGELOG.md with latest changes, you move completed tasks from TODOs.md there.


fix: anchor run CWD to project root; add doctor --blueprint source check
- 'aqueduct run' now resolves all CLI paths to absolute, then os.chdir to
the project root (dir containing aqueduct.yml, found by walking up from
the blueprint file). Relative paths in Blueprint YAML resolve consistently
regardless of where the CLI is invoked.
- 'aqueduct doctor --blueprint <file>' probes every Ingress/Egress source:
local path existence, parent-dir writability for Egress, TCP socket probe
(3s) for JDBC endpoints. Cloud URIs skipped (covered by storage check).
Path resolution uses the same project-root walk-up logic as run.