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
10. with doctor command do we do this: "After you have defined the connection properties, you can test your connection to the database
itself to ensure that it is functional. This is an excellent troubleshooting technique to confirm that
your database is available to (at the very least) the Spark driver" Example:

```java
import java.sql.DriverManager
val connection = DriverManager.getConnection(url)
connection.isClosed()
connection.close()
```



16. I have tried to use the 'lineage' cli command. But not sure, why it fails: 

>>> aqueduct run blueprint.yml 
▶ nyc_taxi_demo  (1 modules)  run=905873af-1830-46b8-aea5-2163bcacae12  engine=spark  master=local[*]
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
26/05/01 22:10:33 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
  ✓ yellow_taxi_trips                                                           

✓ pipeline complete  run_id=905873af-1830-46b8-aea5-2163bcacae12

>>> aqueduct lineage blueprint.yml 
✗ lineage.db not found at .aqueduct/signals/lineage.db





17. regarding 'report' cli command, it requires user to pass run_id, should we create some kind list command to see all the run_ids? or too confusing, or is useful? should user keep track of the run_ids themselves?




18. when approval_mode = human, does it ignore 'max_patches_per_run' config, since it has to be 1 right? did we specify this in docs anywhere as a note or warning. However i ran with such settings and see such logs: 'LLM self-healing (1/3)\nLLM API call failed (attempt 1/3): timed out\nLLM API call failed (attempt 1/3): timed out' . Moreover, when 'max_patches_per_run' unspecified the default is 5, is there a reason?




19. I have created a very simple, you can say the simplest ever case of failure as a test to see if llm would work, which i purposefully tried to read '*.parqut' files instead of '*.parquet' files, but all I see is these logs:

Module 'yellow_taxi_trips': attempt 1/1 failed ([yellow_taxi_trips] source not found or unreadable at 'data/yellow//*.parqut': [PATH_NOT_FOUND] Path does not exist: file:/home/sakhund/Personal/Projects/Aqueduct/examples/nyc_taxi_demo/data/yellow/*.parqut.); giving up
  ↻ LLM self-healing (1/5)  failed_module=yellow_taxi_trips
LLM API call failed (attempt 1/3): timed out
LLM agent failed to produce a valid PatchSpec after 3 attempts for pipeline 'nyc_taxi_demo' run '64852305-3369-4371-bc6f-3ab0b192ef58'
  ✗ LLM: failed to generate valid patch, stopping
  ✗ yellow_taxi_trips  — [yellow_taxi_trips] source not found or unreadable at 'data/yellow//*.parqut': [PATH_NOT_FOUND] Path does not exist: file:/home/sakhund/Personal/Projects/Aqueduct/examples/nyc_taxi_demo/data/yellow/*.parqut.

✗ pipeline failed  run_id=64852305-3369-4371-bc6f-3ab0b192ef58  failed_module=yellow_taxi_trips

20. from previous example I see 3 different retry mechanisms, one is: 'LLM API call failed (attempt 1/3): timed out', then 2. 'LLM agent failed to produce a valid PatchSpec after 3 attempts for pipeline ...', then 3. 'LLM self-healing (1/5)  failed_module=...' , can you make it more coherent, make it more understandable and clear. Can use set/configure all of them? If yes, where can i see reference/documentation on that?