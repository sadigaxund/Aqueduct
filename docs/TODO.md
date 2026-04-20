## Planned Future Work

### Flink Engine (Planned — do not implement yet)
- Add `aqueduct/executor/flink/` subpackage from scratch (no code reuse from Spark executor — different execution model, different module semantics).
- Extend `pyproject.toml`: `flink = ["apache-flink>=1.18"]` extra.
- `get_executor("flink")` in `executor/__init__.py` already raises `NotImplementedError` as a placeholder.
- Module types that need re-evaluation for Flink: Junction (→ KeyedStream?), Funnel, Regulator (streaming watermarks), Egress mode (no overwrite concept for streaming sinks).
- Blueprint/Manifest schema is engine-agnostic; Flink-specific config goes in `deployment:` or per-module `flink_config:` block — do NOT add Spark-isms to the shared schema.
- Config: `deployment.engine: flink`, `deployment.master_url: "flink://host:8081"` (or jobmanager address).

---

## Open Questions / Discussions

1. Adapt towards frontend > Discuss
2. Update TESTING.md and make it up to date, moreover, handle the ❌s
3. What would be the best way of handling the iterations, that is if an llm couldn't fix the issue on first go, what should the system do? Should it try again with the same blueprint? Or should it try to fix it again with the already fixed blueprint? What kind of automation strategy should we use? having like a max attempts after which it would notify the user about the failure?

    Moreover, should we really let LLM patches let loose on production environments like that, is there a way to introduce a safe mode where it can reliably run and verify the patch, and only if it is safe and correct, it should be applied to the production environment? Some kind of preview run


4. is depot the same as kv store, but then what is context registry/store. im getting confused. I know that we have a static expressions that we can define at the header to avoid magic values, then there is persistent values like last executed date or whatever, and then UDFs (I know we  can use them in a SQL query, but can we inject a spark UDF into expression?), then there is aq.secret, and aq.depot?  More questions, can we have like a deeper schema for these like: ctx.schema1.schema2.some_key and is it applied for all the stores? Moreover, explain where can each of them be resolved, or they are all can be resolved anywhere like within blueprint, sql, expressions, etc.
