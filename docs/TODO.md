## Open Questions / Discussions

---

### Flink Engine
- Add `aqueduct/executor/flink/` subpackage from scratch (no code reuse from Spark executor — different execution model, different module semantics).
- Extend `pyproject.toml`: `flink = ["apache-flink>=1.18"]` extra.
- `get_executor("flink")` in `executor/__init__.py` already raises `NotImplementedError` as a placeholder.
- Module types that need re-evaluation for Flink: Junction (→ KeyedStream?), Funnel, Regulator (streaming watermarks), Egress mode (no overwrite concept for streaming sinks).
- Blueprint/Manifest schema is engine-agnostic; Flink-specific config goes in `deployment:` or per-module `flink_config:` block — do NOT add Spark-isms to the shared schema.
- Config: `deployment.engine: flink`, `deployment.master_url: "flink://<SPARK_MASTER>"` (or jobmanager address).

---

7. do nyc demo, test spark master connectivity, and maybe go fix ScrapeTL and train Gemma

8. When at Phase where we do LLM vector DB thing, also discuss possibility of editing custom prompts to the LLM. 
9. Update SPARK_GUIDE.md with notes from book.
10. regarding nyc example, was last trying to make llm work, couldnt because of above.

11. Prove that this claim stands true: "What context_override does: at expansion time, the compiler merges the parent's context with the arcade's context_override. So ${ctx.input_table} inside the arcade resolves to whatever the parent passed — not a global. Each use of the same Arcade in the same Blueprint gets its own context values.

12. Update CHANGELOG.md with latest changes, you move completed tasks from TODOs.md there.

---



