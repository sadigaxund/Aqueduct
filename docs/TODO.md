## Open Questions / Discussions

---

7. do nyc demo, test spark master connectivity, and maybe go fix ScrapeTL and train Gemma

8. When at Phase where we do LLM vector DB thing, also discuss possibility of editing custom prompts to the LLM. 

9. Update SPARK_GUIDE.md with notes from book.

10. regarding nyc example, was last trying to make llm work, couldnt because of above.

11. Prove that this claim stands true: "What context_override does: at expansion time, the compiler merges the parent's context with the arcade's context_override. So ${ctx.input_table} inside the arcade resolves to whatever the parent passed - not a global. Each use of the same Arcade in the same Blueprint gets its own context values.


---
### Might or Might nots

16. build a benchmark where there are bunch of faulty blueprints, and we try to fix them using LLM, and see how well it does. make it as realistic as possible. try to make blueprints with various different types of errors.

17. aqueduct docs

18. Flink Engine:
- Add `aqueduct/executor/flink/` subpackage from scratch (no code reuse from Spark executor - different execution model, different module semantics).
- Extend `pyproject.toml`: `flink = ["apache-flink>=1.18"]` extra.
- `get_executor("flink")` in `executor/__init__.py` already raises `NotImplementedError` as a placeholder.
- Module types that need re-evaluation for Flink: Junction (→ KeyedStream?), Funnel, Regulator (streaming watermarks), Egress mode (no overwrite concept for streaming sinks).
- Blueprint/Manifest schema is engine-agnostic; Flink-specific config goes in `deployment:` or per-module `flink_config:` block - do NOT add Spark-isms to the shared schema.
- Config: `deployment.engine: flink`, `deployment.master_url: "flink://<SPARK_MASTER>"` (or jobmanager address).

---

### Definite Future Work

18. think about interactive TUI style selection feature, where for commands like lineage, report or patch, if you don't specify something it would justlist possible options

19. Streaming, Machine Learning / MLOps

20. rename doctor command with something that fits overall theme terminilogy like "Surveyor"
however we have a python module with that name, I very much liked something similar to pre-flight to dry-run.

21. Extend on predefined doctor checks like file format mismatch, by adding more tests that are common and can genuinely help detect common bugs.
---

### Running Thoughts/Notes


13. Test if the patch/commit/apply and rollback works as expected.

14. Fix Issue with UDFs


15 FOUND ISSUE with hallucinating shitty '_key', i think:

PatchSpec op names: §8.5 lists replace_module_config, but guardrails in §8.6 and README mention set_module_config_key. Standardize on one.


16. Add Audit instructions to Specs.md so that developer would have guideline on how to fix the faulty blueprints.




