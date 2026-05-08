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

---

### Running Thoughts/Notes


13. Test if the patch/commit/apply and rollback works as expected.

14. maybe implement a syntactic sugor for cli commands like interactive lineage/ report or whatever

15. does aqueduct init not create .git (git init)? if not, should it?

-----------------------
16. Verify if these changes are correct and justified:

a. The documentation and the audit checklist both specify that Environment Variables should have higher priority than Profile Settings. The intended resolution order (highest to lowest) is: CLI Overrides (--ctx), Environment Variables (AQUEDUCT_CTX_*), Profile Settings, Blueprint Defaults. However, the code in build_context_map was implementing them in this order: Apply Environment Variables, Apply Profile Settings. The execution order in aqueduct/parser/resolver.py was swapped.

b. identified a bug in the Provenance Map generation while validating top-level modules: The Parser is responsible for resolving all Tier 0 tokens (like ${ctx.var}) into final values. By the time the Compiler received the Blueprint object, the original expressions were already gone. For example, if a config value was ${ctx.path}, the compiler only saw the resolved string /data/raw. Because of this, the provenance map was incorrectly labeling almost everything as a "literal", losing the information that it actually originated from a context variable or an environment variable. The FIX: I modified aqueduct/compiler/compiler.py to preserve this history. Just like the compiler already does for Arcade sub-blueprints, it now re-loads the raw YAML of the main blueprint during the compilation phase. By comparing the raw YAML (which contains the original ${...} tokens) with the resolved values, the Compiler can now accurately reconstruct the provenance map.Specifically, I: 1) Updated the compile function to read the blueprint_path raw content. 2) Extracted the unresolved raw_context and raw_module_configs. 3) Used these to correctly identify context_ref, env_ref, and tier1 sources for the final Manifest. This ensures that the "Provenance Map" requirement is fully met: every resolved value in the Manifest now correctly tracks its original source.








