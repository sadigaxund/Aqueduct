## Open Questions / Discussions

---


---

### Definite Future Work

18. think about interactive TUI style selection feature, where for commands like lineage, report or patch, if you don't specify something it would justlist possible options

19. Streaming, Machine Learning / MLOps

20. Flink Engine:
- Add `aqueduct/executor/flink/` subpackage from scratch (no code reuse from Spark executor - different execution model, different module semantics).
- Extend `pyproject.toml`: `flink = ["apache-flink>=1.18"]` extra.
- `get_executor("flink")` in `executor/__init__.py` already raises `NotImplementedError` as a placeholder.
- Module types that need re-evaluation for Flink: Junction (→ KeyedStream?), Funnel, Regulator (streaming watermarks), Egress mode (no overwrite concept for streaming sinks).
- Blueprint/Manifest schema is engine-agnostic; Flink-specific config goes in `deployment:` or per-module `flink_config:` block - do NOT add Spark-isms to the shared schema.
- Config: `deployment.engine: flink`, `deployment.master_url: "flink://<SPARK_MASTER>"` (or jobmanager address).

---

### Running Thoughts/Notes


13. Test if the patch/commit/apply and rollback works as expected.

14. maybe implement a syntactic sugor for cli commands like interactive lineage/ report or whatever

15. does aqueduct init not create .git (git init)? if not, should it?


17. Add example maybe: aqueduct's usage with python

18. make sure that some commands like doctor works in all modes (specifically the one that checks file type)

19. standardize the warnings and spark best practices

20. rename doctor command with something that fits overall theme terminilogy like "Surveyor"
however we have a python module with that name, I very much liked something similar to pre-flight to dry-run.

21. maybe rename --store-dir to just --store, while at it, maybe major renaming before a release?
