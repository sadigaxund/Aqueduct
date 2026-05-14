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

17. Add example maybe: aqueduct's usage with python

20. rename doctor command with something that fits overall theme terminilogy like "Surveyor"
however we have a python module with that name, I very much liked something similar to pre-flight to dry-run.

21. maybe rename --store-dir to just --store, while at it, maybe major renaming before a release?


18. make sure that some commands like doctor works in all modes (specifically the one that checks file type)
19. standardize the warnings and spark best practices
20. generate scenarios from Stackoverflow with the most commong bugs/issues vs specific cases
21. Maybe ready prefab modules, where some of the common SQL operations are standardized into just config (e.g. dedupe, mask_email, pivot/unpivot)
22. a kind of UDF, that is specifically designed for aqueduct syntax. In other words, a possibility of passing its parameter through context, config entries or even at udf_registry level. Allows adjustability of a UDF to multiple different blueprints and environments if need be, also would look cool when frontend can render such module.

23. verify this from readme: 'aqueduct init --name my-pipeline' does it take name flag, what for?
24. see if obs logs are written directly into '.aqueduct/' or '.aqueduct/<BLUEPRINT_ID>'
25. maybe rename '- llm' to either '- agent' or '- healing' within doctor command.


HOST_IP=10.0.0.39 MINIO_SECRET_KEY=minioadmin MINIO_ACCESS_KEY=minioadmin aqueduct doctor
Running connectivity checks (Spark may take 10–15s for JVM startup)...
:: loading settings :: url = jar:file:/home/sakhund/.local/lib/python3.14/site-packages/pyspark/jars/ivy-2.5.1.jar!/org/apache/ivy/core/settings/ivysettings.xml
✓ configaqueduct.yml (CWD) or defaultsengine=sparktarget=local[1ms]
Traceback (most recent call last):
File "/home/sakhund/.local/bin/aqueduct", line 6, in <module>
sys.exit(cli())
 ~~~^^
File "/usr/lib/python3.14/site-packages/click/core.py", line 1514, in __call__
return self.main(*args, **kwargs)
 ~~~~~~~~~^^^^^^^^^^^^^^^^^
File "/usr/lib/python3.14/site-packages/click/core.py", line 1435, in main
rv = self.invoke(ctx)
File "/usr/lib/python3.14/site-packages/click/core.py", line 1902, in invoke
return _process_result(sub_ctx.command.invoke(sub_ctx))
 ~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^
File "/usr/lib/python3.14/site-packages/click/core.py", line 1298, in invoke
return ctx.invoke(self.callback, **ctx.params)
 ~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
File "/usr/lib/python3.14/site-packages/click/core.py", line 853, in invoke
return callback(*args, **kwargs)
File "/home/sakhund/Personal/Projects/Aqueduct/aqueduct/cli.py", line 312, in doctor
icon = _STATUS_ICON[r.status]
 ~~~~~~~~~~~~^^^^^^^^^^
KeyError: 'error'




feat(doctor,compile,docs): per-file pre-flight, --show selector, Manifest rationale

- doctor: add --aqtest <file> and --aqscenario <file> flags. Both run as
schema pre-flights: validate the file's top-level version key, resolve the
blueprint: reference, parse that blueprint, and cross-check every
tests[].module / inject_failure.module against the actual module IDs.
Reuses aqueduct.surveyor.scenario.load_scenario; aqtest uses a light
inline parser to avoid importing the Spark test runner. All three
per-file flags (--blueprint, --aqtest, --aqscenario) are additive in a
single doctor pass.

- compile: add --show {manifest,provenance,inputs,all}. Default
'manifest' preserves the previous full-JSON behaviour. 'provenance'
emits a readable per-module table (key, source_type,
original_expression, resolved_value) plus a # Context section.
'inputs' emits the inputs_fingerprint as a per-Ingress table.
'all' prints the full Manifest JSON followed by both tables. Helpers
_render_compile_show / _format_provenance_table / _format_inputs_fingerprint
live in cli.py.

- docs/specs.md: new §4.1.1 'Why a Manifest? Why not run the YAML
directly?'. Tabulates, per Blueprint construct, why compile-time
resolution is required (${ctx.*}, @aq.date.*, @aq.secret,
@aq.depot.get, arcade refs, macros, passive regulators,
--execution-date). Documents that ProvenanceMap and inputs_fingerprint
exist primarily for the LLM, not the Executor, and points at the new
--show selectors for inspection.

- docs/CLI_REFERENCE.md: clarify that aqueduct lineage takes a Blueprint
file path, not a bare ID. Add rows for doctor --aqtest /
--aqscenario and compile --show. Extend the doctor check table to
cover the two new schema pre-flight checks.

- surveyor/llm.py: replace the two silent `except: pass` blocks
(patch-history file load, patches/rules.md read) with
`logger.debug(..., exc_info=True)`. Default runs see nothing; -v
users can now diagnose why a prompt section silently dropped a file.