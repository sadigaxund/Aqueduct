# AGENTS.md — Aqueduct Development Guidebook

## Project Context
Aqueduct is a declarative Spark blueprint engine with LLM-driven self-healing.

## Documentation map

`docs/specs.md` is the **engine reference** — Blueprint format, architecture, module semantics, type system, self-healing grammar. The other docs are specialised and own their own surfaces; specs.md cross-references them rather than duplicating their content.

| Doc | Owns | When to read |
|---|---|---|
| `docs/specs.md` | Blueprint format, architecture (4-layer) §3, Modules §4, Context Registry §5, Lineage §7, Self-Healing & Agent §8, Type System §9, Deployment & Spark Integration §10, Engine Scope §11 | Domain semantics, anything user-facing about the engine itself |
| `docs/cli_reference.md` | Every CLI command and flag with defaults | Touching `@click.option` / new subcommand in `aqueduct/cli/`, or answering "what flag does X" |
| `docs/observability_guide.md` | Store schemas (run_records, heal_attempts, healing_outcomes, failure_contexts, column_lineage, benchmark_results, patch_simulation, signal_overrides, explain_snapshot, probe_signals, module_metrics, maintenance_metrics, depot_kv, patch_index, drift_checks, channel_fingerprints) + diagnostic SQL cookbook | DDL / `ALTER TABLE` changes in `aqueduct/surveyor/` or `aqueduct/executor/`, or writing post-mortem queries |
| `docs/spark_guide.md` | Compiler warnings, performance, tuning, Spark behavior gotchas | Modifying Executor modules, adding Channel ops, debugging Spark perf |
| `docs/production_guide.md` | Cluster deployment, env config, Spark cluster config, path conventions, danger settings, Delta operational notes, production patch lifecycle, security, readiness checklist | Anything related to running Aqueduct on a cluster (k8s, YARN, Databricks, …) |
| `docs/compatibility.md` | Python × Spark support matrix, cloudpickle constraint, production pinning | Changing version pins in `pyproject.toml`, or answering "does X version combo work" |
| `docs/roadmap.md` | Deferred / aspirational items removed from specs.md: streaming, resume-from, MCP, ML inference, Flink, multi-pipeline orchestration | Discussing scope or future direction; never write fresh "deferred" prose into specs.md — push to roadmap.md instead |
| `docs/failure_taxonomy.md` | Recurring defect classes (mined from CHANGELOG `### Fixed`) with detection patterns + CI-enforceable guards; the input catalog for cheap-model audit passes | Fixing any bug (triage it against the table, same commit-set); writing/reviewing a compiler warning (advice-rot + dangerous-advice classes); running an audit skill |
| `SKILL.md` (repo root) | Distilled LLM **Blueprint-authoring** guide: grammar, the 9 module types, edges/ports, Context Registry, UDFs, the `agent:` block, gotchas, a worked example, and the OpenAI-compatible provider `base_url` table. The authoring counterpart to specs.md (specs = exhaustive reference; SKILL = signal-dense how-to-author). | Any change to the Blueprint grammar / module config keys / `agent:` block / provider wiring — keep it in sync with specs.md (it restates the same contracts compactly, so drift = wrong guidance to authoring LLMs) |

AGENTS.md itself is process and constraint guidance only.

## Tech Stack
- **Language**: Python 3.11+. Monolithic CLI — runs on the Spark driver. No servers.
- **Key deps**:
  - `pyspark` — optional (`aqueduct-core[spark]` extra); never imported outside `aqueduct/executor/spark/`
  - `pydantic` + `ruamel.yaml` + `pyyaml` — schema validation, YAML round-trips
  - `click` — CLI
  - `duckdb` — embedded observability store (avoids SQLite write locks)
  - `sqlglot` — SQL lineage; do NOT write a custom SQL parser
  - `httpx` — HTTP client for webhooks and LLM calls; no `anthropic` SDK, no extra install needed

## Packaging & Extras Policy

Optional dependencies follow **two axes only — never invent a third.** Adding a
feature-named extra (`[blob-s3]`, `[openlineage]`, `[drift]`) is forbidden; it
multiplies the surface users have to reason about.

- **Per-vendor leaves:** `aws`, `gcp`, `azure`, `postgres`, `redis`, `airflow`,
  `object-store` — one SDK/capability each.
- **Capability aggregates:** `secrets` (= aws+gcp+azure), `stores`
  (= postgres+redis+object-store), `schedulers` (= airflow), `all`.

A user installs an aggregate or a leaf. When a new optional dependency appears,
map it onto an existing axis: reuse a vendor leaf if it's that vendor's SDK, or
add a leaf that rolls up into the right aggregate. A genuinely new vendor → new
leaf + add it to its aggregate. Never a standalone feature flag. (Example:
Phase 53's object store became the `object-store` leaf inside `stores`; Phase 55
OpenLineage adds **no** extra — `httpx` is already a base dep.)

**Documented exception — dev-tooling extras.** The two-axis rule governs
*runtime-capability* deps (vendor SDKs / store backends used while a pipeline
runs). A small separate class is allowed for **developer/inspection tooling that
never runs in the data path**: `dev` (pytest/black/ruff), `tui` (`textual`, for
`aqueduct studio`), and `dashboard` (`streamlit`+`plotly`, for `aqueduct
dashboard` — a local, read-only, on-demand observability viewer like the Spark
UI). These stay OUT of `all` and out of the runtime axes — a
pipeline never needs them, so bundling them into base/`all` would bloat headless
Spark-driver / CI installs. This is the *only* sanctioned feature-named-extra
category; it is not a loophole for runtime features (those still follow the axes).

## Code Organization & Safety
- **4-layer boundary**: `Parser` → `Compiler` → `Executor` → `Surveyor`. Put logic in the correct layer. Only modify the layer relevant to the task. Topological sort, Probe insertion, and parallel-component detection are sub-steps inside the Executor — not a separate "Planner" layer.
- **Dual-format contract**: Humans write YAML (`Blueprint`); engine consumes JSON (`Manifest`, `FailureContext`).
- **Zero-cost observability**: Never insert `count()`, `show()`, or `collect()` Spark actions into the critical execution path (e.g. inside Probes). Use SparkListener metrics or explicit sampling.
- **Spillway rules**: Transform channels and UDFs must use `try/except` (or Spark `try_*` functions) to catch row-level errors and populate `_aq_error_*` columns without aborting the Spark stage.
- **Immutability**: `@dataclass(frozen=True)` on all internal representations (`Module`, `Edge`, `Manifest`). Each compilation step returns a new immutable object.
- **UDF bodies are out of scope for self-healing.** Aqueduct's PatchSpec grammar cannot modify UDF bodies (Python/Scala/Java code in the `udfs:` block). A pipeline failing because of a bug in UDF logic is a `defer_to_human` situation — the agent diagnoses the UDF as the root cause but cannot rewrite its implementation. This is by design: arbitrary code modification breaks the P5 principle (patch grammar over codegen).
- **Trace every consumer before changing a type or output path.** When you change a field's type (e.g. `str` → `str | None`), a function's output destination (e.g. stdout → stderr), or a sentinel value, grep for every call site and every attribute access on that field FIRST — before making the change. A `Path(None)` crash, an `UnboundLocalError` from a scoped import, or a test assertion on the old sentinel each cost multiple fix rounds that a 30-second grep would have avoided.

## Executor Architecture (Extras Pattern)
- `aqueduct/executor/models.py` — engine-agnostic (`ExecutionResult`, `ModuleResult`)
- `aqueduct/executor/path_keys.py` — engine-agnostic path-field registry (imported by the parser)
- `aqueduct/executor/probe_plugins.py` — engine-agnostic, **pyspark-free** custom-probe-signal resolver (`custom_signal_source`, `resolve_callable`, `AQ_PROBE_ENTRYPOINT_GROUP`). Lives at the top level (not under `spark/`) so the compiler (`wirer`) can validate `type: custom` signal shape and the Spark `probe.py` can resolve callables at runtime — same precedent as `path_keys.py`. Do not add `pyspark` imports here.
- `aqueduct/executor/spark/` — all Spark code (`ingress`, `egress`, `channel`, `executor`, `junction`, `funnel`, `probe`, `session`, `udf`, `assert_`)
- `aqueduct/executor/__init__.py` — `get_executor(manifest, config)` factory

When adding a Spark feature: code in `aqueduct/executor/spark/`. Do not import `pyspark` in `parser`, `compiler`, `surveyor`, `patch`, or `depot`.

**Documented exception (doctor):** the `aqueduct/doctor/` package lazily imports `pyspark` inside three check functions (`check_spark`, `check_storage`, `check_cloudpickle_compat`, all in `doctor/__init__.py`). Top-level `import aqueduct.doctor` must never pull `pyspark` — keep these imports inside function bodies so `--skip-spark` and the `[spark]`-less install path stay viable. The package splits leaf connectivity checks (`doctor/checks_io.py`) from the spark/network/blueprint-source cluster + `run_doctor` (`doctor/__init__.py`); the latter stay together because the test suite monkeypatches several by their `aqueduct.doctor.<name>` path and they call each other by bare global name. `doctor/base.py` holds `CheckResult` + `_ms` to avoid a circular import. Public names re-export from `__init__`, so `from aqueduct.doctor import <check>` is unchanged.

**Documented exception (surveyor):** `surveyor/surveyor.py::_extract_structured_error` lazily imports `from pyspark.errors import PySparkException` inside a `try/except` (falling back to `None` when absent). This is required — structured root-cause extraction must recognise Spark's own exception types — but it is **guarded**: the import lives in the function body, so top-level `import aqueduct.surveyor` stays `pyspark`-free and the `[spark]`-less install path is preserved. Do not promote this to a module-level import.

**Documented exception (dashboard):** `aqueduct/dashboard/app.py::main()` lazily imports `pyspark.sql` inside the function body as a narwhals/plotly circular-import workaround. It is already lazy and does not violate the top-level-purity rule. Do not promote it to module-level or remove the workaround comment.

**Documented exception (executor→patch):** `aqueduct/executor/spark/executor.py::execute()` lazily imports `capture_plan_snapshot` from `aqueduct.patch.explain_gate` inside a function-body `try/except` to capture Phase 29b explain-plan snapshots for Gate 4 (plan-regression detection). This crosses the 4-layer boundary (Executor depending on Patch, which normally sits downstream of Surveyor) but is accepted because: the import is lazy (module-level `import aqueduct.executor.spark.executor` never pulls in `patch/`), it only fires when a `surveyor` or `explain_capture` sink is supplied, and the surrounding `except Exception` is broad-but-justified — plan-snapshot capture is a best-effort diagnostic that must never abort a run. Do not invert the dependency or hoist the import to module level.

When adding an LLM provider: add `_call_<provider>()` in `aqueduct/agent/providers.py` using `httpx`. Wire dispatch in `_call_agent()`. No new dep needed.

**`PROMPT_VERSION` bump policy** (`aqueduct/agent/loop.py`): bump **only** when `_SYSTEM_PROMPT_TEMPLATE` body, persona text, op table, schema-derived rules, or the worked example changes — anything the LLM sees on a *successful* turn. Do NOT bump for reprompt-loop tooling, `_format_validation_error` text, `_detect_structural_error` cases, the escalated-reprompt template, mechanical recovery passes (`_THINK_BLOCK_RE`, `_FENCE_BLOCK_RE`, `_LINE_COMMENT_RE`, `json_repair`), or `_parse_patch_spec` cleanups. Those are failure-path tooling — bumping for them pollutes `healing_outcomes.prompt_version` / `benchmark_results.prompt_version` correlation, making a parser tweak look like a prompt regression on the leaderboard.

**Spark behavior reference**: read `docs/spark_guide.md` before modifying Executor modules or implementing new Channel operations.

## TODOs Memory Rule
`TODOs.md` is the single source of truth for what's next, what's stubbed, and what's deferred.

- **"What's left?" / "What's next?"** → read TODOs.md first.
- **After every planning session** → update TODOs.md with agreed phases.
- **After every phase completion** → mark phase done in TODOs.md; add entry to CHANGELOG.md.
- **When adding a stub** → add to Active Stubs with file + line + acceptance criteria.

## Change-Trigger Matrix

Use this table at coding time, not just at the end of a phase. Whenever you touch the left column, the right column **must** move in the same commit. The phase-end ritual becomes a verification pass: read this table top-to-bottom and confirm every triggered doc/file is current — the matrix is the trigger, not the date on the calendar.

`docs/specs.md` is the **engine reference** for semantics that don't belong elsewhere. Production / CLI / observability / Spark-tuning details now live in their dedicated guides (see Documentation map). Phase / sprint / development artefacts (`Phase 35`, `Sprint 7`, `Task NN`, `pre-30a`, `deferred to Phase NN`, etc.) must stay off **user-facing surfaces**: docs (`docs/**/*.md`), templates (`aqueduct/templates/**` — they get copied into user projects), gallery (`gallery/**`), and scaffolding (`README.md`, `CONTRIBUTING.md`). They are **allowed** in `CHANGELOG.md` / `TODOs.md` (where they belong) and in **source-code comments/docstrings** (`aqueduct/**/*.py`), where `# Phase NN —` is useful provenance — do not strip those. Verify the user-facing surfaces stay clean with `grep -rnE "Phase [0-9]|Sprint [0-9]|Task [0-9]" docs/ gallery/ aqueduct/templates/ README.md CONTRIBUTING.md` (note: **not** `aqueduct/` source) before commits that touched any of them.

> **specs.md drift is the easy failure mode.** The matrix below routes most work to the *dedicated* guides, so specs.md — the engine reference — is the doc that silently goes stale (it lagged 6 phases once, 1.1 → 1.2). It is **not** a catch-all of last resort: any change to a documented **contract** must update specs.md *in the same commit* — a new/renamed `aqueduct.yml` key or top-level block, a new `stores.*` backend or persistent store/table, an `agent.approval` mode/value or exit-code change, a new patch op or CLI contract (not just a flag). When such a change lands, also **bump the `Version X.Y` header** at the top of specs.md. Phase-end verification: `git log -1 --format=%h -- docs/specs.md` should not be many phases behind `aqueduct/config.py` / `aqueduct/stores/` / `aqueduct/cli/` if any of those changed a contract this phase.

| If you change … | You must update … |
| :- | :- |
| Any DDL or `ALTER TABLE` in `aqueduct/surveyor/` or `aqueduct/executor/` | `docs/observability_guide.md` schema table; if the new column enables a meaningful diagnostic, add a cookbook recipe (When → What you learn → What to do next) |
| Any `@click.option` / new sub-command in `aqueduct/cli/` | `docs/cli_reference.md` flag table (include default value) |
| Any pydantic field in `aqueduct/config.py` or `aqueduct/parser/schema.py` | The corresponding template comment block (`aqueduct.yml.template` for engine config, `blueprints/blueprint.yml.template` for Blueprint) |
| Any `StopReason`, `BudgetConfig`, or apply-gate behaviour | `docs/specs.md` §8 + `docs/observability_guide.md` `heal_attempts` section |
| Any new/renamed `aqueduct.yml` key, top-level config block, or `stores.*` backend / persistent store / table | `docs/specs.md` (§3.2 stores, §10 config, or the relevant section) **and** bump the specs.md `Version X.Y` header + the template comment block (row above) |
| Any change to `agent.approval` modes/values, the patch-grammar op list, or the exit-code contract | `docs/specs.md` §8 (Approval Modes / Patch Grammar) + §10.7 exit-code table |
| Any production / deployment / danger-setting / cluster-config detail | `docs/production_guide.md` |
| Any Spark compiler-warning, performance, or tuning behaviour | `docs/spark_guide.md` |
| Any change to `pyproject.toml` version pins or supported Python/Spark range | `docs/compatibility.md` |
| Any newly deferred or aspirational item (NOT a current phase) | `docs/roadmap.md` — never inline "this is deferred" prose into `specs.md` |
| Any new file under `docs/` | `README.md` References list + this Documentation map |
| Any new flag, command, or behaviour visible from the CLI | `docs/cli_reference.md` |
| Any user-facing engine semantic (architecture, module semantics, configs, gates, runtime behaviour) NOT covered by a dedicated guide above | `docs/specs.md` — NO `Phase NN` artefacts |
| Any change to the **Blueprint grammar** (module types/config keys, edges/ports, Context fns, UDF/macro syntax), the **`agent:` block** fields/values, or **LLM provider wiring** (`provider`/`base_url`/auth env var) | `SKILL.md` (repo root) — it restates these contracts compactly for authoring LLMs; update it in the SAME commit as `specs.md` or the schema, else authoring guidance drifts |
| Any new testable feature | A real test at the right layer (`unit` / `integration` / `e2e`), OR a `@pytest.mark.todo("why")` stub if you're deferring it. NEVER an entry in `TEST_MANIFEST.md` (retired → `docs/archive/`). See the Testing section. |
| Any phase / sprint / shippable change | `CHANGELOG.md` `[Unreleased]` only — never bump version, never add a versioned header (user controls release timing) |
| Any new `@aq.*` function in `aqueduct/compiler/runtime.py` | `docs/specs.md` §5.3 function table + `_DISPATCH` table in `runtime.py` |
| Any new path-key entry in `aqueduct/executor/path_keys.py` | The module-type's schema model in `aqueduct/parser/schema.py` (mark path fields with `Annotated[str, FsPath()]`) |
| Any new exit code in `aqueduct/exit_codes.py` | `docs/cli_reference.md` exit-code reference |
| Any new CLI exit path (`sys.exit(...)` in `aqueduct/cli.py`) | Use a named `exit_codes.*` constant — never a bare int (only `sys.exit(130)` for SIGINT is exempt). Classify by the contract docstring in `aqueduct/exit_codes.py`: config/schema/danger-policy → `CONFIG_ERROR`; runtime/data/missing-file/subprocess/record-not-found → `DATA_OR_RUNTIME`; bad-flag / missing-arg → `USAGE_ERROR`; staged-patch → `HEAL_PENDING`; non-interactive gate rejection → `VALIDATION_GATE`. A new *value* added to `exit_codes.py` is a v1.0-contract change → also update `CHANGELOG.md` |

## Source Code Navigation Map

This section documents the internal module structure of key packages. Updated
whenever a package is restructured — use it as the first filter before grepping.

### `aqueduct/models.py` — Cross-layer boundary types

Re-export surface for downstream layers (executor, surveyor). Imports from
`parser.models` and `compiler.models` so executor/surveyor don't reach into
those internals. Pure re-export — definitions still live in their original
modules.

| Symbol | Defined in | Role |
|--------|-----------|------|
| `Manifest` | `compiler.models` | Compiled Blueprint ready for execution |
| `Module` | `parser.models` | Parsed module node |
| `Edge` | `parser.models` | Data-flow edge |
| `RetryPolicy` | `parser.models` | Per-pipeline retry config |
| `ModuleType` | `parser.models` | Module type enum |

### `aqueduct/surveyor/` — Observability, benchmarking, webhooks

| Module | What it owns |
|--------|--------------|
| `surveyor.py` | Main Surveyor class: start/record/stop lifecycle. Re-imports the DDL constants and `_extract_structured_error` (kept under `aqueduct.surveyor.surveyor.*` for callers/tests) |
| `ddl.py` | Observability-store `CREATE TABLE`/`ALTER TABLE` string constants (`_DDL`, `_SIGNAL_OVERRIDES_DDL`, `_EXPLAIN_SNAPSHOT_DDL`, `_HEAL_ATTEMPTS_DDL`). Pure SQL, no imports — re-exported by `surveyor.py` |
| `error_extraction.py` | Structured Spark/Py4J error extraction (`_extract_structured_error`, `_parse_suggested_columns`). Lazily imports `pyspark.errors`/`py4j` INSIDE the function — top-level `import aqueduct.surveyor` stays `pyspark`-free. Re-exported by `surveyor.py` |
| `models.py` | Frozen dataclasses: `RunRecord`, `FailureContext` (the LLM agent's input) |
| `scenario.py` | Scenario benchmark framework: `load_scenario`, `_build_failure_ctx`, effect-based grader |
| `webhook.py` | HTTP dispatch in daemon thread, `${VAR}` template rendering, redaction |
| `openlineage.py` | OpenLineage RunEvent emission (START/COMPLETE/FAIL) to Marquez/DataHub/Atlan via `infra/http.py` daemon delivery — async, best-effort, never blocks the run; carries the column-level `columnLineage` facet. Configured by the top-level `lineage:` block; no `openlineage_url` → no emitter, zero cost |
| `benchmark_store.py` | DuckDB persistence + regression detection for benchmark results |
| `blob_store.py` | Back-compat shim (Phase 53) — `externalise`/`materialize` delegate to `stores/object_store.BlobStore`; new code uses `make_blob_store` directly |

### `aqueduct/patch/` — PatchSpec grammar + apply + validation gates

| Module | What it owns |
|--------|--------------|
| `grammar.py` | `PatchSpec` Pydantic v2 model, 14 operation types, discriminated union |
| `operations.py` | Per-op implementations against Blueprint dict, ruamel YAML round-trip |
| `apply.py` | Apply orchestrator: load → deep-copy → apply ops → re-parse → archive |
| `index.py` | `patch_index` relational table (Phase 53): the truth for the object-store patch lifecycle — status + signature metadata for backend-blind heal-cache lookups (pending/replay/coaching/history) without scanning `patches/` |
| `preview.py` | Lineage gate (Gate 2) + sandbox gate (Gate 3): diff column impact, sandbox replay |
| `explain_gate.py` | Plan regression gate (Gate 4): compare Exchange/Broadcast counts before vs after |
| `ci.py` | CI kit for `approval: ci` — `on_patch_pending` webhook payload schema (`CI_WEBHOOK_REQUIRED_KEYS`) + payload validation. Serverless: a user-owned CI workflow receives the webhook and calls `aqueduct patch import` (see `docs/templates/ci-heal-workflow.yml`) |
| `__init__.py` | Module description only |

### `aqueduct/stores/` — Pluggable store backends

| Module | What it owns |
|--------|--------------|
| `base.py` | ABCs for `ObservabilityStore`, `LineageStore`, `DepotStore` + `RelationalCursor` (`?` → `%s`), `StoreBundle` factory |
| `ddl.py` | Shared **cross-backend** store DDL (currently just `DEPOT_KV_DDL` — the `depot_kv` schema bound by both `DuckDBDepotStore._DDL` and `PostgresDepotStore._DDL`, single source of truth). DDL owned by one layer stays with its owner (`patch_index` in `patch/index.py`, observability tables in `surveyor/ddl.py`, `benchmark_results` in `surveyor/benchmark_store.py`) |
| `duckdb_.py` | DuckDB implementations (single-file embeddable) |
| `postgres.py` | Postgres implementations (connection-pool dedup, schema-per-store) |
| `redis_.py` | Redis depot KV (high-QPS watermark reads) |
| `object_store.py` | `ObjectStore` transport (local/fsspec `_Backend`) + `BlobStore` (zstd blobs) + `PatchStore` (patch lifecycle) + `make_blob_store`/`make_patch_store` factories |
| `read.py` | Canonical backend-aware READ resolver (Phase 69): `resolve_duckdb_obs_path` (single source for the duckdb obs file — `cli._resolve_obs_db` delegates here) + `open_obs_read` (returns an `ObservabilityStore` for duckdb *or* postgres). All read commands must use it instead of raw `duckdb.connect` + hardcoded `.aqueduct/...` paths |
| `queries.py` | The ONE read-time observability query layer (Phase 68) behind every viewer — `aqueduct studio` (via the `tui/data.py` re-export shim), the Streamlit dashboard, and `report --json`. Row dataclasses (`RunRow`, `RunDetail`, `LineageRow`, …) + `discover_stores`/`list_runs`/`run_detail`/`lineage`/`run_sql_readonly`. Backend-agnostic (`RelationalCursor`), no `textual`, no `pyspark`. New viewer query → add here, never inline SQL in a rendering surface |

### `aqueduct/infra/` — Cross-layer infrastructure utilities (no domain logic)

| Module | What it owns |
|--------|--------------|
| `http.py` | Single source of truth for outbound-HTTP mechanics: `RETRYABLE_DELIVERY_STATUS` / `RETRYABLE_PROVIDER_STATUS`, `retry_after_seconds`, `backoff_delay`, `sign_body` (HMAC-SHA256), `fire_and_forget` (daemon thread), `deliver_with_retry` (best-effort POST loop, stderr-logged). The webhook + OpenLineage daemon-delivery paths build on it; `agent/providers.py` aliases its retryable-status constant + helpers (so existing `providers._RETRYABLE_STATUS` / `_retry_after_seconds` patch paths keep working). Add a new outbound-HTTP caller here, do not re-implement retry/backoff inline. |

When adding a new outbound-HTTP path: reuse `infra/http.py`. The Databricks Jobs-API client (`deploy/databricks.py`) and doctor probes are intentionally NOT on this path (synchronous API client / one-shot reachability probe — different shapes).

### `aqueduct/depot/` — Cross-run KV state

| Module | What it owns |
|--------|--------------|
| `depot.py` | `DepotStore` façade — delegates to whichever store backend is configured |
| `__init__.py` | Empty |

### `aqueduct/drift/` — Proactive schema-drift detection (`aqueduct drift`)

| Module | What it owns |
|--------|--------------|
| `classifier.py` | Pure baseline-vs-live schema diff: *dropped/type-changed* = breaking (triggers a predicted heal), *added* = benign (audit only). No pyspark |
| `context.py` | Synthetic in-memory `FailureContext` for a predicted failure — reuses the normal agent + apply-gate machinery unchanged; NOT persisted to `failure_contexts` (that table means "a real run failed") |
| `store.py` | `drift_checks` audit table + self-owned baseline (latest row's `live_schema` is the next check's baseline — no Probe required) |

The CLI command lives in `aqueduct/cli/drift.py`.

### `aqueduct/doctor/` — Pre-flight health checks

| Module | What it owns |
|--------|--------------|
| `__init__.py` | Spark/network cluster + blueprint-source checks + `run_doctor` |
| `base.py` | `CheckResult` dataclass |
| `checks_io.py` | Leaf connectivity checks: config, depot, observability, webhook, agent, secrets, store-backend, aqtest, aqscenario |

### `aqueduct/executor/spark/` — Spark execution

| Module | What it owns |
|--------|--------------|
| `executor.py` | Main loop: topo sort, component detection, module dispatch, parallel mode |
| `ingress.py` | Read: Parquet, Delta, CSV, JSON, JDBC |
| `channel.py` | Transform: SQL, deduplicate, filter, select, rename, cast, repartition, union, sort |
| `egress.py` | Write: overwrite, append, error, merge (Delta MERGE INTO) |
| `junction.py` | Fan-out: conditional, broadcast, partition |
| `funnel.py` | Fan-in: union_all, union, coalesce, zip |
| `probe.py` | Signals: schema_snapshot, row_count_estimate, null_rates, sample_rows, value_distribution, distinct_count, data_freshness, partition_stats, threshold |
| `assert_.py` | Quality gates: min_rows, null_rate, freshness, sql, sql_row, spillway_rate |
| `session.py` | SparkSession management, Delta conf, cloudpickle patch |
| `udf.py` | UDF registry, cloudpickle compatibility |
| `custom_source.py` | Custom Python DataSource (`format: custom`) import/validate/register — Spark 4.0+ |
| `metrics.py` | Zero-extra-action observe() wrapper, Hadoop FS byte count |
| `test_runner.py` | Isolated module test framework (aqueduct test CLI) |
| `warnings/` | Session-startup warning rules (jar_availability), registered in `RULES` list |

### `aqueduct/compiler/` — Blueprint AST → fully-resolved Manifest

| Module | What it owns |
|--------|--------------|
| `compiler.py` | Main orchestrator — 8-step pipeline: Tier 1 resolution, Arcade expansion, Probe/Spillway validation, Regulator compile-away, Manifest assembly |
| `models.py` | `Manifest` frozen dataclass + `to_dict()` serialization |
| `expander.py` | Arcade → flat namespaced module list, edge rewiring, depth-limited recursion (max 10) |
| `lineage.py` | `sqlglot`-based column-level lineage extraction (compile-time only, zero Spark actions) |
| `macros.py` | `{{ macros.name }}` token expansion, parameterized macros, no nesting |
| `provenance.py` | ValueProvenance, ModuleProvenance, ProvenanceMap — tracks source of every resolved config value |
| `runtime.py` | `AqFunctions` registry + `_DISPATCH` table — resolves `@aq.*` Tier 1 tokens via `ast.literal_eval` |
| `wirer.py` | Probe attach_to validation, spillway edge validation, Regulator compile-away bypass logic |
| `warnings/` | Modular compiler warning rules — one file per check, registered in `RULES` list |

### `aqueduct/parser/` — Blueprint YAML → validated AST

| Module | What it owns |
|--------|--------------|
| `__init__.py` | Re-exports: `parse`, `parse_dict`, `ParseError`, `Blueprint`, `Module`, `Edge`, `ContextRegistry` |
| `parser.py` | Orchestration: YAML load → Pydantic validation → context resolution → graph validation → AST assembly |
| `schema.py` | Pydantic v2 models for Blueprint validation (`extra="forbid"` on all types) |
| `models.py` | Immutable dataclasses (`Blueprint`, `Module`, `Edge`, `AgentConfig`, etc.) |
| `resolver.py` | Tier 0 context resolution (`${ENV}`, `${ctx.*}`, profiles, CLI overrides) |
| `graph.py` | Cycle detection (Kahn's algorithm) + topological ordering + spillway validation |
| `fs_path.py` | `FsPath` annotation marker for pydantic path fields |

**Cross-layer note:** `parser/parser.py` imports `get_path_keys` from `aqueduct/executor/path_keys.py`. This is an accepted architectural exception — the path-keys registry lives in the executor layer because path fields are defined per executor module type, not per parser schema. No Spark imports are involved; the file is a pure field-name registry.

### `aqueduct/agent/` — LLM agent loop

| Module | Public API | What it owns |
|--------|-----------|--------------|
| `__init__.py` | `AgentPatchResult`, `PROMPT_VERSION`, `generate_agent_patch`, `stage_patch_for_human`, `archive_patch`, `build_prompt`, `resolve_budget`, `AgentRunConfig`, `generate_cascade_patch` | Thin re-export facade + `resolve_budget()` |
| `loop.py` | `generate_agent_patch`, `stage_patch_for_human`, `archive_patch`, `AgentPatchResult`, `PROMPT_VERSION` | Main orchestration loop, patch I/O, result type |
| `constants.py` | `DEFAULT_MAX_TOKENS`, `DEFAULT_LLM_TIMEOUT`, `DEFAULT_LLM_MODEL`, `ANTHROPIC_API_VERSION` | Leaf-level shared defaults (no intra-package imports — circular-dep safe) |
| `cascade.py` | `generate_cascade_patch` | Multi-model healing cascade — per-tier config inheritance, escalation on stuck/exhausted/deferred |
| `prompts.py` | `build_prompt`, `_build_user_prompt`, `_build_system_prompt`, `_FIELD_ALIASES`, `_VALID_OPS` | Template strings, prompt construction, LLM-facing constants |
| `providers.py` | `_call_agent`, `_call_anthropic`, `_call_openai_compat`, `_ProviderConfig` | HTTP dispatch to Anthropic / OpenAI-compatible endpoints |
| `parse.py` | `_parse_patch_spec`, `_detect_structural_error`, `_format_reprompt_error`, `_format_reprompt_for_next_turn` | Response parsing, structural error detection, reprompt formatting |
| `budget.py` | `BudgetConfig`, `BudgetTracker`, `AttemptRecord`, `DEFAULT_BUDGET` | Multi-axis budget tracking for the reprompt loop (`pause_clock()` excludes gate time) |
| `signature.py` | `ErrorSignature`, `from_*` helpers, `from_failure_context` | Error signature engine (stable dedup hash for budget + heal cache + coaching) |
| `memory.py` | `find_pending`, `find_replay_candidate`, `find_coaching_examples` | Signature memory — zero-token heal-cache lookups (pending reuse, exact replay, coaching retrieval). Phase 53: backed by the `patch_index` SQL table (`patch/index.py`), not a `patches/` dir scan; takes an `obs_store` (+ `patch_store` for replay bodies) |
| `transcript.py` | `TranscriptWriter` (write, header, summary) | Turn-by-turn healing conversation display — terse one-liner per attempt or full verbose block with patch details, tokens/cost, tier labels. Engine-agnostic (no pyspark, no click); returns strings so ``cli/output.py`` handles colour/redaction. Shared by ``run``, ``heal``, and benchmark via the ``on_attempt`` hook. |

**When adding a feature:**
- New LLM provider → add `_call_<provider>()` in `providers.py`, wire in `_call_agent()`
- New prompt rule → edit `_SYSTEM_PROMPT_TEMPLATE` in `prompts.py`, bump `PROMPT_VERSION` in `loop.py`
- New recovery pattern → add to `_parse_patch_spec()` in `parse.py`
- New budget axis → add to `BudgetConfig` / `BudgetTracker` in `budget.py`
- New patch lifecycle event → add to `loop.py`, re-export from `__init__.py`
- New heal-cache lookup → add the SQL to `patch/index.py`, surface it via `memory.py` (an `obs_store` query — no LLM calls)

### `aqueduct/cli/` — Command-line interface (package, split from the old `cli.py`)

`cli.py` was split into a package. `__init__.py` holds the `cli` click group, the
**shared helpers** (env loading, redaction, `_resolve_obs_db`, `_agent_usable`,
the patch-gate helpers, `_patches_root_from_blueprint`, …), and the bottom-of-file
imports that register + re-export each command family. Helpers stay in `__init__`
so `patch("aqueduct.cli._helper")` paths and `from aqueduct.cli import _helper`
keep working. Command families live in submodules:

| Module | Commands |
|--------|----------|
| `__init__.py` | `cli` group + group options; all shared `_*` helpers; command registration/re-export |
| `run.py` | `run` (+ `--sandbox`), `compile` |
| `heal.py` | `heal` |
| `drift.py` | `drift` — proactive schema-drift check + pre-emptive heal (the reactive arm's counterpart); domain logic lives in `aqueduct/drift/` |
| `patch.py` | `patch` group: preview/apply/reject/commit/discard/list/log/rollback |
| `observability.py` | `report`, `runs`, `lineage`, `signal` |
| `benchmark.py` | `benchmark`, `benchmark-diff`, `benchmark-stats` |
| `diagnostics.py` | `validate`, `lint`, `schema`, `doctor` |
| `stores.py` | `stores` group (info, migrate) |
| `output.py` | Consolidated output funnel: `emit()` (structured ``--format``), `warn()` (diagnostic warnings) |
| `style.py` | The single user-facing output vocabulary: `error`/`success`/`warn`/`info` + `StyledLogFormatter` (see "CLI output speaks ONE vocabulary" rule) |
| `project.py` | `init`, `completion`, `test` |

**Rules:** submodules import the group + non-patched helpers from `aqueduct.cli`;
the 6 monkeypatched helpers (`_agent_usable`, `_resolve_obs_db`,
`_run_patch_gates_inline`, `_apply_patch_in_memory`, `_write_patch_to_blueprint`,
`_stage_failed_patch`) are accessed via `import aqueduct.cli as _aqcli` /
`_aqcli._helper(...)` so test patch paths still bite. New commands go in the
matching submodule (or a new one + a bottom-of-`__init__` re-export); new shared
helpers go in `__init__`.

### `aqueduct/tui/` — `aqueduct studio` interactive TUI (Phase 67)

| Module | What it owns |
|--------|--------------|
| `data.py` | Re-export shim over `aqueduct/stores/queries.py` (the shared read layer, Phase 68) — keeps the frozen TUI's import paths working. **No `textual`, no `pyspark`.** New queries go in `stores/queries.py`, not here. |
| `app.py` | The `textual` application (`StudioApp`, `run_studio`). Imports `textual` (the `tui` extra); only loaded after the CLI confirms the dep is installed. Rendering + event wiring only — all data via `data.py`. |

The `studio` command lives in `cli/observability.py` and guards on
`importlib.util.find_spec("textual")` before importing `app.py`, printing an
"install aqueduct-core[tui]" hint otherwise. `tui/__init__.py` must stay
`textual`-free so `import aqueduct.tui.data` works on a base install.

## Git & Commit Conventions

### Commit message format
All commits must follow: `type(scope): message` where type is one of: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`, `release`. Scope refers to the module/area (e.g., `parser`, `executor`, `agent`). Enforced via `pre-commit commit-msg` hook.

Run once per clone: `pre-commit install --hook-type commit-msg`

### Git safety rules
- NEVER `git push` unless explicitly asked.
- NEVER `--amend`, `--force-push`, or create empty commits.
- Stage only intended files; inspect with `git status` + `git diff` before commit.

### Branch conventions (phase branches)
- Use `phase/NNN-NNN-name` naming (NNN ranges are feature-set phase numbers).
- Each commit represents one logical change — no squashing unrelated work.
- Fast-forward merge to main when a phase completes; preserve the branch ref.
- Never rebase or squash-merge — maintain linear history.

### Committing methodology
When building a phase from a sequence of changes:

1. Create branch: `git checkout -b phase/NNN-NNN-name main`
2. For each logical change in sequence: apply files, stage **by explicit path** (`git add <files>` — never `git add -A`/`-u`; the working tree may carry unrelated in-flight changes), `git commit -m "type(scope): message"`
3. When phase is complete: `git checkout main && git merge phase/NNN-NNN-name --ff-only`
4. Preserve branch: `git branch phase/NNN-NNN-name HEAD`

## Common Pitfalls

- **Silent `@aq.depot.get()` when depot is unconfigured.** If a Blueprint references `@aq.depot.get('watermark')` but no depot backend is configured, the call returns `""` silently. Incremental pipelines will re-read all source data every run. Always configure a depot when using incremental Channels, and verify with `aqueduct doctor`.

- **`spark.catalog.dropTempView` in `_write_merge`.** The Delta merge path in `egress.py` drops its temp view before creating it; on first merge the view doesn't exist. The current code guards the drop with try/except — keep that guard (or a `tableExists()` check) when touching this path, or first-merge runs raise `AnalysisException`.

- **Frame store is scoped per parallel component.** In `--parallel` mode, modules in different connected components cannot access each other's frame-store keys. Cross-component data flow requires explicit Depot writes or an Egress→Ingress pair.

- **Probes with missing `attach_to`.** A Probe module without `attach_to` (or with an unresolvable target) runs after all other modules, potentially against a stale or missing DataFrame. The Compiler validates this, but programmatic callers bypassing `validate_probes` may hit a silent skip.

- **Immutable dataclasses across compile steps.** Every compilation pass returns a new frozen dataclass. Mutating a Module/Edge/Manifest in place will silently work (Python dataclasses aren't truly immutable) but breaks the provenance chain. Always use `dataclasses.replace()` and test with `FrozenInstanceError`.

## Bug-Family Prevention Rules

These rules come from the recurring failure patterns that caused the most fixes across releases. Each one prevented 2+ future bugs. The audit guide (`.claude/skills/code-audit/SKILL.md`) checks for violations of every rule below.

### Path anchoring
Every path-typed field in a schema model must use `Annotated[str, FsPath()]`. Every YAML parse must go through `parse_dict(base_dir=…)`, never round-trip through temp files. A relative path resolved against the wrong base directory (CWD, /tmp, an arcade file's dir instead of the parent blueprint's) has been the single most recurring bug class — tempfile detours, sandbox replay, and arcade expansion all hit it independently before `FsPath` anchoring made it structural.

### No silent no-ops
When you add a config field, a callback, a flag, or a schema field — trace it to every consumer. Code that executes but produces no effect and no error is worse than a crash because it silently lies to the user. Examples: a pydantic field with a docstring that no code reads (user thinks they're tuning behavior), a field in the schema that's accepted but never consumed at runtime (user sets it, nothing happens), a callback that's wired but silently skipped under a condition the caller doesn't know about.

### Falsy-trap on optional values
`if not x` on optional values must be `if x is None` unless every falsy value (`0`, `""`, `[]`, `{}`) is semantically identical to "not set." Empty dicts caught by `if not tt` (`time_travel: {}` was silently treated as no-op), empty strings caught by `or` merge (`""` provider fires the wrong fallback), zero caught by `if not count` (a legitimate count of 0 triggers the "do nothing" path). The safe default is `if x is None` everywhere except where you've explicitly reasoned through the falsy cases.

### Over-broad except must carry justification
`except Exception: pass` is allowed only with a comment explaining why silence is correct for *every* exception that could reach it. `except:` (bare) is forbidden. A silent catch that swallowed guardrail errors let invalid patches through; silent DDL migration failures accumulated stale schemas; silent JSON parse failures returned empty defaults with no diagnostic. Every such bug was a one-line comment away from being intentional instead of accidental.

### String-in-context transforms
Any regex or string transform applied to raw text for structural clean-up must verify the target text is outside quoted strings first, or run only as a recovery pass after strict parsing fails (never on valid input). A line-comment regex applied to raw JSON response text corrupted valid patches containing `//` inside string values (like `"value": "SELECT a // 2 FROM t"` was truncated to `"SELECT a"`). The fix: run comment-stripping only after `json.loads` fails, on the recovery path.

### Schema/template sync at change time
When you change a pydantic field, its key name, its nesting level, or its allowed values — update the corresponding template comment block in the same commit. A template showing `guardrails:` flat when the schema requires `agent.guardrails:` nested cost users parse errors when they uncommented the example. A renamed file (`SPARK_GUIDE.md` → `spark_guide.md`) left 8 compiler warnings pointing to dead links for an entire release.

### Import ordering
`from __future__ import annotations` must be the first import in every file (after the module docstring). An import placed above it raises `SyntaxError` whenever bytecode cache is cold — it passes CI (warm cache) and fails in production. Ruff I002 enforces this; the pre-commit hook catches it locally, CI catches it on push.

### Minimum-Python syntax (3.11) — no backslash in f-string expressions
The supported floor is **Python 3.11** (`requires-python = ">=3.11"`). Several newer conveniences are **`SyntaxError` on 3.11** even though they run on the dev/CI default interpreter (3.12+), so a `python -c "import …"` smoke check passes locally and the break only surfaces in a user's 3.11 venv. The one that has bitten us:
- **A backslash inside an f-string *expression* part** — `f"{click.style('▶', …)}"` or `f"{x.replace('\n',' ')}"`. 3.12 (PEP 701) allows it; **3.11 does not.** Backslashes in the *literal* portion of an f-string are fine (`f"a·b"`); only inside `{…}` they break. Fix by hoisting the sub-expression to a local: `arrow = click.style('▶', …)` then `f"{arrow} …"`.

Also avoid other 3.12-only syntax (PEP 695 `type X = …` aliases / `class C[T]` generics). The guard: the `py311-syntax` pre-commit hook runs `python3.11 -m py_compile` on staged files, and the `test-suite` / `version-matrix` CI jobs run on 3.11. Never rely on the local interpreter's version — it is newer than the floor.

### Constants, not literals
When a string value appears in 3+ files — especially if it's also a pydantic `Literal` or `StrEnum` — hoist it to a shared constant and import it. Bare strings like `"trigger_agent"`, `"abort"`, `"quarantine"` are compared against in 5+ files while the `StrEnum` that defines them sits unused in the same package. A typo in one comparison silently falls through to the wrong branch. The enum is the single source of truth; every comparison site imports it.

### Dict dispatch over fragile dispatch
When dispatching on a fixed set of types, prefer a `_DISPATCH` dict (add a type → one line) over a long if-elif chain where adding a type needs surgery in N parallel chains. This is a preference, not a rule — long if-elif chains are defensible when each branch does substantially different work and the type set is stable. The test: "when a new type is added, how many files need changes?" Dict dispatch → 1 file. If-elif → at least the chain file plus any parallel chains (test runner, openlineage, etc.). The existing if-elif chains in `executor.py`, `funnel.py` (mode dispatch), and `test_runner.py` are acceptable.

### CLI output speaks ONE vocabulary
All user-facing output goes through `aqueduct/cli/style.py` — never a raw `print()` and never `click.echo(click.style(...))` with hand-rolled colour/icons in a command. The single vocabulary:

- **Status lines** → `style.error` (`✗` red), `style.success` (`✓` green), `style.warn` (`⚠` whole-line yellow), `style.info` (dim `·` preamble). Do **not** colour only the icon and leave the text plain — that is the bug that produced a bold-yellow `⚠` with white text next to whole-line-yellow warnings.
- **Diagnostic warnings carry a suppressible `rule_id`.** A *known rule* is emitted as `warnings.warn(AqueductWarning, "[aqueduct:rule_id] message")` so `emit_warnings` renders the grouped `⚠ N warnings` block with `· [rule_id] msg` sub-lines and the user can `warnings.suppress: [rule_id]`. A new compile/config check **must** have a `rule_id`. (Runtime probe/assert warnings fire mid-execution via `logger.warning`, so they print inline — chronological, not grouped — but should still gain a `rule_id` as the warning-code surface is unified; the single entry point lives in `cli/output.py`.)
- **Errors exit through `exit_codes.*`** (never a bare int — separate rule above).
- **Logging** in text mode is rendered by `style.StyledLogFormatter` (`⚠`/`✗`/`·`, colour on a TTY only) — never reintroduce a `%(levelname)s:` format or a raw `WARNING:` `click.echo`.
- **`--format json`** output is structured data only — no colour, no icons, no styling.

Warning lifecycle / placement: **engine/config** warnings print above the `▶` run header, **blueprint/compile + session** warnings below it, **runtime** (probe/assert) warnings during execution. The header is the divider between "setup context" and "this run".

Drift to watch for (grep these before a commit that touches `cli/`): raw `print(` or `click.echo(click.style(...))` with hand-rolled colour in a command, `logger.warning` carrying no `rule_id`, a reintroduced `%(levelname)s:` / raw `WARNING:` log format, bare-int `sys.exit`. The consolidated output funnel lives in `cli/output.py` — compose through `emit()` / `warn()`, not raw `click.echo` / `style.*`.

## Audit Guide

Two audit surfaces exist; both check for violations of the prevention rules above. Run one after a phase completes or before a release. Gaps found in audits feed back into this document.

- **`skills/aqskill-audit*.md`** (repo root) — the Aqueduct-specific audit family. `aqskill-audit.md` orchestrates five domain skills (`-health`, `-tests`, `-code`, `-config`, `-style`) with ready-made `rg` detection commands, repo-specific false-positive neutralizers, and a mandatory verify-before-report gate. `aqskill-audit-health.md` carries the 17 detection patterns and a detection→prevention cross-reference table mapping each pattern to the AGENTS.md rule it checks. These are flat reference docs meant to be handed to (sub)agents — not auto-discovered Claude Code skills.
- **`.claude/skills/code-audit/`** — the generic intent-driven audit skill (`/code-audit`): reconstruct intent → diff against implementation → judge design choices. Its `references/` hold the generic detection cookbook and design-judgment library; `aqskill-audit-health.md` embeds the same corpus plus the Aqueduct-specific patterns — if you edit the shared prose, update both.

## Testing

### The three layers (every test carries exactly one marker)

| Marker | Layer | What lives here |
|---|---|---|
| `@pytest.mark.unit` | **Unit** | Fast, pure — no Spark, no network, no external services. The bulk of the suite. |
| `@pytest.mark.integration` | **Integration** | Blueprint/feature level: every `gallery/snippets/**` blueprint parses + compiles, `*.aqtest.yml` modules run on a real `local[1]` Spark, `*.aqscenario.yml` heals run with a **mocked** agent (no live LLM). |
| `@pytest.mark.e2e` | **E2E** | Full pipelines — the `gallery/showcase/**` setups end to end. |

Capability gates (`spark`, `agent`, `airflow`, `slow`) compose with a layer marker and skip when the dependency is absent.

### The test backlog is pytest-native — `TEST_MANIFEST.md` is RETIRED

`TEST_MANIFEST.md` (now frozen in `docs/archive/`) is gone. The suite itself is the source of truth for what passes; do **not** maintain a parallel ledger. Track gaps in code:

- **Unwritten test** → add a `@pytest.mark.todo("what input → what output/error")` stub to **`tests/test_backlog.py`** — the single low-friction landing zone (keeps the manifest's one-place-to-append ergonomics). Give it an `intended:` line (where the real test should live) + a `context:` note. It auto-skips; `pytest --collect-only -m todo` is the living backlog. When you implement it, write the body and **move it to the `intended:` path**, deleting the stub from `test_backlog.py`. Never flip a status by hand.
- **Known bug / regression to fix** → write the test that *should* pass and mark it `@pytest.mark.xfail(strict=True, reason="bug: …")` (in `test_backlog.py` or in place). `xfail_strict` is on, so the build FAILS the moment the bug is fixed, forcing the marker's removal. ❌→✅ maintains itself.
- **Enforcement** → `tests/test_meta_quality.py::test_no_zero_assertion_tests` fails the build if any test verifies nothing (no `assert` / `raises` / `warns` / mock-assert / asserting-helper) and isn't a `todo`/`xfail` stub. A test that runs code but checks no outcome is the cheating it catches.

When fixing a bug, add a regression test capturing the broken→expected behavior so it can't recur silently.

### Two-model split
- **You (Claude)** write core implementation + the high-value behavior tests. Leave `@pytest.mark.todo` stubs for coverage you're deferring; optionally drop issue files in `.dev/ISSUES/` for the cheaper model.
- **Cheaper model** fills in `todo` stubs and broadens coverage — it reads the stubs (`pytest --collect-only -m todo`) and `.dev/ISSUES/`.
- **Never suggest or run test commands.** The user runs the suite. Paste specific failures only if stuck.

### CI workflow (`.github/workflows/test-suite.yml`)

CI runs scoped parallel jobs, each owning a feature area.  A `changes` job
(using `dorny/paths-filter@v4`) detects which files changed; on branches only
matching jobs fire.  On `main` every job runs unconditionally.  Triggers:
push to `feat/**` or `phase/**`, and PRs into `main`/`feat/**`/`phase/**`.

| Job | Runs when | Command |
|---|---|---|
| `parser-tests` | `aqueduct/parser/**` or `tests/test_parser/**` | `pytest tests/test_parser/ -m "not spark"` |
| `compiler-tests` | `aqueduct/compiler/**` or `tests/test_compiler/**` | `pytest tests/test_compiler/ -m "not spark"` |
| `executor-tests` | `aqueduct/executor/**`, `tests/test_executor/**`, `tests/test_compiler/test_blueprints.py`, or `gallery/snippets/**` | `pytest tests/test_executor/ tests/test_compiler/test_blueprints.py -m spark` |
| `snippets` | same as executor | `bash scripts/run_snippets.sh` (full-runs each snippet, slow) |
| `gallery-tests` | `gallery/**`, `tests/test_gallery.py`, parser/compiler | `pytest tests/test_gallery.py` (fast parse/compile/load guard) |
| `surveyor-tests` | `aqueduct/surveyor/**` or `tests/test_surveyor/**` | `pytest tests/test_surveyor/ tests/test_benchmark_store.py` |
| `agent-tests` | `aqueduct/agent/**` or `tests/test_agent/**` | `pytest tests/test_agent/` |
| `patch-tests` | `aqueduct/patch/**` or `tests/test_patch/**` | `pytest tests/test_patch/` |
| `cli-tests` | `aqueduct/cli/**` or `tests/test_cli/**` | `pytest tests/test_cli/` |
| `drift-tests` | `aqueduct/drift/**` or `tests/test_drift/**` | `pytest tests/test_drift/ -m "not spark"` |
| `tui-tests` | `aqueduct/tui/**` or `tests/test_tui/**` | `pytest tests/test_tui/ -m "not spark"` |
| `config-tests` | `aqueduct/config.py`, `redaction.py`, `secrets.py`, `warnings.py`, or their tests | `pytest tests/test_config.py ...` |
| `stores-tests` | `aqueduct/stores/**`, `tests/test_stores/**`, `tests/test_depot/**` (PG + Redis services) | `pytest ... -m integration` |
| `coverage` | `main` pushes + all PRs | `pytest --cov=aqueduct --cov-fail-under=70 -m "not spark"` |

**Branch workflow**: push a change touching only `aqueduct/agent/` → only
`agent-tests` fires (~30s).  Merge to `main` → every job runs (full gate).

**New area**: if you add a new top-level directory under `aqueduct/`, add
its path glob to the `changes` job filter and add a corresponding test job.

### Release (`.github/workflows/release.yml`)

Pushing a **bare semver tag** (`1.2.3`, no `v` prefix) gates on the full suite,
builds sdist+wheel, publishes to PyPI via **Trusted Publishing** (OIDC, no stored
token), then cuts a GitHub Release titled `Aqueduct X.Y.Z` whose body is that
version's `CHANGELOG.md` section.  The `build` job asserts the tag equals
`pyproject` `project.version`.  Release the user's call — never tag or publish
unprompted.

### Testing constraints (reminder)
- **No live LLM calls in pytest.** Agent tests mock `httpx.post` or `_call_agent`. Live-model evaluation belongs to `.aqscenario.yml` scenarios.
- **No mocking the SparkSession** for executor tests — use the real `spark` fixture.
- **Framework**: `pytest`, `pytest-cov` (70% minimum), `pre-commit` with `black` and `ruff`.
- **Fixtures** in `tests/fixtures/`. Use `pytest.raises(match=...)` for validation errors.
- **Immutability**: test `FrozenInstanceError` on dataclass mutation attempts.
- **Test env vars**: `AQ_SPARK_MASTER` (default `local[1]`), `AQ_OLLAMA_URL` (default `http://localhost:11434`; tests skip if unreachable).
