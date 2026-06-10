# AGENTS.md — Aqueduct Development Guidebook

## Project Context
Aqueduct is a declarative Spark blueprint engine with LLM-driven self-healing.

## Documentation map

`docs/specs.md` is the **engine reference** — Blueprint format, architecture, module semantics, type system, self-healing grammar. The other docs are specialised and own their own surfaces; specs.md cross-references them rather than duplicating their content.

| Doc | Owns | When to read |
|---|---|---|
| `docs/specs.md` | Blueprint format, architecture (4-layer), Modules §4, Context Registry §5, Lineage §7, Self-Healing & Agent §8, Type System §9, Spark Integration §10, Engine Scope §13 | Domain semantics, anything user-facing about the engine itself |
| `docs/cli_reference.md` | Every CLI command and flag with defaults | Touching `@click.option` / new subcommand in `aqueduct/cli.py`, or answering "what flag does X" |
| `docs/observability_guide.md` | Store schemas (run_records, heal_attempts, healing_outcomes, failure_contexts, column_lineage, benchmark_results, patch_simulation, signal_overrides, explain_snapshot, probe_signals, module_metrics, maintenance_metrics, depot_kv) + diagnostic SQL cookbook | DDL / `ALTER TABLE` changes in `aqueduct/surveyor/` or `aqueduct/executor/`, or writing post-mortem queries |
| `docs/spark_guide.md` | Compiler warnings, performance, tuning, Spark behavior gotchas | Modifying Executor modules, adding Channel ops, debugging Spark perf |
| `docs/production_guide.md` | Cluster deployment, env config, Spark cluster config, path conventions, danger settings, Delta operational notes, production patch lifecycle, security, readiness checklist | Anything related to running Aqueduct on a cluster (k8s, YARN, Databricks, …) |
| `docs/compatibility.md` | Python × Spark support matrix, cloudpickle constraint, production pinning | Changing version pins in `pyproject.toml`, or answering "does X version combo work" |
| `docs/roadmap.md` | Deferred / aspirational items removed from specs.md: streaming, resume-from, MCP, ML inference, Flink, multi-pipeline orchestration | Discussing scope or future direction; never write fresh "deferred" prose into specs.md — push to roadmap.md instead |

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

## Code Organization & Safety
- **4-layer boundary**: `Parser` → `Compiler` → `Executor` → `Surveyor`. Put logic in the correct layer. Only modify the layer relevant to the task. Topological sort, Probe insertion, and parallel-component detection are sub-steps inside the Executor — not a separate "Planner" layer.
- **Dual-format contract**: Humans write YAML (`Blueprint`); engine consumes JSON (`Manifest`, `FailureContext`).
- **Zero-cost observability**: Never insert `count()`, `show()`, or `collect()` Spark actions into the critical execution path (e.g. inside Probes). Use SparkListener metrics or explicit sampling.
- **Spillway rules**: Transform channels and UDFs must use `try/except` (or Spark `try_*` functions) to catch row-level errors and populate `_aq_error_*` columns without aborting the Spark stage.
- **Immutability**: `@dataclass(frozen=True)` on all internal representations (`Module`, `Edge`, `Manifest`). Each compilation step returns a new immutable object.
- **UDF bodies are out of scope for self-healing.** Aqueduct's PatchSpec grammar cannot modify UDF bodies (Python/Scala/Java code in the `udfs:` block). A pipeline failing because of a bug in UDF logic is a `defer_to_human` situation — the agent diagnoses the UDF as the root cause but cannot rewrite its implementation. This is by design: arbitrary code modification breaks the P5 principle (patch grammar over codegen).

## Executor Architecture (Extras Pattern)
- `aqueduct/executor/models.py` — engine-agnostic (`ExecutionResult`, `ModuleResult`)
- `aqueduct/executor/spark/` — all Spark code (`ingress`, `egress`, `channel`, `executor`, `junction`, `funnel`, `probe`, `session`, `udf`, `assert_`)
- `aqueduct/executor/__init__.py` — `get_executor(manifest, config)` factory

When adding a Spark feature: code in `aqueduct/executor/spark/`. Do not import `pyspark` in `parser`, `compiler`, `surveyor`, `patch`, or `depot`.

**Documented exception (doctor):** the `aqueduct/doctor/` package lazily imports `pyspark` inside three check functions (`check_spark`, `check_storage`, `check_cloudpickle_compat`, all in `doctor/__init__.py`). Top-level `import aqueduct.doctor` must never pull `pyspark` — keep these imports inside function bodies so `--skip-spark` and the `[spark]`-less install path stay viable. The package splits leaf connectivity checks (`doctor/checks_io.py`) from the spark/network/blueprint-source cluster + `run_doctor` (`doctor/__init__.py`); the latter stay together because the test suite monkeypatches several by their `aqueduct.doctor.<name>` path and they call each other by bare global name. `doctor/base.py` holds `CheckResult` + `_ms` to avoid a circular import. Public names re-export from `__init__`, so `from aqueduct.doctor import <check>` is unchanged.

**Documented exception (surveyor):** `surveyor/surveyor.py::_extract_structured_error` lazily imports `from pyspark.errors import PySparkException` inside a `try/except` (falling back to `None` when absent). This is required — structured root-cause extraction must recognise Spark's own exception types — but it is **guarded**: the import lives in the function body, so top-level `import aqueduct.surveyor` stays `pyspark`-free and the `[spark]`-less install path is preserved. Do not promote this to a module-level import.

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

`docs/specs.md` is the **engine reference** for semantics that don't belong elsewhere. Production / CLI / observability / Spark-tuning details now live in their dedicated guides (see Documentation map). Phase / sprint / development artefacts (`Phase 35`, `Sprint 7`, `Task NN`, `pre-30a`, `deferred to Phase NN`, etc.) belong **only** in `CHANGELOG.md` and `TODOs.md`. Never in: source code (`aqueduct/**/*.py`), docs (`docs/**/*.md`), templates (`aqueduct/templates/**`), gallery (`gallery/**`), or user-facing scaffolding (`README.md`, `CONTRIBUTING.md`). Verify with `grep -rnE "Phase [0-9]|Sprint [0-9]|Task [0-9]" aqueduct/ docs/ gallery/ README.md CONTRIBUTING.md` before commits that touched any of those surfaces.

| If you change … | You must update … |
| :- | :- |
| Any DDL or `ALTER TABLE` in `aqueduct/surveyor/` or `aqueduct/executor/` | `docs/observability_guide.md` schema table; if the new column enables a meaningful diagnostic, add a cookbook recipe (When → What you learn → What to do next) |
| Any `@click.option` / new sub-command in `aqueduct/cli.py` | `docs/cli_reference.md` flag table (include default value) |
| Any pydantic field in `aqueduct/config.py` or `aqueduct/parser/schema.py` | The corresponding template comment block (`aqueduct.yml.template` for engine config, `blueprints/blueprint.yml.template` for Blueprint) |
| Any `StopReason`, `BudgetConfig`, or apply-gate behaviour | `docs/specs.md` §8 + `docs/observability_guide.md` `heal_attempts` section |
| Any production / deployment / danger-setting / cluster-config detail | `docs/production_guide.md` |
| Any Spark compiler-warning, performance, or tuning behaviour | `docs/spark_guide.md` |
| Any change to `pyproject.toml` version pins or supported Python/Spark range | `docs/compatibility.md` |
| Any newly deferred or aspirational item (NOT a current phase) | `docs/roadmap.md` — never inline "this is deferred" prose into `specs.md` |
| Any new file under `docs/` | `README.md` References list + this Documentation map |
| Any new flag, command, or behaviour visible from the CLI | `docs/cli_reference.md` |
| Any user-facing engine semantic (architecture, module semantics, configs, gates, runtime behaviour) NOT covered by a dedicated guide above | `docs/specs.md` — NO `Phase NN` artefacts |
| Any new testable feature | `tests/TEST_MANIFEST.md` checklist item |
| Any phase / sprint / shippable change | `CHANGELOG.md` `[Unreleased]` only — never bump version, never add a versioned header (user controls release timing) |
| Any new `@aq.*` function in `aqueduct/compiler/runtime.py` | `docs/specs.md` §5.3 function table + `_DISPATCH` table in `runtime.py` |
| Any new path-key entry in `aqueduct/executor/path_keys.py` | The module-type's schema model in `aqueduct/parser/schema.py` (mark path fields with `Annotated[str, FsPath()]`) |
| Any new exit code in `aqueduct/exit_codes.py` | `docs/cli_reference.md` exit-code reference |
| Any new CLI exit path (`sys.exit(...)` in `aqueduct/cli.py`) | Use a named `exit_codes.*` constant — never a bare int (only `sys.exit(130)` for SIGINT is exempt). Classify by the contract docstring in `aqueduct/exit_codes.py`: config/schema/danger-policy → `CONFIG_ERROR`; runtime/data/missing-file/subprocess/record-not-found → `DATA_OR_RUNTIME`; bad-flag / missing-arg → `USAGE_ERROR`; staged-patch → `HEAL_PENDING`; non-interactive gate rejection → `VALIDATION_GATE`. A new *value* added to `exit_codes.py` is a v1.0-contract change → also update `CHANGELOG.md` |

## Source Code Navigation Map

This section documents the internal module structure of key packages. Updated
whenever a package is restructured — use it as the first filter before grepping.

### `aqueduct/surveyor/` — Observability, benchmarking, webhooks

| Module | What it owns |
|--------|--------------|
| `surveyor.py` | Main Surveyor class: start/record/stop lifecycle, DDL, structured error extraction |
| `models.py` | Frozen dataclasses: `RunRecord`, `FailureContext` (the LLM agent's input) |
| `scenario.py` | Scenario benchmark framework: `load_scenario`, `_build_failure_ctx`, effect-based grader |
| `webhook.py` | HTTP dispatch in daemon thread, `${VAR}` template rendering, redaction |
| `benchmark_store.py` | DuckDB persistence + regression detection for benchmark results |
| `blob_store.py` | Zstd externalisation of fat columns (manifest_json, provenance_json, stack_trace) |

### `aqueduct/patch/` — PatchSpec grammar + apply + validation gates

| Module | What it owns |
|--------|--------------|
| `grammar.py` | `PatchSpec` Pydantic v2 model, 13 operation types, discriminated union |
| `operations.py` | Per-op implementations against Blueprint dict, ruamel YAML round-trip |
| `apply.py` | Apply orchestrator: load → deep-copy → apply ops → re-parse → archive |
| `preview.py` | Lineage gate (Gate 2) + sandbox gate (Gate 3): diff column impact, sandbox replay |
| `explain_gate.py` | Plan regression gate (Gate 4): compare Exchange/Broadcast counts before vs after |
| `__init__.py` | Module description only |

### `aqueduct/stores/` — Pluggable store backends

| Module | What it owns |
|--------|--------------|
| `base.py` | ABCs for `ObservabilityStore`, `LineageStore`, `DepotStore` + `RelationalCursor` (`?` → `%s`), `StoreBundle` factory |
| `duckdb_.py` | DuckDB implementations (single-file embeddable) |
| `postgres.py` | Postgres implementations (connection-pool dedup, schema-per-store) |
| `redis_.py` | Redis depot KV (high-QPS watermark reads) |

### `aqueduct/depot/` — Cross-run KV state

| Module | What it owns |
|--------|--------------|
| `depot.py` | `DepotStore` façade — delegates to whichever store backend is configured |
| `__init__.py` | Empty |

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
| `probe.py` | Signals: schema, null_rates, distribution, distinct, freshness, row_count |
| `assert_.py` | Quality gates: min_rows, null_rate, freshness, sql, sql_row, spillway_rate |
| `session.py` | SparkSession management, Delta conf, cloudpickle patch |
| `udf.py` | UDF registry, cloudpickle compatibility |
| `metrics.py` | Zero-extra-action observe() wrapper, Hadoop FS byte count |
| `test_runner.py` | Isolated module test framework (aqueduct test CLI) |

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
| `__init__.py` | `AgentPatchResult`, `PROMPT_VERSION`, `MAX_REPROMPTS`, `generate_agent_patch`, `stage_patch_for_human`, `archive_patch`, `build_prompt`, `resolve_budget` | Thin re-export facade + `resolve_budget()` |
| `loop.py` | `generate_agent_patch`, `stage_patch_for_human`, `archive_patch`, `AgentPatchResult`, `PROMPT_VERSION` | Main orchestration loop, patch I/O, result type |
| `prompts.py` | `build_prompt`, `_build_user_prompt`, `_build_system_prompt`, `_FIELD_ALIASES`, `_VALID_OPS` | Template strings, prompt construction, LLM-facing constants |
| `providers.py` | `_call_agent`, `_call_anthropic`, `_call_openai_compat`, `_ProviderConfig` | HTTP dispatch to Anthropic / OpenAI-compatible endpoints |
| `parse.py` | `_parse_patch_spec`, `_detect_structural_error`, `_format_reprompt_error`, `_format_reprompt_for_next_turn` | Response parsing, structural error detection, reprompt formatting |
| `budget.py` | `BudgetConfig`, `BudgetTracker`, `AttemptRecord`, `DEFAULT_BUDGET` | Multi-axis budget tracking for the reprompt loop |
| `signature.py` | `ErrorSignature`, `from_*` helpers | Error signature engine (stable dedup hash for budget + coaching) |

**When adding a feature:**
- New LLM provider → add `_call_<provider>()` in `providers.py`, wire in `_call_agent()`
- New prompt rule → edit `_SYSTEM_PROMPT_TEMPLATE` in `prompts.py`, bump `PROMPT_VERSION` in `loop.py`
- New recovery pattern → add to `_parse_patch_spec()` in `parse.py`
- New budget axis → add to `BudgetConfig` / `BudgetTracker` in `budget.py`
- New patch lifecycle event → add to `loop.py`, re-export from `__init__.py`

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
2. For each logical change in sequence: apply files, `git add -A`, `git commit -m "type(scope): message"`
3. When phase is complete: `git checkout main && git merge phase/NNN-NNN-name --ff-only`
4. Preserve branch: `git branch phase/NNN-NNN-name HEAD`

## Common Pitfalls

- **Silent `@aq.depot.get()` when depot is unconfigured.** If a Blueprint references `@aq.depot.get('watermark')` but no depot backend is configured, the call returns `""` silently. Incremental pipelines will re-read all source data every run. Always configure a depot when using incremental Channels, and verify with `aqueduct doctor`.

- **`spark.catalog.dropTempView` in `_write_merge`.** The Delta merge path in `egress.py` drops a temp view before creating it. On first merge the view doesn't exist — the call raises `AnalysisException`. Always guard `dropTempView` with try/except or check `tableExists()` first.

- **Frame store is scoped per parallel component.** In `--parallel` mode, modules in different connected components cannot access each other's frame-store keys. Cross-component data flow requires explicit Depot writes or an Egress→Ingress pair.

- **Probes with missing `attach_to`.** A Probe module without `attach_to` (or with an unresolvable target) runs after all other modules, potentially against a stale or missing DataFrame. The Compiler validates this, but programmatic callers bypassing `validate_probes` may hit a silent skip.

- **Immutable dataclasses across compile steps.** Every compilation pass returns a new frozen dataclass. Mutating a Module/Edge/Manifest in place will silently work (Python dataclasses aren't truly immutable) but breaks the provenance chain. Always use `dataclasses.replace()` and test with `FrozenInstanceError`.

## Testing Workflow (Two-Model Split)
- **You (Claude)** write core implementation. Add checklist items to `tests/TEST_MANIFEST.md` when adding testable features. Optionally create issue files in `.dev/ISSUES/` for the cheaper model.
- **Cheaper model** handles test generation — reads `tests/TEST_MANIFEST.md` and `.dev/ISSUES/`.
- **Never suggest or run test commands.** User handles all test execution. Only paste specific failures if stuck.

### When to add to TEST_MANIFEST.md

Every new feature, bug fix, or behavioral change that is testable should add a `⏳` item under the relevant module section. The entry should describe the exact behavior being tested: what input, what output, what error. Follow the existing `✅` / `⏳` / `❌` convention. Mark items `✅` only when the user confirms the test passes — never self-mark.

When fixing a bug, add a regression test entry that captures the *broken* behavior (what was happening before the fix) and the *expected* behavior (what happens after). This prevents the same bug from recurring without a test catching it.

### CI workflow (`.github/workflows/test-suite.yml`)

CI runs 9 parallel jobs, each scoped to a feature area.  A `changes` job
(using `dorny/paths-filter@v3`) detects which files changed; on branches
only matching jobs fire.  On `main` every job runs unconditionally.

| Job | Runs when | Command |
|---|---|---|
| `parser-tests` | `aqueduct/parser/**` or `tests/test_parser/**` | `pytest tests/test_parser/` |
| `compiler-tests` | `aqueduct/compiler/**` or `tests/test_compiler/**` | `pytest tests/test_compiler/` |
| `executor-tests` | `aqueduct/executor/**`, `tests/test_executor/**`, or `tests/test_blueprints.py` | `pytest ... -m spark` |
| `surveyor-tests` | `aqueduct/surveyor/**` or `tests/test_surveyor/**` | `pytest tests/test_surveyor/ tests/test_benchmark_store.py` |
| `agent-tests` | `aqueduct/agent/**` or `tests/test_agent/**` | `pytest tests/test_agent/` |
| `patch-tests` | `aqueduct/patch/**` or `tests/test_patch/**` | `pytest tests/test_patch/` |
| `cli-tests` | `aqueduct/cli.py` or `tests/test_cli/**` | `pytest tests/test_cli/` |
| `config-tests` | `aqueduct/config.py`, `redaction.py`, `secrets.py`, `warnings.py`, or their tests | `pytest tests/test_config.py ...` |
| `stores-tests` | `aqueduct/stores/**` or `tests/test_stores/**` (PG + Redis services) | `pytest ... -m integration` |

**Branch workflow**: push a change touching only `aqueduct/agent/` → only
`agent-tests` fires (~30s).  Merge to `main` → all 9 jobs run (full gate).

**New area**: if you add a new top-level directory under `aqueduct/`, add
its path glob to the `changes` job filter and add a corresponding test job.

### Testing constraints (reminder)
- **No live LLM calls in pytest.** Agent tests mock `httpx.post` or `_call_agent`. Live-model evaluation belongs to `.aqscenario.yml` scenarios.
- **No mocking the SparkSession** for executor tests — use the real `spark` fixture.
- **Framework**: `pytest`, `pytest-cov` (80% minimum), `pre-commit` with `black` and `ruff`.
- **Fixtures** in `tests/fixtures/`. Use `pytest.raises(match=...)` for validation errors.
- **Immutability**: test `FrozenInstanceError` on dataclass mutation attempts.
- **Test env vars**: `AQ_SPARK_MASTER` (default `local[1]`), `AQ_OLLAMA_URL` (default `http://localhost:11434`; tests skip if unreachable).
