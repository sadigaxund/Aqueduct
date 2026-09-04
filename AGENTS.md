# AGENTS.md — Aqueduct Development Guidebook

## Project Context
Aqueduct is a declarative Spark blueprint engine with LLM-driven self-healing.

## Documentation map

`docs/specs.md` is the **engine reference** — Blueprint format, architecture, module semantics, type system, self-healing grammar. The other docs are specialised and own their own surfaces; specs.md cross-references them rather than duplicating their content.

| Doc | Owns | When to read |
|---|---|---|
| `docs/specs.md` | Blueprint format, architecture (4-layer) §3, Modules §4, Context Registry §5, Lineage §7, Self-Healing & Agent §8, Type System §9, Deployment & Engine Integration §10 (incl. §10.9 engines + capability framework), Engine Scope §11 | Domain semantics, anything user-facing about the engine itself |
| `docs/cli_reference.md` | Every CLI command and flag with defaults | Touching `@click.option` / new subcommand in `aqueduct/cli/`, or answering "what flag does X" |
| `docs/observability_guide.md` | Store schemas (run_records, heal_attempts, healing_outcomes, failure_contexts, column_lineage, benchmark_results, patch_simulation, signal_overrides, probe_signals, module_metrics, maintenance_metrics, depot_kv, patch_index, drift_checks, channel_fingerprints) + diagnostic SQL cookbook | DDL / `ALTER TABLE` changes in `aqueduct/surveyor/` or `aqueduct/executor/`, or writing post-mortem queries |
| `docs/spark_guide.md` | Compiler warnings, performance, tuning, Spark behavior gotchas | Modifying Executor modules, adding Channel ops, debugging Spark perf |
| `docs/production_guide.md` | Cluster deployment, env config, Spark cluster config, path conventions, danger settings, Delta operational notes, production patch lifecycle, security, readiness checklist | Anything related to running Aqueduct on a cluster (k8s, YARN, Databricks, …) |
| `docs/compatibility.md` | Python × Spark support matrix, cloudpickle constraint, production pinning | Changing version pins in `pyproject.toml`, or answering "does X version combo work" |
| `docs/extending.md` | Engine-author guide: `ExecutorProtocol`, the capability-declaration workflow (scaffold/sync/check/docs), entry-point registration, the two error types, layer rules, testing expectations. Carries the explicit ExecutorProtocol-is-unstable warning. | Adding or modifying an execution engine, or changing `aqueduct/executor/protocol.py` / `capabilities.py` / `capability_tooling.py` |
| *(none)* | `docs/roadmap.md` was removed; deferred/rejected-work notes now live in the issue tracker, not a repo doc | Discussing scope or future direction; never write fresh "deferred" prose into specs.md |
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
- **Engine leaves:** `spark`, `duckdb` — one execution engine each. An engine
  leaf is distinct from a per-vendor leaf: it is not wrapping a vendor SDK, it
  is registering an execution engine — the thing that runs a compiled
  blueprint. What makes an extra an engine leaf (mechanically, not just by
  name) is that it registers an `aqueduct.engines` entry point (see
  `[project.entry-points."aqueduct.engines"]`) and ships its own
  `capabilities.yml` declaring what that engine supports. Both engine leaves
  are pulled into `all`, same as every other leaf, but neither rolls into
  `secrets`, `stores`, or `schedulers` — there is no "engines" aggregate,
  because installing more than one engine at a time is a normal, expected
  configuration (not an aggregate-of-convenience the way `secrets` bundles
  every cloud vendor's SDK).

A user installs an aggregate or a leaf. When a new optional dependency appears,
map it onto an existing axis: reuse a vendor leaf if it's that vendor's SDK, or
add a leaf that rolls up into the right aggregate. A genuinely new vendor → new
leaf + add it to its aggregate. Never a standalone feature flag. (Example:
Phase 53's object store became the `object-store` leaf inside `stores`.)

**Documented exception — dev-tooling extras.** The two-axis rule governs
*runtime-capability* deps (vendor SDKs / store backends used while a pipeline
runs). A small separate class is allowed for **developer/inspection tooling that
never runs in the data path**: `dev` (pytest/black/ruff) and `dashboard`
(`streamlit`+`plotly`, for `aqueduct dashboard` — a local, read-only, on-demand
observability viewer like the Spark UI). These stay OUT of `all` and out of the runtime axes — a
pipeline never needs them, so bundling them into base/`all` would bloat headless
Spark-driver / CI installs. This is the *only* sanctioned feature-named-extra
category; it is not a loophole for runtime features (those still follow the axes).

## Code Organization & Safety
- **4-layer boundary**: `Parser` → `Compiler` → `Executor` → `Surveyor`. Put logic in the correct layer. Only modify the layer relevant to the task. Topological sort, Probe insertion, and parallel-component detection are sub-steps inside the Executor — not a separate "Planner" layer.
- **Dual-format contract**: Humans write YAML (`Blueprint`); engine consumes JSON (`Manifest`, `FailureContext`).
- **Zero-cost observability**: Never insert `count()`, `show()`, or `collect()` Spark actions into the critical execution path (e.g. inside Probes). Use SparkListener metrics or explicit sampling. When an observability feature genuinely cannot be built without an extra action, it ships behind a named flag in the `metrics:` block of `aqueduct.yml`, defaulted to `false` (production stays zero-cost), and `execute()` emits a startup warning naming the flag whenever it is on. Silent performance degradation is the thing being prevented — a user must never pay for an extra scan without being told which setting bought it. This governs *added* actions only: `metrics.use_observe` defaults to `true` because `DataFrame.observe()` attaches to a pass the run already makes and adds no action.
- **Spillway rules**: Transform channels and UDFs must use `try/except` (or Spark `try_*` functions) to catch row-level errors and populate `_aq_error_*` columns without aborting the Spark stage.
- **Immutability**: `@dataclass(frozen=True)` on all internal representations (`Module`, `Edge`, `Manifest`). Each compilation step returns a new immutable object.
- **UDF bodies are out of scope for self-healing.** Aqueduct's PatchSpec grammar cannot modify UDF bodies (Python/Scala/Java code registered via the top-level `udf_registry:` block). A pipeline failing because of a bug in UDF logic is a `defer_to_human` situation — the agent diagnoses the UDF as the root cause but cannot rewrite its implementation. This is by design: arbitrary code modification breaks the P5 principle (patch grammar over codegen).
- **Every extensibility seam is an entry-point group plus an allowlist.** When a new kind of user-supplied code needs to plug in, the pattern is fixed: a setuptools entry-point group named `aqueduct.<family>` for discovery, and — wherever that plugin code executes on the driver — an explicit allowlist in `aqueduct.yml` naming what may load. Discovery alone is not authorization; an installed package must never gain execution simply by being importable. Existing instances: `aqueduct.engines` (`executor/capabilities.py::load_engines`), `aqueduct.probe_signals` (`executor/probe_plugins.py`), the reserved `aqueduct.tools` group (`tools/registry.py`), and the planned `aqueduct.actuators` for Phase 82 domains 3/5. Follow the shape rather than inventing a fifth mechanism. **The patch grammar is not a seam** — `PatchSpec` ops are a closed list, permanently, because the gates' replayability depends on the op set being finite and known.
- **Trace every consumer before changing a type or output path.** When you change a field's type (e.g. `str` → `str | None`), a function's output destination (e.g. stdout → stderr), or a sentinel value, grep for every call site and every attribute access on that field FIRST — before making the change. A `Path(None)` crash, an `UnboundLocalError` from a scoped import, or a test assertion on the old sentinel each cost multiple fix rounds that a 30-second grep would have avoided.

## Executor Architecture (Extras Pattern)
- `aqueduct/executor/models.py` — engine-agnostic (`ExecutionResult`, `ModuleResult`)
- `aqueduct/executor/path_keys.py` — engine-agnostic path-field registry (imported by the parser)
- `aqueduct/executor/probe_plugins.py` — engine-agnostic, **pyspark-free** custom-probe-signal resolver (`custom_signal_source`, `resolve_callable`, `AQ_PROBE_ENTRYPOINT_GROUP`). Lives at the top level (not under `spark/`) so the compiler (`wirer`) can validate `type: custom` signal shape and the Spark `probe.py` can resolve callables at runtime — same precedent as `path_keys.py`. Do not add `pyspark` imports here.
- `aqueduct/executor/channel_ops.py` — engine-agnostic, **pyspark-free** Channel op-name registry (`SQL_OPS`, `SINGLE_INPUT_OPS`, `MULTI_INPUT_OPS`, `ALL_OPS`). `spark/channel.py` imports pyspark at module level, so its op constants had to be hoisted here — same precedent as `path_keys.py`/`probe_plugins.py` — so the capability-leaf walker (below) can read them without a Spark install.
- `aqueduct/executor/probe_sampling.py` — engine-agnostic, **pyspark-free** shared `ProbeSampling` frozen dataclass (`max_sample_rows`, `default_sample_fraction`, `sample_rows_keep_last_n`) — hoisted out of `spark/probe.py` (its former sole owner, with `duckdb_/probe.py` carrying a deliberate copy) so `aqueduct/cli/run_setup.py` can build every engine's sampling config without importing a Spark module — same precedent as `path_keys.py`/`channel_ops.py`. Both engine probe modules re-export `ProbeSampling` from here so existing `aqueduct.executor.spark.probe.ProbeSampling` / `aqueduct.executor.duckdb_.probe.ProbeSampling` importers keep working unchanged.
- `aqueduct/executor/capabilities.py` — engine-agnostic, **pyspark-free** capability model (Phase 78): `Support` (`SUPPORTED`/`UNSUPPORTED`/`IGNORED_WITH_WARNING`), frozen `Capability` (verdict + optional `requires` version constraint + `hint`), frozen `EngineCapabilities` (one engine's leaf-id → `Capability` table, `.verdict()`), `CAPABILITY_REGISTRY`/`register()`/`get_capabilities()`, `load_declaration()` (load + HARD-VALIDATE an engine's YAML declaration; there is deliberately **no** default-verdict sweep — see the rule below), and a minimal PEP440-lite specifier parser/evaluator (`validate_specifier`, `version_satisfies`) — `packaging` is not a declared dependency, so this stays hand-rolled rather than importing `packaging.specifiers`. Phase 78 Step 1 adds `load_engines()` — idempotently resolves the `aqueduct.engines` setuptools entry-point group (`AQ_ENGINES_ENTRYPOINT_GROUP`), the seam a future engine (DuckDB) registers through with zero core edits. `get_capabilities()` calls it before the registry lookup and raises `UnknownEngineError` (a `CompileError` subclass in `aqueduct/errors.py`, not a `KeyError`) for an unregistered engine — the framework fails closed instead of degrading to a silent no-op gate. Two error-taxonomy rules apply here and must not be regressed: (a) callers distinguish "unregistered engine" from "blueprint failed to compile" by exception TYPE (`UnknownEngineError`, whose `.engines` list also flags the empty-registry/stale-install case via `.no_engines_registered`), NEVER by matching the message text; (b) a failing `aqueduct.engines` entry point raises `EnginePluginError` (an `AqueductError`) naming the entry point, so a broken third-party engine plugin never escapes as a bare `ImportError` from `aqueduct.yml` loading. Because engine validation is fail-closed, the empty-registry state carries its own actionable message (`NO_ENGINES_HINT` — entry points invisible, reinstall) instead of "Registered engines: []".
- `aqueduct/executor/capability_leaves.py` — engine-agnostic, **pyspark-free** grammar leaf walker (Phase 78): `all_leaves()` derives the canonical capability-leaf-id set by introspecting `parser/schema.py` pydantic models (module types + nested config blocks) and reading the op/mode/format constants next to their dispatch code (`channel_ops.py`, `spark/egress.py`'s `SUPPORTED_MODES`/`ON_NEW_COLUMNS_POLICIES`, `spark/junction.py`'s `VALID_MODES`, `spark/funnel.py`'s `VALID_MODES`). `INGRESS_FORMATS`/`EGRESS_FORMATS`/`FEATURE_FLAGS` are the hand-curated exceptions (not schema-derivable — see the module docstring for why). This is the anti-drift core: a new schema field / op / mode automatically becomes a leaf that every registered engine must give a verdict for (enforced by `tests/test_capabilities/test_closure.py`).
- `aqueduct/executor/config_leaves.py` — engine-agnostic, **pyspark-free** ENGINE-CONFIG leaf walker (Phase 78 Step 3; Q4 step 2 added the scoping below). `all_config_leaves(engine=None)` derives the ENGINE-SCOPED `config.*` checklist by introspecting `aqueduct/config.py`'s `AqueductConfig` pydantic models (recursing into nested `BaseModel` fields; a `list[Model]`/`dict[str, Model]` field like `stores.depots`/`agent.cascade` is one atomic leaf, not enumerated per key), filtered to fields carrying an EXPLICIT `Field(..., json_schema_extra={"engine_scoped": True})` tag — every field must carry `True` or `False`, there is no untagged/implicit state. `engine=<name>` additionally drops every OTHER engine's `engine.<name>.*` leaves (positionally owned by exactly one engine — derived from the path segment, never a hand-listed engine name). `core_config_leaves()` is the `False`-tagged complement (~88 of 105 leaves — `webhooks.*`/`secrets.*`/`stores.*`/`lineage.*`/most of `agent.*`/`danger.*` — that never appear in any engine's table because they run in core code paths with no per-engine implementation to diverge from), derived from the SAME tag so the two cannot drift. A field discovered with NO `engine_scoped` key at all raises `CapabilityScopeError` naming the field and both resolutions; a field under `engine.<name>.*` tagged `False` (or untagged) ALSO raises — there is no valid "core" reading for a field namespaced to one engine. `explicitly_set_config_leaves(cfg)` walks an actual validated config INSTANCE via `model_fields_set` at each nesting level, returning only the leaves the user explicitly wrote — narrowed to the SAME `engine_scoped` tag, since once a core leaf leaves the checklist there is no row for it — `aqueduct/config.py::load_config()` calls this at config-resolution time, once `deployment.engine` is known, and emits a suppressible `engine_key_ignored` warning (same rule id and `aqueduct.warnings.emit` machinery as the blueprint gate) for any explicitly-set leaf whose verdict isn't `SUPPORTED` — WARN, never ERROR (the same `aqueduct.yml` must stay valid across engines). Closes the gap left by Step 0's leaf walker (`capability_leaves.py` covers Blueprint grammar only, not `aqueduct.yml`). Imports `aqueduct.config` at module level; `config.py` imports this module back only lazily (inside `load_config()`), avoiding a circular import.
- `aqueduct/executor/capability_tooling.py` — engine-agnostic, **pyspark-free** capability-declaration tooling (Phase 78): `governed_leaves(engine=None)` (grammar ∪ engine-scoped config, threading `engine` through to `config_leaves.all_config_leaves(engine=...)` — Q4 step 2), `discover_declarations()` (every `capabilities.yml` this install governs — resolved from the `aqueduct.engines` entry points via `find_spec`, WITHOUT importing an engine module, because an engine with an incomplete table cannot be imported), `check()`/`sync()`/`scaffold()`/`render_matrix()`/`write_matrix()`, each now computing that ENGINE's own checklist rather than one shared cross-engine set. `sync(prune_orphans=True)` deletes an orphaned row's text block by default (mechanical, comment-preserving — `_remove_leaf_rows`) rather than only reporting it, because an orphaned row already makes the declaration invalid; `--no-prune` on the CLI falls back to report-only. Returns values; the CLI (`aqueduct/cli/dev.py`) does all rendering. This is the code behind `aqueduct dev capabilities …`; `scripts/capabilities.py` only forwards to it.
- `aqueduct/executor/spark/capabilities.py` + `capabilities.yml` — the Spark engine's capability declaration. The **YAML is the declaration** (one explicit row per leaf — 311 as of 2026-08-16: 294 grammar + 17 engine-scoped config, Q4 step 2 — each `supported`/`unsupported`/`ignored_with_warning`/`undeclared`, plus optional `requires` + `hint`); the `.py` only loads and registers it via `load_declaration()`, passing `all_config_leaves(engine="spark")` (not the cross-engine union) as the checklist. **pyspark-free** despite living under `spark/`. There is NO default-verdict sweep: an engine states what it supports leaf by leaf. A new engine ships its own `capabilities.yml` — data, not Python. (Count drifts as leaves are added — treat this as a snapshot, not a contract; the authoritative count is `len(governed_leaves())`/the YAML itself. DuckDB, the other registered engine, declares 319 rows as of the same date: 294 grammar + 25 engine-scoped config — the grammar count matches Spark's because both share the same Blueprint grammar; only the engine-scoped config leaf count differs.)
- `aqueduct/executor/protocol.py` — engine-agnostic, **pyspark-free** `ExecutorProtocol` + `PromptRules` (Phase 78 Step 2): a frozen dataclass (`engine`, `execute`, `extract_error`, `prompt_rules`) an engine registers via `register_protocol()` as an import side effect of its `aqueduct.engines` entry-point module, mirroring `capabilities.py`'s `EngineCapabilities`/`CAPABILITY_REGISTRY` pattern. `__post_init__` raises `EnginePluginError` (an `AqueductError` — an engine-plugin author is a user of this seam, so the error-taxonomy rule applies; never a bare builtin) if `execute`, `extract_error`, or the `PromptRules` pack is missing/incomplete — the structural guarantee that an engine cannot register without an error extractor (exception → `FailureContext` fields) or a prompt-rules pack. `PromptRules` (`persona`, `root_cause_note`, `rules`, all required non-empty) is the engine's half of the healing system prompt; see the composition rule below. `get_protocol(engine)` calls `capabilities.load_engines()` before its registry lookup and raises `UnknownEngineError` for an unregistered engine, same fail-closed posture as `get_capabilities()`. See `docs/specs.md` §10.9.
- `aqueduct/executor/session_config.py` — engine-agnostic, **pyspark-free** `SessionSpec` engine-config resolution (Phase 82 remediation, generalized in the `spark_config`→`engine_config` cross-engine remediation pass): `resolve_session_engine_config(cfg, engine, manifest)` builds each engine's `aqueduct.yml`-level config (Spark's merged `conf` dict; every other engine's `engine.<name>` sub-model dumped via `model_dump()`) and layers that SAME engine's `Manifest.engine_config[engine]` entry on top (Blueprint wins on a key conflict) — one precedence rule, shared by every registered engine, not a Spark-only special case. Also `session_secrets_options(cfg, manifest)` (the `secrets:` block bag for `SessionSpec.engine_options`) and `session_config_fingerprint(cfg, engine, manifest)` (a deterministic hash of `resolve_session_engine_config`'s output — `aqueduct/cli/run.py`'s `_execute_target` compares it against the live session's fingerprint before every single-engine execution and rebuilds only on mismatch, so a Manifest is never executed against a session built from a different Manifest; this is the ONLY part of a `SessionSpec` that can vary within one run, since `cfg` and the target `engine` are fixed at process start). Split out of `protocol.py` on purpose — these are session-config helpers, not part of the `ExecutorProtocol` contract an engine implements, and `protocol.py`'s public surface is the thing tracked for the BYO-engine stability criterion (see the "READINESS TEST" standing decision). Every `SessionSpec` builder resolves through here: `aqueduct/cli/run.py` (single-engine and polyglot), `aqueduct/cli/drift.py`, and `aqueduct/patch/preview.py::run_sandbox_gate` (Gate 3) — so a sandbox replay sees the SAME engine config a real run would use, not a hardcoded `manifest.spark_config` read that only ever populated for a Spark target (the defect this module's extraction fixed, then generalized to every engine's Blueprint-level override).
- `aqueduct/executor/orchestrator.py` — engine-agnostic multi-island polyglot execution orchestrator (Phase 81 step 3). A single-engine Manifest still runs through that one engine's own `ExecutorProtocol.execute()` directly (e.g. via `cli/run.py`); a Manifest whose `islands` span more than one engine needs this coordinator ABOVE the per-engine call — opens a session per island in island-topological order, executes each island's modules through its own engine on an island-scoped sub-Manifest, and performs each boundary's synthetic Handoff module's spill write/read between islands.
- `aqueduct/executor/spill.py` — engine-agnostic cross-engine handoff spill directory lifecycle (Phase 81 step 3). The engine-native parquet write/read at a handoff boundary lives in each engine's own executor; this module owns everything AROUND that write/read — where a boundary's spill directory lives, deleting it on a successful run, keeping it on failure (so a manual `aqueduct run --resume <run_id>` after a plain failure, with no Blueprint edit in between, can resume from the upstream island's already-materialized output). A heal-triggered rerun does NOT get this: a heal patches the Manifest, which changes the whole-Manifest hash and therefore the spill path (even when the patch touched only a downstream island), and the heal-retry path never carries a resume id at all — see `cli/run.py`'s `resume_run_id if patch_count == 0 else None`.
- `aqueduct/executor/spark/engine.py` — the Spark engine's `aqueduct.engines` entry-point target (Phase 78 Step 1 built the registration seam; Step 2 fills in the `ExecutorProtocol`). Importing it registers, as import side effects, `spark/capabilities.py`'s declaration AND its `ExecutorProtocol` (the `SPARK` module constant) — both **pyspark-free**. `execute`/`extract_error` are thin wrappers deferring the actual `pyspark` import to call time; `extract_error` delegates to the existing `aqueduct.surveyor.error_extraction._extract_structured_error` — a refactor (naming existing behavior through the new seam), not new behavior. (The Step-1-compatible `load_executor()` lazy accessor was removed — zero callers remained anywhere once Step 2 callers moved to `get_protocol(engine).execute` through the registry; see CHANGELOG `### Removed`.) The dependency direction is one-way: this module imports NOTHING from `aqueduct/agent/` — Spark's healing prompt-rules pack lives beside it in `aqueduct/executor/spark/prompt_rules.py`. Core never imports a `spark.*` module by name — this is the seam `load_engines()` resolves.
- `aqueduct/executor/spark/prompt_rules.py` — Spark's `PromptRules` pack (`SPARK_PROMPT_RULES`): the engine-specific half of the healing system prompt (persona, root-cause note, the four Spark-flavored rule bullets). Pure data, **pyspark-free**. Text is verbatim from the pre-split `_SYSTEM_PROMPT_TEMPLATE` — the composed Spark prompt is byte-identical to the pre-split prompt, guarded by `tests/test_agent/test_prompt_composition.py`. Rewording it is a prompt change: follow the `PROMPT_VERSION` bump policy.
- `aqueduct/executor/spark/` — all Spark code (`ingress`, `egress`, `channel`, `executor`, `junction`, `funnel`, `probe`, `session`, `udf`, `assert_`, `capabilities`, `engine`)
- `aqueduct/executor/__init__.py` — `get_executor(engine: str = "spark")` factory; Phase 78 Step 2 resolves it through `aqueduct.executor.protocol.get_protocol(engine).execute` instead of a hardcoded Spark-only branch, so an unknown engine raises `UnknownEngineError` (not a bare `ValueError`)

**Adding a grammar leaf or a config key requires a capability verdict — and the build WILL break until you give one.** Any new module-type field, Channel op, Egress write mode, Junction/Funnel fan mode, or feature flag becomes a capability leaf automatically (`aqueduct/executor/capability_leaves.py::all_leaves()`) or, for the hand-curated categories (`INGRESS_FORMATS`/`EGRESS_FORMATS`/`FEATURE_FLAGS`), needs one line added there. The same applies to `aqueduct.yml` engine-config fields, with one extra mandatory step: any new pydantic field anywhere under `aqueduct.config.AqueductConfig` must carry an EXPLICIT `Field(..., json_schema_extra={"engine_scoped": True})` (dispatches through an engine) or `{"engine_scoped": False}` (core-only) tag — see "Config-leaf scoping" below. There is no untagged state: a field with neither key raises `CapabilityScopeError` naming the field. Only a `True`-tagged field becomes a `config.*` capability leaf (`aqueduct/executor/config_leaves.py::all_config_leaves()`) that any engine is asked about; a `False`-tagged one never does — never hand-listed either way.

The workflow when you add one:

1. Add the field/op/mode (for a `config.*` field: also decide + set its `engine_scoped` tag — see below). **The build breaks** — engine registration raises `CapabilityDeclarationError` naming the leaf (or `CapabilityScopeError` if it's an `engine.<name>.*` field you forgot to tag), and `tests/test_capabilities/test_closure.py` fails.
2. `aqueduct dev capabilities sync` — appends the new leaf to **every** engine's `capabilities.yml` as `undeclared` (or, for an `engine.<name>.*` leaf, only to that ONE engine's file). Build stays red: `undeclared` is a sentinel, **not** a verdict, and is explicitly distinct from `unsupported`.
3. Replace each `undeclared` with a real verdict (`supported` | `unsupported` | `ignored_with_warning`, plus optional `requires`/`hint`). **If the leaf is an EXECUTION leaf** (`capability_leaves.py::execution_leaves()` — `module.type.*`, `channel.op.*`, ingress/egress format/mode/on_new_columns, junction/funnel `.mode.*`, `feature.*`; never `config.*` or a `module.field.*`/`<block>.field.*` leaf) **and the verdict is `supported`, also add a `tests:` key** naming the pytest id(s) that actually exercise it on that engine — `tests/test_capabilities/test_verdict_test_links.py` fails the build otherwise, naming the engine and leaf. Do not invent an id or downgrade the verdict to dodge it.
4. Build passes. If the verdict is version-gated or refused, `aqueduct dev capabilities docs` regenerates the matrix in `docs/compatibility.md`.

**Starting a NEW engine:** `aqueduct dev capabilities scaffold --engine <name>` generates its complete `capabilities.yml` with every leaf on THAT engine's own checklist `undeclared` — grammar leaves plus its own engine-scoped config leaves (never a core leaf, never another engine's `engine.<name>.*` leaves). The engine cannot register until each row is a real decision. Do **not** copy Spark's `capabilities.yml` — that inherits ~206 `supported` rows, i.e. a silent claim to support the entire grammar. Read it as a reference; never clone it.

**The capability tooling SHIPS — it is not a repo script.** The implementation lives in `aqueduct/executor/capability_tooling.py` and is exposed as `aqueduct dev capabilities scaffold|sync|check|docs` (`aqueduct/cli/dev.py`). `scripts/` is not in the wheel (`packages = ["aqueduct"]`), so a third-party engine author who `pip install`s aqueduct would otherwise have no way to generate the table their engine cannot register without. `scripts/capabilities.py` is a thin wrapper that forwards to the same CLI — do not grow a second implementation there, and do not add capability logic to `scripts/`.

**Three capability error types, never conflated.** `EnginePluginError` = the `aqueduct.engines` entry point failed to IMPORT (broken/half-installed plugin; the message says reinstall). `CapabilityDeclarationError` = the declaration is incomplete/invalid (a leaf with no row, an `undeclared` row, an orphaned row, an illegal verdict, a malformed `requires`) — a dev-time build failure whose fix is `aqueduct dev capabilities sync` + a verdict, never a reinstall. `CapabilityScopeError` (Q4 step 2) = a `config.*` field's engine-scoping is undecided or contradictory: EVERY field must carry an explicit `engine_scoped: True`/`False` tag in `aqueduct/config.py` — a field with neither raises, naming the field and both resolutions; a field under an `engine.<name>.*` block is additionally restricted to `True` (tagging it `False`, or leaving it untagged, is a contradiction and also raises — there is no valid "core" reading for something namespaced to one engine). All three are direct `AqueductError` subclasses — none derived from another — precisely so a handler written for one cannot silently swallow another and re-conflate two states with different fixes; `load_engines()`'s broad `except Exception` re-raises all of them unchanged for exactly this reason. Callers branch by TYPE (`aqueduct/doctor/checks_io.py` does, catching `CapabilityDeclarationError`, `EnginePluginError`, and `CapabilityScopeError` in one clause because they share the same RESPONSE shape while staying distinguishable by type), never by message substring.

**Never make the break go away with a default.** `all_leaves_default()` — a helper that filled every leaf with one default verdict — is why this guarantee was previously a lie: it derived each engine's table from the SAME walker the closure test compared it against, so the two could never disagree and every new leaf was silently swept into `supported`. A planted schema key passed the build. The closure test now compares the walker (code) against each engine's YAML (data) read from disk — two independent sources — and `tests/test_capabilities/test_closure.py::test_declaration_has_no_default_sweep` fails the build if any such sweep returns.

**Config-leaf scoping (Q4 step 2) — narrowing the checklist is a different operation from defaulting a verdict.** Of the 105 `config.*` leaves `AqueductConfig` derives, ~88 are CORE: `webhooks.*`, `secrets.*`, `stores.*`, most of `agent.*`/`danger.*` run in core code paths that never dispatch through an engine at all, so asking every engine to declare a verdict for webhook backoff was a category error the framework used to force anyway. Every field in `aqueduct/config.py` carries an EXPLICIT `Field(..., json_schema_extra={"engine_scoped": True})` (the ~17 leaves that genuinely dispatch through an engine — an engine module reads it, or it reaches `ExecutorProtocol`) or `{"engine_scoped": False}` (the ~88 core leaves) tag — **there is no untagged state and no "untagged means core" fallback.** A field with neither key raises `CapabilityScopeError` naming the field and both resolutions, the instant the walker runs; that is what makes "absent = core" impossible to reach by omission — an earlier design let a missing tag fall back to core implicitly, which is the identical shape as the banned `all_leaves_default()` bug (a verdict/classification nobody decided) and was rejected for exactly that reason. `all_config_leaves()`/`core_config_leaves()` (`config_leaves.py`) derive from the SAME tag and cannot drift apart; reclassifying a key is now a one-word `True`↔`False` diff at the field itself. `engine.<name>.*` leaves (`engine.spark.master_url`, a future `engine.duckdb.*`) are additionally positional: they appear ONLY in that one engine's checklist (`all_config_leaves(engine=...)`), never another's — a field found there tagged `False` (or untagged) is ALSO a contradiction and raises `CapabilityScopeError`. This is NOT the banned `all_leaves_default()` shape: narrowing a checklist from an independently-tagged source is a different operation from deriving a table from the SAME walker that validates it.

No committed snapshot exists for the core/engine-scoped split (a `core_config_leaves.yml` + CI `git diff`, the `docs/compatibility.md` pattern, was considered and dropped — the tag already is the one source of truth). What replaces it — proven sufficient by the enforcement table below, and proven falsifiable both ways (add a temporary untagged field → `CapabilityScopeError` names it; flip `config.probes.max_sample_rows` from `True` to `False` → the invariant test goes red naming the leaf and the engine):

| someone does this | caught by | how loud |
| :- | :- | :- |
| untags a key some engine declared `unsupported`/`ignored_with_warning` | the invariant test (`tests/test_capabilities/test_config_scope_invariant.py`) | red build, names the leaf |
| untags a key but leaves the rows behind | the existing orphaned-row check (`dev capabilities check`) | red build, names the leaf |
| untags a key every engine declared `supported` | nothing, AND NOTHING NEEDS TO — a `supported` verdict emits no warning, so there was no warning path to delete | inert by construction |
| adds a config field anywhere, forgets the tag | the walker raises `CapabilityScopeError` naming the field | every command fails locally |
| tags a field under `engine.<name>.*` as `False` (or leaves it untagged) | the walker raises `CapabilityScopeError` — a contradiction | every command fails locally |
| tags a new field `True` | new `undeclared` rows | red build |

Row 1 is the load-bearing insight: **the only keys whose reclassification can destroy user-visible behavior are exactly the keys the invariant test forbids reclassifying.** A leaf some engine declares non-`supported` has a live `engine_key_ignored` warning path (`_warn_ignored_config_keys`); reclassifying it to core would silently delete that path with nothing else noticing.

**Justify a classification by a consumer, never by another engine's verdict.** An earlier `config.lineage.*` leaf (removed along with the OpenLineage-emission feature it configured) was once classified engine-scoped *because* DuckDB declared it `ignored_with_warning` — circular reasoning that treated a verdict as evidence for the classification that produces it, and the verdict itself was wrong. Note the direction the invariant test fails in: it forbids reclassifying any leaf some engine declares non-`supported`, which protects genuine warning paths **and** wrong verdicts equally. It is a ratchet, not an oracle. So when tagging a field, grep its readers and decide from them; an existing `capabilities.yml` row is a claim to be checked, not a source of truth.

Blueprint-grammar leaves and config leaves share one closure test but differ in ENFORCEMENT at runtime: an `unsupported` grammar leaf is a compile-time `CompileError`; a non-`supported` config leaf only ever warns (`engine_key_ignored`, at config-resolution time) — the same `aqueduct.yml` has to stay valid across engines. See `docs/specs.md` §10.9.

When adding a Spark feature: code in `aqueduct/executor/spark/`. Do not import `pyspark` in `parser`, `compiler`, `surveyor`, `patch`, or `depot`.

**Documented exception (doctor):** the `aqueduct/doctor/` package lazily imports `pyspark` inside four check functions (`check_spark`, `check_storage`, `check_cloudpickle_compat`, and the Spark branch of `check_handoff_engine_access` — all in `doctor/__init__.py`). Top-level `import aqueduct.doctor` must never pull `pyspark` — keep these imports inside function bodies so `--skip-spark` and the `[spark]`-less install path stay viable. The package splits leaf connectivity checks (`doctor/checks_io.py`) from the spark/network/blueprint-source cluster + `run_doctor` (`doctor/__init__.py`); the latter stay together because the test suite monkeypatches several by their `aqueduct.doctor.<name>` path and they call each other by bare global name. `doctor/base.py` holds `CheckResult` + `_ms` to avoid a circular import. Public names re-export from `__init__`, so `from aqueduct.doctor import <check>` is unchanged.

**Documented exception (surveyor):** `surveyor/surveyor.py::_extract_structured_error` lazily imports `from pyspark.errors import PySparkException` inside a `try/except` (falling back to `None` when absent). This is required — structured root-cause extraction must recognise Spark's own exception types — but it is **guarded**: the import lives in the function body, so top-level `import aqueduct.surveyor` stays `pyspark`-free and the `[spark]`-less install path is preserved. Do not promote this to a module-level import.

**Documented exception (dashboard):** `aqueduct/dashboard/app.py::main()` lazily imports `pyspark.sql` inside the function body as a narwhals/plotly circular-import workaround. It is already lazy and does not violate the top-level-purity rule. Do not promote it to module-level or remove the workaround comment.

When adding an LLM provider: add `_call_<provider>()` in `aqueduct/agent/providers.py` using `httpx`. Wire dispatch in `_call_agent()`. No new dep needed.

**The healing system prompt is COMPOSED, not monolithic** (Phase 78 Step 2). The ENGINE-INDEPENDENT scaffold (PatchSpec schema, op-selection table, provenance rules, output contract, the generic defer categories) lives in `aqueduct/agent/prompts.py` and must name no engine. Every engine-flavored string lives in that engine's `PromptRules` pack (`aqueduct/executor/spark/prompt_rules.py` for Spark) and is pulled in at build time through `ExecutorProtocol.prompt_rules`. So: a **Spark-specific** prompt rule goes in the Spark pack, a rule true of **every** engine goes in the scaffold. When in doubt, keep it generic — a rule stranded in one engine's pack is invisible to the next engine; where a rule is half-generic, SPLIT it (that is what `DeferRules.infra_examples` / `udf_languages` are: a generic bullet with an engine-supplied fragment). Do NOT re-add an engine import to `aqueduct/agent/`; the agent resolves the engine's pack through the registry, never by name.

**The scaffold is NOT just `_SYSTEM_PROMPT_TEMPLATE`.** Parts of the prompt (`defer_rules`) are assembled at RUNTIME inside `_build_system_prompt`, so a guard that greps the template constant cannot see them — that blind spot is exactly how three Spark strings (Hive metastore locks, Python/Scala UDF code, "Spark internals") survived the first pass at this split. The guard in `tests/test_agent/test_prompt_composition.py` therefore composes the prompt for a FAKE non-Spark engine, across every `allow_defer` × `tools_enabled` combination, and fails the build if `Spark`/`pyspark`/`AnalysisException`/`Py4J`/`Hive`/`Scala`/`metastore` appears anywhere in it. If you add a new runtime-assembled prompt fragment, it is in scope for that guard by construction — do not exempt it.

**Engine DATA in the prompt is not the same thing as an engine STRING in the scaffold.** The "Engine/session config (`set_engine_config`)" section renders the target engine's whole `engine_config_allowlist.yml` (allow entries with type/enum/range, deny entries with their `reason`) into the composed prompt, so Spark's prompt legitimately contains `spark.*` keys while the agent layer names no engine: the section is assembled by `prompts.py::_render_engine_config_policy` from the same core, engine-NAME-keyed loader Gate 1 uses (`aqueduct/executor/engine_config_allowlist.py`), imported lazily, never from an engine module. Do NOT "fix" this by moving it into `PromptRules` — that pack is the engine's own prose, and `ExecutorProtocol`'s surface is frozen for the BYO-engine readiness criterion; the allowlist is core data an engine only supplies a YAML file for. An engine with no allowlist file is told in the prompt that the op is unavailable, never shown an empty table (an empty table is the silent no-op: the model would think it may write config and be refused every time).

**`PROMPT_VERSION` bump policy** (`aqueduct/agent/loop.py`): bump **only** when the composed system prompt's body changes — the scaffold, ANY registered engine's `PromptRules` pack (persona / root-cause note / rules), the op table, schema-derived rules, or the worked example — i.e. anything the LLM sees on a *successful* turn. Moving text between the scaffold and an engine's pack without changing the composed output is a refactor, NOT a prompt change: no bump — anything the LLM sees on a *successful* turn. Do NOT bump for reprompt-loop tooling, `_format_validation_error` text, `_detect_structural_error` cases, the escalated-reprompt template, mechanical recovery passes (`_THINK_BLOCK_RE`, `_FENCE_BLOCK_RE`, `_LINE_COMMENT_RE`, `json_repair`), or `_parse_patch_spec` cleanups. Those are failure-path tooling — bumping for them pollutes `healing_outcomes.prompt_version` / `benchmark_results.prompt_version` correlation, making a parser tweak look like a prompt regression on the leaderboard.

**Spark behavior reference**: read `docs/spark_guide.md` before modifying Executor modules or implementing new Channel operations.

## TODOs Memory Rule
`~/.claude/projects/-home-sakhund-Personal-Projects-Aqueduct/memory/TODOs.md` (NOT a file in this repo — it lives in the user's Claude memory directory, outside version control) is the single source of truth for what's next, what's stubbed, and what's deferred.

- **"What's left?" / "What's next?"** → read that TODOs.md first.
- **After every planning session** → update TODOs.md with agreed phases.
- **After every phase completion** → mark phase done in TODOs.md; add entry to CHANGELOG.md.
- **When adding a stub** → add to Active Stubs with file + line + acceptance criteria.

## Maintenance playbook — "I am adding X, what breaks and what do I update?"

The Change-Trigger Matrix below is the exhaustive lookup table. This section is the
short version for the five changes that break the build on purpose: what you will
SEE, the command you run, and the decision only a human can make. If the two ever
disagree, the matrix wins.

### Adding a grammar leaf or an `aqueduct.yml` config key

**What you see.** The build goes red in two places at once: engine registration
raises `CapabilityDeclarationError` naming the leaf, and
`tests/test_capabilities/test_closure.py` fails with the same name. Nothing you can
do to `parser/schema.py` or `config.py` makes it green.

**What you run.** `aqueduct dev capabilities sync` (shipped command, not a repo
script). It appends the new leaf to **every** registered engine's `capabilities.yml`
as `undeclared`. `aqueduct dev capabilities check` reports what is still open
without writing; that is what CI runs.

**What you decide.** The build stays red after `sync`, deliberately. `undeclared` is
a sentinel meaning "nobody has decided yet", and it is NOT a synonym for
`unsupported` ("we decided this engine cannot do it"). Conflating those two is what
made the closure guarantee a tautology once already. Replace each `undeclared` with
a real verdict per engine: `supported` (optionally with `requires:` for a version
floor and `hint:`), `unsupported`, or `ignored_with_warning`.

**Do not** copy another engine's verdicts to make the red go away. Spark's table is
mostly `supported` rows because Spark implements most of the grammar; pasting them
into a new engine is a silent claim to implement all of them, which is precisely the
failure this framework exists to catch. The row count itself is a moving snapshot
(`len(governed_leaves())`, or the YAML row count directly) — never hardcode it in a
check. Read Spark's table; never clone it.

### Adding or bumping a dependency

**What you see.** Nothing breaks immediately, which is exactly why this one is easy
to get wrong: an unpinned CI lane will keep resolving fresh and can go red weeks
later, on a commit that did not touch dependencies.

**What you run.** Widen or add the range in `pyproject.toml`, then `bash
scripts/lock.sh`, then commit **both** the `pyproject.toml` change and the
regenerated `requirements/*.txt`.

**What you decide.** Nothing, if you keep the two tiers straight:

- **Library dependencies stay RANGES.** `aqueduct-core` is published to PyPI. An
  exact pin in `pyproject.toml` would make it uninstallable next to a user's own
  stack. Never pin there.
- **The CI/dev environment is LOCKED.** `requirements/ci-py311.txt` (and one
  `requirements/compat-py3.NN.txt` per compat interpreter) are pip *constraints*
  files: they add nothing to an install, they only fix the version of whatever the
  ranges resolve to. The reproducible lanes set `PIP_CONSTRAINT` to them, so they
  install the same tree in six months as today.
- **The canary lane ignores the lock, on purpose.** `version-matrix.yml`'s
  `snippets` job sets no constraint. It resolves fresh so an upstream release that
  breaks Aqueduct shows up there first, as a red annotation on a
  `continue-on-error` lane, instead of arriving later as a mystery flake in a lane
  that is supposed to be reproducible. Adding `PIP_CONSTRAINT` to the canary looks
  like a tidy-up and deletes the only early-warning signal in CI. `tests/test_meta_ci.py`
  fails the build if anyone does.

### Adding an engine

1. `aqueduct dev capabilities scaffold --engine <name>` writes a complete
   `capabilities.yml` with every leaf `undeclared`. Work through it leaf by leaf.
2. Implement the `ExecutorProtocol` (`aqueduct/executor/protocol.py`): `execute`
   (Manifest → `ExecutionResult`), `extract_error` (engine exception →
   `FailureContext` fields), and a `PromptRules` pack (the engine's half of the
   healing prompt). All three are required; `__post_init__` raises
   `EnginePluginError` if any is missing, so an engine cannot register without an
   error extractor or with another engine's healing advice.
3. Declare an `aqueduct.engines` entry point pointing at the module that registers
   both, as an import side effect. Core never imports an engine by name.
4. Extend the compatibility-matrix coverage: add the engine's tests to
   `version-matrix.yml`'s `compat` job (test paths + the `-m` marker expression)
   so it is actually exercised in CI, not just registered. `tests/test_meta_ci.py`
   resolves the registered engine list from `CAPABILITY_REGISTRY` (never a
   hardcoded name list) and fails the build if any registered engine isn't
   covered there.

The two errors you will meet here are different states and must not be conflated:
`EnginePluginError` = the entry point failed to import (broken/half-installed
package; reinstall). `CapabilityDeclarationError` = the declaration is incomplete or
invalid (run `sync`, declare a verdict; reinstalling fixes nothing).

### Changing the healing prompt

Bump `PROMPT_VERSION` (`aqueduct/agent/loop.py`) when the **composed** system prompt
changes: the engine-independent scaffold, any engine's `PromptRules` pack, the op
table, the schema-derived rules, or the worked example. Anything the LLM sees on a
successful turn.

Do NOT bump when text merely MOVES between the scaffold and an engine's pack and the
composed output is byte-identical (that is a refactor), or for failure-path tooling
(reprompt templates, parser recovery passes, validation-error formatting). Bumping
for those pollutes the `prompt_version` correlation in `healing_outcomes` /
`benchmark_results` and makes a parser tweak look like a prompt regression.

### Changing a documented contract

A new or renamed `aqueduct.yml` key, a new `stores.*` backend or persistent table, an
`agent.approval` mode, an exit code, a patch op, a CLI contract: update
`docs/specs.md` **in the same commit** and bump its `Version X.Y` header. specs.md is
the engine reference; it goes stale silently because the matrix routes most work to
the dedicated guides. Grammar / `agent:` block / provider wiring also updates
`SKILL.md`, or authoring LLMs get stale contracts.

## Change-Trigger Matrix

Use this table at coding time, not just at the end of a phase. Whenever you touch the left column, the right column **must** move in the same commit. The phase-end ritual becomes a verification pass: read this table top-to-bottom and confirm every triggered doc/file is current — the matrix is the trigger, not the date on the calendar.

`docs/specs.md` is the **engine reference** for semantics that don't belong elsewhere. Production / CLI / observability / Spark-tuning details now live in their dedicated guides (see Documentation map). Phase / sprint / development artefacts (`Phase 35`, `Sprint 7`, `Task NN`, `pre-30a`, `deferred to Phase NN`, etc.) must stay off **user-facing surfaces**: docs (`docs/**/*.md`), templates (`aqueduct/templates/**` — they get copied into user projects), gallery (`gallery/**`), and scaffolding (`README.md`, `CONTRIBUTING.md`). They are **allowed** in `CHANGELOG.md` / `TODOs.md` (where they belong) and in **source-code comments/docstrings** (`aqueduct/**/*.py`), where `# Phase NN —` is useful provenance — do not strip those. Verify the user-facing surfaces stay clean with `grep -rnE "Phase [0-9]|Sprint [0-9]|Task [0-9]" docs/ gallery/ aqueduct/templates/ README.md CONTRIBUTING.md` (note: **not** `aqueduct/` source) before commits that touched any of them.

**A source field that RENDERS INTO a user-facing surface is itself user-facing**, even though it lives under `aqueduct/`. The live case: `capabilities.yml` `hint:` strings are copied verbatim into the generated matrix in `docs/compatibility.md`, so a hint reading "unsupported until Phase 81 step 2" reaches users through a file the grep above does not scan. The grep only catches it *after* someone regenerates, and the fix always belongs upstream in the YAML. The same reasoning covers any other data file whose text is rendered rather than executed. Write hints as a durable statement of what the engine does or does not do, never as a schedule.

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
| Any change to `pyproject.toml` version pins or supported Python/Spark range | `docs/compatibility.md` — **prose sections only.** Two regions of that file are GENERATED and must never be hand-edited: the capability matrix under `<!-- Generated by aqueduct dev capabilities docs — do not edit by hand. -->` (regenerate with that command) and the `<!-- COMPAT_RESULTS_START/END -->` block, which the `version-matrix` workflow auto-pushes into as `[skip ci]` commits. A local edit inside either region is lost on the next sync — one such fix had to be re-applied after being clobbered. If generated content is wrong, fix the generator (`capability_tooling.py`) or the workflow, never the output |
| Adding or registering a new execution engine (`aqueduct.engines` entry point) | `version-matrix.yml`'s `compat` job (test paths + `-m` marker expression) **and** a PRE-merge lane in `test-suite.yml` (copy the `parser-tests` job shape; add a `changes` path filter for the engine's source + test directories) — `tests/test_meta_ci.py` fails the build if a registered engine isn't covered in either file |
| Any newly deferred or aspirational item (NOT a current phase) | The issue tracker (`docs/roadmap.md` was removed) — never inline "this is deferred" prose into `specs.md` |
| Any new file under `docs/` | `README.md` References list + this Documentation map |
| Any new flag, command, or behaviour visible from the CLI | `docs/cli_reference.md` |
| Any user-facing engine semantic (architecture, module semantics, configs, gates, runtime behaviour) NOT covered by a dedicated guide above | `docs/specs.md` — NO `Phase NN` artefacts |
| Any change to the **Blueprint grammar** (module types/config keys, edges/ports, Context fns, UDF/macro syntax), the **`agent:` block** fields/values, or **LLM provider wiring** (`provider`/`base_url`/auth env var) | `SKILL.md` (repo root) — it restates these contracts compactly for authoring LLMs; update it in the SAME commit as `specs.md` or the schema, else authoring guidance drifts |
| Any new testable feature | A real test at the right layer (`unit` / `integration` / `e2e`), OR a `@pytest.mark.todo("why")` stub if you're deferring it. NEVER an entry in `TEST_MANIFEST.md` (retired → `docs/archive/`). See the Testing section. |
| Any phase / sprint / shippable change | `CHANGELOG.md` `[Unreleased]` only — never bump version, never add a versioned header (user controls release timing) |
| Any new `@aq.*` function in `aqueduct/compiler/runtime.py` | `docs/specs.md` §5.3 function table + `_DISPATCH` table in `runtime.py` |
| Any new path-key entry in `aqueduct/executor/path_keys.py` | The module-type's schema model in `aqueduct/parser/schema.py` (mark path fields with `Annotated[str, FsPath()]`) |
| Any new exit code in `aqueduct/exit_codes.py` | `docs/cli_reference.md` exit-code reference |
| Any new CLI exit path (`sys.exit(...)` in `aqueduct/cli.py`) | Use a named `exit_codes.*` constant — never a bare int (only `sys.exit(130)` for SIGINT is exempt). Classify by the contract docstring in `aqueduct/exit_codes.py`: config/schema/danger-policy → `CONFIG_ERROR`; runtime/data/missing-file/subprocess/record-not-found → `DATA_OR_RUNTIME`; bad-flag / missing-arg → `USAGE_ERROR` (`64`, sysexits `EX_USAGE` — Click's own `UsageError` is repointed here too, see `aqueduct/cli/__init__.py`); staged-patch → `HEAL_PENDING`; non-interactive gate rejection → `VALIDATION_GATE`. A new *value* added to `exit_codes.py` is a v1.0-contract change → also update `CHANGELOG.md` |

## Source Code Navigation Map

This section documents the internal module structure of key packages. Updated
whenever a package is restructured — use it as the first filter before grepping.

### `aqueduct/models.py` — Cross-layer boundary types

Re-export surface for downstream layers (executor, surveyor). Imports from
`parser.models`, `compiler.models`, and `compiler.islands` so executor/surveyor
don't reach into those internals. Pure re-export — definitions still live in
their original modules.

| Symbol | Defined in | Role |
|--------|-----------|------|
| `Manifest` | `compiler.models` | Compiled Blueprint ready for execution |
| `Island` | `compiler.islands` | Per-engine island (Phase 81 cross-engine boundary) |
| `Module` | `parser.models` | Parsed module node |
| `Edge` | `parser.models` | Data-flow edge |
| `RetryPolicy` | `parser.models` | Per-pipeline retry config |
| `ModuleType` | `parser.models` | Module type enum |

### `aqueduct/surveyor/` — Observability, benchmarking, webhooks

| Module | What it owns |
|--------|--------------|
| `surveyor.py` | Main Surveyor class: start/record/stop lifecycle. Re-imports the DDL constants and `_extract_structured_error` (kept under `aqueduct.surveyor.surveyor.*` for callers/tests) |
| `ddl.py` | Observability-store `CREATE TABLE`/`ALTER TABLE` string constants (`_DDL`, `_SIGNAL_OVERRIDES_DDL`, `_HEAL_ATTEMPTS_DDL`). Pure SQL, no imports — re-exported by `surveyor.py` |
| `error_extraction.py` | Structured Spark/Py4J error extraction (`_extract_structured_error`, `_parse_suggested_columns`). Lazily imports `pyspark.errors`/`py4j` INSIDE the function — top-level `import aqueduct.surveyor` stays `pyspark`-free. Re-exported by `surveyor.py` |
| `models.py` | Frozen dataclasses: `RunRecord`, `FailureContext` (the LLM agent's input) |
| `scenario.py` | Scenario benchmark framework: `load_scenario`, `_build_failure_ctx`, effect-based grader |
| `webhook.py` | HTTP dispatch in daemon thread, `${VAR}` template rendering, redaction |
| `benchmark_store.py` | DuckDB persistence + regression detection for benchmark results |
| `blob_store.py` | Back-compat shim (Phase 53) — `externalise`/`materialize` delegate to `stores/object_store.BlobStore`; new code uses `make_blob_store` directly |

### `aqueduct/patch/` — PatchSpec grammar + apply + validation gates

| Module | What it owns |
|--------|--------------|
| `grammar.py` | `PatchSpec` Pydantic v2 model, 14 operation types, discriminated union |
| `operations.py` | Per-op implementations against Blueprint dict, ruamel YAML round-trip |
| `apply.py` | Apply orchestrator: load → deep-copy → apply ops → re-parse → archive |
| `index.py` | `patch_index` relational table (Phase 53): the truth for the object-store patch lifecycle — status + signature metadata for backend-blind `aqueduct patch list`/`pull`, the pending-patch guard, and history lookups without scanning `patches/` |
| `preview.py` | Lineage gate (Gate 2) + sandbox gate (Gate 3): diff column impact, sandbox replay |
| `ci.py` | Heal-as-PR / patch-import kit backing `patch pr`, `patch commit` and `patch import` — `on_patch_pending` webhook payload schema (`CI_WEBHOOK_REQUIRED_KEYS`) + payload validation, plus the branch-naming and commit-message helpers. Serverless: a user-owned CI workflow receives the webhook and calls `aqueduct patch import` |
| `revert.py` | The undo path for an applied heal patch (`aqueduct patch revert`): plans, VERIFIES and applies the restoration of the engine-config keys one `healed_by` record's `engine_config_delta` captured, then stamps that record `reverted_at:` rather than deleting it. Refuses (`RevertError`, a direct `AqueductError` — never a `PatchError`, which means a different thing) every case where the result would not reproduce a state the Blueprint actually had: a patch carrying any non-`set_engine_config` op, a key a later non-reverted patch overwrote, a value edited since, an ambiguous/absent/already-reverted record, or a plan whose re-resolved effective config does not land exactly on the recorded prior values. Adds NO patch op — the grammar stays closed; a restore-to-value goes through the real `SetEngineConfigOp` dispatch and a restore-to-absent is a key deletion no op expresses, which is itself the reason a revert is a Blueprint rewrite rather than a patch |
| `provenance.py` | Phase 79 — classifies each `patch.grammar` op as `dialect_neutral` or `engine_shaped` ONCE, in core, so every consumer (cross-engine recompile) can tell whether a patch generated while healing one engine's run may carry that engine's SQL dialect/cast syntax/format options and could be wrong if reused against a different engine |
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
| `queries.py` | The ONE read-time observability query layer (Phase 68) behind every viewer — the Streamlit dashboard, `report --json`, and the `aqueduct/tools/` registry. Row dataclasses (`RunRow`, `RunDetail`, `LineageRow`, …) + `discover_stores`/`list_runs`/`run_detail`/`lineage`/`run_sql_readonly`/`patch_show`. Backend-agnostic (`RelationalCursor`), no `pyspark`. New viewer query → add here, never inline SQL in a rendering surface |

### `aqueduct/infra/` — Cross-layer infrastructure utilities (no domain logic)

| Module | What it owns |
|--------|--------------|
| `http.py` | Single source of truth for outbound-HTTP mechanics: `RETRYABLE_DELIVERY_STATUS` / `RETRYABLE_PROVIDER_STATUS`, `retry_after_seconds`, `backoff_delay`, `sign_body` (HMAC-SHA256), `fire_and_forget` (daemon thread), `deliver_with_retry` (best-effort POST loop, stderr-logged). The webhook daemon-delivery path builds on it; `agent/providers.py` aliases its retryable-status constant + helpers (so existing `providers._RETRYABLE_STATUS` / `_retry_after_seconds` patch paths keep working). Add a new outbound-HTTP caller here, do not re-implement retry/backoff inline. |
| `module_loading.py` | Single source of truth for "load user code from a dotted path" (`load_module`, `load_callable`): collision-proof `spec_from_file_location` load from a `base_dir` sibling `.py` file when present, `importlib.import_module` fallback otherwise. Backs the secrets resolver, Assert `custom` `fn:`, Probe `custom` `module:`, `udf_registry` `module:`, and `format: custom` DataSource `class:` — all resolve against `Manifest.base_dir` (the top-level Blueprint's directory). Add a new "import user code by dotted path" site here, do not hand-roll `importlib.import_module`/`spec_from_file_location` inline (see failure_taxonomy.md #11). |

When adding a new outbound-HTTP path: reuse `infra/http.py`. Doctor probes are intentionally NOT on this path (one-shot reachability probe — different shape).

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
| `__init__.py` | Spark/network cluster + blueprint-source checks + `run_doctor`; also `check_healed_engine_config` (one row per `healed_by` record carrying an `engine_config_delta` — reports the recorded facts and the `patch revert` command, states NO staleness threshold, and warns only on the derivable equality failure "the recorded value is no longer what resolves") and `check_handoff_engine_access` (Phase 81/82 — per-registered-engine round-trip probe at `handoff.root`; its Spark branch is the 4th lazy-`pyspark`-import site, kept here for the same reason as the other three) |
| `base.py` | `CheckResult` dataclass |
| `checks_io.py` | Leaf connectivity checks: config, depot, observability, webhook, agent, secrets, store-backend, aqtest, aqscenario, capabilities (Phase 78 — version-constrained capability check, see `aqueduct/executor/capabilities.py`), `check_handoff_free_space` (Phase 81/82 — free disk space at `handoff.root`, pure `os`/`shutil`, no engine dependency) |

### `aqueduct/executor/spark/` — Spark execution

| Module | What it owns |
|--------|--------------|
| `executor.py` | Main loop: topo sort, component detection, module dispatch, parallel mode |
| `ingress.py` | Read: Parquet, Delta, CSV, JSON, JDBC |
| `channel.py` | Transform: SQL, deduplicate, filter, select, rename, cast, repartition, union, sort |
| `egress.py` | Write: overwrite, append, error, merge (Delta MERGE INTO) |
| `junction.py` | Fan-out: conditional, broadcast, partition |
| `funnel.py` | Fan-in: union_all, union, coalesce, zip |
| `probe.py` | Signals: schema_snapshot, row_count_estimate, null_rates, sample_rows, value_distribution, distinct_count, data_freshness, execution_partitions, threshold |
| `assert_.py` | Quality gates: min_rows, null_rate, freshness, sql, sql_row, spillway_rate |
| `session.py` | SparkSession management, Delta conf, cloudpickle patch |
| `udf.py` | UDF registry, cloudpickle compatibility |
| `custom_source.py` | Custom Python DataSource (`format: custom`) import/validate/register — Spark 4.0+ |
| `metrics.py` | Zero-extra-action observe() wrapper, Hadoop FS byte count |
| `test_runner.py` | Isolated module test framework (aqueduct test CLI) |
| `error_columns.py` | Spillway error-column name constants (`_aq_error_*`) shared by Channel and Assert, so a receiving module's `error_types` filter matches consistently across every spillway edge |
| `type_render.py` | Phase 80 (Arrow type-hub) — hub-vocabulary (`aqueduct/typehub.py`) to Spark DDL type-string rendering, replacing the old 5-entry `_TYPE_ALIASES` dict in `ingress.py`. Pure string logic, no `pyspark` import |
| `warnings/` | Session-startup warning rules (jar_availability), registered in `RULES` list |

### `aqueduct/compiler/` — Blueprint AST → fully-resolved Manifest

| Module | What it owns |
|--------|--------------|
| `compiler.py` | Main orchestrator — pipeline: Tier 1 resolution, Arcade expansion, Probe/Spillway validation, Regulator compile-away, engine capability gate (Phase 78), Manifest assembly |
| `models.py` | `Manifest` frozen dataclass + `to_dict()` serialization |
| `expander.py` | Arcade → flat namespaced module list, edge rewiring, depth-limited recursion (max 10) |
| `lineage.py` | `sqlglot`-based column-level lineage extraction (compile-time only, zero Spark actions); also `referenced_function_names`/`referenced_function_names_in_expr` (Phase 81) — the same sqlglot parse, read for function-call names instead of column lineage, backing `udf_attribution.py` |
| `macros.py` | `{{ macros.name }}` token expansion, parameterized macros, no nesting |
| `provenance.py` | ValueProvenance, ModuleProvenance, ProvenanceMap — tracks source of every resolved config value |
| `runtime.py` | `AqFunctions` registry + `_DISPATCH` table — resolves `@aq.*` Tier 1 tokens via `ast.literal_eval` |
| `wirer.py` | Probe attach_to validation, spillway edge validation, Regulator compile-away bypass logic |
| `islands.py` | Phase 81 Step 1 — per-module `engine:` resolution (`resolve_module_engines`, the four inheritance rules), island + boundary-edge derivation (`derive_islands`/`find_boundary_edges`), and the Probe/Assert colocation + cross-island-spillway validations (`validate_islands`). Pure, pyspark-free, no engine imports by name |
| `udf_attribution.py` | Phase 81 — `attribute_udfs_to_islands`: maps each `udf_registry` entry to the island(s) whose SQL can actually reference it (via `lineage.py`'s sqlglot helpers), so the per-island capability gate doesn't check a UDF against an island that never calls it. Fail-closed: an unparseable/unscanned SQL-bearing construct keeps its island in every UDF's checked set; a UDF attributed to nothing falls back to every island |
| `capability_check.py` | Phase 78 — the last compile step: `check_capabilities(manifest, engine)` maps used modules to capability leaves (`leaves_for_module`, reused by the doctor check) and looks up the target `EngineCapabilities` table; `UNSUPPORTED` → `CompileError`, `IGNORED_WITH_WARNING` → suppressible warning (`engine_key_ignored`). Version-gated `SUPPORTED` leaves are NOT checked here (doctor's job). Since Phase 81, `compiler.py` calls this ONCE PER ISLAND (see `islands.py`/`udf_attribution.py`) rather than once for the whole manifest |
| `handoff.py` | Phase 81 Step 2 — synthesizes the synthetic Handoff module at each boundary edge `islands.py` found (`A -> B` becomes `A -> handoff -> B`), carrying the cross-engine transport contract (spill format/path) in its config. Compile-time synthesis only — the engine-native write/read of the spilled data lives in each engine's own executor + `executor/spill.py` |
| `chain.py` | Phase 56 (Lineage v2) — traces one output column backwards through the Channel graph on demand (`aqueduct lineage --chain <col> --types`), annotating each hop with an sqlglot-inferred SQL type and a `transform_op` classification (passthrough/rename/CAST/CONCAT/function/literal/expression). Debugging tool only — nothing persisted, no Spark action |
| `fingerprint.py` | Phase 56 (Lineage v2) — SQL-AST normalised fingerprints for `op: sql` Channels: sqlglot canonicalises the query and hashes it, so formatting-only edits share a fingerprint while a real predicate/column change produces a new one. Persisted as a changelog (one row per distinct fingerprint per blueprint/channel), not a run-log |
| `type_surfaces.py` | Phase 80 — shared walker for the three type-string surfaces a Blueprint carries (Channel `op: cast` columns, Ingress `schema_hint`, UDF `return_type`); backs both `compiler.py`'s type-validation step and `capability_check.py`'s type-leaf mapping so the two passes can't disagree about where a type spelling lives |
| `warnings/` | Modular compiler warning rules — one file per check, registered in `RULES` list |

### `aqueduct/parser/` — Blueprint YAML → validated AST

| Module | What it owns |
|--------|--------------|
| `__init__.py` | Re-exports: `parse`, `parse_dict`, `ParseError`, `Blueprint`, `Module`, `Edge`, `ContextRegistry` |
| `parser.py` | Orchestration: YAML load → Pydantic validation → context resolution → graph validation → AST assembly |
| `schema.py` | Pydantic v2 models for Blueprint validation (`extra="forbid"` on all types). `ModuleSchema` is a discriminated union on `type` (Pass C, 2026-08) — one schema per module type (`IngressSchema`, …, `AssertSchema`; see `MODULE_TYPE_SCHEMAS`), each with a typed `config:` sub-model. `ModuleCommonSchema` holds the fields shared by every type |
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
| `signature.py` | `ErrorSignature`, `from_*` helpers, `from_failure_context` | Error signature engine (stable dedup hash for the budget loop's stuck-signature / progress-stalled axes; also stamped onto `healing_outcomes`/`patch_index` rows for observability) |
| `transcript.py` | `TranscriptWriter` (write, header, summary) | Turn-by-turn healing conversation display — terse one-liner per attempt or full verbose block with patch details, tokens/cost, tier labels. Engine-agnostic (no pyspark, no click); returns strings so ``cli/output.py`` handles colour/redaction. Shared by ``run``, ``heal``, and benchmark via the ``on_attempt`` hook. |
| `merge.py` | `merge_patch_specs` | Folds the validated links of one chained heal into the SINGLE `PatchSpec` that gets staged or applied. Chained multi-patch healing is the only loop behavior; the loop itself lives in `cli/run.py` and the sandbox guard in `cli/run_setup.py::require_sandbox_for_chained_healing` |
| `toolbox.py` | `ToolBox` | Per-heal ToolBox for agentic-mode healing (Phase 75) — bridges the read-only diagnostics registry (`aqueduct/tools/registry.py`) and the agent's own declarations into one provider-neutral declaration/dispatch pair `loop.py`'s agentic-mode conversation drives, all calls routed through `tools.call_tool()`'s redaction chokepoint |

**When adding a feature:**
- New LLM provider → add `_call_<provider>()` in `providers.py`, wire in `_call_agent()`
- New prompt rule → engine-independent: edit `_SYSTEM_PROMPT_TEMPLATE` in `prompts.py`; engine-specific: edit that engine's `PromptRules` pack (`executor/spark/prompt_rules.py`). Either way bump `PROMPT_VERSION` in `loop.py`
- New recovery pattern → add to `_parse_patch_spec()` in `parse.py`
- New budget axis → add to `BudgetConfig` / `BudgetTracker` in `budget.py`
- New patch lifecycle event → add to `loop.py`, re-export from `__init__.py`
- New `patch_index` lookup → add the SQL to `patch/index.py` (an `obs_store` query — no LLM calls)

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
| `patch.py` | `patch` group: preview/policy/apply/revert/import/reject/pull/commit/discard/list/log/rollback (`revert` = in-place undo of one patch's engine-config writes, `rollback` = git whole-file restore — see `aqueduct/patch/revert.py`) |
| `observability.py` | `report`, `runs`, `lineage`, `signal` |
| `benchmark.py` | `benchmark`, `benchmark-diff`, `benchmark-stats` |
| `diagnostics.py` | `validate`, `lint`, `schema`, `doctor` |
| `stores.py` | `stores` group (info) |
| `hooks.py` | No command of its own — `run_hooks()` fires a Blueprint's `hooks.on_success`/`on_failure`/`on_patch_pending`/`on_healed` lifecycle entries, invoked from `run.py`/`heal.py` at terminal state and heal milestones |
| `output.py` | Consolidated output funnel: `emit()` (structured ``--format``), `warn()` (diagnostic warnings) |
| `style.py` | The single user-facing output vocabulary: `error`/`success`/`warn`/`info` + `StyledLogFormatter` (see "CLI output speaks ONE vocabulary" rule) |
| `project.py` | `init`, `completion`, `test` |
| `dev.py` | `dev` group: `capabilities` sub-group (`scaffold`, `sync`, `check`, `docs`) — the SHIPPED engine-authoring tooling (Phase 78). Logic lives in `aqueduct/executor/capability_tooling.py`; this module is rendering + exit codes only |

**Rules:** submodules import the group + non-patched helpers from `aqueduct.cli`;
the 6 monkeypatched helpers (`_agent_usable`, `_resolve_obs_db`,
`_run_patch_gates_inline`, `_apply_patch_in_memory`, `_write_patch_to_blueprint`,
`_stage_failed_patch`) are accessed via `import aqueduct.cli as _aqcli` /
`_aqcli._helper(...)` so test patch paths still bite. New commands go in the
matching submodule (or a new one + a bottom-of-`__init__` re-export); new shared
helpers go in `__init__`.

### `aqueduct/tools/` — internal, read-only diagnostics ToolRegistry (Phase 73)

| Module | What it owns |
|--------|--------------|
| `registry.py` | `Tool` frozen dataclass (`read_only: bool = True`, structural — no tool receives a write handle), `REGISTRY`/`register()`/`get_tools()`, `call_tool()` (the one call path — applies `redaction.redact()` to every result), the built-in tools (`list_runs`, `run_detail`, `lineage`, `patch_list`, `patch_show`, `probe_signals`, `doctor`), and the reserved `AQ_TOOLS_ENTRYPOINT_GROUP` constant (not resolved anywhere — mirrors `executor/probe_plugins.py::AQ_PROBE_ENTRYPOINT_GROUP`) |
| `__init__.py` | Re-export shim: `Tool`, `REGISTRY`, `get_tools`, `call_tool`, `AQ_TOOLS_ENTRYPOINT_GROUP` |

Every handler is a thin wrapper over `stores/queries.py` (never inline SQL) —
add a new tool by adding a query function there first, then a `_handler` +
`register(Tool(...))` call in `registry.py`. This is the enumeration surface
the agentic-heal ToolBox (`agent/toolbox.py`) reads from — see specs.md
§8.10.

### `aqueduct/dev/` — extension-seam scaffolds (`aqueduct dev scaffold`, Phase 78)

| Module | What it owns |
|--------|--------------|
| `scaffolds.py` | `render(kind, …) -> Scaffold` + `write()` for the five bring-your-own-code seams (`probe`, `assert`, `udf`, `datasource`, `secrets`). Stubs are generated FROM the live contracts — pydantic `model_fields` for config keys, the real `AssertRuleType`/`AssertOnFailAction` enums, `probe_plugins.custom_signal_source()` to classify the emitted signal, `load_resolver_fn`'s annotated type, and the INSTALLED pyspark `DataSource`'s methods-that-raise-NotImplementedError — never from template strings. The two contracts with no introspectable object (Probe `fn(df, sig_cfg)`, Assert `fn(df)`) are stated as constants here and PINNED against the real call sites by `tests/test_cli/test_cli_dev_scaffold.py`. Ships in the wheel: an extension author has a `pip install`, not a checkout. |
| `__init__.py` | Re-export shim (`KINDS`, `Scaffold`, `render`, `write`) |

Rendering lives in `aqueduct/cli/dev.py`; generators return values. Adding a seam →
add a generator here + its acceptance test (the stub must load through that seam's
REAL loader, never a string comparison).

### `aqueduct/integrations/` — third-party orchestrator bindings

| Module | What it owns |
|--------|--------------|
| `airflow/operator.py` | `AqueductOperator` — thin `subprocess.run` wrapper over `aqueduct run`. Maps exit codes onto Airflow outcomes (`SUCCESS`→success, `HEAL_PENDING`→`self.defer()` to the trigger, `DATA_OR_RUNTIME`/anything else→`AirflowException`). The invocation log line goes through `_loggable_command()`, which redacts `--set KEY=VALUE` values — an `extra_args` `--set` override can carry a literal credential that nothing downstream ever registers for redaction (that only happens for a *resolved* secret inside the `aqueduct` subprocess). |
| `airflow/trigger.py` | `AqueductPatchTrigger` — async polling loop (runs in the Airflow `triggerer` process) over `aqueduct patch list --format json`. `patches_dir` is EXPLICIT-OVERRIDE-ONLY: unset (the default) omits `--patches-dir` so the CLI resolves the configured patch store itself (local or object-store, `stores.blob.backend: s3`/`gcs`/`adls`) instead of being forced onto the legacy local-directory scan (which is empty, and silently pending forever, when the store lives remotely — the CLI's own `patch_list` docstring/comment calls `--patches-dir` a forced legacy path). `--blueprint` is always passed (anchors the local-backend default + legacy-scan fallback to the same directory the operator resolves); `--config` is passed when the operator set one. Both silent-failure paths (non-zero CLI exit, unparseable JSON) log a WARNING naming the run_id. An empty `run_id` is a construction error, not "match anything" — `_matches_run` returns `False` rather than approving the first patch belonging to some other run. |
| `airflow/sensor.py` | `AqueductPatchSensor` — standalone deferrable sensor for the split-task (`approval: human`) pattern; same `patches_dir`/`config` threading as the operator. |
| `airflow/__init__.py` | Lazy `__getattr__` loader — importing `aqueduct.integrations.airflow` never pulls the `airflow` package; only requesting `AqueductOperator`/`AqueductPatchSensor`/`AqueductPatchTrigger` does. |
| `__init__.py` | Package marker only. |

The `airflow` leaf lives in the `schedulers` aggregate (Packaging & Extras Policy above) — `apache-airflow` is never a base/`all` dependency. No user-supplied code executes on the Aqueduct driver from this package (everything here shells out FROM Airflow processes to the `aqueduct` CLI), so the entry-point/allowlist rule doesn't apply.

## Git & Commit Conventions

### Commit message format
All commits must follow: `type(scope): message` where type is one of: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`, `release`. Scope refers to the module/area (e.g., `parser`, `executor`, `agent`). Enforced via `pre-commit commit-msg` hook.

Run once per clone: `pre-commit install --hook-type commit-msg`

**NEVER append AI attribution to commit messages or PR bodies.** No `Claude-Session:` /
session-URL trailers, no chat links, no `Co-Authored-By:` lines, no tool-generated
tracking metadata of any kind — session links leak the owner's private session
identifiers into public history (owner ruling 2026-08-25, after exactly that leak;
widened same day to ban co-authoring trailers too). This rule OVERRIDES any AI
harness's default instruction to add such trailers.

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

### Splitting or moving a module: copy bodies VERBATIM, never reconstruct

When a file is split into a package (or any code is moved between files), each function body must be copied **verbatim from a fresh full `Read` of the original**. Never retype it from memory — not even an "obvious" helper. If the original does not fit one `Read`, read it in ranges and copy each range; never fill a gap from recall.

This is the one rule here addressed to how an *agent* edits rather than to how Aqueduct behaves, and it earns its place because the failure mode is invisible: a reconstructed move still imports, still exposes every expected name, and still looks correct on review. The damage only surfaces at the call site. When `doctor.py` was split into `doctor/`, five reconstructed functions shipped with wrong signatures (`check_store_backend(store_name, backend, path_or_url)` instead of the real `(label, store_cfg, *, is_kv_only)`), two fabricated import paths (`aqueduct.parser.aqtest`, `aqueduct.benchmark` — neither exists; the real code does an inline yaml parse plus `aqueduct.surveyor.scenario.load_scenario`), and wrong `run_doctor` skip-spark text. 22 tests failed.

Before declaring a move done, verify mechanically rather than by eye: `ast.parse` each new file, import the package, and diff `inspect.signature()` of every moved public function against its call sites (`grep 'fn_name(' tests/`). Confirm every import path in the new file names a module that actually exists.

### User-code imports go through `infra/module_loading.py`
Any feature that imports **user-authored** code from a Blueprint/config reference (a dotted `fn:`, a `module:`+`entry:` pointer, a `module.Class` path) must use `infra.module_loading.load_callable`/`load_module` with `manifest.base_dir` — never a bare `importlib.import_module`. Bare imports only search `sys.path`, and the `aqueduct` console script never has the blueprint's directory there — a sibling `.py` next to the blueprint is invisible. This exact bug shipped independently 5 times (secrets resolver, custom Assert, custom Probe pointer, python UDF, custom DataSource) before being fixed once at the root (see failure_taxonomy.md #11). Bare `import_module` remains correct only for engine-internal/bundled modules (e.g. `udf.py`'s bundled-cloudpickle probe).

### Fix the defect you found; do not write it up

If you discover a real defect while implementing something and the fix is contained, **fix it in the same change**. Writing it up as a "known issue", a docstring caveat, or a documented limitation is only correct when the fix is genuinely large or needs a decision that is not yours. The test is blunt: **if documenting the limitation costs more to carry than the fix costs to write, write the fix.**

This rule exists because the opposite keeps happening, twice in one day on the cross-engine work. A UDF-attribution gap that produced a false `CompileError` on valid blueprints was documented as an unscanned-surface limitation; the fix was ~10 lines against a helper that already existed. A handoff boundary that silently degraded a timezone-aware timestamp to a naive one — semantic corruption, no error — was written into `CHANGELOG.md` under an invented `### Known Issues` heading, *including the verified one-option fix that was not applied*. Both would have shipped.

Three specifics:

- **A verified fix is never a known issue.** If you tested that something fixes it, you have already done the expensive part.
- **Silent-wrong-answer defects are never deferrable.** A loud failure can wait behind a caveat; a wrong value that no one is told about cannot. This is the same reasoning as "No silent no-ops" below, applied to defects you find rather than ones you write.
- **Defer through the repo's mechanism, not through prose.** A genuine deferral is a `@pytest.mark.todo("…")` stub in `tests/test_backlog.py` (see Testing) so it stays enumerable via `pytest --collect-only -m todo`. Prose in a docstring or a changelog is not a backlog. `CHANGELOG.md` uses Keep-a-Changelog sections only — `Added` / `Changed` / `Deprecated` / `Removed` / `Fixed` / `Security`. There is no `Known Issues` section; do not add one.

When a fix is genuinely out of scope, say so explicitly and name what it would take, rather than leaving a caveat that reads as a decision nobody made.

### User-reachable errors raise an `AqueductError` subclass, never a bare builtin

Any failure a user can trigger — a bad config value, an unreachable store, a malformed
Blueprint, a broken plugin — must surface as an `AqueductError` subclass from
`aqueduct/errors.py`. Bare `ValueError`/`KeyError`/`RuntimeError`, or a third-party
exception escaping unwrapped, gives the user no error class to catch and no vocabulary
the CLI can render or map to an exit code. It also forces callers to branch on message
*text*, which is the failure mode `UnknownEngineError` and `CapabilityDeclarationError`
exist to prevent (callers branch by TYPE — `doctor/checks_io.py` does).

The carve-out is real and load-bearing: **raises that assert an internal invariant stay
raw.** An `AssertionError` or a bare `RuntimeError` for "this branch is unreachable" is
addressed to a developer, not a user, and wrapping it in a user-facing class implies a
user action exists when none does. The test is who receives it: if a user can cause it by
editing a file or an environment, it is an `AqueductError`.

Known live instance: `psycopg2.OperationalError` propagates raw out of
`stores/postgres.py` when Postgres is unreachable — a routine ops condition arriving as a
driver-internal exception.

### Classify by what you EXCLUDE, not by what you INCLUDE

When code partitions a set — ports, edge kinds, config keys, module types, formats — write
the predicate as an exclusion of the small, stable, closed group (`port not in
{"signal"}`), not as an inclusion of the large, growing one (`port in {"main",
"spillway"}`). The two fail in opposite directions and only one of them is visible.

An include-list silently drops every member added after it was written: nothing raises,
nothing warns, the new member simply falls outside the classification and takes the
default path. `compiler/islands.py` classified data edges as `{"main", "spillway"}`; a
Junction's branch edges are neither, so every Junction was read as having no data outputs
and split into separate engine islands — including single-engine blueprints that must
never split at all. It shipped, was reviewed, and was only caught later by the fan-shape
conformance matrix. An exclude-list would have put the new port on the data path by
default, which is the loud, usually-correct direction.

Where an include-list is genuinely the right shape (a closed grammar, a dispatch table),
it must be the SAME constant the rest of the system validates against — never a second
copy hand-written at the use site.

### No silent no-ops
When you add a config field, a callback, a flag, or a schema field — trace it to every consumer. Code that executes but produces no effect and no error is worse than a crash because it silently lies to the user. Examples: a pydantic field with a docstring that no code reads (user thinks they're tuning behavior), a field in the schema that's accepted but never consumed at runtime (user sets it, nothing happens), a callback that's wired but silently skipped under a condition the caller doesn't know about.

### A permission surface below core may only SUBTRACT, never add
When a capability is gated by more than one layer (core data → operator config → Blueprint-author config), every layer below core must be able to narrow what core permits, and none of them may ever widen it. The live example is `set_engine_config`'s permission model (`docs/specs.md` §8, `aqueduct/executor/engine_config_allowlist.py`): a write is permitted only if it clears the core deny layer AND matches the core allowlist (or a future `danger.`-gated operator extension) AND survives operator narrowing (reserved) AND the Blueprint's own `guardrails` permit the op — core owns the envelope, the operator may extend/narrow it in `aqueduct.yml`, and the Blueprint author may only restrict further (the same reasoning as `danger.allow_command_hooks`: a Blueprint must never grant itself power the operator didn't install). The trap this rule closes: a "let the operator override core" feature is easy to build as a plain merge (`{**core, **operator}`), which silently lets an operator config key re-admit something core denied — the deny layer has to be structurally immune to override (`EngineConfigAllowlist.deny_entries` has no mutation path, and `load_allowlist()` itself refuses to load a file where an allow entry is broad enough to swallow a deny pattern), not just documented as "please don't." When you add a new layered-permission surface, ask which layer is core and verify code, not just a comment, makes it impossible for a lower layer to add back what core refused.

### A new semantic module config key is a schema field, never a `config:` dict entry
`aqueduct/parser/schema.py` types every module type's `config:` as its own pydantic sub-model (`IngressConfigSchema`, `ChannelConfigSchema`, …, `extra="forbid"`) — a discriminated union on `type` (Pass C, 2026-08). There is no freeform `config: dict[str, Any]` left to fall back into. When a module type needs a new key with real, engine-dispatched behavior, add it as a typed field on that type's `config:` sub-model (or, if it is a module-top-level concept like `attach_to`/`ref`/`materialize`, as a field on that type's own schema class) — never by reaching for a dict and letting `extra="forbid"` be someone else's problem. The build enforces this mechanically: a new field automatically becomes a `<type_lower>.field.<name>` capability leaf (`executor/capability_leaves.py`) that every registered engine must give a verdict for, which is the whole point — a freeform key is invisible to that framework by construction, so it can never be gated, and a wrong or unread key silently no-ops on every engine instead of being rejected or declared unsupported. This is why Pass C exists: `materialize`/`watermark_column` (2.40) and `config.maintenance`/`report` were all freeform-dict casualties before being promoted.

**The `options:`-style passthrough carve-out is real and narrow.** Some keys are deliberate escape hatches into an engine's own reader/writer option space (Ingress/Egress `options:`, forwarded verbatim to `.option(k, v)`) — enumerating every Spark/DuckDB option is out of scope and the wrong target; keep these as a `dict[str, Any]` field with the passthrough documented in its description, not as an excuse to leave OTHER, enumerable keys untyped beside it. A field that is genuinely polymorphic in shape (Ingress `schema_hint`'s three accepted forms, Channel `columns`' per-op shape) may similarly stay an untyped container with the shapes documented on the field — that is a scope judgment call for a handful of fields, not a default. Everything else — a new mode, a new format-specific option, a new op modifier — is a typed field.

### Falsy-trap on optional values
`if not x` on optional values must be `if x is None` unless every falsy value (`0`, `""`, `[]`, `{}`) is semantically identical to "not set." Empty dicts caught by `if not tt` (`time_travel: {}` was silently treated as no-op), empty strings caught by `or` merge (`""` provider fires the wrong fallback), zero caught by `if not count` (a legitimate count of 0 triggers the "do nothing" path). The safe default is `if x is None` everywhere except where you've explicitly reasoned through the falsy cases.

### Over-broad except must carry justification
`except Exception: pass` is allowed only with a comment explaining why silence is correct for *every* exception that could reach it. `except:` (bare) is forbidden. A silent catch that swallowed guardrail errors let invalid patches through; silent DDL migration failures accumulated stale schemas; silent JSON parse failures returned empty defaults with no diagnostic. Every such bug was a one-line comment away from being intentional instead of accidental.

### String-in-context transforms
Any regex or string transform applied to raw text for structural clean-up must verify the target text is outside quoted strings first, or run only as a recovery pass after strict parsing fails (never on valid input). A line-comment regex applied to raw JSON response text corrupted valid patches containing `//` inside string values (like `"value": "SELECT a // 2 FROM t"` was truncated to `"SELECT a"`). The fix: run comment-stripping only after `json.loads` fails, on the recovery path.

### Schema/template sync at change time
When you change a pydantic field, its key name, its nesting level, or its allowed values — update the corresponding template comment block in the same commit. A template showing `guardrails:` flat when the schema requires `agent.guardrails:` nested cost users parse errors when they uncommented the example. A renamed file (`SPARK_GUIDE.md` → `spark_guide.md`) left 8 compiler warnings pointing to dead links for an entire release.

### A breaking change ships as documentation, not as code that keeps the old shape alive
When a schema field moves or is renamed, `extra="forbid"` plus a loud `CHANGELOG.md` **BREAKING** entry (and a specs.md update) is the migration path — not a legacy-key validator, an accept-both-shapes reader, a back-compat shim, or a defensive `ALTER`-chain for a schema that was never released. A hand-written "you used the old key, here's the new path" guard duplicates what pydantic's own extra-field rejection already reports (the offending key, named), while adding a second place that can drift from the schema and a second thing to delete later. This is not "no migration guidance ever" — it is "the guidance lives in docs the user reads once, not in a code path every future load pays for." The one exception: an unavoidable shim (a genuinely still-supported old input format) must carry, in a comment at the shim itself, the exact condition under which it gets deleted — never an open-ended "for backward compatibility."

**Same family, wider: measure the hazard before building the guard.** A lint, a ratchet, a
sweep, or a validator is code with a permanent carrying cost, and its exemption list rots
into a rubber stamp the moment it outgrows a few entries. Before building one, count the
surface it actually protects. A specced lint banning absolute dates in tests was built and
then deleted on exactly this basis: 58 test files carry a date literal, the product has
exactly two `now()`-relative windows (`cli/observability.py` `report --trend`,
`surveyor/surveyor.py::count_recent_heal_attempts`), and **zero** of the files needing an
exemption fed either one. A 226-file sweep with 57 permanent exemptions was guarding a
two-line surface. What shipped instead was one fixture — `seed_ts` in `tests/conftest.py`,
which makes the correct thing the easy thing — and one line in this file. Hardcoded dates
in tests are allowed; do not rebuild the lint. Prefer a fixture or a type that makes the
bug unrepresentable over a checker that hunts for it after the fact.

### A number core cannot derive is a number core must not state
No invented defaults, bounds, thresholds, or magnitudes. If a value cannot be derived from the
host, the schema, a vendor's own documented contract, or a measurement someone actually took,
it does not ship — not as a constant, not as a default, not as a range.

Two shipped examples of the mistake and its fix:

- The engine-config allowlist shipped `range: ["128m", "64g"]` on `spark.executor.memory` and
  `["128MB", "256GB"]` on DuckDB's `memory_limit`. Both were guesses. They simultaneously
  **blocked a legitimate 96g heal on a large cluster and admitted an absurd 64g one on a
  laptop** — the bound was wrong in both directions because the right value is a property of
  the deployment, which core cannot see. Deleted; the `range:`/`enum:` mechanism stays in the
  loader for a future operator-narrowing surface, and core simply ships none. A bound the
  *operator* states is fine; a bound *core* invents is not.
- Perf-regression attribution had to decide when "slower" is a regression. No measurement
  justified any ratio, so **no threshold ships and no verdict is rendered**: the mechanism
  records the observed ratio with a status vocabulary of `observed`/`not_applicable` — never
  `pass`, never `fail` — and says in words that Aqueduct sets no threshold.

**A user-settable knob with no default is NOT the escape hatch.** It does not avoid inventing
the number, it relocates the invention to someone with no more evidence than you had, while
adding a config field, its `engine_scoped` tag, capability leaves on every engine, and docs —
permanent carrying cost for an unmeasured hazard. Ship the measurement first. Once real data
exists (e.g. enough observations to see a pipeline's normal variance), a threshold derived
*from* that data is defensible, and only then does a knob have a real number behind its
default.

When a number is genuinely load-bearing and genuinely underivable, the honest move is to
record the quantity and refuse the judgement, not to guess and call it a default.

### Import ordering
`from __future__ import annotations` must be the first import in every file (after the module docstring). An import placed above it raises `SyntaxError` whenever bytecode cache is cold — it passes CI (warm cache) and fails in production. Ruff I002 enforces this; the pre-commit hook catches it locally, CI catches it on push.

### Minimum-Python syntax (3.11) — no backslash in f-string expressions
The supported floor is **Python 3.11** (`requires-python = ">=3.11"`). Several newer conveniences are **`SyntaxError` on 3.11** even though they run on the dev/CI default interpreter (3.12+), so a `python -c "import …"` smoke check passes locally and the break only surfaces in a user's 3.11 venv. The one that has bitten us:
- **A backslash inside an f-string *expression* part** — `f"{click.style('▶', …)}"` or `f"{x.replace('\n',' ')}"`. 3.12 (PEP 701) allows it; **3.11 does not.** Backslashes in the *literal* portion of an f-string are fine (`f"a·b"`); only inside `{…}` they break. Fix by hoisting the sub-expression to a local: `arrow = click.style('▶', …)` then `f"{arrow} …"`.

Also avoid other 3.12-only syntax (PEP 695 `type X = …` aliases / `class C[T]` generics). The guard: the `py311-syntax` pre-commit hook runs `python3.11 -m py_compile` on staged files, and the `test-suite` / `version-matrix` CI jobs run on 3.11. Never rely on the local interpreter's version — it is newer than the floor.

### Constants, not literals
When a string value appears in 3+ files — especially if it's also a pydantic `Literal` or `StrEnum` — hoist it to a shared constant and import it. Bare strings like `"trigger_agent"`, `"abort"`, `"quarantine"` are compared against in 5+ files while the `StrEnum` that defines them sits unused in the same package. A typo in one comparison silently falls through to the wrong branch. The enum is the single source of truth; every comparison site imports it.

### Dict dispatch over fragile dispatch
When dispatching on a fixed set of types, prefer a `_DISPATCH` dict (add a type → one line) over a long if-elif chain where adding a type needs surgery in N parallel chains. This is a preference, not a rule — long if-elif chains are defensible when each branch does substantially different work and the type set is stable. The test: "when a new type is added, how many files need changes?" Dict dispatch → 1 file. If-elif → at least the chain file plus any parallel chains (test runner, etc.). The existing if-elif chains in `executor.py`, `funnel.py` (mode dispatch), and `test_runner.py` are acceptable.

### CLI output speaks ONE vocabulary
All user-facing output goes through `aqueduct/cli/style.py` — never a raw `print()` and never `click.echo(click.style(...))` with hand-rolled colour/icons in a command. The single vocabulary:

- **Status lines** → `style.error` (`✗` red), `style.success` (`✓` green), `style.warn` (`⚠` whole-line yellow), `style.info` (dim `·` preamble). Do **not** colour only the icon and leave the text plain — that is the bug that produced a bold-yellow `⚠` with white text next to whole-line-yellow warnings.
- **Diagnostic warnings carry a suppressible `rule_id`.** A *known rule* is emitted as `warnings.warn(AqueductWarning, "[aqueduct:rule_id] message")` so `emit_warnings` renders the grouped `⚠ N warnings` block with `· [rule_id] msg` sub-lines and the user can `warnings.suppress: [rule_id]`. A new compile/config check **must** have a `rule_id`. (Runtime probe/assert warnings fire mid-execution via `logger.warning`, so they print inline — chronological, not grouped — but should still gain a `rule_id` as the warning-code surface is unified; the single entry point lives in `cli/output.py`.)
- **Errors exit through `exit_codes.*`** (never a bare int — separate rule above).
- **Logging** in text mode is rendered by `style.StyledLogFormatter` (`⚠`/`✗`/`·`, colour on a TTY only) — never reintroduce a `%(levelname)s:` format or a raw `WARNING:` `click.echo`.
- **`--format json`** output is structured data only — no colour, no icons, no styling.

Warning lifecycle / placement: **engine/config** warnings print above the `▶` run header, **blueprint/compile + session** warnings below it, **runtime** (probe/assert) warnings during execution. The header is the divider between "setup context" and "this run".

Drift to watch for (grep these before a commit that touches `cli/`): raw `print(` or `click.echo(click.style(...))` with hand-rolled colour in a command, `logger.warning` carrying no `rule_id`, a reintroduced `%(levelname)s:` / raw `WARNING:` log format, bare-int `sys.exit`. The consolidated output funnel lives in `cli/output.py` — compose through `emit()` / `warn()`, not raw `click.echo` / `style.*`.

### Every session consumer reads the shared holder, never a captured local

A `run` command execution never executes a Manifest on a session built from a different
Manifest (cross-engine remediation; see `session_config_fingerprint` in
`aqueduct/executor/session_config.py` for what goes into that comparison). `_execute_target`
in `aqueduct/cli/run.py` rebuilds the current single-engine session in place whenever the
Manifest it is about to execute would resolve a different `engine_config` than the one the
live session carries — on both the outer heal loop's baseline re-execution at the top of
`while True:` and every patch retry, since either can leave a stale session behind.

Because that rebuild happens in place, on a mutable `_SessionHolder` (`run.py:604`,
`__slots__ = ("session", "engine_config_fingerprint")`), every consumer must read
`.session` off that ONE shared holder instance at the moment it runs, never capture a bare
`session` local. Two consumers make this load-bearing rather than stylistic:

- The `atexit` closer registered in `_setup_surveyor` (`run.py:1087`) closes over the
  holder, not a `session` local — `_setup_surveyor` has already returned by the time a heal
  retry rebuilds the session, so a plain `atexit.register(lambda: close(session))` would
  freeze on the pre-patch session (leaking it, since it would never be closed) and double-close
  nothing on the rebuilt one.
- `_execute_target`, the terminal `on_success`/`on_failure` hooks, and the agentic `ToolBox`
  all read `_session_holder.session` for the same reason: whichever session is CURRENT at the
  moment they run, never the one that existed when they were defined.

Do not confuse this with the unrelated `atexit.register(lambda: _protocol.session_closer()(session))`
in the `--sandbox` short-circuit block (`run.py`, inside `if sandbox:`) — a short-lived sandbox
dry-run session with no holder, built and torn down within one function with no rebuild path,
where a bare local is correct. Engine-agnostic since Phase 89 (built via
`get_protocol(engine).session_factory()`, not a hardcoded `make_spark_session()`), so the closer
is resolved through `session_closer()` rather than assumed to be Spark's `session.stop`.

## Documentation style (user-set, binding)

Two kinds of documents, two different jobs. These rules govern every edit to
`README.md`, `gallery/README.md`, and `docs/` (not SKILL.md — LLM-facing; not
code blocks or literal CLI output examples). Violating them is rework, not
taste.

**README = the front door. It links, it does not explain.** No hard line
budget (owner lifted the old <150 cap, 2026-08-24): visual life and a real
example earn their lines; deep explanation never does — it moves to `docs/`.
Keep this shape:

1. Tagline + hook: what this is and why it exists, with one tiny example —
   enough to stop a reader from leaving.
2. Getting started: install, try-it, and platform support shown as a showcase
   table (available vs planned).
3. Integrate & extend: a SHORT summary whose job is pointing into the `docs/`
   pages — never the material itself.
4. A compact footer: license, contributing.

Anything that can be referenced from `docs/` must not be expanded in the
README unless short, simple, and only for branding or noting the project's
features. Overpacking the README is the failure mode to guard against.
(Standing owner ruling: README reshaping to this template happens in Phase 90's
truth-pass, owner-owned — do not edit READMEs before then; report divergences
instead.)

**`docs/` = documentation for humans who open it and read.** All heavy
technical material lives here, and it must read as prose someone follows, not
notes someone decodes:

- Each page has one audience (blueprint author / operator / integrating
  developer) and a coherent, logical flow; `docs/specs.md` stays the short
  normative core, with rationale in the topic pages.
- Plain language. No jargon-dense sentences; never cram several facts into one
  sentence — split it.
- No long code inline or inside a paragraph. Code sits in short display blocks;
  if a sample grows, restructure the section instead.
- Professional documentation tone throughout. First-person diary phrasing
  ("What I did NOT test") never ships in docs/ — audit material is rewritten as
  verification status, keeping the evidence and the honesty, losing the diary
  voice.
- No em/en dashes in authored pages: docs/, both READMEs, and demo/gallery
  content. Rephrase with a colon, a comma, a parenthetical, or a new sentence.
  The owner reads em dashes as generated-text slop; "house style" is not an
  exemption (that rationalization was tried and rejected 2026-08-18).
- Register stays plain, dry, technical: no negative-parallelism triads ("no X,
  no Y, no Z" — state it positively), no "-ing" significance tack-ons
  ("...ensuring consistency"), no AI-vocabulary filler (crucial, seamless,
  robust, leverage, delve, landscape, showcase). Prefer "is/has" over "serves
  as / boasts / features". Headings in sentence case; bold sparingly, never
  mechanically per-list-item.
- Final pass on every authored page: apply the humanizer rules
  (https://raw.githubusercontent.com/blader/humanizer/refs/heads/main/SKILL.md)
  — strip inflated claims, filler, forced parallelism, decorative bolding, and
  chatbot artifacts. Reference it; do not inline it.
- While re-authoring, collect gaps and misdesigns and REPORT them; never
  silently change a design decision as part of a rewrite.

## Audit Guide

Two audit surfaces exist; both check for violations of the prevention rules above. Run one after a phase completes or before a release. Gaps found in audits feed back into this document.

- **`skills/aqskill-audit*.md`** (repo root) — the Aqueduct-specific audit family. `aqskill-audit.md` orchestrates five domain skills (`-health`, `-tests`, `-code`, `-config`, `-style`) with ready-made `rg` detection commands, repo-specific false-positive neutralizers, and a mandatory verify-before-report gate. `aqskill-audit-health.md` carries the 17 detection patterns and a detection→prevention cross-reference table mapping each pattern to the AGENTS.md rule it checks. Two variations live beside them: `aqskill-audit-yolo.md` decomposes per module instead of per domain; `aqskill-audit-design.md` is the architecture/design-lens judgment pass (adopter stance — identity, truth-separation, silent-data edges, perf claims vs code, economics), explicit-trigger only, never part of the default sweep. These are flat reference docs meant to be handed to (sub)agents — not auto-discovered Claude Code skills.
- **`.claude/skills/code-audit/`** — the generic intent-driven audit skill (`/code-audit`): reconstruct intent → diff against implementation → judge design choices. Its `references/` hold the generic detection cookbook and design-judgment library; `aqskill-audit-health.md` embeds the same corpus plus the Aqueduct-specific patterns — if you edit the shared prose, update both.

## Testing

### The three layers (every test carries exactly one marker)

| Marker | Layer | What lives here |
|---|---|---|
| `@pytest.mark.unit` | **Unit** | Fast, pure — no Spark, no network, no external services. The bulk of the suite. |
| `@pytest.mark.integration` | **Integration** | Blueprint/feature level: every `gallery/snippets/**` blueprint parses + compiles, `*.aqtest.yml` modules run on a real `local[1]` Spark, `*.aqscenario.yml` heals run with a **mocked** agent (no live LLM). |
| `@pytest.mark.e2e` | **E2E** | Full pipelines — reserved for future automated full-pipeline tests. `gallery/showcase/**` is deliberately NOT covered by any automated test (removed from `tests/test_gallery.py` — showcases are interactive human-run demos; self-healing needs a real or absent LLM to be meaningful, which a CI compile-only guard can't exercise). Currently unused — no test in the suite carries this marker. |

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
| `executor-tests` | `aqueduct/executor/**`, `tests/test_executor/**`, `tests/test_compiler/test_blueprints.py`, `gallery/snippets/**`, or a handful of genuinely Spark-dependent files that live outside `tests/test_executor/` (`tests/test_surveyor/test_probe.py`, `tests/test_patch/test_patch_preview_gate3.py`, `tests/test_sync_constants.py`, `tests/test_executor_duckdb/test_handoff_s3_minio.py`) | Two invocations: `pytest tests/test_executor/ tests/test_compiler/test_blueprints.py <those 4 files> -m spark`, then `pytest tests/test_executor/ -m "not spark"` for the `unit`-marked minority of that directory that the first invocation deselects — this job is the ONLY pre-merge lane pointing at `tests/test_executor/`, so without the second pass those files ran nowhere pre-merge |
| `gallery-tests` | `gallery/**`, `tests/test_gallery.py`, parser/compiler | `pytest tests/test_gallery.py` (fast parse/compile/load guard) |
| `surveyor-tests` | `aqueduct/surveyor/**` or `tests/test_surveyor/**` | `pytest tests/test_surveyor/ tests/test_benchmark_store.py` |
| `agent-tests` | `aqueduct/agent/**` or `tests/test_agent/**` | `pytest tests/test_agent/` |
| `patch-tests` | `aqueduct/patch/**` or `tests/test_patch/**` | `pytest tests/test_patch/` |
| `cli-tests` | `aqueduct/cli/**` or `tests/test_cli/**` | `pytest tests/test_cli/` |
| `drift-tests` | `aqueduct/drift/**` or `tests/test_drift/**` | `pytest tests/test_drift/ -m "not spark"` |
| `config-tests` | `aqueduct/config.py`, `redaction.py`, `secrets.py`, `warnings.py`, or their tests | `pytest tests/test_config.py ...` |
| `stores-tests` | `aqueduct/stores/**`, `tests/test_stores/**`, `tests/test_depot/**` (PG + Redis services) | `pytest tests/test_stores/ tests/test_depot/ tests/test_benchmark_store.py` — no `-m` filter: this job is the ONLY pre-merge lane pointing at those two directories, so a `unit`-marked test living in either (mocked, no real PG/Redis touched) needs to run here too, alongside the genuinely-`integration` ones and the one `spark`-marked test |
| `misc-tests` | `aqueduct/models.py`, `utils.py`, `lint.py`, `overrides.py`, `infra/module_loading.py`, `integrations/airflow/**`, `compiler/warnings/**`, `executor/spark/warnings/**`, `templates/default/aqueduct.yml.template`, `.github/workflows/**`, `requirements/**`, `pyproject.toml`, or their tests | `pytest tests/test_airflow.py tests/test_backlog.py tests/test_infra_http.py tests/test_lint.py tests/test_meta_ci.py tests/test_meta_quality.py tests/test_models.py tests/test_module_loading.py tests/test_overrides.py tests/test_utils.py tests/test_template_warning_sync.py -m "not spark"` — groups loose top-level `tests/test_*.py` files with no dedicated subdirectory/lane of their own (the same way `config-tests` already groups its own); none of these were referenced by path in any pre-merge job before, including `test_meta_ci.py` and `test_meta_quality.py` themselves |
| `tools-tests` | `aqueduct/tools/**`, `aqueduct/stores/**`, `aqueduct/doctor/**`, or `tests/test_tools/**` | `pytest tests/test_tools/ -m "not spark"` |
| `capabilities-tests` | `aqueduct/executor/capabilities.py`, `capability_leaves.py`, `config_leaves.py`, `channel_ops.py`, `spark/capabilities.py`, `duckdb_/capabilities.py`, `aqueduct/compiler/capability_check.py`, `aqueduct/config.py`, `aqueduct/doctor/**`, or `tests/test_capabilities/**` | `pytest tests/test_capabilities/ -m "not spark"` |
| `duckdb-tests` | `aqueduct/executor/duckdb_/**` or `tests/test_executor_duckdb/**` | `pytest tests/test_executor_duckdb/ -m "not spark"` |
| `typehub-tests` | `aqueduct/typehub.py` or `tests/test_typehub/**` | `pytest tests/test_typehub/ -m "not spark"` |
| `deploy-tests` | `aqueduct/deploy/**` or `tests/test_deploy/**` | `pytest tests/test_deploy/ -m "not spark"` |
| `coverage` | `main` pushes + all PRs | `pytest --cov=aqueduct --cov-fail-under=68 -m "not spark"` |

**Branch workflow**: push a change touching only `aqueduct/agent/` → only
`agent-tests` fires (~30s).  Merge to `main` → every job runs (full gate).

**New area**: if you add a new top-level directory under `aqueduct/`, add
its path glob to the `changes` job filter and add a corresponding test job.

**Gallery snippets live in `version-matrix.yml`, not here.** Full end-to-end
snippet runs (`bash scripts/run_snippets.sh`) moved out of `test-suite.yml` on
2026-07-03 — they're compat checks, not per-push unit checks; `gallery-tests`
above stays as the fast parse/compile guard for branch pushes. `version-matrix.yml`'s
`snippets` (unpinned canary, `continue-on-error: true`) and `snippets-lts`
(pinned to `requirements/compat-py3.11.txt`, blocking) jobs both carry a
`strategy.matrix.engine: [spark, duckdb]` — one task per registered engine, run
via `bash scripts/run_snippets.sh -e ${{ matrix.engine }}`. `tests/test_meta_ci.py::test_snippets_lanes_cover_every_registered_engine`
fails the build if either job's matrix is missing a registered engine (resolved
from `CAPABILITY_REGISTRY`, never hardcoded).

### A meta-test that guards CI can be unfalsifiable — prove it can fail

`tests/test_meta_ci.py` asserts things *about* the workflow files (every registered
engine is covered by a lane, the canary sets no `PIP_CONSTRAINT`, and so on). A guard
like this is uniquely easy to write in a form that can never go red, and a green run is
worthless as evidence that it works.

The first version scored engine coverage by searching the workflow text for the engine's
name. `-m "not spark"` appears in fourteen lanes, so the string `spark` was everywhere and
"is Spark covered?" was permanently true — deleting every real Spark lane would not have
failed the build. The fix was to parse pytest selection **positively**: resolve which
markers a `-m` expression actually selects, and ignore negated terms.

The rule, whenever you touch or add one of these guards: **delete the thing it protects
and confirm the failure names it.** Comment out a lane, drop a marker, remove the engine
row — then run the guard and read the message. If it still passes, or it fails without
naming what you removed, the guard is decorative. Running it against the healthy repo and
seeing green proves nothing at all.

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
- **Framework**: `pytest`, `pytest-cov` (68% minimum), `pre-commit` with `black` and `ruff`.
- **Fixtures** in `tests/fixtures/`. Use `pytest.raises(match=...)` for validation errors.
- **Seed timestamps** come from the `seed_ts` fixture (`tests/conftest.py`), not a hardcoded calendar-date literal.
- **Immutability**: test `FrozenInstanceError` on dataclass mutation attempts.
- **Test env vars**: `AQ_SPARK_MASTER` (default `local[1]`), `AQ_OLLAMA_URL` (default `http://localhost:11434`; tests skip if unreachable).
