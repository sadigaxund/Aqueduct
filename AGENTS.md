# AGENTS.md — Aqueduct Development Guidebook

## Project context

Aqueduct is a declarative Spark/DuckDB blueprint engine with LLM-driven
self-healing. Python 3.11+, monolithic CLI on the executor's driver process,
no servers.

**4-layer boundary**: `Parser` → `Compiler` → `Executor` → `Surveyor`. Modify
only the layer relevant to the task. Topo sort, Probe insertion, and
parallel-component detection are Executor sub-steps, not a "Planner" layer.

**Key deps**: `pyspark` (optional `[spark]`), `duckdb` (engine + embedded obs
store), `pydantic`/`ruamel.yaml`/`pyyaml`, `click`, `sqlglot` (SQL lineage —
never hand-roll a SQL parser), `httpx` (no `anthropic` SDK).

**`pyspark` stays out of `parser`/`compiler`/`surveyor`/`patch`/`depot`.**
Three documented lazy (function-body-only) exceptions: **doctor**
(`doctor/__init__.py`'s Spark/storage/cloudpickle/handoff checks, so
`--skip-spark` works), **surveyor error extraction**
(`surveyor.py::_extract_structured_error` imports `PySparkException` in a
`try/except`), **dashboard** (`dashboard/app.py::main()`, a narwhals/plotly
circular-import workaround).

Other standing rules: `@dataclass(frozen=True)` everywhere, each compile step
returns a new object via `dataclasses.replace()`; never `count()`/`show()`/
`collect()` in a Probe's critical path (added actions ship behind a
`metrics:` flag defaulted `false`); Channels/UDFs populate `_aq_error_*`
spillway columns via `try/except` instead of aborting the stage.

## Documentation map

`docs/specs.md` is the **engine reference**; other docs own their surfaces
and specs.md cross-references rather than duplicates.

| Doc | Owns | When to read |
|---|---|---|
| `docs/specs.md` | Blueprint format, architecture §3, Modules §4, Context Registry §5, Self-Healing §8, Type System §9, Deployment/Engine Integration §10, Engine Scope §11 | Domain semantics |
| `docs/cli_reference.md` | Every CLI command/flag + defaults | New `@click.option`/subcommand |
| `docs/observability_guide.md` | Store schemas + SQL cookbook | DDL changes, post-mortem queries |
| `docs/spark_guide.md` | Compiler warnings, perf, Spark gotchas | Executor modules, new Channel ops |
| `docs/production_guide.md` | Cluster deploy, danger settings, patch lifecycle | Running on a cluster |
| `docs/compatibility.md` | Python × Spark matrix, pinning | Version pins in `pyproject.toml` |
| `docs/extending.md` | Engine-author guide: `ExecutorProtocol`, capability workflow | Adding an execution engine |
| `docs/failure_taxonomy.md` | Recurring defect classes + detection patterns | Triaging a bug fix |
| `SKILL.md` (root) | Blueprint-authoring guide: grammar, module types, `agent:` block, providers | Grammar/`agent:`/provider changes — sync with specs.md |

AGENTS.md itself is process/constraint guidance only.

## Packaging axes

Optional deps follow **two axes only** — never a feature-named extra.

- **Per-vendor leaves**: `aws`, `gcp`, `azure`, `postgres`, `redis`, `airflow`,
  `object-store` → aggregates `secrets`/`stores`/`schedulers`/`all`.
- **Engine leaves**: `spark`, `duckdb` — each registers an `aqueduct.engines`
  entry point + own `capabilities.yml`; both roll into `all` but into no
  "engines" aggregate (running >1 engine at once is normal, not a bundle).

New dep → map onto an existing axis; never a standalone flag.

**Dev-tooling carve-out**: `dev` (pytest/black/ruff) is the only sanctioned
feature-named extra — never runs in the data path, stays out of `all`.
`[mcp]` was deleted this phase; `dev` is now the only such extra.

## Layer rules & Source Code Navigation Map

**Every extensibility seam is an entry-point group + an allowlist** —
discovery is never authorization (`aqueduct.engines`, `aqueduct.probe_signals`,
reserved `aqueduct.tools`). The patch grammar is NOT a seam — ops are a closed
list, permanently.

**Trace every consumer before changing a type or output path** — grep every
call site first.

Use this map as the first filter before grepping the tree.

### `aqueduct/executor/`
| Module | Owns |
|---|---|
| `models.py`, `path_keys.py`, `probe_plugins.py`, `channel_ops.py`, `probe_sampling.py` | engine-agnostic pyspark-free shared registries |
| `capabilities.py` | capability model + registry, `load_engines()`; no default-verdict sweep |
| `capability_leaves.py` | grammar leaf walker over `parser/schema.py` |
| `config_leaves.py` | `aqueduct.yml` leaf walker, filtered by `engine_scoped` tag |
| `capability_tooling.py` | `scaffold`/`sync`/`check`/`docs` behind `aqueduct dev capabilities` |
| `spark/capabilities.py`+`.yml`, `duckdb_/capabilities.py`+`.yml` | each engine's own declaration (data) |
| `protocol.py` | `ExecutorProtocol`+`PromptRules`; `register_protocol()` raises `EnginePluginError` if incomplete |
| `session_config.py` | per-engine `SessionSpec` resolution + fingerprint |
| `orchestrator.py`, `spill.py` | multi-island execution + cross-engine handoff spill lifecycle |
| `spark/engine.py`, `spark/prompt_rules.py` | Spark's entry-point target (lazy pyspark) + its `PromptRules` pack |
| `spark/` | Spark code: ingress/egress/channel/executor/junction/funnel/probe/session/udf/assert_ |
| `__init__.py` | `get_executor(engine)` via `protocol.get_protocol(engine).execute` |

### `aqueduct/surveyor/`, `aqueduct/patch/`
| Module | Owns |
|---|---|
| `surveyor/surveyor.py` | Main class: start/record/stop |
| `surveyor/error_extraction.py` | Structured Spark/Py4J error extraction (lazy pyspark) |
| `surveyor/models.py` | `RunRecord`, `FailureContext` |
| `surveyor/scenario.py`, `webhook.py`, `benchmark_store.py` | scenario framework, HTTP dispatch, DuckDB benchmark persistence |
| `patch/grammar.py` | `PatchSpec`, 14 op types, discriminated union |
| `patch/operations.py`, `apply.py` | per-op impl + apply orchestrator |
| `patch/index.py` | `patch_index` table — patch lifecycle truth |
| `patch/preview.py` | Lineage gate (Gate 2) + sandbox gate (Gate 3) |
| `patch/ci.py` | Heal-as-PR / patch-import kit |
| `patch/revert.py` | Undo path for an applied heal patch — no new patch op |
| `patch/provenance.py` | classifies ops `dialect_neutral`/`engine_shaped` for cross-engine recompile |

### `aqueduct/stores/`, `aqueduct/infra/`
| Module | Owns |
|---|---|
| `stores/base.py` | ABCs: `ObservabilityStore`, `LineageStore`, `DepotStore`, `RelationalCursor` |
| `stores/duckdb_.py`/`postgres.py`/`redis_.py` | backend implementations |
| `stores/object_store.py` | `ObjectStore`/`BlobStore`/`PatchStore` |
| `stores/read.py` | canonical backend-aware read resolver |
| `stores/queries.py` | THE read-time query layer — every viewer goes through here |
| `infra/http.py` | outbound-HTTP mechanics: retry/backoff/HMAC/fire-and-forget |
| `infra/module_loading.py` | `load_module`/`load_callable` — the ONE way to import user code by dotted path |

### `aqueduct/depot/`, `aqueduct/drift/`, `aqueduct/doctor/`
| Module | Owns |
|---|---|
| `depot/depot.py` | `DepotStore` façade |
| `drift/classifier.py` | baseline-vs-live schema diff (dropped/type-changed = breaking, added = benign) |
| `drift/store.py` | `drift_checks` audit table, self-owned baseline |
| `doctor/__init__.py` | Spark/network/blueprint-source checks + `run_doctor` |
| `doctor/checks_io.py` | leaf connectivity checks (config/depot/obs/webhook/agent/secrets/capabilities) |

### `aqueduct/compiler/`, `aqueduct/parser/`
| Module | Owns |
|---|---|
| `compiler/compiler.py` | orchestrator: Tier 1 resolution, Arcade expansion, per-island capability gate, Manifest assembly |
| `compiler/lineage.py` | sqlglot column lineage + referenced-function-name extraction |
| `compiler/runtime.py` | `AqFunctions` registry, `@aq.*` dispatch |
| `compiler/islands.py` | per-module engine resolution, island + boundary-edge derivation |
| `compiler/udf_attribution.py` | maps `udf_registry` entries to referencing islands |
| `compiler/capability_check.py` | last compile step — module→leaf mapping, per-island engine check |
| `compiler/handoff.py` | synthesizes the Handoff module at each cross-engine boundary |
| `compiler/chain.py`, `fingerprint.py` | on-demand lineage trace; sqlglot-normalized SQL fingerprints |
| `parser/parser.py` | YAML → validated → context-resolved → graph-validated AST |
| `parser/schema.py` | Pydantic v2, discriminated union on `type`, `extra="forbid"` |
| `parser/graph.py` | cycle detection + topo order + spillway validation |

`parser/parser.py` importing `get_path_keys` from `executor/path_keys.py` is
an accepted cross-layer exception (no Spark imports involved).

### `aqueduct/agent/` — LLM agent loop
| Module | Owns |
|---|---|
| `loop.py` | `generate_agent_patch`, `stage_patch_for_human`, `PROMPT_VERSION` |
| `cascade.py` | multi-model healing cascade |
| `prompts.py` | engine-independent prompt scaffold |
| `providers.py` | HTTP dispatch to Anthropic/OpenAI-compatible endpoints |
| `parse.py` | response parsing, structural error detection, reprompt formatting |
| `budget.py`, `signature.py` | budget tracking; stable error-dedup hash |
| `transcript.py` | turn-by-turn healing display (engine-agnostic) |
| `merge.py` | `merge_patch_specs` — folds one chained heal into a single `PatchSpec` |
| `toolbox.py` | per-heal `ToolBox`, routed through `tools.call_tool()` |

**Prompt is COMPOSED, not monolithic**: the engine-independent scaffold
(`prompts.py`) names no engine; engine-flavored strings live in that engine's
`PromptRules` pack, pulled through `ExecutorProtocol.prompt_rules`. `agent/`
imports no engine module by name.

**`PROMPT_VERSION` bump policy**: bump only when the *composed* prompt body
changes (scaffold, any `PromptRules` pack, op table, worked example) — not for
failure-path tooling or a refactor with identical composed output.

### `aqueduct/cli/`
| Module | Commands |
|---|---|
| `__init__.py` | `cli` group, shared `_*` helpers, command registration |
| `run.py` | `run` (+`--sandbox`), `compile` |
| `heal.py`, `drift.py` | `heal`; `drift` |
| `patch.py` | `patch` group: preview/policy/apply/revert/import/reject/pull/commit/discard/list/log/rollback |
| `observability.py` | `report`, `runs`, `lineage`, `signal` |
| `benchmark.py` | `benchmark`, `benchmark-diff`, `benchmark-stats` |
| `diagnostics.py` | `validate`, `lint`, `schema`, `doctor` |
| `output.py`, `style.py` | output funnel + the one user-facing output vocabulary |
| `project.py` | `init`, `completion`, `test` |
| `dev.py` | `dev capabilities scaffold|sync|check|docs` |

6 monkeypatched helpers (`_agent_usable`, `_resolve_obs_db`,
`_run_patch_gates_inline`, `_apply_patch_in_memory`, `_write_patch_to_blueprint`,
`_stage_failed_patch`) are accessed via `import aqueduct.cli as _aqcli` so
test patch paths keep biting.

### `aqueduct/tools/`, `aqueduct/dev/`, `aqueduct/integrations/`
| Module | Owns |
|---|---|
| `tools/registry.py` | read-only diagnostics registry — every handler wraps `stores/queries.py` |
| `dev/scaffolds.py` | generates seam stubs (probe/assert/udf/datasource/secrets) FROM live contracts |
| `integrations/airflow/` | thin `subprocess`/polling wrappers over the `aqueduct` CLI; no user code on the driver |

## Capability workflow (short)

Any new grammar leaf or `aqueduct.yml` config field needs an explicit verdict
per engine — **the build breaks until you give one.**

1. Add the field/op/mode. A `config.*` field must also carry
   `Field(..., json_schema_extra={"engine_scoped": True|False})` — no
   untagged state (raises `CapabilityScopeError`); an `engine.<name>.*` field
   tagged `False` also raises. Build goes red:
   `CapabilityDeclarationError`/`CapabilityScopeError` + `test_closure.py`.
2. `aqueduct dev capabilities sync` — appends the leaf to every engine's
   `capabilities.yml` as `undeclared` (a sentinel, not a verdict — still red).
3. Replace each `undeclared` with a real verdict (`supported`/`unsupported`/
   `ignored_with_warning`, optional `requires`/`hint`). An EXECUTION leaf
   (`module.type.*`, `channel.op.*`, format/mode — never `config.*`) marked
   `supported` also needs `tests:` naming the pytest id(s), or
   `test_verdict_test_links.py` fails. Never clone another engine's table.
4. Build passes; `aqueduct dev capabilities docs` regenerates
   `docs/compatibility.md`.

**Engine-scoped tagging**: only fields that genuinely dispatch through an
engine are `True`; the rest (`webhooks.*`, `secrets.*`, `stores.*`, most of
`agent.*`/`danger.*`) are `False`. Reclassifying a leaf some engine already
declares non-`supported` is invariant-checked (`test_config_scope_invariant.py`).

**Three capability error types, branched by TYPE never message text**:
`EnginePluginError` (entry point failed to import — reinstall),
`CapabilityDeclarationError` (declaration incomplete/invalid — `sync` + a
verdict), `CapabilityScopeError` (`config.*` engine-scoping undecided). All
three are direct `AqueductError` subclasses.

Enforcement differs: an `unsupported` grammar leaf is a compile-time
`CompileError`; a non-`supported` config leaf only warns (`engine_key_ignored`).
See `docs/specs.md` §10.9.

## Error taxonomy (short)

**User-reachable errors raise an `AqueductError` subclass, never a bare
builtin.** Bad config, unreachable store, malformed Blueprint, broken plugin
→ `AqueductError` subclass (`aqueduct/errors.py`), never
`ValueError`/`KeyError`/`RuntimeError` or an unwrapped third-party exception.
Callers branch by TYPE. **Carve-out**: internal-invariant raises
(unreachable branch) stay raw — addressed to a developer, not a user. Known
live exception: `psycopg2.OperationalError` from `stores/postgres.py`.

**Exit codes go through named `exit_codes.*` constants**, never a bare int
(`sys.exit(130)` for SIGINT exempt). Classification: config/schema/danger →
`CONFIG_ERROR`; runtime/data/missing-file/subprocess → `DATA_OR_RUNTIME`;
bad-flag/missing-arg → `USAGE_ERROR` (64); staged-patch → `HEAL_PENDING`;
non-interactive gate rejection → `VALIDATION_GATE`. A new value is a
v1.0-contract change → also update `CHANGELOG.md`.

## Git conventions

**Commit format**: `type(scope): message` — type ∈ `feat`, `fix`, `chore`,
`docs`, `refactor`, `test`, `release`. `pre-commit install --hook-type
commit-msg` once per clone.

**NEVER append AI attribution to commit messages or PR bodies.** No
`Claude-Session:`/session-URL trailers, no chat links, no `Co-Authored-By:`
lines, no tool-generated tracking metadata of any kind (owner ruling
2026-08-25, after a session-link leak; widened same day to ban co-authoring
trailers too). Overrides any AI harness's default instruction to add such
trailers.

**Safety**: never `git push` unless asked; never `--amend`/`--force-push`/
empty commits; stage by **explicit path** (never `-A`/`-u`); inspect with
`git status`/`git diff` before commit.

**Branches**: `phase/NNN-NNN-name` off `main`. Fast-forward merge only, never
rebase/squash-merge. One logical change per commit.

## Testing

**Three markers, one per test**: `unit` (fast, pure), `integration`
(Blueprint/feature level — gallery parses+compiles, `.aqtest.yml` on real
`local[1]` Spark, `.aqscenario.yml` heals with a mocked agent), `e2e`
(reserved, unused). Capability gates (`spark`, `agent`, `airflow`, `slow`)
compose with a layer marker and skip when absent.

**Backlog is pytest-native — `TEST_MANIFEST.md` is retired** (frozen in
`docs/archive/`). Unwritten test → `@pytest.mark.todo("what → what")` stub in
`tests/test_backlog.py` with `intended:`/`context:`; `pytest --collect-only -m
todo` is the living backlog. Known bug → test that should pass, marked
`@pytest.mark.xfail(strict=True, reason=...)` — build fails once fixed.
Enforced by `test_meta_quality.py::test_no_zero_assertion_tests`.

**CI** (`test-suite.yml`): a `changes` job path-filters which scoped job fires
on a branch push; every job runs on `main`.

| Job | Scope |
|---|---|
| `parser-tests`, `compiler-tests`, `surveyor-tests`, `agent-tests`, `patch-tests`, `cli-tests`, `drift-tests`, `config-tests` | matching `aqueduct/` package |
| `executor-tests` | `aqueduct/executor/`, gallery snippets, + a few Spark-dependent files outside `tests/test_executor/` |
| `gallery-tests` | fast parse/compile/load guard |
| `stores-tests` | `stores/`, `tests/test_depot/` (PG+Redis) — only pre-merge lane for those dirs |
| `misc-tests` | loose top-level `tests/test_*.py` (incl. `test_meta_ci.py`, `test_meta_quality.py`) |
| `tools-tests`, `capabilities-tests`, `duckdb-tests`, `typehub-tests`, `deploy-tests` | matching package |
| `coverage` | `main` + all PRs, `--cov-fail-under=68` |

Gallery e2e snippet runs live in `version-matrix.yml` (`snippets` canary +
`snippets-lts` pinned-blocking), one task per registered engine —
`test_meta_ci.py` fails if either job's matrix is missing an engine.

**A meta-test guarding CI can be unfalsifiable — prove it can fail.** Before
trusting/adding a workflow-file guard, delete the thing it protects and
confirm the failure names it. A green run alone proves nothing.

**Constraints**: no live LLM calls in pytest (mock `httpx.post`/`_call_agent`);
no mocking `SparkSession`, use the real fixture; seed timestamps from
`seed_ts`, never a hardcoded date; test `FrozenInstanceError` on mutation.
Run `.venv/bin/python -m pytest`, always `-p no:randomly`, never the full
suite locally (OOM risk), one process at a time.

## Doc style (user-set, binding)

**README is the front door — OWNER-OWNED.** Never edit `README.md` or
`gallery/README.md`; report divergences instead.

**`docs/` is prose for humans**: one audience per page, plain language, no
jargon-dense or multi-fact sentences, code in short display blocks, no
first-person diary phrasing.

**No em/en dashes** in `docs/`, both READMEs, or gallery content — use a
colon, comma, parenthetical, or new sentence.

**No AI-vocabulary filler** (crucial, seamless, robust, leverage, delve,
landscape, showcase); no negative-parallelism triads; no "-ing"
significance tack-ons; prefer "is/has" over "serves as/boasts/features".
Sentence-case headings; bold sparingly.

**No `Phase NN`/`Sprint NN` on user-facing surfaces** (docs, templates,
gallery, README, CONTRIBUTING) — allowed in `CHANGELOG.md`/`TODOs.md` and
source comments. A rendered field (e.g. `capabilities.yml` `hint:` copied
into `docs/compatibility.md`) counts as user-facing. Verify:
`rg -nE "Phase [0-9]|Sprint [0-9]" docs/ gallery/ aqueduct/templates/ README.md CONTRIBUTING.md`
— mechanically enforced for `docs/`/`aqueduct/templates/`/`gallery/`/
`README.md` by `test_meta_quality.py::test_no_phase_sprint_on_user_facing_surfaces`.

Final pass: apply the humanizer skill rules. While re-authoring, collect
gaps/misdesigns and report them; never silently change a design decision.

## Bug-family rules

Each prevented 2+ recurring bugs. Where a mechanical check exists, it's
named — do not rebuild it.

- **Splitting/moving a module: copy bodies VERBATIM, never reconstruct from
  memory.** A reconstructed move still imports and looks correct on review —
  damage surfaces only at the call site (a `doctor.py` split once shipped 5
  wrong signatures, failing 22 tests). Read the original in ranges if it
  doesn't fit one `Read`. Verify: `ast.parse` the new file, diff
  `inspect.signature()` against call sites.
- User-code imports go through `infra/module_loading.py` — never bare
  `importlib.import_module` (only searches `sys.path`).
- Fix the defect you found; do not write it up. A verified fix is never a
  "known issue"; deferrals are a `todo` stub in `test_backlog.py`, never
  prose (there is no `CHANGELOG.md` "Known Issues" section).
- Classify by what you EXCLUDE, not INCLUDE, for a closed set — an
  include-list silently drops every new member.
- No silent no-ops — trace every new field/callback/flag to its consumer.
- A permission surface below core may only SUBTRACT, never add (no
  `{**core, **override}` merges that re-admit what core denied).
- A new semantic module config key is a typed schema field, never a
  `config:` dict entry (`extra="forbid"` everywhere); the `options:`
  passthrough carve-out is real but narrow.
- Falsy-trap: `if not x` on an optional must be `if x is None` unless every
  falsy value means "not set."
- Over-broad `except Exception: pass` needs a comment justifying it; bare
  `except:` is forbidden.
- String-in-context transforms (regex on raw text) must verify outside
  quoted strings, or run only as a post-parse-failure recovery pass.
- Schema/template sync at change time — enforced by
  `test_template_warning_sync.py`.
- A breaking schema change ships as documentation (`extra="forbid"` + a
  `CHANGELOG.md` **BREAKING** entry + specs.md update), not a back-compat
  shim, except a genuinely still-supported old format carrying its exact
  deletion condition in a comment.
- Measure the hazard before building a guard — count the surface it
  protects first (an absolute-date-in-tests lint guarded 2 files against 58
  needing exemption; deleted for one fixture).
- A number core cannot derive is a number core must not state. No invented
  defaults/bounds. A user-settable knob with no default relocates the
  invention rather than avoiding it — ship the measurement first.
- Import ordering: `from __future__ import annotations` first — ruff I002.
- Python 3.11 syntax only — no backslash inside an f-string expression, no
  PEP 695 syntax. Enforced by the `py311-syntax` pre-commit hook; CI runs on
  3.11, never trust the local interpreter's version.
- Constants, not literals — hoist a string in 3+ files to a shared constant.
- Dict dispatch over fragile dispatch for a fixed, evolving type set.
- CLI output speaks ONE vocabulary — `cli/style.py`
  (`error`/`success`/`warn`/`info`) via `cli/output.py`; never raw `print()`.
  Warnings carry a suppressible `rule_id`; exits go through `exit_codes.*`.
- Every session consumer reads the shared holder, never a captured local —
  `_SessionHolder` (`cli/run.py`) may be rebuilt in place mid-run; `atexit`
  closers and terminal hooks read `.session` off the holder at call time.

## Change-trigger matrix

Whenever you touch the left column, update the right column in the same
commit.

| If you change … | You must update … |
|---|---|
| DDL/`ALTER TABLE` in `surveyor/`/`executor/` | `docs/observability_guide.md` schema table (+cookbook recipe if useful) |
| `@click.option`/new subcommand | `docs/cli_reference.md` flag table |
| A pydantic field in `config.py`/`parser/schema.py` | The matching template comment block |
| `StopReason`/`BudgetConfig`/apply-gate behaviour | `docs/specs.md` §8 + `heal_attempts` section |
| New/renamed `aqueduct.yml` key/block, or `stores.*` backend | `docs/specs.md` (bump `Version X.Y`) + the template block |
| `agent.approval` modes, patch ops, exit-code contract | `docs/specs.md` §8 + §10.7 |
| Production/deployment/danger-setting/cluster config | `docs/production_guide.md` |
| Spark compiler-warning/perf/tuning behaviour | `docs/spark_guide.md` |
| `pyproject.toml` version pins or Python/Spark range | `docs/compatibility.md` prose only — the capability matrix + `COMPAT_RESULTS` blocks are GENERATED |
| New/registered execution engine | `version-matrix.yml` `compat` job + a pre-merge `test-suite.yml` lane — `test_meta_ci.py` enforces coverage |
| Any newly deferred/aspirational item | The issue tracker — never inline "deferred" prose into specs.md |
| New file under `docs/` | `README.md` References list + this Documentation map |
| Any testable feature | A real test, or a `todo`/`xfail` stub — never `TEST_MANIFEST.md` |
| Any phase/sprint/shippable change | `CHANGELOG.md` `[Unreleased]` only — never bump version |
| New `@aq.*` function in `compiler/runtime.py` | `docs/specs.md` §5.3 + `_DISPATCH` table |
| New path-key entry in `executor/path_keys.py` | The module's schema model (`Annotated[str, FsPath()]`) |
| New exit code | `docs/cli_reference.md` exit-code reference (+`CHANGELOG.md` — v1.0 contract) |
| Blueprint grammar / `agent:` block / provider wiring | `SKILL.md`, same commit as specs.md/schema |
