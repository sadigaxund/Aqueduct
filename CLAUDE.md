# CLAUDE.md — Aqueduct Development Guidebook

## Project Context
Aqueduct is an intelligent, declarative Spark blueprint engine with LLM-driven self-healing.
- **Full spec**: `docs/specs.md` (20+ pages, read it once, don't restate it).
- **This file**: Practical guidance for working on the codebase.

## Tech Stack (The "Vibe Build" Choices)
- **Language**: Python 3.11+ — you're strongest here, PySpark is first-class, and control-plane performance is irrelevant.
- **Architecture**: Monolithic CLI. Runs on the Spark driver. No servers, no microservices.
- **Key Dependencies**:
  - `pyspark` (optional — `aqueduct-core[spark]` extra; not imported outside `aqueduct/executor/spark/`)
  - `pydantic` + `ruamel.yaml` + `pyyaml` (schema validation, YAML round-trips)
  - `click` (CLI)
  - `duckdb` (embedded observability store — avoids SQLite locks)
  - `sqlglot` (SQL lineage — do NOT write a custom parser)
  - `httpx` (HTTP client for webhooks and LLM calls)
  - LLM self-healing uses `httpx` (already a core dep) — no `anthropic` SDK, no extra install needed

## Development Priorities (Vibe Build Order)
Build in this exact sequence to validate assumptions early. Phases 1–8 are complete as of v0.1.0. Follow this and never read the TODO.md file, thats for human reference only.

1. **Parser** → Validate Blueprint YAML, resolve static context, output AST. ✅
2. **Compiler** → Resolve Tier 1 runtime functions, expand Arcades, wire Probes/Spillways, output fully resolved Manifest. ✅
3. **Ingress/Egress Wrapper** → Prove Spark I/O works via YAML config. ✅
4. **Single SQL Channel** → Register upstream as temp view, run query. ✅
5. **Mock Surveyor** → On failure, just log/webhook; no LLM yet. ✅
6. **Patch Grammar (Manual)** → `aqueduct patch apply` works from CLI. ✅
7. **LLM Integration** → Final step. ✅
8. **Resilience, Lineage, Self‑Healing** → RetryPolicy, deadline, column lineage (sqlglot), Arcade validation, LLM loop fully wired. ✅

## Code Organization & Safety
- **Layered Architecture**: Respect the 5 core layer boundaries (`Parser` -> `Compiler` -> `Planner` -> `Executor` -> `Surveyor`). Put logic in the correct layer and module.
- **Dual-Format Contract**: Humans and CLI tools write YAML (`Blueprint`), but the engine and runtime LLM agents exclusively consume JSON (`Manifest`, `FailureContext`).
- **Zero-Cost Observability**: Never insert `count()`, `show()`, or `collect()` Spark actions into the critical execution path (e.g., inside Probes). Use SparkListener metrics or explicit sampling.
- **Spillway Rules**: Transform channels and custom UDFs must use `try/except` wrappers (or Spark's `try_*` native functions) to catch row-level errors and populate `_aq_error_*` columns without aborting the Spark stage.
- Use `@dataclass(frozen=True)` for all internal representations (`Module`, `Edge`, `Manifest`). Prevents accidental mutation.
- Keep clear boundaries: AST generation → validation → compilation. Each step returns a new immutable object.
- When making changes, only modify the layer relevant to the task. If the LLM edits unrelated files, stop and prompt it to explain why.

## TODOs Memory Rule

The file `~/.claude/projects/-home-sakhund-Personal-Projects-Aqueduct/memory/TODOs.md` is the **single source of truth** for what's next, what's stubbed, and what's deferred.

- **When asked "what's left?" or "what's next?"** — read TODOs.md first, answer from it.
- **After every planning session** — update TODOs.md with agreed phases and decisions.
- **After every phase completion** — mark the phase done in TODOs.md, move completed stubs to archive.
- **When adding a stub** — add it to the Active Stubs section with file + line + acceptance criteria.
- **Never let a stub silently disappear** without being tracked here.

## End-of-Phase Checklist

After completing each implementation phase, always update ALL of:
1. `docs/specs.md` — add/update spec for new feature
2. `README.md` — user-facing docs and examples
3. `.dev/TESTING.md` — add test checklist items for the new feature
4. `aqueduct.template.yml` — add config examples
5. `examples/comprehensive_demo/blueprint.yml` — add usage example

## Development Process Files (`.dev/`)
These files live in the `.dev/` directory and are **shared via Git** (except `JOURNAL.md`).

- **`.dev/TESTING.md`** – Master test checklist. **You (Claude) update this** when adding new features that require tests. The cheap model reads it to generate missing tests.
- **`.dev/ISSUES/`** – Active test tasks (one markdown file per missing test).
- **`.dev/RESOLVED/`** – Archive of completed test tasks.
- **`.dev/JOURNAL.md`** – Personal session log. **Not committed.** Read at session start, update at session end.

## Testing Workflow (Two-Model Split)
- **You (Claude)** write core implementation. When you add a feature that needs testing, add a checklist item to `.dev/TESTING.md` and optionally create an issue file in `.dev/ISSUES/`.
- **A cheaper model** handles test generation. It reads `.dev/TESTING.md` and `.dev/ISSUES/` to produce pytest functions and fill coverage gaps.
- **Never suggest or run test commands yourself.** I handle all test execution separately.
- I run tests locally. I'll only paste specific failures to you if I'm stuck.

### Testing Standards (for Reference)
- Framework: `pytest`, `pytest-cov` (80% minimum), `pre-commit` with `black` and `ruff`.
- Fixtures in `tests/fixtures/`.
- Use `pytest.raises` with `match=` for validation errors.
- Immutability: test `FrozenInstanceError` on dataclass mutation attempts.
- Performance: compare `df.explain()` baselines; use local SparkSession with `spark.sql.adaptive.enabled=false`.

### Spark behavior reference
- Read `.dev/SPARK_GUIDE.md` before modifying Executor modules or implementing new Channel operations.

## Executor Architecture (Extras Pattern)

Aqueduct uses the `aqueduct-core[spark]` extras pattern. The executor is engine‑agnostic at the top level, with Spark‑specific code isolated in a subpackage.

**Structure:**
- `aqueduct/executor/models.py` – engine‑agnostic (`ExecutionResult`, `ModuleResult`)
- `aqueduct/executor/spark/` – all Spark‑specific modules (`ingress`, `egress`, `channel`, `executor`, `junction`, `funnel`, `probe`, `session`, `udf`)
- `aqueduct/executor/__init__.py` – exports `get_executor(manifest, config)` factory

**When adding a new Spark feature:**
- Place the code in `aqueduct/executor/spark/`.
- Update imports in `cli.py` and tests to use the factory or the `spark` subpackage.
- Do **not** import `pyspark` in engine‑agnostic modules (`parser`, `compiler`, `surveyor`, `patch`, `depot`).

**When adding a new LLM provider:**
- Add a `_call_<provider>()` function in `surveyor/llm.py` using `httpx`. No new SDK dependency.
- Wire it in `_call_llm()` dispatch. No extras change needed — `httpx` is already a core dep.

**Testing environment variables:**
- `AQ_SPARK_MASTER` – Spark master URL for tests (default `local[1]`)
- `AQ_LLM_URL` – LLM endpoint for tests (default `http://localhost:11434`); tests requiring LLM are skipped if unreachable.