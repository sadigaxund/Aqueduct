# CLAUDE.md — Aqueduct Development Guidebook

## Project Context
Aqueduct is an intelligent, declarative Spark pipeline engine with LLM-driven self-healing.
- **Full spec**: `docs/specs.md` (20+ pages, read it once, don't restate it).
- **This file**: Practical guidance for working on the codebase.

## Tech Stack (The "Vibe Build" Choices)
- **Language**: Python 3.11+ — you're strongest here, PySpark is first-class, and control-plane performance is irrelevant.
- **Architecture**: Monolithic CLI. Runs on the Spark driver. No servers, no microservices.
- **Key Dependencies**:
  - `pyspark`
  - `pydantic` + `pyyaml` (schema validation)
  - `click` or `typer` (CLI)
  - `duckdb` (embedded observability store — avoids SQLite locks)
  - `sqlglot` (SQL lineage — do NOT write a custom parser)
  - `anthropic` (LLM loop)

## Development Priorities (Vibe Build Order)
Build in this exact sequence to validate assumptions early:
1. **Parser** → Validate Blueprint YAML, resolve static context, output AST.
2. **Compiler** → Resolve Tier 1 runtime functions, expand Arcades, wire Probes/Spillways, output fully resolved Manifest.
3. **Ingress/Egress Wrapper** → Prove Spark I/O works via YAML config.
4. **Single SQL Channel** → Register upstream as temp view, run query.
5. **Mock Surveyor** → On failure, just log/webhook; no LLM yet.
6. **Patch Grammar (Manual)** → `aqueduct patch apply` works from CLI.
7. **LLM Integration** → Final step.

## Code Organization & Safety
- **Layered Architecture**: Respect the 5 core layer boundaries (`Parser` -> `Compiler` -> `Planner` -> `Executor` -> `Surveyor`). Put logic in the correct layer and module.
- **Dual-Format Contract**: Humans and CLI tools write YAML (`Blueprint`), but the engine and runtime LLM agents exclusively consume JSON (`Manifest`, `FailureContext`).
- **Zero-Cost Observability**: Never insert `count()`, `show()`, or `collect()` Spark actions into the critical execution path (e.g., inside Probes). Use SparkListener metrics or explicit sampling.
- **Spillway Rules**: Transform channels and custom UDFs must use `try/except` wrappers (or Spark's `try_*` native functions) to catch row-level errors and populate `_aq_error_*` columns without aborting the Spark stage.
- Use `@dataclass(frozen=True)` for all internal representations (`Module`, `Edge`, `Manifest`). Prevents accidental mutation.
- Keep clear boundaries: AST generation → validation → compilation. Each step returns a new immutable object.
- When making changes, only modify the layer relevant to the task. If the LLM edits unrelated files, stop and prompt it to explain why.

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