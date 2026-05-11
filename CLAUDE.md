# CLAUDE.md — Aqueduct Development Guidebook

## Project Context
Aqueduct is a declarative Spark blueprint engine with LLM-driven self-healing.
- **Full spec**: `docs/specs.md` — read it for domain details. This file is process and constraint guidance only.
- **What's next**: `~/.claude/projects/-home-sakhund-Personal-Projects-Aqueduct/memory/TODOs.md`

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
- **5-layer boundary**: `Parser` → `Compiler` → `Planner` → `Executor` → `Surveyor`. Put logic in the correct layer. Only modify the layer relevant to the task.
- **Dual-format contract**: Humans write YAML (`Blueprint`); engine consumes JSON (`Manifest`, `FailureContext`).
- **Zero-cost observability**: Never insert `count()`, `show()`, or `collect()` Spark actions into the critical execution path (e.g. inside Probes). Use SparkListener metrics or explicit sampling.
- **Spillway rules**: Transform channels and UDFs must use `try/except` (or Spark `try_*` functions) to catch row-level errors and populate `_aq_error_*` columns without aborting the Spark stage.
- **Immutability**: `@dataclass(frozen=True)` on all internal representations (`Module`, `Edge`, `Manifest`). Each compilation step returns a new immutable object.

## Executor Architecture (Extras Pattern)
- `aqueduct/executor/models.py` — engine-agnostic (`ExecutionResult`, `ModuleResult`)
- `aqueduct/executor/spark/` — all Spark code (`ingress`, `egress`, `channel`, `executor`, `junction`, `funnel`, `probe`, `session`, `udf`, `assert_`)
- `aqueduct/executor/__init__.py` — `get_executor(manifest, config)` factory

When adding a Spark feature: code in `aqueduct/executor/spark/`. Do not import `pyspark` in `parser`, `compiler`, `surveyor`, `patch`, or `depot`.

When adding an LLM provider: add `_call_<provider>()` in `surveyor/llm.py` using `httpx`. Wire in `_call_llm()` dispatch. No new dep needed.

**Spark behavior reference**: read `.dev/SPARK_GUIDE.md` before modifying Executor modules or implementing new Channel operations.

## TODOs Memory Rule
`~/.claude/projects/-home-sakhund-Personal-Projects-Aqueduct/memory/TODOs.md` is the single source of truth for what's next, what's stubbed, and what's deferred.

- **"What's left?" / "What's next?"** → read TODOs.md first.
- **After every planning session** → update TODOs.md with agreed phases.
- **After every phase completion** → mark phase done in TODOs.md; add entry to CHANGELOG.md.
- **When adding a stub** → add to Active Stubs with file + line + acceptance criteria.

## End-of-Phase Checklist
After completing each implementation phase, update ALL of:
1. `docs/specs.md` — add/update spec for the new feature
2. `README.md` — user-facing docs and examples
3. `tests/TEST_MANIFEST.md` — add test checklist items
4. `aqueduct/templates/default/aqueduct.yml.template` — add engine config examples
5. `aqueduct/templates/default/blueprints/blueprint.yml.template` — add blueprint usage examples

## Testing Workflow (Two-Model Split)
- **You (Claude)** write core implementation. Add checklist items to `tests/TEST_MANIFEST.md` when adding testable features. Optionally create issue files in `.dev/ISSUES/` for the cheaper model.
- **Cheaper model** handles test generation — reads `tests/TEST_MANIFEST.md` and `.dev/ISSUES/`.
- **Never suggest or run test commands.** User handles all test execution. Only paste specific failures if stuck.

**Standards (reference):**
- Framework: `pytest`, `pytest-cov` (80% minimum), `pre-commit` with `black` and `ruff`.
- Fixtures in `tests/fixtures/`. Use `pytest.raises(match=...)` for validation errors.
- Immutability: test `FrozenInstanceError` on dataclass mutation attempts.

**Test env vars:**
- `AQ_SPARK_MASTER` — Spark master URL for tests (default `local[1]`)
- `AQ_OLLAMA_URL` — Ollama URL for LLM tests (default `http://localhost:11434`); tests skip if unreachable
