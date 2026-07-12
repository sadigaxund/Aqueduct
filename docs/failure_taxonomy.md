# Failure Taxonomy: recurring defect classes and their guards

Dev-facing catalog of defect classes that have actually occurred in this repo
(mined from CHANGELOG `### Fixed` entries and session audits). Purpose:

1. **Prevention**: each class carries a guard; prefer a CI-enforced guard
   (test / lint / grep) over an AGENTS.md prose rule.
2. **Cheap-LLM audits**: an audit agent given this file pattern-matches KNOWN
   classes reliably even on small models; novel-bug discovery is not its job.
3. **Living doc**: when a fix lands in CHANGELOG `### Fixed`, ask: is this a
   new class or an instance of one below? Append/update in the same commit-set.

| # | Class | Signature / how to detect | Guard |
|---|-------|---------------------------|-------|
| 1 | **Docstring–implementation divergence** | A function's documented output/contract contradicts its body (e.g. `output.warn` documented `{prefix}⚠ …` but emitted `⚠ {prefix}…`). Detect: read docstring, read body, compare (an ideal cheap-LLM audit task). | Regression test pinning the documented shape (`test_output.py::test_warn_with_prefix_drops_icon`). |
| 2 | **Advice rot in diagnostics** | A warning/error message's *claim or advice* no longer matches the implementation it describes (e.g. `perf_incremental_watermark_scan` described a DAG re-scan after the executor moved to reading written output; `nondeterministic_fanout` advised "upstream" while exempting on self-checkpoint). | Every warning rule gets a test asserting the load-bearing message fragment (`test_warning_rules_audit.py`); rule docstrings must cite the implementation function they describe. |
| 3 | **Dangerous remediation advice** | The suggested fix is worse than the disease for a subset of users (`delivery_append_retry_dupes` suggested `mode=overwrite`, which destroys history on incremental sinks). Detect: for each advice string ask "on which configuration does following this advice lose data?" | Advice review is part of any new warning's PR; test pins the safe advice text. |
| 4 | **Partial-coverage wiring** | A cross-cutting mechanism is wired into one code path but not its siblings (blueprint `warnings.suppress` reached the registry pass but not the 8 inline `_w()` calls; redaction missed raw `print(file=sys.stderr)` sites). Detect: for each mechanism, enumerate ALL emit/consume sites and check each consults it. | Coverage test per mechanism (e.g. `test_blueprint_suppress_covers_inline_rules`); grep sweep when adding a new emit site. |
| 5 | **Stale parallel install shadowing** | Behavior "regresses" because a second, older copy of the package wins `sys.path` (user-site dir beat the editable `.pth`; `.venv` held a non-editable snapshot). Detect: `aqueduct.__file__` not under the repo; old message formats reappearing. | `doctor` candidate: self-check `aqueduct.__file__` vs expected root; dev habit: smoke via `python -c` from repo root only. |
| 6 | **Guard added, sibling paths forgotten** | An exception guard/normalizer handles the reported case but not adjacent inputs of the same shape (freshness guarded `float()` `ValueError` but the Assert dispatch still leaked other exception types until the generic catch was added). | When guarding a case, add the enclosing generic boundary too; test both the specific and a synthetic "other" failure. |
| 7 | **Enum/constant drift** | Bare string literals compared against an enum-backed field across many call sites; one site typos silently (pre-`ExecutionStatus` era: ~90 bare `"success"` comparisons). | Introduce the enum once, forbid new bare literals via review + grep (`rg '"success"\|"skipped"' aqueduct/` should only hit serialization boundaries). |
| 8 | **Normalizer blind spot** | An input-normalization rule-set misses one quoting/encoding variant of the same concept (heal-cache signature collapses `'…'`/`"…"` but not Spark 4's backtick identifiers → duplicate cache entries). | When adding a normalizer rule, enumerate the *family* (all quote chars, all path styles); table-driven tests. |
| 9 | **Doc-example vs schema drift** | Docs/templates/SKILL show a config shape the schema/executor doesn't accept, or miss a required field (test fixtures missing required `name:`; smoke blueprints written in a nonexistent `steps:` channel form). | Compile every doc/template example in CI where feasible (gallery/snippets already are); SKILL.md examples belong to the Change-Trigger Matrix. |
| 10 | **Deterministic-failure-as-warning** | A condition that *always* fails at runtime is only warned about at compile time (`maintenance.optimize` on non-Delta, escalated to CompileError 2026-07-03). Detect: for each warning ask "is there any configuration where the run succeeds anyway?"; if no, it should be an error. | Review question codified here; escalations documented in CHANGELOG `### Changed`. |
| 11 | **Duplicated import-resolution logic, one site fixed at a time** | A "load user code from a dotted path" mechanism gets hand-rolled independently at each of its 5 call sites (secrets resolver, Assert `custom` `fn:`, Probe `custom` `module:`, `udf_registry` `module:`, `format: custom` `class:`) instead of sharing one implementation; each copy accumulates its own subset of fixes (collision-proofing, `base_dir` fallback), so a bug fixed at one site silently persists at the others (the `secrets.py` stdlib-name-collision fix didn't reach Assert's `_load_callable` until a later pass). Detect: grep for `importlib.import_module`/`spec_from_file_location` across `aqueduct/`; every result should call the shared `aqueduct/infra/module_loading.py` helper, not reimplement it. | One shared loader (`aqueduct/infra/module_loading.py`); new "load code by dotted path" needs land there, not inline; `Manifest.base_dir` is now threaded to every site so the resolution rule (base_dir file, else import) is uniform. |

## Conventions this file imposes

- **Warning rules**: the docstring names the implementation site the claim is
  based on; a unit test pins the load-bearing message fragment; version-
  dependent claims (Spark behavior) name the version and the config flag that
  changes them.
- **Audit skills**: point cheap-model audit prompts at this table; report per
  class, cite file:line, and propose the guard from the Guard column.
- **CHANGELOG discipline**: every `### Fixed` entry gets a one-line triage
  against this table (existing class → note it; new class → add a row).
