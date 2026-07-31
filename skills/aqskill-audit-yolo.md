---
name: aqskill-audit-yolo
description: Granular per-module Aqueduct audit — one isolated sub-agent per aqueduct/ package and per top-level file, each enforcing the full AGENTS.md ruleset inside its own scope, with per-module reports written to .dev/AUDITS/pending/. Use for release pre-flight, after multi-phase sessions, or when the domain-wide pass produces too many cross-module false positives. Read-only: no source edits, reports only.
---

# Aqueduct Granular (per-module) Audit

A variation of `aqskill-audit.md` for module-isolated granularity. Same accuracy model, same precision gate, same AGENTS.md contract — different decomposition: **one sub-agent per module** instead of one per domain. Each module agent runs every applicable domain check inside its own scope, so the domain skills become per-module checklists rather than codebase-wide passes.

## Accuracy model — separate recall from precision (read first)

`rg`-driven sub-agents are good at **recall** (finding candidates) and bad at **precision** (judging them). Don't conflate the two:

1. **Detection pass = candidates, not findings.** Sub-agents emit a CANDIDATES list — accept some false positives here; high recall is the goal.
2. **Verification pass = precision.** Every candidate must clear its skill's "Verify before you report" gate: read the `file:line` + context, check the repo neutralizers, write a **concrete failure scenario**, and confirm it's **still true at HEAD**. A candidate with no failure scenario is SUSPECTED → drop or clearly label. This pass is the single biggest accuracy lever — run it as a distinct step, ideally with the strongest model available, before anything reaches the final report.
3. **Cheaper models do step 1; the strongest model (or the user) owns step 2.** Never let a cheap model's raw candidate list become the final verdict.

Common false positives this guards against (all observed): "missing pytest marker" when `conftest.py` auto-marks; "convert this `ValueError`" when it's a pydantic validator; "stale assertion" when a wrapper re-raises the right type or the file is already migrated; "pyspark violation" when the import is lazy/in-function; "stale Phase artifact" in a location where AGENTS.md allows it.

Three hallucination MODES confirmed in the 2026-07-03 pending-audit verification — the verification pass must test each candidate against all three explicitly:

1. **Grep-without-context.** An `rg` hit cited as a violation, but the line is docstring/comment prose, or the cited line ALREADY uses the enum/helper the finding demands, or a keyword match gets an invented usage context. Gate: read ±10 lines and classify the hit as code / docstring / serialization-boundary BEFORE reporting.
2. **Drop-in-equivalence assumption.** "Helper X already exists, replace hand-rolled Y" without checking signature/vocabulary compatibility. Gate: before proposing a replacement, diff the two signatures/key-sets and state the mapping.
3. **Failure-scenario invention contradicted by nearby code.** The failure story assumes a divergence the code already prevents, or the proposed fix breaks documented intent. Gate: the failure scenario must survive the imports and docstrings of the cited file.

## Domain skills — the checklist sources

| Skill | Domain | Adjusted for yolo mode as… |
|-------|--------|-----------------------------|
| `aqskill-audit-health` | Secrets, stubs, debt markers, dead code, commented-out code, resource leaks | Baseline scan inside **every** unit agent. |
| `aqskill-audit-code` | Exit codes, cross-layer imports, leaks, falsy-traps, CLI drift, redaction | Runs inside every unit agent; the checks apply to the unit's own surface (see emphasis table for which checks matter where). |
| `aqskill-audit-config` | Schema↔template sync, path anchoring, silent no-ops, stale artifacts | Runs in units that touch schema/config/template surfaces (parser, config, templates, cli scaffolding, executor path_keys consumers). Other units only get the silent-no-ops + stale-artifact checks. |
| `aqskill-audit-style` | Over-broad except, string-in-context, Python 3.11, dispatch, constants | Runs inside every unit agent. |
| `aqskill-audit-tests` | Test assertions, structure, blast-radius, coverage | **Not a separate agent in yolo mode.** Adjusted to **test-adjacency per unit**: each unit agent checks its counterpart `tests/test_<unit>/` directory (and module-named test files) for coverage gaps that let its findings ship, assertions on its error types/sentinels, and marker placement for unit-owned tests. Cross-cutting test structural audits (conftest fixtures, meta-tests, CI-workflow guards) stay a single follow-up pass owned by the orchestrator if the user asks for them. |

Each unit agent reads the applicable domain skill files under `skills/` and AGENTS.md itself (reference prevention rules by name, never re-derive them from memory).

## Decomposition model — module-first, domain-inside

Spawn one sub-agent per audit unit. The inventory is **derived at audit time**, never hand-maintained:

```bash
for d in aqueduct/*/; do [ -f "$d/__init__.py" ] && echo "PKG $d"; done
ls aqueduct/*.py
```

1. **Every Python package** under `aqueduct/` (dir with `__init__.py`).
2. **Every top-level Python file** in `aqueduct/`. `__init__.py` is a package marker — fold into the top-level-file batch (trivial, one agent) or skip.

**Run the commands; do not trust a list.** Any inventory written down here is a snapshot that rots — a package added after this file was last edited would silently never be audited, and one removed would be spawned as an empty unit. (Live example: `tui/` is slated for deletion, so it will disappear from the real inventory on its own.) This is the same hand-maintained-list drift the AGENTS.md prevention rules exist to stop, so the audit tooling must not reintroduce it. Treat the emphasis table below as a lookup keyed by unit name — units absent from it get the universal sweep only, which is the correct default for a package nobody has written a row for yet.

New package → new unit. The executor unit includes `spark/` and `duckdb_/` plus the engine-agnostic top-level files (models.py, path_keys.py, probe_plugins.py, channel_ops.py, capabilities.py, capability_leaves.py, config_leaves.py, capability_tooling.py, protocol.py, spill.py, orchestrator.py, handoff/).

### Unit → skill emphasis (embed in the agent prompt so budget isn't wasted)

| Unit | Extra emphasis beyond the universal sweep |
|------|-------------------------------------------|
| `parser` | Schema↔template sync, FsPath anchoring, `extra="forbid"`, pydantic aliases traced to their executor consumers (the UDF `class`/`class_name` family), populate_by_name semantics |
| `executor` | pyspark discipline (engine-agnostic top-level + `duckdb_/` must be pyspark-free; `spark/` may import), capability framework (leaf walkers vs engine declarations, the three error types never conflated), spillway row-level rules, zero-cost observability |
| `cli` | `exit_codes.*` contract, style.py/output.py vocabulary, help text vs schema/overrides routing, redaction of emitted output |
| `config` | `engine_scoped` tag on EVERY field (missing tag → `CapabilityScopeError`), template sync per field, silent no-ops, zero-consumer classes |
| `stores` | Resource leaks (duckdb.connect / psycopg2), raw driver exceptions reaching users, canonical path resolution (`read.py`) |
| `agent` | Bare ValueError/RuntimeError → AqueductError subclass, recovery-path regexes (sanctioned only after strict parse), provider credential checks, prompt-rule placement (scaffold vs engine packs), PROMPT_VERSION policy |
| `surveyor` | Lazy pyspark only in the documented sites, DDL constants in ddl.py, webhook/openlineage redaction, zero-cost observability |
| `doctor` | The 4 lazy-pyspark sites only, error taxonomy branching by TYPE, DSN redaction |
| `compiler` | 4-layer import direction, island classification exclude-lists, capability gate rule_ids + suppression, Manifest immutability |
| `patch` | Patch-op closure, YAML round-trip fidelity, except justifications |
| `infra` | Single-source rules (http.py outbound mechanics, module_loading.py user-code imports) — no hand-rolled duplicates |
| `templates` | Sync with config.py/schema.py, stale text, no Phase artifacts |
| `tools` / `mcp` / `tui` / `dashboard` | Read-only surfaces, redaction chokepoint (call_tool), extra-import discipline (mcp SDK / textual / pyspark lazy only) |
| `deploy` / `drift` / `dev` / `depot` | Full AGENTS.md sweep; dev/ TODO stubs in scaffolds are intentional |
| `cli` + `diagnostics` + `templates` | The `aqueduct_config` "1.0" vs "2.0" version-stamp family, `--set` routing help text |
| `config` + `parser` + `templates` | Schema↔template sync is a three-way check: field → template comment block → docs/specs.md |
| Top-level files | Per-file contracts: errors.py taxonomy, exit_codes.py classification, redaction.py sink list, overrides.py routing, warnings.py rule_ids, secrets.py resolver, utils.py purity, lint.py ruleset, models.py re-export purity, typehub.py type system |

## Prompt template for unit agents

Every unit agent prompt contains, verbatim-shaped:

- **STRICT READ-ONLY**: no create/edit/delete of any file; the final message is the report text.
- **Scope**: audit ONLY `<unit path>` (plus its `tests/test_<unit>/` counterpart for the test-adjacency section). Do not audit other modules.
- **Read first**: `AGENTS.md` (the prevention rules are the contract; documented exceptions neutralize candidates) + the applicable domain skill files from the table above.
- **The precision gate** (verbatim from this skill's Accuracy model section, including the three hallucination modes).
- **The unit's emphasis rows** from the table above.
- **The report template** below.

## Verification pass (mandatory, unchanged)

The orchestrator owns precision. Every candidate in every returned unit report must survive the gate before the report is persisted: re-read flagged rows, apply neutralizers, confirm at HEAD, drop hallucinated rows (the 3 modes), attach failure scenarios. Unit agents return candidates; the orchestrator is the final verdict. Do this with the strongest model available.

## Report artifacts — one file per unit

Write each unit's audit to `.dev/AUDITS/pending/` with the shared date prefix, the `audit-yolo` tag, and the unit suffix:

```
.dev/AUDITS/pending/2026-07-31-audit-yolo-agent.md
.dev/AUDITS/pending/2026-07-31-audit-yolo-config.md
.dev/AUDITS/pending/2026-07-31-audit-yolo-utils.md
```

Suffix = package dir name or top-level file stem (no `.py`). Existing full-audit files in the directory stay untouched. Verify at the end that nothing outside `.dev/AUDITS/pending/` was written (the orchestrator is the only writer).

Per-unit report template:

```markdown
# <unit> — audit (2026-07-31)

## Summary
2–4 sentences: overall state, counts by severity, top issue.

## Health (aqskill-audit-health)
| file:line | issue | verdict | failure scenario |

## Code (aqskill-audit-code)
### Exit-code violations
### Cross-layer imports   (file:line | import | direction | justification/verdict)
### Resource leaks
### Falsy-traps
### CLI vocabulary drift
### Redaction bypasses

## Config (aqskill-audit-config)   — only if the unit touches schema/config/template surfaces
Schema↔template sync / path anchoring / silent no-ops / stale artifacts

## Style (aqskill-audit-style)
Over-broad except / string-in-context / 3.11 syntax / dispatch / constants

## Test adjacency (aqskill-audit-tests)
Coverage gaps that let the above findings ship; error-type assertions; marker placement

## AGENTS.md rule violations
| file:line | rule | verdict | failure scenario |

## Verified clean / neutralized
Bullets of dropped candidates and why.

## Top findings by severity
Numbered, each with a fix hint.
```

Every row carries verdict (PROVEN/SUSPECTED) + a concrete failure scenario. "(no findings)" for empty sections — never omit a section.

## Parallel execution & batching

All unit agents are independent — spawn them in parallel batches of 8–10 (total N = package count + top-level file count). Persist each batch's reports to `.dev/AUDITS/pending/` immediately after it returns, then launch the next batch. Do not mix units from different batches in one file.

## Scope control

The user may scope by unit ("audit just cli/ and config.py"), by batch ("only the top-level files"), or by domain inside a unit ("audit executor's pyspark discipline"). Default = full inventory.

## Post-audit

After fixes, update AGENTS.md prevention rules if a bug class recurs across units — the per-module layout makes cross-module repetition visible. Reports stay in `.dev/AUDITS/pending/` until triaged; move triaged ones to `.dev/AUDITS/triaged/`. The audit skill is the detection layer; AGENTS.md is the prevention layer.
