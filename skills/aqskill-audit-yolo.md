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

### Measured calibration (29-report run, triaged 2026-08-03 → 2026-08-07)

Spend budget where it pays. Measured across all 29 unit reports:

- **HIGH/MEDIUM findings held up at a high rate** — better than this skill's own disclaimer implies. Nearly every top-3-by-severity finding per report survived empirical verification. Do not re-litigate them from scratch; verify and act.
- **LOW/SUSPECTED was mostly noise**, and the audit had usually self-labelled it SUSPECTED already. Triage top-down by severity and stop when the yield dies.
- **Only two clear hallucinations** across ~50 verified candidates. The bigger risks turned out to be modes 4 and 5 above (stale and misattributed), not invention.
- The findings that mattered most were **not the ones the reports ranked highest**. Three of the four severe bugs surfaced while investigating an adjacent, lower-ranked item: a secret-redaction filter that protected almost nothing, an exit code that was structurally unreachable, and a deploy path broken end to end. **Follow the thread when a small finding smells structural.**

### Derive, never restate

Any inventory in a prompt, a doc, or this file is a snapshot that rots. Derive every list from
the repo at audit time and show the command that produced it. This is not theoretical: the
2026-08-07 sync audit was handed a known-surface list naming
`aqueduct/executor/capabilities.yml` — a path that does not exist (declarations live per-engine)
— and caught it only because it was required to derive rather than copy. The same rule applies
to counts: never report a number you did not compute.

Common false positives this guards against (all observed): "missing pytest marker" when `conftest.py` auto-marks; "convert this `ValueError`" when it's a pydantic validator; "stale assertion" when a wrapper re-raises the right type or the file is already migrated; "pyspark violation" when the import is lazy/in-function; "stale Phase artifact" in a location where AGENTS.md allows it.

Three hallucination MODES confirmed in the 2026-07-03 pending-audit verification — the verification pass must test each candidate against all three explicitly:

1. **Grep-without-context.** An `rg` hit cited as a violation, but the line is docstring/comment prose, or the cited line ALREADY uses the enum/helper the finding demands, or a keyword match gets an invented usage context. Gate: read ±10 lines and classify the hit as code / docstring / serialization-boundary BEFORE reporting.
2. **Drop-in-equivalence assumption.** "Helper X already exists, replace hand-rolled Y" without checking signature/vocabulary compatibility. Gate: before proposing a replacement, diff the two signatures/key-sets and state the mapping.
3. **Failure-scenario invention contradicted by nearby code.** The failure story assumes a divergence the code already prevents, or the proposed fix breaks documented intent. Gate: the failure scenario must survive the imports and docstrings of the cited file.

Two further modes, both confirmed during the 2026-08-03/07 triage of this skill's own output:

4. **Stale claim — true when written, false at HEAD.** A finding describes a real defect that a later pass already fixed. Two of these reached the triage stage in one batch (`config.py`'s "raw `warnings.warn` bypass sites" were already routed through `emit()`, with a comment saying so). Costs more than a hallucination: the fixer "fixes" working code. Gate: **confirm the defect still exists at HEAD before changing anything** — reproduce it, don't just read the cited line. Reports carry a date; the repo has moved since.
5. **Misattributed location — right defect, wrong file.** The redaction audit reported "the root-logger filter does not scrub tracebacks" against `redaction.py`; the filter actually lives in `cli/__init__.py`. The defect was real and, once traced, far more severe than reported. **This mode is lethal under per-unit decomposition**: a unit agent scoped to `redaction.py` finds nothing at the cited line and drops a true finding as a false positive. Gate: when a candidate's cited line does not contain the thing described, search for the described thing before dropping it — then record it as a cross-unit handoff (below), not as a dropped candidate.

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

The orchestrator owns precision. Every candidate in every returned unit report must survive the gate before the report is persisted: re-read flagged rows, apply neutralizers, confirm at HEAD, drop hallucinated rows (the 5 modes), attach failure scenarios. Unit agents return candidates; the orchestrator is the final verdict. Do this with the strongest model available.

**Apply the rejection ledger here.** Check every surviving candidate against
`skills/audit-adjudicated.md` and drop matches unless the candidate brings NEW evidence (a
reproduction the original adjudication lacked, or a code change since its date). **Detection
agents must never be shown that file** — it would poison their recall and permanently bury any
entry in it that is wrong. Blind re-reports are the signal that tells you whether the ledger is
right and whether the audit is exhausted.

## Cross-unit handoffs — the cost of module isolation

Per-unit scoping is what makes this skill precise, and it is also its one structural blind spot:
**a bug whose symptom and cause live in different units belongs to neither agent.** Measured
instance — the most severe finding of the entire 2026-08 run (a secret-redaction filter attached
to the root logger *object*, so every record propagated from a named logger went unredacted) was
invisible to the `redaction` unit (the filter is not in that file) and off-topic for the `cli`
unit (it reads as a redaction concern). It surfaced only because the redaction unit's
misattributed row was passed to the cli unit as an explicit handoff.

So: a unit agent that finds a defect **outside** its scope must never silently drop it and must
never edit it. It records a `## Cross-unit handoffs` section — `file:line`, what is wrong, and
which unit owns it. The orchestrator routes each handoff into the owning unit's brief as a
**pre-verified item**, not as a fresh candidate. Reports without that section are incomplete.

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

## Triage phase — consuming the reports

Detection produces reports; **triage is a separate pass with its own rules**, and it is where the
value is realised. Run it in batches grouped by FILE OWNERSHIP, not by report count, so no two
batches can edit the same file. Serialize the batches.

Per report, in order:

1. Read it. Verify every finding at HEAD before believing it — all five hallucination modes,
   especially **stale** (mode 4).
2. Fix what survives, top-down by severity. Stop when yield dies.
3. **Append every rejected finding to `skills/audit-adjudicated.md`** — `file:line`, one of its
   six reason labels, the date, and enough of the reasoning that a future pass does not have to
   re-derive it. A rejection that only lives inside a triaged report is a rejection the next
   cycle will re-litigate. `MISATTRIBUTED` is never a drop: re-file it to the owning unit.
4. `git mv` the report to `.dev/AUDITS/triaged/`. Note `.dev/` is gitignored, so this move is
   not committable — that is expected, not a failure.

Non-negotiables for a triage agent, each learned by having it go wrong:

- **Commit after every report.** A host process died mid-run with ~15 files of uncommitted work
  across four packages. Nothing was lost, but only by luck.
- **Stage by explicit path. Never `git add -A`.** The tree routinely carries unrelated in-flight
  changes owned by the user.
- **No destructive git, ever** — no `checkout --`, `reset --hard`, `stash`, `clean`. An agent
  destroyed uncommitted work with `git checkout --` to undo a formatting change.
- **Memory discipline.** Never run the full suite (~3700 tests, spawns Spark JVMs). Scope pytest
  to the directories touched, one at a time; no xdist. Concurrent agents each running full suites
  OOM-killed the host. Prefer `--collect-only` when the question is about selection, not behavior.
- **Run pytest through `rtk proxy`.** A shell hook otherwise rewrites and filters pytest output;
  this produced one wrong conclusion (a ~200-file false positive) before it was noticed. Never
  conclude "absent" from truncated output.
- **Do not verify only with `-m "not spark"`.** Every coordinator check in one session used that
  exclusion, so spark-marked tests never ran locally and CI caught two regressions as a result.
- A test added during triage must assert the claim its docstring makes.
  `tests/test_meta_quality.py::test_no_zero_assertion_tests` rejects a bare call with a
  `# must not raise` comment — and it caught a triage agent doing exactly that.
- A guard added during triage must be **falsifiable**: reintroduce the bug, confirm the guard
  fails and names the offender, then revert. Per AGENTS.md's meta-test rule.

## Post-audit

After fixes, update AGENTS.md prevention rules if a bug class recurs across units — the per-module layout makes cross-module repetition visible. Reports stay in `.dev/AUDITS/pending/` until triaged; move triaged ones to `.dev/AUDITS/triaged/`. The audit skill is the detection layer; AGENTS.md is the prevention layer.

Two companion audits exist alongside the per-unit sweep and answer questions this decomposition
cannot: a **sync-surface audit** (every place two artifacts must agree, whether a guard enforces
it, and whether they agree today) and an **extensibility inventory** (every closed set, its
extension path, and the real cost to add one member). Both are derivation-first and read-only.
They catch drift *between* units, where per-unit isolation is weakest.
