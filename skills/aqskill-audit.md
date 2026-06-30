---
name: aqskill-audit
description: Full Aqueduct codebase audit — runs all five domain audits (tests, code, config, style, health) in parallel. Use when merging a large feature, before a release, or after a multi-phase change session. Also run individual aqskill-audit-<domain> skills for scoped checks. Reads AGENTS.md for prevention rules.
---

# Aqueduct Full Audit

Runs all five domain audits. Each is self-contained and can be invoked independently.

## Accuracy model — separate recall from precision (read first)

`rg`-driven sub-agents are good at **recall** (finding candidates) and bad at **precision** (judging them). Don't conflate the two:

1. **Detection pass = candidates, not findings.** Sub-agents emit a CANDIDATES list — accept some false positives here; high recall is the goal.
2. **Verification pass = precision.** Every candidate must clear its skill's "Verify before you report" gate: read the `file:line` + context, check the repo neutralizers, write a **concrete failure scenario**, and confirm it's **still true at HEAD**. A candidate with no failure scenario is SUSPECTED → drop or clearly label. This pass is the single biggest accuracy lever — run it as a distinct step, ideally with the strongest model available, before anything reaches the final report.
3. **Cheaper models do step 1; the strongest model (or the user) owns step 2.** Never let a cheap model's raw candidate list become the final verdict.

Common false positives this guards against (all observed): "missing pytest marker" when `conftest.py` auto-marks; "convert this `ValueError`" when it's a pydantic validator; "stale assertion" when a wrapper re-raises the right type or the file is already migrated; "pyspark violation" when the import is lazy/in-function; "stale Phase artifact" in a location where AGENTS.md allows it.

## Domain skills

| Skill | Domain | When to use alone |
|-------|--------|-------------------|
| `aqskill-audit-tests` | Test assertions, structure, blast-radius, coverage | After exception-type changes, config removals, or store-backend changes |
| `aqskill-audit-code` | Exit codes, cross-layer imports, leaks, falsy-traps, CLI drift, redaction | After touching CLI output, error handling, or infra code |
| `aqskill-audit-config` | Schema↔template sync, path anchoring, silent no-ops, stale artifacts | After adding/removing config fields or changing key names |
| `aqskill-audit-style` | Over-broad except, string-in-context, Python 3.11, dispatch, constants | After touching error handling, text parsing, or dispatch logic |
| `aqskill-audit-health` | General code health: secrets, stubs, debt markers, dead code, commented-out code, language-agnostic patterns | Any codebase change — runs first as a baseline |

## Running the full audit

1. **Load** `aqskill-audit-health` first — run the general health scan (secrets, stubs, debt markers, dead code). These apply to every codebase and establish a baseline.
2. **Load** `AGENTS.md` for the 17 Aqueduct-specific prevention rules (do NOT duplicate — reference by rule name).
3. **Invoke** all four Aqueduct-specific domain skills in parallel using sub-agents: `aqskill-audit-tests`, `aqskill-audit-code`, `aqskill-audit-config`, `aqskill-audit-style`. Each returns **candidates**.
4. **Verification pass (mandatory).** Run each skill's "Verify before you report" gate over every candidate — read the code, apply the neutralizers, attach a failure scenario, confirm at HEAD. Drop everything that fails the gate. Do this with the strongest available model. Only verified findings proceed.
5. **Compile** the surviving findings into a single report (every row tagged PROVEN/SUSPECTED with its failure scenario):

```markdown
# Aqueduct Audit — [date]

## Health summary
2–4 sentences. Cross-reference findings across domains — often a test gap +
a code smell + a template mismatch are the same root cause seen from three angles.

## Health audit (aqskill-audit-health)
[insert table — secrets, stubs, debt markers, dead code, commented-out code, resource leaks]

## Test audit (aqskill-audit-tests)
[insert table]

## Code audit (aqskill-audit-code)
[insert table]

## Config audit (aqskill-audit-config)
[insert table]

## Style audit (aqskill-audit-style)
[insert table]

## Top N by severity
[prioritized list — combine findings across domains, deduplicate]
```

Each sub-agent must produce its output in the format specified by its domain skill. If a sub-agent finds nothing, report "(no findings)" — never omit a section.

## Parallel execution

All four domain skills are **independent** — they scan disjoint file sets and can run simultaneously. Use sub-agents to maximize throughput. The `codebase-audit.md` general checks (secrets, stubs, debt markers) can run as a fifth parallel sub-agent or as a pre-pass.

## Scope control

The user may ask for a scoped audit (e.g., "audit just the config changes from this session"). In that case, run only the relevant domain skill(s). The full audit (`aqaudit`) is the default when no scope is given.

## Post-audit

After fixing findings, update AGENTS.md prevention rules if the same class of bug was found in multiple places. The audit skill is the detection layer; AGENTS.md is the prevention layer. Each audit cycle should tighten the feedback loop between them.
