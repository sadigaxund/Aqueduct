---
name: aqskill-audit-design
description: Architecture/design-lens deep audit of Aqueduct — judges the SYSTEM, not defect instances. Adversarial adopter stance (a company choosing its pipeline tool; senior data engineer + data architect + systems designer). Covers product identity and scope creep, blueprint-as-truth violations, silent-data edges, performance claims vs code, healing-loop economics, governance/process tax, contract integrity, and missing-for-production gaps. Expensive explicit-trigger companion to the five-domain sweep — full spec read plus two parallel subagent audits plus synthesis; produces a prioritized design review, not a fix list. Use for release/maturity gates, "is this production ready", adoption decisions, or after adding a subsystem.
---

# Aqueduct Design-Lens Audit

A companion to the five-domain sweep (`aqskill-audit.md`) sharing its accuracy model
(recall vs precision, derive-never-restate, verify-before-report) but answering a
different question. The domain skills detect **violations of known rules**; this skill
judges **whether the design is right**. Its findings are usually "working as coded, wrong
as designed" — features whose existence, cost shape, or failure mode is the finding. Do
not fold it into the default sweep; it fires on explicit trigger.

## Triggers

- Pre-release readiness gate, or "is this production ready?"
- An adoption/evaluation question ("would you build our pipelines on this?")
- After a subsystem lands (new module type, engine, governance framework, approval mode)
- Periodic identity-drift check: has implementation scope walked away from §11's scope statement?

## Reviewer stance (binding, not style)

- Persona: a company evaluating tools to build their pipeline on, staffed by a senior
  data engineer, a data architect, and a systems designer.
- **No affirmations, no salvaging.** Zero weight to sunk development cost, backward
  compatibility, or "it is already implemented." Judge toward the cleanest finished
  build that would actually be useful.
- Every spec/docs claim about cost, security, or behavior is a hypothesis to verify at
  its dispatch site in code — never accept a documented claim as a fact.

## Method — four phases

### Phase 0 — Inputs (read fully, solo)

In order: `docs/specs.md` (ENTIRE file — skim-reading misses the accretion pattern that
is itself a finding), `AGENTS.md`, the `[Unreleased]` tail of `CHANGELOG.md`,
`pyproject.toml` (extras policy, entry points), skim README + SKILL.md. Note the
three-version-space state and every place a contract is stamped stable.

### Phase 1 — Lens pass over the spec

Re-read the spec with these lenses loaded; collect candidates with section cites before
spawning anything:

| Lens | Questions |
|------|-----------|
| L1 Identity/scope | Does implemented scope match the declared scope statement? Which subsystems would a first-week adopter never touch? Which distinct constructs answer the SAME user question (quality gates, variable injection, warning surfaces)? |
| L2 Truth/state separation | Does any path write machine-generated, append-only history into version-controlled source files? Is growth bounded? Who prunes? Does a stated principle ("Blueprint is the single source of truth") survive the repo's own write paths? |
| L3 Silent-data edges | Any warn-and-proceed on stale state (resume/retry past hash mismatches)? Fail-open persistence sequences (material effect written, bookkeeping recorded later, no transaction)? Silent defaults on missing config (empty-string sentinel style)? Shared global names racing under parallel execution? |
| L4 Perf claims vs code | Take each "zero-cost"/"metadata-only" claim to its dispatch site. Do DEFAULT method choices violate it? Are any aggregations outside the danger gate? Are intermediates ever freed? Per-iteration rebuilds (sessions, spills, sweeps)? Store write pattern (connection-per-write)? |
| L5 Economics | Is any second-order feature stacked on a first-order capability whose real-world hit rate is unmeasured? Commit-frequency evidence for speculative generality? Cost per invocation of the flagship loop, decomposed and documented? |
| L6 Governance/process tax | Walk the maintenance playbook's own matrix: how many artifacts does ONE new field/op touch? How many lines of hand-maintained declaration data? Do normative numbers diverge between copies (divergence IS a finding — prove it by deriving the number twice)? |
| L7 Contracts | Exit-code collisions (including third-party defaults like Click's UsageError=2)? Version spaces coherent? Breaking changes shipped inside minors while contracts are stamped stable? Deprecation-window policy stated and followed? |
| L8 Production gaps | Concurrency locking; metrics export (Prometheus/OTel); written threat model incl. prompt-injection posture; idempotency enforcement/linting; effective-config visibility ("what will this run resolve to"); SLA monitoring; actor identity/audit trail; backfill ergonomics. |
| L9 Process artifacts | Corpus size (AGENTS.md + skills/ + docs/) vs what a contributor can hold; normative content duplicated across documents; rules encoded as prose vs enforceable hooks/tests; coverage floor vs safety-criticality of the claims. |

### Phase 2 — Parallel code audits (exactly two subagents)

Split along the trust boundary so scopes are disjoint: **A = execution layer**, **B =
agent/healing + product surface**. Adapt these skeletons; keep the mandatory bits
(read-only; report file path specified up front; `file:line` evidence; UNVERIFIED
marking; severity vocabulary; a top-3/pre-production-fixes closing list).

**Skeleton A — executor/perf:** DuckDB Channel/Funnel materialization lifecycle (are
temp tables dropped? default disk home? pushdown lost?); polyglot orchestration churn
(sessions rebuilt per heal iteration? spills reachable after patches? orphan-sweep cost
at every start on remote roots?); zero-cost observability verified per signal dispatch
site (default methods, ungated aggregations, inter-module caching); checkpoint/resume
hash-mismatch policy; `--parallel` shared-session races; observability-store write
pattern and retention/VACUUM story; watermark transactionality; micro-costs bounded;
session-fingerprint rebuild soundness.

**Skeleton B — agent/process-surface:** machine-written Blueprint content (measure a
real record's size; growth/prune policy; git-diff impact); sandbox-gate cost
decomposition (session build + replay shape + caching absence) and progressive-chain
multiplication; prompt-injection inventory (everything entering prompts, defenses
present AND absent, guardrail default posture); signature-memory normalization
false-positive vectors; surface-area census DERIVED AT RUNTIME (commands/flags/config
fields/warning rule ids/module types/ops — see Census commands); capability-framework
tax (artifacts per new field, walked from the repo's own checklist); exit-code contract
verification; AGENTS.md/skills corpus stats + redundancy spot-checks; gap-table greps
(prometheus/otel, lock/CAS primitives, RBAC/audit-log, schema registry).

**Subagent management protocol:** max two in parallel. Each returns a completion
summary AND writes its own report file — **verify the file exists and is non-empty
before trusting the status**. If a subagent reports success with no file, reply with the
single word "resume" (retry up to 3–4 times). If it stays stuck or empty, discard it and
resend the FULL original prompt as a fresh task. Never let an unverifiable "completed"
stand.

### Phase 3 — Synthesizer spot-check (mandatory)

The subagents ran at recall settings; the synthesizer owns precision (same gate as the
family model). Before citing any BLOCKER/HIGH in the master report, personally verify
its anchor claim at the cited function — minimum: the biggest correctness claim, the
biggest perf claim, and the biggest process claim, one grep/read each.

### Phase 4 — Master synthesis

Write `.dev/AUDITS/pending/<date>-design-review.md`, with sub-reports beside it
(`<date>-design-executor-perf.md`, `<date>-design-agent-surface.md`); grouping the set under a
`design-audit/` subfolder of `pending/` is fine when preferred — keep the date prefix either way
so triage sorting works, and reference sub-reports by filename, never duplicate their tables.

Template:

```
# Aqueduct Design Review — <date>
0. Verdict (adoptable-as-what / blocker count)          ≤5 sentences
1..n. Sections per lens that yielded findings            evidence-cited
Prioritized recommendations:                             P0 correctness/trust,
                                                         P1 perf honesty,
                                                         P2 product focus, P3 polish
Closing judgment                                         honest trajectory read
```

Executive summary ≤12 bullets. Every BLOCKER/HIGH carries `file:line`. Measured beats
estimated beats asserted — quantify record sizes, artifact counts, JVM startups, LIST
round-trips wherever possible. Label each finding **PROVEN** (code-verified defect or
claim/code mismatch) or **JUDGMENT** (design opinion grounded in cited evidence) —
economics and scope findings are usually JUDGMENT; never dress them as PROVEN.

## Census commands — derive at runtime, never trust stored numbers

Show the command next to every number reported (same anti-rot rule as yolo):

```bash
wc -l AGENTS.md SKILL.md docs/*.md skills/*.md       # prose-corpus mass
rg -c '@click.option' aqueduct/cli/*.py              # flag census
python -c "...pydantic walk of AqueductConfig..."    # config-field census (derive, don't copy docs)
git log --oneline --since=<window> -- <feature-path> | wc -l   # speculative-generality probe
```

Cross-check derived numbers against the numbers stated in docs/AGENTS.md — a mismatch
between two normative copies is itself a finding (observed: "~105 config leaves" in
prose vs 132 fields derived at runtime).

## Calibration notes (2026-08-24 run — what paid off)

- Verifying spec admissions against dispatch sites found the highest-value items:
  ungated probe aggregations behind a "zero-cost" brand, never-dropped temp tables,
  warn-and-proceed resume. Docs admissions are leads, not conclusions.
- MEASURING the polluted-record size (~55 lines/1.5 KB) converted an opinion into a
  BLOCKER. Measure artifacts, don't characterize them.
- The two-subagent split had zero overlap noise; resist adding a third.
- Known limit: adoption-gap weighing (L8) depends on the evaluator's deployment context —
  keep those findings labeled JUDGMENT and scoped.
