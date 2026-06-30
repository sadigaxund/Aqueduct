---
name: aqskill-audit-health
description: General code-health audit — secrets, stubs, debt markers, dead code, commented-out code, hardcoded values, resource leaks, and language-agnostic patterns. Runs as a baseline before domain-specific audits. Extends the prevention rules in AGENTS.md with detection commands.
---

# Aqueduct Code-Health Audit

General-purpose detection patterns — secrets, stubs, debt markers, dead code, hardcoded values, resource leaks, and language-agnostic patterns. Runs first as a baseline; Aqueduct-specific patterns are covered by the domain skills (`aqskill-audit-tests`, `aqskill-audit-code`, `aqskill-audit-config`, `aqskill-audit-style`). See `aqskill-audit.md` for the full workflow.

You already know clean-code principles. The hard, high-value work an audit needs from you is three things you do *not* do reliably on your own:

1. **Reason top-down from intent.** Form an expectation of how the system *should* behave before judging the code, so you catch what's *missing*, not just what's present-and-ugly.
2. **Exercise real engineering judgment.** Tell defensible repetition from genuine debt; recognize when an abstraction would cost more than it saves; spot the specific ways agent-written code goes wrong. Mechanical rule-application ("DRY violation → extract helper") is often *wrong* and destroys credibility.
3. **Prove every finding.** Trace it, cite `file:line`, and never assert a bug or a "missing feature" you haven't confirmed against the actual code. Specifically: (a) attach a **concrete failure scenario** (input/state → wrong result) — no scenario means it's SUSPECTED, not a finding; (b) confirm it's **still true at HEAD** by re-reading current content, never from a remembered or expected state; (c) before flagging, apply the **repo neutralizers** in the relevant domain skill (`aqskill-audit-tests`/`-code`/`-config`/`-style`) — a mechanism that already makes the candidate correct (conftest auto-marking, pydantic-validator exemption, wrapper re-raise, lazy pyspark, AGENTS-allowed Phase artifacts) means **drop it**, don't downgrade to "minor."

## The core idea: audit against intent, not against dogma

A linter judges code against rules. This audit judges code against **what the system is supposed to do**. That reframing is the whole point — it's what surfaces the forgotten component, the half-wired feature, the comment that promises an invariant the code doesn't keep. You cannot find a *missing* thing by reading what's there; you find it by knowing what *should* be there and noticing the gap.

So the work is always: **build the expectation → reverse-engineer the reality → diff them → judge the gaps and the choices.**

---

## Workflow

### Phase 0 — Reconstruct intended behavior (do this first, before opinions form)

Build an explicit model of what the system is supposed to do. Forming this *before* reading implementation deeply is what keeps you from rationalizing whatever the code happens to do. Sources, roughly in order of trustworthiness about *intent*:

- **README, docs, design notes, ADRs, AGENTS.md, changelogs** — stated capabilities, architecture rules, and rationale.
- **Tests** — the most honest spec of intended behavior; they encode what someone expected to be true. Note what's tested *and what conspicuously isn't*.
- **Public surface** — CLI `--help`, API routes, exported functions, config schema, type signatures. These advertise capabilities.
- **Issue tracker / TODOs / commit messages** (if available) — promised or in-flight work.
- **The user.** Ask them directly what the system is meant to do and what they were unsure about. They know the intent better than any doc.

Write down, concretely, the **expected capabilities and invariants**: "should validate input before persisting", "patch apply and patch check must enforce identical guardrails", "supports resume after crash". This list is what you audit against. Flag anything documented-but-vague here — ambiguity in the spec is itself a finding.

### Phase 1 — Reverse-engineer what the code actually does

Map the expected capabilities to the code that implements them. Trace the real control/data flow for each major feature from entry point to effect. Run the detection patterns below to surface stubs, debt markers, hardcoded values, swallowed errors, and dead code that the read-through would miss. Build a feature → implementation map, and note where the trail goes cold.

### Phase 2 — Diff intent against reality (the heart of the audit)

For each expected capability and invariant, ask:

- **Missing or forgotten.** Documented/implied but absent. No code path realizes it.
- **Half-implemented.** Present in shape but not behavior: a stub that returns plausible mock/empty data, a function wired into the call graph but never finished, a flag that's read but does nothing, a happy path with the error/edge cases skipped.
- **Drifted.** The code does something *subtly different* from what its docs, comments, or tests claim.
- **Broken/unmaintained invariant.** A promise asserted somewhere ("always validated before save", "these two stay in sync") that nothing structurally guarantees — so it can silently break as the code evolves. These are the highest-value latent bugs.
- **Orphaned / scope creep.** Code doing things nothing asked for — leftover experiments, speculative generality, features no longer reachable.

### Phase 3 — Judge the design choices (with judgment, not rules)

For the notable decisions, don't pattern-match — ask **why is it this way, and is that defensible given the intent and constraints?** Use the [Design Judgment](#design-judgment) section below.

### Phase 4 — Verify and calibrate

Turn candidates into findings. Confirm each by re-reading the code — discard false positives ruthlessly; a short trustworthy report beats a long noisy one. Assign **severity** and **confidence** separately (rubric below). Cap low-value nits and report patterns once rather than per-occurrence.

### Phase 5 — Report

Use the [Report Format](#report-format) below. Lead with the intent-vs-implementation picture, because that's the headline the user cares about.

---

## Severity and Confidence

Tag every finding with one of each. They are independent axes — a high-severity issue you couldn't fully trace is *High / Speculative*, not a confident alarm.

- **Severity** — *Critical* (security, data loss, core feature broken in normal use) · *High* (real bug/risk likely to bite, or missing core capability) · *Medium* (friction, fragility, drift not yet breaking) · *Low* (polish).
- **Confidence** — *Confirmed* (traced; certainly true) · *Likely* (strong evidence, not fully verifiable) · *Speculative* (a lead for the user to check — say so plainly).

---

## Report Format

ALWAYS use this structure so audits are comparable across time.

```markdown
# Code Audit — [project] — [date]

## Health summary
2–4 sentences: overall state, the single most important thing to address,
and whether the codebase is in good / fair / concerning shape.

## Intended behavior (reconstructed)
The capabilities and invariants this system is meant to uphold, and the sources
they came from. Flag anything that was ambiguous or undocumented.

## Intent vs. implementation
The core of the audit. For each significant gap:
- **[Capability/invariant]** — `file:line` — *(Severity / Confidence)*
  - **Expected:** what should happen, per [source].
  - **Actual:** what the code does (or doesn't).
  - **Why it matters / Fix:** consequence + recommended change.
Group: Missing · Half-implemented · Drifted · Broken invariant · Orphaned.

## Design-choice findings
Doubtful decisions, with the *reasoning* for why they're doubtful — and explicit
notes where a tempting refactor would NOT be an improvement (so the user trusts
the ones that are). Distinguish real debt from defensible repetition.

## Other findings
Bugs, hardcoded values, swallowed errors, dead code, etc. (severity-ordered).
Low/polish items summarized as patterns, not enumerated.

## Recommendations (prioritized)
Ordered action plan: what to fix first, second, third.
```

Save the report as a Markdown file the user can keep and diff against the next audit, and present it.

---

## Detection Patterns — Reconnaissance Cookbook

Concrete searches for the Phase 2 sweep. Treat every hit as a **candidate**, not a finding — verify in Phase 4 before reporting.

Commands use `ripgrep` (`rg`), which respects `.gitignore` and is fast. If `rg` is unavailable, the `grep -rn --include` equivalents work too. Always exclude vendored/build dirs: add `-g '!{node_modules,dist,build,vendor,.git,__pycache__,venv,.venv}'` to `rg`, or the equivalent `--exclude-dir` to `grep`.

### 1. Debt markers and temporary code
The fastest way to find what the author flagged as unfinished.
```bash
rg -ni --stats '\b(TODO|FIXME|HACK|XXX|BUG|WIP|REFACTOR|OPTIMIZE|DEPRECATED)\b'
rg -ni '(temporary|temp hack|for now|remove (this|later)|don.t ship|placeholder|quick fix|workaround|kludge)'
```
The `--stats` count is a useful debt metric to report and to track across audits.

### 2. Stubs and unfinished work
Functions that were scaffolded and never filled in, or that return fake data.
```bash
rg -n 'NotImplementedError|not[ _]?implemented|raise NotImplemented|TODO.*implement'
rg -n 'return (None|null|nil|\[\]|\{\}|0|false|""|mock|dummy|fake|sample)'
rg -n -U 'def \w+\([^)]*\):\s*\n\s*(pass|\.\.\.)\s*$'
```

### 3. Hardcoded secrets and credentials
**Highest priority.** Any real hit is Critical. Verify it is a real secret and not a placeholder/example before alarming.
```bash
rg -ni '(api[_-]?key|secret|password|passwd|token|access[_-]?key|private[_-]?key|client[_-]?secret)\s*[:=]\s*["'\''][^"'\''\s]{6,}'
rg -n 'AKIA[0-9A-Z]{16}'
rg -n 'sk-[A-Za-z0-9]{20,}'
rg -n 'ghp_[A-Za-z0-9]{36}'
rg -n '-----BEGIN [A-Z ]*PRIVATE KEY-----'
rg -n '://[^:@/\s]+:[^@/\s]+@'
```
Also check whether secrets live in committed `.env`, config, or fixture files: `rg --files | rg -i '\.env|secret|credential'`.

### 4. Hardcoded URLs, paths, and environment values
Values that should usually come from config/env vars.
```bash
rg -n 'https?://(localhost|127\.0\.0\.1|[0-9]{1,3}(\.[0-9]{1,3}){3}|[a-z0-9.-]+\.(com|net|io|internal|local))'
rg -n '/(Users|home)/[a-z]+/'
rg -n '[A-Z]:\\\\'
rg -n ':[0-9]{2,5}\b'
```

Also search for duplicated path/filename strings across modules — these should be single-sourced:
```bash
# Duplicated directory path prefixes (like ".aqueduct" appearing 50+ times)
rg -n '"\.(aqueduct|patch|blob|benchmark|depot|snapshot|watermark|lineage)"' --glob '!test_*' .
# Default filenames duplicated across modules
rg -n '"(observability\.db|depot\.db|benchmark\.duckdb|aqueduct\.yml)"' --glob '!test_*' .
```

### 5. Swallowed errors
Errors caught and ignored hide real failures.
```bash
# Python: bare/blanket except, or except that only passes
rg -n -U 'except[^\n]*:\s*\n\s*pass'
rg -n 'except\s*:'
# JS/TS/Java/C#: empty catch blocks
rg -n -U 'catch\s*\([^)]*\)\s*\{\s*\}'
# Go: explicitly discarded errors
rg -n '_\s*[:=]\s*[^\n]*\berr\b|_ = '
```
Also search for over-broad `except Exception: return None` — these swallow OOM/KeyboardInterrupt alongside expected errors:
```bash
rg -n -U 'except\s+(Exception|BaseException)?\s*:\s*\n\s*(pass|return\s+None)' --glob '!test_*' .
```

### 6. Debug residue
Statements meant to be removed before shipping.
```bash
rg -n '\bconsole\.(log|debug|trace)\('
rg -n '\bprint\(|pprint\(|breakpoint\(\)'
rg -n 'fmt\.Print(ln|f)?\('
```

### 7. Commented-out code
Dead code parked in comments — should be deleted (git remembers it).
```bash
rg -n '^\s*(//|#)\s*(if|for|while|return|def |function |class |import |const |let |var )'
```

### 8. Dead and unused code
Prefer language tooling (see §10). Quick grep heuristic:
```bash
rg -n 'def my_func|function myFunc|const myFunc'   # the definition
rg -c '\bmyFunc\b'                                 # total occurrences; 1 = defined but unused
```

### 9. Resource leaks
Resources acquired without a guaranteed release.
```bash
rg -n '=\s*open\(' | rg -v 'with '
rg -n '\.acquire\(\)|\.lock\(\)|\.connect\(\)|\.open\('
```

### 10. Language-specific analyzers
Prefer the ecosystem's own analyzers; they have far fewer false positives than grep.

- **Python**: `ruff check`, `pyflakes`, `bandit`, `vulture`, `mypy`.
- **JavaScript/TypeScript**: `eslint` (with `no-unused-vars`, `no-console`), `tsc --noEmit`, `depcheck`, `knip`.
- **Go**: `go vet`, `staticcheck`, `golangci-lint`, `ineffassign`, `errcheck`.
- **Rust**: `cargo clippy`, `cargo +nightly udeps`.
- **Java/Kotlin**: `SpotBugs`, `PMD`, `detekt`.
- **Any**: `semgrep` for security/anti-patterns; `gitleaks`/`trufflehog` for secrets.

### 11. Cross-layer import violations

Architecture rules are easy to write and hard to enforce. Check them.

```bash
# Layer boundary: forbidden dependencies must stay confined
rg -n 'import pyspark|from pyspark' --glob '!{executor/spark/**,test*/**,doctor/**}' .

# Lower layer importing from higher layer (e.g. parser importing from cli)
rg -n 'from <PROJECT>\.(cli|agent|patch)' --glob '{parser/**,compiler/**}' .

# Executor importing from surveyor/agent (upstream)
rg -n 'from <PROJECT>\.(surveyor|agent|patch)' --glob '{executor/**}' .

# General: find every cross-package import and trace direction
rg -n 'from <PROJECT>\.\w+ import' --glob '!test_*' .
```

Track all cross-layer imports in a table: file, import, direction, justification. Lazy imports (inside function bodies) mitigate startup coupling but still create a hard runtime dependency. For every one, ask: could this be moved to the correct layer, or inverted (the lower layer defines an interface/event that the higher layer subscribes to)?

Also check for bypasses of the plugin/backend abstraction:
```bash
# Direct DB connections that should go through the store abstraction
rg -n 'duckdb\.connect' --glob '!{*duckdb*.py,*read.py,test_*}' .

# Direct YAML load/dump that bypasses the parser layer
rg -n 'yaml\.(load|dump|safe_load|safe_dump)' --glob '!{test_*,parser/**,patch/**}' .
```

### 12. Config plumbing

Check whether config values declared in the schema actually reach the code that's supposed to use them.

```bash
# Find config pydantic fields — then manually grep consumers
rg -n '\.(\w+):' aqueduct/config.py | rg 'str \| None|int \| None|bool'
```

Check these patterns manually:

- **Dead config**: a pydantic field with a docstring that no code reads. Users who set it think they're tuning behavior; it silently does nothing.
- **Default duplication**: default values repeated in config.py field default AND CLI `@click.option(default=…)` AND somewhere else.
- **Override chain gaps**: a setting is in the engine config but has no blueprint-level counterpart, so per-blueprint overrides silently fail.
- **Raw dict access bypassing typed model**: `cfg_dict.get("key")` instead of `cfg.typed_field` — bypasses pydantic defaults and validation.
- **Falsy-trap**: `if not config_value` instead of `if config_value is None` on optional fields — catches `0`, `""`, `[]`, and empty dicts as "not set" when they're legitimate values.

### 13. String literals as identifiers ("project vocabulary")

Strings used as comparisons, dict keys, or field names across multiple files should be constants or enums. Otherwise a rename or typo silently breaks dispatching.

```bash
# Find pydantic Literal/enum definitions (the schema defines the vocabulary)
rg -n "Literal\[|StrEnum" aqueduct/parser/schema.py aqueduct/config.py

# Find bare-string comparisons of those same values elsewhere (callers not importing the enum)
rg -n '"auto"|"disabled"|"human"|"aggressive"|"ci"|"pass"|"fail"' --glob '!{test_*,parser/schema.py}' .

# Find status/state vocabulary used across many modules — rank by frequency
rg -n '"success"|"error"|"skipped"|"pending"|"applied"|"rejected"' --glob '!test_*' . | awk -F: '{print $2}' | sort | uniq -c | sort -rn

# Find common column-name prefixes that should join an existing constants module
rg -n '_aq_' --glob '{executor/**,compiler/**}' | rg -v 'test_|error_columns'

# Find the same constant defined in two places
rg -n '= "aqueduct\.yml"' --glob '!test_*' .
```

The severity of a finding scales with: number of files touched if the string changes, distance between comparison sites, and whether the string is also a pydantic `Literal` or `StrEnum` (which means the schema already defines the vocabulary — callers just aren't importing it).

### 14. DDL and schema uniqueness

Ensure each database table is created exactly once and all migrations are traceable.

```bash
# Find every CREATE TABLE
rg -n -i 'create\s+table\s+(if\s+not\s+exists\s+)?(\w+)' --glob '!test_*' .

# Check for duplicate table definitions (same table name in two files)
rg -n -i 'create\s+table\s+(if\s+not\s+exists\s+)?([a-z_]+)' --glob '!test_*' . \
  | sed 's/.*create table[^a-z]*\([a-z_]*\).*/\1/' | sort | uniq -d

# Check for ALTER TABLE migrations — are they idempotent?
rg -n 'ALTER TABLE' --glob '!test_*' .

# Check for DDL in unexpected layers
rg -n '_DDL\s*=\"?CREATE|CREATE TABLE.*DDL|DDL.*CREATE' --glob '!{surveyor/**,stores/**}' .

# Duplicate class-level DDL attributes (silent overwrite)
rg -n '_DDL\s*=' --glob '!test_*' .
```

### 15. Module/handler dispatch patterns

Check how a fixed set of types is dispatched — dict-based dispatch is preferred (adding a type is one line). Long if-elif chains are defensible when each branch does substantially different work and the set is stable. The test: when a new type is added, how many files need changes?

```bash
# Find if-elif dispatch chains
rg -U -n 'if .*\.type\s*==.*:[\s\S]{0,200}elif .*\.type\s*==.*:' --glob '!test_*' .

# Find dict-based dispatch (preferred — adding a type is one line)
rg -n '_DISPATCH\s*=|_HANDLERS\s*=|_REGISTRY\s*=' --glob '!test_*' .

# Find enumerated sets of types — are they different semantic subsets or redundant copies?
rg -n '(INGRESS|CHANNEL|EGRESS|JUNCTION|FUNNEL|PROBE|ASSERT|UDF|ARCADE)' \
  --glob '!test_*' . | rg 'frozenset|set\(|list\['

# Find string comparisons that should use enum values
rg -n '== "(Ingress|Channel|Egress|Junction|Funnel|Probe|Assert|UDF|Arcade)"' --glob '!test_*' .
```

### 16. Error handling consistency

```bash
# bare/over-broad except — distinguish documented best-effort from risky silence
rg -n -U 'except\s+(Exception|BaseException)?\s*:\s*\n\s*(pass|return\s+None)' --glob '!test_*' .

# sys.exit() with bare ints — should use named ExitCode constants
rg -n 'sys\.exit\(\d+\)' --glob '!test_*' . | rg -v 'sys\.exit\(130\)'

# Inconsistent logging: print() / click.echo() mixing with logger module
rg -n 'print\(.*file=sys\.stderr' --glob '!test_*' .
rg -n 'click\.echo.*err=True' --glob '!test_*' . | head -30

# Error message consistency — spot-check capitalization and punctuation
rg -n 'ValueError\(|raise\s+ValueError\(' --glob '!test_*' . | head -40
```

### 17. Missing proof-of-life (non-trivial logic with zero test coverage)

Complex code — branches, loops, parsers, money/security paths — should leave at least one runnable check behind. Identify non-trivial untested logic:

```bash
# Find complex functions (high cyclomatic complexity candidates)
rg -n -U 'def \w+\([^)]*\):[^}]{300,}' --glob '!test_*' .

# Find modules with no corresponding test file
# (requires filesystem: for each source module, check if test_<module>.py exists)

# Parse/validation functions should have at least one sentinel test
rg -n 'def parse_|def validate_|def decode_|def encode_' --glob '!test_*' .
```

For each hit: check whether a test exists that exercises the non-trivial path. A return-type stub that always returns `True` or `None` is not proof-of-life — the test must actually fail if the logic breaks.

---

## Design Judgment

### Duplication: shape vs. logic

DRY is about not duplicating **knowledge** (a business rule, a calculation, a contract), not about never repeating **syntax**. The test is **what must change together**:

- **Real debt — duplicated logic.** The same rule/decision lives in N places, and a change to it must be made in all N or they drift out of sync. A fix applied to one copy but not the others is the classic resulting bug. *This* is worth extracting.
- **Usually fine — duplicated shape.** Several blocks look structurally alike but encode *different* decisions, wrap *different* operations, or carry *different* messages. They don't change together. Collapsing them couples unrelated concerns and is often a net loss.

Ask: if requirements change, would these pieces change *for the same reason, at the same time*? Yes → candidate for extraction. No → leave them.

### When an abstraction earns itself

Recommend extraction only when **all** hold; otherwise the explicit version is better:

- The pieces are genuinely the **same concept**, not just the same shape.
- They will **change together**.
- The abstraction is **simpler to read** than the duplication, not just shorter.
- It doesn't force awkward parameters/adapters to paper over differences.
- It doesn't destroy **locality** that aids debugging — explicit code keeps stack traces, grep, and "which step failed" obvious.

Over-abstraction is itself a finding: a helper used once, a config knob nothing sets, a "framework" for a problem that occurs twice, speculative generality. The **rule of three** (wait for the third real occurrence) is a reasonable default.

### The ponytail ladder — concrete YAGNI tests

When judging whether code should exist at all, climb this ladder. Stop at the first rung that holds:

1. **Does this need to exist at all?** Speculative need = skip it.
2. **Already in this codebase?** A helper, util, type, or pattern that already lives here → reuse it.
3. **Stdlib does it?** Use it.
4. **Native platform feature covers it?** DB constraint over app code, CSS over JS, Spark built-in over custom UDF.
5. **Already-installed dependency solves it?** Use it. Never add a new dep for what a few lines can do.
6. **Can it be one line?** One line.
7. **Only then:** the minimum code that works.

Any code that fails rungs 1–3 is a design-choice finding: "X lines of custom code where stdlib/platform does it."

### Agent/AI-authored fingerprints

Code written by coding agents has characteristic blind spots:

- **Plausible stubs.** Functions that return well-typed mock/empty/default values so everything compiles and tests pass, but the real behavior was never implemented.
- **Over-defensive boilerplate.** try/except around things that can't fail, redundant validation, null checks for values never null — noise hiding the one check that matters.
- **Uniform error handling that erases meaning.** Every error funneled to the same message/exit/return, so *which* stage failed and *why* is lost.
- **Locally-optimal, globally-wrong.** A choice clean in one file but conflicts with a convention, invariant, or capability elsewhere — because the author didn't hold the whole system in view.
- **Comments that over-promise.** "Always validated", "identical to X", "thread-safe" — assertions the code doesn't guarantee. Verify every such claim; treat unverified ones as findings.
- **Happy-path completeness.** The main flow works; cancellation, partial failure, retries, empty inputs, and concurrency were skipped.

### Invariants asserted but not enforced

The highest-value latent bugs. A comment, doc, or name *promises* something the code doesn't structurally guarantee:

- "These two sites enforce the same rules" — but they're separate call sites that can diverge.
- "Validated before save" — but one save path skips validation.
- "Idempotent" / "atomic" / "always closed" — claims with no mechanism behind them.

For each, find whether a single source of truth enforces it, or whether it relies on every caller remembering. If the latter, recommend making the invariant **structural** (one shared function/type/guard).

### Worked example

```python
try:
    spec = load_patch_spec(_Path(patch_file))
except PatchError as exc:
    click.echo(f"✗ patch schema error: {exc}", err=True)
    sys.exit(exit_codes.DATA_OR_RUNTIME)
# Guardrails gate — deterministic. Identical enforcement used by
# `patch apply`; surfaced here so reviewers see violations up front.
try:
    _check_guardrails(spec, bp_raw, provenance_map=None)
except PatchError as exc:
    click.echo(f"✗ Guardrails gate blocked: {exc}", err=True)
    sys.exit(exit_codes.DATA_OR_RUNTIME)
try:
    bp_after = apply_patch_to_dict(bp_raw, spec)
except PatchError as exc:
    click.echo(f"✗ patch could not be applied in memory: {exc}", err=True)
    sys.exit(exit_codes.DATA_OR_RUNTIME)
```

**The mechanical (wrong) finding:** "DRY violation — extract the try/except into a helper." Do **not** report this. These three blocks duplicate *shape*, not *logic*. Each wraps a different operation with a distinct, human-meaningful failure message. A helper would need awkward adapters for mismatched signatures, would erase which stage failed from the stack trace, and would be unwound the instant one stage needs its own exit code.

**The real finding:** the comment asserts an invariant — *"Identical enforcement used by `patch apply`."* That's duplicated **logic** (the guardrail rules) across two call sites, maintained only by a comment. If `patch apply` evolves independently, enforcement **silently drifts** — a latent correctness/security bug. Report *this*: High severity, Likely confidence. Fix: make the invariant structural — route both commands through one shared guardrail gate so they cannot diverge.

---

## Prevention Cross-Reference

Each detection pattern maps to an AGENTS.md prevention rule. When a finding recurs across audits, the prevention rule should be strengthened. The Aqueduct-specific domain skills (`aqskill-audit-tests`, `aqskill-audit-code`, `aqskill-audit-config`, `aqskill-audit-style`) cover Aqueduct-specific rules; this skill covers the general patterns.

| Detection § | Catches violation of |
|---|---|
| §4 hardcoded paths | Trace every consumer before changing a type or output path (§Code Organization) |
| §5 swallowed errors | Never `except: pass` without comment justifying why silence is correct |
| §9 resource leaks | Resource acquired → guaranteed release |
| §11 cross-layer imports | 4-layer boundary + documented pyspark exceptions (§Code Organization) |
| §12 dead config | Trace every consumer before changing a type; no silent no-ops |
| §12 falsy-trap | `if not x` on optionals → `if x is None` unless falsy path is identical |
| §13 string literals | Project vocabulary → constants or StrEnum; import them, don't re-type |
| §14 duplicate DDL | Each table created exactly once; DDL-migration idempotency |
| §15 string enum comparisons | Import the StrEnum, don't re-type the string value |
| §16 bare `sys.exit(N)` | Named `exit_codes.*` constants (only `sys.exit(130)` for SIGINT excepted) |
| §16 inconsistent error messages | Consistent capitalization and punctuation |
| §17 missing tests | Non-trivial logic leaves at least one runnable check behind |

---

## Auditor Failures to Avoid

- **Hallucinated findings** — asserting a bug or a "missing feature" you didn't trace. If unconfirmed, mark it Speculative or cut it.
- **Mechanical rule-application** — flagging defensible repetition or local verbosity as debt. Judge against intent and the cost of the alternative, per the Design Judgment section.
- **Judging only what's present** — the whole value is catching what's *absent or half-done*; that requires Phase 0.
- **Scope creep into rewriting** — recommend and show small before/after snippets; don't rebuild unasked.
- **Noise** — cap nits, report patterns once, lead with what matters.
- **Vague locations** — always `file:line`, never "somewhere in the auth module".
