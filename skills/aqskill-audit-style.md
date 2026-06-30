---
name: aqskill-audit-style
description: Audit Aqueduct for over-broad except clauses, string-in-context safety, minimum-Python syntax violations, dispatch-pattern consistency, and scattered constants. Use after touching error handling, text parsing, or multi-type dispatch. Triggers include "audit code style", "check error handling", "find Python 3.11 issues", "audit constants".
---

# Aqueduct Style & Syntax Audit

Extends `codebase-audit.md` with Aqueduct-specific style patterns. All prevention rules live in AGENTS.md — this skill provides detection, not rules.

## ⚠️ Verify before you report (precision gate — read first)

`rg` produces **candidates, not findings**. Promote one ONLY after:
1. **Read the cited `file:line` + the lines around it** (esp. the comment ABOVE an `except`).
2. **Check the repo neutralizers below** — drop if one applies.
3. **Write a concrete failure scenario.** For a *style* finding the "failure" is concrete harm (e.g. "this `except Exception: pass` would swallow a real `KeyError` from a typo'd key and the user sees no error"). No concrete harm → drop; style nits without harm are noise.
4. **Confirm it's still true at HEAD.**

Tag each row **PROVEN**/**SUSPECTED**.

## Repo neutralizers — false-positive guards (check before flagging)

- **Accepted dispatch chains.** The if-elif chains in `executor/spark/executor.py`, `executor/spark/funnel.py` (mode), and `test_runner.py` are **policy-accepted** (AGENTS.md). Do not re-flag them; only flag *new* >3-branch type-dispatch chains elsewhere.
- **Commented except is justified.** `except Exception: pass/continue` with a comment above explaining why silence is correct for that path is **fine**. Read the comment before flagging. Only bare/generic ("# ignore errors") or comment-less swallows are findings.
- **f-string 3.11 grep is noisy.** The backslash-in-f-string regex over-matches. Confirm with an actual `python3.11 -m py_compile` **failure** before reporting — if it compiles, drop it.
- **Recovery-pass regex is safe.** String/regex transforms in `agent/parse.py` that run **only after** strict `json.loads` fails (fence/think-block strip, json_repair) are by design. Flag only transforms applied to *valid* input.
- **Constant hoisting threshold.** A literal is a finding only at **3+ files** AND where a shared `StrEnum`/constant already exists or clearly should. A value local to one module is not a finding.

## Over-broad except justification

Every `except Exception: pass` or bare `except:` must carry a comment explaining why silence is correct for EVERY possible exception.

```bash
# Bare except (forbidden — always flag)
rg -n -U 'except\s*:' aqueduct/ --glob '!test_*'

# except Exception: pass — check for justification comment above
rg -B 2 -U 'except\s+(Exception|BaseException)?\s*:\s*\n\s*(pass|return\s+None)' \
  aqueduct/ --glob '!test_*'

# Focus areas: patch/apply.py, executor/spark/, agent/loop.py, surveyor/
```

For each hit, check the comment above. An acceptable justification explains WHY silence is correct for the specific failure (e.g., "`# DBFS close-handle is best-effort cleanup on error; the real failure is the block upload`"). An unacceptable one is bare or generic ("`# ignore errors`").

## String-in-context safety

Regex or string transforms on raw text (JSON, YAML, SQL) must verify the target is outside quoted strings, or run only as a recovery pass after strict parsing fails.

```bash
# Find regex applied to LLM responses or user config text
rg -n 're\.sub|re\.match|re\.search|\.replace\(' aqueduct/agent/parse.py
rg -n 're\.sub|re\.match|re\.search' aqueduct/patch/

# For each: verify it runs ONLY in a recovery path (after json.loads fails), not on valid input
# Known-safe: fence/think-block stripping in parse.py (deterministic, content-preserving)
# High-risk: line-comment regex, json_repair applied before strict parsing
```

## Minimum-Python syntax (3.11)

Python 3.11 forbids backslash inside f-string `{…}` expressions.

```bash
# Compile-check the whole tree
python3.11 -m py_compile aqueduct/**/*.py

# Or grep for the pattern
rg -n 'f".*\{.*\\.*".*"' aqueduct/ --glob '!test_*' | head -20

# Known safe: _md = "\u2014" hoisted to a variable in dashboard/app.py
# Flag any NEW instances since that fix
```

## Dict-dispatch vs if-elif chains

When dispatching on a fixed set of types, prefer `_DISPATCH` dict. Existing chains in `executor.py` and `test_runner.py` are accepted.

```bash
# Find len>=4 if-elif chains on the same attribute (potential new chains)
rg -U -n 'if .*\.type\s*==.*:[\s\S]{0,300}elif .*\.type\s*==.*:[\s\S]{0,300}elif' \
  aqueduct/ --glob '!test_*'

# Find existing dict dispatch patterns (the preferred approach)
rg -n '_DISPATCH\s*=|_HANDLERS\s*=|_REGISTRY\s*=' aqueduct/ --glob '!test_*'

# Check: when a new module type is added, how many files need changes?
rg -n 'ModuleType\.' aqueduct/executor/ aqueduct/compiler/ aqueduct/cli/ | \
  rg -v 'test_|__init__'
```

## Constants-not-literals

String values appearing in 3+ files should be hoisted to a shared constant or `StrEnum`.

```bash
# Find pydantic Literal values — these define the vocabulary
rg -n 'Literal\[' aqueduct/parser/schema.py aqueduct/config.py

# For each, check callers import the enum/constant rather than using bare strings
# Example: "solved" / "error" / "pending" — are they in a StrEnum?
rg -n '"solved"|"exhausted_attempts"|"budget_seconds_exceeded"' aqueduct/ --glob '!test_*'
rg -n '"pending"|"applied"|"rejected"' aqueduct/ --glob '!test_*'

# Status strings used across many files
rg -n '"success"|"error"|"skipped"' aqueduct/cli/ aqueduct/executor/ | rg -v 'test_|__init__'

# Config key strings duplicated in docs and code
rg -n '"agent\.approval"|"danger\.allow_multi_patch"|"stores\.depots"' aqueduct/ docs/ --glob '!test_*'
```

## Type-checking

```bash
# Run mypy if installed (skip if not — use py_compile as fallback)
python3.11 -m mypy aqueduct/ --ignore-missing-imports 2>&1 | head -100

# Fallback: ensure every .py file compiles
python3.11 -m compileall -q aqueduct/ 2>&1
```

## Output format

```markdown
## Style audit — [date]

Every row MUST carry a **verdict (PROVEN/SUSPECTED)** and the **concrete harm** it causes.
Only rows that survived the verify gate.

### Over-broad except violations
| file:line | catch type | justification? | verdict | concrete harm |

### String-in-context risks
| file:line | transform | applied before parse? | risk |

### Python 3.11 syntax issues
| file:line | expression | fix |

### Dispatch consistency
| file:line | pattern | issue |

### Scattered constants
| value | files | should use |
```
