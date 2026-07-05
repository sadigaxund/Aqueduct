---
name: aqskill-audit-code
description: Audit Aqueduct source for exit-code consistency, cross-layer import violations, resource leaks, falsy-traps, CLI vocabulary drift, and redaction bypasses. Use after touching CLI output, error handling, store backends, or infra code. Triggers include "audit code quality", "check error handling", "find cross-layer imports", "verify redaction".
---

# Aqueduct Code-Layer Audit

Extends `aqskill-audit-health.md` with Aqueduct-specific code patterns. All prevention rules live in AGENTS.md — this skill provides detection, not rules.

## ⚠️ Verify before you report (precision gate — read first)

`rg` produces **candidates, not findings**. Promote a candidate to a reported finding ONLY after:
1. **Read the cited `file:line` + context** (is the import module-level or inside a `def`? is the falsy value reachable as a distinct value?).
2. **Check the repo neutralizers below** — drop the candidate if one applies.
3. **Write a concrete failure scenario** ("input/state X → wrong result Z"). No scenario → SUSPECTED, not a finding.
4. **Confirm it's still true at HEAD** — re-read current content; the invariant fixes already landed many of these.

Tag each row **PROVEN**/**SUSPECTED**. Drop SUSPECTED unless cheap to check.

## Repo neutralizers — false-positive guards (check before flagging)

- **Lazy pyspark is allowed.** A `import pyspark` is a violation only if it's **module-level (column 0)** AND outside `executor/spark/` AND not a documented exception. Documented/acceptable: `doctor/*` (function-body), `surveyor/surveyor.py` + `surveyor/error_extraction.py` (function-body), and `dashboard/app.py::main()` (lazy narwhals/plotly init). Before flagging, confirm the import is at top level, not inside a function.
- **One-shot stderr is allowed.** `print(..., file=sys.stderr)` is a redaction concern only on **daemon-thread / repeated delivery** paths. One-shot CLI-exception prints (e.g. `openlineage.py` build-step failure, `patch/apply.py` archive warning) are accepted. Distinguish the two before flagging.
- **Pydantic validator.** `raise ValueError` inside `@field_validator`/`@model_validator` is **required** — never recommend converting it to an `AqueductError` subclass.
- **Known/tracked items.** The Executor→Surveyor `fire_webhook` import and a few others are already tracked in `.dev/`. Report once as "known," don't re-raise as newly discovered.
- **Falsy-trap needs a distinct value.** `if not x` is a bug only if a falsy value (`0`/`""`/`[]`) is a **semantically distinct, reachable** state. Many are correct because falsy == unset there. Prove the distinct-value path or drop it.

## Exit-code consistency

Every `sys.exit()` must use a named `exit_codes.*` constant. Only `sys.exit(130)` for SIGINT is exempt.

```bash
rg -n 'sys\.exit\(\d+\)' aqueduct/ --glob '!*__pycache__*' | rg -v 'sys\.exit\(130\)'
```

## Cross-layer import violations

The 4-layer boundary: Parser → Compiler → Executor → Surveyor. Upward imports are violations.

```bash
# Executor importing from surveyor (Layer 3 → Layer 4)
rg -n 'from aqueduct\.(surveyor|agent|patch)' aqueduct/executor/ | rg -v 'test_'

# Parser importing from compiler (Layer 1 → Layer 2)
rg -n 'from aqueduct\.compiler' aqueduct/parser/ | rg -v 'test_'

# Any non-executor/spark module importing pyspark
# (doctor/ may import pyspark lazily inside function bodies — verify the import is INSIDE a def)
rg -n 'import pyspark|from pyspark' aqueduct/ \
  --glob '!{executor/spark/**,test*/**,tests/**,doctor/**,surveyor/surveyor.py}'

# Dashboard (already fixed — verify)
rg -n 'import pyspark' aqueduct/dashboard/
```

## Resource leaks

```bash
# httpx.Client() without context manager (with ...)
rg -n 'httpx\.Client\(\)' aqueduct/ | rg -v '(with |return )'

# duckdb.connect() without explicit .close() (not inside a context manager)
rg -U -n 'duckdb\.connect\(' aqueduct/ \
  | rg -v 'context manager|with.*connect|\.close\(\)'

# SparkSession.getOrCreate() without teardown
rg -n 'getOrCreate\(\)' aqueduct/executor/spark/session.py
```

## Falsy-traps on optional values

`if not x` where `x` could be `0`, `""`, `[]`, `{}`, `False` — should be `if x is None`.

```bash
# Focus: config fields, optional values, counts, empty-string defaults
# Manual: scan agent/budget.py, config.py, compiler/runtime.py for:
rg -n 'if not .*:\s*$' aqueduct/agent/budget.py aqueduct/config.py | head -30
```

Common falsy-trap patterns to inspect:
- `if not config_value` on optional int/str fields
- `if not count` where count=0 is legitimate
- `if not result` where `[]` or `""` is valid
- `or` merge with empty-string fallback (`x or default` where `x=""` is valid)

## CLI vocabulary drift

All user-facing output must go through `aqueduct/cli/style.py` or `aqueduct/cli/output.py`. Never raw `print()` or hand-rolled `click.echo(click.style(...))`.

```bash
# Raw print() in CLI commands
rg -n 'print\(' aqueduct/cli/ | rg -v '(test_|__pycache__|def.*print)'

# Hand-rolled click.echo with inline colour/icons (should use style.error/success/warn/info)
rg -n 'click\.echo\(click\.style' aqueduct/cli/ | rg -v 'style\.py|output\.py'

# Raw WARNING: or %(levelname)s: log format (should use StyledLogFormatter)
rg -n 'WARNING:|%(levelname)s' aqueduct/cli/style.py aqueduct/cli/output.py
```

## Redaction bypass

URLs, DSNs, and keys printed outside the `_RedactingFilter` path are visible in logs.

```bash
# Bare print() in non-CLI modules that may carry URLs
rg -n 'print\(.*url' aqueduct/infra/ aqueduct/doctor/ aqueduct/agent/

# click.echo in non-CLI modules
rg -n 'click\.echo' aqueduct/ | rg -v 'aqueduct/cli/'

# Direct stderr writes
rg -n 'file=sys\.stderr' aqueduct/ | rg -v 'test_'

# Doctor checks printing DSNs (password leak)
rg -n 'url=\{|dsn=' aqueduct/doctor/
```

## Agent-specific

```bash
# Agent errors that should carry AqueductError type (not bare ValueError/RuntimeError)
rg -n 'raise ValueError|raise RuntimeError' aqueduct/agent/ | rg -v 'test_'

# Provider credential checks using ConfigError (not RuntimeError)
rg -n 'API_KEY.*not set|base_url.*must be set' aqueduct/agent/providers.py
```

## Output format

```markdown
## Code audit — [date]

Every row MUST carry a **verdict (PROVEN/SUSPECTED)** and a **failure scenario**.
Only rows that survived the verify gate.

### Exit-code violations
| file:line | bare int | should use | verdict |

### Cross-layer imports
| file:line | import | direction (upward?) | justification |

### Resource leaks
| file:line | resource | leak type |

### Falsy-traps
| file:line | expression | risks |

### CLI vocabulary drift
| file:line | raw output | should use |

### Redaction bypasses
| file:line | what's printed | risk |
```
