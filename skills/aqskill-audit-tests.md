---
name: aqskill-audit-tests
description: Audit Aqueduct tests for stale assertions, missing markers, hardcoded local paths, and specs-to-tests coverage gaps. Use after a code change that reclassifies exceptions, removes config values, or switches I/O capture. Triggers include "audit tests", "are tests up to date", "check test coverage", "verify tests match code".
---

# Aqueduct Test Audit

Extends `aqskill-audit-health.md` with Aqueduct-specific test patterns. All prevention rules live in AGENTS.md — this skill provides detection, not rules.

## ⚠️ Verify before you report (precision gate — read first)

`rg` produces **candidates, not findings**. A candidate is guilty until proven. Promote it to a reported finding ONLY after all four:

1. **Read the cited `file:line` + surrounding context** — not just the matched line.
2. **Check the repo neutralizers below.** If a mechanism already makes the candidate correct, **DROP it** (do not downgrade to "minor" — drop).
3. **Write a concrete failure scenario:** "with input/state X, this test/line yields wrong result Z." Can't write one → it's SUSPECTED, not a finding.
4. **Confirm it's still true at HEAD.** The code may already be fixed — re-read current file content; never report from a remembered or expected state. (A whole resolved issue was once re-filed because nobody opened the already-`caplog` test.)

Tag every row **PROVEN** (did 1–4) or **SUSPECTED** (pattern only). Drop SUSPECTED unless it's genuinely cheap for the user to check.

## Repo neutralizers — things that make a candidate a FALSE positive (check each before flagging)

- **Auto-marker.** `tests/conftest.py::pytest_collection_modifyitems` assigns a layer marker to **every** unmarked test (`unit`, or `spark` if it pulls the `spark` fixture). So a missing `@pytest.mark.*` is **NOT** a finding on its own, and "won't run in CI" is **false** for almost all of them. The ONLY real marker bug: a file that needs pyspark at **import/collection time** (e.g. `from aqueduct.executor.spark...` at module level) but does **not** use the `spark` fixture → auto-tagged `unit` → the no-spark CI job collects it → ImportError. Flag only those.
- **Pydantic validator.** A `ValueError`/`AssertionError` raised inside a `@field_validator`/`@model_validator` is **correct and required** — pydantic converts it to `ValidationError`. Tests asserting `ValueError`/`ValidationError` on those paths are correct, not stale.
- **Wrapper re-raise.** An exception assertion can be right because a wrapper re-raises as the asserted type (e.g. `compiler/compiler.py` wraps `ExpandError` → `CompileError`). Trace the exception the test actually receives before calling it stale.
- **Already migrated.** Before flagging `capsys`→`caplog` (or any "should change to"), OPEN the file — it may already be done.
- **DuckDB tests pass today.** A hardcoded `.aqueduct/`/`duckdb.connect(...)` path is a *portability-to-remote-store* concern, not a failing test. Do NOT label it "will fail" unless the suite is actually being run against Postgres/S3. Report as low-severity portability debt, if at all.
- **Removed-value reality.** Confirm a value/alias is actually removed from the schema/code at HEAD before flagging tests that use it (read `aqueduct/parser/schema.py` etc.). If code and a test disagree, the bug may be in the test OR the code — say which, with the source-of-truth line.

## Error-type freshness

After reclassifying exception types (ValueError→ParseError, RuntimeError→ConfgError, etc.), find tests that catch the old type.

```bash
# Tests asserting ValueError on parser graph/resolver code (should be ParseError)
rg -n "pytest\.raises\(ValueError" tests/test_parser/test_graph.py tests/test_parser/test_resolver.py

# Tests asserting RuntimeError on @aq.* resolution (should be CompileError)
rg -n "pytest\.raises\(RuntimeError" tests/test_compiler/

# Tests asserting RuntimeError on Databricks config/auth (should be ConfigError/DeployError)
rg -n "pytest\.raises\(RuntimeError" tests/test_deploy/

# Tests asserting RuntimeError on missing API keys/credentials (should be ConfigError)
rg -n "pytest\.raises\(RuntimeError" tests/test_agent/

# Tests expecting RuntimeError from @aq.secret() — now raises SecretsError directly
rg -n "pytest\.raises\(RuntimeError.*secret" tests/test_compiler/
```

## Literal/value removal fallout

After removing a Literal value (e.g., `"aggressive"` from approval values), find stale references.

```bash
# Stale approval value references in tests
rg -n '"aggressive"' tests/

# Removed config aliases still in tests
rg -n 'aggressive_max_patches|allow_aggressive_patching|--allow-aggressive' tests/
```

## I/O capture mismatch

When `print(file=sys.stderr)` changes to `logger.warning()`, `capsys` no longer captures output.

```bash
# Tests using capsys that may need caplog instead
rg -n 'capsys\)' tests/test_surveyor/test_*webhook*.py tests/test_surveyor/test_openlineage.py

# Verify each capsys.assert matches a real capsys.readouterr() call (not unused fixture)
```

## Test structure

```bash
# Tests named after specific issues (should merge into feature files)
rg -l 'test_.*issue|test_cli_issue|test_.*bug' tests/

# Marker check — DO NOT flag "missing @pytest.mark" blindly: conftest auto-marks
# every unmarked test (see neutralizers). The ONLY real bug is a file needing
# pyspark at collection time but NOT using the `spark` fixture. Find those:
#   1. files that import executor.spark at module level:
rg -l '^from aqueduct\.executor\.spark|^import aqueduct\.executor\.spark' tests/
#   2. of those, the ones that neither declare a marker nor use the `spark` fixture
#      (read each candidate — if it has `pytestmark`/`@pytest.mark.spark` or a
#      `spark` fixture arg, it's fine; only the remainder need `@pytest.mark.spark`).

# Tests in wrong directory
# compiler tests in cli/
rg -n 'from aqueduct\.compiler' tests/test_cli/test_*.py
# parser tests in executor/
rg -n 'from aqueduct\.parser' tests/test_executor/
```

## Blast-radius — hardcoded local paths

Non-local stores (s3/gcs/adls, postgres) make hardcoded paths break.

```bash
# Hardcoded .aqueduct/ paths (not from fixtures)
rg -n '"\.aqueduct/' tests/ | rg -v 'fixture|conftest|test_config'

# Hardcoded duckdb.connect with literal path
rg -n 'duckdb\.connect\("' tests/

# Hardcoded patches/ directory
rg -n '"patches/"' tests/test_cli/test_cli_patch.py tests/test_cli/test_cli_heal.py

# Tests that patch away config resolution — silently hide remote-store mismatches
rg -n 'patch\("aqueduct\.config' tests/
```

## Specs-to-tests coverage

Cross-reference `docs/specs.md` and `docs/cli_reference.md` against tests.

```bash
# For each CLI flag, check at least one test exercises it
# Manual cross-reference: read cli_reference.md flag table, grep tests/

# For each module type, check config key validation
# Ingress table:, Egress table:, Channel spillway_condition, etc.

# For each danger.* setting: default→blocked, true→allowed, startup warning
rg -n 'allow_full_probe_actions|allow_multi_patch|allow_full_preflight|allow_skip_sandbox' tests/

# For each store backend, at least one integration test
rg -n 'postgres|redis|s3|gcs' tests/test_stores/

# Patch lifecycle: each command tested
rg -l 'patch.*apply|patch.*reject|patch.*commit|patch.*discard|patch.*rollback' tests/
```

## Output format

```markdown
## Test audit — [date]

Every row below MUST carry a **verdict (PROVEN/SUSPECTED)** and a **failure scenario**
(the input/state → wrong result). Rows that survived the verify gate only.

### Error-type mismatches
| file:line | current assertion | expected | verdict | failure scenario |

### Stale value references
| file:line | stale reference | what to change |

### I/O capture mismatches
| file:line | capsys usage | should be caplog? |

### Structure issues
| file:line | issue | suggested fix |

### Blast-radius (hardcoded paths)
| file:line | hardcoded value | why it breaks | suggested fix |

### Coverage gaps
| feature | documented at | test? |
```
