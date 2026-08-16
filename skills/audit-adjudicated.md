# Adjudicated audit findings — the rejection ledger

Findings that a triage pass **examined and rejected**, with the reason. This file exists so
repeated audit cycles cannot re-litigate the same noise.

## How to use it

**Detection agents must NOT read this file.** Handing a detector a rejection list poisons recall
— the one thing cheap models are actually good at — and it would permanently bury any entry here
that turns out to be wrong. Detectors stay blind and report whatever they see.

**The orchestrator applies this file as a filter during the verification pass.** A candidate
matching an entry below is dropped unless it arrives with *new evidence* — a reproduction the
original adjudication did not have, or a code change since the date shown that invalidates the
reasoning.

**A blind re-report is a signal, not a failure.** If a fresh audit re-raises many entries from
this file, one of two things is true: the ledger entry is wrong, or the audit is exhausted and
should stop being run. Track the ratio; it is the honest measure of when auditing has stopped
paying.

**Append, don't rewrite.** An entry is only removed when it is shown to be WRONG — and then the
removal is itself a finding worth a commit message. Correcting a bad rejection is the whole
reason detectors stay blind.

## Rejection reasons (use these labels)

| Label | Meaning |
|---|---|
| `NEUTRALIZED` | Real pattern, but a documented exception or nearby code prevents the failure |
| `BY-DESIGN` | Works as intended; a recorded decision says so (cite it) |
| `NO-SCENARIO` | Could not produce a concrete failure; SUSPECTED at best |
| `STALE` | Was true when reported, already fixed at HEAD |
| `MISATTRIBUTED` | Defect real but in a different file — **re-file it, do not drop it** |
| `COSMETIC` | True but no correctness/security/UX consequence; hygiene only |

---

## Ledger

Seeded 2026-08-08 from the 29-report `aqskill-audit-yolo` run (2026-08-03 → 2026-08-08).

| Date | Finding | `file:line` | Reason | Notes |
|---|---|---|---|---|
| 2026-08-08 | Raw `ValueError` raises should become `AqueductError` subclasses | `parser/schema.py` (14 sites) | `BY-DESIGN` | All 14 are inside `@field_validator`/`@model_validator`. Raising `ValueError` there is the correct pydantic idiom — pydantic converts it to `ValidationError`. Converting them would break parsing. AGENTS.md:622 carves this out explicitly. |
| 2026-08-08 | `redaction.py` registry mutated without a lock | `aqueduct/redaction.py` | `NO-SCENARIO` | No concrete failure under CPython's GIL; self-labelled SUSPECTED by the audit. |
| 2026-08-08 | Root-logger filter does not scrub tracebacks | reported against `redaction.py` | `MISATTRIBUTED` | Filter lives in `cli/__init__.py`, not `redaction.py`. **Re-filed and fixed** in `f2f2b4f` — and turned out far more severe than reported. Kept here as the worked example of why MISATTRIBUTED is never a drop. |
| 2026-08-08 | Raw `warnings.warn` bypass sites in `config.py` | `config.py:541`, `config.py:1856` | `STALE` | Both already route through `emit()`, with comments saying so. Zero `warnings.warn(` calls remain in that file. |
| 2026-08-08 | `call_tool` redaction routing is not test-locked | `aqueduct/tools/registry.py` | `STALE` | `test_call_tool_redacts_secret_in_run_detail` covers it and predates the audit by three weeks. `call_tool`'s redact step is handler-agnostic, so one handler proves the mechanism. |
| 2026-08-08 | Hardcoded constructor leaf names in the capability gate | `compiler/capability_check.py` | `NO-SCENARIO` | Real future-drift risk, no current bug. Design question, not a defect. |
| 2026-08-08 | A future tool returning an MCP-reserved dict key | `aqueduct/mcp/server.py` | `NO-SCENARIO` | Dormant while the registry stays 7 closed built-ins. |
| 2026-08-08 | `load_module`/`load_callable` raise bare builtins | `infra/module_loading.py` | `BY-DESIGN` | Documented in the function's own docstring: callers wrap into domain errors. |
| 2026-08-08 | `if self.config:` / `if self.patches_dir:` falsy traps | `integrations/airflow/operator.py` | `NO-SCENARIO` | Only bites on an explicit empty string, which has no meaning here. |
| 2026-08-08 | `Phase N` markers in capabilities.yml | `duckdb_/capabilities.yml:1,41,422,592,608`; `spark/capabilities.yml:440` | `NEUTRALIZED` | All are YAML **comments**. Only `hint:` values render into `docs/compatibility.md`, so the user-facing-surface rule holds. Verified: `rg 'hint:.*Phase'` → no match. |
| 2026-08-08 | `ModuleType` (Ingress, Egress) checks duplicated across doctor / jar_availability | `aqueduct/doctor/`, `spark/warnings/jar_availability.py` | `COSMETIC` | Closed, audited pairing that will not grow. A "constants not literals" nit, not the growing-set bug. |
| 2026-08-08 | `Support` 2-of-4 membership check | `compiler/capability_check.py` | `NEUTRALIZED` | Stable, tightly-coupled framework enum. |
| 2026-08-08 | `ExecutionStatus` membership check | `cli/run.py` | `NEUTRALIZED` | Already written as an exclude-list — the safe direction. |
| 2026-08-08 | OpenLineage event-type subset | `surveyor/openlineage.py` | `BY-DESIGN` | Governed by an external spec, not an internal growing enum. |
| 2026-08-08 | Patch status `pending`/`applied`/`rejected` subset | `aqueduct/patch/` | `BY-DESIGN` | A closed 3-state machine. |
| 2026-08-08 | `cascade.py` `_ESCALATION_REASONS` / `_TIER_RETRY_REASONS` over `StopReason` | `agent/cascade.py` | `NEUTRALIZED` | An unclassified member defaults to the loud abort path — fails safe. |
| 2026-08-08 | Funnel / Junction `VALID_MODES` membership | `spark/funnel.py`, `spark/junction.py` | `BY-DESIGN` | Already the single canonical constant, imported by `capability_leaves.py`. |
| 2026-08-08 | Streamlit config-summary hand-picked field lists | `aqueduct/dashboard/app.py` | `COSMETIC` | Read-only debug panel; not a correctness or security gate. |
| 2026-08-08 | `governed_leaves()` should reject every unregistered engine name | `executor/capability_tooling.py:132` | `NEUTRALIZED` (partial) | The **raise was added** (`7611fc8`), but `scaffold()`/`check()`/`sync()` legitimately need unregistered names — that is what scaffolding a new engine means, and each engine's `capabilities.py` walks leaves during its own registration. They pass `require_registered=False`. Do not "fix" those three call sites. |
| 2026-08-16 | `engine.<name>.*` Blueprint block produces no capability leaf, so nothing in the capability framework governs `engine.spark.conf.<key>` | `parser/schema.py:934-951`, `1141`; `executor/capability_leaves.py:231-243` | `BY-DESIGN` | Ratified standing decision: "Blueprint engine-block fields are NOT capability leaves — the allowlist is the ONLY gate on the healing surface; no second mechanism catches an allowlist mistake." ⚠ The 2026-08-16 audit ALSO claimed the surface therefore has "zero governance ... it compiles, applies and runs" — that half is FALSE: `patch/apply.py:552-553` calls `_check_engine_config_allowlist` unconditionally for every `set_engine_config` op. The worker was misled by the (then) stale module docstring, fixed in `5aac67b`. If this is re-reported WITH the zero-governance claim, it is still wrong. |
| 2026-08-16 | Nothing validates a HAND-AUTHORED `engine.spark.conf.<key>` in a Blueprint | `parser/schema.py:872-880` (`conf: dict[str, Any]`, shape only) | `BY-DESIGN` | Ratified 2026-08-16: hand-authored `conf` is deliberately unvalidated. The engine-config allowlist gates the healing AGENT's self-granted config changes; it is not, and was never meant to be, a gate on the operator's own Blueprint. A human editing their own Blueprint already has arbitrary power over it (they can rewrite the file, or `aqueduct.yml`) — gating hand-authored config would gate the operator against themselves for no benefit. The intent is now recorded as a one-sentence docstring on `SparkEngineBlockSchema.conf` rather than a new validator. |
| 2026-08-16 | `AqueductConfig.__eq__` compares the private `_cli_engine_overrides` (`--set`) attr, so configs differing only by `--set` are unequal | `config.py:1595` (`PrivateAttr`), pydantic 2.12.5 `BaseModel.__eq__` | `NO-SCENARIO` | Mechanically true. Exhaustive search of `aqueduct/` AND `tests/` for `cfg ==`, `== cfg`, `cfg in [...]`, dict-key / set / `lru_cache` use of an `AqueductConfig` INSTANCE found ZERO call sites — every hit compares a scalar field or a dict. Latent trap for a future author who memoizes on config identity; no reachable failure today. |
| 2026-08-16 | `run_polyglot` silently defaults an unlisted island's engine config to `{}` | `executor/orchestrator.py:488-497` | `NO-SCENARIO` | `(engine_configs or {}).get(island.engine, {})`. The one real caller (`cli/run.py:2041-2044`) builds the dict per island via `resolve_session_engine_config`. Defense-in-depth gap, not reachable via the shipped CLI. |
| 2026-08-16 | `budget.py` comment describes a stall metric ("...OR has not grown vs the previous window") half of which was never implemented | `agent/budget.py:377-388` | `COSMETIC` | Code is `return len(unique) == 1`; the comment itself then explains the conservative choice, and `same_signature_overall` catches the broader case. Misleading first sentence only. |
| 2026-08-16 | `PATHLESS_INGRESS_FORMATS` carries a vestigial `depot` entry | `executor/path_keys.py:90` | `COSMETIC` | `capability_leaves.py:180-191` records that depot never had an Ingress dispatch branch on any engine; the `ingress.format.depot` leaf was removed for that reason. No code path reads it for depot. |
