# Aqueduct Architectural Audit Report

**Date**: 2026-05-04  
**Subject**: Architectural Consistency & Documentation Alignment  
**Status**: Remediation Required

## 1. Summary
This audit cross-referenced `docs/specs.md`, `README.md`, the core implementation (`aqueduct/`), and the test suite (`tests/`). While the core engine is functionally robust, there is significant "documentation drift" in `specs.md` following the recent observability refactor.

---

## 2. Critical Mismatches

### 2.1 Observability Store Convergence (DB Naming)
*   **Spec Status**: `specs.md` (§10.1) defines three separate databases: `runs.db`, `signals.db`, and `lineage.db`.
*   **Actual Status**: The implementation has converged on a single **`obs.db`** (Observability Store) which houses `run_records`, `probe_signals`, `failure_contexts`, and `signal_overrides`.
*   **Impact**: Confusion for developers inspecting `.aqueduct/` directory; `aqueduct doctor` and CLI commands already expect `obs.db`.
*   **Action**: Update `specs.md` to reflect `obs.db` as the primary store for run-scoped and signal metadata.

### 2.2 Metrics: `None` vs `0`
*   **Spec Status**: `specs.md` (§6.4) suggests that on Spark < 3.3, row count metrics will be `0`.
*   **Actual Status**: The engine (`metrics.py`) returns `None` for uncollected or missing observations.
*   **Impact**: Logical inconsistency. `None` correctly indicates "unknown/unmeasured," whereas `0` implies a measured result of zero records.
*   **Action**: Align `specs.md` wording to specify `None` as the "uncollected" state. Update any remaining legacy tests that still assert `0`.

### 2.3 `signal_overrides` Table Location
*   **Spec Status**: `specs.md` (§11.1) points to `signals.db`.
*   **Actual Status**: `surveyor.py` and `cli.py` use `obs.db`.
*   **Impact**: Manual database queries via `duckdb` will fail if using spec-provided paths.
*   **Action**: Synchronize all documentation to `obs.db`.

---

## 3. CLI & Command Discrepancies

### 3.1 Missing `runs` Command & `new` vs `init`
*   **Findings**: 
    *   `aqueduct runs` is a core command used to list history (implemented in `cli.py` and mentioned in `README`), but it is completely missing from the `specs.md` CLI Reference (§11).
    *   `specs.md` (§11.1) refers to `aqueduct new <name>` for scaffolding projects. The actual implemented command is **`aqueduct init`**.
*   **Action**: Add `aqueduct runs` and rename `new` to `init` in the formal specification.

### 3.2 Arcade Separator Evolution
*   **Findings**: The test suite (`test_compiler.py`) was lagging behind the architectural change from `.` (dot) to `__` (double underscore) for namespacing. This was fixed during the audit, but `specs.md` should be verified for any lingering dot-notation examples in the Arcade section.

---

## 4. Logical & Enum Inconsistencies

### 4.1 Module Status Enums
*   **Inconsistency**: 
    *   `specs.md` (§5.2): `success | error | skipped`
    *   `specs.md` (§6.3): `running | success | failed | patched | skipped`
    *   `surveyor.py`: Uses `error` for failed modules.
*   **Risk**: Potential for "failed" vs "error" confusion in LLM Patch logic if the agent expects one and gets the other.
*   **Action**: Standardize on a single enum set across all documentation and code. Recommended: `success | error | skipped | patched`.

---

## 5. Remediation Plan

| Item | Priority | Files to Update |
| :--- | :--- | :--- |
| **Sync DB Naming** | High | `docs/specs.md` |
| **Fix Metrics Phrasing** | Medium | `docs/specs.md` |
| **Add `runs` CLI Ref** | Medium | `docs/specs.md` |
| **Standardize Enums** | Low | `docs/specs.md`, `aqueduct/surveyor/models.py` |

---
**Auditor Conclusion**: The engine implementation is ahead of the documentation. Updating `specs.md` to match the "ObsDB" convergence is the most urgent task to prevent developer friction.
