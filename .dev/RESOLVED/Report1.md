# Aqueduct Engine: Deep Contract Audit Report

**Date:** 2026-05-08  
**Status:** ⚠️ Phase C/D Blocked (Critical Implementation Gaps Found)

## 1. Executive Summary
The Aqueduct engine's core structural logic (DAG execution, topo-sorting, Junction/Funnel routing) is robust and production-ready. However, the "Deep Contracts"—the fine-print safety and scalability features promised in `specs.md`—show significant implementation gaps. 

**Key Finding:** Several features documented as "Core" are currently either unimplemented or diverge from the documented API, creating a risk of silent failures in production.

---

## 2. Critical Implementation Gaps (Blockers)

### 2.1 `schema_hint` API Mismatch
*   **Spec Promise:** `schema_hint` is a flat dictionary (e.g., `id: LONG, val: DOUBLE`).
*   **Observed Behavior:** The Spark executor silently ignores flat dictionaries. It expects a nested structure (`{mode: "strict", columns: [...]}`).
*   **Risk:** Users believe they are enforcing schema at Ingress, but the engine is bypassing all checks without warning.

### 2.2 Missing `spillway_rate` Assertion
*   **Spec Promise:** Prevents "silent data loss" by aborting the pipeline if too much data is quarantined.
*   **Observed Behavior:** The `spillway_rate` rule type is completely absent from `aqueduct/executor/spark/assert_.py`. 
*   **Risk:** Pipelines can "succeed" with 0 rows of good data and 1,000,000 rows in the spillway, violating the primary safety contract.

### 2.3 Unsupported Egress `mode: merge`
*   **Spec Promise:** Supports Delta Lake `MERGE INTO` semantics via `mode: merge` and `merge_key`.
*   **Observed Behavior:** `merge` is missing from the `SUPPORTED_MODES` in `egress.py`. 
*   **Risk:** Standard Lakehouse upsert patterns are impossible to implement in Aqueduct today.

### 2.4 Incomplete Spillway Metadata
*   **Spec Promise:** Every spillway row contains 4 metadata columns: `_aq_error_module`, `_aq_error_msg`, `_aq_error_type`, and `_aq_error_ts`.
*   **Observed Behavior:** Only 3 columns are implemented. `_aq_error_type` is missing.
*   **Risk:** Inconsistent observability data for downstream error handling/healing.

---

## 3. Verified Successes (Pass)

Despite the gaps above, the following complex systems were validated successfully:
*   [x] **Checkpoint & Resume:** Multi-run state reload and manifest-hash stale detection works perfectly.
*   [x] **Logic Patterns:** All `Junction` (conditional/broadcast/partition) and `Funnel` (union/coalesce/zip) modes correctly handle lazy Spark plans.
*   [x] **Probe Suppression:** `block_full_actions: true` correctly identifies and blocks costly signals (null_rates, distinct_count) while allowing free ones (schema, partitions).
*   [x] **Regulator Gates:** Passive compile-away and active `on_block: skip` propagation are iron-clad.
*   [x] **Error Signaling:** `trigger_agent: true` set on ExecutionResult for self-healing.

---

## 4. Recommendations for Release

1.  **Standardize `schema_hint`:** Update `ingress.py` to support the flat dictionary format shown in all public examples.
2.  **Implement `spillway_rate`:** Add the aggregate logic to `assert_.py` to calculate the quarantine fraction.
3.  **Implement `mode: merge`:** Add Delta Lake `MERGE INTO` handler to `egress.py`.
4.  **Enrich `schema_match`:** Add a mapping layer so users can use `LONG` or `DOUBLE` instead of being forced to use Spark-internal `bigint` strings.

## 5. Audit Traceability
All validation scripts, blueprints, and bug-reproduction tests are located in:
`tmp/audit/15-deep-contracts/`
