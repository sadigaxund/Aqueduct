# Aqueduct Benchmarks (`aqscenarios`)

The Benchmarking suite is a data-driven evaluation layer designed to measure and visualize the self-healing reliability of Aqueduct across different LLM providers and prompt versions.

It enables model selection based on evidence, catches prompt regressions in CI, and serves as a public leaderboard for Aqueduct's autonomous capabilities.

## The `benchmark` Command

Aqueduct includes a native benchmarking CLI to run the scenario suite against one or more models:

```bash
# Run the full suite against the default model
aqueduct benchmark --scenarios gallery/aqscenarios/suite-01-healing/

# Compare multiple models side-by-side
aqueduct benchmark --scenarios ... --models claude-3-5-sonnet,gpt-4o,llama-3-70b
```

### Example Comparison Output

| Scenario                      | claude-3.5-sonnet | llama-3-70b | gpt-4o |
| :---------------------------- | :---------------- | :---------- | :----- |
| `schema_drift_column_rename`  | **PASS** 0.94     | **PASS** 0.81 | FAIL   |
| `bad_path_typo`               | **PASS** 0.99     | **PASS** 0.88 | **PASS** 0.72 |
| `oom_config_fix`              | FAIL              | FAIL        | FAIL   |
| ...                           |                   |             |        |
| **Parse rate**                | 100%              | 92%         | 87%    |
| **Apply rate**                | 91%               | 85%         | 79%    |

## Canonical Scenarios

The goal is to maintain 20–30 canonical scenarios covering the most frequent data engineering failure classes:
- **Schema Drift**: Column renames, type changes, missing fields.
- **Pathing Errors**: Typos, incorrect S3/DBFS prefixes, missing partitions.
- **Format Mismatches**: CSV vs. Parquet vs. Delta confusion.
- **Resource/OOM**: Memory config fixes, executor tuning.
- **SQL Errors**: Column not found, invalid window functions, syntax errors.

## Scoring & Metrics

- **Parse Rate**: Percentage of LLM responses that correctly follow the `PatchSpec` JSON schema.
- **Apply Rate**: Percentage of patches that successfully pass internal validation and can be applied to the Blueprint.
- **Success Rate (PASS)**: Percentage of patches that result in a successful Spark run with correct data output.
- **Accuracy Score**: A decimal value (0.0 - 1.0) comparing the generated patch against the **Ground Truth** (`expected.patch`) using AST-based comparison.

## Prompt Versioning

Goal: correlate score improvements/regressions to specific system-prompt changes.

**Status — partially implemented:**
- `PROMPT_VERSION` constant (`aqueduct/agent/__init__.py`), manually bumped on significant prompt changes. ✅
- Stamped into applied-patch metadata (`_aq_meta.prompt_version`). ✅
- **Not yet:** `prompt_version` is *not* recorded in `healing_outcomes`, and benchmark results carry no `prompt_version` (benchmark has no persistence at all). So cross-version correlation / regression tracking is **not possible yet** — tracked as Phase 33 (benchmark persistence + `healing_outcomes.prompt_version`).

## Future: Integrity & Signing (Phase 25)

To support **Aggressive Mode** (autonomous patching without human review), we are implementing a signing layer:
- **Patch Signatures**: SHA-256 hashes of patches + blueprint state.
- **Verification**: `aqueduct run` verifies signatures before applying autonomous patches to detect tampering.
- **Audit Log**: Verified patches are surfaced in `aqueduct patch log` with a `✓` status.
