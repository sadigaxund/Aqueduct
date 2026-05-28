# Showcase 03 — Self-Healing Tutorial

Three interactive demos that walk you through the self-healing loop step by step. Each demo uses a different defect and agent config so you can see how the Agent behaves in different scenarios:

| Demo | What it proves |
|---|---|
| `01_schema_drift.yml` | Spark `UNRESOLVED_COLUMN.WITH_SUGGESTION` → structured error extraction → LLM resolves the correct column name from the suggestion list |
| `02_guardrail_apply_reject.yml` | Apply-time guardrail rejection feeds back into the reprompt loop — the LLM self-corrects instead of failing silently |
| `03_unrecoverable_budget.yml` | Multi-axis budget (time + tokens + signatures) actually terminates runaway loops and records the correct termination reason |

All three write to the same `observability.db` so a single query at the end shows the whole picture.

## Prereqs

- Python 3.11+, `pip install -e '.[spark,llm]'` from repo root (or wheel install of `aqueduct-core[spark]`)
- Java 17 + Spark 4.0 (`use_java17: true` in `aqueduct.yml` will pick the right JDK if `JAVA_HOME_17` is set)
- Ollama (or any OpenAI-compatible endpoint) running locally with a small coder model, e.g.
  ```bash
  ollama pull qwen2.5-coder:7b
  export AQ_OLLAMA_URL=http://localhost:11434
  ```
- DuckDB CLI optional but recommended for the post-run inspection (`brew install duckdb` / `apt install duckdb`)

## Get into the demo dir

```bash
cd gallery/showcase/03-self-healing
```

Everything below assumes you are inside this directory. The local `aqueduct.yml` is the engine config; the three blueprints are under `blueprints/`.

---

## DEMO 1 — Structured error extraction

```bash
aqueduct run blueprints/01_schema_drift.yml
```

Expect: run fails (status=error). Take note of the printed `run_id` — call it `R1`.

### 1a. Confirm the structured error was captured

```bash
duckdb .aqueduct/obs/schema_drift_demo/observability.db <<'SQL'
SELECT run_id, error_class, object_name, suggested_columns, sql_state
FROM failure_contexts
ORDER BY started_at DESC LIMIT 1;
SQL
```

You should see:

```
error_class         = UNRESOLVED_COLUMN.WITH_SUGGESTION
object_name         = event_ts
suggested_columns   = ["event_id","event_time","user_id","event_type"]
sql_state           = 42703
```

If `error_class IS NULL` → the structured error extractor did not fire. Either the exception was not a `PySparkException` (check the stack trace), or `_extract_structured_error` threw and swallowed the error. Run `python -c "from aqueduct.surveyor.surveyor import _extract_structured_error; print('ok')"` to confirm it imports.

### 1b. Confirm the prompt uses the structured block

```bash
aqueduct heal R1 --print-prompt | tee /tmp/prompt-R1.txt
```

Then:

```bash
grep -n "Root cause (structured)" /tmp/prompt-R1.txt   # expect at least 1 hit
grep -n "Actual columns available" /tmp/prompt-R1.txt  # expect: event_id, event_time, ...
grep -nc "## Stack trace" /tmp/prompt-R1.txt           # expect: 0
```

If you see `## Stack trace` the structured extraction fell back to the raw stack trace — meaning the error didn't contain parseable structured fields (or `_extract_structured_error` threw).

### 1c. Confirm the patch landed

```bash
ls patches/pending/
cat patches/pending/*schema_drift*.json | python -m json.tool | head -40
```

Look at `root_cause` and the `operations[]` block — should mention `event_time` (the real column), NOT `ts` or any hallucinated name. The structured `proposal` list from Spark eliminates the column-name guess; the LLM never needs to invent a column name.

---

## DEMO 2 — Guardrail rejection with reprompt

```bash
aqueduct run blueprints/02_guardrail_apply_reject.yml
```

Take the `run_id` — call it `R2`.

### 2a. Confirm multiple attempts, first rejected by apply gate

```bash
duckdb .aqueduct/obs/guardrail_apply_reject_demo/observability.db <<'SQL'
SELECT attempt_num, gate_that_rejected, escalated, stop_reason,
       substr(signature_hash, 1, 8) AS sig
FROM heal_attempts
WHERE run_id = (SELECT MAX(run_id) FROM heal_attempts)
ORDER BY attempt_num;
SQL
```

You should see 2+ rows. The first one(s) should have `gate_that_rejected = 'apply'` — the guardrail rejected the model's first patch. Pre-1.1.0 this would have been the FINAL row (rejection dropped silently); in 1.1.0 the loop reprompts.

Terminal `stop_reason` should be one of:
- `solved` — the model picked a non-forbidden op on retry (best case)
- `stuck_signature` — model kept proposing the same forbidden op even after escalation (also a valid outcome for a small model)
- `exhausted_attempts` — ran out of reprompt budget

All three confirm the loop is doing the right thing. The bug we are guarding against is "1 row, gate_that_rejected=apply, stop_reason=exhausted_attempts" with NO retry — that would mean the rejection was wasted.

### 2b. Eyeball the reprompt content

```bash
aqueduct heal R2 --print-prompt | grep -A 20 "guardrail"
```

The prompt should explicitly tell the model that the previous attempt was rejected by a guardrail and which op was forbidden. That is the Phase 34 unification — apply-side failures travel back through the same prompt builder as schema failures.

---

## DEMO 3 — Multi-axis budget terminates correctly (Phase 34)

```bash
aqueduct run blueprints/03_unrecoverable_budget.yml
```

Take the `run_id` — call it `R3`.

### 3a. Confirm budget axes did the job

```bash
duckdb .aqueduct/obs/unrecoverable_budget_demo/observability.db <<'SQL'
SELECT attempt_num, escalated, stop_reason,
       tokens_in, tokens_out, latency_ms
FROM heal_attempts
WHERE run_id = (SELECT MAX(run_id) FROM heal_attempts)
ORDER BY attempt_num;
SQL
```

You should see:

- Total rows ≤ `max_reprompts` (4 in the config).
- Exactly ONE row with `escalated = true` (triggered when `same_error_consecutive=2` tripped; the loop then bumped temperature to 0.8 and switched to the skeleton-anchored reprompt template for one more attempt).
- Final row's `stop_reason` ∈ `{stuck_signature, budget_seconds_exceeded, budget_tokens_exceeded, exhausted_attempts}`. The exact reason depends on your model speed — slow model → `budget_seconds_exceeded`; fast model → `stuck_signature`.

### 3b. Confirm the stderr / final message names the reason

Scroll the `aqueduct run` output above. The final error line should explicitly say which axis tripped (e.g. `agent loop ended with stop_reason=stuck_signature`). If it's a generic "ran out of attempts" with no `stop_reason` → CLI wire-through regressed.

---

## Cross-cutting check — vocabulary sanity

After all three demos have run:

```bash
duckdb -c "ATTACH '.aqueduct/obs/schema_drift_demo/observability.db' AS d1;
           ATTACH '.aqueduct/obs/guardrail_apply_reject_demo/observability.db' AS d2;
           ATTACH '.aqueduct/obs/unrecoverable_budget_demo/observability.db' AS d3;
           SELECT 'd1' src, stop_reason, COUNT(*) FROM d1.heal_attempts GROUP BY 2
           UNION ALL SELECT 'd2', stop_reason, COUNT(*) FROM d2.heal_attempts GROUP BY 2
           UNION ALL SELECT 'd3', stop_reason, COUNT(*) FROM d3.heal_attempts GROUP BY 2;"
```

Every `stop_reason` value should be one of: `solved`, `exhausted_attempts`, `budget_seconds_exceeded`, `budget_tokens_exceeded`, `stuck_signature`, `progress_stalled`, `api_error`. Anything else → enum drift.

---

## Clean up

```bash
rm -rf .aqueduct patches/pending/*
```

Leaves the blueprints + CSV untouched so you can re-run.
