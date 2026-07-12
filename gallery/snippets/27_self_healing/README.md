# Self-Healing Demo

Demonstrates Aqueduct's LLM-driven self-healing agent.

## Setup

```bash
pip install -r requirements.txt
```

## How it works

The blueprint has a deliberate error — `total * 1.1` references a column
named `total` that doesn't exist (the actual column is `total_amt`).

When the pipeline fails:
1. Aqueduct captures the structured error from Spark
2. The LLM agent analyses the failure context
3. The agent proposes a PatchSpec fixing `total` → `total_amt`
4. Staged patches are reviewed and applied with `aqueduct patch commit`
5. You inspect the staged patch and apply with `aqueduct patch commit`

## How to Run

```bash
python populate_data.py

aqueduct run blueprint.yml          # fails with column-not-found
aqueduct heal blueprint.yml         # agent analyses and proposes fix
aqueduct patch list                 # see staged patches
aqueduct patch commit               # commit and apply the fix
```

## `@aq.*` runtime functions

This snippet also uses Probe signals (`schema_snapshot`, `row_count_estimate`)
to show how `@aq.date.today()`, `@aq.run.id()`, and Context Registry tokens get
injected at compile time — see the probe results in the observability store.

**Requires an LLM provider** (set `AQ_OPENAI_API_KEY` or configure in
`aqueduct.yml` agent block). Without it, `heal` will report the error but skip
agent analysis.
