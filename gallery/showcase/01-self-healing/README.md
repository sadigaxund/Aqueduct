# Self-Healing Showcase

A single pipeline that demonstrates how Aqueduct **parses, compiles, observes,
quality-gates, and self-heals** — all from one declarative YAML file.

New to Aqueduct? The main [Core Concepts](../../../README.md#core-concepts)
section explains Channel / Assert in about a minute. Skip it if you just want
to watch a pipeline fail and heal itself.

## Pipeline

```
ingest_orders (Ingress)
      │
compute_totals (Channel)      ← total_value = quantity * unit_price
      │
classify_priority (Channel)   ← priority_tier — 👾 has a deliberate bug
      │
quality_gate (Assert)
   ├─ main     → store_orders
   └─ spillway → quarantine_storage
```

## What to run

```bash
cd gallery/showcase/01-self-healing
cp .env.template .env   # optional — point at a real agent endpoint/model
aqueduct run blueprints/showcase_orders.yml
```

Every value in `.env.template` already has a working default (shown after
`:-` in `aqueduct.yml`/`blueprints/showcase_orders.yml`) — you only need a
real `.env` to point `AQ_OLLAMA_URL`/`AQ_OLLAMA_MODEL` at a real endpoint or
tighten the agent budget.

The pipeline **will fail** — `classify_priority` references `total_val`,
which doesn't exist. The real column, computed one step upstream in
`compute_totals`, is `total_value`.

The Surveyor captures the structured Spark error, and the agent loop fires
to fix it. Watch for:

1. Pipeline fails with `UNRESOLVED_COLUMN.WITH_SUGGESTION`
2. Agent receives the structured root cause + suggested columns
3. Agent proposes a patch fixing `total_val → total_value`
4. Patch passes gates (guardrails → compile → lineage → sandbox)
5. Re-run succeeds → output written to `data/output/`

Then take a look at what actually happened:

```bash
pip install -r requirements.txt   # duckdb + pandas + rich, one-time
python inspect_results.py
```

Shows the run's terminal status, the healing attempt history, the full
PatchSpec the agent wrote (rationale, root cause, confidence, operations),
and a real before/after diff of the blueprint (reconstructed from
`patches/backups/` — the snapshot Aqueduct writes before overwriting the
file).

Inspect the healing record:

```bash
duckdb .aqueduct/showcase.self_healing.orders/observability.db \
  "SELECT attempt_num, gate_that_rejected, stop_reason, tokens_in, tokens_out
   FROM heal_attempts ORDER BY attempt_num"
```

## Without an LLM

If no agent endpoint is available, the pipeline still fails clearly — the
healing loop reports "all tiers unreachable". Inspect the structured error
directly:

```bash
duckdb .aqueduct/showcase.self_healing.orders/observability.db \
  "SELECT error_class, object_name, suggested_columns FROM failure_contexts"
```

Expected: `error_class = UNRESOLVED_COLUMN.WITH_SUGGESTION`,
`object_name = total_val`, `suggested_columns` includes `total_value`.

## What each piece demonstrates

| Module | What it shows |
|--------|---------------|
| `ingest_orders` | Context Registry (`${ctx.data_dir}`), `schema_hint` type contracts |
| `compute_totals` | SQL Channel deriving a column the next step depends on |
| `classify_priority` | SQL Channel with CASE routing — **bug planted** (`total_val` instead of `total_value`) |
| `quality_gate` | Assert with `not_null` + `sql_row` rules; **spillway** routes bad rows to quarantine |
| `quarantine_storage` | Spillway sink — bad orders (quantity=0 or negative price) isolated here |
| Agent block | `approval: auto` with a confidence threshold — the healing loop |
