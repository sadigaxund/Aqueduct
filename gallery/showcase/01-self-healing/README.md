# Self-Healing Showcase

A single pipeline that demonstrates how Aqueduct **parses, compiles, observes,
routes, quality-gates, and self-heals** — all from one declarative YAML file.

## Pipeline

```
ingest_orders ─┐
load_products ─┼─→ enrich_products (Arcade: join)   ← product_name, category, total_value
               │
load_customers ┼─→ enrich_customers (Arcade: join)  ← customer_name, region, segment
               │
               ▼
        classify_priority (Channel)   ← priority_tier — 👾 has a deliberate bug
               │
        quality_gate (Assert)
           ├─ main → route_by_priority (Junction)
           │           ├── express  → store_express
           │           ├── standard → store_standard
           │           └── bulk     → store_bulk
           └─ spillway → quarantine_storage
```

Reference data (`load_products`, `load_customers`) is read once at the parent
level and passed into each Arcade as a named upstream — the Arcades themselves
contain nothing but the join, so they stay reusable and parent-agnostic.

## What to run

```bash
cd gallery/showcase/01-self-healing
cp .env.template .env   # optional — point at a real agent endpoint/model
aqueduct run blueprints/showcase_orders.yml
```

Every value in `.env.template` already has a working default (shown after
`:-` in `aqueduct.yml`/`blueprints/showcase_orders.yml`) — you only need a
real `.env` to point `AQ_OLLAMA_URL`/`AQ_OLLAMA_MODEL` at a real endpoint,
tighten the agent budget, or send the `on_success`/`on_failure` webhooks
somewhere that isn't a 404.

The pipeline **will fail** — `classify_priority` references `total_val`
which doesn't exist (the real column is `total_value`).

The Surveyor captures the structured Spark error, and the agent loop fires
to fix it. Watch for:

1. `Source Health` — probe prints schema + row count inline
2. Both Arcades render as a nested tree (`enrich_products └─ ✓ join`)
3. Pipeline fails with `UNRESOLVED_COLUMN.WITH_SUGGESTION`
4. `on_failure` hooks fire (command + webhook) — the healing loop is
   engaged, but the run's terminal state is still "failed" until it resolves
5. Agent receives the structured root cause + suggested columns
6. Agent proposes a patch fixing `total_val → total_value`
7. Patch passes gates (guardrails → compile → lineage → sandbox)
8. Re-run succeeds → `on_success` hooks fire → output written to `data/output/`

Then take a look at what actually happened:

```bash
pip install -r requirements.txt   # duckdb + pandas + rich, one-time
python inspect_results.py
```

Shows the run's terminal status, the healing attempt history, the full
PatchSpec the agent wrote (rationale, root cause, confidence, operations),
a real before/after diff of the blueprint (reconstructed from
`patches/backups/` — the snapshot Aqueduct writes before overwriting the
file), and the final output row counts per priority tier.

Inspect the healing record:

```bash
duckdb .aqueduct/showcase.self_healing.orders/observability.db \
  "SELECT attempt_num, gate_that_rejected, stop_reason, tokens_in, tokens_out
   FROM heal_attempts ORDER BY attempt_num"
```

## Without an LLM

If no agent endpoint is available, the pipeline still fails clearly — the
healing loop reports "all tiers unreachable" and the `on_failure` hooks still
fire (hooks care about the run's terminal state, not whether an agent was
reachable). Inspect the structured error directly:

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
| `load_products` / `load_customers` | Reference data read once at parent level, joined by both Arcades |
| `enrich_products` | **Arcade** — join-only sub-pipeline, takes two real upstream inputs via `context_override` |
| `enrich_customers` | **Arcade** — same shape, chained after `enrich_products`'s output |
| `classify_priority` | SQL Channel with CASE routing — **bug planted** (`total_val` instead of `total_value`) |
| `quality_gate` | Assert with `not_null` + `sql_row` rules; **spillway** routes bad rows to quarantine |
| `route_by_priority` | Junction `conditional` mode — three branches by `priority_tier` |
| `observe_source` | Probe with `report: stdout` — inline schema + row count |
| `quarantine_storage` | Spillway sink — one bad order (quantity=0 or negative price) isolated here |
| `hooks` | `on_success`/`on_failure` — command + webhook, fired based on the run's terminal state |
| Agent block | `approval: auto` with multi-axis budget — the healing loop |
