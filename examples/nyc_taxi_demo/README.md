# NYC Taxi Analytics — Aqueduct Demo Pipeline

End-to-end implementation guide for building a production-grade Aqueduct pipeline
against the NYC Taxi Trip Record dataset. This README is your implementation tracker —
work through each section top to bottom.

---

## What you will build

```
[Yellow Trips Parquet] ─────────────────────────────────────────────────────────┐
[Green Trips Parquet]  ─────────────────────────────────────────────────────────┤
[Taxi Zone CSV]        ── Ingress ── Channel(normalize_zones)                   │
                                                                                  │
yellow: Ingress ── Assert(quality) ── main ─────────────────────────────────────┤
                        │ spillway                                                │
                   [Egress: dead_letter_yellow]                                   │
                                                                                  │
green:  Ingress ── Assert(quality) ── main ─────────────────────────────────────┤
                        │ spillway                                                │
                   [Egress: dead_letter_green]                                    │
                                                                                  │
        Funnel(union_all) ── Channel(enrich_with_zones)                          │
             ── Probe(schema + null_rates)                                        │
             ── Regulator(freshness gate)                                         │
             ── Channel(feature_engineering)                                      │
             ── Junction(split_by_borough)                                        │
                   │                                                              │
        ┌──────────┼──────────┬──────────┬──────────┐                            │
   [manhattan] [brooklyn] [queens] [bronx] [staten_island]                        │
                   │                                                              │
        Funnel(merge_boroughs) ── Channel(final_aggregations)                    │
                   │                                                              │
        ┌──────────┼──────────┐                                                  │
   [Egress:    [Egress:   [Egress:                                               │
    analytics]  ml_ready]  kpi_depot]                                            │
```

**Capabilities exercised:**
- Multiple Ingress (Parquet + CSV)
- Assert module with all rule types: schema_match, min_rows, null_rate, freshness, sql_row
- Spillway quarantine for bad records (negative fares, impossible GPS coordinates)
- Funnel (union yellow + green)
- Channel (SQL enrichment, feature engineering, aggregations)
- Probe (schema snapshot, null rates, SparkListener row count)
- Regulator (freshness gate: block if data > 48h stale)
- Junction (split by NYC borough — 5 branches)
- Multiple Egress: analytics Parquet, ML-ready features, KPI depot store
- Per-module retry policy on Ingress (flaky S3 simulation)
- Checkpoint/resume on the Funnel module
- LLM self-healing demo: introduce a schema rename, watch auto-fix

---

## Prerequisites

### Software
- Python 3.11+
- Java 17 (required for PySpark; `sdk use java 17` or `use_java17` alias)
- Apache Spark 3.5.x (installed via `aqueduct-core[spark]`)
- Aqueduct installed: `pip install aqueduct-core[spark]`

### Verify setup
```bash
python -c "import pyspark; print(pyspark.__version__)"
aqueduct --version
```

### Directory layout after setup
```
nyc_taxi_demo/
  data/
    yellow/          ← Yellow trip Parquet files (one per month)
    green/           ← Green trip Parquet files
    zones/           ← taxi_zone_lookup.csv
  output/            ← created at runtime by Aqueduct
  dead_letter/       ← quarantine rows written here
  aqueduct.yml       ← engine config (Spark master, store dir)
  blueprint.yml      ← YOUR pipeline (you build this)
  patches/           ← LLM-generated patches land here
  .aqueduct/         ← observability store (obs.db)
```

---

## Step 1 — Download the data

### Yellow Taxi Trips (Parquet)
```bash
mkdir -p data/yellow data/green data/zones

# Download 2 months of Yellow trips (~500K rows each, ~30MB each)
curl -L "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet" \
     -o data/yellow/2024-01.parquet

curl -L "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet" \
     -o data/yellow/2024-02.parquet
```

### Green Taxi Trips (Parquet)
```bash
curl -L "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet" \
     -o data/green/2024-01.parquet

curl -L "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-02.parquet" \
     -o data/green/2024-02.parquet
```

### Taxi Zone Lookup (CSV)
```bash
curl -L "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv" \
     -o data/zones/taxi_zone_lookup.csv
```

### Inspect the schemas
```bash
python -c "
import pyarrow.parquet as pq
print('=== YELLOW ===')
print(pq.read_schema('data/yellow/2024-01.parquet'))
print()
print('=== GREEN ===')
print(pq.read_schema('data/green/2024-01.parquet'))
"
```

**Key columns to note:**

| Yellow column | Green column | Notes |
|---|---|---|
| `tpep_pickup_datetime` | `lpep_pickup_datetime` | Different names — normalize in Channel |
| `tpep_dropoff_datetime` | `lpep_dropoff_datetime` | Different names — normalize in Channel |
| `PULocationID` | `PULocationID` | Same |
| `DOLocationID` | `DOLocationID` | Same |
| `fare_amount` | `fare_amount` | Same |
| `passenger_count` | `passenger_count` | Same |
| `trip_distance` | `trip_distance` | Same |
| `payment_type` | `payment_type` | Same |
| *(absent)* | `trip_type` | Green-only: 1=street-hail, 2=dispatch |

Both need a `taxi_type` column added (yellow/green) and pickup/dropoff renamed to
`pickup_datetime`/`dropoff_datetime` for schema unification before Funnel.

---

## Step 2 — Create `aqueduct.yml`

Engine config. Start local, upgrade to remote cluster later.

```yaml
# aqueduct.yml
deployment:
  engine: spark
  master_url: "local[4]"      # use 4 cores; change to spark://10.0.0.39:7077 for remote

spark_config:
  spark.sql.adaptive.enabled: "true"
  spark.sql.shuffle.partitions: "50"   # small for local mode
  spark.sql.session.timeZone: "UTC"

stores:
  store_dir: ".aqueduct"

webhooks:
  on_failure: "" # set to Slack/PagerDuty URL when ready
```

---

## Step 3 — Build `blueprint.yml` (your implementation task)

Work through these sections in order. Each section maps to a capability.

### 3.1 — Top-level header

```yaml
aqueduct: "1.0"
id: nyc.taxi.analytics
name: "NYC Taxi Analytics Pipeline"
description: |
  Ingests Yellow and Green taxi trip data, validates quality,
  enriches with zone geography, features engineering, splits by borough,
  and produces analytics + ML-ready outputs.

context:
  yellow_path: "data/yellow/*.parquet"
  green_path:  "data/green/*.parquet"
  zones_path:  "data/zones/taxi_zone_lookup.csv"
  output_path: "output"
  dead_letter_path: "dead_letter"
```

### 3.2 — Three Ingress modules

Read Yellow, Green, and Zone CSV. Each is a separate Ingress.

**Hints:**
- Yellow and Green: `format: parquet`, use wildcard path for multiple monthly files
- Zones: `format: csv`, need `options: {header: "true", inferSchema: "true"}`
- Use `${ctx.xxx_path}` for paths

**Checklist:**
- [ ] `read_yellow` Ingress
- [ ] `read_green` Ingress
- [ ] `read_zones` Ingress

### 3.3 — Normalize Zones (Channel)

Simple SQL to standardize the zone lookup table. Rename columns:
- `LocationID` → `zone_id`
- `Borough`    → `borough`
- `Zone`       → `zone_name`
- `service_zone` → `service_zone`

**Checklist:**
- [ ] `normalize_zones` Channel with `op: sql`
- [ ] Query selects and renames the four columns from `read_zones`

### 3.4 — Quality Assert on Yellow (Assert)

This is the core data quality gate. Yellow trips have known issues:

| Rule | Type | Value | on_fail |
|---|---|---|---|
| Schema has required columns | `schema_match` | `{fare_amount: double, PULocationID: int, ...}` | `abort` |
| Not empty | `min_rows` | 1000 | `abort` |
| `fare_amount` not mostly null | `null_rate` | column: fare_amount, max: 0.05 | `abort` |
| Pickup time exists | `null_rate` | column: tpep_pickup_datetime, max: 0.001 | `abort` |
| Valid fare (non-negative) | `sql_row` | `fare_amount >= 0` | `quarantine` |
| Valid coordinates | `sql_row` | `PULocationID > 0 AND PULocationID <= 265` | `quarantine` |
| Reasonable distance | `sql_row` | `trip_distance >= 0 AND trip_distance < 500` | `quarantine` |

Rows failing `quarantine` rules go to `dead_letter_yellow` via spillway edge.

**Checklist:**
- [ ] `assert_yellow` Assert module with all 7 rules
- [ ] Edge: `read_yellow` → `assert_yellow` (main)
- [ ] Edge: `assert_yellow` → `dead_letter_yellow` (spillway)
- [ ] `dead_letter_yellow` Egress writing to `${ctx.dead_letter_path}/yellow/`

### 3.5 — Quality Assert on Green (Assert)

Same structure as Yellow but different column names:

| Rule | Adjust for Green |
|---|---|
| `schema_match` | `lpep_pickup_datetime` instead of `tpep_pickup_datetime` |
| `null_rate` | `lpep_pickup_datetime` |
| Same `sql_row` rules | Same logic |

**Checklist:**
- [ ] `assert_green` Assert module
- [ ] Edge: `read_green` → `assert_green` (main)
- [ ] Spillway → `dead_letter_green` Egress

### 3.6 — Normalize Yellow (Channel)

After Assert, normalize column names so Yellow and Green can be unioned:

```sql
SELECT
  'yellow'                    AS taxi_type,
  tpep_pickup_datetime        AS pickup_datetime,
  tpep_dropoff_datetime       AS dropoff_datetime,
  PULocationID                AS pickup_location_id,
  DOLocationID                AS dropoff_location_id,
  passenger_count,
  trip_distance,
  fare_amount,
  payment_type,
  NULL                        AS trip_type       -- green-only column, fill with NULL
FROM assert_yellow
```

**Checklist:**
- [ ] `normalize_yellow` Channel
- [ ] Edge: `assert_yellow` → `normalize_yellow`

### 3.7 — Normalize Green (Channel)

Same as above but for Green (`lpep_*` → pickup/dropoff, `trip_type` is not NULL):

```sql
SELECT
  'green'                     AS taxi_type,
  lpep_pickup_datetime        AS pickup_datetime,
  lpep_dropoff_datetime       AS dropoff_datetime,
  PULocationID                AS pickup_location_id,
  DOLocationID                AS dropoff_location_id,
  passenger_count,
  trip_distance,
  fare_amount,
  payment_type,
  trip_type
FROM assert_green
```

**Checklist:**
- [ ] `normalize_green` Channel
- [ ] Edge: `assert_green` → `normalize_green`

### 3.8 — Funnel (merge Yellow + Green)

Union both normalized DataFrames. Schemas are now identical.

```yaml
type: Funnel
config:
  mode: union_all
  inputs: [normalize_yellow, normalize_green]
  schema_check: strict
```

Enable checkpoint on this Funnel — it's the most expensive join point. If the
pipeline crashes downstream, `--resume` will reload from here.

**Checklist:**
- [ ] `merge_trips` Funnel
- [ ] `checkpoint: true` on this module
- [ ] Edges from both normalize modules → merge_trips

### 3.9 — Enrich with Zone Data (Channel)

Join merged trips with the normalized zone lookup:

```sql
SELECT
  t.*,
  pu.borough   AS pickup_borough,
  pu.zone_name AS pickup_zone,
  do.borough   AS dropoff_borough,
  do.zone_name AS dropoff_zone
FROM merge_trips t
LEFT JOIN normalize_zones pu ON t.pickup_location_id  = pu.zone_id
LEFT JOIN normalize_zones do ON t.dropoff_location_id = do.zone_id
```

**Hint:** Multi-input Channel — both `merge_trips` and `normalize_zones` connect
to this Channel via main edges. Both are registered as temp views by their module ID.

**Checklist:**
- [ ] `enrich_with_zones` Channel
- [ ] Edges: `merge_trips` → `enrich_with_zones`, `normalize_zones` → `enrich_with_zones`

### 3.10 — Probe (observability)

Attach a Probe to `enrich_with_zones` to capture quality signals after enrichment:

```yaml
type: Probe
attach_to: enrich_with_zones
config:
  signals:
    - type: schema_snapshot
    - type: row_count_estimate
      method: spark_listener
    - type: null_rates
      columns: [pickup_borough, dropoff_borough, fare_amount]
      fraction: 0.05
    - type: sample_rows
      n: 10
```

**Checklist:**
- [ ] `probe_enriched` Probe
- [ ] Signal edge: `probe_enriched` → `freshness_gate` (port: signal)

### 3.11 — Regulator (freshness gate)

Gate downstream analytics on data freshness. If the Probe signals stale data
(null_rate for borough too high, indicating zone join failure), block.

```yaml
type: Regulator
config:
  on_block: warn      # start with warn; change to abort when confident
```

**Checklist:**
- [ ] `freshness_gate` Regulator
- [ ] Edge: `enrich_with_zones` → `freshness_gate` (main)
- [ ] Edge: `probe_enriched` → `freshness_gate` (signal)
- [ ] Edge: `freshness_gate` → `feature_engineering` (main)

### 3.12 — Feature Engineering (Channel)

Derive analytical features. This is where Spark SQL becomes expressive:

```sql
SELECT
  *,
  -- Trip duration in minutes
  (UNIX_TIMESTAMP(dropoff_datetime) - UNIX_TIMESTAMP(pickup_datetime)) / 60.0
    AS trip_duration_min,

  -- Fare per mile (avoid division by zero)
  CASE WHEN trip_distance > 0 THEN fare_amount / trip_distance ELSE NULL END
    AS fare_per_mile,

  -- Time features
  HOUR(pickup_datetime)                         AS pickup_hour,
  DAYOFWEEK(pickup_datetime)                    AS pickup_dow,
  CASE WHEN DAYOFWEEK(pickup_datetime) IN (1,7) THEN 1 ELSE 0 END
    AS is_weekend,

  -- Surge proxy: flag trips with fare_per_mile > 10
  CASE WHEN fare_amount / NULLIF(trip_distance, 0) > 10.0 THEN 1 ELSE 0 END
    AS is_surge_proxy,

  -- Payment type label
  CASE payment_type
    WHEN 1 THEN 'credit_card'
    WHEN 2 THEN 'cash'
    WHEN 3 THEN 'no_charge'
    WHEN 4 THEN 'dispute'
    ELSE 'unknown'
  END AS payment_label
FROM freshness_gate
```

**Checklist:**
- [ ] `feature_engineering` Channel
- [ ] Edge: `freshness_gate` → `feature_engineering`

### 3.13 — Junction (split by borough)

Split the enriched, featured trips by pickup borough:

```yaml
type: Junction
config:
  mode: conditional
  branches:
    - id: manhattan
      condition: "pickup_borough = 'Manhattan'"
    - id: brooklyn
      condition: "pickup_borough = 'Brooklyn'"
    - id: queens
      condition: "pickup_borough = 'Queens'"
    - id: bronx
      condition: "pickup_borough = 'Bronx'"
    - id: other
      condition: "_else_"       # Staten Island + unknowns
```

**Checklist:**
- [ ] `split_by_borough` Junction
- [ ] Edge: `feature_engineering` → `split_by_borough`

### 3.14 — Per-Borough Aggregations (Channel × 5)

For each borough branch, compute trip statistics. Use the same SQL template,
just change the input view name.

Manhattan example:
```sql
SELECT
  DATE(pickup_datetime)    AS trip_date,
  pickup_hour,
  taxi_type,
  COUNT(*)                 AS trip_count,
  SUM(fare_amount)         AS total_revenue,
  AVG(fare_amount)         AS avg_fare,
  AVG(trip_duration_min)   AS avg_duration_min,
  AVG(trip_distance)       AS avg_distance,
  SUM(is_surge_proxy)      AS surge_trip_count,
  SUM(is_weekend)          AS weekend_trip_count
FROM split_by_borough.manhattan
GROUP BY 1, 2, 3
```

**Note:** `split_by_borough.manhattan` is the frame_store key — the SQL FROM clause
uses the branch port key, which IS the temp view name registered by Aqueduct.
Use `FROM __input__` since it's single-input for each aggregation Channel.

**Checklist:**
- [ ] `agg_manhattan` Channel; edge: `split_by_borough` (port: manhattan) → `agg_manhattan`
- [ ] `agg_brooklyn` Channel; edge port: brooklyn
- [ ] `agg_queens` Channel; edge port: queens
- [ ] `agg_bronx` Channel; edge port: bronx
- [ ] `agg_other` Channel; edge port: other

### 3.15 — Funnel (merge borough aggregations)

```yaml
type: Funnel
config:
  mode: union_all
  inputs:
    - agg_manhattan
    - agg_brooklyn
    - agg_queens
    - agg_bronx
    - agg_other
  schema_check: strict
```

**Checklist:**
- [ ] `merge_borough_aggs` Funnel
- [ ] Edges from all 5 agg modules → merge_borough_aggs

### 3.16 — Final Analytics Channel

Add `loaded_at` timestamp and `pipeline_run_id` for lineage:

```sql
SELECT
  *,
  CURRENT_TIMESTAMP()  AS loaded_at,
  '@aq.run.id()'       AS pipeline_run_id
FROM merge_borough_aggs
ORDER BY trip_date, pickup_hour, taxi_type
```

**Checklist:**
- [ ] `final_analytics` Channel

### 3.17 — Three Egress modules

**Analytics output** (human-readable aggregated stats):
```yaml
id: egress_analytics
type: Egress
config:
  format: parquet
  path: "${ctx.output_path}/analytics/date=@aq.date.today(format='%Y-%m-%d')"
  mode: overwrite
  partition_by: [trip_date]
```

**ML-ready features** (full trip-level enriched dataset):
```yaml
id: egress_ml_features
type: Egress
config:
  format: parquet
  path: "${ctx.output_path}/ml_features"
  mode: overwrite
```

**KPI depot store** (total revenue for this run — cross-run comparison):
```yaml
id: egress_kpi_revenue
type: Egress
config:
  format: depot
  key: "nyc_taxi.total_revenue_today"
  value_expr: "SUM(fare_amount)"   # aggregate over final_analytics
```

**Checklist:**
- [ ] `egress_analytics` Egress; edge from `final_analytics`
- [ ] `egress_ml_features` Egress; edge from `feature_engineering`
- [ ] `egress_kpi_revenue` Egress; edge from `final_analytics`

---

## Step 4 — Retry policy config

Add to Yellow and Green Ingress modules to simulate production-grade resilience:

```yaml
on_failure:
  max_attempts: 3
  backoff_strategy: exponential
  backoff_base_seconds: 5
  on_exhaustion: abort
```

---

## Step 5 — Add agent config for LLM self-healing demo

```yaml
agent:
  approval_mode: auto
  provider: openai_compat
  base_url: "http://localhost:11434/v1"   # Ollama
  model: "gemma3:12b"
  max_tokens: 2048
  on_pending_patches: warn
```

---

## Step 6 — Run it

```bash
# First run
aqueduct run blueprint.yml

# Check observability
python -c "
import duckdb
con = duckdb.connect('.aqueduct/obs.db')
print(con.sql('SELECT module_id, records_written, duration_ms FROM module_metrics ORDER BY captured_at').df())
con.close()
"

# Check probe signals (null rates after enrichment)
python -c "
import duckdb, json
con = duckdb.connect('.aqueduct/obs.db')
rows = con.sql(\"SELECT probe_id, signal_type, payload FROM probe_signals\").fetchall()
for r in rows:
    print(r[0], r[1], json.loads(r[2]))
con.close()
"

# Check dead-letter quarantine
python -c "
import pyarrow.parquet as pq
t = pq.read_table('dead_letter/yellow/')
print(f'Quarantined yellow rows: {len(t)}')
print(t.to_pandas()[['fare_amount','trip_distance','_aq_error_rule']].head(10))
"

```

```bash
# Run with checkpoint enabled, then resume after crash
# (Set checkpoint: true on merge_trips, kill after Funnel succeeds)
aqueduct run blueprint.yml --resume <run_id_from_first_run>
```

### Pro-tips for Production
- **Targeted Runs**: Use `aqueduct run blueprint.yml --module agg_manhattan` to run only a specific module and its dependencies.
- **Secret Management**: Instead of hardcoding paths or credentials, use `${sec.my_secret}` in the YAML and provide them via environment variables or a secret manager.
- **Incremental Ingress**: For S3/GCS sources, use `format: parquet` with `options: {recursiveFileLookup: "true"}` and combined with Aqueduct's state store to only process new files.

---

## Step 7 — LLM self-healing demo

Introduce a breaking schema change to simulate a real-world schema drift:

```bash
# 1. Rename fare_amount → fare in the source data (simulate upstream schema change)
python -c "
import pyarrow.parquet as pq, pyarrow as pa
t = pq.read_table('data/yellow/2024-01.parquet')
t = t.rename_columns([c if c != 'fare_amount' else 'fare' for c in t.schema.names])
pq.write_table(t, 'data/yellow/2024-01.parquet')
print('Renamed fare_amount → fare')
"

# 2. Run pipeline — Assert schema_match will fail
aqueduct run blueprint.yml
# Expected: pipeline fails with schema_match error on assert_yellow

# 3. Watch LLM generate a patch and auto-apply it
# (see patches/applied/ for the generated PatchSpec JSON)

# 4. Run again with the auto-applied patch
aqueduct run blueprint.yml
```

---

## Step 8 — Verify outputs

### Analytics output
```python
import pyarrow.parquet as pq, pandas as pd
df = pq.read_table('output/analytics/').to_pandas()
print(df.groupby('taxi_type')[['trip_count','total_revenue']].sum())
print(df.groupby('pickup_hour')['avg_fare'].mean().sort_index())
```

### KPI from depot store
```python
import duckdb
con = duckdb.connect('.aqueduct/depot.db')
print(con.sql("SELECT key, value, updated_at FROM depot ORDER BY updated_at DESC").df())
```

### Run history
```python
import duckdb
con = duckdb.connect('.aqueduct/obs.db')
print(con.sql("SELECT run_id, status, started_at, finished_at FROM run_records").df())
```

---

## What to present at the end

Run the pipeline once (working), measure these, then present them:

| Metric | Command |
|---|---|
| Lines of YAML vs equivalent PySpark | Count blueprint.yml lines vs estimated raw PySpark |
| Rows processed per module | `SELECT * FROM module_metrics` in signals.db |
| Quarantine rate (bad data %) | `dead_letter/yellow/` row count ÷ raw row count |
| Quality gate catches | Which Assert rules fired, how many rows quarantined |
| LLM patch latency | Time from failure to auto-applied patch (schema drift demo) |
| Resume savings | First run time vs resumed run time (only re-runs post-Funnel) |

---

## Troubleshooting

| Issue | Cause | Fix |
|---|---|---|
| `java.lang.OutOfMemoryError` | Too many partitions on local | Add `spark.sql.shuffle.partitions: "8"` to aqueduct.yml |
| Schema mismatch in Funnel | Yellow/Green normalize Channels not producing identical schemas | Check NULL columns are cast to same type |
| Assert schema_match fail on fresh data | Column names changed in NYC data updates | Update `expected` in schema_match rule |
| Dead-letter dir empty despite bad rows | Spillway edge missing from blueprint | Add `from: assert_yellow, to: dead_letter_yellow, port: spillway` edge |
| `_aq_error_*` columns in main output | sql_row rule routing to main instead of spillway | Confirm `on_fail: quarantine` (not `on_fail: abort`) |
