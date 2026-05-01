# Comprehensive End-to-End Test — Phase 6.9

Validates all Phase 6 Aqueduct module types against real MinIO and Spark services.

## Blueprint

```
raw_orders   (Ingress, MinIO s3a://) ─┐
raw_customers (Ingress, MinIO s3a://) ─┤
                                       ↓
                         clean_and_enrich  (Channel — SQL: filter nulls/future-dates, LEFT JOIN, RLIKE email validation)
                                       │
                         probe_enriched   (Probe — schema_snapshot, null_rates, sample_rows)
                                       │ signal
                         quality_gate     (Regulator — on_block: skip; gate always open in Phase 6)
                                       │
                         split_by_region  (Junction — conditional: us / eu / other)
                        /        |        \
                 (port: us) (port: eu) (port: other)
                        \        |        /
                         merge_all        (Funnel — union_all)
                                       │
                         tag_metadata     (Channel — add processed_at + currency)
                                       │
                         final_output     (Egress — MinIO parquet, date-partitioned)
```

## Prerequisites

### Python packages
```bash
pip install aqueduct boto3 pandas pyarrow
```

### Java 17 (required for local Spark)
```bash
source ~/.bashrc && use_java17
```

### Spark JAR dependencies
On first run, Spark downloads `hadoop-aws` and `aws-java-sdk-bundle` from Maven Central
(configured in `aqueduct.yml` under `spark.jars.packages`).  Ensure the cluster nodes
have internet access, or pre-stage the JARs.

## Steps

### 1 — Generate mock data
```bash
cd examples/comprehensive_test
python generate_data.py
```

Expected output:
```
Generating mock data …
  customers :   200  (malformed emails: 20)
  orders    :  1000  (null amounts: 50, future dates: 30)

Local copies → examples/comprehensive_test/data/
Connecting to MinIO at http://<IP ADDRESS>:9000 …
  created bucket 'raw-data'   (or: already exists)
Uploading …
  ✓  s3://raw-data/orders/orders.parquet
  ✓  s3://raw-data/customers/customers.parquet

✓ Upload complete.
```

### 2 — Run the blueprint
```bash
aqueduct run blueprint.yml --config aqueduct.yml
```

Expected output (abbreviated):
```
▶ comprehensive.order.blueprint  (9 modules)  run=<uuid>  master=spark://<IP ADDRESS>:7077
  ✓ raw_orders
  ✓ raw_customers
  ✓ clean_and_enrich
  ✓ probe_enriched
  ✓ quality_gate
  ✓ split_by_region
  ✓ merge_all
  ✓ tag_metadata
  ✓ final_output

✓ blueprint complete  run_id=<uuid>
```

Note the run_id — needed for verification queries below.

## Verification

### A — Processed output in MinIO

Using the MinIO CLI (`mc`):
```bash
mc alias set myminio http://<IP ADDRESS>:9000 admin admin12345
mc ls myminio/processed-data/orders/
# expect: date=YYYY-MM-DD/orders.parquet
mc stat "myminio/processed-data/orders/date=$(date +%Y-%m-%d)/orders.parquet"
```

Using `boto3` (Python):
```python
import boto3, pandas as pd
from io import BytesIO

s3 = boto3.client("s3", endpoint_url="http://<IP ADDRESS>:9000",
                  aws_access_key_id="admin", aws_secret_access_key="admin12345")
from datetime import date
key = f"orders/date={date.today()}/orders.parquet"
obj = s3.get_object(Bucket="processed-data", Key=key)
df  = pd.read_parquet(BytesIO(obj["Body"].read()))
print(df.shape)          # expect ~920 rows (1000 - ~50 null amounts - ~30 future dates)
print(df.columns.tolist())
print(df["currency"].value_counts())   # USD / EUR split
```

### B — Run record in DuckDB

```bash
duckdb .aqueduct/signals/runs.db
```

```sql
SELECT run_id, blueprint_id, status, started_at, finished_at
FROM run_records
ORDER BY started_at DESC
LIMIT 3;
```

Expected: one row per blueprint execution with `status='success'`.

### C — Probe signals in DuckDB

```bash
duckdb .aqueduct/signals/signals.db
```

```sql
-- All signals for the most recent run
SELECT probe_id, signal_type, captured_at
FROM probe_signals
ORDER BY captured_at DESC
LIMIT 20;

-- Schema snapshot: column names and types
SELECT json_extract(payload, '$.fields') AS schema_fields
FROM probe_signals
WHERE signal_type = 'schema_snapshot'
ORDER BY captured_at DESC
LIMIT 1;

-- Null rates for critical columns
SELECT
  json_extract(payload, '$.null_rates.amount')         AS null_rate_amount,
  json_extract(payload, '$.null_rates.customer_email') AS null_rate_email,
  json_extract(payload, '$.null_rates.region')         AS null_rate_region
FROM probe_signals
WHERE signal_type = 'null_rates'
ORDER BY captured_at DESC
LIMIT 1;

-- Sample rows captured by the probe
SELECT json_extract(payload, '$.rows') AS sample_rows
FROM probe_signals
WHERE signal_type = 'sample_rows'
ORDER BY captured_at DESC
LIMIT 1;
```

### D — Inspect the compiled Manifest

```bash
aqueduct compile blueprint.yml | python -m json.tool | head -60
```

Verify that:
- `blueprint_id` = `comprehensive.order.blueprint`
- All 9 module IDs are present
- `probe_enriched.attach_to` = `clean_and_enrich`
- `quality_gate` config has `on_block: skip`
- Egress path contains today's date (e.g. `date=2026-04-18`)

## Known Phase 6 Limitations

| Feature requested | Status | Phase |
|---|---|---|
| PostgreSQL JDBC ingress | Not implemented — JDBC not in `SUPPORTED_FORMATS` | Phase 7+ |
| PostgreSQL JDBC egress | Not implemented | Phase 7+ |
| Spillway module type | Not a module type; it is a signal-port convention | Phase 7+ |
| Depot KV store (`format: depot`) | Not implemented | Phase 7+ |
| `@aq.depot.get()` runtime function | Stub only; Depot DB not wired at runtime | Phase 7+ |
| Python UDF registration via YAML | Not supported; email validation uses RLIKE inline | Phase 7+ |
| Regulator gate evaluation (`passed` signal) | No signal type emits `passed`; gate always open | Phase 7+ |

## Troubleshooting

**S3A connection refused / access denied**
- Verify MinIO is reachable: `curl http://<IP ADDRESS>:9000/minio/health/live`
- Check `spark.hadoop.fs.s3a.*` keys in `aqueduct.yml`

**`hadoop-aws` JAR not found / ClassNotFoundException**
- Ensure Spark nodes have outbound Maven access, or add `-Dspark.jars.packages=...` manually
- Confirm `spark.jars.packages` in `aqueduct.yml` uses `aws-java-sdk-bundle:1.12.262` (transitive dep of `hadoop-aws:3.3.4`; 1.12.367 does not exist on Maven Central)

**Executor OOM**
- Increase `spark.driver.memory` / `spark.executor.memory` in `aqueduct.yml`
- Reduce `fraction` in Probe `null_rates` signal config (currently 0.2)

**Regulator shows `status="skipped"` unexpectedly**
- Check `signals.db` for the probe signal payload: no `passed` key → gate should be open
- Verify `run_id` matches between `runs.db` and `signals.db` queries
