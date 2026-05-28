# Showcase 04 — SocketNews + Snowflake + Airflow + Dashboard

**Near-realtime news ingestion from SocketNews.com, processed by Aqueduct, stored in Snowflake, scheduled by Airflow, visualized in Grafana.**

This showcase is a **scaffold** — the architecture and docker-compose are described here so you can build it in an afternoon. The actual code and config files will be added per-sprint.

---

## Architecture

```
SocketNews (WebSocket / REST)     ← realtime news streams
    │
    ▼
S3/MinIO (raw Parquet land)       ← landed every 5 min by a lightweight consumer
    │
    ▼
Aqueduct (Spark)                  ← clean, enrich, aggregate, self-heal
    │  ┌─ error → LLM agent → patch → resume ─┐
    │  └──────── self-healing loop ─────────────┘
    │
    ▼
Snowflake (JDBC Egress)           ← warehouse: fact + dimension tables
    │
    ▼
Grafana / Superset                ← dashboards on Snowflake
```

### Data Flow

1. **SocketNews consumer** — a small Python script (or Airflow `PythonOperator`) connects to SocketNews via Socket.IO, subscribes to one or more topic channels, and lands batches as Parquet files on S3/MinIO every 5 minutes. The consumer tracks its offset in a Depot watermark so no articles are duplicated or missed across restarts.

2. **Aqueduct pipeline** — runs on a Spark cluster (local or standalone), reads the latest Parquet batch via Depot watermark, cleans/normalizes the data, enriches it (country codes, language tags, deduplication), and writes to Snowflake via JDBC Egress.

3. **Self-healing** — SocketNews API schema can change (new fields, renamed fields). When the pipeline breaks, the Surveyor captures the failure, invokes the LLM agent, and the agent patches the Blueprint. The Airflow DAG detects `HEAL_PENDING` exit code, defers, and resumes when the patch is applied.

4. **Dashboard** — Grafana connected to Snowflake (or a Postgres intermediate) showing: articles ingested per hour, top countries, top topics, error rates, pipeline health.

---

## Components

| Component | Role | How it connects |
|---|---|---|
| **SocketNews** | Real-time news data source | Socket.IO subscription → Parquet files on S3 |
| **MinIO** | S3-compatible object storage | Stores raw Parquet batches (one file per micro-batch) |
| **Aqueduct** | Pipeline orchestration + self-healing | Runs hourly, reads MinIO, writes Snowflake |
| **Snowflake** | Cloud data warehouse | JDBC target for clean/enriched articles |
| **Airflow** | Scheduler + DAG management | Triggers consumer + Aqueduct pipeline; handles `HEAL_PENDING` deferral |
| **Grafana** | Dashboard / visualization | Queries Snowflake for live metrics |

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Python | 3.11+ | |
| Spark | 3.5+ or 4.0 | Spark 4.0 recommended for Delta write support |
| Java | 17 | |
| Docker + Compose | recent | For MinIO, Airflow, Spark cluster |
| Snowflake account | free trial | JDBC driver auto-downloaded by Spark |
| SocketNews API key | free tier | Register at [socketnews.com](https://socketnews.com) |
| Ollama (or LLM API key) | recent | For self-healing agent; a local 7B model works |

---

## Getting Started (coming soon)

The full implementation will include:

1. **`socketnews_consumer/`** — Python package that connects to SocketNews, subscribes to topics, lands Parquet batches on MinIO with Depot watermark tracking.
2. **`blueprints/news_pipeline.yml`** — Aqueduct Blueprint that reads Parquet → cleans → writes Snowflake.
3. **`aqueduct.yml`** — Engine config with Snowflake JDBC connection, Depot backend, and agent config.
4. **`dags/news_dag.py`** — Airflow DAG with two tasks: consumer (every 5 min) + Aqueduct pipeline (hourly).
5. **`docker-compose.yml`** — Full stack: MinIO, Spark cluster, Airflow, Grafana.
6. **`grafana/dashboards/`** — Pre-built dashboards for the portfolio.

---

## Portfolio Illustration

The diagram for this showcase captures the full pipeline in one view:

**Left-to-right data flow:**
```
SocketNews (globe icon) → S3 buckets (stacked cylinders) → Spark executor (fire icon → Snowflake flake → Grafana bar chart
```

**Around the Spark node, a cycle:**
```
Spark error (red bolt) → structured FailureContext (document icon) → LLM (brain icon) → patch (wrench icon) → gates (checkmark/shield) → re-run (green play)
```

**At the bottom, a timeline:**
```
14:00 ───────── 14:05 ───────── 14:10 ───────── ▶
 (batch 1)      (batch 2)      (batch 3)
    ↓              ↓              ↓
 Watermark:       Watermark:      Watermark:
 T14:00 → T14:05  T14:05 → T14:10 T14:10 → T14:15
```

This tells the story: **realtime source → reliable batch processing → autonomous self-healing → warehouse → dashboard.**

---

## Why This Works for a Portfolio

| Element | Why it impresses |
|---|---|
| **SocketNews API** | Real-world, third-party data source — not fake sample data |
| **Snowflake** | Industry-standard warehouse — shows enterprise integration |
| **Airflow** | Orchestration maturity — dependency management, deferral pattern |
| **Self-healing** | Differentiator — most pipelines just crash; this one fixes itself |
| **Grafana dashboard** | Visual output — graphs everyone can understand |
| **Micro-batch + watermarks** | Proves you understand incremental processing patterns |