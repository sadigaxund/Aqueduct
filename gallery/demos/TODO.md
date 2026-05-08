# Demos To Implement

The `demos/` directory contains end-to-end, runnable pipelines that showcase Aqueduct's progression from simple to complex.

## 01-hello-world [DONE]
- **Goal**: The "5-minute" experience.
- **Features**: Local CSV ingress, simple SQL Channel (filter/rename), and local CSV egress.
- **Complexity**: Low. No UDFs or Arcades.

## 02-nyc-taxi-etl [IN PROGRESS]
- **Goal**: Real-world data engineering.
- **Features**: Multi-source unioning, Arcades for logic reuse, Python UDFs for feature engineering, and complex joins.
- **Status**: Core logic works; refining UDF stability.

## 03-llm-self-healing [PLANNED]
- **Goal**: Showcase the "Agentic" part of Aqueduct.
- **Features**: Intentionally broken data/blueprint that triggers a Regulator -> LLM loop -> Patch generation.

## 04-financial-compliance [IDEA]
- **Goal**: Strict data quality and auditing.
- **Features**: Extensive Assertions, Probes for value distribution, and Regulator gates that block downstream execution on anomaly.

## 05-multi-cloud-migration [IDEA]
- **Goal**: Data movement patterns.
- **Features**: S3 to GCS transfer, schema mapping, and watermark-based incremental loading.
