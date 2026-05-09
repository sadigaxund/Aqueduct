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

## Error Handling
- [ ] `dead_letter_office.yml`: A pattern for capturing and storing all row-level failures across the pipeline.
- [ ] `partial_success_retry.yml`: Configuring retry policies that ignore specific transient error types.

## Reusability
- [ ] `arcade_nested_context.yml`: Passing dynamic context through multiple layers of Arcades.
- [ ] `shared_udf_package.yml`: Best practice for organizing and importing a shared UDF library.

## State Management
- [ ] `idempotent_backfill.yml`: Using `@aq.date` and context profiles to run historical slices.
- [ ] `watermark_progression.yml`: Reading a high-watermark from the Depot and updating it after an Egress.

## Advanced LLM Loops
- [ ] `agent_custom_prompt.yml`: Customizing the system prompt for the self-healing agent.
- [ ] `automated_patch_review.yml`: Using a Regulator to gate the application of LLM-generated patches.

## Security
- [ ] `secret_fallback.yml`: Using `@aq.secret` with a local environment variable fallback for CI/CD.
